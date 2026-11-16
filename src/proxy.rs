//! `POST /v1/chat/completions`: resolve the model, rewrite the body and the
//! headers, call the upstream and pass its response through.
//!
//! M1 scope: one attempt on the first candidate route, no retries, no
//! failover, no usage capture. Paid routes are refused outright until spend
//! cap enforcement exists.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use futures_util::StreamExt;
use futures_util::future::ready;
use serde_json::Value;

use crate::AppState;
use crate::catalog::{self, Candidate, ResolveError};
use crate::error::ApiError;

/// Request body limit for `/v1/chat/completions`. axum's 2 MB default is far
/// too small for agent requests that carry large contexts.
pub const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024 * 1024;

/// Upper bound on establishing a TCP (and TLS) connection to an upstream.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Longest requested model name echoed back in an error message.
const MAX_ECHOED_MODEL_CHARS: usize = 200;

pub const HEADER_PROVIDER: &str = "x-router-provider";
pub const HEADER_UPSTREAM_MODEL: &str = "x-router-upstream-model";
pub const HEADER_ATTEMPTS: &str = "x-router-attempts";
pub const HEADER_OVERHEAD_MS: &str = "x-router-overhead-ms";

/// Response headers never copied from the upstream: hop-by-hop headers
/// (RFC 9110 section 7.6.1), framing headers the server sets itself, and
/// cookies (the router is a transport, not a browser session).
const DROPPED_RESPONSE_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "set-cookie",
];

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
}

/// Build the shared upstream HTTP client: rustls with the `ring` crypto
/// provider, connection pooling, no redirects and no response decompression
/// (bodies pass through byte-identical).
pub fn build_client() -> Result<reqwest::Client, String> {
    // Fails only when a provider is already installed (e.g. a second client
    // in the same process), which is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .tcp_nodelay(true)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| format!("cannot build HTTP client: {e}"))
}

async fn chat_completions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let started = Instant::now();
    match forward(&state, &headers, body, started).await {
        Ok(response) => response,
        Err(e) => e.into_response(),
    }
}

async fn forward(
    state: &AppState,
    client_headers: &HeaderMap,
    body: Result<Bytes, BytesRejection>,
    started: Instant,
) -> Result<Response, ApiError> {
    let body = body.map_err(|rejection| {
        let status = rejection.status();
        let code = if status == StatusCode::PAYLOAD_TOO_LARGE {
            "request_too_large"
        } else {
            "invalid_body"
        };
        ApiError::new(status, "invalid_request_error", code, rejection.body_text())
    })?;

    let mut request: Value = serde_json::from_slice(&body).map_err(|e| {
        ApiError::bad_request(
            "invalid_json",
            format!("request body is not valid JSON: {e}"),
        )
    })?;
    drop(body);
    let fields = request.as_object_mut().ok_or_else(|| {
        ApiError::bad_request("invalid_request", "request body must be a JSON object")
    })?;

    let requested = match fields.get("model") {
        Some(Value::String(m)) => m.clone(),
        Some(_) => {
            return Err(
                ApiError::bad_request("invalid_model", "\"model\" must be a string")
                    .with_param("model"),
            );
        }
        None => {
            return Err(
                ApiError::bad_request("missing_model", "missing required field \"model\"")
                    .with_param("model"),
            );
        }
    };

    let resolution =
        catalog::resolve(&state.config, &requested).map_err(|e| model_error(&requested, e))?;
    // M1: only the first candidate is attempted (failover is M2).
    let Some(candidate) = resolution.first() else {
        return Err(model_error(
            &requested,
            ResolveError::UnknownModel {
                suggestions: Vec::new(),
            },
        ));
    };
    let provider = candidate.provider;

    if provider.paid {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "permission_error",
            "paid_route_disabled",
            format!(
                "provider \"{}\" is paid; paid routes are disabled until spend-cap enforcement is implemented",
                provider.id
            ),
        )
        .with_param("model"));
    }

    let wants_stream = fields.get("stream").and_then(Value::as_bool) == Some(true);
    fields.insert(
        "model".to_string(),
        Value::String(candidate.model.to_string()),
    );
    for param in &provider.drop_params {
        fields.remove(param);
    }
    let outgoing = serde_json::to_vec(&request).map_err(|_| internal("cannot encode request"))?;

    let upstream_headers = upstream_request_headers(candidate, client_headers)?;
    let url = upstream_url(&provider.base_url);

    let overhead = started.elapsed();
    let upstream = state
        .http
        .post(url)
        .headers(upstream_headers)
        .body(outgoing)
        .send()
        .await
        .map_err(|_| upstream_unreachable(&provider.id))?;

    let status = upstream.status();
    let mut headers = forwarded_response_headers(upstream.headers());
    add_router_headers(&mut headers, candidate, 1, overhead);

    let is_event_stream = upstream
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.trim_start()
                .to_ascii_lowercase()
                .starts_with("text/event-stream")
        });

    let body = if wants_stream || is_event_stream {
        // Forward chunks as they arrive. A transport error mid-stream ends
        // the body (the SSE error event is M2). Dropping this body, e.g.
        // because the client went away, drops the upstream response and so
        // cancels the upstream request.
        let chunks = upstream
            .bytes_stream()
            .scan((), |_, chunk| ready(chunk.ok()))
            .map(Ok::<Bytes, Infallible>);
        Body::from_stream(chunks)
    } else {
        let bytes = upstream
            .bytes()
            .await
            .map_err(|_| upstream_unreachable(&provider.id))?;
        Body::from(bytes)
    };

    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    Ok(response)
}

/// `{base_url}/chat/completions`, tolerating a trailing slash on `base_url`.
fn upstream_url(base_url: &str) -> String {
    format!("{}/chat/completions", base_url.trim_end_matches('/'))
}

/// Headers sent upstream: the provider key replaces whatever the client sent
/// (the client's own key never reaches a provider), plus `content-type` and
/// the client's `accept`. Nothing else from the client is forwarded.
fn upstream_request_headers(
    candidate: Candidate<'_>,
    client_headers: &HeaderMap,
) -> Result<HeaderMap, ApiError> {
    let provider = candidate.provider;
    let key = provider
        .keys
        .first()
        .ok_or_else(|| internal("provider has no API key"))?;
    let mut auth = HeaderValue::from_str(&format!("Bearer {}", key.expose())).map_err(|_| {
        internal(format!(
            "API key for provider \"{}\" contains characters not allowed in an HTTP header",
            provider.id
        ))
    })?;
    auth.set_sensitive(true);

    let mut headers = HeaderMap::new();
    headers.insert(header::AUTHORIZATION, auth);
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    if let Some(accept) = client_headers.get(header::ACCEPT) {
        headers.insert(header::ACCEPT, accept.clone());
    }
    Ok(headers)
}

/// Copy end-to-end upstream response headers, dropping hop-by-hop and
/// framing headers, anything the upstream's `Connection` header names, and
/// any `x-router-*` header (those are ours to set).
fn forwarded_response_headers(upstream: &HeaderMap) -> HeaderMap {
    let connection_listed: Vec<String> = upstream
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .filter(|name| !name.is_empty())
        .collect();

    let mut out = HeaderMap::with_capacity(upstream.len());
    for (name, value) in upstream {
        let n = name.as_str();
        if DROPPED_RESPONSE_HEADERS.contains(&n)
            || n.starts_with("x-router-")
            || connection_listed.iter().any(|c| c == n)
        {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

fn add_router_headers(
    headers: &mut HeaderMap,
    candidate: Candidate<'_>,
    attempts: u32,
    overhead: Duration,
) {
    let values = [
        (
            HEADER_PROVIDER,
            HeaderValue::from_str(&candidate.provider.id),
        ),
        (
            HEADER_UPSTREAM_MODEL,
            HeaderValue::from_str(candidate.model),
        ),
        (
            HEADER_ATTEMPTS,
            HeaderValue::from_str(&attempts.to_string()),
        ),
        (
            HEADER_OVERHEAD_MS,
            HeaderValue::from_str(&format!("{:.3}", overhead.as_secs_f64() * 1000.0)),
        ),
    ];
    for (name, value) in values {
        // A provider id or model id with characters that cannot appear in a
        // header value is left out rather than failing the request.
        if let Ok(value) = value {
            headers.insert(HeaderName::from_static(name), value);
        }
    }
}

fn model_error(requested: &str, error: ResolveError) -> ApiError {
    let shown: String = requested.chars().take(MAX_ECHOED_MODEL_CHARS).collect();
    let ellipsis = if shown.len() < requested.len() {
        "…"
    } else {
        ""
    };
    let (message, suggestions) = match error {
        ResolveError::UnknownModel { suggestions } => (
            format!(
                "model \"{shown}{ellipsis}\" is not a configured pool or provider_id/model route"
            ),
            suggestions,
        ),
        ResolveError::NotInAllowlist {
            provider,
            suggestions,
        } => (
            format!(
                "model \"{shown}{ellipsis}\" is not in provider \"{provider}\"'s models allowlist"
            ),
            suggestions,
        ),
    };
    let message = if suggestions.is_empty() {
        message
    } else {
        format!("{message}; did you mean: {}", suggestions.join(", "))
    };
    ApiError::new(
        StatusCode::NOT_FOUND,
        "invalid_request_error",
        "model_not_found",
        message,
    )
    .with_param("model")
    .with_suggestions(suggestions)
}

/// The upstream could not be reached or failed before a complete response.
/// Deliberately carries no transport error text, which can contain URLs.
fn upstream_unreachable(provider_id: &str) -> ApiError {
    ApiError::new(
        StatusCode::BAD_GATEWAY,
        "upstream_error",
        "upstream_unreachable",
        format!("could not get a response from provider \"{provider_id}\""),
    )
}

fn internal(message: impl Into<String>) -> ApiError {
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "server_error",
        "internal_error",
        message,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_url_handles_trailing_slash() {
        assert_eq!(upstream_url("http://h/v1"), "http://h/v1/chat/completions");
        assert_eq!(upstream_url("http://h/v1/"), "http://h/v1/chat/completions");
    }

    #[test]
    fn hop_by_hop_and_router_headers_are_not_forwarded() {
        let mut h = HeaderMap::new();
        h.insert(
            "content-type",
            HeaderValue::from_static("text/event-stream"),
        );
        h.insert(
            "connection",
            HeaderValue::from_static("keep-alive, x-custom-hop"),
        );
        h.insert("x-custom-hop", HeaderValue::from_static("1"));
        h.insert("keep-alive", HeaderValue::from_static("timeout=5"));
        h.insert("transfer-encoding", HeaderValue::from_static("chunked"));
        h.insert("content-length", HeaderValue::from_static("12"));
        h.insert("set-cookie", HeaderValue::from_static("a=b"));
        h.insert("x-router-provider", HeaderValue::from_static("spoofed"));
        h.insert(
            "x-ratelimit-remaining-requests",
            HeaderValue::from_static("9"),
        );
        h.append("x-multi", HeaderValue::from_static("1"));
        h.append("x-multi", HeaderValue::from_static("2"));

        let out = forwarded_response_headers(&h);
        let mut names: Vec<&str> = out.keys().map(HeaderName::as_str).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            ["content-type", "x-multi", "x-ratelimit-remaining-requests"]
        );
        assert_eq!(out.get_all("x-multi").iter().count(), 2);
    }

    #[test]
    fn model_error_is_404_with_suggestions_and_truncated_echo() {
        let long = "m".repeat(1000);
        let e = model_error(
            &long,
            ResolveError::UnknownModel {
                suggestions: vec!["coder".to_string()],
            },
        );
        assert_eq!(e.status, StatusCode::NOT_FOUND);
        assert!(e.message.len() < 400, "{}", e.message);
        assert!(e.message.ends_with("did you mean: coder"));
        assert_eq!(e.body()["error"]["suggestions"][0], "coder");
        assert_eq!(e.body()["error"]["param"], "model");
    }
}
