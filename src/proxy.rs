//! `POST /v1/chat/completions`: resolve the model, pick candidates, call the
//! upstream with retries and pass its response through.
//!
//! Retry loop (PRD section 6): each eligible `(provider, key, model)` entry
//! is tried in order, up to `max_attempts`, while no byte has gone to the
//! client. A failure after streaming started cannot fail over: the stream
//! ends with an SSE `error` event. Paid routes are refused outright until
//! spend cap enforcement exists.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use futures_util::StreamExt;
use futures_util::future::ready;
use serde_json::{Map, Value, json};

use crate::AppState;
use crate::catalog::{self, ResolveError, RouteKind};
use crate::config::{ApiKey, Provider, Strategy};
use crate::error::ApiError;
use crate::limiter;
use crate::select::{Entry, Skipped};

/// Request body limit for `/v1/chat/completions`. axum's 2 MB default is far
/// too small for agent requests that carry large contexts.
pub const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024 * 1024;

/// Upper bound on establishing a TCP (and TLS) connection to an upstream.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Longest requested model name echoed back in an error message.
const MAX_ECHOED_MODEL_CHARS: usize = 200;

/// Most upstream error body kept for replaying to the client.
const MAX_ERROR_BODY_BYTES: usize = 256 * 1024;

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

/// How an upstream status is handled (PRD section 6 failure table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Success,
    /// Returned to the client unchanged, no retry, no state change.
    Passthrough,
    RateLimited,
    ServerError,
    KeyRejected,
    Missing,
}

fn classify(status: StatusCode, is_pool: bool) -> Class {
    match status.as_u16() {
        200..=299 => Class::Success,
        429 => Class::RateLimited,
        500 | 502 | 503 | 504 => Class::ServerError,
        401..=403 => Class::KeyRejected,
        404 if is_pool => Class::Missing,
        _ => Class::Passthrough,
    }
}

/// The last retryable upstream error, returned if every attempt fails.
struct LastError {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
    provider: String,
    model: String,
}

async fn forward(
    state: &Arc<AppState>,
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
    let Some(first) = resolution.first() else {
        return Err(model_error(
            &requested,
            ResolveError::UnknownModel {
                suggestions: Vec::new(),
            },
        ));
    };

    if first.provider.paid {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "permission_error",
            "paid_route_disabled",
            format!(
                "provider \"{}\" is paid; paid routes are disabled until spend-cap enforcement is implemented",
                first.provider.id
            ),
        )
        .with_param("model"));
    }

    let wants_stream = fields.get("stream").and_then(Value::as_bool) == Some(true);
    let pool = match resolution.kind {
        RouteKind::Pool(name) => Some((
            name,
            state
                .config
                .pool(name)
                .map_or(Strategy::Priority, |p| p.strategy),
        )),
        RouteKind::Explicit => None,
    };
    let is_pool = pool.is_some();

    let plan = state
        .limiter
        .plan(&resolution.candidates, pool, Instant::now());
    if plan.eligible.is_empty() {
        return Err(unavailable(&plan.skipped, Instant::now()));
    }

    let max_attempts = state.config.max_attempts.max(1);
    let first_byte_timeout = Duration::from_secs(state.config.first_byte_timeout_secs);
    let mut attempts: u32 = 0;
    let mut first_overhead: Option<Duration> = None;
    let mut last: Option<LastError> = None;
    let mut last_provider = first.provider.id.clone();

    for entry in &plan.eligible {
        if attempts >= max_attempts {
            break;
        }
        let provider = entry.provider;
        let Some(key) = provider.keys.get(entry.key_idx) else {
            continue;
        };
        // State may have changed since planning (breaker, disabled key).
        if state
            .limiter
            .skip_reason(&provider.id, entry.key_idx, entry.model, Instant::now())
            .is_some()
        {
            continue;
        }
        attempts += 1;
        last_provider.clone_from(&provider.id);

        let outgoing = encode_request(fields, provider, entry.model)?;
        let upstream_headers = upstream_request_headers(key, &provider.id, client_headers)?;
        let url = upstream_url(&provider.base_url);
        state
            .limiter
            .note_request(&provider.id, entry.key_idx, entry.model);
        let overhead = *first_overhead.get_or_insert_with(|| started.elapsed());

        let send = state
            .http
            .post(url)
            .headers(upstream_headers)
            .body(outgoing)
            .send();
        let upstream = match tokio::time::timeout(first_byte_timeout, send).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) | Err(_) => {
                apply_failure(state, entry, Class::ServerError, None, "unreachable");
                continue;
            }
        };

        let status = upstream.status();
        let class = classify(status, is_pool);
        if !matches!(class, Class::Success | Class::Passthrough) {
            let retry_after = upstream
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| limiter::parse_retry_after(v, SystemTime::now()));
            let headers = forwarded_response_headers(upstream.headers());
            let body = read_capped(upstream).await;
            apply_failure(state, entry, class, retry_after, status.as_str());
            last = Some(LastError {
                status,
                headers,
                body,
                provider: provider.id.clone(),
                model: entry.model.to_string(),
            });
            continue;
        }

        let mut headers = forwarded_response_headers(upstream.headers());
        add_router_headers(&mut headers, &provider.id, entry.model, attempts, overhead);
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
            if class == Class::Success {
                state
                    .limiter
                    .record_success(&provider.id, entry.key_idx, entry.model);
            }
            Body::from_stream(guarded_stream(state, entry, upstream))
        } else {
            match upstream.bytes().await {
                Ok(bytes) => {
                    if class == Class::Success {
                        state
                            .limiter
                            .record_success(&provider.id, entry.key_idx, entry.model);
                    }
                    Body::from(bytes)
                }
                Err(_) if class == Class::Success => {
                    // Nothing has reached the client yet, so this can fail over.
                    apply_failure(state, entry, Class::ServerError, None, "body_error");
                    continue;
                }
                Err(_) => return Err(upstream_unreachable(&provider.id)),
            }
        };

        let mut response = Response::new(body);
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        return Ok(response);
    }

    if attempts == 0 {
        // Every entry went unavailable between planning and attempting.
        let replan = state
            .limiter
            .plan(&resolution.candidates, None, Instant::now());
        return Err(unavailable(&replan.skipped, Instant::now()));
    }
    match last {
        Some(l) => {
            let mut headers = l.headers;
            add_router_headers(
                &mut headers,
                &l.provider,
                &l.model,
                attempts,
                first_overhead.unwrap_or_default(),
            );
            let mut response = Response::new(Body::from(l.body));
            *response.status_mut() = l.status;
            *response.headers_mut() = headers;
            Ok(response)
        }
        None => Err(upstream_unreachable(&last_provider)),
    }
}

/// Serialise the request for one entry: `model` rewritten, the provider's
/// `drop_params` removed. Dropped fields are put back afterwards so the next
/// attempt (another provider) starts from the original body.
fn encode_request(
    fields: &mut Map<String, Value>,
    provider: &Provider,
    model: &str,
) -> Result<Vec<u8>, ApiError> {
    fields.insert("model".to_string(), Value::String(model.to_string()));
    let removed: Vec<(&String, Value)> = provider
        .drop_params
        .iter()
        .filter_map(|p| fields.remove(p).map(|v| (p, v)))
        .collect();
    let encoded = serde_json::to_vec(&*fields);
    for (name, value) in removed {
        fields.insert(name.clone(), value);
    }
    encoded.map_err(|_| internal("cannot encode request"))
}

/// Read an error body, keeping at most [`MAX_ERROR_BODY_BYTES`].
async fn read_capped(mut response: reqwest::Response) -> Bytes {
    let mut buf: Vec<u8> = Vec::new();
    while let Ok(Some(chunk)) = response.chunk().await {
        let room = MAX_ERROR_BODY_BYTES.saturating_sub(buf.len());
        buf.extend_from_slice(chunk.get(..room.min(chunk.len())).unwrap_or_default());
        if buf.len() >= MAX_ERROR_BODY_BYTES {
            break;
        }
    }
    Bytes::from(buf)
}

/// Record a retryable failure in the limiter and log the warnings.
fn apply_failure(
    state: &AppState,
    entry: &Entry<'_>,
    class: Class,
    retry_after: Option<Duration>,
    detail: &str,
) {
    let provider = entry.provider.id.as_str();
    let model = entry.model;
    let key = entry
        .provider
        .keys
        .get(entry.key_idx)
        .map_or_else(|| "?".to_string(), ApiKey::redacted);
    let now = Instant::now();
    match class {
        Class::RateLimited => {
            let len =
                state
                    .limiter
                    .record_rate_limit(provider, entry.key_idx, model, retry_after, now);
            eprintln!(
                "warn: cooldown provider={provider} key={key} model={model} for {}s (429)",
                len.as_secs()
            );
        }
        Class::ServerError => {
            if state.limiter.record_server_failure(provider, now) {
                eprintln!(
                    "warn: circuit breaker open provider={provider} for {}s (last failure: {detail})",
                    limiter::BREAKER_OPEN.as_secs()
                );
            }
        }
        Class::KeyRejected => {
            if state.limiter.disable_key(provider, entry.key_idx) {
                eprintln!(
                    "error: key disabled until restart provider={provider} key={key} (upstream {detail})"
                );
            }
        }
        Class::Missing => {
            if state.limiter.mark_missing(provider, model) {
                eprintln!("warn: model missing provider={provider} model={model} (upstream 404)");
            }
        }
        Class::Success | Class::Passthrough => {}
    }
}

/// Forward the upstream body as it arrives. If the upstream fails mid-stream
/// the client gets one final SSE `error` event and the stream ends (no retry,
/// the failure counts toward the provider's breaker). Dropping the body, e.g.
/// because the client went away, drops the upstream response and so cancels
/// the upstream request.
fn guarded_stream(
    state: &Arc<AppState>,
    entry: &Entry<'_>,
    upstream: reqwest::Response,
) -> impl futures_util::Stream<Item = Result<Bytes, Infallible>> + use<> {
    let state = Arc::clone(state);
    let provider = entry.provider.id.clone();
    let error_event = Bytes::from(format!(
        "event: error\ndata: {}\n\n",
        ApiError::new(
            StatusCode::BAD_GATEWAY,
            "upstream_error",
            "upstream_stream_interrupted",
            format!("provider \"{provider}\" dropped the stream before it finished"),
        )
        .body()
    ));
    upstream.bytes_stream().scan(false, move |failed, chunk| {
        if *failed {
            return ready(None);
        }
        ready(match chunk {
            Ok(bytes) => Some(Ok(bytes)),
            Err(_) => {
                *failed = true;
                if state
                    .limiter
                    .record_server_failure(&provider, Instant::now())
                {
                    eprintln!(
                        "warn: circuit breaker open provider={provider} for {}s (stream failure)",
                        limiter::BREAKER_OPEN.as_secs()
                    );
                }
                eprintln!("warn: stream interrupted provider={provider}");
                Some(Ok(error_event.clone()))
            }
        })
    })
}

/// 503 listing every skipped route with its reason. `Retry-After` is the
/// time until the earliest cooldown or breaker expiry, when there is one.
fn unavailable(skipped: &[Skipped], now: Instant) -> ApiError {
    let routes: Vec<Value> = skipped
        .iter()
        .map(|s| {
            let mut v = json!({"route": s.route, "reason": s.reason.name()});
            if let Some(obj) = v.as_object_mut() {
                if let Some(key) = &s.key {
                    obj.insert("key".to_string(), json!(key));
                }
                if let Some(until) = s.reason.until() {
                    obj.insert(
                        "retry_after_secs".to_string(),
                        json!(secs_until(until, now)),
                    );
                }
            }
            v
        })
        .collect();
    let earliest = skipped
        .iter()
        .filter_map(|s| s.reason.until())
        .min()
        .map(|u| secs_until(u, now));
    let error = ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "upstream_error",
        "no_route_available",
        "no route is available for this model right now",
    )
    .with_routes(Value::Array(routes));
    match earliest {
        Some(secs) => error.with_retry_after(u32::try_from(secs).unwrap_or(u32::MAX)),
        None => error,
    }
}

/// Whole seconds until `until`, rounded up, at least 1.
fn secs_until(until: Instant, now: Instant) -> u64 {
    let d = until.saturating_duration_since(now);
    (d.as_secs() + u64::from(d.subsec_nanos() > 0)).max(1)
}

/// `{base_url}/chat/completions`, tolerating a trailing slash on `base_url`.
fn upstream_url(base_url: &str) -> String {
    format!("{}/chat/completions", base_url.trim_end_matches('/'))
}

/// Headers sent upstream: the provider key replaces whatever the client sent
/// (the client's own key never reaches a provider), plus `content-type` and
/// the client's `accept`. Nothing else from the client is forwarded.
fn upstream_request_headers(
    key: &ApiKey,
    provider_id: &str,
    client_headers: &HeaderMap,
) -> Result<HeaderMap, ApiError> {
    let mut auth = HeaderValue::from_str(&format!("Bearer {}", key.expose())).map_err(|_| {
        internal(format!(
            "API key for provider \"{}\" contains characters not allowed in an HTTP header",
            provider_id
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
    provider_id: &str,
    model: &str,
    attempts: u32,
    overhead: Duration,
) {
    let values = [
        (HEADER_PROVIDER, HeaderValue::from_str(provider_id)),
        (HEADER_UPSTREAM_MODEL, HeaderValue::from_str(model)),
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
