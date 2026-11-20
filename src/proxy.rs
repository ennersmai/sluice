//! `POST /v1/chat/completions`: resolve the model, pick candidates, call the
//! upstream with retries and pass its response through.
//!
//! Retry loop (PRD section 6): each eligible `(provider, key, model)` entry
//! is tried in order, up to `max_attempts`, while no byte has gone to the
//! client. A failure after streaming started cannot fail over: the stream
//! ends with an SSE `error` event. Before planning, explicit paid routes are
//! checked against the daily spend cap, and when no route is eligible but one
//! frees up within `max_wait_secs` the request waits and re-plans. Token usage
//! is captured from the response (see [`Tap`]) and settled once the response
//! ends or is cancelled.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use futures_util::{Stream, StreamExt};
use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::{Map, Value, json};

use crate::AppState;
use crate::catalog::{self, ResolveError, RouteKind};
use crate::config::{ApiKey, Provider, Strategy};
use crate::error::ApiError;
use crate::events::ReqTrack;
use crate::limiter::{self, RemainingHints, Usage};
use crate::select::{self, Entry, ReqInfo, Skipped};

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
/// Request header: `1` excludes providers that train on input, `0` opts out
/// of `private_default`.
pub const HEADER_PRIVATE: &str = "x-router-private";

/// Output allowance assumed for the pre-flight estimate when the request
/// sets neither `max_tokens` nor `max_completion_tokens`.
const DEFAULT_MAX_OUTPUT_TOKENS: u64 = 1024;

/// Longest partial SSE line held while scanning for usage.
const MAX_CARRY_BYTES: usize = 1024 * 1024;

/// Minimum pause before re-planning after a wait, so a bad expiry can never
/// turn the wait loop into a busy loop.
const MIN_WAIT: Duration = Duration::from_millis(10);

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
    // Shared with the response body (settled when it ends) and dropped with
    // this future if the client goes away, which ends the request as 499.
    let mut track: Option<Arc<ReqTrack>> = None;
    match forward(&state, &headers, body, started, &mut track).await {
        Ok(response) => response,
        Err(e) => {
            match &track {
                Some(t) => t.end(e.status.as_u16(), Some(e.code), Usage::default()),
                None => state.events.log_unrouted(e.status.as_u16()),
            }
            e.into_response()
        }
    }
}

/// How an upstream status is handled (PRD section 6 failure table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Success,
    /// Returned to the client unchanged, no retry, no state change.
    Passthrough,
    RateLimited,
    /// This request is too big for the route (pool only): fail over with
    /// no cooldown and no breaker count.
    TooLarge,
    /// HTTP 5xx, first-byte timeout or idle timeout (all model-specific):
    /// counts toward the route's breaker.
    ServerError,
    /// Connect error, timeout or broken body: counts toward the provider's
    /// breaker.
    Unreachable,
    /// 401: the key is disabled until restart.
    KeyRejected,
    /// 402: the whole provider is out of credit until cleared.
    PaymentRequired,
    /// 403: this (provider, model) is unavailable until restart.
    Forbidden,
    Missing,
}

fn classify(status: StatusCode, is_pool: bool) -> Class {
    match status.as_u16() {
        200..=299 => Class::Success,
        429 => Class::RateLimited,
        413 if is_pool => Class::TooLarge,
        500 | 502 | 503 | 504 => Class::ServerError,
        401 => Class::KeyRejected,
        402 => Class::PaymentRequired,
        403 => Class::Forbidden,
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
    track_slot: &mut Option<Arc<ReqTrack>>,
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

    let wants_stream = fields.get("stream").and_then(Value::as_bool) == Some(true);
    let track = Arc::clone(track_slot.insert(state.events.begin(
        fields.get("model").and_then(Value::as_str).unwrap_or("-"),
        wants_stream,
    )));

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
        check_paid(state, first.provider, first.model)?;
    }

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

    let session = if is_pool {
        session_key(
            client_headers,
            &state.config.session_headers,
            &requested,
            fields,
        )
    } else {
        None
    };
    let injection = inject_usage_option(fields, wants_stream);
    let (prompt_estimate, estimate) = estimate_tokens(fields);
    let req = ReqInfo {
        estimate,
        private: is_private(client_headers, state.config.private_default),
    };

    // Wait for a route to free up when that happens within `max_wait_secs`.
    // The future is dropped if the client disconnects, which ends the wait.
    let max_wait = Duration::from_secs(state.config.max_wait_secs);
    let mut wait_deadline: Option<Instant> = None;
    let mut plan = loop {
        let now = Instant::now();
        let plan = state.limiter.plan(&resolution.candidates, pool, &req, now);
        if !plan.eligible.is_empty() {
            break plan;
        }
        let deadline = *wait_deadline.get_or_insert(now + max_wait);
        let earliest = plan.skipped.iter().filter_map(|s| s.reason.until()).min();
        match earliest {
            Some(at) if !max_wait.is_zero() && at <= deadline => {
                let wake = at.max(now + MIN_WAIT);
                tokio::time::sleep_until(tokio::time::Instant::from_std(wake)).await;
            }
            _ => {
                note_skipped(&track, &plan.skipped);
                return Err(unavailable(&plan.skipped, now));
            }
        }
    };

    if let Some(k) = session
        && let Some((p, m)) = state.sticky.get(k, Instant::now())
    {
        select::prefer(&mut plan.eligible, &p, &m);
    }

    let max_attempts = state.config.max_attempts.max(1);
    let first_byte_timeout = Duration::from_secs(state.config.first_byte_timeout_secs);
    let stream_idle_timeout = Duration::from_secs(state.config.stream_idle_timeout_secs);
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
            .skip_reason(
                provider,
                entry.key_idx,
                entry.model,
                req.estimate,
                Instant::now(),
            )
            .is_some()
        {
            continue;
        }
        attempts += 1;
        last_provider.clone_from(&provider.id);
        let attempt_start = Instant::now();

        let outgoing = encode_request(fields, provider, entry.model)?;
        let upstream_headers = upstream_request_headers(key, &provider.id, client_headers)?;
        let url = upstream_url(&provider.base_url);
        state
            .limiter
            .note_request(provider, entry.key_idx, entry.model, Instant::now());
        let overhead = *first_overhead.get_or_insert_with(|| started.elapsed());

        // The first-byte deadline covers the request, the response headers
        // and (for 2xx) the first non-empty body chunk, all measured from
        // here. Nothing reaches the client before that chunk arrives, so a
        // stall at any of those points can still fail over.
        let deadline = tokio::time::Instant::now() + first_byte_timeout;
        let send = state
            .http
            .post(url)
            .headers(upstream_headers)
            .body(outgoing)
            .send();
        let (mut upstream, first_chunk) =
            match tokio::time::timeout_at(deadline, await_first_byte(send)).await {
                Ok(Ok(r)) => r,
                Ok(Err(FirstByteError::Send)) => {
                    track.attempt(provider, entry.model, key, "unreachable", attempt_start);
                    apply_failure(state, entry, Class::Unreachable, None, "unreachable", None);
                    continue;
                }
                Ok(Err(FirstByteError::Body)) => {
                    track.attempt(provider, entry.model, key, "body_error", attempt_start);
                    apply_failure(state, entry, Class::Unreachable, None, "body_error", None);
                    continue;
                }
                Ok(Err(FirstByteError::StreamError)) => {
                    // A model-level failure on this route: count it toward the
                    // route's breaker and try the next candidate.
                    track.attempt(provider, entry.model, key, "stream_error", attempt_start);
                    apply_failure(state, entry, Class::ServerError, None, "stream_error", None);
                    continue;
                }
                Err(_) => {
                    track.attempt(provider, entry.model, key, "timeout", attempt_start);
                    apply_failure(state, entry, Class::ServerError, None, "timeout", None);
                    continue;
                }
            };

        let first_elapsed = attempt_start.elapsed();
        let status = upstream.status();
        let class = classify(status, is_pool);
        if is_pool && class == Class::Passthrough && matches!(status.as_u16(), 400 | 422) {
            // A 400/422 only fails over when it says the request is too big.
            let mut headers = forwarded_response_headers(upstream.headers());
            let Ok(body) = tokio::time::timeout_at(deadline, read_capped(upstream)).await else {
                track.attempt(provider, entry.model, key, "timeout", attempt_start);
                apply_failure(state, entry, Class::ServerError, None, "timeout", None);
                continue;
            };
            if let Some(kind) = too_large_kind(&body) {
                if kind == TooLargeKind::Size {
                    learn_ceiling(state, provider, entry.model, req.estimate);
                }
                track.attempt(provider, entry.model, key, "too_large", attempt_start);
                last = Some(LastError {
                    status,
                    headers,
                    body,
                    provider: provider.id.clone(),
                    model: entry.model.to_string(),
                });
                continue;
            }
            add_router_headers(&mut headers, &provider.id, entry.model, attempts, overhead);
            let reason = format!("http_{}", status.as_u16());
            track.attempt(provider, entry.model, key, &reason, attempt_start);
            track.end(status.as_u16(), None, Usage::default());
            let mut response = Response::new(Body::from(body));
            *response.status_mut() = status;
            *response.headers_mut() = headers;
            return Ok(response);
        }
        if !matches!(class, Class::Success | Class::Passthrough) {
            let hint = (class == Class::RateLimited)
                .then(|| header_hint(upstream.headers()))
                .flatten();
            let headers = forwarded_response_headers(upstream.headers());
            let Ok(body) = tokio::time::timeout_at(deadline, read_capped(upstream)).await else {
                track.attempt(provider, entry.model, key, "timeout", attempt_start);
                apply_failure(state, entry, Class::ServerError, None, "timeout", None);
                continue;
            };
            let google = (class == Class::RateLimited)
                .then(|| limiter::google_quota_hint(&body))
                .flatten();
            let per_day = google.is_some_and(|g| g.per_day)
                || (class == Class::RateLimited && limiter::daily_quota_text(&body));
            // 429 precedence: a per-day quota cools until the provider's next
            // daily reset; then the header hint, Google's RetryInfo, the
            // "try again in" body text; else the exponential backoff.
            let retry_after = if per_day {
                Some(Duration::from_secs(limiter::secs_until_day_reset(
                    limiter::unix_secs(SystemTime::now()),
                    provider.reset_utc_hour,
                )))
            } else {
                match (class, hint) {
                    (_, Some(d)) => Some(d),
                    (Class::RateLimited, None) => google
                        .and_then(|g| g.retry_delay)
                        .or_else(|| limiter::body_hint(&body))
                        .map(limiter::clamp_hint),
                    _ => None,
                }
            };
            let reason = if class == Class::TooLarge {
                learn_ceiling(state, provider, entry.model, req.estimate);
                "too_large".to_string()
            } else if per_day {
                "http_429_per_day".to_string()
            } else {
                format!("http_{}", status.as_u16())
            };
            track.attempt(provider, entry.model, key, &reason, attempt_start);
            apply_failure(state, entry, class, retry_after, &reason, Some(&body));
            last = Some(LastError {
                status,
                headers,
                body,
                provider: provider.id.clone(),
                model: entry.model.to_string(),
            });
            continue;
        }

        let hints = remaining_hints(upstream.headers());
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

        let scope = limiter::scope_key(provider, entry.key_idx, entry.model);
        let body = if wants_stream || is_event_stream {
            let tap = if class == Class::Success {
                state.limiter.record_success(&scope, entry.model);
                if let Some(k) = session {
                    let now = Instant::now();
                    state.sticky.set(k, &provider.id, entry.model, now);
                }
                track.attempt(provider, entry.model, key, "ok", attempt_start);
                Some(Tap::new(
                    Settle::new(state, entry, prompt_estimate, hints, &track, status),
                    injection.injected,
                ))
            } else {
                track.attempt(
                    provider,
                    entry.model,
                    key,
                    &format!("http_{}", status.as_u16()),
                    attempt_start,
                );
                track.end(status.as_u16(), None, Usage::default());
                None
            };
            if class == Class::Success {
                track.first_byte(first_elapsed);
            }
            Body::from_stream(guarded_stream(
                state,
                entry,
                upstream,
                first_chunk,
                tap,
                stream_idle_timeout,
            ))
        } else {
            let mut buf: Vec<u8> = first_chunk.map(|c| c.to_vec()).unwrap_or_default();
            let read = loop {
                match next_chunk(&mut upstream, stream_idle_timeout).await {
                    Ok(Some(c)) => buf.extend_from_slice(&c),
                    Ok(None) => break Ok(Bytes::from(buf)),
                    Err(e) => break Err(e),
                }
            };
            match read {
                Ok(bytes) => {
                    if class == Class::Success {
                        state.limiter.record_success(&scope, entry.model);
                        if let Some(k) = session {
                            let now = Instant::now();
                            state.sticky.set(k, &provider.id, entry.model, now);
                        }
                        track.attempt(provider, entry.model, key, "ok", attempt_start);
                        track.first_byte(first_elapsed);
                        let mut settle =
                            Settle::new(state, entry, prompt_estimate, hints, &track, status);
                        if let Some((usage, cost)) =
                            usage_from_body(&bytes, settle.cost_field.as_deref())
                        {
                            settle.usage = Some(usage);
                            settle.reported_cost = cost;
                        }
                        settle.out_bytes = bytes.len() as u64;
                        settle.completed = true;
                        // Records usage as it drops; the body is already
                        // fully buffered.
                        drop(settle);
                    } else {
                        track.attempt(
                            provider,
                            entry.model,
                            key,
                            &format!("http_{}", status.as_u16()),
                            attempt_start,
                        );
                        track.end(status.as_u16(), None, Usage::default());
                    }
                    Body::from(bytes)
                }
                Err(ChunkError::Idle) => {
                    track.attempt(provider, entry.model, key, "idle_timeout", attempt_start);
                    if class == Class::Success {
                        apply_failure(state, entry, Class::ServerError, None, "idle_timeout", None);
                    }
                    return Err(ApiError::new(
                        StatusCode::GATEWAY_TIMEOUT,
                        "upstream_error",
                        "idle_timeout",
                        format!(
                            "provider \"{}\" stopped sending the response body",
                            provider.id
                        ),
                    ));
                }
                Err(ChunkError::Body) if class == Class::Success => {
                    // Nothing has reached the client yet, so this can fail over.
                    track.attempt(provider, entry.model, key, "body_error", attempt_start);
                    apply_failure(state, entry, Class::Unreachable, None, "body_error", None);
                    continue;
                }
                Err(ChunkError::Body) => {
                    track.attempt(provider, entry.model, key, "body_error", attempt_start);
                    return Err(upstream_unreachable(&provider.id));
                }
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
            .plan(&resolution.candidates, None, &req, Instant::now());
        note_skipped(&track, &replan.skipped);
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
            track.end(l.status.as_u16(), None, Usage::default());
            let mut response = Response::new(Body::from(l.body));
            *response.status_mut() = l.status;
            *response.headers_mut() = headers;
            Ok(response)
        }
        None => Err(upstream_unreachable(&last_provider)),
    }
}

/// Longest prefix (in chars) of a message's JSON text that feeds a derived
/// session key.
const SESSION_PREFIX_CHARS: usize = 4096;

/// The stickiness key of a pool request (see `sticky.rs`): the first
/// non-empty configured session header, else a hash of the first
/// system/developer and first user message. Only the hash leaves here.
fn session_key(
    headers: &HeaderMap,
    names: &[String],
    requested: &str,
    fields: &Map<String, Value>,
) -> Option<u64> {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut h = DefaultHasher::new();
    let from_header = names.iter().find_map(|n| {
        headers
            .get(n.as_str())
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
    });
    if let Some(value) = from_header {
        ("h", requested, value).hash(&mut h);
        return Some(h.finish());
    }
    let messages = fields.get("messages")?.as_array()?;
    let first = |roles: &[&str]| {
        messages
            .iter()
            .find(|m| {
                m.get("role")
                    .and_then(Value::as_str)
                    .is_some_and(|r| roles.contains(&r))
            })
            .and_then(|m| serde_json::to_string(m).ok())
            .map(|s| s.chars().take(SESSION_PREFIX_CHARS).collect::<String>())
    };
    let user = first(&["user"])?;
    let system = first(&["system", "developer"]);
    ("d", requested, system, user).hash(&mut h);
    Some(h.finish())
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

enum FirstByteError {
    Send,
    Body,
    /// A 2xx stream whose first event is an error (for example a provider
    /// streaming `{"error": ...}` after a malformed tool call). Nothing has
    /// reached the client yet, so the request can still fail over.
    StreamError,
}

/// Bytes read past the first data line while checking it, at most.
const FIRST_LINE_LIMIT: usize = 64 * 1024;

/// True when the first non-empty, non-comment SSE line in `bytes` is complete.
fn first_data_line_complete(bytes: &[u8]) -> bool {
    first_data_line(bytes).is_some_and(|(_, complete)| complete)
}

/// The first line that is neither empty nor an SSE comment, and whether it
/// ends with a newline.
fn first_data_line(bytes: &[u8]) -> Option<(&[u8], bool)> {
    let mut rest = bytes;
    loop {
        let (line, complete, next) = match rest.iter().position(|&b| b == b'\n') {
            Some(i) => (&rest[..i], true, &rest[i + 1..]),
            None => (rest, false, &rest[rest.len()..]),
        };
        let trimmed = line.strip_suffix(b"\r").unwrap_or(line);
        if !trimmed.is_empty() && !trimmed.starts_with(b":") {
            return Some((trimmed, complete));
        }
        if !complete {
            return None;
        }
        rest = next;
    }
}

/// True when a stream opens with an error event instead of content:
/// `event: error`, or `data:` carrying a JSON object with a top-level `error`.
fn opens_with_error(bytes: &[u8]) -> bool {
    let Some((line, _)) = first_data_line(bytes) else {
        return false;
    };
    if line.starts_with(b"event:") {
        return line[6..].trim_ascii() == b"error";
    }
    let Some(payload) = line.strip_prefix(b"data:") else {
        return false;
    };
    serde_json::from_slice::<Value>(payload.trim_ascii())
        .ok()
        .and_then(|v| v.as_object().map(|o| o.contains_key("error")))
        .unwrap_or(false)
}

/// Send the request and, for a 2xx response, wait for the first non-empty
/// body chunk (SSE comment lines do not count). Other statuses
/// return as soon as the headers arrive. Run under the first-byte deadline.
async fn await_first_byte(
    send: impl std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
) -> Result<(reqwest::Response, Option<Bytes>), FirstByteError> {
    let mut response = send.await.map_err(|_| FirstByteError::Send)?;
    if !response.status().is_success() {
        return Ok((response, None));
    }
    // Comment-only bytes (`: keep-alive` lines) are held back and forwarded
    // together with the first real chunk; they do not stop the deadline.
    let mut held: Vec<u8> = Vec::new();
    let mut scan = LineScan::default();
    let mut data_started = false;
    loop {
        match response.chunk().await {
            Ok(Some(c)) => {
                data_started |= scan.feed(&c);
                held.extend_from_slice(&c);
                // Once data has started, read on (still under the first-byte
                // deadline) until its first line is complete, so a stream that
                // opens with an error event can be failed over.
                if data_started
                    && (first_data_line_complete(&held) || held.len() >= FIRST_LINE_LIMIT)
                {
                    if opens_with_error(&held) {
                        return Err(FirstByteError::StreamError);
                    }
                    return Ok((response, Some(Bytes::from(held))));
                }
            }
            Ok(None) => {
                if opens_with_error(&held) {
                    return Err(FirstByteError::StreamError);
                }
                let rest = (!held.is_empty()).then(|| Bytes::from(held));
                return Ok((response, rest));
            }
            Err(_) => return Err(FirstByteError::Body),
        }
    }
}

/// Tracks line starts across chunks to tell whether bytes carry a line that
/// is neither empty nor an SSE comment (`:` prefix).
struct LineScan {
    at_line_start: bool,
}

impl Default for LineScan {
    fn default() -> Self {
        Self {
            at_line_start: true,
        }
    }
}

impl LineScan {
    /// Feed bytes; true when a non-empty, non-comment line has started.
    fn feed(&mut self, bytes: &[u8]) -> bool {
        let mut data = false;
        for &b in bytes {
            if self.at_line_start && b != b':' && b != b'\n' && b != b'\r' {
                data = true;
            }
            if self.at_line_start && b == b'\r' {
                continue;
            }
            self.at_line_start = b == b'\n';
        }
        data
    }
}

/// With only comment lines arriving, a stream is cut after this many idle
/// periods.
const COMMENT_ONLY_IDLE_FACTOR: u32 = 5;

enum ChunkError {
    Idle,
    Body,
}

/// Next body chunk, failing when none arrives within `idle` (0 disables).
async fn next_chunk(
    response: &mut reqwest::Response,
    idle: Duration,
) -> Result<Option<Bytes>, ChunkError> {
    if idle.is_zero() {
        return response.chunk().await.map_err(|_| ChunkError::Body);
    }
    match tokio::time::timeout(idle, response.chunk()).await {
        Ok(r) => r.map_err(|_| ChunkError::Body),
        Err(_) => Err(ChunkError::Idle),
    }
}

/// Longest upstream error-body excerpt in a log line, in chars.
const MAX_EXCERPT_CHARS: usize = 300;
/// Bytes of an error body looked at for the excerpt.
const EXCERPT_SCAN_BYTES: usize = 4 * 1024;

/// A bounded, single-line, sanitised excerpt of an upstream error body for
/// the 429 and 402 log lines: configured keys redacted, control characters
/// mapped to spaces, `"` to `'`, whitespace collapsed, at most
/// [`MAX_EXCERPT_CHARS`] chars (with `…` when cut). Error bodies only, never
/// request text.
fn error_excerpt(body: &[u8], keys: &[ApiKey]) -> String {
    let head = body
        .get(..body.len().min(EXCERPT_SCAN_BYTES))
        .unwrap_or_default();
    let mut text = String::from_utf8_lossy(head).into_owned();
    for k in keys.iter().filter(|k| !k.is_anonymous()) {
        text = text.replace(k.expose(), "[redacted]");
    }
    let mapped: String = text
        .chars()
        .map(|c| match c {
            '"' => '\'',
            c if c.is_control() => ' ',
            c => c,
        })
        .collect();
    let collapsed = mapped.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() > MAX_EXCERPT_CHARS {
        let mut cut: String = collapsed.chars().take(MAX_EXCERPT_CHARS).collect();
        cut.push('…');
        cut
    } else {
        collapsed
    }
}

/// Record a retryable failure in the limiter and log the warnings. `body` is
/// the upstream error body when it was read; only the 429 and 402 lines use
/// it, as a sanitised excerpt.
fn apply_failure(
    state: &AppState,
    entry: &Entry<'_>,
    class: Class,
    retry_after: Option<Duration>,
    detail: &str,
    body: Option<&[u8]>,
) {
    let excerpt = || {
        body.map(|b| error_excerpt(b, &entry.provider.keys))
            .filter(|e| !e.is_empty())
    };
    let provider = entry.provider.id.as_str();
    let model = entry.model;
    let key = entry
        .provider
        .keys
        .get(entry.key_idx)
        .map_or_else(|| "?".to_string(), ApiKey::redacted);
    let now = Instant::now();
    let events = &state.events;
    if matches!(
        class,
        Class::RateLimited
            | Class::ServerError
            | Class::Unreachable
            | Class::KeyRejected
            | Class::PaymentRequired
            | Class::Forbidden
            | Class::Missing
    ) {
        events.note_error(provider, Some(model), detail);
    }
    match class {
        Class::RateLimited => {
            let scope = limiter::scope_key(entry.provider, entry.key_idx, model);
            let len = state.limiter.record_rate_limit(scope, retry_after, now);
            let secs = len.as_secs();
            let why = if detail == "http_429_per_day" {
                format!(
                    "429 daily quota exhausted, until next reset_utc_hour={}",
                    entry.provider.reset_utc_hour
                )
            } else {
                "429".to_string()
            };
            match excerpt() {
                Some(e) => crate::elog!(
                    "warn: cooldown provider={provider} key={key} model={model} for {secs}s ({why}) body=\"{e}\""
                ),
                None => crate::elog!(
                    "warn: cooldown provider={provider} key={key} model={model} for {secs}s ({why})"
                ),
            }
            events.route_state(provider, Some(model), Some(key), "cooldown", Some(len));
        }
        Class::ServerError => {
            if state.limiter.record_route_failure(provider, model, now) {
                crate::elog!(
                    "warn: circuit breaker open provider={provider} model={model} for {}s (last failure: {detail})",
                    limiter::BREAKER_OPEN.as_secs()
                );
                events.route_state(
                    provider,
                    Some(model),
                    None,
                    "breaker_open",
                    Some(limiter::BREAKER_OPEN),
                );
            }
        }
        Class::Unreachable => {
            if state.limiter.record_server_failure(provider, now) {
                crate::elog!(
                    "warn: circuit breaker open provider={provider} for {}s (last failure: {detail})",
                    limiter::BREAKER_OPEN.as_secs()
                );
                events.route_state(
                    provider,
                    None,
                    None,
                    "breaker_open",
                    Some(limiter::BREAKER_OPEN),
                );
            }
        }
        Class::KeyRejected => {
            if state.limiter.disable_key(provider, entry.key_idx) {
                crate::elog!(
                    "error: key disabled until restart provider={provider} key={key} (upstream {detail})"
                );
                events.route_state(provider, None, Some(key), "key_disabled", None);
            }
        }
        Class::PaymentRequired => {
            if state.limiter.block_payment(provider) {
                match excerpt() {
                    Some(e) => crate::elog!(
                        "error: provider disabled until cleared on the dashboard provider={provider} key={key} model={model} (upstream 402 payment required) body=\"{e}\""
                    ),
                    None => crate::elog!(
                        "error: provider disabled until cleared on the dashboard provider={provider} key={key} model={model} (upstream 402 payment required)"
                    ),
                }
                events.route_state(provider, None, Some(key), "payment_required", None);
            }
        }
        Class::Forbidden => {
            if state.limiter.mark_unavailable(provider, model) {
                crate::elog!(
                    "warn: route unavailable until restart provider={provider} model={model} (upstream {detail})"
                );
                events.route_state(provider, Some(model), None, "unavailable", None);
            }
        }
        Class::Missing => {
            if state.limiter.mark_missing(provider, model) {
                crate::elog!(
                    "warn: model missing provider={provider} model={model} (upstream 404)"
                );
                events.route_state(provider, Some(model), None, "missing", None);
            }
        }
        Class::Success | Class::Passthrough | Class::TooLarge => {}
    }
}

/// Forward the upstream body as it arrives. If the upstream fails mid-stream
/// the client gets one final SSE `error` event and the stream ends (no retry,
/// the failure counts toward the provider's breaker). Dropping the body, e.g.
/// because the client went away, drops the upstream response and so cancels
/// the upstream request; the [`Tap`] (when present) then settles whatever
/// usage was seen so far.
fn guarded_stream(
    state: &Arc<AppState>,
    entry: &Entry<'_>,
    upstream: reqwest::Response,
    first_chunk: Option<Bytes>,
    tap: Option<Tap>,
    idle: Duration,
) -> impl Stream<Item = Result<Bytes, Infallible>> + use<> {
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
    let idle_event = Bytes::from(format!(
        "event: error\ndata: {}\n\n",
        ApiError::new(
            StatusCode::GATEWAY_TIMEOUT,
            "upstream_error",
            "idle_timeout",
            format!("provider \"{provider}\" stopped sending data (idle_timeout)"),
        )
        .body()
    ));
    let inner = futures_util::stream::iter(first_chunk.map(Ok)).chain(upstream.bytes_stream());
    let seed = StreamState {
        inner: Box::pin(inner),
        tap,
        done: false,
        state: Arc::clone(state),
        provider,
        model: entry.model.to_string(),
        idle,
        scan: LineScan::default(),
        data_deadline: Instant::now() + idle * COMMENT_ONLY_IDLE_FACTOR,
        error_event,
        idle_event,
    };
    futures_util::stream::unfold(seed, |mut st| async move {
        if st.done {
            return None;
        }
        loop {
            let next = if st.idle.is_zero() {
                Some(st.inner.next().await)
            } else {
                // Any bytes reset the idle timer, but comment-only traffic
                // may not outlast `data_deadline`.
                let wait = st
                    .idle
                    .min(st.data_deadline.saturating_duration_since(Instant::now()));
                tokio::time::timeout(wait, st.inner.next()).await.ok()
            };
            if let Some(Some(Ok(bytes))) = &next
                && st.scan.feed(bytes)
            {
                st.data_deadline = Instant::now() + st.idle * COMMENT_ONLY_IDLE_FACTOR;
            }
            let Some(next) = next else {
                st.done = true;
                if let Some(tap) = st.tap.as_mut() {
                    tap.settle.failed = true;
                    st.state
                        .events
                        .note_error(&st.provider, Some(&st.model), "idle_timeout");
                    if st.state.limiter.record_route_failure(
                        &st.provider,
                        &st.model,
                        Instant::now(),
                    ) {
                        crate::elog!(
                            "warn: circuit breaker open provider={} model={} for {}s (idle timeout)",
                            st.provider,
                            st.model,
                            limiter::BREAKER_OPEN.as_secs()
                        );
                    }
                }
                crate::elog!(
                    "warn: stream idle timeout provider={} model={}",
                    st.provider,
                    st.model
                );
                let event = st.idle_event.clone();
                return Some((Ok(event), st));
            };
            match next {
                Some(Ok(bytes)) => match st.tap.as_mut() {
                    Some(tap) => {
                        if let Some(out) = tap.push(bytes) {
                            return Some((Ok(out), st));
                        }
                    }
                    None => return Some((Ok(bytes), st)),
                },
                Some(Err(_)) => {
                    st.done = true;
                    if let Some(tap) = st.tap.as_mut() {
                        tap.settle.failed = true;
                    }
                    st.state
                        .events
                        .note_error(&st.provider, None, "stream_interrupted");
                    if st
                        .state
                        .limiter
                        .record_server_failure(&st.provider, Instant::now())
                    {
                        crate::elog!(
                            "warn: circuit breaker open provider={} for {}s (stream failure)",
                            st.provider,
                            limiter::BREAKER_OPEN.as_secs()
                        );
                    }
                    crate::elog!("warn: stream interrupted provider={}", st.provider);
                    let event = st.error_event.clone();
                    return Some((Ok(event), st));
                }
                None => {
                    st.done = true;
                    if let Some(tap) = st.tap.as_mut() {
                        tap.settle.completed = true;
                    }
                    let rest = st.tap.as_mut().and_then(Tap::finish);
                    return rest.map(|b| (Ok(b), st));
                }
            }
        }
    })
}

struct StreamState {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    tap: Option<Tap>,
    done: bool,
    state: Arc<AppState>,
    provider: String,
    model: String,
    idle: Duration,
    scan: LineScan,
    /// Cut the stream if no data line has arrived by then.
    data_deadline: Instant,
    error_event: Bytes,
    idle_event: Bytes,
}

/// Usage of one response, recorded in the limiter when dropped, so it is
/// settled whether the response finished, failed or was cancelled by the
/// client. Without a `usage` object the estimate is the prompt estimate in
/// and `bytes forwarded / 4` out.
struct Settle {
    state: Arc<AppState>,
    provider: String,
    key_idx: usize,
    model: String,
    prompt_estimate: u64,
    hints: RemainingHints,
    usage: Option<Usage>,
    reported_cost: Option<f64>,
    cost_field: Option<String>,
    out_bytes: u64,
    track: Arc<ReqTrack>,
    /// Upstream status, which is also what the client got.
    status: u16,
    /// The response was delivered in full.
    completed: bool,
    /// The upstream failed after streaming began.
    failed: bool,
}

impl Settle {
    fn new(
        state: &Arc<AppState>,
        entry: &Entry<'_>,
        prompt_estimate: u64,
        hints: RemainingHints,
        track: &Arc<ReqTrack>,
        status: StatusCode,
    ) -> Self {
        Self {
            state: Arc::clone(state),
            provider: entry.provider.id.clone(),
            key_idx: entry.key_idx,
            model: entry.model.to_string(),
            prompt_estimate,
            hints,
            usage: None,
            reported_cost: None,
            cost_field: entry.provider.usage_cost_field.clone(),
            out_bytes: 0,
            track: Arc::clone(track),
            status: status.as_u16(),
            completed: false,
            failed: false,
        }
    }
}

impl Drop for Settle {
    fn drop(&mut self) {
        let usage = self.usage.unwrap_or(Usage {
            input: self.prompt_estimate,
            output: self.out_bytes / 4,
        });
        if let Some(provider) = self.state.config.provider(&self.provider) {
            let crossed = self.state.limiter.record_usage(
                provider,
                self.key_idx,
                &self.model,
                usage,
                self.hints,
                Instant::now(),
                self.reported_cost,
            );
            if crossed {
                let now = Instant::now();
                if let Some(status) = self.state.limiter.credit_status(provider, now) {
                    let reset_time = crate::events::format_utc(status.resets_at);
                    crate::elog!(
                        "warn: credit guard reached provider={} spent=${:.4} of ${:.2} ({:.0}%), ineligible until {}",
                        provider.id,
                        status.spent_usd,
                        status.credit_usd,
                        (status.spent_usd / status.credit_usd) * 100.0,
                        reset_time
                    );
                    self.state.events.route_state(
                        &provider.id,
                        None,
                        None,
                        "credit_exhausted",
                        None,
                    );
                }
            }
        }
        let error = if self.failed {
            Some("stream_interrupted")
        } else if self.completed {
            None
        } else {
            Some("client_closed")
        };
        self.track.end(self.status, error, usage);
    }
}

/// Scans a streamed response line by line for the `usage` object without
/// buffering the stream: only a partial trailing line is carried between
/// chunks. When `filter` is set (the router injected `include_usage` and the
/// client did not ask for it) the usage-only chunk (`choices: []`) is removed
/// from what is forwarded, which makes the tap hold back a partial trailing
/// line until its newline arrives; otherwise chunks pass through untouched
/// and immediately.
struct Tap {
    settle: Settle,
    carry: Vec<u8>,
    /// A dropped `data:` line's blank separator line is dropped too.
    drop_blank: bool,
    /// The current line outgrew [`MAX_CARRY_BYTES`]: pass its tail unscanned.
    skip_line: bool,
    filter: bool,
}

impl Tap {
    fn new(settle: Settle, filter: bool) -> Self {
        Self {
            settle,
            carry: Vec::new(),
            drop_blank: false,
            skip_line: false,
            filter,
        }
    }

    /// Process one upstream chunk; returns what to forward, if anything.
    fn push(&mut self, chunk: Bytes) -> Option<Bytes> {
        self.settle.out_bytes = self.settle.out_bytes.saturating_add(chunk.len() as u64);
        if !self.filter {
            self.scan(&chunk, None);
            return Some(chunk);
        }
        let mut out = Vec::with_capacity(chunk.len());
        self.scan(&chunk, Some(&mut out));
        (!out.is_empty()).then(|| Bytes::from(out))
    }

    /// End of stream: handle an unterminated last line.
    fn finish(&mut self) -> Option<Bytes> {
        if self.carry.is_empty() {
            return None;
        }
        let line = std::mem::take(&mut self.carry);
        let keep = self.handle_line(&line);
        (self.filter && keep).then(|| Bytes::from(line))
    }

    fn scan(&mut self, chunk: &[u8], mut out: Option<&mut Vec<u8>>) {
        let mut pos = 0;
        while let Some(rel) = chunk
            .get(pos..)
            .and_then(|rest| rest.iter().position(|b| *b == b'\n'))
        {
            let end = pos + rel;
            let seg = chunk.get(pos..end).unwrap_or_default();
            pos = end + 1;
            let owned: Vec<u8>;
            let line: &[u8] = if self.carry.is_empty() {
                seg
            } else {
                self.carry.extend_from_slice(seg);
                owned = std::mem::take(&mut self.carry);
                &owned
            };
            let keep = if self.skip_line {
                self.skip_line = false;
                true
            } else {
                self.handle_line(line)
            };
            if keep && let Some(o) = out.as_mut() {
                o.extend_from_slice(line);
                o.push(b'\n');
            }
        }
        let rest = chunk.get(pos..).unwrap_or_default();
        if rest.is_empty() {
            return;
        }
        if self.skip_line {
            if let Some(o) = out.as_mut() {
                o.extend_from_slice(rest);
            }
        } else if self.carry.len() + rest.len() > MAX_CARRY_BYTES {
            // Pathologically long line: give up scanning it, pass it on.
            if let Some(o) = out.as_mut() {
                o.append(&mut self.carry);
                o.extend_from_slice(rest);
            }
            self.carry.clear();
            self.skip_line = true;
        } else {
            self.carry.extend_from_slice(rest);
        }
    }

    /// Inspect one complete line; returns false when it must not be forwarded.
    fn handle_line(&mut self, line: &[u8]) -> bool {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            return !std::mem::take(&mut self.drop_blank);
        }
        self.drop_blank = false;
        let Some(payload) = line.strip_prefix(b"data:") else {
            return true;
        };
        let payload = payload.trim_ascii_start();
        if payload.first() != Some(&b'{') || !mentions_usage_object(payload) {
            return true;
        }
        let Ok(chunk) = serde_json::from_slice::<StreamChunk>(payload) else {
            return true;
        };
        let Some(fields) = chunk.usage else {
            return true;
        };
        self.settle.reported_cost = fields.cost(self.settle.cost_field.as_deref());
        let Some(usage) = fields.into_usage() else {
            return true;
        };
        self.settle.usage = Some(usage);
        let usage_only = chunk.choices.is_none_or(|c| c.is_empty());
        if self.filter && usage_only {
            self.drop_blank = true;
            return false;
        }
        true
    }
}

#[derive(Deserialize)]
struct StreamChunk {
    usage: Option<UsageFields>,
    choices: Option<Vec<IgnoredAny>>,
}

#[derive(Deserialize)]
struct BodyUsage {
    usage: Option<UsageFields>,
}

#[derive(Deserialize)]
struct UsageFields {
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    total_tokens: Option<u64>,
    #[serde(flatten)]
    extra: serde_json::Map<String, Value>,
}

impl UsageFields {
    /// Extract the reported cost from the `extra` fields, if the field name is
    /// configured and the value is a finite, non-negative number.
    fn cost(&self, field: Option<&str>) -> Option<f64> {
        let name = field?;
        self.extra
            .get(name)?
            .as_f64()
            .filter(|v| v.is_finite() && *v >= 0.0)
    }

    fn into_usage(self) -> Option<Usage> {
        if self.prompt_tokens.is_none() && self.completion_tokens.is_none() {
            // Only a total: count it all as output so it is not lost.
            return self.total_tokens.map(|t| Usage {
                input: 0,
                output: t,
            });
        }
        Some(Usage {
            input: self.prompt_tokens.unwrap_or(0),
            output: self.completion_tokens.unwrap_or(0),
        })
    }
}

/// The `usage` object of a buffered (non-streaming) response body.
/// Returns both the parsed `Usage` and the reported cost (if any).
fn usage_from_body(body: &[u8], cost_field: Option<&str>) -> Option<(Usage, Option<f64>)> {
    let bu = serde_json::from_slice::<BodyUsage>(body).ok()?;
    let fields = bu.usage?;
    let cost = fields.cost(cost_field);
    let usage = fields.into_usage()?;
    Some((usage, cost))
}

/// Cheap pre-check before JSON parsing a chunk: does it contain
/// `"usage"` followed by `:` and an object (not `null`)?
fn mentions_usage_object(payload: &[u8]) -> bool {
    const KEY: &[u8] = b"\"usage\"";
    let mut from = 0;
    while let Some(i) = payload
        .get(from..)
        .and_then(|p| p.windows(KEY.len()).position(|w| w == KEY))
    {
        let after = from + i + KEY.len();
        let rest = payload.get(after..).unwrap_or_default().trim_ascii_start();
        if let Some(rest) = rest.strip_prefix(b":")
            && rest.trim_ascii_start().first() == Some(&b'{')
        {
            return true;
        }
        from = after;
    }
    false
}

/// `x-ratelimit-remaining-requests` / `-tokens` as whole numbers; anything
/// else (durations, floats, junk) is ignored.
/// Cooldown hint from the headers of a 429: `Retry-After`, else the
/// `x-ratelimit-reset-*` pair.
fn header_hint(headers: &HeaderMap) -> Option<Duration> {
    let text = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    if let Some(d) = text(header::RETRY_AFTER.as_str())
        .and_then(|v| limiter::parse_retry_after(v, SystemTime::now()))
    {
        return Some(d);
    }
    let hints = remaining_hints(headers);
    limiter::reset_hint(
        text("x-ratelimit-reset-requests"),
        text("x-ratelimit-reset-tokens"),
        hints.requests,
        hints.tokens,
    )
    .map(limiter::clamp_hint)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TooLargeKind {
    /// The request exceeds a context or size limit: worth learning a ceiling.
    Size,
    /// Fails over like a size refusal but is not about size.
    Other,
}

/// Whether a 400/422 error body says the request is refused in a way another
/// route may serve, and which kind. Looks at the error body only, bounded,
/// case-insensitive.
fn too_large_kind(body: &[u8]) -> Option<TooLargeKind> {
    const SIZE: &[&str] = &[
        "context_length_exceeded",
        "maximum context length",
        "context length",
        "context_length",
        "too many tokens",
        "reduce the length",
        "request too large",
    ];
    // Request shapes a particular provider cannot accept, while another route can:
    // - Gemini rejects history whose tool calls came from another model (no thought_signature).
    // - Cloudflare Workers AI rejects array-form message content
    //   ("Bad input: ... Type mismatch of '/messages/0/content', 'array' not in 'string'").
    const OTHER: &[&str] = &[
        "thought_signature",
        "type mismatch of '/messages",
        "bad input: error: oneof",
    ];
    let head = body.get(..body.len().min(16 * 1024)).unwrap_or_default();
    let text = String::from_utf8_lossy(head).to_ascii_lowercase();
    let any = |list: &[&str]| list.iter().any(|p| text.contains(p));
    if any(SIZE) {
        Some(TooLargeKind::Size)
    } else if any(OTHER) {
        Some(TooLargeKind::Other)
    } else {
        None
    }
}

/// Remember that `model` refused a request of `estimate` tokens.
fn learn_ceiling(state: &AppState, provider: &Provider, model: &str, estimate: u64) {
    if let Some(c) = state.limiter.record_ceiling(&provider.id, model, estimate) {
        crate::elog!(
            "warn: size ceiling learned provider={} model={model} ceiling={c} tokens (estimate)",
            provider.id
        );
    }
}

fn remaining_hints(headers: &HeaderMap) -> RemainingHints {
    let get = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
    };
    RemainingHints {
        requests: get("x-ratelimit-remaining-requests"),
        tokens: get("x-ratelimit-remaining-tokens"),
    }
}

/// Ask the upstream for a usage chunk on streaming requests. Returns whether
/// the router added `include_usage` (so the client did not ask for it and the
/// usage-only chunk must be hidden from it). A `stream_options` that is not
/// an object is left alone.
fn inject_usage_option(fields: &mut Map<String, Value>, wants_stream: bool) -> Injection {
    if !wants_stream {
        return Injection { injected: false };
    }
    let injected = match fields.get_mut("stream_options") {
        None | Some(Value::Null) => {
            fields.insert("stream_options".to_string(), json!({"include_usage": true}));
            true
        }
        Some(Value::Object(o)) => {
            let asked = o.get("include_usage") == Some(&Value::Bool(true));
            if !asked {
                o.insert("include_usage".to_string(), Value::Bool(true));
            }
            !asked
        }
        Some(_) => false,
    };
    Injection { injected }
}

#[derive(Debug, Clone, Copy)]
struct Injection {
    injected: bool,
}

/// `(prompt tokens, pre-flight estimate)`: characters in `messages` and
/// `tools` divided by 4, and that plus `max_tokens` /
/// `max_completion_tokens` (1024 when neither is set).
fn estimate_tokens(fields: &Map<String, Value>) -> (u64, u64) {
    fn chars(v: &Value) -> u64 {
        match v {
            Value::String(s) => s.chars().count() as u64,
            Value::Array(a) => a.iter().map(chars).sum(),
            Value::Object(o) => o.values().map(chars).sum(),
            _ => 0,
        }
    }
    let prompt: u64 = ["messages", "tools"]
        .iter()
        .filter_map(|k| fields.get(*k))
        .map(chars)
        .sum::<u64>()
        / 4;
    let output = ["max_tokens", "max_completion_tokens"]
        .iter()
        .find_map(|k| fields.get(*k).and_then(Value::as_u64))
        .unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS);
    (prompt, prompt.saturating_add(output))
}

/// `x-router-private: 1` (or `true`) excludes providers that train on input,
/// `0` (or `false`) opts out of `private_default`.
fn is_private(headers: &HeaderMap, default: bool) -> bool {
    match headers
        .get(HEADER_PRIVATE)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
    {
        Some("1") => true,
        Some("0") => false,
        Some(v) if v.eq_ignore_ascii_case("true") => true,
        Some(v) if v.eq_ignore_ascii_case("false") => false,
        _ => default,
    }
}

/// Gate an explicit paid route: spending must be enabled, the model must have
/// a configured price (never spend at an unknown price) and today's spend must
/// be under the cap.
fn check_paid(state: &AppState, provider: &Provider, model: &str) -> Result<(), ApiError> {
    let deny = |code: &'static str, message: String| {
        ApiError::new(StatusCode::FORBIDDEN, "permission_error", code, message).with_param("model")
    };
    let cap = state.config.paid_daily_cap_usd;
    if cap <= 0.0 {
        return Err(deny(
            "paid_disabled",
            format!(
                "provider \"{}\" is paid and paid_daily_cap_usd is 0, which blocks paid routes",
                provider.id
            ),
        ));
    }
    if !provider.prices.contains_key(model) {
        return Err(deny(
            "paid_price_missing",
            format!(
                "provider \"{}\" has no [provider.prices] entry for this model; refusing to spend at an unknown price",
                provider.id
            ),
        ));
    }
    let spent = state.limiter.spend_today(Instant::now());
    if spent >= cap {
        return Err(deny(
            "paid_cap_reached",
            format!(
                "today's paid spend ${spent:.4} has reached the cap of ${cap:.2} (resets at 00:00 UTC)"
            ),
        ));
    }
    Ok(())
}

/// 503 listing every skipped route with its reason. `Retry-After` is the
/// time until the earliest cooldown or breaker expiry, when there is one.
/// Hand the skipped routes to the request log line.
fn note_skipped(track: &ReqTrack, skipped: &[Skipped]) {
    track.skipped(
        skipped
            .iter()
            .map(|s| format!("{}:{}", s.route, s.reason.name())),
    );
}

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
/// (the client's own key never reaches a provider; a keyless provider gets no
/// `Authorization` at all), plus `content-type` and
/// the client's `accept`. Nothing else from the client is forwarded.
fn upstream_request_headers(
    key: &ApiKey,
    provider_id: &str,
    client_headers: &HeaderMap,
) -> Result<HeaderMap, ApiError> {
    let mut headers = HeaderMap::new();
    if !key.is_anonymous() {
        let mut auth =
            HeaderValue::from_str(&format!("Bearer {}", key.expose())).map_err(|_| {
                internal(format!(
                    "API key for provider \"{}\" contains characters not allowed in an HTTP header",
                    provider_id
                ))
            })?;
        auth.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, auth);
    }
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
    fn session_key_prefers_header_then_derives_from_first_messages() {
        let names = vec!["x-session-id".to_string(), "x-opencode-session".to_string()];
        let none = HeaderMap::new();
        let fields = |v: Value| v.as_object().cloned().unwrap_or_default();
        let base = fields(json!({"messages": [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "hello"}
        ]}));
        let key = |h: &HeaderMap, f: &Map<String, Value>| session_key(h, &names, "coder", f);

        let a = key(&none, &base);
        assert!(a.is_some());
        assert_eq!(a, key(&none, &base.clone()));

        let other = fields(json!({"messages": [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "goodbye"}
        ]}));
        assert_ne!(a, key(&none, &other));

        let longer = fields(json!({"messages": [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi"},
            {"role": "user", "content": "more"}
        ]}));
        assert_eq!(a, key(&none, &longer));

        let mut with = HeaderMap::new();
        with.insert("x-opencode-session", HeaderValue::from_static("s1"));
        let h = key(&with, &base);
        assert!(h.is_some());
        assert_ne!(h, a);
        assert_eq!(h, key(&with, &other), "the header overrides the messages");

        let no_user = fields(json!({"messages": [{"role": "system", "content": "x"}]}));
        assert_eq!(key(&none, &no_user), None);
        assert_eq!(key(&none, &fields(json!({}))), None);
    }

    #[test]
    fn error_excerpt_is_bounded_single_line_and_redacts_keys() {
        let keys = [ApiKey::new("sk-test-secret-12345678"), ApiKey::anonymous()];
        let long = "x".repeat(2000);
        let e = error_excerpt(long.as_bytes(), &keys);
        assert!(e.chars().count() <= 301);
        assert!(e.ends_with('…'));
        assert_eq!(error_excerpt(b"a\nb\r\n\tc", &keys), "a b c");
        let e = error_excerpt(b"{\"error\":\"bad key sk-test-secret-12345678\"}", &keys);
        assert!(!e.contains("sk-test-secret-12345678"));
        assert!(e.contains("[redacted]"));
        assert_eq!(e, "{'error':'bad key [redacted]'}");
        assert_eq!(error_excerpt(b"", &keys), "");
    }

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

    fn tap(filter: bool) -> Tap {
        let text = "[[provider]]
id = \"p\"
base_url = \"http://p/v1\"
keys_env = \"K\"
billing = \"free\"
";
        let config = crate::config::Config::from_toml_str(text, &|_| Some("k".into())).unwrap();
        let state = AppState::new(config).unwrap();
        let provider = state.config.provider("p").unwrap();
        let entry = Entry {
            provider,
            key_idx: 0,
            model: "m",
        };
        let track = state.events.begin("m", true);
        let settle = Settle::new(
            &state,
            &entry,
            0,
            RemainingHints::default(),
            &track,
            StatusCode::OK,
        );
        Tap::new(settle, filter)
    }

    const CONTENT: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}],\"usage\":null}

";
    const USAGE: &str = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":5,\"total_tokens\":16}}

";
    const DONE: &str = "data: [DONE]

";

    /// Feed `stream` split at `cut` and return (forwarded bytes, usage).
    fn run(stream: &str, cut: usize, filter: bool) -> (Vec<u8>, Option<Usage>) {
        let mut t = tap(filter);
        let (a, b) = stream.as_bytes().split_at(cut);
        let mut out = Vec::new();
        for part in [a, b] {
            if let Some(o) = t.push(Bytes::copy_from_slice(part)) {
                out.extend_from_slice(&o);
            }
        }
        if let Some(o) = t.finish() {
            out.extend_from_slice(&o);
        }
        (out, t.settle.usage)
    }

    #[test]
    fn tap_captures_usage_across_any_chunk_split_and_forwards_verbatim_when_asked() {
        let stream = format!("{CONTENT}{USAGE}{DONE}");
        let want = Usage {
            input: 11,
            output: 5,
        };
        for cut in 0..=stream.len() {
            let (out, usage) = run(&stream, cut, false);
            assert_eq!(out, stream.as_bytes(), "cut {cut}");
            assert_eq!(usage, Some(want), "cut {cut}");
        }
    }

    #[test]
    fn tap_hides_the_usage_only_chunk_from_clients_that_did_not_ask() {
        let stream = format!("{CONTENT}{USAGE}{DONE}");
        let expected = format!("{CONTENT}{DONE}");
        for cut in 0..=stream.len() {
            let (out, usage) = run(&stream, cut, true);
            assert_eq!(String::from_utf8_lossy(&out), expected, "cut {cut}");
            assert_eq!(usage.map(|u| u.total()), Some(16), "cut {cut}");
        }
        // CRLF framing and a usage object inside a chunk that has choices.
        let with_choices = "data: {\"choices\":[{}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2}}\r\n\r\n";
        let (out, usage) = run(with_choices, 7, true);
        assert_eq!(out, with_choices.as_bytes());
        assert_eq!(usage.map(|u| u.total()), Some(3));
        let crlf = USAGE.replace('\n', "\r\n");
        let (out, _) = run(&crlf, 3, true);
        assert!(out.is_empty(), "{}", String::from_utf8_lossy(&out));
    }

    #[test]
    fn usage_helpers() {
        assert!(mentions_usage_object(b"{\"usage\": {\"a\":1}}"));
        assert!(!mentions_usage_object(b"{\"usage\":null}"));
        assert!(!mentions_usage_object(b"{\"x\":1}"));
        let body = br#"{"id":"x","usage":{"prompt_tokens":3,"completion_tokens":4}}"#;
        assert_eq!(
            usage_from_body(body, None),
            Some((
                Usage {
                    input: 3,
                    output: 4
                },
                None
            ))
        );
        // With cost field
        let body_with_cost =
            br#"{"id":"x","usage":{"prompt_tokens":3,"completion_tokens":4,"cost":0.0123}}"#;
        assert_eq!(
            usage_from_body(body_with_cost, Some("cost")),
            Some((
                Usage {
                    input: 3,
                    output: 4
                },
                Some(0.0123)
            ))
        );
        // cost field missing -> no cost
        assert_eq!(
            usage_from_body(body_with_cost, None),
            Some((
                Usage {
                    input: 3,
                    output: 4
                },
                None
            ))
        );
        // cost field not a number -> no cost
        let body_bad_cost =
            br#"{"id":"x","usage":{"prompt_tokens":3,"completion_tokens":4,"cost":"x"}}"#;
        assert_eq!(
            usage_from_body(body_bad_cost, Some("cost")),
            Some((
                Usage {
                    input: 3,
                    output: 4
                },
                None
            ))
        );
        assert_eq!(usage_from_body(b"{}", None), None);
        assert_eq!(usage_from_body(b"nope", None), None);
    }

    #[test]
    fn estimate_and_private_header() {
        let v: Value = json!({"messages": [{"role": "user", "content": "x".repeat(400)}], "max_completion_tokens": 50});
        let f = v.as_object().cloned().unwrap_or_default();
        let (prompt, est) = estimate_tokens(&f);
        assert_eq!((prompt, est), (101, 151)); // 404 chars / 4, role counted
        let none = Map::new();
        assert_eq!(estimate_tokens(&none), (0, 1024));

        let mut h = HeaderMap::new();
        assert!(!is_private(&h, false));
        assert!(is_private(&h, true));
        h.insert(HEADER_PRIVATE, HeaderValue::from_static("1"));
        assert!(is_private(&h, false));
        h.insert(HEADER_PRIVATE, HeaderValue::from_static("0"));
        assert!(!is_private(&h, true));
    }

    #[test]
    fn stream_options_injection() {
        let mut f = Map::new();
        assert!(inject_usage_option(&mut f, true).injected);
        assert_eq!(f["stream_options"], json!({"include_usage": true}));
        let mut f = json!({"stream_options": {"include_usage": true, "x": 1}})
            .as_object()
            .cloned()
            .unwrap_or_default();
        assert!(!inject_usage_option(&mut f, true).injected);
        let mut f = json!({"stream_options": {"x": 1}})
            .as_object()
            .cloned()
            .unwrap_or_default();
        assert!(inject_usage_option(&mut f, true).injected);
        assert_eq!(f["stream_options"], json!({"x": 1, "include_usage": true}));
        let mut f = Map::new();
        assert!(!inject_usage_option(&mut f, false).injected);
        assert!(f.is_empty());
    }
}
