//! sluice: a local OpenAI-compatible LLM router.
//!
//! The binary in `main.rs` is a thin wrapper; everything lives here so
//! integration tests can run the server in-process.

pub mod billing;
pub mod catalog;
pub mod config;
pub mod discover;
pub mod error;
pub mod events;
pub mod limiter;
pub mod logsink;
pub mod proxy;
pub mod select;
pub mod state;
pub mod status;
pub mod sticky;

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{Request, State};
use axum::http::header;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use tokio::net::TcpListener;

use crate::config::Config;
use crate::error::ApiError;

/// Shared application state, held behind one `Arc`.
#[derive(Debug)]
pub struct AppState {
    pub config: Config,
    /// One pooled HTTP client shared by all upstream calls.
    pub http: reqwest::Client,
    /// Cooldowns, breakers, disabled keys and missing routes.
    pub limiter: limiter::Limiter,
    /// Event channel, recent-request ring buffer and request log.
    pub events: Arc<events::Events>,
    /// When the process started, for the uptime shown by `/status`.
    pub started: Instant,
    /// Session -> route stickiness for pool requests.
    pub sticky: sticky::Sticky,
}

impl AppState {
    /// Fails only if the upstream HTTP client cannot be built (startup error).
    pub fn new(config: Config) -> Result<Arc<Self>, String> {
        let http = proxy::build_client()?;
        let limiter = limiter::Limiter::new();
        let now = Instant::now();
        // Config seeds of credit already spent this period; a floor, so the
        // order relative to loading the state file does not matter.
        for p in &config.providers {
            if p.credit.as_ref().is_some_and(|c| c.seed.is_some()) && !limiter.seed_credit(p, now) {
                crate::elog!(
                    "warn: credit_spent_date for provider={} is from an earlier credit period, ignored",
                    p.id
                );
            }
        }
        let sticky = sticky::Sticky::new(Duration::from_secs(config.sticky_ttl_secs));
        Ok(Arc::new(Self {
            config,
            http,
            limiter,
            events: events::Events::new(),
            started: now,
            sticky,
        }))
    }
}

/// Build the HTTP application. When the config requires client auth
/// (non-loopback bind), every path, including `/health` and unknown paths,
/// is behind the bearer check.
pub fn app(state: Arc<AppState>) -> Router {
    let router = Router::new()
        .merge(status::router())
        .merge(proxy::router())
        .fallback(not_found);
    let router = if state.config.client_auth.is_some() {
        router.layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer,
        ))
    } else {
        router
    };
    router.with_state(state)
}

/// Serve `app(state)` on `listener` until `shutdown` completes, then drain
/// in-flight connections.
pub async fn serve<F>(
    listener: TcpListener,
    state: Arc<AppState>,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    // One shared ticker for the `counters` event; idle without subscribers.
    let ticker = tokio::spawn(status::ticker(state.clone()));
    let events = state.events.clone();
    let result = axum::serve(listener, app(state))
        .with_graceful_shutdown(async move {
            shutdown.await;
            // `/events` streams never end by themselves.
            events.shutdown();
        })
        .await;
    ticker.abort();
    result
}

async fn not_found() -> ApiError {
    ApiError::not_found("unknown path")
}

async fn require_bearer(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let Some(expected) = state.config.client_auth.as_ref() else {
        return next.run(request).await;
    };
    let presented = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(bearer_token);
    match presented {
        Some(token) if constant_time_eq(token.as_bytes(), expected.expose().as_bytes()) => {
            next.run(request).await
        }
        _ => ApiError::unauthorized().into_response(),
    }
}

/// Extract the token from `Bearer <token>` (scheme is case-insensitive).
fn bearer_token(value: &str) -> Option<&str> {
    let (scheme, token) = value.trim().split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

/// Comparison whose duration does not depend on where the inputs differ.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_parsing() {
        assert_eq!(bearer_token("Bearer abc"), Some("abc"));
        assert_eq!(bearer_token("bearer   abc "), Some("abc"));
        assert_eq!(bearer_token("Basic abc"), None);
        assert_eq!(bearer_token("Bearer "), None);
        assert_eq!(bearer_token("abc"), None);
    }

    #[test]
    fn constant_time_eq_works() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }
}
