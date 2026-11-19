//! Operational endpoints: `GET /health`, `GET /status` (JSON snapshot),
//! `GET /events` (server-sent events), `GET /v1/models` and the admin
//! endpoints. Nothing here returns key material (only the last four
//! characters of a key) or message text.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use axum::extract::{Path, State};
use axum::http::{Uri, header};
use axum::response::IntoResponse;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::Stream;
use serde_json::{Map, Value, json};

use crate::AppState;
use crate::config::{LimitScope, Limits, Provider};
use crate::error::ApiError;
use crate::limiter::{
    CREDIT_GUARD_FRACTION, CreditStatus, HOURLY_BUCKETS, ProviderView, ScopeKey, unix_secs,
};

/// Interval of the shared `counters` event.
const COUNTERS_INTERVAL: Duration = Duration::from_secs(2);
/// SSE comment keep-alive interval.
const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// The dashboard page, embedded at compile time.
pub const DASHBOARD_HTML: &str = include_str!("../ui/index.html");

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(dashboard))
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/events", get(events))
        .route("/v1/models", get(models))
        .route("/admin/provider/{id}/pause", post(pause))
        .route("/admin/provider/{id}/resume", post(resume))
        .route("/admin/cooldowns/clear", post(clear_cooldowns))
        .route("/admin/ceilings/clear", post(clear_ceilings))
        .route("/admin/provider/{id}/payment/clear", post(clear_payment))
        .route("/admin/provider/{id}/cooldown", post(set_cooldown))
}

async fn dashboard() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        DASHBOARD_HTML,
    )
}

/// Returns 200 whenever the process is up.
async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

// ---------------------------------------------------------------------------
// /v1/models
// ---------------------------------------------------------------------------

/// Every pool name plus every allowlisted `provider/model` (free and paid).
/// Providers without an allowlist contribute nothing.
async fn models(State(state): State<Arc<AppState>>) -> Json<Value> {
    let config = &state.config;
    let mut ids: Vec<(String, String)> = config
        .pools
        .iter()
        .map(|p| (p.name.clone(), "sluice".to_string()))
        .collect();
    for p in &config.providers {
        for m in p.models.iter().flatten() {
            ids.push((format!("{}/{m}", p.id), p.id.clone()));
        }
    }
    let mut seen = std::collections::HashSet::new();
    let data: Vec<Value> = ids
        .into_iter()
        .filter(|(id, _)| seen.insert(id.clone()))
        .map(|(id, owner)| json!({"id": id, "object": "model", "created": 0, "owned_by": owner}))
        .collect();
    Json(json!({"object": "list", "data": data}))
}

// ---------------------------------------------------------------------------
// Admin
// ---------------------------------------------------------------------------

fn known_provider<'a>(state: &'a AppState, id: &str) -> Result<&'a Provider, ApiError> {
    state
        .config
        .provider(id)
        .ok_or_else(|| ApiError::not_found(format!("unknown provider \"{id}\"")))
}

async fn pause(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    set_paused(&state, &id, true)
}

async fn resume(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    set_paused(&state, &id, false)
}

fn set_paused(state: &AppState, id: &str, paused: bool) -> Result<Json<Value>, ApiError> {
    let provider = known_provider(state, id)?;
    state.limiter.set_paused(&provider.id, paused);
    let name = if paused { "paused" } else { "resumed" };
    eprintln!("warn: provider {} {name} by admin", provider.id);
    state
        .events
        .route_state(&provider.id, None, None, name, None);
    Ok(Json(json!({"provider": provider.id, "paused": paused})))
}

/// `POST /admin/provider/{id}/cooldown?secs=N`: cool the whole provider for
/// N seconds (for example when its free allowance is nearly spent). Saved to
/// the state file at once, so it survives a restart. Lifted early by
/// `POST /admin/cooldowns/clear?provider=id`.
async fn set_cooldown(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    uri: Uri,
) -> Result<Json<Value>, ApiError> {
    let provider = known_provider(&state, &id)?;
    let secs = uri
        .query()
        .and_then(|q| q.split('&').find_map(|pair| pair.strip_prefix("secs=")))
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|s| *s > 0)
        .ok_or_else(|| {
            ApiError::bad_request("invalid_secs", "secs must be a positive whole number")
        })?;
    let len = Duration::from_secs(secs);
    let now = Instant::now();
    let until = state.limiter.set_provider_cooldown(&provider.id, len, now);
    let remaining = until.saturating_duration_since(now);
    crate::state::save_logged(&state);
    crate::elog!(
        "warn: provider {} cooled by admin for {}s",
        provider.id,
        remaining.as_secs()
    );
    state
        .events
        .route_state(&provider.id, None, None, "cooldown", Some(remaining));
    Ok(Json(
        json!({"provider": provider.id, "cooldown_secs": remaining.as_secs()}),
    ))
}

/// `POST /admin/provider/{id}/payment/clear`: lift a 402 block.
async fn clear_payment(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let provider = known_provider(&state, &id)?;
    let cleared = state.limiter.clear_payment(&provider.id);
    crate::elog!(
        "warn: provider {} payment block cleared by admin",
        provider.id
    );
    state
        .events
        .route_state(&provider.id, None, None, "cleared", None);
    Ok(Json(json!({"provider": provider.id, "cleared": cleared})))
}

/// The validated `?provider=id` of an admin request.
fn provider_param<'a>(state: &'a AppState, uri: &Uri) -> Result<Option<&'a str>, ApiError> {
    let wanted = uri.query().and_then(|q| {
        q.split('&')
            .find_map(|pair| pair.strip_prefix("provider="))
            .filter(|v| !v.is_empty())
    });
    match wanted {
        Some(id) => Ok(Some(known_provider(state, id)?.id.as_str())),
        None => Ok(None),
    }
}

fn announce_cleared(state: &AppState, provider: Option<&str>) {
    match provider {
        Some(id) => state.events.route_state(id, None, None, "cleared", None),
        None => {
            for p in &state.config.providers {
                state.events.route_state(&p.id, None, None, "cleared", None);
            }
        }
    }
}

/// `POST /admin/cooldowns/clear`, optionally `?provider=id`. Also forgets
/// learned size ceilings.
async fn clear_cooldowns(
    State(state): State<Arc<AppState>>,
    uri: Uri,
) -> Result<Json<Value>, ApiError> {
    let provider = provider_param(&state, &uri)?;
    let cleared = state.limiter.clear_cooldowns(provider);
    let ceilings = state.limiter.clear_ceilings(provider);
    announce_cleared(&state, provider);
    Ok(Json(
        json!({"cleared": cleared, "ceilings_cleared": ceilings, "provider": provider}),
    ))
}

/// `POST /admin/ceilings/clear`, optionally `?provider=id`.
async fn clear_ceilings(
    State(state): State<Arc<AppState>>,
    uri: Uri,
) -> Result<Json<Value>, ApiError> {
    let provider = provider_param(&state, &uri)?;
    let cleared = state.limiter.clear_ceilings(provider);
    announce_cleared(&state, provider);
    Ok(Json(json!({"cleared": cleared, "provider": provider})))
}

// ---------------------------------------------------------------------------
// /events
// ---------------------------------------------------------------------------

/// SSE stream of the broadcast channel. A subscriber that lags past the
/// channel capacity has its stream ended, so the browser reconnects.
async fn events(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    let rx = state.events.subscribe();
    let stop = state.events.shutdown_signal();
    let stream = futures_util::stream::unfold((rx, stop), |(mut rx, mut stop)| async move {
        let next = tokio::select! {
            received = rx.recv() => received.ok(),
            _ = stop.wait_for(|down| *down) => None,
        };
        let ev = next?;
        let out = SseEvent::default().event(ev.name).data(&*ev.data);
        Some((Ok(out), (rx, stop)))
    });
    Sse::new(stream).keep_alive(KeepAlive::new().interval(KEEP_ALIVE))
}

/// One shared task that publishes `counters` every 2 s, and only while
/// somebody is subscribed.
pub async fn ticker(state: Arc<AppState>) {
    let mut tick = tokio::time::interval(COUNTERS_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        state.events.publish("counters", || snapshot(&state, false));
    }
}

// ---------------------------------------------------------------------------
// /status
// ---------------------------------------------------------------------------

async fn status(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(snapshot(&state, true))
}

fn limits_json(l: &Limits) -> Value {
    json!({"rpm": l.rpm, "rpd": l.rpd, "tpm": l.tpm, "tpd": l.tpd, "tpmo": l.tpmo})
}

fn scope_name(s: LimitScope) -> &'static str {
    match s {
        LimitScope::Provider => "provider",
        LimitScope::Key => "key",
        LimitScope::KeyModel => "key_model",
    }
}

fn expires_ms(at: Instant, now: Instant) -> u64 {
    u64::try_from(at.saturating_duration_since(now).as_millis()).unwrap_or(u64::MAX)
}

/// Overall provider state and when it ends, first match wins: `paused`,
/// `payment_required` (402 until cleared), `credit_exhausted` (until the
/// credit reset), `key_disabled` (every key), `breaker_open`, `cooling` (a
/// provider-wide cooldown, or every key cooling) or `ok`.
fn provider_state(
    p: &Provider,
    v: &ProviderView,
    credit_until: Option<Instant>,
    now: Instant,
) -> (&'static str, Option<Instant>) {
    if v.paused {
        return ("paused", None);
    }
    if v.payment_required {
        return ("payment_required", None);
    }
    if let Some(until) = credit_until {
        return ("credit_exhausted", Some(until));
    }
    if !p.keys.is_empty() && v.disabled_keys.len() >= p.keys.len() {
        return ("key_disabled", None);
    }
    if let Some(until) = v.breaker_until.filter(|u| *u > now) {
        return ("breaker_open", Some(until));
    }
    let wide = v
        .cooldowns
        .iter()
        .filter(|(k, _)| k.key.is_none())
        .map(|(_, u)| *u)
        .max();
    let per_key: Option<Instant> = (0..p.keys.len())
        .map(|i| {
            let own = v
                .cooldowns
                .iter()
                .filter(|(k, _)| k.key == Some(i) && k.model.is_none())
                .map(|(_, u)| *u)
                .max();
            match (own, wide) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            }
        })
        .collect::<Option<Vec<Instant>>>()
        .and_then(|all| all.into_iter().min());
    match per_key.or(wide) {
        Some(until) => ("cooling", Some(until)),
        None => ("ok", None),
    }
}

fn cooldown_json(scope: &ScopeKey, until: Instant, now: Instant, p: &Provider) -> Value {
    let kind = match (scope.key, scope.model.as_ref()) {
        (None, _) => "provider",
        (Some(_), None) => "key",
        (Some(_), Some(_)) => "route",
    };
    json!({
        "scope": kind,
        "key": scope.key.and_then(|i| p.keys.get(i)).map(|k| k.redacted()),
        "model": scope.model,
        "expires_in_ms": expires_ms(until, now),
    })
}

/// Dense last-24-hours token history, oldest first.
fn hourly_json(state: &AppState, id: &str, now: Instant, unix: u64) -> Vec<Value> {
    let sparse: BTreeMap<u64, (u64, u64)> = state
        .limiter
        .hourly(id, now)
        .into_iter()
        .map(|(h, i, o)| (h, (i, o)))
        .collect();
    let cur = unix / 3600;
    (cur + 1 - HOURLY_BUCKETS..=cur)
        .map(|h| {
            let (i, o) = sparse.get(&h).copied().unwrap_or((0, 0));
            json!({"hour": h * 3600, "tokens_in": i, "tokens_out": o})
        })
        .collect()
}

/// Provider fields shared by `/status` and `counters`. With `detail` the
/// per-model rows, cooldown list and hourly history are added.
fn provider_json(state: &AppState, p: &Provider, now: Instant, unix: u64, detail: bool) -> Value {
    let view = state.limiter.provider_view(p, now);
    let credit = state.limiter.credit_status(p, now);
    // The credit reset as an `Instant`, when the guard has been reached.
    let credit_until = credit
        .filter(|c| c.exhausted)
        .map(|c| now + Duration::from_secs(c.resets_at.saturating_sub(unix)));
    let (name, until) = provider_state(p, &view, credit_until, now);
    let ev = state.events.provider_events(&p.id);
    let day_start_hour = unix / 86_400 * 24;
    let hourly = state.limiter.hourly(&p.id, now);
    let (tin, tout) = hourly
        .iter()
        .filter(|(h, _, _)| *h >= day_start_hour)
        .fold((0u64, 0u64), |(a, b), (_, i, o)| {
            (a.saturating_add(*i), b.saturating_add(*o))
        });
    let mut out = json!({
        "id": p.id,
        "paid": p.paid,
        "billing": p.billing.name(),
        "state": name,
        "expires_in_ms": until.map(|u| expires_ms(u, now)),
        "requests_minute": view.requests_minute,
        "requests_day": view.requests_day,
        "tokens_minute": view.tokens_minute,
        "tokens_day": view.tokens_day,
        "tokens_month": view.tokens_month,
        "tokens_in_today": tin,
        "tokens_out_today": tout,
        "payment_required": view.payment_required,
        "spend_today_usd": state.limiter.provider_spend_today(&p.id, now),
        "credit": credit.map(credit_json),
        "ttft_ms": {"median": ev.ttft.median, "p95": ev.ttft.p95, "samples": ev.ttft.samples},
        "last_error": ev.last_error.as_ref().map(|e| e.to_json()),
    });
    if !detail {
        return out;
    }
    let mut model_names: Vec<String> = p.models.iter().flatten().cloned().collect();
    let pool_routes = state
        .config
        .pools
        .iter()
        .flat_map(|pool| pool.routes.iter())
        .filter(|r| r.provider == p.id)
        .map(|r| r.model.clone());
    let seen = ev.models.iter().map(|(m, _)| m.clone());
    for m in pool_routes.chain(seen) {
        if !model_names.contains(&m) {
            model_names.push(m);
        }
    }
    let models: Vec<Value> = model_names
        .iter()
        .map(|m| {
            let stat = ev.models.iter().find(|(n, _)| n == m).map(|(_, s)| s);
            let route_cooldown = view
                .cooldowns
                .iter()
                .filter(|(k, _)| k.model.as_deref() == Some(m.as_str()))
                .map(|(_, u)| *u)
                .max();
            let route_breaker = view
                .route_breakers
                .iter()
                .find(|(n, _)| n == m)
                .map(|(_, u)| *u);
            let (state_name, until) = if view.missing.contains(m) {
                ("missing", None)
            } else if view.unavailable.contains(m) {
                ("unavailable", None)
            } else if let Some(u) = route_breaker {
                ("breaker_open", Some(u))
            } else if let Some(u) = route_cooldown {
                ("cooling", Some(u))
            } else {
                ("ok", None)
            };
            let learned = view.ceilings.iter().find(|(n, _)| n == m).map(|(_, c)| *c);
            let ceiling = crate::limiter::effective_ceiling(p, m, learned);
            json!({
                "model": m,
                "state": state_name,
                "ceiling_tokens": ceiling.map(|(c, _)| c),
                "ceiling_source": ceiling.map(|(_, s)| s.name()),
                "expires_in_ms": until.map(|u| expires_ms(u, now)),
                "requests_today": stat.map_or(0, |s| s.requests),
                "tokens_in_today": stat.map_or(0, |s| s.tokens_in),
                "tokens_out_today": stat.map_or(0, |s| s.tokens_out),
                "limits": p.model_limits.get(m).map(limits_json),
                "last_error": stat.and_then(|s| s.last_error.as_ref()).map(|e| e.to_json()),
            })
        })
        .collect();
    if let Some(o) = out.as_object_mut() {
        o.insert("trains_on_input".to_string(), json!(p.trains_on_input));
        o.insert("keys".to_string(), json!(p.keys.len()));
        o.insert(
            "disabled_keys".to_string(),
            json!(
                view.disabled_keys
                    .iter()
                    .filter_map(|i| p.keys.get(*i))
                    .map(|k| k.redacted())
                    .collect::<Vec<_>>()
            ),
        );
        o.insert("missing_models".to_string(), json!(view.missing));
        o.insert("unavailable_models".to_string(), json!(view.unavailable));
        o.insert(
            "cooldowns".to_string(),
            Value::Array(
                view.cooldowns
                    .iter()
                    .map(|(k, u)| cooldown_json(k, *u, now, p))
                    .collect(),
            ),
        );
        o.insert(
            "limits".to_string(),
            json!({
                "scope": scope_name(p.limits.scope),
                "rpm": p.limits.limits.rpm, "rpd": p.limits.limits.rpd,
                "tpm": p.limits.limits.tpm, "tpd": p.limits.limits.tpd,
                "tpmo": p.limits.limits.tpmo,
            }),
        );
        o.insert("models".to_string(), Value::Array(models));
        o.insert(
            "hourly".to_string(),
            Value::Array(hourly_json(state, &p.id, now, unix)),
        );
    }
    out
}

/// A credit provider's period: amounts in USD, times in Unix ms.
fn credit_json(c: CreditStatus) -> Value {
    json!({
        "usd": c.credit_usd,
        "spent_usd": c.spent_usd,
        "guard_usd": c.credit_usd * CREDIT_GUARD_FRACTION,
        "period_start_ms": c.period_start.saturating_mul(1000),
        "resets_at_ms": c.resets_at.saturating_mul(1000),
        "exhausted": c.exhausted,
    })
}

/// The `/status` body (`detail`) and the `counters` event payload (without
/// per-model rows, cooldown list, hourly history and recent requests).
fn snapshot(state: &AppState, detail: bool) -> Value {
    let now = Instant::now();
    let unix = unix_secs(SystemTime::now());
    let providers: Vec<Value> = state
        .config
        .providers
        .iter()
        .map(|p| provider_json(state, p, now, unix, detail))
        .collect();
    let sum = |field: &str| {
        providers
            .iter()
            .filter_map(|p| p.get(field).and_then(Value::as_u64))
            .fold(0u64, u64::saturating_add)
    };
    let (recent, active, active_streams) = state.events.recent();
    let mut out = Map::new();
    let mut put = |k: &str, v: Value| {
        out.insert(k.to_string(), v);
    };
    put("uptime_secs", json!(state.started.elapsed().as_secs()));
    put("now_ms", json!(unix * 1000));
    put("requests_today", json!(sum("requests_day")));
    put("tokens_in_today", json!(sum("tokens_in_today")));
    put("tokens_out_today", json!(sum("tokens_out_today")));
    put("active_requests", json!(active));
    put("active_streams", json!(active_streams));
    put(
        "paid",
        json!({
            "spend_usd": state.limiter.spend_today(now),
            "cap_usd": state.config.paid_daily_cap_usd,
        }),
    );
    put(
        "spend_today_usd",
        json!(state.limiter.total_spend_today(now)),
    );
    put("sticky_sessions", json!(state.sticky.len(now)));
    put("providers", Value::Array(providers));
    if detail {
        put("version", json!(env!("CARGO_PKG_VERSION")));
        put(
            "pools",
            Value::Array(
                state
                    .config
                    .pools
                    .iter()
                    .map(|pool| {
                        json!({
                            "name": pool.name,
                            "routes": pool.routes.iter()
                                .map(|r| format!("{}/{}", r.provider, r.model))
                                .collect::<Vec<_>>(),
                        })
                    })
                    .collect::<Vec<_>>(),
            ),
        );
        put(
            "recent",
            Value::Array(recent.iter().map(|r| r.to_json()).collect()),
        );
    }
    Value::Object(out)
}
