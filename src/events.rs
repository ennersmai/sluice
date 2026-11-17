//! Live view of the router: a broadcast channel of typed events (feeding
//! `/events`), a ring buffer of the last 200 requests, per-provider time to
//! first token samples, last errors and per-model counters (all feeding
//! `/status`), and the one-line-per-request log on stdout.
//!
//! Nothing here ever holds a lock across an `.await`. With no subscriber
//! connected, publishing costs one atomic load (`receiver_count`): payloads
//! are built by closures that only run when somebody is listening. Payloads
//! carry ids, provider and model names, status codes, counts and timings,
//! plus the last four characters of a key; never prompt or completion text.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use serde_json::{Value, json};
use tokio::sync::{broadcast, watch};

use crate::config::{ApiKey, Provider};
use crate::limiter::{Usage, unix_secs};

/// Broadcast channel capacity. A subscriber that falls this far behind is
/// dropped (its SSE stream ends and the browser reconnects).
pub const CHANNEL_CAPACITY: usize = 256;
/// Most failed or skipped routes listed on a log line.
const MAX_LOG_LIST: usize = 10;
/// Requests kept for `/status`, in flight and finished.
pub const RING_CAPACITY: usize = 200;
/// Time to first token samples kept per provider.
pub const TTFT_SAMPLES: usize = 200;
/// Attempts kept in one request's chain.
const MAX_CHAIN: usize = 32;
/// Distinct `(provider, model)` pairs with counters. Bounds memory when a
/// provider has no allowlist and clients name arbitrary models.
const MAX_MODEL_STATS: usize = 512;
/// Longest requested model name stored or logged.
const MAX_MODEL_CHARS: usize = 200;
/// Longest error reason stored.
const MAX_REASON_CHARS: usize = 64;

/// One serialised event, cloned cheaply per subscriber.
#[derive(Debug, Clone)]
pub struct Event {
    pub name: &'static str,
    pub data: Arc<str>,
}

/// One attempt in a request's chain.
#[derive(Debug, Clone)]
pub struct AttemptRec {
    pub n: u32,
    pub provider: String,
    pub model: String,
    /// Redacted key (last four characters).
    pub key: String,
    /// `ok`, or the failure reason (`http_429`, `timeout`, `unreachable`, ...).
    pub outcome: String,
    pub ms: u64,
}

impl AttemptRec {
    fn to_json(&self) -> Value {
        json!({
            "n": self.n, "provider": self.provider, "model": self.model,
            "key": self.key, "outcome": self.outcome, "ms": self.ms,
        })
    }
}

/// A request in the ring buffer.
#[derive(Debug, Clone)]
pub struct ReqRecord {
    pub id: u64,
    pub time_ms: u64,
    pub requested: String,
    pub stream: bool,
    pub done: bool,
    pub attempts: Vec<AttemptRec>,
    pub provider: Option<String>,
    pub upstream_model: Option<String>,
    pub ttft_ms: Option<u64>,
    pub tokens_in: u64,
    pub tokens_out: u64,
    pub duration_ms: Option<u64>,
    pub status: Option<u16>,
    pub error: Option<&'static str>,
}

impl ReqRecord {
    pub fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "time_ms": self.time_ms,
            "model": self.requested,
            "stream": self.stream,
            "in_flight": !self.done,
            "attempts": self.attempts.iter().map(AttemptRec::to_json).collect::<Vec<_>>(),
            "provider": self.provider,
            "upstream_model": self.upstream_model,
            "ttft_ms": self.ttft_ms,
            "tokens_in": self.tokens_in,
            "tokens_out": self.tokens_out,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "error": self.error,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ErrorRec {
    pub reason: String,
    pub model: Option<String>,
    pub time_ms: u64,
}

impl ErrorRec {
    pub fn to_json(&self) -> Value {
        json!({"reason": self.reason, "model": self.model, "time_ms": self.time_ms})
    }
}

/// Counters for one `(provider, model)`, valid for UTC day `day`.
#[derive(Debug, Clone, Default)]
pub struct ModelStat {
    pub day: u64,
    pub requests: u64,
    pub tokens_in: u64,
    pub tokens_out: u64,
    pub last_error: Option<ErrorRec>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TtftStats {
    pub median: Option<u64>,
    pub p95: Option<u64>,
    pub samples: usize,
}

/// Everything `/status` needs from the event side for one provider.
#[derive(Debug, Default)]
pub struct ProviderEvents {
    pub ttft: TtftStats,
    pub last_error: Option<ErrorRec>,
    /// Models seen, with today's counters (zeros for an earlier day).
    pub models: Vec<(String, ModelStat)>,
}

#[derive(Debug, Default)]
struct Inner {
    ring: VecDeque<ReqRecord>,
    ttft: HashMap<String, VecDeque<u64>>,
    last_error: HashMap<String, ErrorRec>,
    models: HashMap<(String, String), ModelStat>,
    active: u64,
    active_streams: u64,
}

#[derive(Debug)]
pub struct Events {
    tx: broadcast::Sender<Event>,
    shutdown: watch::Sender<bool>,
    next_id: AtomicU64,
    inner: Mutex<Inner>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Cap a client-supplied name and replace control characters, so it is safe
/// in a log line.
fn clean(s: &str, max: usize) -> String {
    s.chars()
        .take(max)
        .map(|c| if c.is_control() { '_' } else { c })
        .collect()
}

impl Events {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tx: broadcast::channel(CHANNEL_CAPACITY).0,
            shutdown: watch::channel(false).0,
            next_id: AtomicU64::new(1),
            inner: Mutex::new(Inner::default()),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.tx.subscribe()
    }

    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }

    /// Ends every `/events` stream so graceful shutdown can finish.
    pub fn shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    pub fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    /// Publish an event; `build` only runs when somebody is subscribed.
    pub fn publish(&self, name: &'static str, build: impl FnOnce() -> Value) {
        if self.tx.receiver_count() == 0 {
            return;
        }
        let data: Arc<str> = build().to_string().into();
        let _ = self.tx.send(Event { name, data });
    }

    /// Register an accepted request and publish `request_start`.
    pub fn begin(self: &Arc<Self>, requested: &str, stream: bool) -> Arc<ReqTrack> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let requested = clean(requested, MAX_MODEL_CHARS);
        let time_ms = now_ms();
        {
            let mut inner = self.lock();
            inner.active += 1;
            if stream {
                inner.active_streams += 1;
            }
            if inner.ring.len() >= RING_CAPACITY {
                inner.ring.pop_front();
            }
            inner.ring.push_back(ReqRecord {
                id,
                time_ms,
                requested: requested.clone(),
                stream,
                done: false,
                attempts: Vec::new(),
                provider: None,
                upstream_model: None,
                ttft_ms: None,
                tokens_in: 0,
                tokens_out: 0,
                duration_ms: None,
                status: None,
                error: None,
            });
        }
        self.publish(
            "request_start",
            || json!({"id": id, "time_ms": time_ms, "model": requested, "stream": stream}),
        );
        Arc::new(ReqTrack {
            events: Arc::clone(self),
            id,
            started: Instant::now(),
            stream,
            ended: AtomicBool::new(false),
            data: Mutex::new(TrackData::default()),
        })
    }

    /// A route changed state: `cooldown`, `breaker_open`, `key_disabled`,
    /// `missing`, `paused`, `resumed` or `cleared`.
    pub fn route_state(
        &self,
        provider: &str,
        model: Option<&str>,
        key: Option<String>,
        state: &'static str,
        expires_in: Option<Duration>,
    ) {
        self.publish("route_state", || {
            json!({
                "provider": provider, "model": model, "key": key, "state": state,
                "expires_in_ms": expires_in.map(millis),
            })
        });
    }

    /// Remember the last error of a provider (and model) for `/status`.
    pub fn note_error(&self, provider: &str, model: Option<&str>, reason: &str) {
        let rec = ErrorRec {
            reason: clean(reason, MAX_REASON_CHARS),
            model: model.map(|m| clean(m, MAX_MODEL_CHARS)),
            time_ms: now_ms(),
        };
        let mut inner = self.lock();
        if let Some(m) = model {
            let day = now_ms() / 86_400_000;
            if let Some(stat) = inner.model_stat(provider, m, day) {
                stat.last_error = Some(rec.clone());
            }
        }
        inner.last_error.insert(provider.to_string(), rec);
    }

    /// Print the log line of a request that failed before it was tracked
    /// (unreadable body or JSON).
    pub fn log_unrouted(&self, status: u16) {
        crate::logsink::out(&log_line(&LogEntry {
            unix: unix_secs(SystemTime::now()),
            suffix: "",
            requested: "-",
            served: None,
            status,
            ttft_ms: None,
            total: Duration::ZERO,
            usage: Usage::default(),
            attempts: 0,
        }));
    }

    /// Newest first, with the in-flight and in-flight streaming counts.
    pub fn recent(&self) -> (Vec<ReqRecord>, u64, u64) {
        let inner = self.lock();
        (
            inner.ring.iter().rev().cloned().collect(),
            inner.active,
            inner.active_streams,
        )
    }

    pub fn provider_events(&self, provider: &str) -> ProviderEvents {
        let day = now_ms() / 86_400_000;
        let inner = self.lock();
        let mut samples: Vec<u64> = inner
            .ttft
            .get(provider)
            .map_or_else(Vec::new, |q| q.iter().copied().collect());
        samples.sort_unstable();
        let pick = |p: f64| {
            let rank = (p * samples.len() as f64).ceil() as usize;
            samples.get(rank.saturating_sub(1)).copied()
        };
        let ttft = TtftStats {
            median: pick(0.5),
            p95: pick(0.95),
            samples: samples.len(),
        };
        let models = inner
            .models
            .iter()
            .filter(|((p, _), _)| p == provider)
            .map(|((_, m), s)| {
                let mut s = s.clone();
                if s.day != day {
                    s.requests = 0;
                    s.tokens_in = 0;
                    s.tokens_out = 0;
                }
                (m.clone(), s)
            })
            .collect();
        ProviderEvents {
            ttft,
            last_error: inner.last_error.get(provider).cloned(),
            models,
        }
    }
}

impl Inner {
    fn record(&mut self, id: u64) -> Option<&mut ReqRecord> {
        self.ring.iter_mut().rev().find(|r| r.id == id)
    }

    /// Counters for `(provider, model)` on UTC day `day`, rolled over when
    /// stale. `None` when the table is full and the pair is new.
    fn model_stat(&mut self, provider: &str, model: &str, day: u64) -> Option<&mut ModelStat> {
        let key = (provider.to_string(), model.to_string());
        if !self.models.contains_key(&key) && self.models.len() >= MAX_MODEL_STATS {
            return None;
        }
        let stat = self.models.entry(key).or_default();
        if stat.day != day {
            stat.day = day;
            stat.requests = 0;
            stat.tokens_in = 0;
            stat.tokens_out = 0;
        }
        Some(stat)
    }
}

#[derive(Debug, Clone)]
struct Served {
    provider: String,
    model: String,
    paid: bool,
}

#[derive(Debug, Default)]
struct TrackData {
    attempts: u32,
    ttft_ms: Option<u64>,
    /// The latest route attempted.
    last: Option<Served>,
    /// Failed attempts as `provider/model:reason`, for the log line.
    fails: Vec<String>,
    /// Routes skipped as ineligible (503), for the log line.
    skipped: Vec<String>,
}

/// Handle for one request. Ends the request exactly once: explicitly through
/// [`ReqTrack::end`], or as `client_closed` (499) when dropped unfinished
/// (the client went away).
#[derive(Debug)]
pub struct ReqTrack {
    events: Arc<Events>,
    id: u64,
    started: Instant,
    stream: bool,
    ended: AtomicBool,
    data: Mutex<TrackData>,
}

impl ReqTrack {
    fn data(&self) -> MutexGuard<'_, TrackData> {
        self.data.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record an attempt that began at `since` and has now ended with
    /// `outcome`: `ok` or a failure reason.
    pub fn attempt(
        &self,
        provider: &Provider,
        model: &str,
        key: &ApiKey,
        outcome: &str,
        since: Instant,
    ) {
        let ms = millis(since.elapsed());
        let n = {
            let mut d = self.data();
            d.attempts += 1;
            if outcome != "ok" && d.fails.len() < MAX_LOG_LIST {
                d.fails.push(format!(
                    "{}/{}:{}",
                    provider.id,
                    model,
                    outcome.strip_prefix("http_").unwrap_or(outcome)
                ));
            }
            d.last = Some(Served {
                provider: provider.id.clone(),
                model: model.to_string(),
                paid: provider.paid,
            });
            d.attempts
        };
        let rec = AttemptRec {
            n,
            provider: provider.id.clone(),
            model: model.to_string(),
            key: key.redacted(),
            outcome: clean(outcome, MAX_REASON_CHARS),
            ms,
        };
        {
            let mut inner = self.events.lock();
            let day = now_ms() / 86_400_000;
            if let Some(s) = inner.model_stat(&rec.provider, &rec.model, day) {
                s.requests += 1;
            }
            if let Some(r) = inner.record(self.id)
                && r.attempts.len() < MAX_CHAIN
            {
                r.attempts.push(rec.clone());
            }
        }
        let id = self.id;
        self.events.publish("attempt", || {
            let mut v = rec.to_json();
            if let Some(o) = v.as_object_mut() {
                o.insert("id".to_string(), json!(id));
            }
            v
        });
    }

    /// Remember routes skipped as ineligible (`provider/model:reason`) for
    /// the log line of a 503.
    pub fn skipped(&self, routes: impl IntoIterator<Item = String>) {
        self.data().skipped = routes.into_iter().take(MAX_LOG_LIST).collect();
    }

    /// First upstream byte arrived, `ttft` after the serving attempt began.
    /// Only the first call counts.
    pub fn first_byte(&self, ttft: Duration) {
        let ms = millis(ttft);
        let provider = {
            let mut d = self.data();
            if d.ttft_ms.is_some() {
                return;
            }
            d.ttft_ms = Some(ms);
            d.last.as_ref().map(|s| s.provider.clone())
        };
        {
            let mut inner = self.events.lock();
            if let Some(r) = inner.record(self.id) {
                r.ttft_ms = Some(ms);
            }
            if let Some(p) = provider {
                let q = inner.ttft.entry(p).or_default();
                if q.len() >= TTFT_SAMPLES {
                    q.pop_front();
                }
                q.push_back(ms);
            }
        }
        let id = self.id;
        self.events
            .publish("first_byte", || json!({"id": id, "ttft_ms": ms}));
    }

    /// Finish the request: updates the ring, publishes `request_end` and
    /// prints the log line. Later calls do nothing.
    pub fn end(&self, status: u16, error: Option<&'static str>, usage: Usage) {
        if self.ended.swap(true, Ordering::AcqRel) {
            return;
        }
        let total = self.started.elapsed();
        let (attempts, ttft_ms, served, suffix) = {
            let d = self.data();
            let mut suffix = String::new();
            if !d.fails.is_empty() {
                suffix = format!(" fails={}", d.fails.join(","));
            }
            if !d.skipped.is_empty() {
                suffix.push_str(&format!(" skipped={}", d.skipped.join(",")));
            }
            (d.attempts, d.ttft_ms, d.last.clone(), suffix)
        };
        let requested;
        {
            let mut inner = self.events.lock();
            inner.active = inner.active.saturating_sub(1);
            if self.stream {
                inner.active_streams = inner.active_streams.saturating_sub(1);
            }
            if let Some(s) = &served {
                let day = now_ms() / 86_400_000;
                if let Some(m) = inner.model_stat(&s.provider, &s.model, day) {
                    m.tokens_in = m.tokens_in.saturating_add(usage.input);
                    m.tokens_out = m.tokens_out.saturating_add(usage.output);
                }
            }
            match inner.record(self.id) {
                Some(r) => {
                    r.done = true;
                    r.status = Some(status);
                    r.error = error;
                    r.duration_ms = Some(millis(total));
                    r.tokens_in = usage.input;
                    r.tokens_out = usage.output;
                    r.provider = served.as_ref().map(|s| s.provider.clone());
                    r.upstream_model = served.as_ref().map(|s| s.model.clone());
                    requested = r.requested.clone();
                }
                None => requested = "-".to_string(),
            }
        }
        let id = self.id;
        self.events.publish("request_end", || {
            json!({
                "id": id, "status": status, "error": error,
                "provider": served.as_ref().map(|s| s.provider.as_str()),
                "model": served.as_ref().map(|s| s.model.as_str()),
                "tokens_in": usage.input, "tokens_out": usage.output,
                "ttft_ms": ttft_ms, "duration_ms": millis(total), "attempts": attempts,
            })
        });
        crate::logsink::out(&log_line(&LogEntry {
            unix: unix_secs(SystemTime::now()),
            suffix: &suffix,
            requested: &requested,
            served: served
                .as_ref()
                .map(|s| (s.provider.as_str(), s.model.as_str(), s.paid)),
            status,
            ttft_ms,
            total,
            usage,
            attempts,
        }));
    }
}

impl Drop for ReqTrack {
    fn drop(&mut self) {
        self.end(499, Some("client_closed"), Usage::default());
    }
}

struct LogEntry<'a> {
    unix: u64,
    requested: &'a str,
    /// Provider id, upstream model, paid.
    served: Option<(&'a str, &'a str, bool)>,
    status: u16,
    ttft_ms: Option<u64>,
    total: Duration,
    usage: Usage,
    attempts: u32,
    /// ` fails=...` and/or ` skipped=...`, or empty.
    suffix: &'a str,
}

/// PRD section 9 plus the optional suffix:
/// `2026-11-16T08:45:12Z coder -> nvidia/vendor/example-model 200 ttft=412ms total=3.1s in=1840 out=612 attempts=2 free`
fn log_line(e: &LogEntry<'_>) -> String {
    let route = match e.served {
        Some((p, m, _)) => format!("{p}/{m}"),
        None => "-".to_string(),
    };
    let tier = match e.served {
        Some((_, _, true)) => "paid",
        Some((_, _, false)) => "free",
        None => "-",
    };
    let ttft = e
        .ttft_ms
        .map_or_else(|| "-".to_string(), |ms| format!("{ms}ms"));
    format!(
        "{} {} -> {} {} ttft={} total={:.1}s in={} out={} attempts={} {}{}",
        format_utc(e.unix),
        e.requested,
        route,
        e.status,
        ttft,
        e.total.as_secs_f64(),
        e.usage.input,
        e.usage.output,
        e.attempts,
        tier,
        e.suffix
    )
}

/// `2026-11-16T08:45:12Z` for Unix seconds (Hinnant's civil-from-days).
pub fn format_utc(unix: u64) -> String {
    let days = i64::try_from(unix / 86_400).unwrap_or(0);
    let rem = unix % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_formatting() {
        assert_eq!(format_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_utc(1_000_000_000), "2001-09-09T01:46:40Z");
        assert_eq!(format_utc(1_709_164_799), "2024-02-28T23:59:59Z");
        assert_eq!(format_utc(1_709_164_800), "2024-02-29T00:00:00Z");
    }

    #[test]
    fn log_line_appends_the_failure_suffix() {
        let line = log_line(&LogEntry {
            unix: 1_000_000_000,
            requested: "coder",
            served: Some(("nvidia", "m", false)),
            status: 200,
            ttft_ms: None,
            total: Duration::from_millis(100),
            usage: Usage::default(),
            attempts: 2,
            suffix: " fails=groq/openai/gpt-oss-120b:429,nvidia/x:timeout",
        });
        assert!(
            line.ends_with("attempts=2 free fails=groq/openai/gpt-oss-120b:429,nvidia/x:timeout"),
            "{line}"
        );
    }

    #[test]
    fn log_line_matches_the_prd_format() {
        let line = log_line(&LogEntry {
            unix: 1_000_000_000,
            requested: "coder",
            served: Some(("nvidia", "vendor/example-model", false)),
            status: 200,
            ttft_ms: Some(412),
            total: Duration::from_millis(3100),
            usage: Usage {
                input: 1840,
                output: 612,
            },
            attempts: 2,
            suffix: "",
        });
        assert_eq!(
            line,
            "2001-09-09T01:46:40Z coder -> nvidia/vendor/example-model 200 ttft=412ms total=3.1s in=1840 out=612 attempts=2 free"
        );
    }

    #[test]
    fn publishing_without_subscribers_does_not_build_the_payload() {
        let events = Events::new();
        events.publish("x", || unreachable!("no subscriber"));
    }
}
