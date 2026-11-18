//! Route state: scoped quota counters, cooldowns with exponential backoff,
//! per-provider circuit breakers, disabled keys, missing routes, pause flags,
//! request counts, hourly token buckets and paid spend.
//!
//! All methods take `&self` and lock one `std::sync::Mutex` for a short,
//! non-async critical section. Time is passed in (`now`) so tests never sleep.
//! Wall-clock time (day boundaries, persistence) is derived from `now` and an
//! anchor pair captured at construction, so one `Instant` drives everything.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::catalog::Candidate;
use crate::config::{LimitScope, Limits, Provider, Strategy};
use crate::select::{self, QuotaKind, ReqInfo, RouteView, Selection, Skip};

/// First 429 backoff when the upstream gives no `Retry-After`.
pub const BACKOFF_BASE: Duration = Duration::from_secs(15);
pub const BACKOFF_CAP: Duration = Duration::from_secs(15 * 60);
/// Consecutive retryable failures on one provider that open its breaker.
pub const BREAKER_THRESHOLD: u32 = 3;
pub const BREAKER_OPEN: Duration = Duration::from_secs(60);
/// Longest honoured `Retry-After`.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(24 * 3600);
/// The rolling window length.
const WINDOW: Duration = Duration::from_secs(60);
/// Hours of per-provider token history kept for the dashboard.
pub const HOURLY_BUCKETS: u64 = 24;
/// Cooldowns at most this long are not worth persisting.
pub const PERSIST_COOLDOWN_MIN: Duration = Duration::from_secs(60);
/// Credit guard fraction: when spend reaches this fraction of credit_usd,
/// the provider becomes ineligible until the next period reset.
pub const CREDIT_GUARD_FRACTION: f64 = 0.9;

/// Backoff after `step` earlier consecutive 429s: 15 s, 30 s, ... capped.
pub fn backoff(step: u32) -> Duration {
    let secs = BACKOFF_BASE.as_secs().saturating_mul(1u64 << step.min(20));
    Duration::from_secs(secs).min(BACKOFF_CAP)
}

/// Scope a cooldown or counter applies to. `None` fields are wildcards.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScopeKey {
    pub provider: String,
    pub key: Option<usize>,
    pub model: Option<String>,
}

impl ScopeKey {
    pub fn route(provider: &str, key: usize, model: &str) -> Self {
        Self {
            provider: provider.to_string(),
            key: Some(key),
            model: Some(model.to_string()),
        }
    }
    pub fn key_wide(provider: &str, key: usize) -> Self {
        Self {
            provider: provider.to_string(),
            key: Some(key),
            model: None,
        }
    }
    pub fn provider_wide(provider: &str) -> Self {
        Self {
            provider: provider.to_string(),
            key: None,
            model: None,
        }
    }
}

/// The bucket a provider's `[provider.limits]` apply to for this route.
/// Cooldowns from a 429 use the same scope.
pub fn scope_key(provider: &Provider, key: usize, model: &str) -> ScopeKey {
    match provider.limits.scope {
        LimitScope::Provider => ScopeKey::provider_wide(&provider.id),
        LimitScope::Key => ScopeKey::key_wide(&provider.id, key),
        LimitScope::KeyModel => ScopeKey::route(&provider.id, key, model),
    }
}

/// Tokens used by one request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub input: u64,
    pub output: u64,
}

impl Usage {
    pub fn total(&self) -> u64 {
        self.input.saturating_add(self.output)
    }
}

/// Remaining-allowance hints from `x-ratelimit-remaining-*` response headers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RemainingHints {
    pub requests: Option<u64>,
    pub tokens: Option<u64>,
}

/// Seconds since the Unix epoch.
pub fn unix_secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// Index of the quota day containing `unix`, with days starting at
/// `reset_hour` UTC.
pub fn day_index(unix: u64, reset_hour: u8) -> u64 {
    unix.saturating_sub(u64::from(reset_hour) * 3600) / 86_400
}

/// Unix time at which quota day `day` ends.
fn day_end(day: u64, reset_hour: u8) -> u64 {
    (day + 1) * 86_400 + u64::from(reset_hour) * 3600
}

#[derive(Debug, Clone, Copy)]
struct Cooldown {
    until: Instant,
    /// Consecutive 429s without `Retry-After`; drives the backoff.
    step: u32,
}

#[derive(Debug, Default, Clone, Copy)]
struct Breaker {
    consecutive: u32,
    open_until: Option<Instant>,
}

type Window = VecDeque<(Instant, u64)>;

/// Counters for one scope: rolling 60 s window and the current quota day.
#[derive(Debug, Default, Clone)]
struct Bucket {
    win_req: Window,
    win_tok: Window,
    day: u64,
    day_req: u64,
    day_tok: u64,
}

fn window_sum(q: &Window, now: Instant) -> u64 {
    q.iter()
        .filter(|(t, _)| now.saturating_duration_since(*t) < WINDOW)
        .map(|(_, n)| *n)
        .fold(0, u64::saturating_add)
}

/// When the window will have shed at least `over` units, oldest first.
fn window_frees(q: &Window, over: u64, now: Instant) -> Option<Instant> {
    let mut freed = 0u64;
    for (t, n) in q {
        if now.saturating_duration_since(*t) >= WINDOW {
            continue;
        }
        freed = freed.saturating_add(*n);
        if freed >= over {
            return Some(*t + WINDOW);
        }
    }
    None
}

fn push_window(q: &mut Window, now: Instant, n: u64) {
    while q
        .front()
        .is_some_and(|(t, _)| now.saturating_duration_since(*t) >= WINDOW)
    {
        q.pop_front();
    }
    q.push_back((now, n));
}

impl Bucket {
    /// Roll the day counters over when `day` differs from the stored one.
    fn roll(&mut self, day: u64) {
        if self.day != day {
            self.day = day;
            self.day_req = 0;
            self.day_tok = 0;
        }
    }

    /// First known limit this request (`estimate` tokens) would exceed.
    /// Token limits never block while their counter is still empty: the
    /// estimate may be wrong, so a first request is always attempted.
    fn check(
        &self,
        limits: &Limits,
        estimate: u64,
        day: u64,
        day_free: Instant,
        now: Instant,
    ) -> Option<Skip> {
        let quota = |limit, until| Some(Skip::Quota { limit, until });
        if let Some(l) = limits.rpm {
            let used = window_sum(&self.win_req, now);
            if used.saturating_add(1) > l {
                let until = window_frees(&self.win_req, used.saturating_add(1) - l, now);
                return quota(QuotaKind::Rpm, until);
            }
        }
        if let Some(l) = limits.tpm {
            let used = window_sum(&self.win_tok, now);
            if used > 0 && used.saturating_add(estimate) > l {
                let until = window_frees(&self.win_tok, used.saturating_add(estimate) - l, now);
                return quota(QuotaKind::Tpm, until);
            }
        }
        let (day_req, day_tok) = if self.day == day {
            (self.day_req, self.day_tok)
        } else {
            (0, 0)
        };
        if let Some(l) = limits.rpd
            && day_req.saturating_add(1) > l
        {
            return quota(QuotaKind::Rpd, Some(day_free));
        }
        if let Some(l) = limits.tpd
            && day_tok > 0
            && day_tok.saturating_add(estimate) > l
        {
            return quota(QuotaKind::Tpd, Some(day_free));
        }
        None
    }
}

type RouteId = (String, usize, String);

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Spend {
    /// UTC day number (days since the epoch).
    pub day: u64,
    pub usd: f64,
}

#[derive(Debug)]
pub struct State {
    /// `Instant` and wall clock captured together; see [`State::wall`].
    anchor: (Instant, SystemTime),
    cooldowns: HashMap<ScopeKey, Cooldown>,
    breakers: HashMap<String, Breaker>,
    disabled: HashSet<(String, usize)>,
    missing: HashSet<(String, String)>,
    /// Routes refused with a 403 (region or plan): until restart.
    unavailable: HashSet<(String, String)>,
    /// Per-(provider, model) breakers for HTTP 5xx.
    route_breakers: HashMap<(String, String), Breaker>,
    /// Learned size ceilings (pre-flight estimate) per (provider, model).
    ceilings: HashMap<(String, String), u64>,
    paused: HashSet<String>,
    requests: HashMap<RouteId, u64>,
    pool_counters: HashMap<String, usize>,
    key_counters: HashMap<String, usize>,
    buckets: HashMap<ScopeKey, Bucket>,
    /// Per provider: hour number -> (tokens in, tokens out).
    hourly: HashMap<String, BTreeMap<u64, (u64, u64)>>,
    spend: Spend,
    /// Per-provider UTC day spend.
    provider_spend: HashMap<String, Spend>,
    /// Per-provider credit period spend.
    credit: HashMap<String, CreditSpend>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            anchor: (Instant::now(), SystemTime::now()),
            cooldowns: HashMap::new(),
            breakers: HashMap::new(),
            disabled: HashSet::new(),
            missing: HashSet::new(),
            unavailable: HashSet::new(),
            route_breakers: HashMap::new(),
            ceilings: HashMap::new(),
            paused: HashSet::new(),
            requests: HashMap::new(),
            pool_counters: HashMap::new(),
            key_counters: HashMap::new(),
            buckets: HashMap::new(),
            hourly: HashMap::new(),
            spend: Spend::default(),
            provider_spend: HashMap::new(),
            credit: HashMap::new(),
        }
    }
}

impl State {
    /// Wall-clock time for an `Instant`.
    fn wall(&self, at: Instant) -> SystemTime {
        let (a_i, a_s) = self.anchor;
        if at >= a_i {
            a_s + (at - a_i)
        } else {
            a_s.checked_sub(a_i - at).unwrap_or(UNIX_EPOCH)
        }
    }

    fn unix(&self, at: Instant) -> u64 {
        unix_secs(self.wall(at))
    }

    /// Buckets a request on this route is counted in: the scope bucket, plus
    /// the route bucket when the model has its own limits.
    fn buckets_for(provider: &Provider, key: usize, model: &str) -> Vec<ScopeKey> {
        let scope = scope_key(provider, key, model);
        let mut out = vec![scope.clone()];
        if provider.model_limits.contains_key(model) {
            let route = ScopeKey::route(&provider.id, key, model);
            if route != scope {
                out.push(route);
            }
        }
        out
    }

    fn quota_skip(
        &self,
        provider: &Provider,
        key: usize,
        model: &str,
        estimate: u64,
        now: Instant,
    ) -> Option<Skip> {
        let unix = self.unix(now);
        let day = day_index(unix, provider.reset_utc_hour);
        let to_reset = day_end(day, provider.reset_utc_hour).saturating_sub(unix);
        let day_free = now + Duration::from_secs(to_reset.max(1));
        let scope = scope_key(provider, key, model);
        if let Some(s) = self
            .buckets
            .get(&scope)
            .and_then(|b| b.check(&provider.limits.limits, estimate, day, day_free, now))
        {
            return Some(s);
        }
        let limits = provider.model_limits.get(model)?;
        self.buckets
            .get(&ScopeKey::route(&provider.id, key, model))?
            .check(limits, estimate, day, day_free, now)
    }

    /// Private helper to compute credit status for a provider.
    fn credit_status(&self, provider: &Provider, now: Instant) -> Option<CreditStatus> {
        let credit = provider.credit.as_ref()?;
        let unix = self.unix(now);
        let period_start =
            crate::billing::month_start(unix, provider.month_reset_day, provider.reset_utc_hour);
        let resets_at = crate::billing::next_month_start(
            unix,
            provider.month_reset_day,
            provider.reset_utc_hour,
        );
        let spent = self
            .credit
            .get(&provider.id)
            .filter(|cs| cs.period_start == period_start)
            .map(|cs| cs.usd)
            .unwrap_or(0.0);
        let exhausted = spent >= credit.usd * CREDIT_GUARD_FRACTION;
        Some(CreditStatus {
            credit_usd: credit.usd,
            spent_usd: spent,
            period_start,
            resets_at,
            exhausted,
        })
    }
}

impl RouteView for State {
    fn skip_reason(
        &self,
        provider: &Provider,
        key: usize,
        model: &str,
        estimate: u64,
        now: Instant,
    ) -> Option<Skip> {
        let id = provider.id.as_str();
        if self.paused.contains(id) {
            return Some(Skip::Paused);
        }
        // Credit guard: skip if this credit provider's spend has reached the guard fraction.
        if let Some(cs) = self.credit_status(provider, now)
            && cs.exhausted
        {
            let until_secs = cs.resets_at.saturating_sub(self.unix(now)).max(1);
            return Some(Skip::CreditExhausted {
                until: now + Duration::from_secs(until_secs),
            });
        }
        if self.disabled.contains(&(id.to_string(), key)) {
            return Some(Skip::KeyDisabled);
        }
        if self.missing.contains(&(id.to_string(), model.to_string())) {
            return Some(Skip::Missing);
        }
        if self
            .unavailable
            .contains(&(id.to_string(), model.to_string()))
        {
            return Some(Skip::Unavailable);
        }
        if estimate > 0
            && effective_ceiling(
                provider,
                model,
                self.ceilings
                    .get(&(id.to_string(), model.to_string()))
                    .copied(),
            )
            .is_some_and(|(c, _)| c <= estimate)
        {
            return Some(Skip::TooLarge);
        }
        if let Some(until) = self
            .route_breakers
            .get(&(id.to_string(), model.to_string()))
            .and_then(|b| b.open_until)
            .filter(|u| *u > now)
        {
            return Some(Skip::BreakerOpen { until });
        }
        if let Some(until) = self
            .breakers
            .get(id)
            .and_then(|b| b.open_until)
            .filter(|u| *u > now)
        {
            return Some(Skip::BreakerOpen { until });
        }
        let cooling = [
            ScopeKey::provider_wide(id),
            ScopeKey::key_wide(id, key),
            ScopeKey::route(id, key, model),
        ]
        .iter()
        .filter_map(|k| self.cooldowns.get(k))
        .map(|c| c.until)
        .filter(|u| *u > now)
        .max();
        if let Some(until) = cooling {
            return Some(Skip::Cooldown { until });
        }
        self.quota_skip(provider, key, model, estimate, now)
    }

    fn request_count(&self, provider: &str, key: usize, model: &str) -> u64 {
        self.requests
            .get(&(provider.to_string(), key, model.to_string()))
            .copied()
            .unwrap_or(0)
    }

    fn key_rotation(&self, provider: &str) -> usize {
        self.key_counters.get(provider).copied().unwrap_or(0)
    }
}

/// Where an effective size ceiling comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CeilingSource {
    Learned,
    Config,
}

impl CeilingSource {
    pub fn name(self) -> &'static str {
        match self {
            Self::Learned => "learned",
            Self::Config => "config",
        }
    }
}

/// The lower of the configured `max_input_tokens` and the learned ceiling.
/// On a tie the configured value wins.
pub fn effective_ceiling(
    provider: &Provider,
    model: &str,
    learned: Option<u64>,
) -> Option<(u64, CeilingSource)> {
    let config = provider
        .model_limits
        .get(model)
        .and_then(|l| l.max_input_tokens);
    match (config, learned) {
        (Some(c), Some(l)) if l < c => Some((l, CeilingSource::Learned)),
        (Some(c), _) => Some((c, CeilingSource::Config)),
        (None, Some(l)) => Some((l, CeilingSource::Learned)),
        (None, None) => None,
    }
}

/// A cooldown, with its deadline as wall-clock Unix seconds, for persistence.
#[derive(Debug, Clone, PartialEq)]
pub struct CooldownSnap {
    pub scope: ScopeKey,
    pub until_unix: u64,
    pub step: u32,
}

/// Credit spend for one provider, per credit period.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct CreditSpend {
    pub period_start: u64,
    pub usd: f64,
}

/// Status of a credit provider's spending for the current period.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CreditStatus {
    pub credit_usd: f64,
    pub spent_usd: f64,
    pub period_start: u64,
    pub resets_at: u64,
    pub exhausted: bool,
}

/// A day counter for one bucket, for persistence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaySnap {
    pub scope: ScopeKey,
    pub day: u64,
    pub requests: u64,
    pub tokens: u64,
}

/// Everything that survives a restart.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Snapshot {
    pub days: Vec<DaySnap>,
    /// (provider, hour number, tokens in, tokens out)
    pub hourly: Vec<(String, u64, u64, u64)>,
    pub cooldowns: Vec<CooldownSnap>,
    /// (provider, model, ceiling in estimated tokens)
    pub ceilings: Vec<(String, String, u64)>,
    pub spend: Spend,
    /// Per-provider UTC day spend.
    pub provider_spend: Vec<(String, Spend)>,
    /// Per-provider credit period spend.
    pub credit: Vec<(String, CreditSpend)>,
}

/// State of one provider, with deadlines as `Instant`s.
#[derive(Debug, Default, Clone)]
pub struct ProviderView {
    pub paused: bool,
    pub breaker_until: Option<Instant>,
    /// Active cooldowns, soonest expiry first.
    pub cooldowns: Vec<(ScopeKey, Instant)>,
    /// Indices of disabled keys.
    pub disabled_keys: Vec<usize>,
    /// Upstream models marked missing.
    pub missing: Vec<String>,
    /// Upstream models refused with a 403 (region or plan).
    pub unavailable: Vec<String>,
    /// Open per-model breakers with their expiry.
    pub route_breakers: Vec<(String, Instant)>,
    /// Learned size ceilings by model.
    pub ceilings: Vec<(String, u64)>,
    /// Counters of the provider's limit scope, summed over its buckets.
    pub requests_minute: u64,
    pub requests_day: u64,
    pub tokens_minute: u64,
    pub tokens_day: u64,
}

#[derive(Debug, Default)]
pub struct Limiter {
    inner: Mutex<State>,
}

impl Limiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a limiter whose wall clock is `wall` at `at`, so tests can pin dates.
    /// Test-oriented.
    pub fn with_anchor(at: Instant, wall: SystemTime) -> Self {
        let inner = State {
            anchor: (at, wall),
            ..State::default()
        };
        Self {
            inner: Mutex::new(inner),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Expand, filter and order `candidates`, then advance the rotation
    /// counters (pool round-robin and each provider's key rotation).
    pub fn plan<'a>(
        &self,
        candidates: &[Candidate<'a>],
        pool: Option<(&str, Strategy)>,
        req: &ReqInfo,
        now: Instant,
    ) -> Selection<'a> {
        let mut st = self.lock();
        let (strategy, counter) = match pool {
            Some((name, s)) => (s, st.pool_counters.get(name).copied().unwrap_or(0)),
            None => (Strategy::Priority, 0),
        };
        let selection = select::select(candidates, strategy, counter, req, &*st, now);
        if let Some((name, _)) = pool {
            let c = st.pool_counters.entry(name.to_string()).or_insert(0);
            *c = c.wrapping_add(1);
        }
        let mut seen: Vec<&str> = Vec::new();
        for c in candidates {
            if !seen.contains(&c.provider.id.as_str()) {
                seen.push(&c.provider.id);
                let k = st.key_counters.entry(c.provider.id.clone()).or_insert(0);
                *k = k.wrapping_add(1);
            }
        }
        selection
    }

    /// Re-check one entry just before attempting it.
    pub fn skip_reason(
        &self,
        provider: &Provider,
        key: usize,
        model: &str,
        estimate: u64,
        now: Instant,
    ) -> Option<Skip> {
        self.lock().skip_reason(provider, key, model, estimate, now)
    }

    /// Count a request sent upstream (feeds `least_used` and the request
    /// counters of every applicable bucket).
    pub fn note_request(&self, provider: &Provider, key: usize, model: &str, now: Instant) {
        let mut st = self.lock();
        let c = st
            .requests
            .entry((provider.id.clone(), key, model.to_string()))
            .or_insert(0);
        *c = c.saturating_add(1);
        let day = day_index(st.unix(now), provider.reset_utc_hour);
        for scope in State::buckets_for(provider, key, model) {
            let b = st.buckets.entry(scope).or_default();
            b.roll(day);
            b.day_req = b.day_req.saturating_add(1);
            push_window(&mut b.win_req, now, 1);
        }
    }

    /// Record tokens used by a finished (or cancelled) request: bucket
    /// counters, the provider's hourly history and spend tracking.
    /// `hints` (provider-reported remaining allowance) then overwrite the
    /// provider-scope counters where they map cleanly:
    ///
    /// - `x-ratelimit-remaining-tokens` is a per-minute figure for the
    ///   providers that send it, so it sets the rolling token window when
    ///   `tpm` is known.
    /// - `x-ratelimit-remaining-requests` is per minute for some providers and
    ///   per day for others, so it is applied only when exactly one of `rpm`
    ///   and `rpd` is configured (that one), and ignored when both or neither
    ///   are known.
    ///
    /// Returns `true` when this call moved a credit provider from not exhausted
    /// to exhausted (crossed the 90% guard fraction).
    #[allow(clippy::too_many_arguments)]
    pub fn record_usage(
        &self,
        provider: &Provider,
        key: usize,
        model: &str,
        usage: Usage,
        hints: RemainingHints,
        now: Instant,
        reported_cost_usd: Option<f64>,
    ) -> bool {
        let mut st = self.lock();
        let unix = st.unix(now);
        let day = day_index(unix, provider.reset_utc_hour);
        let total = usage.total();
        for scope in State::buckets_for(provider, key, model) {
            let b = st.buckets.entry(scope).or_default();
            b.roll(day);
            b.day_tok = b.day_tok.saturating_add(total);
            push_window(&mut b.win_tok, now, total);
        }
        let hour = unix / 3600;
        let h = st.hourly.entry(provider.id.clone()).or_default();
        let slot = h.entry(hour).or_insert((0, 0));
        slot.0 = slot.0.saturating_add(usage.input);
        slot.1 = slot.1.saturating_add(usage.output);
        h.retain(|k, _| *k + HOURLY_BUCKETS > hour);

        // Compute cost once
        let cost = crate::billing::request_cost_usd(provider, model, usage, reported_cost_usd);

        // Per-provider UTC day spend for every billing type
        let utc_day = unix / 86_400;
        let ps = st.provider_spend.entry(provider.id.clone()).or_default();
        if ps.day != utc_day {
            ps.day = utc_day;
            ps.usd = 0.0;
        }
        ps.usd += cost;

        // Paid providers: also track in global spend (for paid_daily_cap_usd)
        if provider.paid {
            if st.spend.day != utc_day {
                st.spend = Spend {
                    day: utc_day,
                    usd: 0.0,
                };
            }
            st.spend.usd += cost;
        }

        // Credit providers: track per credit period and detect crossing
        let mut credit_exhausted_now = false;
        if let Some(credit) = &provider.credit {
            let start = crate::billing::month_start(
                unix,
                provider.month_reset_day,
                provider.reset_utc_hour,
            );
            let cs = st.credit.entry(provider.id.clone()).or_default();
            let was_exhausted = if cs.period_start == start {
                cs.usd >= credit.usd * CREDIT_GUARD_FRACTION
            } else {
                false
            };
            if cs.period_start != start {
                cs.period_start = start;
                cs.usd = 0.0;
            }
            cs.usd += cost;
            let now_exhausted = cs.usd >= credit.usd * CREDIT_GUARD_FRACTION;
            credit_exhausted_now = !was_exhausted && now_exhausted;
        }

        let limits = provider.limits.limits;
        let b = st
            .buckets
            .entry(scope_key(provider, key, model))
            .or_default();
        if let (Some(rem), Some(l)) = (hints.tokens, limits.tpm) {
            b.win_tok.clear();
            b.win_tok.push_back((now, l.saturating_sub(rem)));
        }
        if let Some(rem) = hints.requests {
            match (limits.rpm, limits.rpd) {
                (Some(l), None) => {
                    b.win_req.clear();
                    b.win_req.push_back((now, l.saturating_sub(rem)));
                }
                (None, Some(l)) => {
                    b.day = day;
                    b.day_req = l.saturating_sub(rem);
                }
                _ => {}
            }
        }
        credit_exhausted_now
    }

    /// Paid spend for the current UTC day, in USD.
    pub fn spend_today(&self, now: Instant) -> f64 {
        let st = self.lock();
        if st.spend.day == st.unix(now) / 86_400 {
            st.spend.usd
        } else {
            0.0
        }
    }

    /// Per-provider spend for the current UTC day, in USD.
    pub fn provider_spend_today(&self, provider: &str, now: Instant) -> f64 {
        let st = self.lock();
        let utc_day = st.unix(now) / 86_400;
        st.provider_spend
            .get(provider)
            .filter(|ps| ps.day == utc_day)
            .map(|ps| ps.usd)
            .unwrap_or(0.0)
    }

    /// Total estimated spend across all providers for the current UTC day, in USD.
    pub fn total_spend_today(&self, now: Instant) -> f64 {
        let st = self.lock();
        let utc_day = st.unix(now) / 86_400;
        st.provider_spend
            .values()
            .filter(|ps| ps.day == utc_day)
            .map(|ps| ps.usd)
            .sum()
    }

    /// Credit spend for the current credit period, in USD.
    /// Returns 0.0 when the stored period is not the current one.
    pub fn credit_spent(&self, provider: &Provider, now: Instant) -> f64 {
        let st = self.lock();
        let unix = st.unix(now);
        let start =
            crate::billing::month_start(unix, provider.month_reset_day, provider.reset_utc_hour);
        st.credit
            .get(&provider.id)
            .filter(|cs| cs.period_start == start)
            .map(|cs| cs.usd)
            .unwrap_or(0.0)
    }

    /// Full credit status for the current credit period, including whether
    /// the guard fraction has been reached.
    pub fn credit_status(&self, provider: &Provider, now: Instant) -> Option<CreditStatus> {
        self.lock().credit_status(provider, now)
    }

    /// Tokens in and out per hour for `provider`, oldest first, covering the
    /// last 24 hours: `(hour number since the epoch, in, out)`.
    pub fn hourly(&self, provider: &str, now: Instant) -> Vec<(u64, u64, u64)> {
        let st = self.lock();
        let hour = st.unix(now) / 3600;
        st.hourly.get(provider).map_or_else(Vec::new, |h| {
            h.iter()
                .filter(|(k, _)| **k + HOURLY_BUCKETS > hour)
                .map(|(k, (i, o))| (*k, *i, *o))
                .collect()
        })
    }

    /// A 2xx response: reset the backoff at `scope`, the route's breaker
    /// count and the provider's breaker count.
    pub fn record_success(&self, scope: &ScopeKey, model: &str) {
        let mut st = self.lock();
        st.cooldowns.remove(scope);
        if let Some(b) = st.breakers.get_mut(&scope.provider) {
            b.consecutive = 0;
        }
        if let Some(b) = st
            .route_breakers
            .get_mut(&(scope.provider.clone(), model.to_string()))
        {
            b.consecutive = 0;
        }
    }

    /// An HTTP 5xx on one route. Returns true when this trips the route's
    /// breaker.
    pub fn record_route_failure(&self, provider: &str, model: &str, now: Instant) -> bool {
        let mut st = self.lock();
        let b = st
            .route_breakers
            .entry((provider.to_string(), model.to_string()))
            .or_default();
        b.consecutive = b.consecutive.saturating_add(1);
        if b.consecutive >= BREAKER_THRESHOLD {
            b.consecutive = 0;
            b.open_until = Some(now + BREAKER_OPEN);
            true
        } else {
            false
        }
    }

    /// A 429: cool `scope` (the provider's limit scope) down. Uses
    /// `retry_after` when given, else the exponential backoff. Returns the
    /// cooldown length.
    pub fn record_rate_limit(
        &self,
        scope: ScopeKey,
        retry_after: Option<Duration>,
        now: Instant,
    ) -> Duration {
        let mut st = self.lock();
        let prev = st.cooldowns.get(&scope).map_or(0, |c| c.step);
        let (len, step) = match retry_after {
            Some(d) => (d, prev),
            None => (backoff(prev), prev.saturating_add(1)),
        };
        st.cooldowns.insert(
            scope,
            Cooldown {
                until: now + len,
                step,
            },
        );
        len
    }

    /// A connect error or first-byte timeout, counted per provider. Returns true when this trips the breaker.
    pub fn record_server_failure(&self, provider: &str, now: Instant) -> bool {
        let mut st = self.lock();
        let b = st.breakers.entry(provider.to_string()).or_default();
        b.consecutive = b.consecutive.saturating_add(1);
        if b.consecutive >= BREAKER_THRESHOLD {
            b.consecutive = 0;
            b.open_until = Some(now + BREAKER_OPEN);
            true
        } else {
            false
        }
    }

    /// Disable a key until restart. Returns true when newly disabled.
    pub fn disable_key(&self, provider: &str, key: usize) -> bool {
        self.lock().disabled.insert((provider.to_string(), key))
    }

    /// Mark a pool route's model missing until restart. Returns true when new.
    pub fn mark_missing(&self, provider: &str, model: &str) -> bool {
        self.lock()
            .missing
            .insert((provider.to_string(), model.to_string()))
    }

    /// Mark a route unavailable (403: region or plan) until restart.
    /// Returns true when new.
    pub fn mark_unavailable(&self, provider: &str, model: &str) -> bool {
        self.lock()
            .unavailable
            .insert((provider.to_string(), model.to_string()))
    }

    pub fn set_paused(&self, provider: &str, paused: bool) {
        let mut st = self.lock();
        if paused {
            st.paused.insert(provider.to_string());
        } else {
            st.paused.remove(provider);
        }
    }

    /// Clear cooldowns and open breakers, for one provider or all. Returns
    /// how many were cleared.
    pub fn clear_cooldowns(&self, provider: Option<&str>) -> usize {
        let mut st = self.lock();
        let mut n = 0;
        st.cooldowns.retain(|k, _| {
            let hit = provider.is_none_or(|p| k.provider == p);
            n += usize::from(hit);
            !hit
        });
        for (id, b) in st.breakers.iter_mut() {
            if provider.is_none_or(|p| id == p) {
                n += usize::from(b.open_until.is_some());
                *b = Breaker::default();
            }
        }
        for ((id, _), b) in st.route_breakers.iter_mut() {
            if provider.is_none_or(|p| id == p) {
                n += usize::from(b.open_until.is_some());
                *b = Breaker::default();
            }
        }
        n
    }

    /// Learn that `model` refused a request estimated at `estimate` tokens.
    /// Keeps the lower of the old and new value. Returns the new ceiling
    /// when it was learned or lowered.
    pub fn record_ceiling(&self, provider: &str, model: &str, estimate: u64) -> Option<u64> {
        if estimate == 0 {
            return None;
        }
        let mut st = self.lock();
        let slot = st
            .ceilings
            .entry((provider.to_string(), model.to_string()))
            .or_insert(u64::MAX);
        if estimate < *slot {
            *slot = estimate;
            Some(estimate)
        } else {
            None
        }
    }

    /// Forget learned ceilings, for one provider or all. Returns how many.
    pub fn clear_ceilings(&self, provider: Option<&str>) -> usize {
        let mut st = self.lock();
        let before = st.ceilings.len();
        st.ceilings
            .retain(|(p, _), _| provider.is_some_and(|id| id != p));
        before - st.ceilings.len()
    }

    /// Read-only state of one provider for `/status` and `/events`.
    pub fn provider_view(&self, provider: &Provider, now: Instant) -> ProviderView {
        let st = self.lock();
        let id = provider.id.as_str();
        let mut view = ProviderView {
            paused: st.paused.contains(id),
            breaker_until: st
                .breakers
                .get(id)
                .and_then(|b| b.open_until)
                .filter(|u| *u > now),
            ..ProviderView::default()
        };
        view.cooldowns = st
            .cooldowns
            .iter()
            .filter(|(k, c)| k.provider == id && c.until > now)
            .map(|(k, c)| (k.clone(), c.until))
            .collect();
        view.cooldowns.sort_by_key(|(_, until)| *until);
        view.disabled_keys = st
            .disabled
            .iter()
            .filter(|(p, _)| p == id)
            .map(|(_, k)| *k)
            .collect();
        view.disabled_keys.sort_unstable();
        view.missing = st
            .missing
            .iter()
            .filter(|(p, _)| p == id)
            .map(|(_, m)| m.clone())
            .collect();
        view.missing.sort();
        view.unavailable = st
            .unavailable
            .iter()
            .filter(|(p, _)| p == id)
            .map(|(_, m)| m.clone())
            .collect();
        view.unavailable.sort();
        view.route_breakers = st
            .route_breakers
            .iter()
            .filter(|((p, _), _)| p == id)
            .filter_map(|((_, m), b)| b.open_until.filter(|u| *u > now).map(|u| (m.clone(), u)))
            .collect();
        view.route_breakers.sort();
        view.ceilings = st
            .ceilings
            .iter()
            .filter(|((p, _), _)| p == id)
            .map(|((_, m), c)| (m.clone(), *c))
            .collect();
        view.ceilings.sort();
        let day = day_index(st.unix(now), provider.reset_utc_hour);
        // Only the buckets of the provider's own scope, so route buckets
        // that exist for per-model limits are not counted twice.
        for (k, b) in st.buckets.iter().filter(|(k, _)| k.provider == id) {
            let primary = match provider.limits.scope {
                LimitScope::Provider => k.key.is_none(),
                LimitScope::Key => k.key.is_some() && k.model.is_none(),
                LimitScope::KeyModel => k.key.is_some() && k.model.is_some(),
            };
            if !primary {
                continue;
            }
            view.requests_minute += window_sum(&b.win_req, now);
            view.tokens_minute += window_sum(&b.win_tok, now);
            if b.day == day {
                view.requests_day += b.day_req;
                view.tokens_day += b.day_tok;
            }
        }
        view
    }

    /// Everything worth persisting. Disabled keys, missing routes, pauses and
    /// breakers are deliberately not included (they last until restart or
    /// are short). Cooldowns of 60 s or less are not worth keeping.
    pub fn snapshot(&self, now: Instant) -> Snapshot {
        let st = self.lock();
        let unix = st.unix(now);
        Snapshot {
            days: st
                .buckets
                .iter()
                .filter(|(_, b)| b.day_req > 0 || b.day_tok > 0)
                .map(|(k, b)| DaySnap {
                    scope: k.clone(),
                    day: b.day,
                    requests: b.day_req,
                    tokens: b.day_tok,
                })
                .collect(),
            hourly: st
                .hourly
                .iter()
                .flat_map(|(p, h)| h.iter().map(|(k, (i, o))| (p.clone(), *k, *i, *o)))
                .collect(),
            cooldowns: st
                .cooldowns
                .iter()
                .filter(|(_, c)| c.until > now + PERSIST_COOLDOWN_MIN)
                .map(|(k, c)| CooldownSnap {
                    scope: k.clone(),
                    until_unix: unix + c.until.saturating_duration_since(now).as_secs(),
                    step: c.step,
                })
                .collect(),
            ceilings: st
                .ceilings
                .iter()
                .map(|((p, m), c)| (p.clone(), m.clone(), *c))
                .collect(),
            spend: st.spend,
            provider_spend: st
                .provider_spend
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
            credit: st.credit.iter().map(|(k, v)| (k.clone(), *v)).collect(),
        }
    }

    /// Load a snapshot, discarding anything past its reset or expiry.
    /// `reset_hour` gives each provider's day boundary; scopes of unknown
    /// providers are dropped.
    /// `month_start` maps (provider id, unix) to the current monthly period start,
    /// `None` for an unknown provider. Used to restore credit spend.
    pub fn restore(
        &self,
        snap: Snapshot,
        reset_hour: &dyn Fn(&str) -> Option<u8>,
        month_start: &dyn Fn(&str, u64) -> Option<u64>,
        model_known: &dyn Fn(&str, &str) -> bool,
        now: Instant,
    ) {
        let mut st = self.lock();
        let unix = st.unix(now);
        for d in snap.days {
            let Some(hour) = reset_hour(&d.scope.provider) else {
                continue;
            };
            if d.day != day_index(unix, hour) {
                continue;
            }
            let b = st.buckets.entry(d.scope).or_default();
            b.day = d.day;
            b.day_req = d.requests;
            b.day_tok = d.tokens;
        }
        let cur_hour = unix / 3600;
        for (p, hour, i, o) in snap.hourly {
            if reset_hour(&p).is_none() || hour + HOURLY_BUCKETS <= cur_hour || hour > cur_hour {
                continue;
            }
            st.hourly.entry(p).or_default().insert(hour, (i, o));
        }
        for c in snap.cooldowns {
            if reset_hour(&c.scope.provider).is_none() || c.until_unix <= unix {
                continue;
            }
            let until = now + Duration::from_secs(c.until_unix - unix);
            st.cooldowns.insert(
                c.scope,
                Cooldown {
                    until,
                    step: c.step,
                },
            );
        }
        for (p, m, c) in snap.ceilings {
            if c > 0 && model_known(&p, &m) {
                st.ceilings.insert((p, m), c);
            }
        }
        if snap.spend.day == unix / 86_400 {
            st.spend = snap.spend;
        }
        // provider_spend: keep entries whose provider is known and whose day is today
        for (p, ps) in snap.provider_spend {
            if reset_hour(&p).is_none() {
                continue;
            }
            if ps.day != unix / 86_400 {
                continue;
            }
            let entry = st.provider_spend.entry(p).or_default();
            entry.day = ps.day;
            entry.usd = entry.usd.max(ps.usd);
        }
        // credit: keep entries only when month_start(id, unix) == Some(entry.period_start)
        // Merge as usd = max(existing, snapshot), which SL-07 relies on.
        for (p, cs) in snap.credit {
            let Some(current_start) = month_start(&p, unix) else {
                continue;
            };
            if cs.period_start != current_start {
                continue;
            }
            let entry = st.credit.entry(p).or_default();
            entry.period_start = cs.period_start;
            entry.usd = entry.usd.max(cs.usd);
        }
    }
}

/// Parse a `Retry-After` value: delta-seconds (fractions accepted) or an
/// IMF-fixdate HTTP-date. The result is rounded up to whole seconds, is at
/// least one second and at most 24 h, so sub-second hints still back off.
pub fn parse_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let v = value.trim();
    let secs = if !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        v.parse::<f64>().ok().filter(|s| s.is_finite())?
    } else {
        let at = parse_http_date(v)?;
        match at.duration_since(now) {
            Ok(d) => d.as_secs_f64(),
            Err(_) => 0.0,
        }
    };
    let whole = secs.clamp(0.0, MAX_RETRY_AFTER.as_secs_f64()).ceil() as u64;
    Some(Duration::from_secs(whole.max(1)))
}

/// Parse an OpenAI/Groq style duration (`7.66s`, `2m59.56s`, `292ms`,
/// `1h2m3s`; a bare number means seconds). Rounded up to whole milliseconds.
pub fn parse_duration_hint(value: &str) -> Option<Duration> {
    let v = value.trim();
    if v.is_empty() {
        return None;
    }
    let mut ms = 0.0_f64;
    let mut rest = v;
    while !rest.is_empty() {
        let num_len = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        let (num, tail) = rest.split_at(num_len);
        let n: f64 = num.parse().ok().filter(|n: &f64| n.is_finite())?;
        let unit_len = tail
            .find(|c: char| !c.is_ascii_alphabetic())
            .unwrap_or(tail.len());
        let (unit, next) = tail.split_at(unit_len);
        let scale = match unit {
            "ms" => 1.0,
            "" | "s" => 1000.0,
            "m" => 60_000.0,
            "h" => 3_600_000.0,
            _ => return None,
        };
        ms += n * scale;
        rest = next;
    }
    Some(Duration::from_millis(ms.ceil() as u64))
}

/// Cooldown from `x-ratelimit-reset-*` headers. Window kinds whose
/// remaining count is 0 take precedence; otherwise the larger present value.
pub fn reset_hint(
    reset_requests: Option<&str>,
    reset_tokens: Option<&str>,
    remaining_requests: Option<u64>,
    remaining_tokens: Option<u64>,
) -> Option<Duration> {
    let req = reset_requests.and_then(parse_duration_hint);
    let tok = reset_tokens.and_then(parse_duration_hint);
    let exhausted = [
        req.filter(|_| remaining_requests == Some(0)),
        tok.filter(|_| remaining_tokens == Some(0)),
    ];
    exhausted
        .into_iter()
        .flatten()
        .max()
        .or_else(|| req.into_iter().chain(tok).max())
}

/// The duration in "try again in 7.66s" inside an error body.
pub fn body_hint(body: &[u8]) -> Option<Duration> {
    const NEEDLE: &str = "try again in ";
    let text = String::from_utf8_lossy(body.get(..body.len().min(16 * 1024)).unwrap_or_default())
        .to_ascii_lowercase();
    let at = text.find(NEEDLE)? + NEEDLE.len();
    let token = text.get(at..)?.split_whitespace().next()?;
    let token = token.trim_end_matches(|c: char| !c.is_ascii_alphanumeric());
    parse_duration_hint(token)
}

/// Clamp a precise hint to between 1 s and 24 h.
pub fn clamp_hint(d: Duration) -> Duration {
    d.clamp(Duration::from_secs(1), MAX_RETRY_AFTER)
}

/// `Sun, 06 Nov 1994 08:49:37 GMT`.
fn parse_http_date(v: &str) -> Option<SystemTime> {
    let parts: Vec<&str> = v.split_whitespace().collect();
    let [_, day, month, year, time, tz] = parts.as_slice() else {
        return None;
    };
    if !tz.eq_ignore_ascii_case("GMT") {
        return None;
    }
    let day: i64 = day.parse().ok()?;
    let year: i64 = year.parse().ok()?;
    let month = [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ]
    .iter()
    .position(|m| month.eq_ignore_ascii_case(m))? as i64
        + 1;
    let mut t = time.split(':');
    let h: i64 = t.next()?.parse().ok()?;
    let m: i64 = t.next()?.parse().ok()?;
    let s: i64 = t.next()?.parse().ok()?;
    if t.next().is_some() || !(1..=31).contains(&day) || h > 23 || m > 59 || s > 60 {
        return None;
    }
    // Days since 1970-01-01 (Hinnant's days-from-civil).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = u64::try_from(days * 86_400 + h * 3600 + m * 60 + s).ok()?;
    UNIX_EPOCH.checked_add(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn cfg(extra: &str) -> Config {
        let text = format!(
            "[[provider]]\nid = \"p\"\nbase_url = \"http://p/v1\"\nkeys_env = \"K\"\nbilling = \"free\"\n{extra}\n"
        );
        Config::from_toml_str(&text, &|_| Some("key-aaaa-0001,key-aaaa-0002".to_string()))
            .unwrap_or_else(|e| panic!("{e}"))
    }

    fn skip(
        l: &Limiter,
        c: &Config,
        key: usize,
        model: &str,
        est: u64,
        now: Instant,
    ) -> Option<Skip> {
        l.skip_reason(&c.providers[0], key, model, est, now)
    }

    fn rate_limit(l: &Limiter, retry: Option<Duration>, now: Instant) -> u64 {
        l.record_rate_limit(ScopeKey::route("p", 0, "m"), retry, now)
            .as_secs()
    }

    fn hourly_io(l: &Limiter, now: Instant) -> Vec<(u64, u64)> {
        l.hourly("p", now).iter().map(|h| (h.1, h.2)).collect()
    }

    #[test]
    fn t5_backoff_doubles_from_15s_caps_at_15min_and_resets_on_success() {
        let expected = [15, 30, 60, 120, 240, 480, 900, 900, 900];
        for (step, secs) in expected.iter().enumerate() {
            assert_eq!(backoff(step as u32), Duration::from_secs(*secs));
        }
        assert_eq!(backoff(u32::MAX), BACKOFF_CAP);

        let c = cfg("[provider.limits]\nscope = \"key_model\"");
        let l = Limiter::new();
        let now = Instant::now();
        let lens: Vec<u64> = (0..4).map(|_| rate_limit(&l, None, now)).collect();
        assert_eq!(lens, [15, 30, 60, 120]);
        assert_eq!(
            skip(&l, &c, 0, "m", 0, now),
            Some(Skip::Cooldown {
                until: now + Duration::from_secs(120)
            })
        );
        // Still cooling just before expiry, free right after.
        assert!(skip(&l, &c, 0, "m", 0, now + Duration::from_secs(119)).is_some());
        assert!(skip(&l, &c, 0, "m", 0, now + Duration::from_secs(120)).is_none());

        l.record_success(&ScopeKey::route("p", 0, "m"), "m");
        assert!(skip(&l, &c, 0, "m", 0, now).is_none());
        assert_eq!(rate_limit(&l, None, now), 15);
    }

    #[test]
    fn cooldowns_apply_at_the_providers_limit_scope() {
        let now = Instant::now();
        let ra = Some(Duration::from_secs(30));
        // (scope, other model on key 0 also cooled, key 1 also cooled)
        for (scope, other_model, other_key) in [
            ("provider", true, true),
            ("key", true, false),
            ("key_model", false, false),
        ] {
            let c = cfg(&format!("[provider.limits]\nscope = \"{scope}\""));
            let l = Limiter::new();
            l.record_rate_limit(scope_key(&c.providers[0], 0, "m"), ra, now);
            assert!(skip(&l, &c, 0, "m", 0, now).is_some(), "{scope}");
            assert_eq!(
                skip(&l, &c, 0, "other", 0, now).is_some(),
                other_model,
                "{scope}"
            );
            assert_eq!(skip(&l, &c, 1, "m", 0, now).is_some(), other_key, "{scope}");
        }
    }

    #[test]
    fn retry_after_is_used_verbatim_and_does_not_advance_backoff() {
        let l = Limiter::new();
        let now = Instant::now();
        assert_eq!(rate_limit(&l, Some(Duration::from_secs(30)), now), 30);
        assert_eq!(rate_limit(&l, None, now), 15);
    }

    #[test]
    fn breaker_opens_after_three_consecutive_failures_for_60s() {
        let c = cfg("");
        let l = Limiter::new();
        let now = Instant::now();
        assert!(!l.record_server_failure("p", now));
        assert!(!l.record_server_failure("p", now));
        assert!(l.record_server_failure("p", now));
        let open = Skip::BreakerOpen {
            until: now + BREAKER_OPEN,
        };
        assert_eq!(skip(&l, &c, 0, "m", 0, now), Some(open));
        assert!(skip(&l, &c, 0, "m", 0, now + BREAKER_OPEN).is_none());
        // A success in between resets the count.
        assert!(!l.record_server_failure("q", now));
        l.record_success(&ScopeKey::route("q", 0, "m"), "m");
        assert!(!l.record_server_failure("q", now));
        assert!(!l.record_server_failure("q", now));
    }

    #[test]
    fn disabled_missing_and_paused_are_sticky() {
        let c = cfg("");
        let l = Limiter::new();
        let now = Instant::now();
        assert!(l.disable_key("p", 0));
        assert!(!l.disable_key("p", 0));
        assert_eq!(skip(&l, &c, 0, "m", 0, now), Some(Skip::KeyDisabled));
        assert!(skip(&l, &c, 1, "m", 0, now).is_none());
        assert!(l.mark_missing("p", "m"));
        assert_eq!(skip(&l, &c, 1, "m", 0, now), Some(Skip::Missing));
        l.set_paused("p", true);
        assert_eq!(skip(&l, &c, 1, "x", 0, now), Some(Skip::Paused));
        l.set_paused("p", false);
        assert!(skip(&l, &c, 1, "x", 0, now).is_none());
    }

    #[test]
    fn rpm_window_blocks_until_the_oldest_request_ages_out() {
        let c = cfg("[provider.limits]\nrpm = 2");
        let p = &c.providers[0];
        let l = Limiter::new();
        let t0 = Instant::now();
        l.note_request(p, 0, "m", t0);
        l.note_request(p, 1, "m", t0 + Duration::from_secs(10));
        let now = t0 + Duration::from_secs(20);
        // provider scope: shared across keys and models.
        let want = Skip::Quota {
            limit: QuotaKind::Rpm,
            until: Some(t0 + WINDOW),
        };
        assert_eq!(skip(&l, &c, 0, "other", 0, now), Some(want));
        assert!(skip(&l, &c, 0, "m", 0, t0 + WINDOW).is_none());
    }

    #[test]
    fn tpm_uses_usage_plus_estimate_but_never_blocks_an_empty_window() {
        let c = cfg("[provider.limits]\ntpm = 1000");
        let p = &c.providers[0];
        let l = Limiter::new();
        let now = Instant::now();
        assert!(skip(&l, &c, 0, "m", 5000, now).is_none(), "empty counter");
        let usage = Usage {
            input: 500,
            output: 100,
        };
        l.record_usage(p, 0, "m", usage, RemainingHints::default(), now, None);
        assert!(skip(&l, &c, 0, "m", 400, now).is_none());
        let s = skip(&l, &c, 0, "m", 401, now);
        assert_eq!(
            s,
            Some(Skip::Quota {
                limit: QuotaKind::Tpm,
                until: Some(now + WINDOW)
            })
        );
        assert_eq!(hourly_io(&l, now), [(500, 100)]);
    }

    #[test]
    fn key_scope_is_per_key_and_model_limits_apply_in_addition() {
        let c = cfg(
            "[provider.limits]\nscope = \"key\"\nrpm = 10\n[provider.model_limits.\"m\"]\nrpm = 1",
        );
        let p = &c.providers[0];
        let l = Limiter::new();
        let now = Instant::now();
        l.note_request(p, 0, "m", now);
        assert!(matches!(
            skip(&l, &c, 0, "m", 0, now),
            Some(Skip::Quota { .. })
        ));
        assert!(skip(&l, &c, 1, "m", 0, now).is_none(), "other key");
        assert!(skip(&l, &c, 0, "x", 0, now).is_none(), "other model");
    }

    #[test]
    fn day_counters_reset_at_the_providers_reset_hour() {
        let c = cfg("reset_utc_hour = 6\n[provider.limits]\nrpd = 1");
        let p = &c.providers[0];
        let l = Limiter::new();
        let t0 = Instant::now();
        l.note_request(p, 0, "m", t0);
        let s = skip(&l, &c, 0, "m", 0, t0).unwrap();
        assert_eq!(s.name(), "quota_rpd");
        let until = s.until().unwrap();
        assert!(until > t0 && until <= t0 + Duration::from_secs(86_400));
        assert!(skip(&l, &c, 0, "m", 0, until).is_none(), "new day");
        assert_eq!(day_index(6 * 3600, 6), 0);
        assert_eq!(day_index(6 * 3600 - 1, 6), 0);
        assert_eq!(day_index(6 * 3600 + 86_400, 6), 1);
        assert_eq!(day_end(0, 6), 86_400 + 6 * 3600);
    }

    #[test]
    fn remaining_headers_overwrite_counters_where_they_map_cleanly() {
        let c = cfg("[provider.limits]\ntpm = 1000\nrpd = 100");
        let p = &c.providers[0];
        let l = Limiter::new();
        let now = Instant::now();
        l.note_request(p, 0, "m", now);
        let hints = RemainingHints {
            requests: Some(40),
            tokens: Some(300),
        };
        let usage = Usage {
            input: 1,
            output: 1,
        };
        l.record_usage(p, 0, "m", usage, hints, now, None);
        // Tokens used are now 700, so 301 more does not fit.
        assert_eq!(skip(&l, &c, 0, "m", 301, now).unwrap().name(), "quota_tpm");
        assert!(skip(&l, &c, 0, "m", 300, now).is_none());
        assert_eq!(l.snapshot(now).days[0].requests, 60);
    }

    #[test]
    fn snapshot_round_trip_keeps_day_hourly_long_cooldowns_and_spend() {
        let c = cfg("[provider.limits]\nrpd = 5");
        let p = &c.providers[0];
        let l = Limiter::new();
        let now = Instant::now();
        l.note_request(p, 0, "m", now);
        let usage = Usage {
            input: 7,
            output: 3,
        };
        l.record_usage(p, 0, "m", usage, RemainingHints::default(), now, None);
        let long = Some(Duration::from_secs(300));
        l.record_rate_limit(ScopeKey::route("p", 0, "m"), long, now);
        let short = Some(Duration::from_secs(30));
        l.record_rate_limit(ScopeKey::route("p", 1, "m"), short, now);
        let snap = l.snapshot(now);
        assert_eq!(snap.cooldowns.len(), 1, "only cooldowns longer than 60 s");

        let l2 = Limiter::new();
        let reset = |id: &str| (id == "p").then_some(0u8);
        let month_start =
            |id: &str, unix: u64| (id == "p").then(|| crate::billing::month_start(unix, 1, 0));
        l2.restore(snap, &reset, &month_start, &|_, _| true, now);
        assert_eq!(hourly_io(&l2, now), [(7, 3)]);
        assert_eq!(skip(&l2, &c, 0, "m", 0, now).unwrap().name(), "cooldown");
        assert!(skip(&l2, &c, 1, "m", 0, now).is_none());
        assert_eq!(l2.snapshot(now).days[0].requests, 1);
    }

    #[test]
    fn restore_discards_stale_and_unknown_entries() {
        let now = Instant::now();
        let l = Limiter::new();
        let unix = l.lock().unix(now);
        let snap = Snapshot {
            days: vec![
                DaySnap {
                    scope: ScopeKey::provider_wide("p"),
                    day: day_index(unix, 0) - 1,
                    requests: 9,
                    tokens: 9,
                },
                DaySnap {
                    scope: ScopeKey::provider_wide("gone"),
                    day: day_index(unix, 0),
                    requests: 9,
                    tokens: 9,
                },
            ],
            hourly: vec![("p".into(), unix / 3600 - 24, 1, 1)],
            cooldowns: vec![CooldownSnap {
                scope: ScopeKey::provider_wide("p"),
                until_unix: unix - 1,
                step: 3,
            }],
            ceilings: vec![("p".into(), "gone".into(), 100)],
            spend: Spend {
                day: unix / 86_400 - 1,
                usd: 5.0,
            },
            provider_spend: vec![],
            credit: vec![],
        };
        let reset = |id: &str| (id == "p").then_some(0u8);
        let month_start =
            |id: &str, unix: u64| (id == "p").then(|| crate::billing::month_start(unix, 1, 0));
        let model_known = |id: &str, model: &str| (id == "p") && (model != "gone");
        l.restore(snap, &reset, &month_start, &model_known, now);
        assert_eq!(l.snapshot(now), Snapshot::default());
        assert_eq!(l.spend_today(now), 0.0);
    }

    #[test]
    fn duration_hint_parsing() {
        let ms = |s: &str| parse_duration_hint(s).map(|d| d.as_millis());
        assert_eq!(ms("7.66s"), Some(7660));
        assert_eq!(ms("2m59.56s"), Some(179_560));
        assert_eq!(ms("292ms"), Some(292));
        assert_eq!(ms("292.5ms"), Some(293));
        assert_eq!(ms("1h2m3s"), Some(3_723_000));
        assert_eq!(ms("6m0s"), Some(360_000));
        assert_eq!(ms("2"), Some(2000));
        assert_eq!(ms(""), None);
        assert_eq!(ms("abc"), None);
        assert_eq!(ms("5x"), None);
        assert_eq!(
            body_hint(b"{\"message\":\"Rate limit. Please try again in 292.5ms. Visit\"}"),
            Some(Duration::from_millis(293))
        );
        assert_eq!(
            body_hint(b"Try again in 2m59.56s."),
            Some(Duration::from_millis(179_560))
        );
        assert_eq!(body_hint(b"nothing"), None);
        assert_eq!(
            reset_hint(Some("1s"), Some("9s"), Some(5), Some(0)),
            Some(Duration::from_secs(9))
        );
        assert_eq!(
            reset_hint(Some("1s"), Some("9s"), Some(0), Some(5)),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            reset_hint(Some("1s"), Some("9s"), None, None),
            Some(Duration::from_secs(9))
        );
        assert_eq!(
            clamp_hint(Duration::from_millis(292)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn retry_after_parsing() {
        let now = UNIX_EPOCH + Duration::from_secs(784_111_700);
        let secs = |s: &str| parse_retry_after(s, now).map(|d| d.as_secs());
        assert_eq!(secs("30"), Some(30));
        assert_eq!(secs("0.2925"), Some(1), "sub-second rounds up, never 0");
        assert_eq!(secs("0"), Some(1));
        assert_eq!(secs("1.2"), Some(2));
        // 784111777 = Sun, 06 Nov 1994 08:49:37 GMT
        assert_eq!(secs("Sun, 06 Nov 1994 08:49:37 GMT"), Some(77));
        assert_eq!(secs("Sun, 06 Nov 1994 08:00:00 GMT"), Some(1), "past date");
        assert_eq!(secs("999999999"), Some(86_400));
        assert_eq!(secs("soon"), None);
        assert_eq!(secs(""), None);
    }

    #[test]
    fn spend_is_tracked_for_every_provider_and_free_without_prices_costs_zero() {
        // Free provider with no prices: usage adds 0.0 to spend but still counts requests/day
        let c = cfg("[provider.limits]\nrpd = 100");
        let p = &c.providers[0];
        let l = Limiter::new();
        let now = Instant::now();
        let usage = Usage {
            input: 1000,
            output: 1000,
        };
        l.record_usage(p, 0, "m", usage, RemainingHints::default(), now, None);
        assert_eq!(l.provider_spend_today("p", now), 0.0);
        assert_eq!(l.total_spend_today(now), 0.0);
        // Request/day counters still work
        let s = skip(&l, &c, 0, "m", 0, now);
        assert!(s.is_none(), "day counter should not block yet");

        // Free provider with prices: adds positive amount
        let c2 = cfg(
            "[provider.limits]\nrpd = 100\n[provider.prices.\"m\"]\nin_per_mtok = 1.0\nout_per_mtok = 2.0",
        );
        let p2 = &c2.providers[0];
        let l2 = Limiter::new();
        l2.record_usage(p2, 0, "m", usage, RemainingHints::default(), now, None);
        // (1000 * 1.0 + 1000 * 2.0) / 1e6 = 0.003
        assert!((l2.provider_spend_today("p", now) - 0.003).abs() < 1e-9);
        assert!((l2.total_spend_today(now) - 0.003).abs() < 1e-9);
    }

    #[test]
    fn credit_spend_accumulates_per_period_and_rolls_over() {
        // reset_day = 1, so period starts at 1st of month 00:00 UTC
        // with_anchor at 2026-11-30 12:00 UTC
        let text = r#"
[[provider]]
id = "p"
base_url = "http://p/v1"
keys_env = "K"
billing = "credit"
credit_usd = 10.0
credit_reset_day = 1
reset_utc_hour = 0
[provider.prices."m"]
in_per_mtok = 1.0
out_per_mtok = 0.0
"#;
        let c = Config::from_toml_str(text, &|_| Some("key-aaaa-0001,key-aaaa-0002".to_string()))
            .unwrap();
        let p = &c.providers[0];
        // 2026-11-30 12:00 UTC
        let unix = 1796030400; // 2026-11-30 12:00:00 UTC
        let at = UNIX_EPOCH + Duration::from_secs(unix);
        let l = Limiter::with_anchor(Instant::now(), at);
        let now = Instant::now();

        // Record twice, each 1M input tokens = $1.00
        let usage = Usage {
            input: 1_000_000,
            output: 0,
        };
        l.record_usage(p, 0, "m", usage, RemainingHints::default(), now, None);
        l.record_usage(p, 0, "m", usage, RemainingHints::default(), now, None);
        assert!((l.credit_spent(p, now) - 2.0).abs() < 1e-9);

        // At 2026-12-01 00:00 UTC, credit_spent should be 0 (new period)
        let unix2 = 1796160000; // 2026-12-01 00:00:00 UTC
        let at2 = UNIX_EPOCH + Duration::from_secs(unix2);
        let l2 = Limiter::with_anchor(Instant::now(), at2);
        let now2 = Instant::now();
        // Same usage recorded in l2 would start fresh
        l2.record_usage(p, 0, "m", usage, RemainingHints::default(), now2, None);
        assert!((l2.credit_spent(p, now2) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn credit_spend_reaching_90_percent_makes_provider_ineligible() {
        // credit_usd = 10, in_per_mtok = 1.0
        // 8,990,000 input tokens = $8.99 (eligible), 10,000 more = $9.00 (exhausted)
        let text = r#"
[[provider]]
id = "p"
base_url = "http://p/v1"
keys_env = "K"
billing = "credit"
credit_usd = 10.0
credit_reset_day = 1
reset_utc_hour = 0
[provider.prices."m"]
in_per_mtok = 1.0
out_per_mtok = 0.0
"#;
        let c = Config::from_toml_str(text, &|_| Some("key-aaaa-0001".to_string())).unwrap();
        let p = &c.providers[0];
        let unix = 1796030400; // 2026-11-30 12:00 UTC
        let at = UNIX_EPOCH + Duration::from_secs(unix);
        let l = Limiter::with_anchor(Instant::now(), at);
        let now = Instant::now();

        // 8,990,000 input tokens = $8.99 (below 90% = $9.00)
        let usage1 = Usage {
            input: 8_990_000,
            output: 0,
        };
        let crossed1 = l.record_usage(p, 0, "m", usage1, RemainingHints::default(), now, None);
        assert!(!crossed1, "should not be exhausted yet");
        let skip = l.skip_reason(p, 0, "m", 0, now);
        assert!(
            skip.is_none(),
            "provider should still be eligible: {:?}",
            skip
        );

        // Another 10,000 input tokens = $0.01, total $9.00 (exactly 90%)
        let usage2 = Usage {
            input: 10_000,
            output: 0,
        };
        let crossed2 = l.record_usage(p, 0, "m", usage2, RemainingHints::default(), now, None);
        assert!(crossed2, "should be exhausted now");
        let skip = l.skip_reason(p, 0, "m", 0, now);
        assert!(matches!(skip, Some(Skip::CreditExhausted { .. })));
        assert_eq!(skip.unwrap().name(), "credit_exhausted");
    }

    #[test]
    fn credit_resets_on_the_reset_day() {
        // Case 1: reset_day = 1, at 2026-11-30 23:00 UTC
        let text1 = r#"
[[provider]]
id = "p"
base_url = "http://p/v1"
keys_env = "K"
billing = "credit"
credit_usd = 10.0
credit_reset_day = 1
reset_utc_hour = 0
[provider.prices."m"]
in_per_mtok = 1.0
out_per_mtok = 0.0
"#;
        let c1 = Config::from_toml_str(text1, &|_| Some("key-aaaa-0001".to_string())).unwrap();
        let p1 = &c1.providers[0];
        // 2026-11-30 23:00 UTC
        let unix1 = 1796079600; // 2026-11-30 23:00:00 UTC
        let at1 = UNIX_EPOCH + Duration::from_secs(unix1);
        let l1 = Limiter::with_anchor(Instant::now(), at1);
        let now1 = Instant::now();

        // Exhaust the credit: 10M tokens = $10.00
        let usage = Usage {
            input: 10_000_000,
            output: 0,
        };
        l1.record_usage(p1, 0, "m", usage, RemainingHints::default(), now1, None);
        let skip = l1.skip_reason(p1, 0, "m", 0, now1);
        // until should be now + 3600s (1 hour until 2026-12-01 00:00 UTC)
        if let Some(Skip::CreditExhausted { until }) = skip {
            let diff = until.saturating_duration_since(now1).as_secs();
            assert!((3500..=3700).contains(&diff), "until diff: {}", diff);
        } else {
            panic!("expected CreditExhausted skip, got {:?}", skip);
        }

        // At 2026-12-01 00:00 UTC, the provider should be eligible again (new period, 0 spend)
        let unix2 = 1796083200; // 2026-12-01 00:00:00 UTC
        let at2 = UNIX_EPOCH + Duration::from_secs(unix2);
        let l2 = Limiter::with_anchor(Instant::now(), at2);
        let now2 = Instant::now();
        // Don't record usage - just check eligibility with 0 spend
        let skip = l2.skip_reason(p1, 0, "m", 0, now2);
        assert!(
            skip.is_none(),
            "provider should be eligible after reset: {:?}",
            skip
        );

        // Case 2: reset_day = 31 at 2026-11-29, period resets at 2026-11-30 00:00 UTC
        let text2 = r#"
[[provider]]
id = "p"
base_url = "http://p/v1"
keys_env = "K"
billing = "credit"
credit_usd = 10.0
credit_reset_day = 31
reset_utc_hour = 0
[provider.prices."m"]
in_per_mtok = 1.0
out_per_mtok = 0.0
"#;
        let c2 = Config::from_toml_str(text2, &|_| Some("key-aaaa-0001".to_string())).unwrap();
        let p2 = &c2.providers[0];
        // 2026-11-29 12:00 UTC
        let unix3 = 1795953600; // 2026-11-29 12:00:00 UTC
        let at3 = UNIX_EPOCH + Duration::from_secs(unix3);
        let l3 = Limiter::with_anchor(Instant::now(), at3);
        let now3 = Instant::now();

        // Exhaust the credit
        l3.record_usage(p2, 0, "m", usage, RemainingHints::default(), now3, None);
        let skip = l3.skip_reason(p2, 0, "m", 0, now3);
        assert!(matches!(skip, Some(Skip::CreditExhausted { .. })));

        // At 2026-11-30 00:00 UTC, the provider should be eligible (November has 30 days)
        let unix4 = 1795996800; // 2026-11-30 00:00:00 UTC
        let at4 = UNIX_EPOCH + Duration::from_secs(unix4);
        let l4 = Limiter::with_anchor(Instant::now(), at4);
        let now4 = Instant::now();
        // Don't record usage - just check eligibility with 0 spend
        let skip = l4.skip_reason(p2, 0, "m", 0, now4);
        assert!(
            skip.is_none(),
            "provider should be eligible after reset: {:?}",
            skip
        );
    }
}
