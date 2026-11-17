//! Route state: cooldowns with exponential backoff, per-provider circuit
//! breakers, disabled keys, missing routes, pause flags and request counts.
//!
//! All methods take `&self` and lock one `std::sync::Mutex` for a short,
//! non-async critical section. Time is passed in (`now`) so tests never sleep.
//! M3 adds scoped quota counters here.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::catalog::Candidate;
use crate::config::Strategy;
use crate::select::{self, RouteView, Selection, Skip};

/// First 429 backoff when the upstream gives no `Retry-After`.
pub const BACKOFF_BASE: Duration = Duration::from_secs(15);
pub const BACKOFF_CAP: Duration = Duration::from_secs(15 * 60);
/// Consecutive retryable failures on one provider that open its breaker.
pub const BREAKER_THRESHOLD: u32 = 3;
pub const BREAKER_OPEN: Duration = Duration::from_secs(60);
/// Longest honoured `Retry-After`.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(24 * 3600);

/// Backoff after `step` earlier consecutive 429s: 15 s, 30 s, ... capped.
pub fn backoff(step: u32) -> Duration {
    let secs = BACKOFF_BASE.as_secs().saturating_mul(1u64 << step.min(20));
    Duration::from_secs(secs).min(BACKOFF_CAP)
}

/// Scope a cooldown applies to. `None` fields are wildcards, so M3 can cool
/// a whole key (`model: None`) or provider (`key: None, model: None`) by
/// building a wider key; lookups already check all three levels.
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

type RouteId = (String, usize, String);

#[derive(Debug, Default)]
pub struct State {
    cooldowns: HashMap<ScopeKey, Cooldown>,
    breakers: HashMap<String, Breaker>,
    disabled: HashSet<(String, usize)>,
    missing: HashSet<(String, String)>,
    paused: HashSet<String>,
    requests: HashMap<RouteId, u64>,
    pool_counters: HashMap<String, usize>,
    key_counters: HashMap<String, usize>,
}

impl RouteView for State {
    fn skip_reason(&self, provider: &str, key: usize, model: &str, now: Instant) -> Option<Skip> {
        if self.paused.contains(provider) {
            return Some(Skip::Paused);
        }
        if self.disabled.contains(&(provider.to_string(), key)) {
            return Some(Skip::KeyDisabled);
        }
        if self
            .missing
            .contains(&(provider.to_string(), model.to_string()))
        {
            return Some(Skip::Missing);
        }
        if let Some(until) = self
            .breakers
            .get(provider)
            .and_then(|b| b.open_until)
            .filter(|u| *u > now)
        {
            return Some(Skip::BreakerOpen { until });
        }
        [
            ScopeKey::provider_wide(provider),
            ScopeKey::key_wide(provider, key),
            ScopeKey::route(provider, key, model),
        ]
        .iter()
        .filter_map(|k| self.cooldowns.get(k))
        .map(|c| c.until)
        .filter(|u| *u > now)
        .max()
        .map(|until| Skip::Cooldown { until })
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

#[derive(Debug, Default)]
pub struct Limiter {
    inner: Mutex<State>,
}

impl Limiter {
    pub fn new() -> Self {
        Self::default()
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
        now: Instant,
    ) -> Selection<'a> {
        let mut st = self.lock();
        let (strategy, counter) = match pool {
            Some((name, s)) => (s, st.pool_counters.get(name).copied().unwrap_or(0)),
            None => (Strategy::Priority, 0),
        };
        let selection = select::select(candidates, strategy, counter, &*st, now);
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
        provider: &str,
        key: usize,
        model: &str,
        now: Instant,
    ) -> Option<Skip> {
        self.lock().skip_reason(provider, key, model, now)
    }

    /// Count a request sent upstream (feeds `least_used`).
    pub fn note_request(&self, provider: &str, key: usize, model: &str) {
        let mut st = self.lock();
        let c = st
            .requests
            .entry((provider.to_string(), key, model.to_string()))
            .or_insert(0);
        *c = c.saturating_add(1);
    }

    /// A 2xx response: reset the route's backoff and the provider's breaker count.
    pub fn record_success(&self, provider: &str, key: usize, model: &str) {
        let mut st = self.lock();
        st.cooldowns.remove(&ScopeKey::route(provider, key, model));
        if let Some(b) = st.breakers.get_mut(provider) {
            b.consecutive = 0;
        }
    }

    /// A 429: cool the route down. Uses `retry_after` when given, else the
    /// exponential backoff. Returns the cooldown length.
    pub fn record_rate_limit(
        &self,
        provider: &str,
        key: usize,
        model: &str,
        retry_after: Option<Duration>,
        now: Instant,
    ) -> Duration {
        let mut st = self.lock();
        let scope = ScopeKey::route(provider, key, model);
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

    /// A retryable server-side failure. Returns true when this trips the breaker.
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

    pub fn set_paused(&self, provider: &str, paused: bool) {
        let mut st = self.lock();
        if paused {
            st.paused.insert(provider.to_string());
        } else {
            st.paused.remove(provider);
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

    fn cooling(l: &Limiter, now: Instant) -> Option<Skip> {
        l.skip_reason("p", 0, "m", now)
    }

    #[test]
    fn t5_backoff_doubles_from_15s_caps_at_15min_and_resets_on_success() {
        let expected = [15, 30, 60, 120, 240, 480, 900, 900, 900];
        for (step, secs) in expected.iter().enumerate() {
            assert_eq!(backoff(step as u32), Duration::from_secs(*secs));
        }
        assert_eq!(backoff(u32::MAX), BACKOFF_CAP);

        let l = Limiter::new();
        let now = Instant::now();
        let lens: Vec<u64> = (0..4)
            .map(|_| l.record_rate_limit("p", 0, "m", None, now).as_secs())
            .collect();
        assert_eq!(lens, [15, 30, 60, 120]);
        assert_eq!(
            cooling(&l, now),
            Some(Skip::Cooldown {
                until: now + Duration::from_secs(120)
            })
        );
        // Still cooling just before expiry, free right after.
        assert!(cooling(&l, now + Duration::from_secs(119)).is_some());
        assert!(cooling(&l, now + Duration::from_secs(120)).is_none());

        l.record_success("p", 0, "m");
        assert!(cooling(&l, now).is_none());
        assert_eq!(l.record_rate_limit("p", 0, "m", None, now).as_secs(), 15);
    }

    #[test]
    fn retry_after_is_used_verbatim_and_does_not_advance_backoff() {
        let l = Limiter::new();
        let now = Instant::now();
        let ra = Duration::from_secs(30);
        assert_eq!(l.record_rate_limit("p", 0, "m", Some(ra), now), ra);
        assert_eq!(cooling(&l, now), Some(Skip::Cooldown { until: now + ra }));
        // Other keys and models are untouched at route scope.
        assert!(l.skip_reason("p", 1, "m", now).is_none());
        assert!(l.skip_reason("p", 0, "other", now).is_none());
    }

    #[test]
    fn breaker_opens_after_three_consecutive_failures_for_60s() {
        let l = Limiter::new();
        let now = Instant::now();
        assert!(!l.record_server_failure("p", now));
        assert!(!l.record_server_failure("p", now));
        assert!(l.record_server_failure("p", now));
        let open = Skip::BreakerOpen {
            until: now + BREAKER_OPEN,
        };
        assert_eq!(cooling(&l, now), Some(open));
        assert!(cooling(&l, now + BREAKER_OPEN).is_none());
        // A success in between resets the count.
        assert!(!l.record_server_failure("q", now));
        l.record_success("q", 0, "m");
        assert!(!l.record_server_failure("q", now));
        assert!(!l.record_server_failure("q", now));
    }

    #[test]
    fn disabled_missing_and_paused_are_sticky() {
        let l = Limiter::new();
        let now = Instant::now();
        assert!(l.disable_key("p", 0));
        assert!(!l.disable_key("p", 0));
        assert_eq!(cooling(&l, now), Some(Skip::KeyDisabled));
        assert!(l.skip_reason("p", 1, "m", now).is_none());
        assert!(l.mark_missing("p", "m"));
        assert_eq!(l.skip_reason("p", 1, "m", now), Some(Skip::Missing));
        l.set_paused("p", true);
        assert_eq!(l.skip_reason("p", 1, "x", now), Some(Skip::Paused));
        l.set_paused("p", false);
        assert!(l.skip_reason("p", 1, "x", now).is_none());
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
}
