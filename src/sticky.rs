//! Session stickiness for pool requests.
//!
//! Once a pool request from a session is served by a route, later requests
//! from that session try that route first while it is eligible. If it is not
//! eligible, the normal order applies and the session re-sticks to whichever
//! route serves it. Stickiness never bypasses eligibility and does not apply
//! to explicit routes.
//!
//! Session key (computed in `proxy.rs`):
//! 1. The first non-empty header from config `session_headers` (default
//!    `["x-session-id", "x-opencode-session"]`): a hash of ("h", requested
//!    model, header value). OpenCode's own gateway requires
//!    `x-opencode-session`; whether the CLI sends it to custom
//!    OpenAI-compatible providers is unverified, hence the configurable list.
//! 2. Otherwise a hash of ("d", requested model, the first 4096 chars of the
//!    JSON text of the first `system` or `developer` message, the first 4096
//!    chars of the JSON text of the first `user` message). No key when there
//!    is no user message.
//!
//! Only the `u64` hash is kept, in memory. Message text and header values are
//! never stored or logged. `std::hash::DefaultHasher` is deterministic within
//! one build, which is all an in-memory map needs.
//!
//! Entries expire `sticky_ttl_secs` after they were last set (0 disables
//! stickiness), and at most [`MAX_STICKY_SESSIONS`] are kept.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// Most sessions remembered at once.
pub const MAX_STICKY_SESSIONS: usize = 4096;

/// Session key -> (provider id, upstream model, when it was last set).
#[derive(Debug)]
pub struct Sticky {
    inner: Mutex<HashMap<u64, (String, String, Instant)>>,
    ttl: Duration,
}

impl Sticky {
    pub fn new(ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<u64, (String, String, Instant)>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn expired(&self, at: Instant, now: Instant) -> bool {
        self.ttl.is_zero() || now.saturating_duration_since(at) > self.ttl
    }

    /// The route a session is stuck to, if any and not expired. An expired
    /// entry is removed.
    pub fn get(&self, key: u64, now: Instant) -> Option<(String, String)> {
        let mut map = self.lock();
        let (provider, model, at) = map.get(&key)?;
        if self.expired(*at, now) {
            map.remove(&key);
            return None;
        }
        Some((provider.clone(), model.clone()))
    }

    /// Stick a session to a route (overwrites). When the map is over the cap,
    /// expired entries are dropped first, then the oldest ones.
    pub fn set(&self, key: u64, provider: &str, model: &str, now: Instant) {
        if self.ttl.is_zero() {
            return;
        }
        let mut map = self.lock();
        map.insert(key, (provider.to_string(), model.to_string(), now));
        if map.len() > MAX_STICKY_SESSIONS {
            map.retain(|_, (_, _, at)| !self.expired(*at, now));
        }
        while map.len() > MAX_STICKY_SESSIONS {
            let Some(oldest) = map
                .iter()
                .min_by_key(|(_, (_, _, at))| *at)
                .map(|(k, _)| *k)
            else {
                break;
            };
            map.remove(&oldest);
        }
    }

    /// Unexpired sessions.
    pub fn len(&self, now: Instant) -> usize {
        self.lock()
            .values()
            .filter(|(_, _, at)| !self.expired(*at, now))
            .count()
    }

    pub fn is_empty(&self, now: Instant) -> bool {
        self.len(now) == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expires_after_ttl_and_is_bounded() {
        let now = Instant::now();
        let s = Sticky::new(Duration::from_secs(60));
        s.set(1, "a", "m", now);
        assert_eq!(s.get(1, now), Some(("a".to_string(), "m".to_string())));
        assert_eq!(s.get(1, now + Duration::from_secs(61)), None);
        assert_eq!(s.get(1, now), None, "the expired entry was removed");

        let max = MAX_STICKY_SESSIONS as u64;
        for k in 0..max + 10 {
            s.set(k, "a", "m", now + Duration::from_millis(k));
        }
        let later = now + Duration::from_millis(max + 10);
        assert!(s.len(later) <= MAX_STICKY_SESSIONS);
        assert!(s.get(max + 9, later).is_some(), "the newest key survives");
        assert!(s.get(0, later).is_none(), "the oldest key was evicted");

        let off = Sticky::new(Duration::ZERO);
        off.set(1, "a", "m", now);
        assert_eq!(off.get(1, now), None);
        assert_eq!(off.len(now), 0);
    }
}
