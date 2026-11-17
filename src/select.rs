//! Candidate selection: expand resolved routes into `(provider, key, model)`
//! entries, drop the ones that cannot serve right now, and order the rest by
//! the pool's strategy. Pure: all state arrives through [`RouteView`].

use std::time::Instant;

use crate::catalog::Candidate;
use crate::config::{Provider, Strategy};

/// One attempt target: a provider key and the upstream model to send it.
#[derive(Debug, Clone, Copy)]
pub struct Entry<'a> {
    pub provider: &'a Provider,
    pub key_idx: usize,
    pub model: &'a str,
}

/// Why an entry cannot be used right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Skip {
    Paused,
    KeyDisabled,
    Missing,
    BreakerOpen { until: Instant },
    Cooldown { until: Instant },
}

impl Skip {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Paused => "paused",
            Self::KeyDisabled => "key_disabled",
            Self::Missing => "missing",
            Self::BreakerOpen { .. } => "breaker_open",
            Self::Cooldown { .. } => "cooldown",
        }
    }

    /// When the skip ends by itself, if it does.
    pub fn until(&self) -> Option<Instant> {
        match self {
            Self::BreakerOpen { until } | Self::Cooldown { until } => Some(*until),
            _ => None,
        }
    }
}

/// A filtered-out entry, for the 503 body.
#[derive(Debug, Clone)]
pub struct Skipped {
    pub route: String,
    /// Redacted key (last 4 chars), only when the provider has several keys.
    pub key: Option<String>,
    pub reason: Skip,
}

#[derive(Debug, Default)]
pub struct Selection<'a> {
    pub eligible: Vec<Entry<'a>>,
    pub skipped: Vec<Skipped>,
}

/// Read-only view of the route state the selection depends on.
pub trait RouteView {
    fn skip_reason(
        &self,
        provider: &str,
        key_idx: usize,
        model: &str,
        now: Instant,
    ) -> Option<Skip>;
    fn request_count(&self, provider: &str, key_idx: usize, model: &str) -> u64;
    /// Counter used to rotate which of a provider's keys is tried first.
    fn key_rotation(&self, provider: &str) -> usize;
}

pub fn select<'a>(
    candidates: &[Candidate<'a>],
    strategy: Strategy,
    pool_counter: usize,
    view: &impl RouteView,
    now: Instant,
) -> Selection<'a> {
    let mut out = Selection::default();
    for c in candidates {
        let n = c.provider.keys.len();
        if n == 0 {
            continue;
        }
        let start = view.key_rotation(&c.provider.id) % n;
        for i in 0..n {
            let key_idx = (start + i) % n;
            match view.skip_reason(&c.provider.id, key_idx, c.model, now) {
                Some(reason) => out.skipped.push(Skipped {
                    route: format!("{}/{}", c.provider.id, c.model),
                    key: (n > 1)
                        .then(|| c.provider.keys.get(key_idx).map(|k| k.redacted()))
                        .flatten(),
                    reason,
                }),
                None => out.eligible.push(Entry {
                    provider: c.provider,
                    key_idx,
                    model: c.model,
                }),
            }
        }
    }
    match strategy {
        Strategy::Priority => {}
        Strategy::RoundRobin => {
            if !out.eligible.is_empty() {
                let r = pool_counter % out.eligible.len();
                out.eligible.rotate_left(r);
            }
        }
        // Stable sort: ties keep configured order.
        Strategy::LeastUsed => out
            .eligible
            .sort_by_key(|e| view.request_count(&e.provider.id, e.key_idx, e.model)),
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::collections::HashMap;
    use std::time::Duration;

    #[derive(Default)]
    struct Fake {
        skips: HashMap<(String, usize, String), Skip>,
        counts: HashMap<(String, usize, String), u64>,
        rotation: usize,
    }

    impl RouteView for Fake {
        fn skip_reason(&self, p: &str, k: usize, m: &str, _: Instant) -> Option<Skip> {
            self.skips.get(&(p.into(), k, m.into())).copied()
        }
        fn request_count(&self, p: &str, k: usize, m: &str) -> u64 {
            self.counts
                .get(&(p.into(), k, m.into()))
                .copied()
                .unwrap_or(0)
        }
        fn key_rotation(&self, _: &str) -> usize {
            self.rotation
        }
    }

    fn config() -> Config {
        let text = r#"
[[provider]]
id = "a"
base_url = "http://a/v1"
keys_env = "A_KEYS"
[[provider]]
id = "b"
base_url = "http://b/v1"
keys_env = "B_KEYS"
[[pool]]
name = "p"
routes = ["a/m1", "b/m2"]
"#;
        Config::from_toml_str(text, &|k| {
            Some(if k == "A_KEYS" {
                "key-aaaa-0001,key-aaaa-0002".to_string()
            } else {
                "key-bbbb-0001".to_string()
            })
        })
        .unwrap()
    }

    fn cands(c: &Config) -> Vec<Candidate<'_>> {
        c.pool("p")
            .unwrap()
            .routes
            .iter()
            .map(|r| Candidate {
                provider: c.provider(&r.provider).unwrap(),
                model: r.model.as_str(),
            })
            .collect()
    }

    fn ids(s: &Selection<'_>) -> Vec<String> {
        s.eligible
            .iter()
            .map(|e| format!("{}{}/{}", e.provider.id, e.key_idx, e.model))
            .collect()
    }

    #[test]
    fn priority_keeps_order_and_rotates_keys() {
        let cfg = config();
        let now = Instant::now();
        let s = select(&cands(&cfg), Strategy::Priority, 0, &Fake::default(), now);
        assert_eq!(ids(&s), ["a0/m1", "a1/m1", "b0/m2"]);
        let fake = Fake {
            rotation: 1,
            ..Fake::default()
        };
        let s = select(&cands(&cfg), Strategy::Priority, 0, &fake, now);
        assert_eq!(ids(&s), ["a1/m1", "a0/m1", "b0/m2"]);
    }

    #[test]
    fn round_robin_rotates_over_eligible() {
        let cfg = config();
        let now = Instant::now();
        let s = select(&cands(&cfg), Strategy::RoundRobin, 1, &Fake::default(), now);
        assert_eq!(ids(&s), ["a1/m1", "b0/m2", "a0/m1"]);
    }

    #[test]
    fn least_used_orders_by_count_with_stable_ties() {
        let cfg = config();
        let mut fake = Fake::default();
        fake.counts.insert(("a".into(), 0, "m1".into()), 5);
        fake.counts.insert(("b".into(), 0, "m2".into()), 1);
        let s = select(&cands(&cfg), Strategy::LeastUsed, 0, &fake, Instant::now());
        assert_eq!(ids(&s), ["a1/m1", "b0/m2", "a0/m1"]);
    }

    #[test]
    fn filtered_entries_carry_reasons_and_redacted_keys() {
        let cfg = config();
        let now = Instant::now();
        let until = now + Duration::from_secs(9);
        let mut fake = Fake::default();
        fake.skips
            .insert(("a".into(), 0, "m1".into()), Skip::Cooldown { until });
        fake.skips
            .insert(("b".into(), 0, "m2".into()), Skip::Missing);
        let s = select(&cands(&cfg), Strategy::Priority, 0, &fake, now);
        assert_eq!(ids(&s), ["a1/m1"]);
        assert_eq!(s.skipped.len(), 2);
        assert_eq!(s.skipped[0].route, "a/m1");
        assert_eq!(s.skipped[0].key.as_deref(), Some("…0001"));
        assert_eq!(s.skipped[0].reason.until(), Some(until));
        assert_eq!(s.skipped[1].key, None);
        assert_eq!(s.skipped[1].reason.name(), "missing");
    }
}
