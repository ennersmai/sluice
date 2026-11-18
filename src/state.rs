//! Persistence: day counters, hourly token buckets, cooldowns longer than
//! 60 s and paid spend are written to `state_file` as JSON (write to a
//! temporary file, then rename over the target) and loaded at startup.
//!
//! The file holds counters and timestamps only: no keys, prompts or
//! completions. Deadlines are stored as wall-clock Unix seconds and converted
//! back to `Instant`s on load. Keys are referred to by their index in the
//! provider's `*_KEYS` list, so reordering keys between runs misattributes
//! their counters until the next reset.

use std::fs;
use std::io;
use std::path::Path;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::limiter::{CooldownSnap, CreditSpend, DaySnap, MonthSnap, ScopeKey, Snapshot, Spend};

const VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct File {
    version: u32,
    days: Vec<Day>,
    hourly: Vec<Hour>,
    cooldowns: Vec<Cooldown>,
    /// Learned size ceilings; absent in files written before they existed.
    #[serde(default)]
    ceilings: Vec<Ceiling>,
    spend: PaidSpend,
    /// Per-provider UTC day spend.
    #[serde(default)]
    provider_spend: Vec<ProviderSpend>,
    /// Per-provider credit period spend.
    #[serde(default)]
    credit: Vec<CreditPeriod>,
    /// Month (`tpmo`) token counters.
    #[serde(default)]
    months: Vec<Month>,
    /// Providers blocked by a 402 until cleared.
    #[serde(default)]
    payment_required: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct Month {
    #[serde(flatten)]
    scope: Scope,
    period_start_unix: u64,
    tokens: u64,
}

#[derive(Serialize, Deserialize)]
struct Ceiling {
    provider: String,
    model: String,
    tokens: u64,
}

#[derive(Serialize, Deserialize)]
struct Scope {
    provider: String,
    key: Option<usize>,
    model: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Day {
    #[serde(flatten)]
    scope: Scope,
    day: u64,
    requests: u64,
    tokens: u64,
}

#[derive(Serialize, Deserialize)]
struct Hour {
    provider: String,
    hour: u64,
    tokens_in: u64,
    tokens_out: u64,
}

#[derive(Serialize, Deserialize)]
struct Cooldown {
    #[serde(flatten)]
    scope: Scope,
    until_unix: u64,
    step: u32,
}

#[derive(Serialize, Deserialize)]
struct PaidSpend {
    /// UTC day number.
    day: u64,
    usd: f64,
}

#[derive(Serialize, Deserialize)]
struct ProviderSpend {
    provider: String,
    day: u64,
    usd: f64,
}

#[derive(Serialize, Deserialize)]
struct CreditPeriod {
    provider: String,
    period_start_unix: u64,
    usd: f64,
}

impl From<ScopeKey> for Scope {
    fn from(s: ScopeKey) -> Self {
        Self {
            provider: s.provider,
            key: s.key,
            model: s.model,
        }
    }
}

impl From<Scope> for ScopeKey {
    fn from(s: Scope) -> Self {
        Self {
            provider: s.provider,
            key: s.key,
            model: s.model,
        }
    }
}

impl From<Snapshot> for File {
    fn from(s: Snapshot) -> Self {
        Self {
            version: VERSION,
            days: s
                .days
                .into_iter()
                .map(|d| Day {
                    scope: d.scope.into(),
                    day: d.day,
                    requests: d.requests,
                    tokens: d.tokens,
                })
                .collect(),
            hourly: s
                .hourly
                .into_iter()
                .map(|(provider, hour, tokens_in, tokens_out)| Hour {
                    provider,
                    hour,
                    tokens_in,
                    tokens_out,
                })
                .collect(),
            cooldowns: s
                .cooldowns
                .into_iter()
                .map(|c| Cooldown {
                    scope: c.scope.into(),
                    until_unix: c.until_unix,
                    step: c.step,
                })
                .collect(),
            ceilings: s
                .ceilings
                .into_iter()
                .map(|(provider, model, tokens)| Ceiling {
                    provider,
                    model,
                    tokens,
                })
                .collect(),
            spend: PaidSpend {
                day: s.spend.day,
                usd: s.spend.usd,
            },
            provider_spend: s
                .provider_spend
                .into_iter()
                .map(|(provider, spend)| ProviderSpend {
                    provider,
                    day: spend.day,
                    usd: spend.usd,
                })
                .collect(),
            credit: s
                .credit
                .into_iter()
                .map(|(provider, cs)| CreditPeriod {
                    provider,
                    period_start_unix: cs.period_start,
                    usd: cs.usd,
                })
                .collect(),
            months: s
                .months
                .into_iter()
                .map(|m| Month {
                    scope: m.scope.into(),
                    period_start_unix: m.period_start,
                    tokens: m.tokens,
                })
                .collect(),
            payment_required: s.payment_required,
        }
    }
}

impl From<File> for Snapshot {
    fn from(f: File) -> Self {
        Self {
            days: f
                .days
                .into_iter()
                .map(|d| DaySnap {
                    scope: d.scope.into(),
                    day: d.day,
                    requests: d.requests,
                    tokens: d.tokens,
                })
                .collect(),
            hourly: f
                .hourly
                .into_iter()
                .map(|h| (h.provider, h.hour, h.tokens_in, h.tokens_out))
                .collect(),
            cooldowns: f
                .cooldowns
                .into_iter()
                .map(|c| CooldownSnap {
                    scope: c.scope.into(),
                    until_unix: c.until_unix,
                    step: c.step,
                })
                .collect(),
            ceilings: f
                .ceilings
                .into_iter()
                .map(|c| (c.provider, c.model, c.tokens))
                .collect(),
            spend: Spend {
                day: f.spend.day,
                usd: if f.spend.usd.is_finite() {
                    f.spend.usd.max(0.0)
                } else {
                    0.0
                },
            },
            provider_spend: f
                .provider_spend
                .into_iter()
                .map(|ps| {
                    (
                        ps.provider,
                        Spend {
                            day: ps.day,
                            usd: if ps.usd.is_finite() {
                                ps.usd.max(0.0)
                            } else {
                                0.0
                            },
                        },
                    )
                })
                .collect(),
            credit: f
                .credit
                .into_iter()
                .map(|cp| {
                    (
                        cp.provider,
                        CreditSpend {
                            period_start: cp.period_start_unix,
                            usd: if cp.usd.is_finite() {
                                cp.usd.max(0.0)
                            } else {
                                0.0
                            },
                        },
                    )
                })
                .collect(),
            months: f
                .months
                .into_iter()
                .map(|m| MonthSnap {
                    scope: m.scope.into(),
                    period_start: m.period_start_unix,
                    tokens: m.tokens,
                })
                .collect(),
            payment_required: f.payment_required,
        }
    }
}

/// Serialise a snapshot and replace `path` with it atomically (temp file in
/// the same directory, then rename).
pub fn write_snapshot(path: &Path, snapshot: Snapshot) -> io::Result<()> {
    let json = serde_json::to_vec(&File::from(snapshot)).map_err(io::Error::other)?;
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp);
    fs::write(&tmp, json)?;
    fs::rename(&tmp, path).inspect_err(|_| {
        let _ = fs::remove_file(&tmp);
    })
}

/// Save the current state to the configured `state_file`.
pub fn save(state: &AppState) -> io::Result<()> {
    write_snapshot(
        &state.config.state_file,
        state.limiter.snapshot(Instant::now()),
    )
}

/// Save, logging (never failing) on error.
pub fn save_logged(state: &AppState) {
    if let Err(e) = save(state) {
        crate::elog!(
            "warn: cannot write state file {}: {e}",
            state.config.state_file.display()
        );
    }
}

/// Load `state_file` into the limiter. A missing file is silent; an unreadable
/// or corrupt one logs a warning and leaves the state empty.
pub fn load(state: &AppState) {
    let path = &state.config.state_file;
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return,
        Err(e) => {
            crate::elog!("warn: cannot read state file {}: {e}", path.display());
            return;
        }
    };
    match serde_json::from_slice::<File>(&bytes) {
        Ok(f) if f.version == VERSION => {
            let config = &state.config;
            state.limiter.restore(
                f.into(),
                &|id| config.provider(id).map(|p| p.reset_utc_hour),
                &|id, unix| {
                    config.provider(id).map(|p| {
                        crate::billing::month_start(unix, p.month_reset_day, p.reset_utc_hour)
                    })
                },
                &|id, model| config.provider(id).is_some_and(|p| p.allows_model(model)),
                Instant::now(),
            );
        }
        Ok(f) => crate::elog!(
            "warn: state file {} has unknown version {}, ignoring it",
            path.display(),
            f.version
        ),
        Err(e) => crate::elog!(
            "warn: state file {} is corrupt ({e}), starting fresh",
            path.display()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn state_with_file(path: &Path) -> std::sync::Arc<AppState> {
        let text = format!(
            "state_file = '{}'\n[[provider]]\nid = \"p\"\nbase_url = \"http://p/v1\"\nkeys_env = \"K\"\nbilling = \"free\"\n",
            path.display()
        );
        let config = Config::from_toml_str(&text, &|_| Some("k".into())).unwrap();
        AppState::new(config).unwrap()
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("sluice-unit-{}-{name}.json", std::process::id()))
    }

    #[test]
    fn missing_and_corrupt_files_are_not_errors() {
        let path = temp_path("corrupt");
        let _ = fs::remove_file(&path);
        let state = state_with_file(&path);
        load(&state);
        fs::write(&path, b"{ not json").unwrap();
        load(&state);
        fs::write(&path, br#"{"version":99}"#).unwrap();
        load(&state);
        assert_eq!(state.limiter.snapshot(Instant::now()), Snapshot::default());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn save_then_load_round_trips_and_leaves_no_temp_file() {
        let path = temp_path("roundtrip");
        let a = state_with_file(&path);
        let now = Instant::now();
        let p = a.config.provider("p").unwrap();
        a.limiter.note_request(p, 0, "m", now);
        let usage = crate::limiter::Usage {
            input: 4,
            output: 2,
        };
        a.limiter
            .record_usage(p, 0, "m", usage, Default::default(), now, None);
        a.limiter.record_ceiling("p", "m", 5000);
        assert_eq!(a.limiter.snapshot(now).ceilings.len(), 1);
        save(&a).unwrap();
        assert!(!path.with_extension("json.tmp").exists());

        let b = state_with_file(&path);
        load(&b);
        assert_eq!(b.limiter.snapshot(now).days, a.limiter.snapshot(now).days);
        assert_eq!(b.limiter.hourly("p", now), a.limiter.hourly("p", now));
        assert_eq!(
            b.limiter.snapshot(now).ceilings,
            a.limiter.snapshot(now).ceilings
        );
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn credit_spend_survives_restart() {
        let path = temp_path("credit_restart");
        let text = format!(
            "state_file = '{}'\n[[provider]]\nid = \"p\"\nbase_url = \"http://p/v1\"\nkeys_env = \"K\"\nbilling = \"credit\"\ncredit_usd = 10.0\ncredit_reset_day = 1\n[provider.prices.\"m\"]\nin_per_mtok = 1.0\nout_per_mtok = 0.0\n",
            path.display()
        );
        let config = Config::from_toml_str(&text, &|_| Some("k".into())).unwrap();
        let a = AppState::new(config).unwrap();
        let now = Instant::now();
        let p = a.config.provider("p").unwrap();
        // 2,000,000 input tokens at $1.0/Mtok = $2.00
        let usage = crate::limiter::Usage {
            input: 2_000_000,
            output: 0,
        };
        a.limiter
            .record_usage(p, 0, "m", usage, Default::default(), now, None);
        assert!((a.limiter.credit_spent(p, now) - 2.0).abs() < 1e-9);
        assert!((a.limiter.provider_spend_today("p", now) - 2.0).abs() < 1e-9);
        save(&a).unwrap();

        let b = state_with_file(&path);
        load(&b);
        assert!((b.limiter.credit_spent(p, now) - 2.0).abs() < 1e-9);
        assert!((b.limiter.provider_spend_today("p", now) - 2.0).abs() < 1e-9);
        let _ = fs::remove_file(&path);
    }
}
