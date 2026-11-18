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
use crate::limiter::{CooldownSnap, DaySnap, ScopeKey, Snapshot, Spend};

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
            "state_file = '{}'\n[[provider]]\nid = \"p\"\nbase_url = \"http://p/v1\"\nkeys_env = \"K\"\n",
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
            .record_usage(p, 0, "m", usage, Default::default(), now);
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
}
