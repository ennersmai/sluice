//! `router.toml` and `.env` parsing and validation.
//!
//! Parsing is split in two layers:
//!
//! * [`Config::from_toml_str`] is pure: it takes the TOML text and an
//!   environment lookup closure, so every rule is unit-testable without
//!   touching the real process environment or the file system.
//! * [`Config::load`] reads the file and uses the real environment.
//!
//! All problems found are collected and reported together. API keys never
//! appear in error messages or `Debug` output; see [`ApiKey`].

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Config file used when `--config` is not given.
pub const DEFAULT_CONFIG_PATH: &str = "router.toml";
/// Default listen address (loopback only).
pub const DEFAULT_BIND: &str = "127.0.0.1:8787";
/// Environment variable holding the bearer token clients must present when
/// the router listens on a non-loopback address.
pub const ROUTER_API_KEY_ENV: &str = "ROUTER_API_KEY";

// ---------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------

/// An API key. Its `Debug` output shows at most the last four characters, and
/// it deliberately implements neither `Display` nor `Serialize`.
#[derive(Clone, PartialEq, Eq)]
pub struct ApiKey(String);

impl ApiKey {
    /// Keys shorter than this are fully masked, since showing four characters
    /// of a very short key would reveal most of it.
    const MIN_LEN_FOR_SUFFIX: usize = 8;

    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    /// The single slot of a keyless provider: no `Authorization` is sent.
    pub fn anonymous() -> Self {
        Self(String::new())
    }

    pub fn is_anonymous(&self) -> bool {
        self.0.is_empty()
    }

    /// The raw key. Only for building outbound `Authorization` headers and
    /// comparing client tokens; never log or return it.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Redacted form safe for logs and the dashboard: `…abcd`, or `****` for
    /// keys too short to show a suffix safely.
    pub fn redacted(&self) -> String {
        if self.is_anonymous() {
            return "none".to_string();
        }
        let chars: Vec<char> = self.0.chars().collect();
        if chars.len() < Self::MIN_LEN_FOR_SUFFIX {
            "****".to_string()
        } else {
            let suffix: String = chars[chars.len() - 4..].iter().collect();
            format!("…{suffix}")
        }
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ApiKey({})", self.redacted())
    }
}

// ---------------------------------------------------------------------------
// Validated configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub state_file: PathBuf,
    /// Also append request and warning lines to this file when set.
    pub log_file: Option<PathBuf>,
    pub max_attempts: u32,
    pub first_byte_timeout_secs: u64,
    /// Silence allowed once the body has started; 0 disables.
    pub stream_idle_timeout_secs: u64,
    pub max_wait_secs: u64,
    pub private_default: bool,
    pub paid_daily_cap_usd: f64,
    pub providers: Vec<Provider>,
    pub pools: Vec<Pool>,
    /// Bearer token required from clients. `Some` exactly when `bind` is not
    /// a loopback address (then it is mandatory); `None` on loopback.
    pub client_auth: Option<ApiKey>,
}

impl Config {
    pub fn provider(&self, id: &str) -> Option<&Provider> {
        self.providers.iter().find(|p| p.id == id)
    }

    pub fn pool(&self, name: &str) -> Option<&Pool> {
        self.pools.iter().find(|p| p.name == name)
    }
}

#[derive(Debug, Clone)]
pub struct Provider {
    pub id: String,
    pub base_url: String,
    /// `None` for a keyless provider, which has one anonymous key slot.
    pub keys_env: Option<String>,
    /// At least one key, in the order given in the environment variable.
    pub keys: Vec<ApiKey>,
    pub paid: bool,
    pub billing: Billing,
    pub trains_on_input: bool,
    pub reset_utc_hour: u8,
    pub drop_params: Vec<String>,
    /// Upstream model allowlist. `None` means no allowlist was declared.
    pub models: Option<Vec<String>>,
    pub limits: ProviderLimits,
    /// Per-model overrides, always `key_model` scoped.
    pub model_limits: BTreeMap<String, Limits>,
    pub prices: BTreeMap<String, Price>,
    pub usage_cost_field: Option<String>,
}

impl Provider {
    /// Whether `model` may be routed through this provider.
    pub fn allows_model(&self, model: &str) -> bool {
        match &self.models {
            Some(list) => list.iter().any(|m| m == model),
            None => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitScope {
    Provider,
    Key,
    KeyModel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Billing {
    Free,
    Credit,
    Paid,
}

impl Billing {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "free" => Some(Self::Free),
            "credit" => Some(Self::Credit),
            "paid" => Some(Self::Paid),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Free => "free",
            Self::Credit => "credit",
            Self::Paid => "paid",
        }
    }
}

impl LimitScope {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "provider" => Some(Self::Provider),
            "key" => Some(Self::Key),
            "key_model" => Some(Self::KeyModel),
            _ => None,
        }
    }
}

/// Request and token limits. `None` means unknown (configured as `0` or
/// omitted): rely on reactive cooldowns.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Limits {
    pub rpm: Option<u64>,
    pub rpd: Option<u64>,
    pub tpm: Option<u64>,
    pub tpd: Option<u64>,
    /// Largest request (pre-flight token estimate) the model accepts.
    /// Only meaningful in `model_limits`.
    pub max_input_tokens: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderLimits {
    pub scope: LimitScope,
    pub limits: Limits,
}

impl Default for ProviderLimits {
    fn default() -> Self {
        Self {
            scope: LimitScope::Provider,
            limits: Limits::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Price {
    pub in_per_mtok: f64,
    pub out_per_mtok: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    Priority,
    RoundRobin,
    LeastUsed,
}

impl Strategy {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "priority" => Some(Self::Priority),
            "round_robin" => Some(Self::RoundRobin),
            "least_used" => Some(Self::LeastUsed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub provider: String,
    pub model: String,
}

#[derive(Debug, Clone)]
pub struct Pool {
    pub name: String,
    pub strategy: Strategy,
    pub routes: Vec<Route>,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// One or more configuration problems. Messages never contain key material.
#[derive(Debug, Clone)]
pub struct ConfigError {
    problems: Vec<String>,
}

impl ConfigError {
    fn single(msg: impl Into<String>) -> Self {
        Self {
            problems: vec![msg.into()],
        }
    }

    pub fn problems(&self) -> &[String] {
        &self.problems
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.problems.as_slice() {
            [one] => f.write_str(one),
            many => {
                write!(f, "{} problems:", many.len())?;
                for p in many {
                    write!(f, "\n  - {p}")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ConfigError {}

// ---------------------------------------------------------------------------
// Raw file schema
// ---------------------------------------------------------------------------

fn default_bind() -> String {
    DEFAULT_BIND.to_string()
}
fn default_state_file() -> String {
    "state.json".to_string()
}
fn default_max_attempts() -> u32 {
    6
}
fn default_first_byte_timeout_secs() -> u64 {
    20
}
fn default_stream_idle_timeout_secs() -> u64 {
    60
}
fn default_max_wait_secs() -> u64 {
    15
}
fn default_scope() -> String {
    "provider".to_string()
}
fn default_strategy() -> String {
    "priority".to_string()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default = "default_bind")]
    bind: String,
    #[serde(default = "default_state_file")]
    state_file: String,
    #[serde(default)]
    log_file: Option<String>,
    #[serde(default = "default_max_attempts")]
    max_attempts: u32,
    #[serde(default = "default_first_byte_timeout_secs")]
    first_byte_timeout_secs: u64,
    #[serde(default = "default_stream_idle_timeout_secs")]
    stream_idle_timeout_secs: u64,
    #[serde(default = "default_max_wait_secs")]
    max_wait_secs: u64,
    #[serde(default)]
    private_default: bool,
    #[serde(default)]
    paid_daily_cap_usd: f64,
    #[serde(default, rename = "provider")]
    providers: Vec<RawProvider>,
    #[serde(default, rename = "pool")]
    pools: Vec<RawPool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProvider {
    id: String,
    base_url: String,
    #[serde(default)]
    keys_env: Option<String>,
    #[serde(default)]
    paid: Option<bool>,
    #[serde(default)]
    billing: Option<String>,
    #[serde(default)]
    trains_on_input: bool,
    #[serde(default)]
    reset_utc_hour: i64,
    #[serde(default)]
    drop_params: Vec<String>,
    #[serde(default)]
    models: Option<Vec<String>>,
    #[serde(default)]
    limits: Option<RawProviderLimits>,
    #[serde(default)]
    model_limits: BTreeMap<String, RawLimits>,
    #[serde(default)]
    prices: BTreeMap<String, RawPrice>,
    #[serde(default)]
    usage_cost_field: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProviderLimits {
    #[serde(default = "default_scope")]
    scope: String,
    #[serde(default)]
    rpm: u64,
    #[serde(default)]
    rpd: u64,
    #[serde(default)]
    tpm: u64,
    #[serde(default)]
    tpd: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLimits {
    #[serde(default)]
    rpm: u64,
    #[serde(default)]
    rpd: u64,
    #[serde(default)]
    tpm: u64,
    #[serde(default)]
    tpd: u64,
    #[serde(default)]
    max_input_tokens: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPrice {
    in_per_mtok: f64,
    out_per_mtok: f64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPool {
    name: String,
    #[serde(default = "default_strategy")]
    strategy: String,
    routes: Vec<String>,
}

fn known(v: u64) -> Option<u64> {
    (v != 0).then_some(v)
}

fn limits_from(rpm: u64, rpd: u64, tpm: u64, tpd: u64) -> Limits {
    Limits {
        rpm: known(rpm),
        rpd: known(rpd),
        tpm: known(tpm),
        tpd: known(tpd),
        max_input_tokens: None,
    }
}

// ---------------------------------------------------------------------------
// Loading and validation
// ---------------------------------------------------------------------------

impl Config {
    /// Read `path` and validate it against the real process environment.
    /// Load `.env` before calling this.
    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::single(format!("cannot read file: {e}")))?;
        Self::from_toml_str(&text, &|name| std::env::var(name).ok())
    }

    /// Parse and validate TOML text. `env` looks up environment variables
    /// (key lists and `ROUTER_API_KEY`).
    pub fn from_toml_str(
        text: &str,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Config, ConfigError> {
        let raw: RawConfig = toml::from_str(text)
            .map_err(|e| ConfigError::single(format!("cannot parse TOML: {e}")))?;
        validate(raw, env)
    }
}

/// Split a comma-separated key list, trimming entries and skipping empty
/// ones. Errors never include key material.
fn parse_keys(var: &str, value: &str) -> Result<Vec<ApiKey>, String> {
    let keys: Vec<ApiKey> = value
        .split(',')
        .map(str::trim)
        .filter(|k| !k.is_empty())
        .map(ApiKey::new)
        .collect();
    if keys.is_empty() {
        return Err(format!("{var} is set but contains no keys"));
    }
    for (i, a) in keys.iter().enumerate() {
        if let Some(j) = keys[i + 1..].iter().position(|b| a == b) {
            return Err(format!(
                "{var} contains the same key twice (entries {} and {})",
                i + 1,
                i + j + 2
            ));
        }
    }
    Ok(keys)
}

/// Split `provider_id/upstream_model` at the first `/`.
fn split_route(route: &str) -> Option<(&str, &str)> {
    route.split_once('/')
}

fn validate(raw: RawConfig, env: &dyn Fn(&str) -> Option<String>) -> Result<Config, ConfigError> {
    let mut problems: Vec<String> = Vec::new();

    // ---- top level ----
    let bind = match raw.bind.parse::<SocketAddr>() {
        Ok(addr) => addr,
        Err(_) => {
            problems.push(format!(
                "bind = \"{}\" is not a valid socket address (expected IP:port, e.g. {DEFAULT_BIND})",
                raw.bind
            ));
            SocketAddr::from(([127, 0, 0, 1], 8787))
        }
    };

    let mut client_auth = None;
    if !bind.ip().is_loopback() {
        match env(ROUTER_API_KEY_ENV).map(|v| v.trim().to_string()) {
            Some(v) if !v.is_empty() => client_auth = Some(ApiKey::new(v)),
            _ => problems.push(format!(
                "bind = \"{}\" is not a loopback address, so {ROUTER_API_KEY_ENV} must be set to a non-empty value",
                raw.bind
            )),
        }
    }

    if raw.max_attempts == 0 {
        problems.push("max_attempts must be at least 1".to_string());
    }
    if raw.first_byte_timeout_secs == 0 {
        problems.push("first_byte_timeout_secs must be at least 1".to_string());
    }
    if !raw.paid_daily_cap_usd.is_finite() || raw.paid_daily_cap_usd < 0.0 {
        problems.push(format!(
            "paid_daily_cap_usd = {} must be a number >= 0",
            raw.paid_daily_cap_usd
        ));
    }
    if raw.state_file.trim().is_empty() {
        problems.push("state_file must not be empty".to_string());
    }
    if raw.providers.is_empty() {
        problems.push("no [[provider]] defined; at least one is required".to_string());
    }

    // ---- providers ----
    let mut seen_ids: HashSet<&str> = HashSet::new();
    let mut providers = Vec::with_capacity(raw.providers.len());

    for (i, rp) in raw.providers.iter().enumerate() {
        let label = if rp.id.is_empty() {
            format!("provider #{}", i + 1)
        } else {
            format!("provider \"{}\"", rp.id)
        };

        if rp.id.trim().is_empty() {
            problems.push(format!("{label}: id must not be empty"));
        } else if rp.id.contains('/') {
            problems.push(format!("{label}: id must not contain '/'"));
        } else if !seen_ids.insert(rp.id.as_str()) {
            problems.push(format!("{label}: id is defined more than once"));
        }

        let base_url = rp.base_url.trim();
        let host_part = base_url
            .strip_prefix("https://")
            .or_else(|| base_url.strip_prefix("http://"));
        if host_part.is_none_or(str::is_empty) {
            problems.push(format!(
                "{label}: base_url \"{}\" must be an http:// or https:// URL",
                rp.base_url
            ));
        }

        let mut keys = Vec::new();
        match rp.keys_env.as_deref() {
            None => keys.push(ApiKey::anonymous()),
            Some(var) if var.trim().is_empty() => {
                problems.push(format!("{label}: keys_env must not be empty"));
            }
            Some(var) => match env(var) {
                None => problems.push(format!(
                    "{label}: environment variable {var} is not set (put it in .env)"
                )),
                Some(value) => match parse_keys(var, &value) {
                    Ok(k) => keys = k,
                    Err(e) => problems.push(format!("{label}: {e}")),
                },
            },
        }

        let reset_utc_hour = match u8::try_from(rp.reset_utc_hour) {
            Ok(h) if h <= 23 => h,
            _ => {
                problems.push(format!(
                    "{label}: reset_utc_hour = {} must be between 0 and 23",
                    rp.reset_utc_hour
                ));
                0
            }
        };

        for p in &rp.drop_params {
            if p.trim().is_empty() {
                problems.push(format!("{label}: drop_params contains an empty entry"));
            } else if p == "model" {
                problems.push(format!(
                    "{label}: drop_params must not contain \"model\" (the router always sets it)"
                ));
            }
        }

        if let Some(models) = &rp.models
            && models.iter().any(|m| m.trim().is_empty())
        {
            problems.push(format!("{label}: models contains an empty entry"));
        }
        let in_allowlist = |m: &str| match &rp.models {
            Some(list) => list.iter().any(|x| x == m),
            None => true,
        };

        let limits = match &rp.limits {
            None => ProviderLimits::default(),
            Some(l) => {
                let scope = LimitScope::parse(&l.scope).unwrap_or_else(|| {
                    problems.push(format!(
                        "{label}: limits.scope = \"{}\" must be one of provider, key, key_model",
                        l.scope
                    ));
                    LimitScope::Provider
                });
                ProviderLimits {
                    scope,
                    limits: limits_from(l.rpm, l.rpd, l.tpm, l.tpd),
                }
            }
        };

        let mut model_limits = BTreeMap::new();
        for (model, l) in &rp.model_limits {
            if model.trim().is_empty() {
                problems.push(format!("{label}: model_limits has an empty model id"));
            } else if !in_allowlist(model) {
                problems.push(format!(
                    "{label}: model_limits entry \"{model}\" is not in the provider's models allowlist"
                ));
            }
            model_limits.insert(
                model.clone(),
                Limits {
                    max_input_tokens: known(l.max_input_tokens),
                    ..limits_from(l.rpm, l.rpd, l.tpm, l.tpd)
                },
            );
        }

        let mut prices = BTreeMap::new();
        for (model, p) in &rp.prices {
            if model.trim().is_empty() {
                problems.push(format!("{label}: prices has an empty model id"));
            } else if !in_allowlist(model) {
                problems.push(format!(
                    "{label}: prices entry \"{model}\" is not in the provider's models allowlist"
                ));
            }
            for (field, v) in [
                ("in_per_mtok", p.in_per_mtok),
                ("out_per_mtok", p.out_per_mtok),
            ] {
                if !v.is_finite() || v < 0.0 {
                    problems.push(format!(
                        "{label}: prices.\"{model}\".{field} = {v} must be a number >= 0"
                    ));
                }
            }
            prices.insert(
                model.clone(),
                Price {
                    in_per_mtok: p.in_per_mtok,
                    out_per_mtok: p.out_per_mtok,
                },
            );
        }

        // Resolve billing type according to the rules:
        // - billing unset, paid unset -> Paid
        // - billing unset, paid = true -> Paid
        // - billing unset, paid = false -> error "paid = false no longer means free; set billing = "free" (or "credit")"
        // - billing set to valid X, paid unset -> X
        // - billing = "paid" with paid = true, or billing = "free"/"credit" with paid = false -> X (redundant but allowed)
        // - billing = "paid" with paid = false, or billing = "free"/"credit" with paid = true -> error "paid = {v} conflicts with billing = "{b}""
        // - invalid billing string -> error "billing = "{s}" must be one of free, credit, paid"
        let (billing, paid) = match (&rp.billing, rp.paid) {
            (None, None) => (Billing::Paid, true),
            (None, Some(true)) => (Billing::Paid, true),
            (None, Some(false)) => {
                problems.push(format!(
                    "{label}: paid = false no longer means free; set billing = \"free\" (or \"credit\")"
                ));
                (Billing::Paid, true) // default to paid to avoid silently allowing pool inclusion
            }
            (Some(b_str), None) => match Billing::parse(b_str) {
                Some(b) => (b, b == Billing::Paid),
                None => {
                    problems.push(format!(
                        "{label}: billing = \"{b_str}\" must be one of free, credit, paid"
                    ));
                    (Billing::Paid, true)
                }
            },
            (Some(b_str), Some(paid_val)) => match Billing::parse(b_str) {
                Some(b) => {
                    let paid_from_billing = b == Billing::Paid;
                    if paid_from_billing != paid_val {
                        problems.push(format!(
                            "{label}: paid = {paid_val} conflicts with billing = \"{b_str}\""
                        ));
                    }
                    (b, b == Billing::Paid)
                }
                None => {
                    problems.push(format!(
                        "{label}: billing = \"{b_str}\" must be one of free, credit, paid"
                    ));
                    (Billing::Paid, true)
                }
            },
        };

        providers.push(Provider {
            id: rp.id.clone(),
            base_url: base_url.to_string(),
            keys_env: rp.keys_env.clone(),
            keys,
            paid,
            billing,
            trains_on_input: rp.trains_on_input,
            reset_utc_hour,
            drop_params: rp.drop_params.clone(),
            models: rp.models.clone(),
            limits,
            model_limits,
            prices,
            usage_cost_field: rp.usage_cost_field.clone(),
        });
    }

    // ---- pools ----
    let mut seen_pools: HashSet<&str> = HashSet::new();
    let mut pools = Vec::with_capacity(raw.pools.len());

    for (i, rp) in raw.pools.iter().enumerate() {
        let label = if rp.name.is_empty() {
            format!("pool #{}", i + 1)
        } else {
            format!("pool \"{}\"", rp.name)
        };

        if rp.name.trim().is_empty() {
            problems.push(format!("{label}: name must not be empty"));
        } else if rp.name.contains('/') {
            problems.push(format!("{label}: name must not contain '/'"));
        } else if !seen_pools.insert(rp.name.as_str()) {
            problems.push(format!("{label}: name is defined more than once"));
        } else if seen_ids.contains(rp.name.as_str()) {
            problems.push(format!(
                "{label}: name is the same as a provider id; pool names and provider ids must differ"
            ));
        }

        let strategy = Strategy::parse(&rp.strategy).unwrap_or_else(|| {
            problems.push(format!(
                "{label}: strategy = \"{}\" must be one of priority, round_robin, least_used",
                rp.strategy
            ));
            Strategy::Priority
        });

        if rp.routes.is_empty() {
            problems.push(format!("{label}: routes must contain at least one route"));
        }

        let mut routes = Vec::with_capacity(rp.routes.len());
        for route in &rp.routes {
            let Some((pid, model)) = split_route(route) else {
                problems.push(format!(
                    "{label}: route \"{route}\" must have the form provider_id/upstream_model"
                ));
                continue;
            };
            if model.trim().is_empty() {
                problems.push(format!("{label}: route \"{route}\" has an empty model"));
                continue;
            }
            let Some(provider) = providers.iter().find(|p| p.id == pid) else {
                problems.push(format!(
                    "{label}: route \"{route}\" refers to unknown provider \"{pid}\""
                ));
                continue;
            };
            if provider.paid {
                problems.push(format!(
                    "{label}: route \"{route}\" uses paid provider \"{pid}\"; paid providers are reachable only by explicit route, never through a pool"
                ));
                continue;
            }
            if !provider.allows_model(model) {
                problems.push(format!(
                    "{label}: route \"{route}\": model \"{model}\" is not in provider \"{pid}\"'s models allowlist"
                ));
                continue;
            }
            routes.push(Route {
                provider: pid.to_string(),
                model: model.to_string(),
            });
        }

        pools.push(Pool {
            name: rp.name.clone(),
            strategy,
            routes,
        });
    }

    if !problems.is_empty() {
        return Err(ConfigError { problems });
    }

    Ok(Config {
        bind,
        state_file: PathBuf::from(raw.state_file),
        log_file: raw
            .log_file
            .filter(|f| !f.trim().is_empty())
            .map(PathBuf::from),
        max_attempts: raw.max_attempts,
        first_byte_timeout_secs: raw.first_byte_timeout_secs,
        stream_idle_timeout_secs: raw.stream_idle_timeout_secs,
        max_wait_secs: raw.max_wait_secs,
        private_default: raw.private_default,
        paid_daily_cap_usd: raw.paid_daily_cap_usd,
        providers,
        pools,
        client_auth,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const SECRET: &str = "sk-very-secret-material-9f3a";

    /// Environment with one key for the variables used in the tests.
    fn default_env() -> HashMap<String, String> {
        [
            ("OPENROUTER_KEYS", SECRET),
            ("NVIDIA_KEYS", "nv-key-000000001"),
            ("GROQ_KEYS", "gq-key-000000001"),
            ("GEMINI_KEYS", "gm-key-000000001"),
            ("A_KEYS", "a-key-00000001"),
            ("B_KEYS", "b-key-00000001"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn parse_with(text: &str, env: &HashMap<String, String>) -> Result<Config, ConfigError> {
        Config::from_toml_str(text, &|k| env.get(k).cloned())
    }

    fn parse(text: &str) -> Result<Config, ConfigError> {
        parse_with(text, &default_env())
    }

    fn ok(text: &str) -> Config {
        match parse(text) {
            Ok(c) => c,
            Err(e) => panic!("expected valid config, got: {e}"),
        }
    }

    /// Returns the full error text and asserts it contains `needle`.
    fn err_with(text: &str, env: &HashMap<String, String>, needle: &str) -> String {
        match parse_with(text, env) {
            Ok(_) => panic!("expected an error containing {needle:?}, config was accepted"),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains(needle),
                    "error {msg:?} does not contain {needle:?}"
                );
                assert!(!msg.contains(SECRET), "error leaks key material: {msg:?}");
                msg
            }
        }
    }

    fn err(text: &str, needle: &str) -> String {
        err_with(text, &default_env(), needle)
    }

    const ONE_PROVIDER: &str = r#"
[[provider]]
id = "a"
base_url = "https://a.example/v1"
keys_env = "A_KEYS"
billing = "free"
"#;

    /// The PRD section 5 example verbatim, plus the `nvidia` and `groq`
    /// providers its pool refers to.
    const PRD_EXAMPLE: &str = r#"
bind = "127.0.0.1:8787"
state_file = "state.json"
max_attempts = 6
first_byte_timeout_secs = 20
max_wait_secs = 15          # hold a request this long if a route frees up soon, 0 disables
private_default = false
paid_daily_cap_usd = 1.00   # applies to explicit paid requests, 0 blocks paid entirely

[[provider]]
id = "or-free"
base_url = "https://openrouter.ai/api/v1"
keys_env = "OPENROUTER_KEYS"
billing = "free"
trains_on_input = false     # set per provider after reading its terms
reset_utc_hour = 0
drop_params = []            # request fields this provider rejects
models = [                  # allowlist, fill in by hand or from `llm-router models or-free`
  "vendor/example-model:free",
]
[provider.limits]
scope = "provider"          # provider | key | key_model  (see section 7)
rpm = 0                     # copy real numbers from the provider dashboard, 0 = unknown
rpd = 0
tpm = 0
tpd = 0
[provider.model_limits."vendor/example-model:free"]   # optional per-model override
rpm = 0

[[provider]]
id = "or-paid"
base_url = "https://openrouter.ai/api/v1"
keys_env = "OPENROUTER_KEYS"
billing = "paid"
models = ["vendor/example-paid-model"]
[provider.prices."vendor/example-paid-model"]
in_per_mtok = 0.0
out_per_mtok = 0.0

[[provider]]
id = "gemini"
base_url = "https://generativelanguage.googleapis.com/v1beta/openai"
keys_env = "GEMINI_KEYS"
billing = "free"
trains_on_input = true

[[pool]]
name = "coder"
strategy = "priority"       # priority | round_robin | least_used
routes = ["or-free/vendor/example-model:free", "nvidia/vendor/example-model", "groq/example-model"]

[[provider]]
id = "nvidia"
base_url = "https://integrate.api.nvidia.com/v1"
keys_env = "NVIDIA_KEYS"
billing = "free"

[[provider]]
id = "groq"
base_url = "https://api.groq.com/openai/v1"
keys_env = "GROQ_KEYS"
billing = "free"
"#;

    // ---- defaults ----

    #[test]
    fn minimal_config_gets_documented_defaults() {
        let c = ok(ONE_PROVIDER);
        assert_eq!(c.bind, "127.0.0.1:8787".parse::<SocketAddr>().unwrap());
        assert_eq!(c.state_file, PathBuf::from("state.json"));
        assert_eq!(c.max_attempts, 6);
        assert_eq!(c.first_byte_timeout_secs, 20);
        assert_eq!(c.max_wait_secs, 15);
        assert!(!c.private_default);
        assert_eq!(c.paid_daily_cap_usd, 0.0);
        assert!(c.client_auth.is_none());
        assert!(c.pools.is_empty());

        let p = &c.providers[0];
        assert_eq!(p.id, "a");
        assert!(!p.paid);
        assert!(!p.trains_on_input);
        assert_eq!(p.reset_utc_hour, 0);
        assert!(p.drop_params.is_empty());
        assert!(p.models.is_none());
        assert_eq!(p.limits.scope, LimitScope::Provider);
        assert_eq!(p.limits.limits, Limits::default());
        assert!(p.model_limits.is_empty());
        assert!(p.prices.is_empty());
        assert_eq!(p.keys.len(), 1);
    }

    #[test]
    fn limit_scope_defaults_to_provider_when_block_present() {
        let c = ok(&format!("{ONE_PROVIDER}[provider.limits]\nrpm = 5\n"));
        let l = c.providers[0].limits;
        assert_eq!(l.scope, LimitScope::Provider);
        assert_eq!(l.limits.rpm, Some(5));
        assert_eq!(l.limits.rpd, None);
    }

    #[test]
    fn zero_and_missing_limits_mean_unknown() {
        let c = ok(&format!(
            "{ONE_PROVIDER}[provider.limits]\nrpm = 0\nrpd = 100\ntpm = 0\n\
             [provider.model_limits.\"vendor/example-model\"]\ntpd = 0\nrpm = 3\n"
        ));
        let p = &c.providers[0];
        assert_eq!(
            p.limits.limits,
            Limits {
                rpm: None,
                rpd: Some(100),
                tpm: None,
                tpd: None,
                max_input_tokens: None
            }
        );
        assert_eq!(
            p.model_limits["vendor/example-model"],
            Limits {
                rpm: Some(3),
                rpd: None,
                tpm: None,
                tpd: None,
                max_input_tokens: None
            }
        );
    }

    #[test]
    fn all_limit_scopes_parse() {
        for (s, want) in [
            ("provider", LimitScope::Provider),
            ("key", LimitScope::Key),
            ("key_model", LimitScope::KeyModel),
        ] {
            let c = ok(&format!(
                "{ONE_PROVIDER}[provider.limits]\nscope = \"{s}\"\n"
            ));
            assert_eq!(c.providers[0].limits.scope, want);
        }
    }

    #[test]
    fn all_strategies_parse_and_default_is_priority() {
        for (s, want) in [
            ("priority", Strategy::Priority),
            ("round_robin", Strategy::RoundRobin),
            ("least_used", Strategy::LeastUsed),
        ] {
            let c = ok(&format!(
                "{ONE_PROVIDER}[[pool]]\nname = \"p\"\nstrategy = \"{s}\"\nroutes = [\"a/m\"]\n"
            ));
            assert_eq!(c.pools[0].strategy, want);
        }
        let c = ok(&format!(
            "{ONE_PROVIDER}[[pool]]\nname = \"p\"\nroutes = [\"a/m\"]\n"
        ));
        assert_eq!(c.pools[0].strategy, Strategy::Priority);
    }

    // ---- full examples ----

    #[test]
    fn prd_example_parses() {
        let c = ok(PRD_EXAMPLE);
        assert_eq!(c.providers.len(), 5);
        assert_eq!(c.paid_daily_cap_usd, 1.0);
        let or_free = c.provider("or-free").unwrap();
        assert_eq!(
            or_free.models.as_deref(),
            Some(&["vendor/example-model:free".to_string()][..])
        );
        assert_eq!(or_free.limits.limits, Limits::default());
        assert!(
            or_free
                .model_limits
                .contains_key("vendor/example-model:free")
        );
        let or_paid = c.provider("or-paid").unwrap();
        assert!(or_paid.paid);
        assert!(or_paid.prices.contains_key("vendor/example-paid-model"));
        assert!(c.provider("gemini").unwrap().trains_on_input);
        let coder = c.pool("coder").unwrap();
        assert_eq!(
            coder.routes,
            vec![
                Route {
                    provider: "or-free".into(),
                    model: "vendor/example-model:free".into()
                },
                Route {
                    provider: "nvidia".into(),
                    model: "vendor/example-model".into()
                },
                Route {
                    provider: "groq".into(),
                    model: "example-model".into()
                },
            ]
        );
    }

    #[test]
    fn shipped_example_file_is_valid() {
        let c = ok(include_str!("../router.example.toml"));
        assert!(!c.providers.is_empty());
        assert!(c.bind.ip().is_loopback());
    }

    // ---- keys ----

    #[test]
    fn multiple_keys_are_split_and_trimmed() {
        let mut env = default_env();
        env.insert(
            "A_KEYS".into(),
            " key-one-0001 , ,key-two-0002,, key-three-0003 ".into(),
        );
        let c = parse_with(ONE_PROVIDER, &env).unwrap();
        let keys: Vec<&str> = c.providers[0].keys.iter().map(ApiKey::expose).collect();
        assert_eq!(keys, ["key-one-0001", "key-two-0002", "key-three-0003"]);
    }

    #[test]
    fn keys_env_unset_is_rejected() {
        let mut env = default_env();
        env.remove("A_KEYS");
        err_with(
            ONE_PROVIDER,
            &env,
            "provider \"a\": environment variable A_KEYS is not set",
        );
    }

    #[test]
    fn keys_env_without_keys_is_rejected() {
        let mut env = default_env();
        env.insert("A_KEYS".into(), " , ,".into());
        err_with(ONE_PROVIDER, &env, "A_KEYS is set but contains no keys");
        env.insert("A_KEYS".into(), String::new());
        err_with(ONE_PROVIDER, &env, "A_KEYS is set but contains no keys");
    }

    #[test]
    fn duplicate_keys_are_rejected_without_revealing_them() {
        let mut env = default_env();
        env.insert(
            "OPENROUTER_KEYS".into(),
            format!("{SECRET},other-key-1234,{SECRET}"),
        );
        let text = ONE_PROVIDER.replace("A_KEYS", "OPENROUTER_KEYS");
        let msg = err_with(&text, &env, "contains the same key twice (entries 1 and 3)");
        assert!(!msg.contains("9f3a"));
    }

    #[test]
    fn empty_keys_env_name_is_rejected() {
        err(
            &ONE_PROVIDER.replace("\"A_KEYS\"", "\"\""),
            "keys_env must not be empty",
        );
    }

    #[test]
    fn api_key_debug_shows_only_last_four() {
        let k = ApiKey::new(SECRET);
        let dbg = format!("{k:?}");
        assert_eq!(dbg, "ApiKey(…9f3a)");
        assert_eq!(k.redacted(), "…9f3a");
        assert!(!dbg.contains("secret"));
    }

    #[test]
    fn short_api_key_is_fully_masked() {
        let k = ApiKey::new("abc12");
        assert_eq!(format!("{k:?}"), "ApiKey(****)");
    }

    #[test]
    fn config_debug_never_contains_keys() {
        let mut env = default_env();
        env.insert(ROUTER_API_KEY_ENV.into(), "router-client-token-77aa".into());
        let text = PRD_EXAMPLE.replace("127.0.0.1:8787", "0.0.0.0:8787");
        let c = parse_with(&text, &env).unwrap();
        assert!(c.client_auth.is_some());
        let dbg = format!("{c:?}");
        assert!(!dbg.contains(SECRET), "Debug leaks provider key");
        assert!(
            !dbg.contains("router-client-token"),
            "Debug leaks client token"
        );
        assert!(dbg.contains("…9f3a"));
    }

    // ---- provider ids ----

    #[test]
    fn empty_provider_id_is_rejected() {
        err(
            &ONE_PROVIDER.replace("id = \"a\"", "id = \"\""),
            "provider #1: id must not be empty",
        );
    }

    #[test]
    fn provider_id_with_slash_is_rejected() {
        err(
            &ONE_PROVIDER.replace("id = \"a\"", "id = \"a/b\""),
            "provider \"a/b\": id must not contain '/'",
        );
    }

    #[test]
    fn duplicate_provider_id_is_rejected() {
        err(
            &format!("{ONE_PROVIDER}{ONE_PROVIDER}"),
            "provider \"a\": id is defined more than once",
        );
    }

    #[test]
    fn no_providers_is_rejected() {
        err("", "at least one is required");
    }

    #[test]
    fn bad_base_url_is_rejected() {
        err(
            &ONE_PROVIDER.replace("https://a.example/v1", "a.example/v1"),
            "provider \"a\": base_url \"a.example/v1\" must be an http:// or https:// URL",
        );
    }

    // ---- pools ----

    fn with_pool(pool: &str) -> String {
        format!("{ONE_PROVIDER}[[pool]]\n{pool}\n")
    }

    #[test]
    fn empty_pool_name_is_rejected() {
        err(
            &with_pool("name = \"\"\nroutes = [\"a/m\"]"),
            "pool #1: name must not be empty",
        );
    }

    #[test]
    fn pool_name_with_slash_is_rejected() {
        err(
            &with_pool("name = \"x/y\"\nroutes = [\"a/m\"]"),
            "pool \"x/y\": name must not contain '/'",
        );
    }

    #[test]
    fn duplicate_pool_name_is_rejected() {
        let text = format!(
            "{ONE_PROVIDER}[[pool]]\nname = \"p\"\nroutes = [\"a/m\"]\n[[pool]]\nname = \"p\"\nroutes = [\"a/m\"]\n"
        );
        err(&text, "pool \"p\": name is defined more than once");
    }

    #[test]
    fn pool_name_equal_to_provider_id_is_rejected() {
        err(
            &with_pool("name = \"a\"\nroutes = [\"a/m\"]"),
            "pool \"a\": name is the same as a provider id",
        );
    }

    #[test]
    fn pool_without_routes_is_rejected() {
        err(
            &with_pool("name = \"p\"\nroutes = []"),
            "pool \"p\": routes must contain at least one route",
        );
    }

    #[test]
    fn pool_bad_strategy_is_rejected() {
        err(
            &with_pool("name = \"p\"\nstrategy = \"random\"\nroutes = [\"a/m\"]"),
            "pool \"p\": strategy = \"random\" must be one of priority, round_robin, least_used",
        );
    }

    #[test]
    fn route_is_split_at_first_slash() {
        let c = ok(&with_pool(
            "name = \"p\"\nroutes = [\"a/vendor/model:free\"]",
        ));
        assert_eq!(
            c.pools[0].routes,
            vec![Route {
                provider: "a".into(),
                model: "vendor/model:free".into()
            }]
        );
    }

    #[test]
    fn route_without_slash_is_rejected() {
        err(
            &with_pool("name = \"p\"\nroutes = [\"amodel\"]"),
            "pool \"p\": route \"amodel\" must have the form provider_id/upstream_model",
        );
    }

    #[test]
    fn route_with_empty_model_is_rejected() {
        err(
            &with_pool("name = \"p\"\nroutes = [\"a/\"]"),
            "pool \"p\": route \"a/\" has an empty model",
        );
    }

    #[test]
    fn route_to_unknown_provider_is_rejected() {
        err(
            &with_pool("name = \"p\"\nroutes = [\"zzz/m\"]"),
            "pool \"p\": route \"zzz/m\" refers to unknown provider \"zzz\"",
        );
        err(
            &with_pool("name = \"p\"\nroutes = [\"/m\"]"),
            "refers to unknown provider \"\"",
        );
    }

    #[test]
    fn route_outside_allowlist_is_rejected() {
        let text = format!(
            "{ONE_PROVIDER}models = [\"vendor/ok\"]\n[[pool]]\nname = \"p\"\nroutes = [\"a/vendor/ok\", \"a/vendor/other\"]\n"
        );
        err(
            &text,
            "model \"vendor/other\" is not in provider \"a\"'s models allowlist",
        );
        let fixed = text.replace(", \"a/vendor/other\"", "");
        assert_eq!(ok(&fixed).pools[0].routes.len(), 1);
    }

    #[test]
    fn t13_pool_with_paid_route_is_rejected() {
        let text = r#"
[[provider]]
id = "free"
base_url = "https://x.example/v1"
keys_env = "A_KEYS"
billing = "free"

[[provider]]
id = "paid"
base_url = "https://x.example/v1"
keys_env = "B_KEYS"
paid = true

[[pool]]
name = "coder"
routes = ["free/vendor/example-model", "paid/vendor/example-paid-model"]
"#;
        let msg = err(
            text,
            "pool \"coder\": route \"paid/vendor/example-paid-model\" uses paid provider \"paid\"",
        );
        assert!(msg.contains("explicit route"));

        // The same paid provider without the pool reference is fine.
        let without = text.replace(", \"paid/vendor/example-paid-model\"", "");
        let c = ok(&without);
        assert!(c.provider("paid").unwrap().paid);
    }

    #[test]
    fn t13_prd_example_with_paid_route_in_pool_is_rejected() {
        let text = PRD_EXAMPLE.replace(
            "\"groq/example-model\"]",
            "\"groq/example-model\", \"or-paid/vendor/example-paid-model\"]",
        );
        err(
            &text,
            "pool \"coder\": route \"or-paid/vendor/example-paid-model\" uses paid provider \"or-paid\"",
        );
    }

    // ---- billing ----

    #[test]
    fn unset_billing_is_paid_and_rejected_in_pool() {
        // A provider with neither billing nor paid has paid == true and billing == Billing::Paid
        let text = r#"
[[provider]]
id = "p"
base_url = "https://p.example/v1"
keys_env = "A_KEYS"
"#;
        let c = ok(text);
        let p = c.provider("p").unwrap();
        assert!(p.paid);
        assert_eq!(p.billing, Billing::Paid);

        // Putting it in a pool fails with "uses paid provider"
        let text_with_pool = r#"
[[provider]]
id = "p"
base_url = "https://p.example/v1"
keys_env = "A_KEYS"

[[pool]]
name = "coder"
routes = ["p/m"]
"#;
        err(text_with_pool, "uses paid provider");
    }

    #[test]
    fn billing_values_parse() {
        // free, credit and paid each parse
        for b in ["free", "credit", "paid"] {
            let text = format!(
                r#"
[[provider]]
id = "p"
base_url = "https://p.example/v1"
keys_env = "A_KEYS"
billing = "{}"
"#,
                b
            );
            let c = ok(&text);
            assert_eq!(c.provider("p").unwrap().billing.name(), b);
        }
        // billing = "gratis" gives the "must be one of" error
        let text = r#"
[[provider]]
id = "p"
base_url = "https://p.example/v1"
keys_env = "A_KEYS"
billing = "gratis"
"#;
        err(text, "must be one of free, credit, paid");
    }

    #[test]
    fn paid_bool_maps_to_billing_and_conflicts_are_rejected() {
        // paid = true alone is Paid
        let text = r#"
[[provider]]
id = "p"
base_url = "https://p.example/v1"
keys_env = "A_KEYS"
paid = true
"#;
        let c = ok(text);
        let p = c.provider("p").unwrap();
        assert!(p.paid);
        assert_eq!(p.billing, Billing::Paid);

        // billing = "free" with paid = true errors with "conflicts with billing"
        let text = r#"
[[provider]]
id = "p"
base_url = "https://p.example/v1"
keys_env = "A_KEYS"
billing = "free"
paid = true
"#;
        err(text, "conflicts with billing");

        // billing = "paid" with paid = true is accepted (redundant but allowed)
        let text = r#"
[[provider]]
id = "p"
base_url = "https://p.example/v1"
keys_env = "A_KEYS"
billing = "paid"
paid = true
"#;
        let c = ok(text);
        let p = c.provider("p").unwrap();
        assert!(p.paid);
        assert_eq!(p.billing, Billing::Paid);
    }

    #[test]
    fn paid_false_without_billing_is_rejected() {
        // The error contains "no longer means free"
        let text = r#"
[[provider]]
id = "p"
base_url = "https://p.example/v1"
keys_env = "A_KEYS"
paid = false
"#;
        let msg = err(text, "no longer means free");
        assert!(msg.contains("set billing = \"free\""));
    }

    // ---- limits scope ----

    #[test]
    fn bad_limit_scope_is_rejected() {
        err(
            &format!("{ONE_PROVIDER}[provider.limits]\nscope = \"account\"\n"),
            "provider \"a\": limits.scope = \"account\" must be one of provider, key, key_model",
        );
    }

    // ---- bind and auth ----

    #[test]
    fn invalid_bind_is_rejected() {
        err(
            &format!("bind = \"localhost:8787\"\n{ONE_PROVIDER}"),
            "bind = \"localhost:8787\" is not a valid socket address",
        );
        err(
            &format!("bind = \"127.0.0.1\"\n{ONE_PROVIDER}"),
            "is not a valid socket address",
        );
    }

    #[test]
    fn non_loopback_bind_requires_router_api_key() {
        let text = format!("bind = \"0.0.0.0:8787\"\n{ONE_PROVIDER}");
        err(
            &text,
            "is not a loopback address, so ROUTER_API_KEY must be set",
        );

        let mut env = default_env();
        env.insert(ROUTER_API_KEY_ENV.into(), "   ".into());
        err_with(
            &text,
            &env,
            "ROUTER_API_KEY must be set to a non-empty value",
        );

        env.insert(ROUTER_API_KEY_ENV.into(), " client-token-1234 ".into());
        let c = parse_with(&text, &env).unwrap();
        assert_eq!(
            c.client_auth.as_ref().map(ApiKey::expose),
            Some("client-token-1234")
        );
    }

    #[test]
    fn loopback_bind_does_not_require_or_enable_auth() {
        let mut env = default_env();
        env.insert(ROUTER_API_KEY_ENV.into(), "client-token-1234".into());
        for bind in ["127.0.0.1:9000", "[::1]:9000", "127.0.0.2:0"] {
            let c = parse_with(&format!("bind = \"{bind}\"\n{ONE_PROVIDER}"), &env).unwrap();
            assert!(c.client_auth.is_none());
            assert_eq!(c.bind, bind.parse::<SocketAddr>().unwrap());
        }
    }

    // ---- allowlist for model_limits / prices ----

    #[test]
    fn model_limits_outside_allowlist_is_rejected() {
        err(
            &format!("{ONE_PROVIDER}models = [\"m1\"]\n[provider.model_limits.\"m2\"]\nrpm = 1\n"),
            "provider \"a\": model_limits entry \"m2\" is not in the provider's models allowlist",
        );
    }

    #[test]
    fn prices_outside_allowlist_is_rejected() {
        err(
            &format!(
                "{ONE_PROVIDER}models = [\"m1\"]\n[provider.prices.\"m2\"]\nin_per_mtok = 1.0\nout_per_mtok = 2.0\n"
            ),
            "provider \"a\": prices entry \"m2\" is not in the provider's models allowlist",
        );
    }

    #[test]
    fn model_limits_and_prices_without_allowlist_are_accepted() {
        let c = ok(&format!(
            "{ONE_PROVIDER}[provider.model_limits.\"m2\"]\nrpm = 1\n[provider.prices.\"m2\"]\nin_per_mtok = 1.5\nout_per_mtok = 2\n"
        ));
        let p = &c.providers[0];
        assert_eq!(p.model_limits["m2"].rpm, Some(1));
        assert_eq!(
            p.prices["m2"],
            Price {
                in_per_mtok: 1.5,
                out_per_mtok: 2.0
            }
        );
    }

    #[test]
    fn negative_price_is_rejected() {
        err(
            &format!(
                "{ONE_PROVIDER}[provider.prices.\"m\"]\nin_per_mtok = -1.0\nout_per_mtok = 0.0\n"
            ),
            "prices.\"m\".in_per_mtok = -1 must be a number >= 0",
        );
    }

    // ---- numeric ranges ----

    #[test]
    fn reset_utc_hour_range() {
        assert_eq!(
            ok(&format!("{ONE_PROVIDER}reset_utc_hour = 23\n")).providers[0].reset_utc_hour,
            23
        );
        err(
            &format!("{ONE_PROVIDER}reset_utc_hour = 24\n"),
            "provider \"a\": reset_utc_hour = 24 must be between 0 and 23",
        );
        err(
            &format!("{ONE_PROVIDER}reset_utc_hour = -1\n"),
            "reset_utc_hour = -1 must be between 0 and 23",
        );
    }

    #[test]
    fn paid_daily_cap_range() {
        assert_eq!(
            ok(&format!("paid_daily_cap_usd = 2\n{ONE_PROVIDER}")).paid_daily_cap_usd,
            2.0
        );
        assert_eq!(
            ok(&format!("paid_daily_cap_usd = 0.0\n{ONE_PROVIDER}")).paid_daily_cap_usd,
            0.0
        );
        err(
            &format!("paid_daily_cap_usd = -0.5\n{ONE_PROVIDER}"),
            "paid_daily_cap_usd = -0.5 must be a number >= 0",
        );
        err(
            &format!("paid_daily_cap_usd = nan\n{ONE_PROVIDER}"),
            "paid_daily_cap_usd = NaN must be a number >= 0",
        );
    }

    #[test]
    fn zero_attempts_and_timeout_are_rejected() {
        err(
            &format!("max_attempts = 0\n{ONE_PROVIDER}"),
            "max_attempts must be at least 1",
        );
        err(
            &format!("first_byte_timeout_secs = 0\n{ONE_PROVIDER}"),
            "first_byte_timeout_secs must be at least 1",
        );
        assert_eq!(
            ok(&format!("max_wait_secs = 0\n{ONE_PROVIDER}")).max_wait_secs,
            0
        );
    }

    // ---- drop_params ----

    #[test]
    fn drop_params_are_kept_and_model_is_rejected() {
        let c = ok(&format!(
            "{ONE_PROVIDER}drop_params = [\"logprobs\", \"seed\"]\n"
        ));
        assert_eq!(c.providers[0].drop_params, ["logprobs", "seed"]);
        err(
            &format!("{ONE_PROVIDER}drop_params = [\"model\"]\n"),
            "drop_params must not contain \"model\"",
        );
        err(
            &format!("{ONE_PROVIDER}drop_params = [\"\"]\n"),
            "drop_params contains an empty entry",
        );
    }

    // ---- unknown fields ----

    #[test]
    fn unknown_fields_are_rejected_everywhere() {
        err(&format!("bnd = \"127.0.0.1:1\"\n{ONE_PROVIDER}"), "bnd");
        err(
            &format!("{ONE_PROVIDER}trains_on_inputs = true\n"),
            "trains_on_inputs",
        );
        err(
            &format!("{ONE_PROVIDER}[provider.limits]\nrmp = 1\n"),
            "rmp",
        );
        err(
            &format!("{ONE_PROVIDER}[provider.model_limits.\"m\"]\nscope = \"key\"\n"),
            "scope",
        );
        err(
            &format!(
                "{ONE_PROVIDER}[provider.prices.\"m\"]\nin_per_mtok = 1.0\nout_per_mtok = 1.0\ncached = 1.0\n"
            ),
            "cached",
        );
        err(
            &with_pool("name = \"p\"\nroutes = [\"a/m\"]\nstrategey = \"priority\""),
            "strategey",
        );
    }

    #[test]
    fn invalid_toml_is_reported() {
        err("bind = ", "cannot parse TOML");
    }

    // ---- reporting ----

    #[test]
    fn all_problems_are_reported_together() {
        let text = format!(
            "bind = \"nope\"\n{ONE_PROVIDER}reset_utc_hour = 99\n[[pool]]\nname = \"p\"\nroutes = [\"zzz/m\"]\n"
        );
        let e = parse(&text).unwrap_err();
        assert_eq!(e.problems().len(), 3, "{e}");
        let msg = e.to_string();
        assert!(msg.starts_with("3 problems:"));
        assert!(msg.contains("bind"));
        assert!(msg.contains("reset_utc_hour"));
        assert!(msg.contains("zzz"));
    }
}
