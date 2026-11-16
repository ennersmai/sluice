//! Model resolution: turns the request's `model` field into an ordered list
//! of candidate routes and enforces provider allowlists.
//!
//! Resolution order (PRD section 4):
//!
//! 1. Explicit route `provider_id/upstream_model`, split at the first `/`,
//!    when the first segment is a configured provider id. The remainder is
//!    passed upstream verbatim.
//! 2. Pool name.
//! 3. Otherwise unknown, with close matches as suggestions.
//!
//! Everything here is pure (no I/O, no clock), so it is unit tested directly.

use crate::config::{Config, Provider};

/// At most this many suggestions are returned for an unknown model.
pub const MAX_SUGGESTIONS: usize = 5;
/// Requested names longer than this (in chars) get no edit-distance
/// suggestions, which keeps the quadratic distance computation bounded.
const MAX_SUGGEST_QUERY_CHARS: usize = 256;
/// Substring matches need at least this many chars on the contained side, so
/// one- or two-letter names do not match everything.
const MIN_SUBSTRING_CHARS: usize = 3;

/// One route to try: a provider and the upstream model id to send it.
#[derive(Debug, Clone, Copy)]
pub struct Candidate<'a> {
    pub provider: &'a Provider,
    pub model: &'a str,
}

/// How the requested model was addressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteKind<'a> {
    /// `provider_id/upstream_model`.
    Explicit,
    /// A `[[pool]]` with this name.
    Pool(&'a str),
}

/// A successfully resolved model. `candidates` is never empty and is in the
/// pool's configured order.
#[derive(Debug, Clone)]
pub struct Resolution<'a> {
    pub kind: RouteKind<'a>,
    pub candidates: Vec<Candidate<'a>>,
}

impl<'a> Resolution<'a> {
    pub fn first(&self) -> Option<Candidate<'a>> {
        self.candidates.first().copied()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    /// Neither an explicit route to a known provider nor a pool name.
    UnknownModel { suggestions: Vec<String> },
    /// Explicit route whose upstream model is outside the provider's
    /// `models` allowlist.
    NotInAllowlist {
        provider: String,
        suggestions: Vec<String>,
    },
}

impl ResolveError {
    pub fn suggestions(&self) -> &[String] {
        match self {
            Self::UnknownModel { suggestions } | Self::NotInAllowlist { suggestions, .. } => {
                suggestions
            }
        }
    }
}

/// Resolve `requested` against the config. For explicit routes the upstream
/// model borrows from `requested` (it is passed upstream verbatim).
pub fn resolve<'a>(config: &'a Config, requested: &'a str) -> Result<Resolution<'a>, ResolveError> {
    if let Some((provider_id, upstream)) = requested.split_once('/')
        && let Some(provider) = config.provider(provider_id)
    {
        if upstream.is_empty() {
            return Err(ResolveError::UnknownModel {
                suggestions: suggest(config, requested),
            });
        }
        if !provider.allows_model(upstream) {
            return Err(ResolveError::NotInAllowlist {
                provider: provider.id.clone(),
                suggestions: suggest(config, requested),
            });
        }
        return Ok(Resolution {
            kind: RouteKind::Explicit,
            candidates: vec![Candidate {
                provider,
                model: upstream,
            }],
        });
    }

    if let Some(pool) = config.pool(requested) {
        let candidates: Vec<Candidate<'a>> = pool
            .routes
            .iter()
            .filter_map(|route| {
                config.provider(&route.provider).map(|provider| Candidate {
                    provider,
                    model: route.model.as_str(),
                })
            })
            .collect();
        if !candidates.is_empty() {
            return Ok(Resolution {
                kind: RouteKind::Pool(pool.name.as_str()),
                candidates,
            });
        }
    }

    Err(ResolveError::UnknownModel {
        suggestions: suggest(config, requested),
    })
}

/// Close matches for `requested` among pool names and the `provider/model`
/// ids of allowlisted models on free providers (paid providers are never
/// suggested, so a typo cannot steer a client towards spending money).
///
/// A name matches when one contains the other (ignoring case), or when the
/// edit distance to the whole name, or to its part after the provider id, is
/// small relative to the requested length. Results are ordered by match
/// kind, then distance, then name, and capped at [`MAX_SUGGESTIONS`].
pub fn suggest(config: &Config, requested: &str) -> Vec<String> {
    let query_len = requested.chars().count();
    if query_len == 0 || query_len > MAX_SUGGEST_QUERY_CHARS {
        return Vec::new();
    }
    let query = requested.to_lowercase();

    let names = config.pools.iter().map(|p| p.name.clone()).chain(
        config.providers.iter().filter(|p| !p.paid).flat_map(|p| {
            p.models
                .iter()
                .flatten()
                .map(move |m| format!("{}/{m}", p.id))
        }),
    );

    let threshold = (query_len / 3).max(2);
    let mut scored: Vec<(u8, usize, String)> = names
        .filter_map(|name| {
            let lower = name.to_lowercase();
            let tail = lower.split_once('/').map(|(_, t)| t);
            let distance = tail
                .map(|t| edit_distance(&query, t))
                .into_iter()
                .chain([edit_distance(&query, &lower)])
                .min()
                .unwrap_or(usize::MAX);
            let substring = contains_long(&lower, &query)
                || contains_long(&query, &lower)
                || tail.is_some_and(|t| contains_long(&query, t));
            if substring {
                Some((0, distance, name))
            } else if distance <= threshold {
                Some((1, distance, name))
            } else {
                None
            }
        })
        .collect();

    scored.sort();
    scored.dedup_by(|a, b| a.2 == b.2);
    scored
        .into_iter()
        .take(MAX_SUGGESTIONS)
        .map(|(_, _, name)| name)
        .collect()
}

/// `haystack` contains `needle`, and `needle` is long enough to be meaningful.
fn contains_long(haystack: &str, needle: &str) -> bool {
    needle.chars().count() >= MIN_SUBSTRING_CHARS && haystack.contains(needle)
}

/// Levenshtein distance over chars, two rows of memory.
fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur: Vec<usize> = vec![0; b.len() + 1];
    for (i, ca) in a.chars().enumerate() {
        let mut left = i + 1;
        let mut diag = i;
        cur.clear();
        cur.push(left);
        for (&cb, &up) in b.iter().zip(prev.iter().skip(1)) {
            let cost = usize::from(ca != cb);
            let value = (diag + cost).min(up + 1).min(left + 1);
            cur.push(value);
            diag = up;
            left = value;
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev.last().copied().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG: &str = r#"
[[provider]]
id = "alpha"
base_url = "https://alpha.example/v1"
keys_env = "K"
models = ["vendor/model-a", "vendor/some-model:free"]

[[provider]]
id = "beta"
base_url = "https://beta.example/v1"
keys_env = "K"

[[provider]]
id = "money"
base_url = "https://alpha.example/v1"
keys_env = "K"
paid = true
models = ["vendor/model-a-pro"]

[[pool]]
name = "coder"
routes = ["beta/fast-model", "alpha/vendor/model-a"]

[[pool]]
name = "writer"
routes = ["alpha/vendor/some-model:free"]
"#;

    fn config() -> Config {
        match Config::from_toml_str(CONFIG, &|_| Some("key-000000001".to_string())) {
            Ok(c) => c,
            Err(e) => panic!("test config invalid: {e}"),
        }
    }

    fn ids(r: &Resolution<'_>) -> Vec<(String, String)> {
        r.candidates
            .iter()
            .map(|c| (c.provider.id.clone(), c.model.to_string()))
            .collect()
    }

    fn pair(p: &str, m: &str) -> (String, String) {
        (p.to_string(), m.to_string())
    }

    #[test]
    fn explicit_route_splits_at_first_slash() {
        let c = config();
        let r = resolve(&c, "alpha/vendor/some-model:free").unwrap();
        assert_eq!(r.kind, RouteKind::Explicit);
        assert_eq!(ids(&r), [pair("alpha", "vendor/some-model:free")]);
    }

    #[test]
    fn explicit_route_without_allowlist_accepts_any_model() {
        let c = config();
        let r = resolve(&c, "beta/anything/at:all").unwrap();
        assert_eq!(ids(&r), [pair("beta", "anything/at:all")]);
    }

    #[test]
    fn explicit_route_outside_allowlist_is_rejected_with_suggestions() {
        let c = config();
        match resolve(&c, "alpha/vendor/model-b") {
            Err(ResolveError::NotInAllowlist {
                provider,
                suggestions,
            }) => {
                assert_eq!(provider, "alpha");
                assert_eq!(
                    suggestions.first().map(String::as_str),
                    Some("alpha/vendor/model-a")
                );
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn explicit_route_with_empty_model_is_unknown() {
        let c = config();
        assert!(matches!(
            resolve(&c, "alpha/"),
            Err(ResolveError::UnknownModel { .. })
        ));
    }

    #[test]
    fn explicit_paid_route_resolves_paid_provider() {
        // Refusing paid routes is the proxy's job; resolution reports them.
        let c = config();
        let r = resolve(&c, "money/vendor/model-a-pro").unwrap();
        assert!(r.first().unwrap().provider.paid);
    }

    #[test]
    fn pool_resolves_to_ordered_routes() {
        let c = config();
        let r = resolve(&c, "coder").unwrap();
        assert_eq!(r.kind, RouteKind::Pool("coder"));
        assert_eq!(
            ids(&r),
            [pair("beta", "fast-model"), pair("alpha", "vendor/model-a")]
        );
        let first = r.first().unwrap();
        assert_eq!(
            (first.provider.id.as_str(), first.model),
            ("beta", "fast-model")
        );
    }

    #[test]
    fn unknown_first_segment_is_not_an_explicit_route() {
        let c = config();
        assert!(matches!(
            resolve(&c, "vendor/model-a"),
            Err(ResolveError::UnknownModel { .. })
        ));
        assert!(matches!(
            resolve(&c, "gamma/x"),
            Err(ResolveError::UnknownModel { .. })
        ));
    }

    #[test]
    fn names_are_case_sensitive() {
        let c = config();
        assert!(resolve(&c, "Coder").is_err());
        assert!(resolve(&c, "ALPHA/vendor/model-a").is_err());
    }

    #[test]
    fn unknown_model_suggests_close_matches() {
        let c = config();
        // Typo of a pool name.
        assert_eq!(resolve(&c, "codr").unwrap_err().suggestions(), ["coder"]);
        // Upstream id without the provider prefix.
        assert_eq!(
            resolve(&c, "vendor/model-a").unwrap_err().suggestions(),
            ["alpha/vendor/model-a"]
        );
        // Case-insensitive matching.
        assert_eq!(resolve(&c, "WRITER").unwrap_err().suggestions(), ["writer"]);
        // Nothing close.
        assert!(
            resolve(&c, "completely-unrelated-name-xyz")
                .unwrap_err()
                .suggestions()
                .is_empty()
        );
    }

    #[test]
    fn paid_models_are_never_suggested() {
        let c = config();
        let s = suggest(&c, "vendor/model-a-pro");
        assert!(s.iter().all(|n| !n.starts_with("money/")), "{s:?}");
    }

    #[test]
    fn suggestions_are_capped() {
        let mut text = CONFIG.to_string();
        for i in 0..20 {
            text.push_str(&format!(
                "[[pool]]\nname = \"coder{i}\"\nroutes = [\"beta/m\"]\n"
            ));
        }
        let c = Config::from_toml_str(&text, &|_| Some("key-000000001".to_string())).unwrap();
        let s = suggest(&c, "coder");
        assert_eq!(s.len(), MAX_SUGGESTIONS);
        assert_eq!(s.first().map(String::as_str), Some("coder"));
    }

    #[test]
    fn empty_and_huge_queries_get_no_suggestions() {
        let c = config();
        assert!(suggest(&c, "").is_empty());
        assert!(suggest(&c, &"coder".repeat(1000)).is_empty());
    }

    #[test]
    fn edit_distance_basics() {
        assert_eq!(edit_distance("", ""), 0);
        assert_eq!(edit_distance("abc", ""), 3);
        assert_eq!(edit_distance("", "abc"), 3);
        assert_eq!(edit_distance("kitten", "sitting"), 3);
        assert_eq!(edit_distance("coder", "codr"), 1);
        assert_eq!(edit_distance("héllo", "hello"), 1);
    }
}
