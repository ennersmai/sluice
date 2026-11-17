//! `sluice models <provider_id>`: list the model ids a provider offers, to
//! help fill allowlists. Never prints a key.

use std::time::Duration;

use serde_json::Value;

use crate::config::Provider;

const TIMEOUT: Duration = Duration::from_secs(20);

/// Call `GET {base_url}/models` with the provider's first key and return the
/// ids, sorted and de-duplicated, keeping only those containing `filter`
/// (case-insensitive).
pub async fn list_models(
    client: &reqwest::Client,
    provider: &Provider,
    filter: Option<&str>,
) -> Result<Vec<String>, String> {
    let key = provider
        .keys
        .first()
        .ok_or_else(|| format!("provider \"{}\" has no API key", provider.id))?;
    let url = format!("{}/models", provider.base_url.trim_end_matches('/'));
    let response = client
        .get(&url)
        .bearer_auth(key.expose())
        .timeout(TIMEOUT)
        .send()
        .await
        .map_err(|e| format!("GET {url} failed: {}", e.without_url()))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("GET {url} returned HTTP {status}"));
    }
    let body = response
        .bytes()
        .await
        .map_err(|e| format!("reading response from {url} failed: {}", e.without_url()))?;
    let json: Value = serde_json::from_slice(&body)
        .map_err(|_| format!("response from {url} is not valid JSON"))?;
    Ok(extract_ids(&json, filter))
}

/// Accepts `{"data": [{"id": ..}]}` (OpenAI), `{"models": [..]}` and a bare
/// array; entries are objects with `id` or `name`, or plain strings.
pub fn extract_ids(json: &Value, filter: Option<&str>) -> Vec<String> {
    let list = json
        .get("data")
        .or_else(|| json.get("models"))
        .unwrap_or(json);
    let needle = filter.map(str::to_lowercase);
    let mut ids: Vec<String> = list
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| match m {
            Value::String(s) => Some(s.clone()),
            other => other
                .get("id")
                .or_else(|| other.get("name"))
                .and_then(Value::as_str)
                .map(str::to_string),
        })
        .filter(|id| match &needle {
            Some(n) => id.to_lowercase().contains(n),
            None => true,
        })
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_sorts_and_filters() {
        let j = json!({"data": [{"id": "b/Llama"}, {"id": "a/qwen"}, {"id": "b/Llama"}]});
        assert_eq!(extract_ids(&j, None), ["a/qwen", "b/Llama"]);
        assert_eq!(extract_ids(&j, Some("LLAMA")), ["b/Llama"]);
        assert_eq!(extract_ids(&json!(["x", "y"]), Some("y")), ["y"]);
        assert!(extract_ids(&json!({"error": 1}), None).is_empty());
    }
}
