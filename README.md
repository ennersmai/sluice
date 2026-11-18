# sluice

A small local router for LLM APIs. It speaks the OpenAI chat-completions protocol, so OpenCode (or anything else) points at one address, and sluice spreads requests over your free-tier providers: it tracks quotas, backs off on rate limits, fails over to the next route, and shows it all on a local dashboard. One binary, no runtime dependencies, about 5 MB of RAM when idle.

## Quick start

1. Copy `.env.example` to `.env` and put your keys in it (a `*_KEYS` variable may hold a comma-separated list, but only use several keys if they carry genuinely separate quotas; most providers count per account).
2. Copy `router.example.toml` to `router.toml` and edit it (see below).
3. Double-click `run.bat` (or run `sluice.exe`; on Linux `./sluice`). It must start in the folder holding `.env` and `router.toml`, or pass `--config <path>`.
4. Open http://127.0.0.1:8787/ for the dashboard.

Restart to apply config changes; startup is near instant and counters and long cooldowns persist in `state.json`.

## Config reference (`router.toml`)

Top level: `bind` (default `127.0.0.1:8787`; any non-loopback address requires `ROUTER_API_KEY` in `.env`, checked as a bearer token on every path), `state_file`, `log_file` (optional; also append every request line and warning to this file), `max_attempts`, `first_byte_timeout_secs` (time from sending the request until the first non-comment body data of a 2xx response (SSE `:` keep-alive lines do not count), not just the headers; the error-body read of non-2xx responses is bounded by it too; nothing is sent to the client before then, so a stall fails over to the next route and counts toward that route's breaker), `stream_idle_timeout_secs` (default 60, 0 disables: once the body has started, this much silence ends a stream with an SSE `error` event with code `idle_timeout`, or gives a non-streaming request a 504; also counts toward the route's breaker), `max_wait_secs` (hold a request this long if a route frees up soon, 0 disables), `private_default`, `paid_daily_cap_usd`.

Each `[[provider]]`:

| Field | Meaning |
|---|---|
| `id`, `base_url`, `keys_env` | Name, OpenAI-compatible base URL, and the `.env` variable holding its key(s). Omit `keys_env` for a keyless gateway: it has one anonymous key slot, no `Authorization` header is sent, and the key shows as `none`. When set it must exist and be non-empty |
| `paid` | Paid providers are never used by pools, only by an explicit `provider/model` request |
| `trains_on_input` | Set per provider after reading its terms (see below) |
| `reset_utc_hour` | Hour (UTC) when the provider's daily quota resets |
| `drop_params` | Request fields this provider rejects, removed before sending |
| `models` | Allowlist of upstream model ids; omit to allow any |
| `[provider.limits]` | `scope` (`provider`, `key`, `key_model`), `rpm`, `rpd`, `tpm`, `tpd` |
| `[provider.model_limits."id"]` | Optional per-model override |
| `[provider.prices."id"]` | `in_per_mtok`, `out_per_mtok` in USD (paid providers) |

**Copy real limits from each provider's dashboard.** Free-tier numbers published in blog posts and docs go stale, and sluice never hardcodes any. `0` (or leaving a limit out) means unknown: sluice then relies on the provider's 429 responses and `Retry-After` to cool a route down. Known limits let it skip a route without wasting a request.

**`trains_on_input`**: read each provider's terms. If free-tier prompts may be used for training, set `trains_on_input = true`. A request with header `x-router-private: 1` (or `private_default = true`) will never reach such a provider.

**Paid providers and the cap**: a `paid = true` provider is reached only by an explicit address such as `or-paid/vendor/model`, never through a pool. Spend is computed from `prices` and the reported token usage, and requests are refused once `paid_daily_cap_usd` is reached for the day (`0` blocks paid entirely). The cap survives restarts.

**Pools and strategies**: a `[[pool]]` has a `name`, a list of `routes` (`provider/upstream-model`) and a `strategy`: `priority` (first healthy route in order), `round_robin`, or `least_used`. Pools cannot contain paid routes. Clients ask for the pool name as the `model`.

**Failure handling**: 401 and 402 disable that key until restart. A 403 marks only that provider/model route `unavailable` until restart (region or plan restrictions) and the next candidate is tried. HTTP 5xx responses count toward a per-route breaker (3 in a row opens that route for 60 s); connect errors and first-byte timeouts count toward a provider-wide breaker.

## Filling allowlists with `sluice models`

```
sluice models or-free --filter :free
sluice models groq --filter llama --config router.toml
```

Calls the provider's `GET /models` with its first key and prints matching ids, sorted, one per line, ready to paste into `models = [...]`.

## OpenCode

```json
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "router": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "Local Router",
      "options": {
        "baseURL": "http://127.0.0.1:8787/v1",
        "apiKey": "local"
      },
      "models": {
        "coder": { "name": "coder (router pool)" }
      }
    }
  }
}
```

## Endpoints

| Path | Purpose |
|---|---|
| `POST /v1/chat/completions` | Main endpoint, streaming and not |
| `GET /v1/models` | Pool names and allowlisted `provider/model` ids |
| `GET /health` | 200 when up |
| `GET /` | Dashboard |
| `GET /status`, `GET /events` | JSON snapshot, server-sent events |
| `POST /admin/provider/{id}/pause`, `/resume` | Take a provider out of rotation and back |
| `POST /admin/cooldowns/clear[?provider=id]` | Clear cooldowns and open breakers |

Response headers: `x-router-provider`, `x-router-upstream-model`, `x-router-attempts`, `x-router-overhead-ms`. Request header: `x-router-private: 1`.

## Known limitations

- The dashboard needs a loopback bind. Browsers' `EventSource` cannot send a bearer token, so with a non-loopback `bind` (where `ROUTER_API_KEY` is required) the page and its event stream will be refused. API clients are unaffected.
- Keys are tracked by their position in the `*_KEYS` list, so persisted counters and cooldowns follow the index. Do not reorder or remove entries in a `*_KEYS` variable; append new keys at the end.
- Single machine, in-memory counters flushed to `state.json`; no clustering.

## Releases

Pushing any git tag builds a Windows x64 `.exe` and a static Linux musl binary and attaches both to a GitHub Release.
