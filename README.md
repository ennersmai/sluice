# sluice

A small local router for LLM APIs. It speaks the OpenAI chat-completions protocol, so OpenCode (or anything else) points at one address, and sluice spreads requests over your free-tier providers: it tracks quotas, backs off on rate limits, fails over to the next route, and shows it all on a local dashboard. One binary, no runtime dependencies, about 5 MB of RAM when idle.

## Quick start

1. Copy `.env.example` to `.env` and put your keys in it (a `*_KEYS` variable may hold a comma-separated list, but only use several keys if they carry genuinely separate quotas; most providers count per account).
2. Copy `router.example.toml` to `router.toml` and edit it (see below).
3. Double-click `run.bat` (or run `sluice.exe`; on Linux `./sluice`). It must start in the folder holding `.env` and `router.toml`, or pass `--config <path>`.
4. Open http://127.0.0.1:8787/ for the dashboard.

Restart to apply config changes; startup is near instant and counters and long cooldowns persist in `state.json`.

## Config reference (`router.toml`)

Top level: `bind` (default `127.0.0.1:8787`; any non-loopback address requires `ROUTER_API_KEY` in `.env`, checked as a bearer token on every path), `state_file`, `log_file` (optional; also append every request line and warning to this file), `max_attempts`, `first_byte_timeout_secs` (time from sending the request until the first non-comment body data of a 2xx response (SSE `:` keep-alive lines do not count), not just the headers; the error-body read of non-2xx responses is bounded by it too; nothing is sent to the client before then, so a stall fails over to the next route and counts toward that route's breaker), `stream_idle_timeout_secs` (default 60, 0 disables: once the body has started, this much silence ends a stream with an SSE `error` event with code `idle_timeout`, or gives a non-streaming request a 504; also counts toward the route's breaker), `max_wait_secs` (hold a request this long if a route frees up soon, 0 disables), `private_default`, `paid_daily_cap_usd`, `session_headers` (default `["x-session-id", "x-opencode-session"]`: request headers that name a session for stickiness, see below), `sticky_ttl_secs` (default 7200; how long an idle session stays stuck to its route, 0 disables stickiness).

Each `[[provider]]`:

| Field | Meaning |
|---|---|
| `id`, `base_url`, `keys_env` | Name, OpenAI-compatible base URL, and the `.env` variable holding its key(s). Omit `keys_env` for a keyless gateway: it has one anonymous key slot, no `Authorization` header is sent, and the key shows as `none`. When set it must exist and be non-empty |
| `billing` | `free` \| `credit` \| `paid`. Unset means `paid`. Only `free` and `credit` providers may appear in pools; `paid` ones are reached only by an explicit `provider/model` request. The legacy `paid = true` still means paid; `paid = false` without `billing` is rejected (write `billing = "free"`), and conflicting combinations are rejected |
| `credit_usd` | `billing = "credit"` only, required, > 0: the prepaid credit per period, in USD |
| `credit_reset_day` | 1-31, required for credit. Day of the month the credit period starts, at `reset_utc_hour`; a day past the end of a short month means that month's last day. Also sets the month boundary for `tpmo` (on any provider; default the 1st) |
| `credit_spent_usd`, `credit_spent_date` | Credit only, set together: the seed for spend that happened outside sluice, e.g. `credit_spent_usd = 10.0`, `credit_spent_date = "2026-11-18"` (`YYYY-MM-DD`). It applies only to the credit period containing that date (it expires by itself at the next reset) and acts as a floor, `max(tracked, seed)`, so restarts never double-count it |
| `usage_cost_field` | Name of a cost-in-USD field inside the response's `usage` object; when present it is used instead of the price estimate. For OpenRouter use `"cost"` |
| `trains_on_input` | Set per provider after reading its terms (see below) |
| `reset_utc_hour` | Hour (UTC) when the provider's daily quota resets |
| `drop_params` | Request fields this provider rejects, removed before sending |
| `models` | Allowlist of upstream model ids; omit to allow any |
| `[provider.limits]` | `scope` (`provider`, `key`, `key_model`), `rpm`, `rpd`, `tpm`, `tpd`, `tpmo` (tokens per month; the month starts on `credit_reset_day`, else the 1st, at `reset_utc_hour`; persists across restarts; not allowed in `model_limits`) |
| `[provider.model_limits."id"]` | Optional per-model override: `rpm`, `rpd`, `tpm`, `tpd` and `max_input_tokens` (largest request, in sluice's pre-flight token estimate, the model accepts; 0 = unknown) |
| `[provider.prices."id"]` | `in_per_mtok`, `out_per_mtok` in USD. Required (at least one) for `credit` providers, and sluice refuses to start without them. A model with no entry is charged at the highest configured price (input and output maxima taken separately). Free providers without prices count as $0. Spend is estimated for every provider and shown on the dashboard |

**Learned size ceilings.** When a pool route refuses a request as too large (HTTP 413, or a 400/422 naming a context-length limit), sluice records that request's pre-flight token estimate as the route's ceiling and skips the route (reason `too_large`, no upstream call) for any request at least that big. The lower of a configured `max_input_tokens` and the learned value applies; a learned ceiling is never raised automatically, is saved in the state file, and is cleared by the admin endpoints above or when the provider/model leaves the config. `/status` shows `ceiling_tokens` and `ceiling_source` per model.

**Copy real limits from each provider's dashboard.** Free-tier numbers published in blog posts and docs go stale, and sluice never hardcodes any. `0` (or leaving a limit out) means unknown: sluice then relies on the provider's 429 responses and `Retry-After` to cool a route down. Known limits let it skip a route without wasting a request.

**`trains_on_input`**: read each provider's terms. If free-tier prompts may be used for training, set `trains_on_input = true`. A request with header `x-router-private: 1` (or `private_default = true`) will never reach such a provider.

**Credit providers**: a `billing = "credit"` provider (for example Mistral's prepaid credit) has its spend tracked per credit period, from `usage_cost_field` when the provider reports a cost, else from `prices`. Once period spend reaches 90% of `credit_usd`, the provider is skipped before any upstream call (skip reason `credit_exhausted`, shown on the dashboard with the reset date) until the next `credit_reset_day`. Spend persists in `state.json` across restarts. If credit was already spent outside sluice, seed it in config; for Mistral's $10 that is already used up this month:

```toml
[[provider]]
id = "mistral"
base_url = "https://api.mistral.ai/v1"
keys_env = "MISTRAL_KEYS"
billing = "credit"
credit_usd = 10.0
credit_reset_day = 1
credit_spent_usd = 10.0
credit_spent_date = "2026-11-18"   # any date inside the current credit period
[provider.prices."mistral-model-id"]
in_per_mtok = 0.0                   # copy real prices from the provider
out_per_mtok = 0.0
```

A seed dated in an earlier period is ignored with a warning at startup.

**Paid providers and the cap**: a `billing = "paid"` provider is reached only by an explicit address such as `or-paid/vendor/model`, never through a pool. Spend is computed from `prices` and the reported token usage, and requests are refused once `paid_daily_cap_usd` is reached for the day (`0` blocks paid entirely). The cap survives restarts.

**Pools and strategies**: a `[[pool]]` has a `name`, a list of `routes` (`provider/upstream-model`) and a `strategy`: `priority` (first healthy route in order), `round_robin`, or `least_used`. Pools cannot contain paid routes. Clients ask for the pool name as the `model`.

**Failure handling**: a 401 disables that key until restart. A 402 (payment required) disables the whole provider (every key, model, pool and explicit route) until cleared with `POST /admin/provider/{id}/payment/clear` or the dashboard's "Clear 402" button; the block survives restarts, is not lifted by clearing cooldowns, and is logged at error level. The 429 and 402 log lines include a sanitised excerpt of the upstream error body (at most 300 chars, one line, keys redacted). A Google-style 429 whose `QuotaFailure` names a per-day quota (`...PerDay...`) cools the route until the provider's next `reset_utc_hour`, so an exhausted daily quota is not retried all night; Google's `RetryInfo` delay is used otherwise. For Gemini set `reset_utc_hour = 8` (Google resets at midnight Pacific; 8 is safe in both PST and PDT). A 403 marks only that provider/model route `unavailable` until restart (region or plan restrictions) and the next candidate is tried. HTTP 5xx responses count toward a per-route breaker (3 in a row opens that route for 60 s); connect errors and first-byte timeouts count toward a provider-wide breaker.

**Session stickiness**: once a pool request from a session is served by a route, later requests from that session try that route first while it is eligible. The session is named by the first non-empty header from `session_headers`; without one, it is derived from the requested model and the opening of the first system/developer and first user message (no user message, no session). Only a hash is kept in memory, never message text or header values. If the stuck route is not eligible (cooling, paused, out of quota), the normal order applies and the session re-sticks to whichever route serves it. Stickiness never bypasses eligibility and does not apply to explicit routes. Entries expire after `sticky_ttl_secs` and at most 4096 are kept. The dashboard shows the count.

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
| `POST /admin/cooldowns/clear[?provider=id]` | Clear cooldowns, open breakers and learned size ceilings |
| `POST /admin/ceilings/clear[?provider=id]` | Clear learned size ceilings only |
| `POST /admin/provider/{id}/payment/clear` | Lift a 402 (payment required) block |

Response headers: `x-router-provider`, `x-router-upstream-model`, `x-router-attempts`, `x-router-overhead-ms`. Request header: `x-router-private: 1`.

## Known limitations

- The dashboard needs a loopback bind. Browsers' `EventSource` cannot send a bearer token, so with a non-loopback `bind` (where `ROUTER_API_KEY` is required) the page and its event stream will be refused. API clients are unaffected.
- Keys are tracked by their position in the `*_KEYS` list, so persisted counters and cooldowns follow the index. Do not reorder or remove entries in a `*_KEYS` variable; append new keys at the end.
- Single machine, in-memory counters flushed to `state.json`; no clustering.

## Releases

Pushing any git tag builds a Windows x64 `.exe` and a static Linux musl binary and attaches both to a GitHub Release.
