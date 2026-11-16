# PRD: Local LLM Router (Rust)

**Owner:** Mai
**Status:** v2 draft for review, then agent execution
**Working name:** `llm-router` (rename freely, the binary name is set in one place)

**Changes in v2:** web dashboard added, paid handling simplified, limits can be shared across a whole account, per-provider model allowlists, a model discovery command.

---

## 1. What this is

A single small executable that exposes one OpenAI-compatible endpoint on localhost and forwards each request to whichever configured upstream provider can serve it right now. It tracks quotas, cools down routes that are rate-limited, fails over before the client notices, and shows what it is doing on a local web page.

It is a transport. It has no opinion about what a request is for. The calling app picks the model, the router gets it served.

### Goals

1. One endpoint for every client: OpenCode first, Aura and anything else later.
2. Any number of providers, added by editing a config file, with zero code changes.
3. Never stall an agent loop while any eligible route is healthy.
4. Small and fast enough to forget it is running.
5. Paid models are never reached by accident. They are used only when a request names one explicitly.
6. A live dashboard showing per-provider usage and how each request was routed.

### Non-goals (v1)

- No built-in tiers, personas or workload concepts.
- No Anthropic `/v1/messages` format, embeddings, images or audio.
- No database, multi-user auth, caching or prompt compression.
- No config editing from the dashboard. Config lives in files.
- No adapters for providers that are not OpenAI-compatible. They can be added later behind the same route interface.

---

## 2. Hard constraints

Reference machine: Intel i5-4690 (4 cores), 16 GB RAM, Windows.

| Constraint | Target | How it is verified |
|---|---|---|
| Distribution | One static `.exe` (Windows x64) and one Linux musl binary | CI artifacts |
| Runtime dependencies | None, dashboard included in the binary | Runs on a clean machine, offline |
| Startup | Under 200 ms to accepting connections | Timed in CI |
| Idle memory | Under 20 MB RSS | Measured in soak test |
| Memory under 20 concurrent streams, dashboard open | Under 60 MB RSS | Measured in soak test |
| Added latency to first token | Under 2 ms p95 against a local mock upstream | Benchmark test |
| Binary size | Under 12 MB stripped | CI check |
| Launch | `run.bat` double-click, or `llm-router.exe` | Manual |

---

## 3. API surface

Binds to `127.0.0.1:8787` by default. If `bind` is set to anything other than loopback, `ROUTER_API_KEY` becomes mandatory and is checked as a bearer token on every path, dashboard included.

| Method and path | Purpose |
|---|---|
| `POST /v1/chat/completions` | Main endpoint, streaming and non-streaming |
| `GET /v1/models` | Lists every addressable model id (see section 4) |
| `GET /health` | Returns 200 when the process is up |
| `GET /` | Dashboard (section 10) |
| `GET /status` | JSON snapshot of routes, cooldowns, counters and recent requests |
| `GET /events` | Server-sent events stream feeding the dashboard |
| `POST /admin/provider/{id}/pause` and `/resume` | Take a provider out of rotation and put it back |
| `POST /admin/cooldowns/clear` | Clear cooldowns, optionally for one provider |

### Response headers added by the router

- `x-router-provider`: provider id that served the request
- `x-router-upstream-model`: model id sent upstream
- `x-router-attempts`: number of routes tried
- `x-router-overhead-ms`: time spent inside the router before the upstream call

### Request headers understood by the router

- `x-router-private: 1` excludes any provider marked `trains_on_input = true`. Can be made the default in config.

---

## 4. Model addressing

There is one mechanism. The `model` field of the request is resolved in this order:

1. **Explicit route:** `provider_id/upstream_model` (example: `or-free/vendor/some-model:free`). The first path segment is the provider id, the rest is passed upstream verbatim. Sent to that provider only. Key rotation applies, cross-provider failover does not.
2. **Pool:** a name defined under `[[pool]]` in config, with an ordered list of routes. The router fails over across those routes.
3. **No match:** 404 with a JSON error listing close matches.

A pool with one route is a plain alias. If no pools are defined, the router is a pure passthrough with key rotation.

### Model allowlists

Each provider may declare `models = [...]`. When present, only those upstream ids are routable through that provider and anything else is rejected with 404 before any upstream call. This is the guard that keeps an agent from wandering onto a model you did not intend.

### Paid models

- A provider block is either free or `paid = true`. To use both free and paid models from one vendor, define two provider blocks with the same `base_url` (example: `or-free` and `or-paid`).
- Paid providers are reachable only by explicit route. Config validation rejects any pool that contains a paid route, so failover can never spend money.
- `paid_daily_cap_usd` remains as a last safety net for explicit paid requests.

---

## 5. Configuration

Secrets live in `.env`. Everything else lives in `router.toml`. Restart to apply changes (startup is near instant and state persists).

### `.env`

```
OPENROUTER_KEYS=key1
NVIDIA_KEYS=key1
GROQ_KEYS=key1
GEMINI_KEYS=key1
ROUTER_API_KEY=
```

A `*_KEYS` variable may hold a comma-separated list. Use this only for keys that carry genuinely separate quotas. Most providers count limits per account or organisation, so extra keys on one account add nothing.

### `router.toml`

```toml
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
paid = false
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
paid = true
models = ["vendor/example-paid-model"]
[provider.prices."vendor/example-paid-model"]
in_per_mtok = 0.0
out_per_mtok = 0.0

[[provider]]
id = "gemini"
base_url = "https://generativelanguage.googleapis.com/v1beta/openai"
keys_env = "GEMINI_KEYS"
trains_on_input = true

[[pool]]
name = "coder"
strategy = "priority"       # priority | round_robin | least_used
routes = ["or-free/vendor/example-model:free", "nvidia/vendor/example-model", "groq/example-model"]
```

**Limits are never hardcoded in the binary.** A limit of `0` or a missing limit means "unknown, rely on reactive cooldowns". The README must tell the operator to copy real numbers from each provider dashboard, because published free-tier numbers go stale.

### Model discovery command

`llm-router models <provider_id> [--filter <text>]` calls the provider's `GET /models` and prints the ids, one per line, ready to paste into an allowlist. Example: `llm-router models or-free --filter :free`. Free model lists change often, so this replaces hunting through web pages.

---

## 6. Routing algorithm

For each request:

1. **Resolve** the model to an ordered candidate list of `(provider, key, upstream_model)`.
2. **Filter** out candidates that are:
   - paused, disabled or in cooldown,
   - over a known quota (projected: current counter plus this request's estimate),
   - excluded by privacy (`x-router-private` and `trains_on_input`),
   - paid and over the daily cap.
3. **Order** by the pool's strategy.
4. **Attempt** the first candidate. If it fails with a retryable error before any byte has been sent to the client, record the failure and try the next, up to `max_attempts`.
5. **If no candidate is eligible:** when the earliest cooldown expires within `max_wait_secs`, wait and retry. Otherwise return 503 with a JSON body listing each route and why it was skipped, plus a `Retry-After` header set to the earliest expiry.

### Failure handling

| Upstream result | Action |
|---|---|
| 429 | Cool down at the provider's limit scope. Use `Retry-After` when present, otherwise exponential backoff from 15 s, doubling, capped at 15 min. Retry on next route. |
| 500, 502, 503, 504, connect error, first-byte timeout | Retry on next route. Three consecutive failures on one provider open a 60 s circuit breaker for it. |
| 401, 403 | Disable that key until restart, log at error level, retry on next route. |
| 404 on a pool route | The model has likely been withdrawn. Mark that route `missing` until restart, show it on the dashboard, retry on next route. |
| 402 | Treat as a disabled key (out of credit), retry on next route. |
| 400, 413, 422, and 404 on an explicit route | Return to the client unchanged. Do not retry. |
| Failure after streaming has started | Failover is impossible. Emit a final SSE `error` event, close the stream, record the failure. |

A successful response resets that route's backoff.

### Client disconnects

If the client goes away, the upstream request is cancelled immediately so quota is not burned on output nobody reads.

---

## 7. Quota accounting

### Limit scopes

Providers count limits differently, so each `[provider.limits]` block declares a scope:

| Scope | Counters shared across | Typical case |
|---|---|---|
| `provider` (default) | every key and model on that provider block | one account with an account-wide cap |
| `key` | every model on one key | several separate accounts |
| `key_model` | nothing, each key and model pair is separate | per-model published limits |

`[provider.model_limits]` entries are always `key_model` scoped and apply in addition to the provider-level limits. A candidate is eligible only if every applicable counter has headroom.

### Counters

Each scope bucket keeps:

- rolling 60 s window: requests, tokens
- current day: requests, tokens (day boundary set by the provider's `reset_utc_hour`)

For the dashboard, the router also keeps per provider: tokens in and out per hour for the last 24 hours.

**Token sources, in order of preference:**

1. The `usage` object in the upstream response. For streaming calls the router injects `stream_options.include_usage = true` upstream, and forwards the usage chunk to the client only if the client asked for it.
2. Rate-limit response headers (`x-ratelimit-remaining-*`) when a provider sends them. These overwrite local counters, since the provider is the source of truth.
3. Estimate of `characters / 4` when neither is available.

**Pre-flight estimate** for the filter step: prompt characters / 4 plus `max_tokens` (or 1024 if unset).

**Persistence:** day counters, hourly buckets, cooldowns longer than 60 s, and paid spend are written to `state_file` every 30 s and on shutdown, using write-to-temp then rename. On startup the file is loaded and anything past its reset time is discarded. A missing or corrupt file is not an error.

**Paid spend:** computed from usage and the configured prices. When `paid_daily_cap_usd` is reached, paid providers are ineligible until the next UTC day.

---

## 8. Request and response handling

- The request body is parsed as a generic JSON value, not a strict struct. Unknown fields pass through untouched so new client features keep working.
- The router changes only: `model` (rewritten to the upstream id), fields listed in the provider's `drop_params`, and the injected `stream_options`.
- Tool definitions, tool calls, `response_format` and multi-turn messages are never inspected or rewritten.
- Streaming responses are forwarded as received. The router scans `data:` lines only to capture the usage chunk and never buffers the full stream.
- Non-streaming responses are forwarded with the body unchanged.
- The `Authorization` header from the client is replaced with the selected upstream key. Client keys never reach a provider.
- Prompt and completion text is never stored, logged or sent to the dashboard.

---

## 9. Logging

**Stdout, one line per request:**

```
2026-11-16T08:45:12Z coder -> nvidia/vendor/example-model 200 ttft=412ms total=3.1s in=1840 out=612 attempts=2 free
```

Warnings for cooldowns, breaker trips, missing models and disabled keys. Keys are never logged, only their last four characters.

---

## 10. Dashboard

A single page served at `/`. Plain HTML, CSS and JavaScript in one file, embedded in the binary at compile time. No framework, no build step, no external scripts, fonts or CDN calls, so it works offline. Target size under 60 KB.

### Data flow

- On load the page fetches `GET /status` for a full snapshot.
- It then listens on `GET /events` (server-sent events) for live updates.
- Events are published on an in-process broadcast channel. With no dashboard open there are no subscribers and publishing costs almost nothing.

### Events

| Event | Sent when | Payload |
|---|---|---|
| `request_start` | request accepted | request id, time, requested model, streaming flag |
| `attempt` | a route is tried | request id, provider, upstream model, key suffix, outcome (ok or the failure reason) |
| `first_byte` | first upstream byte forwarded | request id, time to first token |
| `request_end` | request finished | request id, status, tokens in and out, duration, attempts |
| `route_state` | cooldown, breaker, pause, disable or missing changes | route, new state, expiry |
| `counters` | every 2 s while at least one subscriber is connected | per-provider counters |

### Panels

1. **Summary bar:** uptime, requests today, tokens in and out today, active streams, paid spend against cap.
2. **Providers:** one card per provider showing state (ok, cooling down with countdown, breaker open, paused, key disabled), requests this minute and today as bars against configured limits, tokens in and out today, median and p95 time to first token, and the last error. A card expands to per-model rows. Each card has pause and resume, and a clear-cooldown button.
3. **Live routing:** in-flight requests and the last 50 finished ones, newest first. Each row shows time, requested model, then the attempt chain as chips: failed routes in red with the reason, the serving route in green. In-flight rows are visibly active. Finished rows add time to first token, tokens and duration.
4. **Usage chart:** stacked bars of tokens per provider per hour over the last 24 hours, drawn with plain SVG.

### Rules

- The dashboard never displays API keys beyond their last four characters and never displays prompt or completion text.
- Server-side memory for the dashboard is bounded: a ring buffer of the last 200 requests and 24 hourly buckets per provider.
- A slow or stalled dashboard client must never slow request handling. Lagging subscribers are dropped and reconnect.
- Layout stays simple: system fonts, one dark theme, readable at 1280 px wide. No animation beyond the in-flight indicator and countdowns.

---

## 11. Architecture

**Crates (keep this list short, add nothing without a written reason):**

- `tokio` runtime
- `axum` server (includes SSE support)
- `reqwest` with `rustls-tls` and `stream` features, one shared client with connection pooling
- `serde`, `serde_json`, `toml`
- `dotenvy`
- `futures-util` for stream handling
- dev only: `wiremock` or an in-process axum mock

Use the latest stable version of each at project creation.

**Modules:**

| File | Responsibility |
|---|---|
| `main.rs` | Args and subcommands, config load, server start, graceful shutdown |
| `config.rs` | `router.toml` and `.env` parsing and validation with clear startup errors |
| `catalog.rs` | Model resolution into candidate routes, allowlist checks |
| `limiter.rs` | Scoped counters, windows, cooldowns, circuit breakers, spend |
| `select.rs` | Filter and order candidates (pure functions, heavily unit tested) |
| `proxy.rs` | Upstream call, retry loop, streaming passthrough, usage capture |
| `events.rs` | Broadcast channel, request ring buffer, hourly buckets |
| `state.rs` | Persistence |
| `status.rs` | `/status`, `/events`, `/health`, `/v1/models`, admin endpoints |
| `ui/index.html` | Dashboard, embedded with `include_str!` |
| `discover.rs` | `models` subcommand |
| `error.rs` | Error type and OpenAI-shaped error bodies |

**State:** one `Arc<AppState>`. Limiter data sits behind a `std::sync::Mutex` held only for short, non-async critical sections. No lock is ever held across an `.await`.

**Release profile:**

```toml
[profile.release]
opt-level = 3
lto = "thin"
codegen-units = 1
strip = true
panic = "abort"
```

**Builds:** a GitHub Actions workflow builds and attaches Windows and Linux binaries on every tag. Local development uses `cargo check` and `cargo test`. A full local release build also works on the reference machine, it just takes a few minutes the first time.

---

## 12. Testing and acceptance

All routing tests run against a scripted mock upstream. No test touches a real provider.

| # | Scenario | Pass condition |
|---|---|---|
| T1 | Non-streaming request, one route | Response body identical to upstream, router headers present |
| T2 | Streaming request | Chunks arrive incrementally, first chunk forwarded within 5 ms of receipt |
| T3 | Tool-call request and `response_format` | Upstream receives fields unchanged apart from the `model` rewrite |
| T4 | First route returns 429 with `Retry-After: 30` | Client gets 200 from second route, first route in cooldown for 30 s |
| T5 | 429 without `Retry-After`, repeated | Backoff doubles from 15 s, capped at 15 min, resets on success |
| T6 | First route stalls past first-byte timeout | Failover to second route |
| T7 | Upstream drops mid-stream | Client receives SSE error event, stream closes, no retry |
| T8 | 401 from a key | Key disabled, next key or route used |
| T9 | 400 from upstream | Returned to client, no retry, no cooldown |
| T10 | Known `rpm` limit reached | Route skipped without sending a request upstream |
| T11 | All routes cooling, earliest frees within `max_wait_secs` | Request waits and succeeds |
| T12 | All routes cooling, none free in time | 503 with per-route reasons and `Retry-After` |
| T13 | Pool containing a paid route | Config validation fails at startup with a clear message |
| T14 | Explicit paid request with cap reached | Rejected, cap survives restart |
| T15 | `x-router-private: 1` | Providers with `trains_on_input` never called |
| T16 | Restart mid-day | Day counters, hourly buckets and long cooldowns restored |
| T17 | Client disconnects mid-stream | Upstream request cancelled |
| T18 | Explicit `provider/model` address | Only that provider is tried |
| T19 | Unknown model, or model outside a provider allowlist | 404 with suggestions, no upstream call |
| T20 | `scope = "provider"` limit, two models on one provider | Requests to either model draw down the same counter |
| T21 | 404 from upstream on a pool route | Route marked missing, next route serves the request |
| T22 | Paused provider | Never selected until resumed |
| T23 | Failover scenario with an `/events` subscriber | Events arrive in order: start, failed attempt, ok attempt, first byte, end |
| T24 | Stalled `/events` subscriber | Request latency unaffected, subscriber dropped |
| T25 | Dashboard file | Contains no external URLs, size under 60 KB |
| T26 | `/status` and events payloads | Contain no key material and no message text |
| T27 | Benchmark against local mock | Overhead under 2 ms p95, idle RSS under 20 MB |

**Manual acceptance:**

- OpenCode, configured with the snippet below, completes a multi-file task through the router.
- With one provider key deliberately invalid, the same task still completes, and the dashboard shows the failed attempt and the route that served it.
- A two-hour agent session with the dashboard open shows flat memory in Task Manager.

### OpenCode config

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

---

## 13. Milestones

Each milestone ends with its tests green and a tagged commit.

| Milestone | Scope | Tests |
|---|---|---|
| M0 | Project skeleton, config parsing with validation, `/health`, CI running `cargo test` | config unit tests, T13 |
| M1 | Passthrough to one provider, non-streaming and streaming, header rewriting, allowlists | T1, T2, T3, T18, T19 |
| M2 | Pools, candidate selection, retry loop, cooldowns, circuit breaker, missing models | T4 to T9, T12, T21 |
| M3 | Scoped counters, pre-flight quota filter, usage capture, wait-for-route | T10, T11, T17, T20 |
| M4 | Persistence, paid cap, privacy filter | T14, T15, T16 |
| M5 | `/status`, `/v1/models`, event channel, `/events`, admin endpoints, request log lines | T22, T23, T24, T26 |
| M6 | Dashboard page | T25, manual check of all four panels |
| M7 | `models` subcommand, `run.bat`, release workflow, README with provider setup, benchmark | T27, manual acceptance |

---

## 14. Rules for the coding agent

1. Build milestones in order. Do not start one until the previous one's tests pass.
2. No `unwrap`, `expect` or panics on the request path. Startup may fail fast with a clear message.
3. Never hold a lock across an `.await`.
4. Never log, store or return an API key or any message text.
5. Never hardcode a provider limit, model id or price in Rust source. They are config.
6. Do not add a dependency without stating why the standard library or an existing crate cannot do it.
7. Keep `select.rs` free of I/O so it can be unit tested exhaustively.
8. The dashboard is one hand-written file. No npm, no bundler, no framework.
9. When a requirement here is ambiguous, stop and ask rather than guess.

---

## 15. Open items for Mai

1. Real limits for each provider in the first `router.toml`, copied from the dashboards.
2. `trains_on_input` for each provider, set after reading its current terms.
3. Final name for the binary and repo.

## 16. Candidates for v1.1

- Quota sync from provider endpoints that report remaining allowance.
- Anthropic `/v1/messages` translation.
- Config reload without restart.
- Light theme and a mobile layout for the dashboard.
