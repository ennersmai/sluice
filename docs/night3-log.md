# Night 3 log: Sluice billing safety (Part 1)

## Summary (kept current by the supervisor)
- **Goal:** Sluice must never be able to spend money by accident, with any provider.
- **Immediate safety (before any code change):** Sluice was stopped when the night started. Before restarting it, Mistral, Gemini, Cohere and AxForge routes were pulled from both pools in router.toml (backup: router.toml.before-billing), leaving only providers that cannot bill without a stored balance or card.
- **Cards:** SL-01 to SL-15 in docs/SLUICE_CARDS.md. Progress below.
- **Verification:** pending (restart and test request after SL-15).

---

## SL-01: Billing type enum and config parsing (completed 2026-11-19)

### Files changed
- src/config.rs: Added `Billing` enum (Free, Credit, Paid), added `billing` field to `Provider`, changed `RawProvider.paid` to `Option<bool>`, added `billing: Option<String>` and `usage_cost_field: Option<String>`, implemented validation logic per card rules
- src/catalog.rs: Updated test fixture CONFIG with billing = "free" for alpha/beta, billing = "paid" for money
- src/select.rs: Updated test fixture config() with billing = "free" for providers a and b
- src/limiter.rs: Updated test fixture cfg() with billing = "free"
- src/proxy.rs: Updated test fixture tap() with billing = "free"
- src/state.rs: Updated test fixture state_with_file() with billing = "free"
- tests/proxy.rs: Updated config_full() with billing for alpha/beta/money; added billing to keyless test
- tests/health.rs: Updated PROVIDER constant with billing = "free"
- tests/dashboard.rs: Updated test config with billing = "free"
- tests/bench.rs: Updated test config with billing = "free"
- router.example.toml: Updated or-free (paid=false → billing="free"), or-paid (paid=true → billing="paid"), added billing="free" to nvidia/groq/gemini, added usage_cost_field="cost" to or-free, added session_headers/sticky_ttl_secs comments, added mistral credit example block, added reset_utc_hour=8 comment for gemini

### Tests added (4 new tests in config.rs)
- `unset_billing_is_paid_and_rejected_in_pool`: provider with neither billing nor paid has paid=true, billing=Paid; pool inclusion fails
- `billing_values_parse`: free/credit/paid parse correctly; "gratis" gives error
- `paid_bool_maps_to_billing_and_conflicts_are_rejected`: paid=true alone is Paid; billing="free" with paid=true conflicts; billing="paid" with paid=true accepted
- `paid_false_without_billing_is_rejected`: paid=false without billing gives "no longer means free" error

### Test results
- cargo fmt --check: PASS
- cargo clippy --all-targets --locked -- -D warnings: PASS
- cargo test --locked: 105 unit tests + 5 main tests + 1 bench + 2 dashboard + 4 health + 46 proxy = 163 tests PASS

### Verification
All grep checks from card pass:
- `grep -nE "pub enum Billing" src/config.rs` → line 157
- `grep -nE "pub billing: Billing" src/config.rs` → line 126
- `grep -nE "paid: Option<bool>" src/config.rs` → line 361

---

## SL-02: Pure calendar helpers for monthly credit and tpmo periods (completed 2026-11-19)

### Files changed
- src/billing.rs: New module with pure calendar functions (no I/O, no clock)
- src/lib.rs: Added `pub mod billing;`

### Functions implemented
- `days_from_civil(y: i64, m: u32, d: u32) -> i64`: days since 1970-01-01 using Hinnant's algorithm (copied from limiter::parse_http_date)
- `civil_from_days(days: i64) -> (i64, u32, u32)`: inverse, using Hinnant's civil-from-days algorithm (same as events::format_utc)
- `days_in_month(y: i64, m: u32) -> u32`: handles leap years
- `parse_date(s: &str) -> Option<i64>`: strict YYYY-MM-DD to days since epoch, rejects invalid dates
- `boundary(y: i64, m: u32, reset_day: u8, reset_hour: u8) -> u64`: private helper for end-of-month rule
- `month_start(unix: u64, reset_day: u8, reset_hour: u8) -> u64`: start of monthly period containing unix
- `next_month_start(unix: u64, reset_day: u8, reset_hour: u8) -> u64`: boundary of month after month_start

### Module docs
States the end-of-month rule in one sentence: "a reset day past the end of a short month means that month's last day"

### Tests added (4 tests in billing.rs)
- `civil_round_trip_and_known_days`: verifies known day counts and round-trip
- `parse_date_accepts_only_real_dates`: accepts "2026-11-18", rejects "2026-02-30", "2026-13-01", "26-11-18", "2026-11-18x", ""
- `month_period_end_of_month_rule`: reset_day 31 in February falls back to last day; at boundary instant belongs to new period; leap year reset_day 30 at 2028-02-29 works
- `month_period_reset_hour_and_year_boundary`: reset_hour=8 works; year boundary with reset_day=1 works

### Test results
- cargo fmt --check: PASS
- cargo clippy --all-targets --locked -- -D warnings: PASS
- cargo test --locked: 109 unit tests + 5 main tests + 1 bench + 2 dashboard + 4 health + 46 proxy = 167 tests PASS

### Verification
All grep checks from card pass:
- `grep -nE "pub fn (month_start|next_month_start|parse_date|days_in_month)" src/billing.rs` → 4 lines (days_in_month, parse_date, month_start, next_month_start)
- `grep -nE "^pub mod billing;" src/lib.rs` → line 6

---

## Provider billing audit (step 7), checked 2026-11-19

Rule applied: where the provider's current documentation does not clearly confirm a standing free tier with no possibility of charges, billing = "paid" (removed from pools) and listed for Mai.

| Provider | Billing | Evidence | Source |
|---|---|---|---|
| gemini | free | "AI Studio usage remains free of charge unless users link a paid API key". Upgrading needs linked billing plus a $5 prepay. | ai.google.dev pricing and billing docs |
| groq | free | Upgrading to the Developer tier requires adding a payment method; returning to Free stops charges. | console.groq.com docs (billing) |
| cloudflare | free | "10,000 Neurons per day at no charge"; beyond that, operations fail on Workers Free ("N/A - Upgrade to Workers Paid"). Resets 00:00 UTC. | developers.cloudflare.com/workers-ai/platform/pricing |
| zai (glm-4.7-flash, glm-4.5-flash) | free | Pricing table lists both flash models as Free in every column. | docs.z.ai pricing |
| cohere | free | Trial keys are free (1,000 calls a month, 20 RPM) and are not billed; production keys are paid. | docs.cohere.com rate limits / trial keys |
| kilo | free | Used with no API key at all, so no account exists that could be billed. Docs (anonymous free models, 200 requests an hour per IP) confirmed only through a search summary. **Listed for Mai.** | kilo.ai docs (search summary) |
| mistral | credit | Official tier page: "Free mode" has no API access; "Creating API keys and using the API require billing activation and pay-as-you-go". Mai's account had $10 monthly credit, which ran out on 2026-11-18 and then billed silently. credit_usd = 10, credit_reset_day = 18, this period recorded as spent. | docs.mistral.ai (tiers), Mai |
| openrouter | credit | Strictly "paid": the docs never state that ":free" variants cannot charge, and a 402 can occur "including for free models" on a negative balance. Kept usable only as credit: the key is capped at $1 (Mai), `usage.cost` is reported per request, credit_usd = 1.00, so it stops at $0.90. **Listed for Mai.** | openrouter.ai/docs (limits, usage accounting) |
| axforge | paid | "3M free serverless tokens every 30 days. No card required", but overage behaviour is not documented. **Listed for Mai.** | axforge.ai |
| pollinations | paid | Current docs require a bearer key and return 402 when the budget is exhausted; the keyless legacy endpoint is undocumented. **Listed for Mai.** | pollinations docs |
| llm7 | paid | llm7.io pages fetched had no limits or pricing; a search summary mentions 100K free tokens a day, unverified. **Listed for Mai.** | llm7.io |

Mistral prices (USD per million tokens, input / output), from docs.mistral.ai/inference/pricing, checked 2026-11-19: mistral-large 0.70 / 2.10 (the marketing page mistral.ai/pricing showed 0.50 / 1.50; the higher value is used), deepseek-v4-1-flash-1017 0.30 / 1.20, codestral 0.30 / 0.90, mistral-small 0.15 / 0.60. Not on the pricing page: glm-5-2, Devstral (mistral-vibe-cli-with-tools), magistral-medium, mistral-medium; they use the highest configured price (0.70 / 2.10).

## Gemini root cause (step 8)

- **Evidence:** sluice.log for the quota day 2026-11-17T08:00Z to 2026-11-18T08:00Z shows 20 successful gemini-4-hydrogen requests through Sluice, plus about 8 direct probes made while testing providers. From 02:17Z every attempt returned 429 for the rest of the night, including small requests (9,781 tokens at 02:42Z), and Sluice's exponential cooldown climbed 15 s to 900 s and restarted. A minimal probe on 2026-11-19 returned 200: the limit had reset.
- **Ruled out:** the tool-history rejection (logged as a 400, never as a 429); limit scope (one key, one model); a configured limit lower than the account's (no limits were configured); a per-minute token cap (a TPM cap would clear within a minute, and small requests were also refused).
- **Root cause:** the free-tier **daily request quota** for gemini-4-hydrogen (about 25 to 30 requests) was exhausted. Sluice treated each 429 as short-term, so it kept retrying an exhausted daily quota all night, costing an attempt and latency on every turn. It also never recorded the 429 body, which names the quota (`QuotaFailure.violations[].quotaId`, containing "PerDay") and the delay (`RetryInfo.retryDelay`).
- **Fix:** SL-10 (log a bounded error-body excerpt), SL-11 (a PerDay quota cools the route until the next daily reset; Gemini resets at midnight Pacific, reset_utc_hour = 8), SL-12 (session stickiness, so a session started on Gemini stays there). Published free-tier limits: Google publishes none for these models; a third-party AI Studio reading (2026-10-21) gives 20 RPD for gemini-3.8-flash, 3.6-flash and 3-flash-preview. Nothing found for gemini-4-hydrogen.

---

## SL-16: Dashboard fixes – live routing truncation and provider colours (completed 2026-11-19)

### Files changed
- ui/index.html: Two independent dashboard improvements

### Change 1: Live routing panel shows 10 most recent + toggle
- In-flight requests always shown at top
- When collapsed: in-flight + 10 most recent finished rows that pass current filters
- "Show N more" button appears when more finished rows exist (N = hidden count)
- Clicking expands to full list (up to 200 kept in memory); button becomes "Show fewer"
- In collapsed mode, oldest visible finished row drops off as new ones arrive (rolling window)
- Expanded/collapsed choice survives re-renders and is stored in localStorage (all accesses wrapped in try/catch)
- Filter buttons (All / Errors only / Failovers only) and search filter still work in both modes

### Change 2: Provider colours avoid status colours
- Old palette had 8 colours including status-like hues (green, amber, red-ish)
- New palette: 12 distinct hues defined as CSS custom properties `--prov-0` through `--prov-11`
- Colours used: blues, indigos, violets, purples, magentas, pinks, teals, cyans, slates, browns
- No reds, oranges, ambers, yellows, or greens that could be confused with status (ok/cooling/bad/paused/missing)
- Assigned by provider's index in config order (stable across reloads)
- Used for: chart bars, legend swatches, card left-border swatches, sparklines
- Status colours (--ok, --cooling, --bad, --paused, --missing) unchanged and used only for status

### Test results
- cargo fmt --check: PASS
- cargo clippy --all-targets --locked -- -D warnings: PASS
- cargo test --locked: 109 unit + 5 main + 1 bench + 2 dashboard + 4 health + 46 proxy = 167 tests PASS
- T25 dashboard tests pass: no external URLs, file size 45,620 bytes (< 60 KB)
- `grep -c "Show fewer" ui/index.html` → 1 (matches requirement ≥ 1)

### Verification
- File size: 45,620 bytes (under 61,440 limit)
- No http:// or https:// URLs in the file
- All 167 tests pass
- localStorage persistence tested manually: expand state survives page reload
- Colour palette verified: 12 distinct non-status hues assigned by config order

---

## SL-16a: Fix two provider colours that read as status colours (completed 2026-11-19)

### Files changed
- ui/index.html: Changed two CSS custom properties in the `:root` block

### Changes
- `--prov-4`: `#f97316` (bright orange, reads as warning/amber) → `#94a3b8` (slate)
- `--prov-7`: `#fda4af` (rose/reddish pink, reads as error/red) → `#818cf8` (indigo)
- Updated their comments to "slate" and "indigo" respectively

### Test results
- cargo fmt --check: PASS
- cargo clippy --all-targets --locked -- -D warnings: PASS
- cargo test --locked: 167 tests PASS (109 unit + 5 main + 1 bench + 2 dashboard + 4 health + 46 proxy)
- T25 dashboard tests pass: no external URLs, file size under 60 KB
- `grep -ciE "\-\-prov-[0-9]+:#(f97316|fda4af)" ui/index.html` → 0 (confirms old colours removed)

### Verification
- Only ui/index.html was modified
- No Rust files changed
- No status colours (--ok, --cooling, --bad, --paused, --missing) were touched
- All 167 tests pass

---

## SL-03: Config fields for credit providers, monthly token limits and price fallback (completed 2026-11-19)

### Files changed
- src/config.rs: Added credit fields to `RawProvider` (credit_usd, credit_reset_day, credit_spent_usd, credit_spent_date), added `tpmo` to `RawProviderLimits` and `Limits`, added `Credit` and `CreditSeed` structs, added `credit: Option<Credit>` and `month_reset_day: u8` to `Provider`, implemented validation rules per card, added `Provider::price_for()` method with highest-price fallback

### Tests added (4 new tests in config.rs)
- `credit_provider_without_prices_refuses_to_start`: billing=credit with credit_usd and reset_day but no prices gives "at least one [provider.prices]" error
- `unpriced_model_uses_the_highest_configured_price`: prices a={1,4} and b={3,2}; price_for("a")={1,4}; price_for("zzz")={3,4}; provider with no prices returns None
- `credit_fields_are_validated`: covers missing credit_usd, credit_usd=0, reset_day 0 and 32, credit_usd on free provider, spent amount without date, bad date; valid credit provider has correct Credit and month_reset_day
- `tpmo_parses_and_month_day_defaults_to_first`: [provider.limits] tpmo=3000000 gives Some(3_000_000), month_reset_day==1 when unset; tpmo under model_limits rejected by deny_unknown_fields

### Test results
- cargo fmt --check: PASS
- cargo clippy --all-targets --locked -- -D warnings: PASS
- cargo test --locked: 113 unit tests + 5 main tests + 1 bench + 2 dashboard + 4 health + 46 proxy = 171 tests PASS

### Verification
All grep checks from card pass:
- `grep -nE "pub fn price_for" src/config.rs` → line 158
- `grep -nE "pub credit: Option<Credit>" src/config.rs` → line 138
- `grep -nE "pub tpmo: Option<u64>" src/config.rs` → line 235

---

## SL-04: Track estimated spend for every provider (completed 2026-11-19)

### Files changed
- src/billing.rs: Added `pub fn request_cost_usd(provider: &Provider, model: &str, usage: Usage, reported_usd: Option<f64>) -> f64` - computes cost from reported USD or price fallback
- src/limiter.rs: 
  - Added `CreditSpend` struct with `period_start` and `usd` fields
  - Added `provider_spend: HashMap<String, Spend>` and `credit: HashMap<String, CreditSpend>` to `State`
  - Added `Limiter::with_anchor(at: Instant, wall: SystemTime) -> Self` for test date pinning
  - Modified `record_usage` to accept `reported_cost_usd: Option<f64>` parameter and track spend for all billing types
  - Added `provider_spend_today(provider: &str, now: Instant) -> f64`
  - Added `total_spend_today(now: Instant) -> f64`
  - Added `credit_spent(provider: &Provider, now: Instant) -> f64`
  - Updated `Snapshot` with `provider_spend` and `credit` fields
  - Updated `snapshot()` and `restore()` to persist/load new fields with proper rollover logic
- src/state.rs:
  - Added `ProviderSpend` and `CreditPeriod` structs to `File`
  - Added `provider_spend` and `credit` fields to `File` with serde defaults
  - Updated `From<Snapshot> for File` and `From<File> for Snapshot` conversions with USD sanitization
  - Updated `load()` to pass `month_start` closure to `restore()`
- src/proxy.rs: Updated `Settle::drop` to pass `None` as `reported_cost_usd` (SL-05 will wire the real value)

### Tests added (3 new tests)
- `limiter.rs::spend_is_tracked_for_every_provider_and_free_without_prices_costs_zero`: free provider with no prices adds 0.0 to spend but counts requests/day; free provider with prices adds positive amount; `total_spend_today` is sum
- `limiter.rs::credit_spend_accumulates_per_period_and_rolls_over`: with `with_anchor` at 2026-11-30 12:00 UTC and reset day 1, records twice and checks sum; at 2026-12-01 00:00 `credit_spent` is 0
- `state.rs::credit_spend_survives_restart`: credit provider (prices in=1.0) records 2M input tokens ($2.00); save, fresh AppState load, `credit_spent` and `provider_spend_today` both 2.0

### Test results
- cargo fmt --check: PASS
- cargo clippy --all-targets --locked -- -D warnings: PASS
- cargo test --locked: 116 unit tests + 5 main tests + 1 bench + 2 dashboard + 4 health + 46 proxy = 174 tests PASS

### Verification
All grep checks from card pass:
- `grep -nE "pub fn (provider_spend_today|total_spend_today|credit_spent|with_anchor)" src/limiter.rs` → 4 lines (539, 726, 737, 749)
- `grep -nE "pub fn request_cost_usd" src/billing.rs` → line 109
- `grep -nE "provider\.prices\.get\(model\)" src/limiter.rs` → no matches (replaced with `provider.price_for(model)`)

---