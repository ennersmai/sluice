ID: SL-01

Goal: Every provider gets a billing type, `billing = "free" | "credit" | "paid"`. If it is unset, the provider is paid. Free must be written out. Paid providers stay explicit-route only and are never allowed in a pool. The old `paid` bool keeps working where it is unambiguous.

Files: src/config.rs, src/catalog.rs (test fixtures only), src/select.rs (test fixture only), src/limiter.rs (test fixture only), src/proxy.rs (test fixture only), src/state.rs (test fixture only), tests/proxy.rs, tests/health.rs, tests/dashboard.rs, tests/bench.rs, router.example.toml

Change:
1. In src/config.rs add `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum Billing { Free, Credit, Paid }`. Give it `fn parse(s: &str) -> Option<Self>` (exact strings "free", "credit", "paid") and `pub fn name(self) -> &'static str`.
2. Add `pub billing: Billing` to `Provider`. Keep `pub paid: bool`, now always set to `billing == Billing::Paid`. The existing call sites (`catalog::suggest`, `proxy::forward` -> `check_paid`, `Limiter::record_usage`, `ReqTrack::attempt`, `status::provider_json`) stay unchanged.
3. In `RawProvider`, change `paid: bool` to `#[serde(default)] paid: Option<bool>` and add `#[serde(default)] billing: Option<String>`.
4. In `validate()`, resolve the billing type for each provider like this. Every error goes into `problems`, prefixed with `{label}: `.
   - billing unset, paid unset -> Paid
   - billing unset, `paid = true` -> Paid
   - billing unset, `paid = false` -> error `paid = false no longer means free; set billing = "free" (or "credit")`
   - billing set to a valid value X, paid unset -> X
   - `billing = "paid"` with `paid = true`, or `billing = "free"`/`"credit"` with `paid = false` -> X (redundant but allowed)
   - `billing = "paid"` with `paid = false`, or `billing = "free"`/`"credit"` with `paid = true` -> error `paid = {v} conflicts with billing = "{b}"`
   - invalid billing string -> error `billing = "{s}" must be one of free, credit, paid`
5. The pool validation keeps using `provider.paid`, so the T13 message stays the same. Credit providers are allowed in pools.
6. Migrate the fixtures. Every test provider that is not meant to be paid gets `billing = "free"`. Replace `paid = false` with `billing = "free"`.
   - src/config.rs tests: `ONE_PROVIDER`, `PRD_EXAMPLE` (or-free, nvidia, groq, gemini; or-paid keeps `paid = true`), and the "free" provider in `t13_pool_with_paid_route_is_rejected`.
   - src/catalog.rs test configs: every non-paid provider.
   - src/select.rs `tests::config`.
   - src/limiter.rs `tests::cfg`: add the line to the format string before `{extra}`.
   - src/proxy.rs `tests::tap`.
   - src/state.rs `tests::state_with_file`.
   - tests/proxy.rs `config_full`: alpha gets `billing = "free"`. beta gets it only when `extra` does not already set billing: `let beta_billing = if extra.contains("billing") { "" } else { "billing = \"free\"" };`. money keeps `paid = true`. The keyless test text in `keyless_provider_sends_no_authorization_upstream` also gets `billing = "free"`.
   - tests/health.rs `PROVIDER`, tests/dashboard.rs and tests/bench.rs provider strings.
   - router.example.toml: or-free `paid = false` becomes `billing = "free"`, or-paid `paid = true` becomes `billing = "paid"`, and nvidia, groq and gemini get `billing = "free"`. This keeps `shipped_example_file_is_valid` green. Add a one-line comment above or-free: `# billing: free | credit | paid; unset means paid`.

Tests (src/config.rs):
- `unset_billing_is_paid_and_rejected_in_pool`: a provider with neither billing nor paid has `paid == true` and `billing == Billing::Paid`. Putting it in a pool fails with "uses paid provider".
- `billing_values_parse`: free, credit and paid each parse. `billing = "gratis"` gives the "must be one of" error.
- `paid_bool_maps_to_billing_and_conflicts_are_rejected`: `paid = true` alone is Paid. `billing = "free"` with `paid = true` errors with "conflicts with billing". `billing = "paid"` with `paid = true` is accepted.
- `paid_false_without_billing_is_rejected`: the error contains `no longer means free`.

Done when:
- `cargo fmt --check`, `cargo clippy --all-targets --locked -- -D warnings` and `cargo test --locked` all pass. The 4 new tests are among them.
- `grep -nE "pub enum Billing" src/config.rs` matches. `grep -nE "pub billing: Billing" src/config.rs` matches.
- `grep -nE "paid: Option<bool>" src/config.rs` matches.

Do not: edit router.toml, .env or any state file. Change no runtime behaviour beyond config parsing. Do not rename or remove `Provider::paid`. PRD rules apply: no unwrap/expect/panic on the request path, no lock across .await, never log keys or message text, no hardcoded provider limits/models/prices, no new crates.

---

ID: SL-02

Goal: Add pure calendar helpers for monthly credit and `tpmo` periods in a new module, src/billing.rs. Nothing calls them yet.

Files: src/billing.rs (new), src/lib.rs

Change:
1. Add `pub mod billing;` to src/lib.rs.
2. In src/billing.rs, write pure functions with no I/O and no clock. Convert integers with `try_from` or `max(0)`; do not use `unwrap`.
   - `pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64`: days since 1970-01-01, using Hinnant's algorithm. The same algorithm is already inlined in `limiter::parse_http_date`; copy it, do not refactor that function.
   - `pub fn civil_from_days(days: i64) -> (i64, u32, u32)`: the inverse, the same algorithm as `events::format_utc`.
   - `pub fn days_in_month(y: i64, m: u32) -> u32`: handles leap years.
   - `pub fn parse_date(s: &str) -> Option<i64>`: strict `YYYY-MM-DD` to days since the epoch. Rejects month 0 or 13, impossible days (2026-02-30), years outside 1970..=9999, and any extra characters.
   - `fn boundary(y: i64, m: u32, reset_day: u8, reset_hour: u8) -> u64`: Unix seconds at `reset_hour`:00 UTC on day `min(reset_day, days_in_month(y, m))` of month (y, m). This is the end-of-month rule: a reset day past the end of a short month means that month's last day.
   - `pub fn month_start(unix: u64, reset_day: u8, reset_hour: u8) -> u64`: the start of the monthly period containing `unix`. Take the civil (y, m) of `unix`. If `unix >= boundary(y, m)`, return that boundary. Otherwise return the previous month's boundary.
   - `pub fn next_month_start(unix: u64, reset_day: u8, reset_hour: u8) -> u64`: the boundary of the month after the month of `month_start(..)`.
3. Add module docs that state the end-of-month rule in one sentence.

Tests (src/billing.rs `#[cfg(test)]`, with helper `at(y, m, d, h) -> u64`):
- `civil_round_trip_and_known_days`: `days_from_civil(1970,1,1) == 0`, `(2000,3,1) == 11017`, `(2026,11,18) == 20775`, `(2028,2,29) == 21243`. `civil_from_days` inverts each.
- `parse_date_accepts_only_real_dates`: "2026-11-18" gives 20775. "2026-02-30", "2026-13-01", "26-11-18", "2026-11-18x" and "" give None.
- `month_period_end_of_month_rule`: with reset_day 31, at 2026-02-15 the start is 2026-01-31 and the next is 2026-02-28. At 2026-02-28 00:00 the start is that instant and the next is 2026-03-31. With reset_day 30 at 2028-02-29 the start is 2028-02-29.
- `month_period_reset_hour_and_year_boundary`: with day 15 and hour 8, at 2026-11-15 07:59:59 the start is 2026-10-15 08:00. At 08:00 the start is 2026-11-15 08:00. With day 1, at 2026-12-31 23:59:59 the start is 2026-12-01 and the next is 2027-01-01.

Done when:
- fmt, `cargo clippy --all-targets --locked -- -D warnings` and `cargo test --locked` pass.
- `grep -nE "pub fn (month_start|next_month_start|parse_date|days_in_month)" src/billing.rs` finds 4 lines.
- `grep -nE "^pub mod billing;" src/lib.rs` matches.

Do not: add a date crate such as chrono or time. Do not touch limiter.rs or events.rs. Do not call these functions from anywhere else yet. Same PRD rules as SL-01.

---

ID: SL-03
> **Supervisor note:** SL-01 already added the parsed field `usage_cost_field: Option<String>` to `Provider` and `RawProvider` (parsing only, nothing reads it yet). Keep it as it is; do not add it a second time.

Goal: Add config fields for credit providers, monthly token limits and provider-reported cost, plus the price fallback. A credit provider without prices refuses to start. Never assume a price of zero.

Files: src/config.rs

Change:
1. Add to `RawProvider`, all `#[serde(default)]`:
   - `credit_usd: Option<f64>`
   - `credit_reset_day: Option<i64>`
   - `credit_spent_usd: Option<f64>`
   - `credit_spent_date: Option<String>`
   - `usage_cost_field: Option<String>`
2. Add `#[serde(default)] tpmo: u64` to `RawProviderLimits` only, not to `RawLimits`.
3. Add `pub tpmo: Option<u64>` to `Limits`. `limits_from` sets `tpmo: None`. For `[provider.limits]` use `Limits { tpmo: known(l.tpmo), ..limits_from(..) }`. Update the literal `Limits { .. }` constructions in config tests by adding `tpmo: None`.
4. Add `pub struct Credit { pub usd: f64, pub seed: Option<CreditSeed> }` and `pub struct CreditSeed { pub usd: f64, pub day: i64 }`, both `#[derive(Debug, Clone, Copy, PartialEq)]`. `day` is days since the epoch from `billing::parse_date`.
5. Add to `Provider`:
   - `pub credit: Option<Credit>`, which is `Some` exactly when billing is Credit
   - `pub month_reset_day: u8`, which is `credit_reset_day` or 1
   - `pub usage_cost_field: Option<String>`
6. Validation in `validate()`. Push each problem with the `{label}: ` prefix:
   - `credit_reset_day` may be set on any billing type, because it also sets the `tpmo` month. It must be 1..=31: `credit_reset_day = {v} must be between 1 and 31`.
   - Billing Credit requires `credit_usd` finite and > 0 (`billing = "credit" needs credit_usd > 0`). It also requires `credit_reset_day`: `billing = "credit" needs credit_reset_day (1-31)`.
   - Billing Credit with an empty `prices` table: `billing = "credit" needs at least one [provider.prices] entry; sluice never assumes a price of zero`.
   - `credit_usd`, `credit_spent_usd` or `credit_spent_date` on a non-credit provider: `{field} is only valid with billing = "credit"`.
   - `credit_spent_usd` and `credit_spent_date` must be set together: `credit_spent_usd and credit_spent_date must be set together`. The amount must be finite and >= 0. The date must pass `billing::parse_date`: `credit_spent_date = "{s}" must be a date YYYY-MM-DD`.
   - `usage_cost_field`, when set, must be non-empty after trim.
7. Add `impl Provider { pub fn price_for(&self, model: &str) -> Option<Price> }`. Return the exact entry if there is one. Otherwise, if `prices` is non-empty, return the highest configured price: `Price { in_per_mtok: max over entries, out_per_mtok: max over entries }`, taken separately for each component. Return `None` only when `prices` is empty. Document it as "an unpriced model is charged at the highest configured price".
8. `check_paid` in proxy.rs is unchanged. Paid providers still require an exact price at request time.

Tests (src/config.rs):
- `credit_provider_without_prices_refuses_to_start`: billing credit with credit_usd and reset day but no prices gives the "at least one [provider.prices]" error.
- `unpriced_model_uses_the_highest_configured_price`: prices a={1,4} and b={3,2}. `price_for("a")` is {1,4}. `price_for("zzz")` is {3,4}. A provider with no prices returns None.
- `credit_fields_are_validated`: covers a missing credit_usd, credit_usd = 0, reset day 0 and 32, credit_usd on a free provider, a spent amount without a date, and a bad date. A valid credit provider has `credit == Some(Credit { usd: 10.0, seed: Some(CreditSeed { usd: 10.0, day: 20775 }) })` and `month_reset_day == 5`.
- `tpmo_parses_and_month_day_defaults_to_first`: `[provider.limits] tpmo = 3000000` gives `Some(3_000_000)`, and `month_reset_day == 1` when unset. `tpmo` under `model_limits` is rejected as an unknown field.

Done when:
- fmt, clippy (`-D warnings`) and `cargo test --locked` pass.
- `grep -nE "pub fn price_for" src/config.rs` matches.
- `grep -nE "pub credit: Option<Credit>" src/config.rs` matches.
- `grep -nE "pub tpmo: Option<u64>" src/config.rs` matches.

Do not: use these fields anywhere at runtime yet. Do not change `check_paid`. Same PRD rules as SL-01. Config-parse errors may fail fast, since that is startup.

---

ID: SL-04

Goal: Track estimated spend for every provider: a per-provider UTC-day total, and a running total per credit period for credit providers. Persist both. A restart never resets them.

Files: src/billing.rs, src/limiter.rs, src/state.rs, src/proxy.rs (one call site)

Change:
1. In billing.rs add `pub fn request_cost_usd(provider: &Provider, model: &str, usage: Usage, reported_usd: Option<f64>) -> f64`. If `reported_usd` is finite and >= 0, use it. Otherwise use `provider.price_for(model)` to compute `(input * in_per_mtok + output * out_per_mtok) / 1e6`. With no prices at all (free providers only, since credit providers always have prices after SL-03) the cost is `0.0`.
2. In limiter.rs:
   - Add `#[derive(Debug, Default, Clone, Copy, PartialEq)] pub struct CreditSpend { pub period_start: u64, pub usd: f64 }`.
   - Add these fields to `State` and to its `Default`:
     - `provider_spend: HashMap<String, Spend>`: the UTC day (`unix / 86_400`) and USD, for every provider.
     - `credit: HashMap<String, CreditSpend>`.
   - Add `pub fn with_anchor(at: Instant, wall: SystemTime) -> Self` to `Limiter`. It builds a limiter whose wall clock is `wall` at `at`, so tests can pin dates. Doc it as test-oriented.
3. `Limiter::record_usage` gains a trailing parameter `reported_cost_usd: Option<f64>`. Inside, compute `cost = billing::request_cost_usd(provider, model, usage, reported_cost_usd)` once. Then:
   - Roll and add to `provider_spend[provider.id]`, for every billing type.
   - If `provider.paid`, add `cost` to the existing global `st.spend`, replacing the inline price math. Day rollover is unchanged.
   - If `provider.credit.is_some()`, compute `start = billing::month_start(unix, provider.month_reset_day, provider.reset_utc_hour)`. Reset the entry to `{start, 0.0}` when its `period_start != start`, then add `cost`.
4. Add these methods:
   - `pub fn provider_spend_today(&self, provider: &str, now: Instant) -> f64`
   - `pub fn total_spend_today(&self, now: Instant) -> f64`: the sum over all providers for the current UTC day
   - `pub fn credit_spent(&self, provider: &Provider, now: Instant) -> f64`: 0.0 when the stored period is not the current one
5. Add to `Snapshot`:
   - `pub provider_spend: Vec<(String, Spend)>`
   - `pub credit: Vec<(String, CreditSpend)>`

   Fill them in `snapshot()`. The existing literal `Snapshot { .. }` in `restore_discards_stale_and_unknown_entries` gets the new fields as empty vectors.
6. `restore` gains a parameter `month_start: &dyn Fn(&str, u64) -> Option<u64>`: (provider id, unix) to the current monthly period start, `None` for an unknown provider. Restore rules:
   - Keep `provider_spend` entries whose provider is known and whose day is today.
   - Keep `credit` entries only when `month_start(id, unix) == Some(entry.period_start)`. Merge as `usd = max(existing, snapshot)`, which SL-07 relies on.
   - Update both existing callers: `state::load` passes `&|id, unix| config.provider(id).map(|p| billing::month_start(unix, p.month_reset_day, p.reset_utc_hour))`, and the limiter tests pass an equivalent closure.
7. In state.rs add `#[serde(default)] provider_spend: Vec<ProviderSpend>` (`provider`, `day`, `usd`) and `#[serde(default)] credit: Vec<CreditPeriod>` (`provider`, `period_start_unix`, `usd`) to `File`, plus both `From` conversions. On load, sanitise `usd` the same way `PaidSpend` is: non-finite becomes 0, negatives clamp to 0. Keep `VERSION = 1`, because older files load through the serde defaults.
8. proxy.rs `Settle::drop`: pass `None` as the new argument. SL-05 wires the real value.

Tests:
- limiter.rs `spend_is_tracked_for_every_provider_and_free_without_prices_costs_zero`: a free provider with no prices and usage 1000/1000 adds 0.0 to the provider's spend but still counts its request/day counters as before. A free provider with prices adds a positive amount. `total_spend_today` is the sum.
- limiter.rs `credit_spend_accumulates_per_period_and_rolls_over`: with `with_anchor` at 2026-11-30 12:00 UTC and reset day 1, record twice and check the sum. At 2026-12-01 00:00 `credit_spent` is 0.
- state.rs `credit_spend_survives_restart`: an `AppState` with a credit provider (prices in=1.0) records usage of 2,000,000 input tokens, so `credit_spent` is 2.0. `save`, then a fresh `AppState` runs `load`, and `credit_spent` is 2.0 and `provider_spend_today` is 2.0.

Done when:
- fmt, clippy (`-D warnings`) and `cargo test --locked` pass.
- `grep -nE "pub fn (provider_spend_today|total_spend_today|credit_spent|with_anchor)" src/limiter.rs` finds 4 lines.
- `grep -nE "pub fn request_cost_usd" src/billing.rs` matches.
- `grep -nE "provider\.prices\.get\(model\)" src/limiter.rs` finds nothing.

Do not: make credit affect selection yet (SL-06). Do not bump the state file VERSION. Do not do any I/O inside the limiter. Same PRD rules as SL-01.

---

ID: SL-05

Goal: When a provider reports the cost of a request in its `usage` object, prefer that number over the price estimate. The field name is configured per provider with `usage_cost_field`. OpenRouter's current docs name it `cost`, in credits, where 1 credit is 1 USD, and OpenRouter returns usage by default.

Files: src/proxy.rs, tests/proxy.rs

Change:
1. Add `#[serde(flatten)] extra: serde_json::Map<String, Value>` to `UsageFields`. Add `fn cost(&self, field: Option<&str>) -> Option<f64>`: `extra.get(field?)?.as_f64()`, filtered to finite and >= 0. The field is top-level inside `usage`; dotted paths are not supported.
2. Change `usage_from_body(body, cost_field: Option<&str>) -> Option<(Usage, Option<f64>)>`, computing the cost before `into_usage` consumes the fields.
3. Add `reported_cost: Option<f64>` and `cost_field: Option<String>` to `Settle`. `Settle::new` sets `cost_field` from `entry.provider.usage_cost_field.clone()`.
4. `Tap::handle_line`: after parsing `chunk.usage`, set `self.settle.reported_cost = fields.cost(self.settle.cost_field.as_deref())` before calling `into_usage`.
5. In the non-streaming success branch of `forward`, set both `settle.usage` and `settle.reported_cost` from the new `usage_from_body`.
6. `Settle::drop` passes `self.reported_cost` to `record_usage`.
7. Update the unit test `usage_helpers` for the new return type, and add one cost assertion to it.

Tests:
- proxy.rs unit `usage_cost_field_is_read_from_the_usage_object`: `{"usage":{"prompt_tokens":3,"completion_tokens":4,"cost":0.0123}}` with `Some("cost")` gives cost `Some(0.0123)`. With `None` it gives None. With `"cost":"x"` it gives None.
- tests/proxy.rs `reported_cost_is_preferred_over_the_price_estimate`: beta extra is `usage_cost_field = "cost"\n[provider.prices."fallback-model"]\nin_per_mtok = 1000.0\nout_per_mtok = 1000.0`. The mock replies 200 with usage 10/5 and `"cost":0.5`. Use `start_router_state`. After the request, poll until `state.limiter.provider_spend_today("beta", Instant::now())` is within 1e-9 of 0.5, with a short retry loop because settling happens on drop. A second mock reply without `cost` adds the estimate of (10+5)*1000/1e6 = 0.015.

Done when:
- fmt, clippy (`-D warnings`) and `cargo test --locked` pass.
- `grep -nE "reported_cost: Option<f64>" src/proxy.rs` matches.
- `grep -nE "fn cost\(&self, field: Option<&str>\)" src/proxy.rs` matches.

Do not: hardcode "cost" or any provider id in Rust source; the field comes only from config. Do not buffer streams. Do not log usage bodies. Same PRD rules as SL-01.

---

ID: SL-06

Goal: Once a credit provider's spend this period reaches 90% of `credit_usd`, it becomes ineligible until its next reset. It is skipped before any upstream call with skip reason `credit_exhausted`.

Files: src/select.rs, src/limiter.rs, src/proxy.rs, tests/proxy.rs

Change:
1. In select.rs add the variant `Skip::CreditExhausted { until: Instant }`. `name()` returns `"credit_exhausted"`, and `until()` returns `Some(until)`.
2. In limiter.rs add `pub const CREDIT_GUARD_FRACTION: f64 = 0.9;`. This is the owner's policy, not a provider value.
3. Add `#[derive(Debug, Clone, Copy, PartialEq)] pub struct CreditStatus { pub credit_usd: f64, pub spent_usd: f64, pub period_start: u64, pub resets_at: u64, pub exhausted: bool }`, and a private `State::credit_status(&self, provider, now) -> Option<CreditStatus>`. It computes the period with `billing::month_start` and `billing::next_month_start`, using `provider.month_reset_day` and `provider.reset_utc_hour`. Set `exhausted = spent_usd >= credit_usd * CREDIT_GUARD_FRACTION`. Expose `pub fn credit_status(&self, provider: &Provider, now: Instant) -> Option<CreditStatus>` on `Limiter`.
4. In `impl RouteView for State`, `skip_reason`: right after the `paused` check, if `credit_status` says exhausted, return `Skip::CreditExhausted { until: now + Duration::from_secs(resets_at - unix) }`. Use `saturating_sub`, with a minimum of 1 s.
5. `Limiter::record_usage` now returns `bool`: true when this call moved a credit provider from not exhausted to exhausted.
6. In `Settle::drop`, when it returns true, log with `crate::elog!("warn: credit guard reached provider={} spent=${:.4} of ${:.2} ({}%), ineligible until {}", ..)`, formatting the reset time with `events::format_utc`. Also call `events.route_state(provider, None, None, "credit_exhausted", None)`. No key and no message text go in the line. Ignore the return value elsewhere (tests).

Tests:
- limiter.rs `credit_spend_reaching_90_percent_makes_provider_ineligible`: a credit provider with credit_usd = 10 and in=1.0 per Mtok. 8,990,000 input tokens leave it eligible. Another 10,000 (total $9.00) returns Some(Skip::CreditExhausted{..}) with name "credit_exhausted". `record_usage` returned true exactly on the crossing call.
- limiter.rs `credit_resets_on_the_reset_day`: use `with_anchor` at 2026-11-30 23:00 UTC with reset day 1, and exhaust the credit. The skip's `until` is now + 3600 s. At now + 3600 s the provider is eligible. A second case uses reset day 31 at 2026-11-29 and checks that the reset is 2026-11-30 00:00.
- tests/proxy.rs `credit_exhausted_provider_is_skipped_without_upstream_call`: beta extra is `billing = "credit"\ncredit_usd = 0.01\ncredit_reset_day = 1\n[provider.prices."fallback-model"]\nin_per_mtok = 1000.0\nout_per_mtok = 0.0`, and the mock is `usage_reply()` (10 input tokens, so $0.01). The first `beta/fallback-model` gives 200. The second gives 503 with `routes[0].reason == "credit_exhausted"` and a `retry-after` header. `beta.calls().len() == 1`.
- tests/proxy.rs `credit_exhausted_survives_restart`: the same config plus `state_file` through `config_full`'s `top`. Exhaust, `sluice::state::save`, restart with `start_router_state(cfg(), true)`. The 503 has reason `credit_exhausted` and the call count is unchanged.

Done when:
- fmt, clippy (`-D warnings`) and `cargo test --locked` pass.
- `grep -nE "CreditExhausted \{" src/select.rs src/limiter.rs` matches in both files.
- `grep -nE "pub const CREDIT_GUARD_FRACTION" src/limiter.rs` matches.
- `grep -nE "\"credit_exhausted\"" src/select.rs` matches.

Do not: apply the guard to free or paid providers. Do not change `paid_daily_cap_usd` or `check_paid`. Do not log on every skipped request, only on the crossing. Same PRD rules as SL-01.

---

ID: SL-07

Goal: Let the owner record spend that happened outside sluice in the current period, for example "Mistral's $10 credit is already spent". The values come from config: `credit_spent_usd` plus `credit_spent_date = "YYYY-MM-DD"`.

Why config and not an admin endpoint: the PRD keeps config in files. A dated value applies only to the credit period containing its date, so it expires by itself at the next reset. Applying it as a floor, `max(persisted, seed)`, makes it idempotent: restarts never double-count it and never lower real spend. An admin endpoint would need its own persistence and UI, and is easier to get wrong.

Files: src/limiter.rs, src/lib.rs, tests/proxy.rs

Change:
1. Add `pub fn seed_credit(&self, provider: &Provider, now: Instant) -> bool` to limiter.rs. If `provider.credit` has a `seed`, compute `seed_unix = seed.day * 86_400 + 86_399`, the end of that UTC day, saturating to u64. If `billing::month_start(seed_unix, ..) == billing::month_start(unix_now, ..)`, set `credit[id] = CreditSpend { period_start, usd: max(current usd for this period, seed.usd) }` and return true. Otherwise return false.
2. In `AppState::new` (src/lib.rs), after building the limiter, loop over providers with a seed. If `seed_credit` returns false, log `crate::elog!("warn: credit_spent_date for provider={} is from an earlier credit period, ignored", id)`. `restore` already merges with max (SL-04), so it does not matter whether the state file loads before or after.

Tests:
- limiter.rs `credit_seed_applies_only_within_its_period_and_never_double_counts`: use `with_anchor` at 2026-11-18 and reset day 1.
  - A seed of $10 dated 2026-11-05 applies, and the provider is credit_exhausted for credit_usd 10.
  - A seed dated 2026-10-31 is ignored.
  - Seed $3, then record $2 of usage: spent is $5. A snapshot restored into a fresh limiter that was seeded first still gives $5, not $8.
- tests/proxy.rs `credit_seed_from_config_makes_the_provider_ineligible`: beta extra is credit with `credit_usd = 10.0`, `credit_reset_day = 1`, `credit_spent_usd = 10.0`, `credit_spent_date` = today's UTC date, and a price. Build today's date with `sluice::events::format_utc(now)[..10]`. The first `beta/fallback-model` request gives 503 with reason `credit_exhausted`, and `beta.calls()` is empty.

Done when:
- fmt, clippy (`-D warnings`) and `cargo test --locked` pass.
- `grep -nE "pub fn seed_credit" src/limiter.rs` matches.
- `grep -nE "seed_credit\(" src/lib.rs` matches.

Do not: add an admin endpoint for seeding. Do not write the seed into the state file other than through normal snapshots. Do not apply a seed whose date falls in another period. Same PRD rules as SL-01.

---

ID: SL-08

Goal: Add a `tpmo` (tokens per month) limit in `[provider.limits]`. The month boundary is `credit_reset_day` (via `Provider::month_reset_day`), else the 1st, at `reset_utc_hour`. The counter persists across restarts.

Files: src/select.rs, src/limiter.rs, src/state.rs

Change:
1. select.rs: add `QuotaKind::Tpmo`, named `"quota_tpmo"`.
2. limiter.rs `Bucket`: add `month: u64` (the period start, Unix seconds) and `month_tok: u64`, plus `fn roll_month(&mut self, start: u64)`, which zeroes `month_tok` when the start changes.
3. Replace the `day, day_free` parameters of `Bucket::check` with a struct reference `&Periods`, where `struct Periods { day: u64, day_free: Instant, month: u64, month_free: Instant }`. `State::quota_skip` builds it using `billing::month_start` and `billing::next_month_start`. After the tpd check add: if `limits.tpmo` is set, `month_tok` (counting it only when `self.month == periods.month`) is > 0 and `month_tok + estimate > l`, return `Quota { limit: Tpmo, until: Some(month_free) }`. This follows the existing rule that an empty counter never blocks.
4. `record_usage`: for each bucket in `buckets_for`, call `roll_month(start)` and add `total` to `month_tok`.
5. Add `tokens_month: u64` to `ProviderView`. Sum it over the primary buckets in `provider_view` the same way as `tokens_day`.
6. Add `pub struct MonthSnap { pub scope: ScopeKey, pub period_start: u64, pub tokens: u64 }` and `Snapshot.months: Vec<MonthSnap>`, filled for buckets with `month_tok > 0`. In `restore`, keep an entry only when `month_start(provider, unix) == Some(period_start)`, using the closure from SL-04.
7. state.rs: add `#[serde(default)] months: Vec<Month>` (flattened `Scope`, `period_start_unix`, `tokens`) with both conversions.

Tests (limiter.rs):
- `tpmo_blocks_until_the_month_boundary`: use `with_anchor` at 2026-11-20, tpmo = 1000, and record 900 tokens. An estimate of 50 passes. An estimate of 101 gives `quota_tpmo` with `until` at 2026-12-01 00:00. With `credit_reset_day = 25`, `until` is 2026-11-25.
- `tpmo_month_counter_survives_restart`: snapshot, restore into a new limiter, and the same skip happens. A snapshot from the previous period is dropped.

Done when:
- fmt, clippy (`-D warnings`) and `cargo test --locked` pass.
- `grep -nE "Tpmo =>" src/select.rs` matches.
- `grep -nE "struct Periods" src/limiter.rs` matches.
- `grep -nE "pub months: Vec<MonthSnap>" src/limiter.rs` matches.

Do not: add `tpmo` to `model_limits`. Do not apply `x-ratelimit-*` hints to the month counter. Same PRD rules as SL-01.

---

ID: SL-09

Goal: A 402 from upstream makes the whole provider ineligible: every key and every model, pools and explicit routes. It stays that way until the owner clears it with a new admin endpoint. It is logged at error level and persists across restart. A 401 keeps today's per-key behaviour.

Files: src/proxy.rs, src/select.rs, src/limiter.rs, src/state.rs, src/status.rs, tests/proxy.rs

Change:
1. proxy.rs: add `Class::PaymentRequired` with the doc "402: the whole provider is out of credit until cleared". `classify` maps `402 => Class::PaymentRequired` and `401 => Class::KeyRejected`. Fix the `KeyRejected` doc comment to say 401 only.
2. Add `Class::PaymentRequired` to the `matches!` list for `note_error` in `apply_failure`. Its arm calls `state.limiter.block_payment(provider)`. When that returns true, log `crate::elog!("error: provider disabled until cleared on the dashboard provider={provider} key={key} model={model} (upstream 402 payment required)")` and call `events.route_state(provider, None, Some(key), "payment_required", None)`.
3. select.rs: add `Skip::PaymentRequired`, named `"payment_required"`, with `until()` None.
4. limiter.rs:
   - Add `payment_required: HashSet<String>` to `State`.
   - Add `pub fn block_payment(&self, provider: &str) -> bool` and `pub fn clear_payment(&self, provider: &str) -> bool`.
   - In `skip_reason`, check it right after `paused` and before the SL-06 credit check.
   - Add `ProviderView.payment_required: bool`.
   - Add `Snapshot.payment_required: Vec<String>`. `restore` keeps entries for known providers, using the `reset_hour` closure returning Some.
   - `clear_cooldowns` does NOT clear it.
5. state.rs: add `#[serde(default)] payment_required: Vec<String>` to `File` and both conversions.
6. status.rs:
   - Add the route `.route("/admin/provider/{id}/payment/clear", post(clear_payment))`. The handler validates with `known_provider`, calls `clear_payment`, logs `warn: provider {id} payment block cleared by admin`, publishes `route_state(.., "cleared", None)`, and returns `{"provider": id, "cleared": bool}`.
   - `provider_state` returns `("payment_required", None)` right after the paused check.
   - `provider_json` adds `"payment_required": view.payment_required`.

Tests (tests/proxy.rs):
- `payment_required_402_disables_the_whole_provider_until_cleared`: alpha has two keys (`config_full` with `"{ALPHA_KEY},{BETA_KEY}"`), and its mock is `Sequence[402, 200]`.
  - `alpha/vendor/model-a` returns 402 with `x-router-attempts: 1`, because the second key is not tried.
  - The next request is 503 with reason `payment_required`, and `alpha.calls().len() == 1`.
  - `/status` shows alpha `state == "payment_required"`.
  - `POST /admin/cooldowns/clear` does not unblock it.
  - `POST /admin/provider/alpha/payment/clear` returns 200 and `cleared == true`, and the next request returns 200.
  - An unknown id gives 404.
- `payment_required_block_survives_restart`: block, `sluice::state::save`, restart with load, and the result is still 503 `payment_required`.
- `t8_401_disables_the_key_and_the_next_key_is_used` stays unchanged and green.

Done when:
- fmt, clippy (`-D warnings`) and `cargo test --locked` pass.
- `grep -nE "402 => Class::PaymentRequired" src/proxy.rs` matches.
- `grep -nE "401 \| 402" src/proxy.rs` finds nothing.
- `grep -nE "/admin/provider/\{id\}/payment/clear" src/status.rs` matches.

Do not: auto-expire the block. Do not clear it on restart or through `clear_cooldowns`. Do not do file I/O in `apply_failure`; the periodic and shutdown saves persist it. Same PRD rules as SL-01.

---

ID: SL-10

Goal: The 429 and 402 warn/error lines include a bounded, sanitised excerpt of the upstream error body, so a quota message like last night's Gemini one is visible. Use error bodies only, never request text.

Files: src/proxy.rs

Change:
1. Add `const MAX_EXCERPT_CHARS: usize = 300;` and `fn error_excerpt(body: &[u8], keys: &[ApiKey]) -> String`. It works on `String::from_utf8_lossy` of at most the first 4 KiB, then:
   - replaces every non-anonymous key's `expose()` text with `[redacted]`
   - maps every control character (newlines and tabs included) to a space
   - replaces `"` with `'`
   - collapses runs of whitespace, then trims
   - keeps at most `MAX_EXCERPT_CHARS` chars, appending `…` when cut
2. `apply_failure` gains a final parameter `body: Option<&[u8]>`. Only the failure branch that read `body` with `read_capped` passes `Some(&body)`; every other call site passes `None`. The `Class::RateLimited` line becomes `warn: cooldown provider=.. key=.. model=.. for {}s (429) body="{excerpt}"` and the SL-09 402 line gets the same ` body="{excerpt}"` suffix. Both are added only when `body` is Some and the excerpt is non-empty. Compute the excerpt inside `apply_failure` from `entry.provider.keys`.
3. `events.note_error` and `track.attempt` reasons stay unchanged, so the dashboard shows no body text.

Tests (proxy.rs unit):
- `error_excerpt_is_bounded_single_line_and_redacts_keys`:
  - A 2000-char body gives at most 301 chars ending in `…`.
  - `"a\nb\r\n\tc"` gives `a b c`.
  - A body containing a configured key `sk-test-secret-12345678` never contains it and does contain `[redacted]`.
  - Quotes become `'`.
  - An empty body gives `""`.

Done when:
- fmt, clippy (`-D warnings`) and `cargo test --locked` pass.
- `grep -nE "fn error_excerpt\(body: &\[u8\], keys: &\[ApiKey\]\)" src/proxy.rs` matches.
- `grep -nF 'body=\"{' src/proxy.rs` finds at least 2 lines: the 429 and 402 format strings.

Do not: log bodies for any status other than 429 and 402. Do not log request bodies, headers or message text. Do not put the excerpt in `/status`, `/events` or the 503 body. Same PRD rules as SL-01.

---

ID: SL-11

Goal: Fix the Gemini root cause. Google 429 bodies that report a per-day quota violation cool the route, at its limit scope, until the provider's next daily reset (`reset_utc_hour`). Google `RetryInfo.retryDelay` is used as the precise hint otherwise. This stops sluice retrying an exhausted daily quota every 15 s to 15 min all night.

Files: src/limiter.rs, src/proxy.rs, tests/proxy.rs

Change:
1. limiter.rs: add `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub struct QuotaHint { pub per_day: bool, pub retry_delay: Option<Duration> }` and `pub fn google_quota_hint(body: &[u8]) -> Option<QuotaHint>`.
   - Parse with `serde_json::from_slice::<Value>`. If the top level is an array, use its first element, because Gemini's OpenAI-compatible endpoint wraps the error that way.
   - Read `error.details[]`. An entry whose `@type` ends with `QuotaFailure` sets `per_day` when any `violations[].quotaId` contains `PerDay`.
   - An entry whose `@type` ends with `RetryInfo` gives `retryDelay` (a string like `"36s"`) through `parse_duration_hint`.
   - Return None when neither is found or the body is not JSON.
   - Match on body shape only; no provider id appears in code.
2. limiter.rs: add `pub fn secs_until_day_reset(unix: u64, reset_hour: u8) -> u64`, equal to `day_end(day_index(unix, h), h) - unix` with a minimum of 1.
3. proxy.rs, in the `!matches!(class, Success | Passthrough)` branch after `body` is read, replace the `retry_after` computation for `Class::RateLimited` with this precedence:
   1. If `google_quota_hint(&body)` has `per_day`, use `Some(Duration::from_secs(limiter::secs_until_day_reset(limiter::unix_secs(SystemTime::now()), provider.reset_utc_hour)))`, and the attempt reason is `"http_429_per_day"`.
   2. Else the header hint (`Retry-After`, `x-ratelimit-reset-*`), as today.
   3. Else `retry_delay` through `clamp_hint`.
   4. Else `body_hint` through `clamp_hint`.
   5. Else None, meaning backoff.

   Pass that reason to `track.attempt` and `apply_failure`. The cooldown uses the existing `limiter::scope_key(..)` in `apply_failure`, so it respects `scope = "key_model"`. Because the hint is longer than 60 s, it is persisted by the existing snapshot.
4. In `apply_failure`, when `detail == "http_429_per_day"`, the warn line says `(429 daily quota exhausted, until next reset_utc_hour={h})` instead of `(429)`. The body excerpt from SL-10 stays.

Tests:
- limiter.rs `google_quota_failure_per_day_and_retry_info_are_parsed`:
  - The array-wrapped body below gives `per_day: true` with `retry_delay` of 36 s.
  - The same body as an object, not array-wrapped, gives the same result.
  - A per-minute quotaId (`GenerateRequestsPerMinutePerProjectPerModel-FreeTier`) gives `per_day: false` with 36 s.
  - `{}`, `[]` and non-JSON give None.
- limiter.rs `secs_until_day_reset_examples`: `(0, 0) == 86_400`, `(7*3600, 8) == 3600`, `(9*3600, 8) == 23*3600`.
- tests/proxy.rs `gemini_per_day_429_stops_upstream_calls_until_the_daily_reset` reproduces last night:
  - beta extra is `reset_utc_hour = 8\n[provider.limits]\nscope = "key_model"`. The mock always returns 429 with the body below and a `retry-after: 36` header. The header is deliberately there, to prove the per-day rule wins.
  - Send 10 requests to `beta/gemini-4-hydrogen`. The first returns 429; the rest return 503 with reason `cooldown`.
  - `beta.calls().len() == 1`.
  - `retry-after` on the 503 is within ±5 s of `sluice::limiter::secs_until_day_reset(now_unix, 8)`, and well above 36.
  - Contrast: a fresh router whose body has the per-minute quotaId and no header gives a `retry-after` in 36..=37.

  Body (array-wrapped, as Gemini's OpenAI-compatible endpoint sends it):
  `[{"error":{"code":429,"message":"You exceeded your current quota. Please retry in 36.5s.","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":[{"quotaMetric":"generativelanguage.googleapis.com/generate_content_free_tier_requests","quotaId":"GenerateRequestsPerDayPerProjectPerModel-FreeTier","quotaValue":"25"}]},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"36s"}]}}]`

Done when:
- fmt, clippy (`-D warnings`) and `cargo test --locked` pass.
- `grep -nE "pub fn google_quota_hint" src/limiter.rs` matches.
- `grep -nE "pub fn secs_until_day_reset" src/limiter.rs` matches.
- `grep -nE "google_quota_hint\(&body\)" src/proxy.rs` matches.
- `grep -nE "\"http_429_per_day\"" src/proxy.rs` matches.

Do not: hardcode "gemini", Google hostnames or model ids in Rust source. Do not change the backoff constants. Do not reset the backoff step on a per-day cooldown. Same PRD rules as SL-01.

---

ID: SL-12

Goal: Session stickiness. Once a pool request from a session is served by a route, later requests from that session try that route first while it is eligible. If it is not eligible, normal order applies and the session re-sticks to whichever route serves it. Memory is bounded.

Files: src/sticky.rs (new), src/lib.rs, src/config.rs, src/select.rs, src/proxy.rs, tests/proxy.rs

Session key, documented in module docs:
1. The first non-empty header from config `session_headers`. The default is `["x-session-id", "x-opencode-session"]`. OpenCode's own gateway requires `x-opencode-session` and the OpenCode CLI is reported to send it, but whether it sends it to custom OpenAI-compatible providers is unverified, hence the configurable list. The key is a hash of ("h", requested model, header value).
2. Otherwise a key derived from the request: a hash of ("d", requested model, the first 4096 chars of the JSON text of the first message with role `system` or `developer`, the first 4096 chars of the JSON text of the first `user` message). There is no key when there is no user message.

Only the `u64` hash is kept. Message text is never stored or logged. Use `std::hash::{DefaultHasher, Hash, Hasher}`, which is deterministic within one build and only held in memory.

Change:
1. config.rs:
   - Add top-level `#[serde(default = ..)] session_headers: Vec<String>` (the default above) and `sticky_ttl_secs: u64` (default 7200; 0 disables stickiness) to `RawConfig` and `Config`.
   - Store header names lowercased.
   - Validate that each is non-empty and only `[A-Za-z0-9-]`: `session_headers entry "{h}" is not a valid header name`.
2. src/sticky.rs:
   - `pub const MAX_STICKY_SESSIONS: usize = 4096;`
   - `pub struct Sticky { inner: std::sync::Mutex<HashMap<u64, (String, String, Instant)>>, ttl: Duration }`, `#[derive(Debug)]`
   - `pub fn new(ttl: Duration) -> Self`
   - `pub fn get(&self, key: u64, now: Instant) -> Option<(String, String)>`: removes the entry and returns None when it is older than ttl, or when ttl is 0.
   - `pub fn set(&self, key: u64, provider: &str, model: &str, now: Instant)`: overwrites. When the map is over `MAX_STICKY_SESSIONS`, first drop expired entries, then remove the entry with the oldest timestamp.
   - `pub fn len(&self, now: Instant) -> usize`: counts unexpired entries.
   - Lock with `unwrap_or_else(PoisonError::into_inner)`, as `Limiter::lock` does. Never across an await.
3. lib.rs: `pub mod sticky;` and `AppState.sticky: sticky::Sticky`, built in `AppState::new` from `config.sticky_ttl_secs`.
4. select.rs: `pub fn prefer(eligible: &mut [Entry<'_>], provider: &str, model: &str)`, a stable `sort_by_key(|e| !(e.provider.id == provider && e.model == model))`. It is pure.
5. proxy.rs:
   - Add `fn session_key(headers: &HeaderMap, names: &[String], requested: &str, fields: &Map<String, Value>) -> Option<u64>`.
   - In `forward`, compute it only when `is_pool`. Make the plan binding `let mut plan`.
   - After the wait loop: `if let Some(k) = session && let Some((p, m)) = state.sticky.get(k, Instant::now()) { select::prefer(&mut plan.eligible, &p, &m); }`.
   - In both success branches, right where `state.limiter.record_success(&scope, entry.model)` is called, also call `state.sticky.set(k, &provider.id, entry.model, Instant::now())` when there is a key.

Tests:
- sticky.rs `expires_after_ttl_and_is_bounded`: get after the ttl gives None. Inserting MAX+10 keys keeps `len` at or below MAX, and the newest key survives. A ttl of 0 never returns.
- select.rs `prefer_moves_the_sticky_route_first_and_keeps_order`: with eligible a0, a1, b0, preferring b gives b0, a0, a1. Preferring an unknown route leaves the order unchanged.
- proxy.rs unit `session_key_prefers_header_then_derives_from_first_messages`:
  - Identical messages give equal keys.
  - A different first user message gives a different key.
  - Adding later messages keeps the key.
  - A header overrides the derived key.
  - No user message gives None.
- tests/proxy.rs `session_sticks_to_its_route_while_healthy_and_moves_when_it_cools`: pool `coder` = [alpha, beta] with priority. alpha's mock always returns 200. beta's mock is `Sequence[200, 200, 429 with retry-after 60, 200]`.
  1. Pause alpha. A request with `x-session-id: s1` is served by beta. Resume alpha.
  2. s1 is served by beta again, even though alpha has higher priority.
  3. s2 is served by alpha.
  4. s1 gets a 429 from beta, fails over, and is served by alpha.
  5. `POST /admin/cooldowns/clear`, then s1 is served by alpha (re-stuck).
- tests/proxy.rs `derived_session_key_sticks_without_a_header`: the same idea with no header and identical messages.

Done when:
- fmt, clippy (`-D warnings`) and `cargo test --locked` pass.
- `grep -nE "pub const MAX_STICKY_SESSIONS" src/sticky.rs` matches.
- `grep -nE "select::prefer\(&mut plan\.eligible" src/proxy.rs` matches.
- `grep -nE "state\.sticky\.set\(" src/proxy.rs` finds 2 lines.

Do not: store or log message text or header values. Do not make stickiness bypass eligibility. Do not apply it to explicit routes. Do not add an LRU crate. Same PRD rules as SL-01.

---

ID: SL-13

Goal: `/status` and the `counters` event expose billing, spend and credit, the 402 block, the `tpmo` month counter and the sticky session count, so the dashboard can show them.

Files: src/status.rs, tests/proxy.rs

Change:
1. `provider_json` (base fields, so they appear in `counters` too) adds:
   - `"billing": p.billing.name()`
   - `"spend_today_usd": state.limiter.provider_spend_today(&p.id, now)`
   - `"payment_required"` (exists since SL-09)
   - `"tokens_month": view.tokens_month`
   - `"credit"`: for credit providers, `state.limiter.credit_status(p, now)` mapped to `{"usd", "spent_usd", "guard_usd": usd * CREDIT_GUARD_FRACTION, "period_start_ms", "resets_at_ms", "exhausted"}` (the times are Unix ms). Null for other providers.
2. The `limits` object in the detail block adds `"tpmo"`. `limits_json` adds `"tpmo"`.
3. `provider_state` order: paused, payment_required, credit_exhausted (with `until` = the reset as an Instant, computed as `now + (resets_at - unix)` seconds), key_disabled, breaker_open, cooling, ok.
4. `snapshot` top level adds:
   - `"spend_today_usd": state.limiter.total_spend_today(now)`, across all providers
   - `"sticky_sessions": state.sticky.len(now)`
   - Keep the existing `"paid"` object unchanged.

Tests (tests/proxy.rs):
- `status_reports_billing_spend_credit_and_sticky_sessions`: beta is credit (as in SL-06, with credit_usd 1.0) and alpha is free with no prices.
  - One alpha request and one beta request through `coder`, with `x-session-id`.
  - `/status`: alpha `billing == "free"` and `spend_today_usd == 0`. beta `billing == "credit"`, `credit.spent_usd > 0`, `credit.resets_at_ms > now_ms`, `credit.exhausted == false`.
  - Top-level `spend_today_usd` equals beta's, and `sticky_sessions >= 1`.
  - money `billing == "paid"`.
- The existing `t26_status_and_events_carry_no_key_material_or_message_text` stays green.

Done when:
- fmt, clippy (`-D warnings`) and `cargo test --locked` pass.
- `grep -nE "\"sticky_sessions\"" src/status.rs` matches.
- `grep -nE "\"spend_today_usd\"" src/status.rs` finds 2 lines.
- `grep -nE "\"credit_exhausted\"" src/status.rs` matches.

Do not: remove or rename existing JSON fields. Do not include body excerpts, keys or session keys. Same PRD rules as SL-01.

---

ID: SL-14

Goal: Dashboard. Each provider card shows its billing type. Credit providers show spend this period against credit, the guard and the reset date. A 402 block shows a "Clear 402" button. The summary bar shows estimated spend today across all providers and the sticky session count. A `tpmo` bar appears when configured.

Files: ui/index.html

Change:
1. CSS:
   - `.tag.credit{color:var(--info);border-color:var(--info)}`
   - `.tag.free{color:var(--ok);border-color:var(--ok)}`
   - `.st-payment_required{--c:var(--bad)}`
   - `.st-credit_exhausted{--c:var(--cooling)}`
2. Add `function fmtUsd4(v)`, which shows 4 decimals when below $1 and 2 otherwise.
3. In `mkTiles`, add `['spend', 'est. spend today']` and `['sticky', 'sticky sessions']`. In `renderSummary`:
   - `setTile('spend', fmtUsd4(S.spend_today_usd), 'all providers, estimate')`
   - `setTile('sticky', String(S.sticky_sessions || 0), 'idle 2 h expiry')`
   - Keep the paid tile.
4. In `setPill`, show the countdown for `credit_exhausted` as well.
5. In `routeState`, add `payment_required` ("402: cleared on dashboard") and `credit_exhausted` (until `p._until`, "credit guard reached").
6. In `updateCard`:
   - Replace the `p.paid` tag with `el('span', 'tag ' + p.billing, p.billing)`.
   - For `p.credit`, add the note `'credit: ' + fmtUsd4(spent_usd) + ' of ' + fmtUsd(usd) + ' (stops at ' + fmtUsd(guard_usd) + '), resets ' + new Date(resets_at_ms).toLocaleDateString()`, with class `'p'` when exhausted.
   - Show the bad-styled note `'402 payment required: provider disabled until cleared'` when `p.payment_required`.
   - Show `'spend today ' + fmtUsd4(p.spend_today_usd)` as a stat or note.
7. In `mkCard`, add a `c.pay = el('button', null, 'Clear 402')`. In `updateCard`, set `c.pay.hidden = !p.payment_required`. Its onclick calls `admin(c, c.pay, '/admin/provider/' + encodeURIComponent(p.id) + '/payment/clear', function () { return 'Clear 402'; }, function (j) { return j.cleared ? 'cleared' : 'nothing to clear'; })`.
8. Add `['tpmo', 'tokens_month', 'tokens / month']` to `BARS`. Hide that bar in `updateCard` when `lim.tpmo` is null, so cards without it look unchanged.

Tests: no new Rust test. `t25_dashboard_file_has_no_external_urls_and_is_small` (under 60 KB, no http(s)://) and `t25_root_serves_the_dashboard_as_html` must stay green.

Done when:
- `cargo test --locked` passes (t25 included).
- `grep -nE "/payment/clear'" ui/index.html` matches.
- `grep -nE "S\.spend_today_usd" ui/index.html` matches.
- `grep -nE "S\.sticky_sessions" ui/index.html` matches.
- `grep -nE "'tag ' \+ p\.billing" ui/index.html` matches.
- `grep -nE "\['tpmo', 'tokens_month'" ui/index.html` matches.
- `wc -c ui/index.html` is under 61440.

Do not: add external scripts, fonts, URLs, frameworks or a build step. Do not show body excerpts, keys or message text. Do not exceed 60 KB. Same PRD rules as SL-01.

---

ID: SL-15

Goal: Document the new behaviour in README.md and router.example.toml: billing, credit, the seed, tpmo, 402 clearing, Gemini per-day handling, stickiness and reported cost.

Files: README.md, router.example.toml

Change:
1. README provider field table:
   - Replace the `paid` row with a `billing` row: `free` | `credit` | `paid`; unset means paid; only free and credit may appear in pools; the legacy `paid = true` still means paid, `paid = false` is rejected, and conflicting combinations are rejected.
   - Add rows for `credit_usd`, `credit_reset_day` (1-31; past the end of a short month means the last day; at `reset_utc_hour`; also sets the `tpmo` month), `credit_spent_usd` + `credit_spent_date` (the seed: applies only to the credit period containing that date, as a floor that never double-counts), and `usage_cost_field` (the name of a cost-in-USD field inside `usage`; for OpenRouter use `"cost"`).
   - Add `tpmo` to the `[provider.limits]` row.
   - Update the `[provider.prices]` row: required for credit; an unpriced model is charged at the highest configured price; free providers without prices count as $0.
2. README sections:
   - **Credit providers**: the 90% guard, skip reason `credit_exhausted`, persistence, and an example for Mistral's already-spent $10.
   - **Failure handling**: 401 disables the key until restart. 402 disables the whole provider until cleared with `POST /admin/provider/{id}/payment/clear` or the dashboard button, survives restart, and is logged at error level. 429 and 402 log lines include a 300-char sanitised body excerpt. Google per-day quota 429s cool the route until the next `reset_utc_hour`; for Gemini set `reset_utc_hour = 8` (Google resets at midnight Pacific; 8 is safe in both PST and PDT).
   - **Session stickiness**: header list and derived key, TTL, the 4096 cap, eligibility, re-sticking, and the count on the dashboard.
3. Add `POST /admin/provider/{id}/payment/clear` to the endpoints table.
4. Add `session_headers` and `sticky_ttl_secs` to the top-level config list.
5. router.example.toml:
   - Add `# session_headers = ["x-session-id", "x-opencode-session"]` and `# sticky_ttl_secs = 7200`.
   - Add `usage_cost_field = "cost"` to or-free.
   - Add a commented credit example block for `mistral` with `billing = "credit"`, `credit_usd = 10.0`, `credit_reset_day = 1`, a commented `credit_spent_usd` / `credit_spent_date`, a placeholder `[provider.prices."example-model"]`, and `tpmo` in limits.
   - Add `reset_utc_hour = 8` with a comment on gemini.
   - Keep every limit and price at 0 or as a placeholder.

Tests: none new. `shipped_example_file_is_valid` must pass with the edited example.

Done when:
- `cargo test --locked` passes.
- `grep -nE "^billing = \"(free|paid)\"" router.example.toml` finds at least 4 lines.
- `grep -nE "^usage_cost_field = \"cost\"" router.example.toml` matches.
- `grep -nE "^# sticky_ttl_secs = 7200" router.example.toml` matches.
- `grep -nE "^\| \`billing\`" README.md` matches.
- `grep -nE "/admin/provider/\{id\}/payment/clear" README.md` matches.

Do not: put real keys, real limits or real prices in the example. Do not edit router.toml or ROUTER_PRD.md. Do not write new .md files.
---

ID: SL-16
Goal: Two dashboard fixes requested by Mai. (1) The live routing panel shows only the 10 most recent requests, with the rest behind a toggle. (2) Provider colours in the usage chart and on the provider cards never use colours that read as status (red, amber, green), so a provider such as or-free or llm7 no longer looks like it is failing.
Files: `ui/index.html` only. Read `tests/dashboard.rs` (T25: no external URLs, under 60 KB).
Change:
1. Live routing: render in-flight rows plus the 10 most recent finished rows that pass the current filters. Below them, when more finished rows exist, show a button "Show N more" (N = how many are hidden). Clicking it expands to the full list (up to the 200 kept in memory) and the button becomes "Show fewer". New rows still appear at the top; in collapsed mode the oldest visible row drops off as new ones arrive (rolling window). The expanded/collapsed choice survives re-renders and is stored in localStorage (wrap every localStorage access in try/catch).
2. Provider colours: find where per-provider series colours are assigned (chart bars, legend swatches, card swatches, sparklines). Replace that palette with 12 distinct hues that avoid the status colours: no reds, oranges, ambers, yellows or greens. Use blues, indigos, violets, purples, magentas, pinks, teals, cyans, slates and browns, all readable on the dark background. Define them as CSS custom properties or one constant array. Assign colours by the provider's index in config order so they are stable across reloads. Status colours (ok, cooling, breaker or error, paused, missing) stay exactly as they are, and are used only for status.
Tests: none new. Run `cargo test --locked`; T25 must still pass (file under 60 KB, no http(s) URLs).
Done when: `cargo test --locked` passes; `node --check` passes on the script extracted from ui/index.html; `grep -c "Show fewer" ui/index.html` is at least 1.
Do not: change any Rust file, the /status or /events payloads, or the status colours. Do not add external fonts or scripts.
