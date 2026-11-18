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
