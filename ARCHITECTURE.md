# Architecture

## Goals

- Provide resilient JSON-RPC proxying across multiple upstream providers.
- Optimize p99 latency with adaptive hedging while controlling upstream cost.
- Keep behavior deterministic and observable via health and hedging runtime stats.
- Use predictive scoring and shadow-mode onboarding for safer provider selection.
- Support modular growth (consensus validation, coalescing, multi-chain routing).
- Default deployment profile is Solana JSON-RPC (`getHealth` probes, `getSlot` lag polling).

## High-Level Components

- HTTP API server: [src/main.rs](/c:/Users/HP/RPC-Gateway/src/main.rs)
- Gateway orchestrator: [src/gateway/mod.rs](/c:/Users/HP/RPC-Gateway/src/gateway/mod.rs)
- Adaptive hedging controller: [src/gateway/adaptive.rs](/c:/Users/HP/RPC-Gateway/src/gateway/adaptive.rs)
- In-flight request coalescer: [src/gateway/coalescing.rs](/c:/Users/HP/RPC-Gateway/src/gateway/coalescing.rs)
- Consensus voting helpers: [src/gateway/consensus.rs](/c:/Users/HP/RPC-Gateway/src/gateway/consensus.rs)
- Cost routing policy: [src/gateway/cost.rs](/c:/Users/HP/RPC-Gateway/src/gateway/cost.rs)
- Predictive scoring + shadow controller: [src/gateway/predictive.rs](/c:/Users/HP/RPC-Gateway/src/gateway/predictive.rs)
- Cache policy and store: [src/gateway/cache.rs](/c:/Users/HP/RPC-Gateway/src/gateway/cache.rs)
- Solana method policy table: [src/gateway/method_policy.rs](/c:/Users/HP/RPC-Gateway/src/gateway/method_policy.rs)
- RPC helpers: [src/gateway/rpc.rs](/c:/Users/HP/RPC-Gateway/src/gateway/rpc.rs)
- Probe payload builder: [src/gateway/probe.rs](/c:/Users/HP/RPC-Gateway/src/gateway/probe.rs)
- Provider health tracker: [src/health.rs](/c:/Users/HP/RPC-Gateway/src/health.rs)
- Configuration model: [src/settings.rs](/c:/Users/HP/RPC-Gateway/src/settings.rs)

## Request Lifecycle

1. `POST /rpc` receives a JSON-RPC payload.
2. Gateway records the request for adaptive RPS tracking.
3. Cache key/TTL policy is resolved by method + payload hash.
4. Solana method policy table (built-in + config overrides) classifies cacheability and consensus-critical status.
5. If cached and unexpired, response is returned immediately.
6. If enabled and request is coalescable, in-flight dedup fanouts concurrent identical calls.
7. Providers are ranked by health score + predictive score + optional cost routing adjustment.
8. Providers in shadow mode are excluded from live routing until warmup + minimum mirrored observations are satisfied.
9. Dispatch strategy is selected:
   - Cross-provider consensus validation for consensus-critical methods (when enabled).
   - Adaptive hedging (parallel width `1..4`) when enabled.
   - Legacy fixed two-provider hedging when adaptive is disabled and delay is configured.
   - Sequential failover otherwise.
10. Successful cacheable responses are inserted with method-specific TTL.
11. Response is returned with metadata headers (`x-rpc-provider`, `x-rpc-coalesced`, `x-rpc-consensus-validated`, etc.).

## Adaptive Hedging Design

Decision inputs:

- RPS over a sliding window (`adaptive_hedging.rps_window_secs`)
- p95 latency spread across ranked candidate providers
- policy thresholds (`medium/high_rps`, `medium/high_latency_spread_ms`)

Decision behavior:

- High load: reduce to `min_hedge_width`.
- High latency spread: increase to `max_hedge_width`.
- Moderate spread and low load: use intermediate width.
- Low variance: use `min_hedge_width`.

Runtime stats tracked:

- total hedged requests
- hedge wins (winner != preferred provider)
- hedge win rate
- observed RPS and observed latency spread
- last decision reason

These are exposed in `/health/providers` under `adaptive_hedging`.

## Provider Health Model

Each provider has an independent `ProviderHealthTracker`:

- records successes/failures
- computes latency EWMA
- opens circuit after consecutive failures
- auto-recovers after configured cooldown
- produces score used in ranking

Active probes run on a timer and feed the same health tracker so rankings stay current even under low live traffic.

## Predictive Scoring + Shadow Mode

Predictive scoring uses:

- `latency_factor`: normalized against `target_latency_ms`
- `success_rate`: from rolling success/failure counters
- `block_factor`: derived from lag to highest observed head block
- `rate_limit_headroom`: parsed from upstream rate-limit headers when available

Composite score is the product of those factors with provider static weight.

Shadow mode:

- Provider with `shadow_mode = true` receives mirrored traffic.
- It is promoted to live once warmup duration and minimum observations are both met.
- Promotion and shadow telemetry are visible in `/health/providers` under `providers[].predictive.shadow`.

## Cost-Aware Routing

- Enabled via `reliability.cost_routing.enabled`.
- `strategy` supports:
  - `cheapest` / `cheapest_healthy_first`: strongest cost bias
  - `balanced`: moderate cost bias
  - `latency_first`: light cost bias
- Provider prices come from `providers[].cost_per_million_requests`.
- Cost factor is applied to ranked provider scores before dispatch.

## Request Coalescing

- Enabled via `reliability.coalescing.enabled`.
- Coalescing key: `method_lowercase + sha256(request_body)`.
- Only one upstream request is executed for concurrent identical coalescable calls.
- Waiting callers receive the same result via in-memory fanout.
- Telemetry is exposed in `/health/providers` under `coalescing`.

## Consensus Validation

- Enabled via `reliability.consensus_validation.enabled`.
- Applied to consensus-critical methods from policy table.
- Queries top ranked providers in parallel (`sample_size` in range `2..3`).
- Majority vote uses method-aware fingerprints (`getBalance.value`, `getLatestBlockhash.blockhash`, etc.).
- If no majority:
  - strict mode (`fail_open = false`) returns `502`
  - fail-open mode returns best available response with `x-rpc-consensus-validated: false`

## Caching Model

- Key: `method_lowercase + sha256(request_body)`
- TTL resolution:
  - method override from `cache.method_ttl_secs`
  - else default `cache.ttl_secs` if method policy marks it cacheable
  - method override with `0` disables cache for that method
- Store:
  - in-memory map with per-entry expiry
  - bounded by capacity
  - evicts expired, then oldest when full

## Concurrency and Cancellation

- Parallel hedges are executed via Tokio tasks.
- Gateway returns on first success and aborts remaining hedge tasks.
- Dropped/aborted request futures cancel in-flight upstream work.
- Shadow mirror requests are fire-and-forget and never block client responses.

## Method Policy Overrides

- Built-in Solana classifications live in [src/gateway/method_policy.rs](/c:/Users/HP/RPC-Gateway/src/gateway/method_policy.rs).
- Operators can override cacheability and consensus-critical flags via:
  - `method_policy.overrides.<method>.cacheable_by_default`
  - `method_policy.overrides.<method>.consensus_critical`
- Overrides are case-insensitive and apply before cache planning and response metadata headers.

## Extension Points

- Add chain router for multi-chain in one process (`/rpc/{chain}`).
- Replace in-memory cache backend with hybrid Redis/SQLite layer.
- Add deeper per-method consensus key extractors for more Solana/EVM methods.
- Add request coalescing limits/backpressure per API key or tenant.
