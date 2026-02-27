# RPC Gateway / Reliability Layer (Rust)

Rust HTTP gateway that forwards JSON-RPC calls to multiple upstream providers with:

- Multi-provider failover
- Adaptive intelligent hedging (width 1-4 based on live p95 spread + RPS)
- Predictive provider scoring (latency EWMA, success rate, block lag, rate-limit headroom)
- Shadow-mode provider onboarding with mirrored traffic and auto-promotion
- Built-in Solana method policy table (cacheability + consensus-critical classification)
- Config-driven method policy overrides for custom/special methods
- Request coalescing/dedup for concurrent identical read calls
- Cross-provider consensus validation for consensus-critical methods
- Cost-aware provider routing policy (cheapest/balanced/latency-first)
- In-memory response caching with per-method TTL overrides
- Provider health tracking and simple circuit-breaker behavior
- Active background health probes

The default profile is now Solana JSON-RPC (the gateway core is still chain-agnostic).

## Endpoints

- `POST /rpc`: forwards a JSON-RPC request body to upstream providers
- `GET /health/live`: liveness check
- `GET /health/providers`: provider health snapshots + adaptive hedging and coalescing runtime stats

## Quick Start

1. Update [config/gateway.toml](/c:/Users/HP/RPC-Gateway/config/gateway.toml) with real provider URLs and auth.
2. Run:

```bash
cargo run
```

3. Send a request:

```bash
curl -sS http://localhost:8080/rpc ^
  -H "content-type: application/json" ^
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getSlot\",\"params\":[]}"
```

## Behavior Notes

- Providers are ranked by dynamic health score + static weight.
- Predictive scoring combines latency factor, success rate, block lag factor, and rate-limit headroom.
- Block lag is polled per provider using `reliability.predictive_scoring.block_lag_method` (default `getSlot`).
- Providers marked `shadow_mode = true` receive mirrored requests but are excluded from live routing until warmup + minimum observations are met.
- On failure, the gateway fails over to the next ranked provider.
- Adaptive mode chooses hedge width `1..4` from:
  - current request rate (RPS window)
  - p95 latency spread across ranked providers
- Under high RPS, hedge width shrinks toward `min_hedge_width`; under high latency variance, it expands toward `max_hedge_width`.
- Legacy fixed two-provider hedging is still available when adaptive hedging is disabled and `hedge_delay_ms > 0`.
- Request coalescing fans out one in-flight upstream call to many identical concurrent requests.
- Consensus validation can query top providers for consensus-critical methods and require majority agreement.
- Cost-aware routing can down-rank expensive providers using `providers[].cost_per_million_requests`.
- Cache keys are `method + sha256(body)`.
- Default cacheability now comes from the Solana method policy table when `cache.cacheable_methods` is empty.
- `method_policy.overrides` can override cacheability/consensus-critical flags per method.
- Unknown methods are treated as non-cacheable by default for safety.
- Default cache TTL is `cache.ttl_secs`; per-method overrides are in `cache.method_ttl_secs`.
- If `cache.cacheable_methods` is set, only listed methods are cacheable (unless a method-specific TTL override is present).
- Background probes run every `reliability.probe.interval_secs` and update provider health even without live traffic.
- Any upstream non-2xx response is treated as failure for failover purposes.

## Config

Default config path is:

- `config/gateway.toml`

Override path with:

- `RPC_GATEWAY_CONFIG=<path>`

Main knobs:

- `reliability.request_timeout_ms`
- `reliability.hedge_delay_ms`
- `reliability.max_failover_attempts`
- `reliability.unhealthy_after_failures`
- `reliability.recovery_after_secs`
- `reliability.adaptive_hedging.enabled`
- `reliability.adaptive_hedging.min_hedge_width`
- `reliability.adaptive_hedging.max_hedge_width`
- `reliability.adaptive_hedging.rps_window_secs`
- `reliability.adaptive_hedging.medium_rps`
- `reliability.adaptive_hedging.high_rps`
- `reliability.adaptive_hedging.medium_latency_spread_ms`
- `reliability.adaptive_hedging.high_latency_spread_ms`
- `reliability.adaptive_hedging.max_latency_samples`
- `reliability.predictive_scoring.enabled`
- `reliability.predictive_scoring.target_latency_ms`
- `reliability.predictive_scoring.max_block_lag`
- `reliability.predictive_scoring.unknown_block_lag_factor`
- `reliability.predictive_scoring.unknown_rate_limit_headroom`
- `reliability.predictive_scoring.block_lag_poll_interval_secs`
- `reliability.predictive_scoring.block_lag_method`
- `reliability.predictive_scoring.block_lag_params`
- `reliability.shadow_mode.enabled`
- `reliability.shadow_mode.default_warmup_secs`
- `reliability.shadow_mode.default_min_observations`
- `reliability.shadow_mode.mirror_max_providers`
- `reliability.coalescing.enabled`
- `reliability.consensus_validation.enabled`
- `reliability.consensus_validation.sample_size`
- `reliability.consensus_validation.fail_open`
- `reliability.cost_routing.enabled`
- `reliability.cost_routing.strategy`
- `reliability.probe.enabled`
- `reliability.probe.interval_secs`
- `reliability.probe.timeout_ms`
- `reliability.probe.method`
- `reliability.probe.params`
- `cache.enabled`
- `cache.ttl_secs`
- `cache.max_capacity`
- `cache.cacheable_methods`
- `cache.method_ttl_secs`
- `method_policy.overrides.<method>.cacheable_by_default`
- `method_policy.overrides.<method>.consensus_critical`
- `providers[].shadow_mode`
- `providers[].shadow_warmup_secs`
- `providers[].shadow_min_observations`
- `providers[].cost_per_million_requests`

## Response Headers

`/rpc` responses include:

- `x-rpc-provider`: provider that returned the response
- `x-rpc-attempts`: number of upstream attempts
- `x-rpc-hedged`: `true` if a hedge race was used
- `x-rpc-hedge-width`: selected hedge width for the request (`0` for cache hits)
- `x-rpc-coalesced`: `true` when response joined an in-flight coalesced request
- `x-rpc-consensus-critical`: `true` when method is marked consensus-critical by policy table
- `x-rpc-consensus-checked`: `true` when cross-provider consensus voting was attempted
- `x-rpc-consensus-validated`: `true` when a majority agreed
- `x-rpc-consensus-agreement`: agreement ratio like `2/3` when checked
- `x-rpc-cache`: `HIT` or `MISS`
- `x-rpc-cache-timestamp`: cache entry timestamp in unix ms (on cache hit)

`GET /health/providers` returns:

- `providers[]` with health snapshot and `predictive` scoring/shadow telemetry
- `adaptive_hedging` runtime telemetry (hedge win rate, observed RPS/spread)
- `coalescing` runtime telemetry (`in_flight`, `leader_count`, `fanout_count`)

## Solana Policy Table

Built-in classifications in [src/gateway/method_policy.rs](/c:/Users/HP/RPC-Gateway/src/gateway/method_policy.rs):

- Cacheable + consensus-critical examples: `getBalance`, `getAccountInfo`, `getProgramAccounts`, `getLatestBlockhash`, `getBlock`, `getTransaction`
- Cacheable + non-critical examples: `getSlot`, `getBlockHeight`, `getHealth`, `getVersion`
- Non-cacheable examples: `sendTransaction`, `requestAirdrop`, `simulateTransaction`

Runtime overrides are supported in config:

```toml
[method_policy.overrides."customExperimentalMethod"]
cacheable_by_default = true
consensus_critical = true
```

## Architecture

System design and module responsibilities are documented in [ARCHITECTURE.md](/c:/Users/HP/RPC-Gateway/ARCHITECTURE.md).
