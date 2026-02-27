mod adaptive;
mod cache;
mod coalescing;
mod consensus;
mod cost;
mod method_policy;
mod predictive;
mod probe;
mod rpc;

use std::{
    cmp::Ordering,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use reqwest::{Client, header};
use serde::Serialize;
use tokio::{task::JoinSet, time};
use tracing::{info, warn};

use crate::{
    health::{ProviderHealthSnapshot, ProviderHealthTracker},
    settings::{ProviderConfig, ReliabilityConfig, Settings},
};

use self::{
    adaptive::{AdaptiveHedgingController, HedgeDecision},
    cache::{CachePolicy, ResponseCache},
    coalescing::{CoalescingStatsView, RequestCoalescer, request_coalescing_key},
    consensus::{ConsensusCandidate, decide_consensus},
    cost::CostRoutingPolicy,
    method_policy::{SolanaMethodPolicy, SolanaMethodPolicyTable},
    predictive::{
        PredictiveScoringController, ProviderPredictiveView, parse_block_number_from_rpc_body,
    },
    probe::build_probe_payload,
};

pub use self::adaptive::HedgingStatsView;

pub struct Gateway {
    client: Client,
    providers: Vec<ProviderRuntime>,
    reliability: ReliabilityConfig,
    predictive_scoring: Arc<PredictiveScoringController>,
    adaptive_hedging: Arc<AdaptiveHedgingController>,
    cache_policy: CachePolicy,
    cache: Option<ResponseCache>,
    coalescer: RequestCoalescer<GatewayResponse, DispatchError>,
    cost_routing: CostRoutingPolicy,
    method_policy: SolanaMethodPolicyTable,
    probe_payload: Bytes,
    block_lag_probe_payload: Bytes,
}

impl Gateway {
    pub fn from_settings(settings: Settings) -> anyhow::Result<Self> {
        let client = Client::builder()
            .pool_idle_timeout(Some(Duration::from_secs(30)))
            .build()?;

        let Settings {
            reliability,
            cache,
            method_policy,
            providers: provider_configs,
            ..
        } = settings;

        let predictive_scoring = Arc::new(PredictiveScoringController::new(
            reliability.predictive_scoring.clone(),
            reliability.shadow_mode.clone(),
            &provider_configs,
        ));
        let cost_routing =
            CostRoutingPolicy::from_config(&reliability.cost_routing, &provider_configs);

        let providers = provider_configs
            .into_iter()
            .map(|config| ProviderRuntime {
                health: Arc::new(ProviderHealthTracker::new(
                    reliability.unhealthy_after_failures,
                    reliability.recovery_after(),
                )),
                config,
            })
            .collect::<Vec<_>>();
        let adaptive_hedging = Arc::new(AdaptiveHedgingController::new(
            reliability.adaptive_hedging.clone(),
            providers.len(),
        ));

        let cache_policy = CachePolicy::from_config(&cache);
        let cache = if cache_policy.enabled() {
            Some(ResponseCache::new(cache.max_capacity))
        } else {
            None
        };
        let coalescer = RequestCoalescer::new(reliability.coalescing.enabled);
        let method_policy = SolanaMethodPolicyTable::from_config(&method_policy);

        let probe_payload = build_probe_payload(&reliability.probe)?;
        let block_lag_probe_payload = Bytes::from(serde_json::to_vec(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": "rpc-gateway-block-lag-probe",
            "method": reliability.predictive_scoring.block_lag_method.clone(),
            "params": reliability.predictive_scoring.block_lag_params.clone(),
        }))?);

        Ok(Self {
            client,
            providers,
            reliability,
            predictive_scoring,
            adaptive_hedging,
            cache_policy,
            cache,
            coalescer,
            cost_routing,
            method_policy,
            probe_payload,
            block_lag_probe_payload,
        })
    }

    pub fn spawn_probe_loop(self: &Arc<Self>) {
        if !self.reliability.probe.enabled {
            return;
        }

        info!(
            interval_secs = self.reliability.probe.interval_secs,
            method = self.reliability.probe.method.as_str(),
            "starting provider background probes"
        );

        let gateway = Arc::clone(self);
        tokio::spawn(async move {
            gateway.probe_loop().await;
        });
    }

    pub fn spawn_predictive_scoring_loop(self: &Arc<Self>) {
        if !self.predictive_scoring.predictive_enabled() {
            return;
        }

        info!(
            interval_secs = self
                .reliability
                .predictive_scoring
                .block_lag_poll_interval_secs,
            method = self
                .reliability
                .predictive_scoring
                .block_lag_method
                .as_str(),
            "starting predictive block-lag polling loop"
        );

        let gateway = Arc::clone(self);
        tokio::spawn(async move {
            gateway.block_lag_poll_loop().await;
        });
    }

    pub async fn execute_rpc(&self, body: Bytes) -> Result<GatewayResponse, DispatchError> {
        self.adaptive_hedging.record_request();

        let method = rpc::extract_rpc_method(&body);
        let policy = self.method_policy.policy_for_opt(method.as_deref());
        let cache_plan =
            self.cache_policy
                .plan(&body, method.as_deref(), policy.cacheable_by_default);

        if let (Some(cache), Some(plan)) = (&self.cache, cache_plan.as_ref()) {
            if let Some(hit) = cache.get(&plan.key).await {
                return Ok(GatewayResponse {
                    body: hit.body,
                    provider: hit.provider,
                    attempts: 0,
                    hedged: false,
                    hedge_width: 0,
                    cache_hit: true,
                    cached_at_unix_ms: Some(hit.cached_at_unix_ms),
                    coalesced: false,
                    consensus_critical: policy.consensus_critical,
                    consensus_checked: false,
                    consensus_validated: false,
                    consensus_agreement: None,
                });
            }
        }

        let should_coalesce =
            self.coalescer.enabled() && policy.cacheable_by_default && method.as_deref().is_some();
        let maybe_coalescing_key = method
            .as_deref()
            .filter(|_| should_coalesce)
            .map(|rpc_method| request_coalescing_key(rpc_method, &body));

        if let Some(coalescing_key) = maybe_coalescing_key {
            let body_for_run = body.clone();
            let method_for_run = method.clone();
            let cache_plan_for_run = cache_plan;
            let (result, coalesced) = self
                .coalescer
                .run_or_join(coalescing_key, || async move {
                    self.execute_uncached_request(
                        body_for_run,
                        method_for_run,
                        policy,
                        cache_plan_for_run,
                    )
                    .await
                })
                .await;
            return result.map(|mut response| {
                response.coalesced = coalesced;
                response
            });
        }

        self.execute_uncached_request(body, method, policy, cache_plan)
            .await
    }

    async fn execute_uncached_request(
        &self,
        body: Bytes,
        method: Option<String>,
        policy: SolanaMethodPolicy,
        cache_plan: Option<cache::CachePlan>,
    ) -> Result<GatewayResponse, DispatchError> {
        self.spawn_shadow_mirror(body.clone(), method.as_deref());

        let mut response =
            if policy.consensus_critical && self.reliability.consensus_validation.enabled {
                self.dispatch_with_consensus_validation(body.clone(), method.as_deref())
                    .await?
            } else {
                self.dispatch_with_reliability(body.clone(), method.as_deref())
                    .await?
            };
        response.cache_hit = false;
        response.consensus_critical = policy.consensus_critical;

        let allow_cache_insert =
            if policy.consensus_critical && self.reliability.consensus_validation.enabled {
                response.consensus_validated
            } else {
                true
            };

        if allow_cache_insert && let (Some(cache), Some(plan)) = (&self.cache, cache_plan) {
            cache
                .insert(
                    plan.key,
                    response.body.clone(),
                    response.provider.clone(),
                    plan.ttl,
                )
                .await;
        }

        Ok(response)
    }

    pub fn provider_health(&self) -> Vec<ProviderHealthView> {
        self.providers
            .iter()
            .enumerate()
            .map(|(index, provider)| {
                let snapshot = provider.health.snapshot(provider.config.weight);
                let predictive = self
                    .predictive_scoring
                    .score_provider(index, &snapshot, provider.config.weight)
                    .view;
                ProviderHealthView {
                    name: provider.config.name.clone(),
                    url: provider.config.url.clone(),
                    snapshot,
                    predictive,
                }
            })
            .collect()
    }

    pub fn hedging_stats(&self) -> HedgingStatsView {
        self.adaptive_hedging.stats_snapshot()
    }

    pub fn coalescing_stats(&self) -> CoalescingStatsView {
        self.coalescer.stats()
    }

    async fn probe_loop(self: Arc<Self>) {
        let mut ticker = time::interval(self.reliability.probe.interval());
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;
            self.probe_all_providers().await;
        }
    }

    async fn probe_all_providers(&self) {
        for provider_index in 0..self.providers.len() {
            self.probe_provider(provider_index).await;
        }
    }

    async fn probe_provider(&self, provider_index: usize) {
        let provider = &self.providers[provider_index];
        let timeout = self.reliability.probe.timeout();

        let mut request = self
            .client
            .post(provider.config.url.as_str())
            .header(header::CONTENT_TYPE, "application/json")
            .body(self.probe_payload.clone());

        for (key, value) in &provider.config.headers {
            request = request.header(key, value);
        }

        let started = Instant::now();
        let response = match time::timeout(timeout, request.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                provider.health.record_failure();
                warn!(
                    provider = provider.config.name.as_str(),
                    error = %error,
                    "provider probe request failed"
                );
                return;
            }
            Err(_) => {
                provider.health.record_failure();
                warn!(
                    provider = provider.config.name.as_str(),
                    timeout_ms = timeout.as_millis(),
                    "provider probe timed out"
                );
                return;
            }
        };

        if response.status().is_success() {
            let _ = self
                .predictive_scoring
                .record_rate_limit_headers(provider_index, response.headers());
            provider.health.record_success(started.elapsed());
            return;
        }

        let status = response.status().as_u16();
        let body = response.bytes().await.unwrap_or_default();
        provider.health.record_failure();
        warn!(
            provider = provider.config.name.as_str(),
            status,
            body = truncate_text_lossy(&body, 160),
            "provider probe returned non-success status"
        );
    }

    async fn block_lag_poll_loop(self: Arc<Self>) {
        let mut ticker = time::interval(self.predictive_scoring.block_lag_poll_interval());
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let method = self.reliability.predictive_scoring.block_lag_method.clone();

        loop {
            ticker.tick().await;
            for provider_index in 0..self.providers.len() {
                self.predictive_scoring
                    .record_shadow_observation(provider_index);
                let _ = self
                    .call_provider(
                        provider_index,
                        self.block_lag_probe_payload.clone(),
                        Some(method.as_str()),
                    )
                    .await;
            }
        }
    }

    fn spawn_shadow_mirror(&self, body: Bytes, rpc_method: Option<&str>) {
        if !self.reliability.shadow_mode.enabled {
            return;
        }

        let candidates = self.shadow_mirror_candidates();
        if candidates.is_empty() {
            return;
        }

        for provider_index in candidates {
            let provider = self.providers[provider_index].clone();
            let client = self.client.clone();
            let request_body = body.clone();
            let method = rpc_method.map(ToString::to_string);
            let default_timeout = self.reliability.request_timeout();
            let predictive = Arc::clone(&self.predictive_scoring);
            let adaptive = Arc::clone(&self.adaptive_hedging);
            let block_lag_method = self.reliability.predictive_scoring.block_lag_method.clone();

            tokio::spawn(async move {
                predictive.record_shadow_observation(provider_index);
                if let Ok(success) = call_provider_once(
                    client,
                    provider,
                    default_timeout,
                    provider_index,
                    request_body,
                    method.as_deref(),
                )
                .await
                {
                    adaptive.record_provider_latency(success.provider_index, success.latency);
                    if let Some(headroom) = success.rate_limit_headroom {
                        predictive.record_rate_limit_headroom(success.provider_index, headroom);
                    }
                    if method
                        .as_deref()
                        .map(|name| name.eq_ignore_ascii_case(block_lag_method.as_str()))
                        .unwrap_or(false)
                        && let Some(block_height) = parse_block_number_from_rpc_body(&success.body)
                    {
                        predictive.record_block_height(success.provider_index, block_height);
                    }
                }
            });
        }
    }

    fn shadow_mirror_candidates(&self) -> Vec<usize> {
        let mut candidates = Vec::new();
        for (index, provider) in self.providers.iter().enumerate() {
            if !self.predictive_scoring.should_mirror_provider(index) {
                continue;
            }
            let snapshot = provider.health.snapshot(provider.config.weight);
            let score = self
                .predictive_scoring
                .score_provider(index, &snapshot, provider.config.weight)
                .composite_score;
            candidates.push((index, score));
        }

        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        candidates
            .into_iter()
            .take(self.predictive_scoring.mirror_max_providers())
            .map(|entry| entry.0)
            .collect()
    }

    async fn dispatch_with_consensus_validation(
        &self,
        body: Bytes,
        rpc_method: Option<&str>,
    ) -> Result<GatewayResponse, DispatchError> {
        let ranked_indexes = self.ranked_provider_indexes();
        if ranked_indexes.is_empty() {
            return Err(DispatchError {
                failures: vec![ProviderFailure {
                    provider: "gateway".to_string(),
                    error: "no providers configured".to_string(),
                }],
            });
        }

        if ranked_indexes.len() < 2 {
            return self.dispatch_with_reliability(body, rpc_method).await;
        }

        let sample_size = self
            .reliability
            .consensus_validation
            .normalized_sample_size()
            .min(ranked_indexes.len());
        let candidates = &ranked_indexes[..sample_size];
        let mut joins = JoinSet::new();
        let default_timeout = self.reliability.request_timeout();

        for provider_index in candidates {
            let client = self.client.clone();
            let provider = self.providers[*provider_index].clone();
            let request_body = body.clone();
            let request_method = rpc_method.map(ToString::to_string);
            let index = *provider_index;
            joins.spawn(async move {
                call_provider_once(
                    client,
                    provider,
                    default_timeout,
                    index,
                    request_body,
                    request_method.as_deref(),
                )
                .await
            });
        }

        let mut successes = Vec::new();
        let mut failures = Vec::new();
        while let Some(result) = joins.join_next().await {
            match result {
                Ok(Ok(success)) => {
                    self.record_success_telemetry(&success, rpc_method);
                    successes.push(success);
                }
                Ok(Err(error)) => failures.push(error),
                Err(join_error) => failures.push(ProviderFailure {
                    provider: "gateway".to_string(),
                    error: format!("consensus task join error: {join_error}"),
                }),
            }
        }

        if successes.len() < 2 {
            if self.reliability.consensus_validation.fail_open
                && let Some(success) = successes
                    .iter()
                    .min_by_key(|candidate| {
                        candidates
                            .iter()
                            .position(|index| *index == candidate.provider_index)
                            .unwrap_or(usize::MAX)
                    })
                    .cloned()
            {
                let mut fallback = build_gateway_response(success, sample_size, false, 1);
                fallback.consensus_checked = true;
                fallback.consensus_validated = false;
                fallback.consensus_agreement = Some(format!("{}/{}", successes.len(), sample_size));
                return Ok(fallback);
            }

            failures.push(ProviderFailure {
                provider: "consensus".to_string(),
                error: format!(
                    "consensus validation requires at least 2 successful providers; got {}",
                    successes.len()
                ),
            });
            return Err(DispatchError { failures });
        }

        let consensus_candidates = successes
            .iter()
            .map(|success| ConsensusCandidate {
                provider: success.provider.clone(),
                body: success.body.clone(),
                provider_index: success.provider_index,
                latency: success.latency,
            })
            .collect::<Vec<_>>();
        let Some(decision) = decide_consensus(rpc_method, consensus_candidates) else {
            failures.push(ProviderFailure {
                provider: "consensus".to_string(),
                error: "failed to parse JSON-RPC payloads for consensus voting".to_string(),
            });
            return Err(DispatchError { failures });
        };

        if !decision.majority {
            if self.reliability.consensus_validation.fail_open {
                let mut fallback = build_gateway_response(
                    CallSuccess {
                        provider: decision.winner.provider,
                        body: decision.winner.body,
                        provider_index: decision.winner.provider_index,
                        latency: decision.winner.latency,
                        rate_limit_headroom: None,
                    },
                    sample_size,
                    false,
                    1,
                );
                fallback.consensus_checked = true;
                fallback.consensus_validated = false;
                fallback.consensus_agreement =
                    Some(format!("{}/{}", decision.agreement, decision.participants));
                return Ok(fallback);
            }

            failures.push(ProviderFailure {
                provider: "consensus".to_string(),
                error: format!(
                    "no majority agreement for {} ({}/{})",
                    rpc_method.unwrap_or("unknown"),
                    decision.agreement,
                    decision.participants
                ),
            });
            return Err(DispatchError { failures });
        }

        let mut response = build_gateway_response(
            CallSuccess {
                provider: decision.winner.provider,
                body: decision.winner.body,
                provider_index: decision.winner.provider_index,
                latency: decision.winner.latency,
                rate_limit_headroom: None,
            },
            sample_size,
            false,
            1,
        );
        response.consensus_checked = true;
        response.consensus_validated = true;
        response.consensus_agreement =
            Some(format!("{}/{}", decision.agreement, decision.participants));
        Ok(response)
    }

    async fn dispatch_with_reliability(
        &self,
        body: Bytes,
        rpc_method: Option<&str>,
    ) -> Result<GatewayResponse, DispatchError> {
        let ranked_indexes = self.ranked_provider_indexes();
        if ranked_indexes.is_empty() {
            return Err(DispatchError {
                failures: vec![ProviderFailure {
                    provider: "gateway".to_string(),
                    error: "no providers configured".to_string(),
                }],
            });
        }

        let attempts_cap = self
            .reliability
            .normalized_max_failover_attempts()
            .min(ranked_indexes.len());
        let candidates = &ranked_indexes[..attempts_cap];
        let mut failures = Vec::new();

        let mut first_sequential_index = 0;
        let mut hedged = false;
        let mut hedge_width = 1;

        if self.reliability.adaptive_hedging.enabled && candidates.len() >= 2 {
            let HedgeDecision {
                hedge_width: adaptive_width,
                ..
            } = self.adaptive_hedging.decide_hedge_width(candidates);
            let adaptive_width = adaptive_width.clamp(1, candidates.len());

            if adaptive_width > 1 {
                match self
                    .attempt_parallel_hedged(
                        body.clone(),
                        &candidates[..adaptive_width],
                        rpc_method,
                    )
                    .await
                {
                    Ok(outcome) => {
                        self.adaptive_hedging
                            .record_hedge_outcome(candidates[0], outcome.success.provider_index);
                        return Ok(build_gateway_response(
                            outcome.success,
                            outcome.attempts,
                            outcome.hedged,
                            outcome.hedge_width,
                        ));
                    }
                    Err(hedge_failures) => {
                        failures.extend(hedge_failures);
                        first_sequential_index = adaptive_width;
                        hedged = true;
                        hedge_width = adaptive_width;
                    }
                }
            }
        } else if self.reliability.hedge_delay_ms > 0 && candidates.len() >= 2 {
            match self
                .attempt_legacy_hedged(body.clone(), candidates[0], candidates[1], rpc_method)
                .await
            {
                Ok(outcome) => {
                    self.adaptive_hedging
                        .record_hedge_outcome(candidates[0], outcome.success.provider_index);
                    return Ok(build_gateway_response(
                        outcome.success,
                        outcome.attempts,
                        outcome.hedged,
                        outcome.hedge_width,
                    ));
                }
                Err(hedge_failures) => {
                    failures.extend(hedge_failures);
                    first_sequential_index = 2;
                    hedged = true;
                    hedge_width = 2;
                }
            }
        }

        for candidate in candidates.iter().skip(first_sequential_index) {
            match self
                .call_provider(*candidate, body.clone(), rpc_method)
                .await
            {
                Ok(success) => {
                    return Ok(build_gateway_response(
                        success,
                        failures.len() + 1,
                        hedged,
                        hedge_width,
                    ));
                }
                Err(error) => failures.push(error),
            }
        }

        Err(DispatchError { failures })
    }

    async fn attempt_legacy_hedged(
        &self,
        body: Bytes,
        primary_index: usize,
        secondary_index: usize,
        rpc_method: Option<&str>,
    ) -> Result<HedgedOutcome, Vec<ProviderFailure>> {
        let mut primary = Box::pin(self.call_provider(primary_index, body.clone(), rpc_method));
        let mut failures = Vec::new();

        match time::timeout(self.reliability.hedge_delay(), &mut primary).await {
            Ok(primary_result) => match primary_result {
                Ok(success) => Ok(HedgedOutcome {
                    success,
                    attempts: 1,
                    hedged: false,
                    hedge_width: 1,
                }),
                Err(primary_error) => {
                    failures.push(primary_error);
                    match self.call_provider(secondary_index, body, rpc_method).await {
                        Ok(success) => Ok(HedgedOutcome {
                            success,
                            attempts: 2,
                            hedged: false,
                            hedge_width: 1,
                        }),
                        Err(secondary_error) => {
                            failures.push(secondary_error);
                            Err(failures)
                        }
                    }
                }
            },
            Err(_) => {
                let mut secondary = Box::pin(self.call_provider(secondary_index, body, rpc_method));
                tokio::select! {
                    primary_result = &mut primary => {
                        match primary_result {
                            Ok(success) => Ok(HedgedOutcome {
                                success,
                                attempts: 2,
                                hedged: true,
                                hedge_width: 2,
                            }),
                            Err(primary_error) => {
                                failures.push(primary_error);
                                match secondary.await {
                                    Ok(success) => Ok(HedgedOutcome {
                                        success,
                                        attempts: 2,
                                        hedged: true,
                                        hedge_width: 2,
                                    }),
                                    Err(secondary_error) => {
                                        failures.push(secondary_error);
                                        Err(failures)
                                    }
                                }
                            }
                        }
                    }
                    secondary_result = &mut secondary => {
                        match secondary_result {
                            Ok(success) => Ok(HedgedOutcome {
                                success,
                                attempts: 2,
                                hedged: true,
                                hedge_width: 2,
                            }),
                            Err(secondary_error) => {
                                failures.push(secondary_error);
                                match primary.await {
                                    Ok(success) => Ok(HedgedOutcome {
                                        success,
                                        attempts: 2,
                                        hedged: true,
                                        hedge_width: 2,
                                    }),
                                    Err(primary_error) => {
                                        failures.push(primary_error);
                                        Err(failures)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    async fn attempt_parallel_hedged(
        &self,
        body: Bytes,
        provider_indexes: &[usize],
        rpc_method: Option<&str>,
    ) -> Result<HedgedOutcome, Vec<ProviderFailure>> {
        if provider_indexes.is_empty() {
            return Err(vec![ProviderFailure {
                provider: "gateway".to_string(),
                error: "adaptive hedge received no providers".to_string(),
            }]);
        }

        let default_timeout = self.reliability.request_timeout();
        let mut joins = JoinSet::new();
        for provider_index in provider_indexes {
            let client = self.client.clone();
            let provider = self.providers[*provider_index].clone();
            let request_body = body.clone();
            let index = *provider_index;
            let request_method = rpc_method.map(ToString::to_string);
            joins.spawn(async move {
                call_provider_once(
                    client,
                    provider,
                    default_timeout,
                    index,
                    request_body,
                    request_method.as_deref(),
                )
                .await
            });
        }

        let mut failures = Vec::new();
        while let Some(result) = joins.join_next().await {
            match result {
                Ok(Ok(success)) => {
                    self.adaptive_hedging
                        .record_provider_latency(success.provider_index, success.latency);
                    joins.abort_all();
                    while joins.join_next().await.is_some() {}
                    return Ok(HedgedOutcome {
                        success,
                        attempts: provider_indexes.len(),
                        hedged: true,
                        hedge_width: provider_indexes.len(),
                    });
                }
                Ok(Err(failure)) => failures.push(failure),
                Err(join_error) => failures.push(ProviderFailure {
                    provider: "gateway".to_string(),
                    error: format!("hedged task join error: {join_error}"),
                }),
            }
        }

        Err(failures)
    }

    fn ranked_provider_indexes(&self) -> Vec<usize> {
        let mut ranked = self
            .providers
            .iter()
            .enumerate()
            .map(|(index, provider)| {
                let snapshot = provider.health.snapshot(provider.config.weight);
                let scored = self.predictive_scoring.score_provider(
                    index,
                    &snapshot,
                    provider.config.weight,
                );
                (
                    index,
                    scored.is_live,
                    snapshot.healthy,
                    scored.composite_score,
                )
            })
            .collect::<Vec<_>>();

        let has_live = ranked.iter().any(|entry| entry.1);
        if has_live {
            ranked.retain(|entry| entry.1);
        }
        let ranked_indexes = ranked.iter().map(|entry| entry.0).collect::<Vec<_>>();
        let min_known_cost = self.cost_routing.min_cost_for_indexes(&ranked_indexes);
        for entry in &mut ranked {
            entry.3 = self
                .cost_routing
                .adjust_score(entry.0, entry.3, min_known_cost);
        }

        ranked.sort_by(|a, b| match b.2.cmp(&a.2) {
            Ordering::Equal => b.3.partial_cmp(&a.3).unwrap_or(Ordering::Equal),
            ordering => ordering,
        });

        ranked.into_iter().map(|entry| entry.0).collect()
    }

    async fn call_provider(
        &self,
        provider_index: usize,
        body: Bytes,
        rpc_method: Option<&str>,
    ) -> Result<CallSuccess, ProviderFailure> {
        let provider = self.providers[provider_index].clone();
        let result = call_provider_once(
            self.client.clone(),
            provider,
            self.reliability.request_timeout(),
            provider_index,
            body,
            rpc_method,
        )
        .await;

        if let Ok(success) = &result {
            self.record_success_telemetry(success, rpc_method);
        }

        result
    }

    fn record_success_telemetry(&self, success: &CallSuccess, rpc_method: Option<&str>) {
        self.adaptive_hedging
            .record_provider_latency(success.provider_index, success.latency);
        if let Some(headroom) = success.rate_limit_headroom {
            self.predictive_scoring
                .record_rate_limit_headroom(success.provider_index, headroom);
        }
        if rpc_method
            .map(|method| {
                method.eq_ignore_ascii_case(
                    self.reliability
                        .predictive_scoring
                        .block_lag_method
                        .as_str(),
                )
            })
            .unwrap_or(false)
            && let Some(block_height) = parse_block_number_from_rpc_body(&success.body)
        {
            self.predictive_scoring
                .record_block_height(success.provider_index, block_height);
        }
    }
}

#[derive(Debug, Clone)]
pub struct GatewayResponse {
    pub body: Bytes,
    pub provider: String,
    pub attempts: usize,
    pub hedged: bool,
    pub hedge_width: usize,
    pub cache_hit: bool,
    pub cached_at_unix_ms: Option<u128>,
    pub coalesced: bool,
    pub consensus_critical: bool,
    pub consensus_checked: bool,
    pub consensus_validated: bool,
    pub consensus_agreement: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DispatchError {
    pub failures: Vec<ProviderFailure>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderFailure {
    pub provider: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderHealthView {
    pub name: String,
    pub url: String,
    #[serde(flatten)]
    pub snapshot: ProviderHealthSnapshot,
    pub predictive: ProviderPredictiveView,
}

#[derive(Clone)]
struct ProviderRuntime {
    config: ProviderConfig,
    health: Arc<ProviderHealthTracker>,
}

#[derive(Clone)]
struct CallSuccess {
    provider: String,
    body: Bytes,
    provider_index: usize,
    latency: Duration,
    rate_limit_headroom: Option<f64>,
}

struct HedgedOutcome {
    success: CallSuccess,
    attempts: usize,
    hedged: bool,
    hedge_width: usize,
}

fn build_gateway_response(
    success: CallSuccess,
    attempts: usize,
    hedged: bool,
    hedge_width: usize,
) -> GatewayResponse {
    GatewayResponse {
        body: success.body,
        provider: success.provider,
        attempts,
        hedged,
        hedge_width,
        cache_hit: false,
        cached_at_unix_ms: None,
        coalesced: false,
        consensus_critical: false,
        consensus_checked: false,
        consensus_validated: false,
        consensus_agreement: None,
    }
}

async fn call_provider_once(
    client: Client,
    provider: ProviderRuntime,
    default_timeout: Duration,
    provider_index: usize,
    body: Bytes,
    _rpc_method: Option<&str>,
) -> Result<CallSuccess, ProviderFailure> {
    let timeout = provider.config.timeout_or_default(default_timeout);
    let mut request = client
        .post(provider.config.url.as_str())
        .header(header::CONTENT_TYPE, "application/json")
        .body(body);

    for (key, value) in &provider.config.headers {
        request = request.header(key, value);
    }

    let started = Instant::now();
    let response = match time::timeout(timeout, request.send()).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            provider.health.record_failure();
            return Err(ProviderFailure {
                provider: provider.config.name.clone(),
                error: format!("request error: {error}"),
            });
        }
        Err(_) => {
            provider.health.record_failure();
            return Err(ProviderFailure {
                provider: provider.config.name.clone(),
                error: format!("request timed out after {} ms", timeout.as_millis()),
            });
        }
    };

    let status = response.status();
    let response_headers = response.headers().clone();
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => {
            provider.health.record_failure();
            return Err(ProviderFailure {
                provider: provider.config.name.clone(),
                error: format!("failed to read response body: {error}"),
            });
        }
    };

    if status.is_success() {
        let latency = started.elapsed();
        provider.health.record_success(latency);
        return Ok(CallSuccess {
            provider: provider.config.name.clone(),
            body: bytes,
            provider_index,
            latency,
            rate_limit_headroom: predictive::parse_rate_limit_headroom(&response_headers),
        });
    }

    provider.health.record_failure();
    Err(ProviderFailure {
        provider: provider.config.name.clone(),
        error: format!(
            "upstream status {}: {}",
            status.as_u16(),
            truncate_text_lossy(&bytes, 200)
        ),
    })
}

fn truncate_text_lossy(bytes: &[u8], max_chars: usize) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.chars().count() <= max_chars {
        return text.to_string();
    }

    let mut output = text.chars().take(max_chars).collect::<String>();
    output.push_str("...");
    output
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering as AtomicOrdering},
        },
        time::Duration,
    };

    use axum::{
        Router,
        body::Bytes,
        extract::State,
        http::{StatusCode, header},
        response::IntoResponse,
        routing::post,
    };
    use serde_json::{Value, json};
    use tokio::{net::TcpListener, task::JoinHandle, time::sleep};

    use super::Gateway;
    use crate::settings::{
        AdaptiveHedgingConfig, CacheConfig, ConsensusValidationConfig, CostRoutingConfig,
        MethodPolicyConfig, MethodPolicyOverride, PredictiveScoringConfig, ProbeConfig,
        ProviderConfig, ReliabilityConfig, ServerConfig, Settings, ShadowModeConfig,
    };

    struct MockProvider {
        url: String,
        hits: Arc<AtomicUsize>,
        handle: JoinHandle<()>,
    }

    #[derive(Clone)]
    struct MockState {
        hits: Arc<AtomicUsize>,
        behavior: MockBehavior,
    }

    #[derive(Clone)]
    struct MockBehavior {
        status: StatusCode,
        response_mode: ResponseMode,
        delay: Duration,
    }

    #[derive(Clone)]
    enum ResponseMode {
        Static(String),
        CounterNumber,
    }

    impl MockProvider {
        async fn spawn(behavior: MockBehavior) -> Self {
            let hits = Arc::new(AtomicUsize::new(0));
            let state = MockState {
                hits: Arc::clone(&hits),
                behavior,
            };

            let app = Router::new()
                .route("/", post(mock_handler))
                .with_state(state);
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener should bind");
            let addr = listener
                .local_addr()
                .expect("listener should have local address");
            let url = format!("http://{addr}/");
            let handle = tokio::spawn(async move {
                axum::serve(listener, app)
                    .await
                    .expect("mock server failed");
            });

            Self { url, hits, handle }
        }

        fn stop(self) {
            self.handle.abort();
        }
    }

    async fn mock_handler(State(state): State<MockState>, _body: Bytes) -> impl IntoResponse {
        if state.behavior.delay > Duration::ZERO {
            sleep(state.behavior.delay).await;
        }

        let call_number = state.hits.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        let response_body = match &state.behavior.response_mode {
            ResponseMode::Static(body) => body.clone(),
            ResponseMode::CounterNumber => {
                format!(r#"{{"jsonrpc":"2.0","id":1,"result":{}}}"#, call_number)
            }
        };

        (
            state.behavior.status,
            [(header::CONTENT_TYPE, "application/json")],
            response_body,
        )
    }

    fn provider_config(name: &str, url: String, weight: u32) -> ProviderConfig {
        ProviderConfig {
            name: name.to_string(),
            url,
            weight,
            timeout_ms: None,
            headers: HashMap::new(),
            shadow_mode: false,
            shadow_warmup_secs: None,
            shadow_min_observations: None,
            cost_per_million_requests: None,
        }
    }

    fn provider_config_with_cost(
        name: &str,
        url: String,
        weight: u32,
        cost_per_million_requests: f64,
    ) -> ProviderConfig {
        let mut provider = provider_config(name, url, weight);
        provider.cost_per_million_requests = Some(cost_per_million_requests);
        provider
    }

    fn reliability_for_test() -> ReliabilityConfig {
        ReliabilityConfig {
            request_timeout_ms: 1_500,
            hedge_delay_ms: 0,
            max_failover_attempts: 3,
            unhealthy_after_failures: 3,
            recovery_after_secs: 1,
            probe: ProbeConfig {
                enabled: false,
                ..ProbeConfig::default()
            },
            adaptive_hedging: AdaptiveHedgingConfig {
                enabled: false,
                ..AdaptiveHedgingConfig::default()
            },
            predictive_scoring: PredictiveScoringConfig {
                enabled: false,
                ..PredictiveScoringConfig::default()
            },
            shadow_mode: ShadowModeConfig {
                enabled: false,
                ..ShadowModeConfig::default()
            },
            coalescing: crate::settings::CoalescingConfig { enabled: true },
            consensus_validation: crate::settings::ConsensusValidationConfig {
                enabled: false,
                sample_size: 3,
                fail_open: false,
            },
            cost_routing: crate::settings::CostRoutingConfig {
                enabled: false,
                strategy: "balanced".to_string(),
            },
        }
    }

    fn disabled_cache() -> CacheConfig {
        CacheConfig {
            enabled: false,
            ttl_secs: 1,
            max_capacity: 100,
            cacheable_methods: Vec::new(),
            method_ttl_secs: HashMap::new(),
        }
    }

    fn settings_for_test(
        providers: Vec<ProviderConfig>,
        reliability: ReliabilityConfig,
        cache: CacheConfig,
    ) -> Settings {
        Settings {
            server: ServerConfig::default(),
            reliability,
            cache,
            method_policy: MethodPolicyConfig::default(),
            providers,
        }
    }

    #[tokio::test]
    async fn failover_uses_secondary_provider_when_primary_fails() {
        let primary = MockProvider::spawn(MockBehavior {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000}}"#.to_string(),
            ),
            delay: Duration::ZERO,
        })
        .await;
        let secondary = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":2}"#.to_string(),
            ),
            delay: Duration::ZERO,
        })
        .await;

        let settings = settings_for_test(
            vec![
                provider_config("primary", primary.url.clone(), 200),
                provider_config("secondary", secondary.url.clone(), 100),
            ],
            reliability_for_test(),
            disabled_cache(),
        );

        let gateway = Gateway::from_settings(settings).expect("gateway should build");
        let body =
            Bytes::from_static(br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#);

        let response = gateway
            .execute_rpc(body)
            .await
            .expect("request should eventually succeed");

        assert_eq!(response.provider, "secondary");
        assert_eq!(response.attempts, 2);
        assert!(!response.hedged);
        assert_eq!(response.hedge_width, 1);
        assert!(!response.cache_hit);
        assert!(String::from_utf8_lossy(&response.body).contains("\"result\":2"));
        assert_eq!(primary.hits.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(secondary.hits.load(AtomicOrdering::SeqCst), 1);

        primary.stop();
        secondary.stop();
    }

    #[tokio::test]
    async fn hedged_requests_prefer_faster_secondary_provider() {
        let primary = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":1}"#.to_string(),
            ),
            delay: Duration::from_millis(300),
        })
        .await;
        let secondary = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":2}"#.to_string(),
            ),
            delay: Duration::from_millis(10),
        })
        .await;

        let mut reliability = reliability_for_test();
        reliability.hedge_delay_ms = 30;
        reliability.max_failover_attempts = 2;

        let settings = settings_for_test(
            vec![
                provider_config("primary", primary.url.clone(), 300),
                provider_config("secondary", secondary.url.clone(), 100),
            ],
            reliability,
            disabled_cache(),
        );

        let gateway = Gateway::from_settings(settings).expect("gateway should build");
        let body =
            Bytes::from_static(br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#);

        let response = gateway
            .execute_rpc(body)
            .await
            .expect("hedged request should succeed");

        assert_eq!(response.provider, "secondary");
        assert_eq!(response.attempts, 2);
        assert!(response.hedged);
        assert_eq!(response.hedge_width, 2);
        assert!(secondary.hits.load(AtomicOrdering::SeqCst) >= 1);

        primary.stop();
        secondary.stop();
    }

    #[tokio::test]
    async fn adaptive_hedging_scales_to_three_parallel_providers() {
        let provider_a = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":10}"#.to_string(),
            ),
            delay: Duration::from_millis(220),
        })
        .await;
        let provider_b = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":11}"#.to_string(),
            ),
            delay: Duration::from_millis(100),
        })
        .await;
        let provider_c = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":12}"#.to_string(),
            ),
            delay: Duration::from_millis(8),
        })
        .await;

        let mut reliability = reliability_for_test();
        reliability.adaptive_hedging = AdaptiveHedgingConfig {
            enabled: true,
            min_hedge_width: 1,
            max_hedge_width: 3,
            rps_window_secs: 30,
            medium_rps: 100.0,
            high_rps: 200.0,
            medium_latency_spread_ms: 40,
            high_latency_spread_ms: 90,
            max_latency_samples: 100,
        };

        let settings = settings_for_test(
            vec![
                provider_config("provider-a", provider_a.url.clone(), 300),
                provider_config("provider-b", provider_b.url.clone(), 200),
                provider_config("provider-c", provider_c.url.clone(), 100),
            ],
            reliability,
            disabled_cache(),
        );

        let gateway = Gateway::from_settings(settings).expect("gateway should build");

        gateway
            .adaptive_hedging
            .record_provider_latency(0, Duration::from_millis(210));
        gateway
            .adaptive_hedging
            .record_provider_latency(1, Duration::from_millis(130));
        gateway
            .adaptive_hedging
            .record_provider_latency(2, Duration::from_millis(20));

        let body =
            Bytes::from_static(br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#);
        let response = gateway
            .execute_rpc(body)
            .await
            .expect("adaptive hedged request should succeed");

        assert!(response.hedged);
        assert_eq!(response.hedge_width, 3);
        assert_eq!(response.attempts, 3);
        assert_eq!(response.provider, "provider-c");

        let stats = gateway.hedging_stats();
        assert_eq!(stats.total_hedged_requests, 1);
        assert_eq!(stats.hedge_win_count, 1);
        assert!(stats.observed_latency_spread_ms >= 150.0);

        provider_a.stop();
        provider_b.stop();
        provider_c.stop();
    }

    #[tokio::test]
    async fn cache_hits_and_expires_with_method_ttl_override() {
        let provider = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::CounterNumber,
            delay: Duration::ZERO,
        })
        .await;

        let mut method_ttl_secs = HashMap::new();
        method_ttl_secs.insert("getSlot".to_string(), 1);
        let cache = CacheConfig {
            enabled: true,
            ttl_secs: 10,
            max_capacity: 100,
            cacheable_methods: vec![],
            method_ttl_secs,
        };

        let settings = settings_for_test(
            vec![provider_config("provider-a", provider.url.clone(), 100)],
            reliability_for_test(),
            cache,
        );

        let gateway = Gateway::from_settings(settings).expect("gateway should build");
        let body =
            Bytes::from_static(br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#);

        let first = gateway
            .execute_rpc(body.clone())
            .await
            .expect("first request should succeed");
        let second = gateway
            .execute_rpc(body.clone())
            .await
            .expect("second request should succeed");

        assert!(!first.cache_hit);
        assert!(second.cache_hit);
        assert_eq!(first.hedge_width, 1);
        assert_eq!(second.hedge_width, 0);
        assert_eq!(provider.hits.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(
            String::from_utf8_lossy(&first.body),
            String::from_utf8_lossy(&second.body)
        );

        sleep(Duration::from_millis(1_100)).await;

        let third = gateway
            .execute_rpc(body)
            .await
            .expect("third request should succeed");

        assert!(!third.cache_hit);
        assert_eq!(provider.hits.load(AtomicOrdering::SeqCst), 2);

        provider.stop();
    }

    #[tokio::test]
    async fn coalescing_fans_out_identical_in_flight_requests() {
        let provider = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":42}"#.to_string(),
            ),
            delay: Duration::from_millis(40),
        })
        .await;

        let settings = settings_for_test(
            vec![provider_config("provider-a", provider.url.clone(), 100)],
            reliability_for_test(),
            disabled_cache(),
        );
        let gateway = Gateway::from_settings(settings).expect("gateway should build");
        let body =
            Bytes::from_static(br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#);

        let (first, second) =
            tokio::join!(gateway.execute_rpc(body.clone()), gateway.execute_rpc(body));
        let first = first.expect("first request should succeed");
        let second = second.expect("second request should succeed");

        assert_eq!(provider.hits.load(AtomicOrdering::SeqCst), 1);
        assert!(first.coalesced || second.coalesced);

        provider.stop();
    }

    #[tokio::test]
    async fn consensus_validation_accepts_majority_for_critical_method() {
        let provider_a = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":90},"value":500}}"#
                    .to_string(),
            ),
            delay: Duration::from_millis(20),
        })
        .await;
        let provider_b = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":91},"value":500}}"#
                    .to_string(),
            ),
            delay: Duration::from_millis(5),
        })
        .await;
        let provider_c = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":91},"value":999}}"#
                    .to_string(),
            ),
            delay: Duration::from_millis(1),
        })
        .await;

        let mut reliability = reliability_for_test();
        reliability.consensus_validation = ConsensusValidationConfig {
            enabled: true,
            sample_size: 3,
            fail_open: false,
        };

        let settings = settings_for_test(
            vec![
                provider_config("provider-a", provider_a.url.clone(), 100),
                provider_config("provider-b", provider_b.url.clone(), 100),
                provider_config("provider-c", provider_c.url.clone(), 100),
            ],
            reliability,
            disabled_cache(),
        );
        let gateway = Gateway::from_settings(settings).expect("gateway should build");
        let body = Bytes::from_static(
            br#"{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["11111111111111111111111111111111"]}"#,
        );

        let response = gateway
            .execute_rpc(body)
            .await
            .expect("consensus validation should succeed");

        assert!(response.consensus_checked);
        assert!(response.consensus_validated);
        assert_eq!(response.consensus_agreement.as_deref(), Some("2/3"));
        assert_eq!(response.provider, "provider-b");

        provider_a.stop();
        provider_b.stop();
        provider_c.stop();
    }

    #[tokio::test]
    async fn consensus_validation_strict_mode_rejects_disagreement() {
        let provider_a = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":90},"value":1}}"#
                    .to_string(),
            ),
            delay: Duration::from_millis(5),
        })
        .await;
        let provider_b = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":91},"value":2}}"#
                    .to_string(),
            ),
            delay: Duration::from_millis(10),
        })
        .await;
        let provider_c = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":91},"value":3}}"#
                    .to_string(),
            ),
            delay: Duration::from_millis(15),
        })
        .await;

        let mut reliability = reliability_for_test();
        reliability.consensus_validation = ConsensusValidationConfig {
            enabled: true,
            sample_size: 3,
            fail_open: false,
        };

        let settings = settings_for_test(
            vec![
                provider_config("provider-a", provider_a.url.clone(), 100),
                provider_config("provider-b", provider_b.url.clone(), 100),
                provider_config("provider-c", provider_c.url.clone(), 100),
            ],
            reliability,
            disabled_cache(),
        );
        let gateway = Gateway::from_settings(settings).expect("gateway should build");
        let body = Bytes::from_static(
            br#"{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["11111111111111111111111111111111"]}"#,
        );

        let error = gateway
            .execute_rpc(body)
            .await
            .expect_err("strict consensus mode should reject disagreement");

        assert!(
            error
                .failures
                .iter()
                .any(|failure| failure.provider == "consensus")
        );

        provider_a.stop();
        provider_b.stop();
        provider_c.stop();
    }

    #[tokio::test]
    async fn cost_routing_prefers_cheapest_provider() {
        let expensive = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":"expensive"}"#.to_string(),
            ),
            delay: Duration::ZERO,
        })
        .await;
        let cheap = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":"cheap"}"#.to_string(),
            ),
            delay: Duration::ZERO,
        })
        .await;

        let mut reliability = reliability_for_test();
        reliability.max_failover_attempts = 1;
        reliability.cost_routing = CostRoutingConfig {
            enabled: true,
            strategy: "cheapest".to_string(),
        };

        let settings = settings_for_test(
            vec![
                provider_config_with_cost("expensive", expensive.url.clone(), 100, 20.0),
                provider_config_with_cost("cheap", cheap.url.clone(), 100, 1.0),
            ],
            reliability,
            disabled_cache(),
        );
        let gateway = Gateway::from_settings(settings).expect("gateway should build");
        let body =
            Bytes::from_static(br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#);

        let response = gateway
            .execute_rpc(body)
            .await
            .expect("request should succeed");

        assert_eq!(response.provider, "cheap");
        assert_eq!(expensive.hits.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(cheap.hits.load(AtomicOrdering::SeqCst), 1);

        expensive.stop();
        cheap.stop();
    }

    #[tokio::test]
    async fn marks_consensus_critical_methods_in_response() {
        let provider = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":42},"value":1000}}"#
                    .to_string(),
            ),
            delay: Duration::ZERO,
        })
        .await;

        let settings = settings_for_test(
            vec![provider_config("provider-a", provider.url.clone(), 100)],
            reliability_for_test(),
            disabled_cache(),
        );
        let gateway = Gateway::from_settings(settings).expect("gateway should build");
        let body = Bytes::from_static(
            br#"{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["11111111111111111111111111111111"]}"#,
        );

        let response = gateway
            .execute_rpc(body)
            .await
            .expect("request should succeed");

        assert!(response.consensus_critical);
        provider.stop();
    }

    #[tokio::test]
    async fn method_policy_override_applies_to_unknown_methods() {
        let provider = MockProvider::spawn(MockBehavior {
            status: StatusCode::OK,
            response_mode: ResponseMode::CounterNumber,
            delay: Duration::ZERO,
        })
        .await;

        let cache = CacheConfig {
            enabled: true,
            ttl_secs: 10,
            max_capacity: 100,
            cacheable_methods: vec![],
            method_ttl_secs: HashMap::new(),
        };

        let mut settings = settings_for_test(
            vec![provider_config("provider-a", provider.url.clone(), 100)],
            reliability_for_test(),
            cache,
        );

        let mut overrides = HashMap::new();
        overrides.insert(
            "customExperimentalMethod".to_string(),
            MethodPolicyOverride {
                cacheable_by_default: Some(true),
                consensus_critical: Some(true),
            },
        );
        settings.method_policy = MethodPolicyConfig { overrides };

        let gateway = Gateway::from_settings(settings).expect("gateway should build");
        let body = Bytes::from_static(
            br#"{"jsonrpc":"2.0","id":1,"method":"customExperimentalMethod","params":[]}"#,
        );

        let first = gateway
            .execute_rpc(body.clone())
            .await
            .expect("first request should succeed");
        let second = gateway
            .execute_rpc(body)
            .await
            .expect("second request should succeed");

        assert!(!first.cache_hit);
        assert!(first.consensus_critical);
        assert!(second.cache_hit);
        assert!(second.consensus_critical);
        assert_eq!(provider.hits.load(AtomicOrdering::SeqCst), 1);

        provider.stop();
    }

    #[tokio::test]
    async fn provider_health_endpoint_data_shows_probe_failures_from_live_traffic() {
        let provider = MockProvider::spawn(MockBehavior {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            response_mode: ResponseMode::Static(
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000}}"#.to_string(),
            ),
            delay: Duration::ZERO,
        })
        .await;

        let settings = settings_for_test(
            vec![provider_config("provider-a", provider.url.clone(), 100)],
            reliability_for_test(),
            disabled_cache(),
        );

        let gateway = Gateway::from_settings(settings).expect("gateway should build");
        let body =
            Bytes::from_static(br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#);
        let _ = gateway.execute_rpc(body).await;

        let health = gateway.provider_health();
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].name, "provider-a");
        assert_eq!(health[0].snapshot.total_failures, 1);
        assert_eq!(health[0].snapshot.total_success, 0);

        provider.stop();
    }

    #[test]
    fn probe_config_is_serialized_into_payload() {
        let settings = Settings {
            server: ServerConfig::default(),
            reliability: ReliabilityConfig {
                request_timeout_ms: 1_500,
                hedge_delay_ms: 0,
                max_failover_attempts: 3,
                unhealthy_after_failures: 3,
                recovery_after_secs: 1,
                probe: ProbeConfig {
                    enabled: true,
                    interval_secs: 8,
                    timeout_ms: 700,
                    method: "getHealth".to_string(),
                    params: json!([]),
                },
                adaptive_hedging: AdaptiveHedgingConfig {
                    enabled: false,
                    ..AdaptiveHedgingConfig::default()
                },
                predictive_scoring: PredictiveScoringConfig {
                    enabled: false,
                    ..PredictiveScoringConfig::default()
                },
                shadow_mode: ShadowModeConfig {
                    enabled: false,
                    ..ShadowModeConfig::default()
                },
                coalescing: crate::settings::CoalescingConfig { enabled: true },
                consensus_validation: crate::settings::ConsensusValidationConfig {
                    enabled: false,
                    sample_size: 3,
                    fail_open: false,
                },
                cost_routing: crate::settings::CostRoutingConfig {
                    enabled: false,
                    strategy: "balanced".to_string(),
                },
            },
            cache: disabled_cache(),
            method_policy: MethodPolicyConfig::default(),
            providers: vec![ProviderConfig {
                name: "provider-a".to_string(),
                url: "http://localhost:8545".to_string(),
                weight: 100,
                timeout_ms: None,
                headers: HashMap::new(),
                shadow_mode: false,
                shadow_warmup_secs: None,
                shadow_min_observations: None,
                cost_per_million_requests: None,
            }],
        };

        let gateway = Gateway::from_settings(settings).expect("gateway should build");
        let payload: Value = serde_json::from_slice(&gateway.probe_payload)
            .expect("probe payload should be valid json");
        assert_eq!(
            payload.get("method").and_then(Value::as_str),
            Some("getHealth")
        );
        assert_eq!(payload.get("params"), Some(&json!([])));
    }
}
