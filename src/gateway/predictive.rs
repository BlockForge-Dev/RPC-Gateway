use std::{
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

use reqwest::header::HeaderMap;
use serde::Serialize;

use crate::{
    health::{ProviderHealthSnapshot, now_unix_ms},
    settings::{PredictiveScoringConfig, ProviderConfig, ShadowModeConfig},
};

pub(super) struct PredictiveScoringController {
    predictive_config: PredictiveScoringConfig,
    shadow_config: ShadowModeConfig,
    state: Mutex<PredictiveScoringState>,
}

impl PredictiveScoringController {
    pub(super) fn new(
        predictive_config: PredictiveScoringConfig,
        shadow_config: ShadowModeConfig,
        providers: &[ProviderConfig],
    ) -> Self {
        let providers = providers
            .iter()
            .map(|provider| ProviderPredictiveState {
                latest_block: None,
                rate_limit_headroom: None,
                shadow: ShadowProviderState::new(provider, &shadow_config),
            })
            .collect::<Vec<_>>();

        Self {
            predictive_config,
            shadow_config,
            state: Mutex::new(PredictiveScoringState { providers }),
        }
    }

    pub(super) fn score_provider(
        &self,
        provider_index: usize,
        snapshot: &ProviderHealthSnapshot,
        static_weight: u32,
    ) -> ProviderScoringResult {
        let mut state = self.lock_state();
        let max_observed_block = state
            .providers
            .iter()
            .filter_map(|provider| provider.latest_block)
            .max();

        let Some(provider) = state.providers.get_mut(provider_index) else {
            return ProviderScoringResult {
                composite_score: f64::NEG_INFINITY,
                is_live: false,
                view: ProviderPredictiveView::default(),
            };
        };
        provider.shadow.promote_if_ready();

        let total = snapshot
            .total_success
            .saturating_add(snapshot.total_failures);
        let success_rate = if total == 0 {
            1.0
        } else {
            snapshot.total_success as f64 / total as f64
        };

        let target_latency_ms = self.predictive_config.normalized_target_latency_ms() as f64;
        let latency_ms = snapshot
            .latency_ewma_ms
            .unwrap_or(target_latency_ms)
            .max(1.0);
        let latency_factor = target_latency_ms / (target_latency_ms + latency_ms);

        let block_lag = match (provider.latest_block, max_observed_block) {
            (Some(provider_block), Some(max_block)) => {
                Some(max_block.saturating_sub(provider_block))
            }
            _ => None,
        };
        let block_factor = block_lag
            .map(|lag| {
                let normalized_lag = (lag.min(self.predictive_config.normalized_max_block_lag())
                    as f64)
                    / self.predictive_config.normalized_max_block_lag() as f64;
                (1.0 - normalized_lag).clamp(0.0, 1.0)
            })
            .unwrap_or(self.predictive_config.normalized_unknown_block_lag_factor());

        let rate_limit_headroom = provider
            .rate_limit_headroom
            .unwrap_or(
                self.predictive_config
                    .normalized_unknown_rate_limit_headroom(),
            )
            .clamp(0.05, 1.0);

        let healthy_factor = if snapshot.healthy { 1.0 } else { 0.0 };
        let weight_factor = (static_weight as f64 / 100.0).clamp(0.1, 10.0);
        let predictive_score =
            latency_factor * success_rate * block_factor * rate_limit_headroom * healthy_factor;
        let composite_score = if self.predictive_config.enabled {
            predictive_score * weight_factor * 10_000.0
        } else {
            snapshot.score
        };

        let shadow_view = provider.shadow.as_view();
        ProviderScoringResult {
            composite_score,
            is_live: shadow_view.live,
            view: ProviderPredictiveView {
                enabled: self.predictive_config.enabled,
                composite_score,
                success_rate,
                latency_factor,
                block_lag,
                block_factor,
                rate_limit_headroom,
                shadow: shadow_view,
            },
        }
    }

    pub(super) fn should_mirror_provider(&self, provider_index: usize) -> bool {
        if !self.shadow_config.enabled {
            return false;
        }

        let mut state = self.lock_state();
        let Some(provider) = state.providers.get_mut(provider_index) else {
            return false;
        };
        provider.shadow.promote_if_ready();
        provider.shadow.enabled && !provider.shadow.promoted
    }

    pub(super) fn record_shadow_observation(&self, provider_index: usize) {
        let mut state = self.lock_state();
        let Some(provider) = state.providers.get_mut(provider_index) else {
            return;
        };
        provider.shadow.record_observation();
    }

    pub(super) fn record_rate_limit_headers(
        &self,
        provider_index: usize,
        headers: &HeaderMap,
    ) -> Option<f64> {
        let Some(headroom) = parse_rate_limit_headroom(headers) else {
            return None;
        };
        self.record_rate_limit_headroom(provider_index, headroom);
        Some(headroom)
    }

    pub(super) fn record_rate_limit_headroom(&self, provider_index: usize, headroom: f64) {
        let mut state = self.lock_state();
        let Some(provider) = state.providers.get_mut(provider_index) else {
            return;
        };
        provider.rate_limit_headroom = Some(headroom.clamp(0.0, 1.0));
    }

    pub(super) fn record_block_height(&self, provider_index: usize, block_height: u64) {
        let mut state = self.lock_state();
        let Some(provider) = state.providers.get_mut(provider_index) else {
            return;
        };
        provider.latest_block = Some(block_height);
    }

    pub(super) fn mirror_max_providers(&self) -> usize {
        self.shadow_config.normalized_mirror_max_providers()
    }

    pub(super) fn predictive_enabled(&self) -> bool {
        self.predictive_config.enabled
    }

    pub(super) fn block_lag_poll_interval(&self) -> Duration {
        self.predictive_config.block_lag_poll_interval()
    }

    fn lock_state(&self) -> MutexGuard<'_, PredictiveScoringState> {
        self.state.lock().unwrap_or_else(|err| err.into_inner())
    }
}

struct PredictiveScoringState {
    providers: Vec<ProviderPredictiveState>,
}

struct ProviderPredictiveState {
    latest_block: Option<u64>,
    rate_limit_headroom: Option<f64>,
    shadow: ShadowProviderState,
}

#[derive(Debug, Clone)]
struct ShadowProviderState {
    enabled: bool,
    promoted: bool,
    shadow_since: Option<Instant>,
    shadow_promoted_at_unix_ms: Option<u128>,
    warmup: Duration,
    min_observations: u64,
    observations: u64,
}

impl ShadowProviderState {
    fn new(provider: &ProviderConfig, config: &ShadowModeConfig) -> Self {
        let enabled = config.enabled && provider.shadow_mode;
        if !enabled {
            return Self {
                enabled: false,
                promoted: true,
                shadow_since: None,
                shadow_promoted_at_unix_ms: Some(now_unix_ms()),
                warmup: Duration::from_secs(0),
                min_observations: 0,
                observations: 0,
            };
        }

        Self {
            enabled: true,
            promoted: false,
            shadow_since: Some(Instant::now()),
            shadow_promoted_at_unix_ms: None,
            warmup: Duration::from_secs(
                provider.shadow_warmup_or_default(config.normalized_default_warmup_secs()),
            ),
            min_observations: provider
                .shadow_min_observations_or_default(config.normalized_default_min_observations()),
            observations: 0,
        }
    }

    fn promote_if_ready(&mut self) {
        if !self.enabled || self.promoted {
            return;
        }

        let Some(shadow_since) = self.shadow_since else {
            return;
        };
        let warmup_done = shadow_since.elapsed() >= self.warmup;
        let enough_observations = self.observations >= self.min_observations;
        if warmup_done && enough_observations {
            self.promoted = true;
            self.shadow_promoted_at_unix_ms = Some(now_unix_ms());
        }
    }

    fn record_observation(&mut self) {
        if !self.enabled {
            return;
        }
        self.observations = self.observations.saturating_add(1);
        self.promote_if_ready();
    }

    fn as_view(&self) -> ShadowStatusView {
        let warmup_remaining_secs = self
            .shadow_since
            .map(|since| self.warmup.saturating_sub(since.elapsed()).as_secs().max(0));

        ShadowStatusView {
            enabled: self.enabled,
            live: self.promoted,
            observations: self.observations,
            min_observations: self.min_observations,
            warmup_remaining_secs: if self.enabled && !self.promoted {
                warmup_remaining_secs
            } else {
                None
            },
            promoted_at_unix_ms: self.shadow_promoted_at_unix_ms,
        }
    }
}

pub(super) struct ProviderScoringResult {
    pub(super) composite_score: f64,
    pub(super) is_live: bool,
    pub(super) view: ProviderPredictiveView,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ProviderPredictiveView {
    pub enabled: bool,
    pub composite_score: f64,
    pub success_rate: f64,
    pub latency_factor: f64,
    pub block_lag: Option<u64>,
    pub block_factor: f64,
    pub rate_limit_headroom: f64,
    pub shadow: ShadowStatusView,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ShadowStatusView {
    pub enabled: bool,
    pub live: bool,
    pub observations: u64,
    pub min_observations: u64,
    pub warmup_remaining_secs: Option<u64>,
    pub promoted_at_unix_ms: Option<u128>,
}

pub(super) fn parse_rate_limit_headroom(headers: &HeaderMap) -> Option<f64> {
    let remaining = parse_header_u64(headers, &["x-ratelimit-remaining", "ratelimit-remaining"])?;
    let limit = parse_header_u64(headers, &["x-ratelimit-limit", "ratelimit-limit"])?;
    if limit == 0 {
        return None;
    }
    Some((remaining as f64 / limit as f64).clamp(0.0, 1.0))
}

fn parse_header_u64(headers: &HeaderMap, names: &[&str]) -> Option<u64> {
    for name in names {
        if let Some(value) = headers.get(*name)
            && let Ok(raw) = value.to_str()
            && let Ok(parsed) = raw.trim().parse::<u64>()
        {
            return Some(parsed);
        }
    }
    None
}

pub(super) fn parse_block_number_from_rpc_body(body: &[u8]) -> Option<u64> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let result = value.get("result")?;

    if let Some(number) = result.as_u64() {
        return Some(number);
    }

    let string_value = result.as_str()?;
    let trimmed = string_value.trim();
    if trimmed.starts_with("0x") || trimmed.starts_with("0X") {
        let hex = trimmed.trim_start_matches("0x").trim_start_matches("0X");
        return u64::from_str_radix(hex, 16).ok();
    }

    trimmed.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, thread, time::Duration};

    use reqwest::header::{HeaderMap, HeaderValue};

    use super::{PredictiveScoringController, parse_block_number_from_rpc_body};
    use crate::{
        health::ProviderHealthSnapshot,
        settings::{PredictiveScoringConfig, ProviderConfig, ShadowModeConfig},
    };

    fn provider(name: &str, shadow_mode: bool) -> ProviderConfig {
        ProviderConfig {
            name: name.to_string(),
            url: "http://localhost:8545".to_string(),
            weight: 100,
            timeout_ms: None,
            headers: HashMap::new(),
            shadow_mode,
            shadow_warmup_secs: Some(1),
            shadow_min_observations: Some(3),
            cost_per_million_requests: None,
        }
    }

    fn snapshot() -> ProviderHealthSnapshot {
        ProviderHealthSnapshot {
            healthy: true,
            consecutive_failures: 0,
            total_success: 10,
            total_failures: 0,
            last_success_unix_ms: None,
            last_failure_unix_ms: None,
            latency_ewma_ms: Some(50.0),
            circuit_open_remaining_ms: None,
            score: 0.0,
        }
    }

    #[test]
    fn shadow_provider_promotes_after_warmup_and_observations() {
        let controller = PredictiveScoringController::new(
            PredictiveScoringConfig::default(),
            ShadowModeConfig {
                enabled: true,
                default_warmup_secs: 1,
                default_min_observations: 3,
                mirror_max_providers: 1,
            },
            &[provider("shadow-a", true)],
        );

        let initial = controller.score_provider(0, &snapshot(), 100);
        assert!(!initial.is_live);
        controller.record_shadow_observation(0);
        controller.record_shadow_observation(0);
        controller.record_shadow_observation(0);
        thread::sleep(Duration::from_millis(1100));
        let after = controller.score_provider(0, &snapshot(), 100);
        assert!(after.is_live);
        assert_eq!(after.view.shadow.observations, 3);
    }

    #[test]
    fn block_lag_and_rate_limit_affect_score() {
        let controller = PredictiveScoringController::new(
            PredictiveScoringConfig::default(),
            ShadowModeConfig::default(),
            &[provider("a", false), provider("b", false)],
        );
        let base = controller
            .score_provider(0, &snapshot(), 100)
            .composite_score;

        controller.record_block_height(0, 100);
        controller.record_block_height(1, 120);
        controller.record_rate_limit_headroom(0, 0.2);

        let degraded = controller
            .score_provider(0, &snapshot(), 100)
            .composite_score;
        assert!(degraded < base);
    }

    #[test]
    fn parses_rate_limit_headers() {
        let controller = PredictiveScoringController::new(
            PredictiveScoringConfig::default(),
            ShadowModeConfig::default(),
            &[provider("a", false)],
        );
        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-remaining", HeaderValue::from_static("30"));
        headers.insert("x-ratelimit-limit", HeaderValue::from_static("60"));
        let headroom = controller.record_rate_limit_headers(0, &headers);
        assert_eq!(headroom, Some(0.5));
    }

    #[test]
    fn parses_block_number_from_json_rpc_body() {
        let body = br#"{"jsonrpc":"2.0","id":1,"result":"0x2a"}"#;
        assert_eq!(parse_block_number_from_rpc_body(body), Some(42));
    }

    #[test]
    fn parses_solana_slot_number_from_json_rpc_body() {
        let body_numeric = br#"{"jsonrpc":"2.0","id":1,"result":265443576}"#;
        assert_eq!(
            parse_block_number_from_rpc_body(body_numeric),
            Some(265443576)
        );

        let body_string = br#"{"jsonrpc":"2.0","id":1,"result":"265443577"}"#;
        assert_eq!(
            parse_block_number_from_rpc_body(body_string),
            Some(265443577)
        );
    }
}
