use std::{collections::HashMap, fs, path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub reliability: ReliabilityConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub method_policy: MethodPolicyConfig,
    pub providers: Vec<ProviderConfig>,
}

impl Settings {
    pub fn load() -> Result<Self> {
        let path = std::env::var("RPC_GATEWAY_CONFIG")
            .unwrap_or_else(|_| "config/gateway.toml".to_string());
        Self::from_path(path)
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let raw = fs::read_to_string(path_ref)
            .with_context(|| format!("failed to read config file: {}", path_ref.display()))?;

        let settings: Self = toml::from_str(&raw)
            .with_context(|| format!("failed to parse config file: {}", path_ref.display()))?;

        if settings.providers.is_empty() {
            bail!("at least one provider must be configured");
        }

        Ok(settings)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
        }
    }
}

fn default_bind_addr() -> String {
    "0.0.0.0:8080".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReliabilityConfig {
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_hedge_delay_ms")]
    pub hedge_delay_ms: u64,
    #[serde(default = "default_max_failover_attempts")]
    pub max_failover_attempts: usize,
    #[serde(default = "default_unhealthy_after_failures")]
    pub unhealthy_after_failures: u32,
    #[serde(default = "default_recovery_after_secs")]
    pub recovery_after_secs: u64,
    #[serde(default)]
    pub probe: ProbeConfig,
    #[serde(default)]
    pub adaptive_hedging: AdaptiveHedgingConfig,
    #[serde(default)]
    pub predictive_scoring: PredictiveScoringConfig,
    #[serde(default)]
    pub shadow_mode: ShadowModeConfig,
    #[serde(default)]
    pub coalescing: CoalescingConfig,
    #[serde(default)]
    pub consensus_validation: ConsensusValidationConfig,
    #[serde(default)]
    pub cost_routing: CostRoutingConfig,
}

impl Default for ReliabilityConfig {
    fn default() -> Self {
        Self {
            request_timeout_ms: default_request_timeout_ms(),
            hedge_delay_ms: default_hedge_delay_ms(),
            max_failover_attempts: default_max_failover_attempts(),
            unhealthy_after_failures: default_unhealthy_after_failures(),
            recovery_after_secs: default_recovery_after_secs(),
            probe: ProbeConfig::default(),
            adaptive_hedging: AdaptiveHedgingConfig::default(),
            predictive_scoring: PredictiveScoringConfig::default(),
            shadow_mode: ShadowModeConfig::default(),
            coalescing: CoalescingConfig::default(),
            consensus_validation: ConsensusValidationConfig::default(),
            cost_routing: CostRoutingConfig::default(),
        }
    }
}

impl ReliabilityConfig {
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    pub fn hedge_delay(&self) -> Duration {
        Duration::from_millis(self.hedge_delay_ms)
    }

    pub fn recovery_after(&self) -> Duration {
        Duration::from_secs(self.recovery_after_secs)
    }

    pub fn normalized_max_failover_attempts(&self) -> usize {
        self.max_failover_attempts.max(1)
    }
}

fn default_request_timeout_ms() -> u64 {
    2_000
}

fn default_hedge_delay_ms() -> u64 {
    150
}

fn default_max_failover_attempts() -> usize {
    3
}

fn default_unhealthy_after_failures() -> u32 {
    3
}

fn default_recovery_after_secs() -> u64 {
    15
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoalescingConfig {
    #[serde(default = "default_coalescing_enabled")]
    pub enabled: bool,
}

impl Default for CoalescingConfig {
    fn default() -> Self {
        Self {
            enabled: default_coalescing_enabled(),
        }
    }
}

fn default_coalescing_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsensusValidationConfig {
    #[serde(default = "default_consensus_validation_enabled")]
    pub enabled: bool,
    #[serde(default = "default_consensus_validation_sample_size")]
    pub sample_size: usize,
    #[serde(default = "default_consensus_validation_fail_open")]
    pub fail_open: bool,
}

impl Default for ConsensusValidationConfig {
    fn default() -> Self {
        Self {
            enabled: default_consensus_validation_enabled(),
            sample_size: default_consensus_validation_sample_size(),
            fail_open: default_consensus_validation_fail_open(),
        }
    }
}

impl ConsensusValidationConfig {
    pub fn normalized_sample_size(&self) -> usize {
        self.sample_size.clamp(2, 3)
    }
}

fn default_consensus_validation_enabled() -> bool {
    false
}

fn default_consensus_validation_sample_size() -> usize {
    3
}

fn default_consensus_validation_fail_open() -> bool {
    false
}

#[derive(Debug, Clone, Deserialize)]
pub struct CostRoutingConfig {
    #[serde(default = "default_cost_routing_enabled")]
    pub enabled: bool,
    #[serde(default = "default_cost_routing_strategy")]
    pub strategy: String,
}

impl Default for CostRoutingConfig {
    fn default() -> Self {
        Self {
            enabled: default_cost_routing_enabled(),
            strategy: default_cost_routing_strategy(),
        }
    }
}

impl CostRoutingConfig {
    pub fn normalized_strategy(&self) -> String {
        let strategy = self.strategy.trim().to_ascii_lowercase();
        if strategy.is_empty() {
            return default_cost_routing_strategy();
        }
        strategy
    }
}

fn default_cost_routing_enabled() -> bool {
    false
}

fn default_cost_routing_strategy() -> String {
    "balanced".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct PredictiveScoringConfig {
    #[serde(default = "default_predictive_scoring_enabled")]
    pub enabled: bool,
    #[serde(default = "default_predictive_target_latency_ms")]
    pub target_latency_ms: u64,
    #[serde(default = "default_predictive_max_block_lag")]
    pub max_block_lag: u64,
    #[serde(default = "default_predictive_unknown_block_lag_factor")]
    pub unknown_block_lag_factor: f64,
    #[serde(default = "default_predictive_unknown_rate_limit_headroom")]
    pub unknown_rate_limit_headroom: f64,
    #[serde(default = "default_predictive_block_lag_interval_secs")]
    pub block_lag_poll_interval_secs: u64,
    #[serde(default = "default_predictive_block_lag_method")]
    pub block_lag_method: String,
    #[serde(default = "default_predictive_block_lag_params")]
    pub block_lag_params: Value,
}

impl Default for PredictiveScoringConfig {
    fn default() -> Self {
        Self {
            enabled: default_predictive_scoring_enabled(),
            target_latency_ms: default_predictive_target_latency_ms(),
            max_block_lag: default_predictive_max_block_lag(),
            unknown_block_lag_factor: default_predictive_unknown_block_lag_factor(),
            unknown_rate_limit_headroom: default_predictive_unknown_rate_limit_headroom(),
            block_lag_poll_interval_secs: default_predictive_block_lag_interval_secs(),
            block_lag_method: default_predictive_block_lag_method(),
            block_lag_params: default_predictive_block_lag_params(),
        }
    }
}

impl PredictiveScoringConfig {
    pub fn normalized_target_latency_ms(&self) -> u64 {
        self.target_latency_ms.max(1)
    }

    pub fn normalized_max_block_lag(&self) -> u64 {
        self.max_block_lag.max(1)
    }

    pub fn normalized_unknown_block_lag_factor(&self) -> f64 {
        self.unknown_block_lag_factor.clamp(0.0, 1.0)
    }

    pub fn normalized_unknown_rate_limit_headroom(&self) -> f64 {
        self.unknown_rate_limit_headroom.clamp(0.05, 1.0)
    }

    pub fn block_lag_poll_interval(&self) -> Duration {
        Duration::from_secs(self.block_lag_poll_interval_secs.max(1))
    }
}

fn default_predictive_scoring_enabled() -> bool {
    true
}

fn default_predictive_target_latency_ms() -> u64 {
    120
}

fn default_predictive_max_block_lag() -> u64 {
    8
}

fn default_predictive_unknown_block_lag_factor() -> f64 {
    0.85
}

fn default_predictive_unknown_rate_limit_headroom() -> f64 {
    1.0
}

fn default_predictive_block_lag_interval_secs() -> u64 {
    6
}

fn default_predictive_block_lag_method() -> String {
    "getSlot".to_string()
}

fn default_predictive_block_lag_params() -> Value {
    Value::Array(Vec::new())
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShadowModeConfig {
    #[serde(default = "default_shadow_mode_enabled")]
    pub enabled: bool,
    #[serde(default = "default_shadow_default_warmup_secs")]
    pub default_warmup_secs: u64,
    #[serde(default = "default_shadow_default_min_observations")]
    pub default_min_observations: u64,
    #[serde(default = "default_shadow_mirror_max_providers")]
    pub mirror_max_providers: usize,
}

impl Default for ShadowModeConfig {
    fn default() -> Self {
        Self {
            enabled: default_shadow_mode_enabled(),
            default_warmup_secs: default_shadow_default_warmup_secs(),
            default_min_observations: default_shadow_default_min_observations(),
            mirror_max_providers: default_shadow_mirror_max_providers(),
        }
    }
}

impl ShadowModeConfig {
    pub fn normalized_default_warmup_secs(&self) -> u64 {
        self.default_warmup_secs.max(1)
    }

    pub fn normalized_default_min_observations(&self) -> u64 {
        self.default_min_observations.max(1)
    }

    pub fn normalized_mirror_max_providers(&self) -> usize {
        self.mirror_max_providers.max(1)
    }
}

fn default_shadow_mode_enabled() -> bool {
    true
}

fn default_shadow_default_warmup_secs() -> u64 {
    300
}

fn default_shadow_default_min_observations() -> u64 {
    50
}

fn default_shadow_mirror_max_providers() -> usize {
    1
}

#[derive(Debug, Clone, Deserialize)]
pub struct AdaptiveHedgingConfig {
    #[serde(default = "default_adaptive_hedging_enabled")]
    pub enabled: bool,
    #[serde(default = "default_adaptive_min_hedge_width")]
    pub min_hedge_width: usize,
    #[serde(default = "default_adaptive_max_hedge_width")]
    pub max_hedge_width: usize,
    #[serde(default = "default_adaptive_rps_window_secs")]
    pub rps_window_secs: u64,
    #[serde(default = "default_adaptive_medium_rps")]
    pub medium_rps: f64,
    #[serde(default = "default_adaptive_high_rps")]
    pub high_rps: f64,
    #[serde(default = "default_adaptive_medium_latency_spread_ms")]
    pub medium_latency_spread_ms: u64,
    #[serde(default = "default_adaptive_high_latency_spread_ms")]
    pub high_latency_spread_ms: u64,
    #[serde(default = "default_adaptive_max_latency_samples")]
    pub max_latency_samples: usize,
}

impl Default for AdaptiveHedgingConfig {
    fn default() -> Self {
        Self {
            enabled: default_adaptive_hedging_enabled(),
            min_hedge_width: default_adaptive_min_hedge_width(),
            max_hedge_width: default_adaptive_max_hedge_width(),
            rps_window_secs: default_adaptive_rps_window_secs(),
            medium_rps: default_adaptive_medium_rps(),
            high_rps: default_adaptive_high_rps(),
            medium_latency_spread_ms: default_adaptive_medium_latency_spread_ms(),
            high_latency_spread_ms: default_adaptive_high_latency_spread_ms(),
            max_latency_samples: default_adaptive_max_latency_samples(),
        }
    }
}

impl AdaptiveHedgingConfig {
    pub fn normalized_min_hedge_width(&self) -> usize {
        self.min_hedge_width.max(1)
    }

    pub fn normalized_max_hedge_width(&self) -> usize {
        self.max_hedge_width.max(self.normalized_min_hedge_width())
    }

    pub fn normalized_rps_window_secs(&self) -> u64 {
        self.rps_window_secs.max(1)
    }

    pub fn normalized_max_latency_samples(&self) -> usize {
        self.max_latency_samples.max(5)
    }
}

fn default_adaptive_hedging_enabled() -> bool {
    true
}

fn default_adaptive_min_hedge_width() -> usize {
    1
}

fn default_adaptive_max_hedge_width() -> usize {
    4
}

fn default_adaptive_rps_window_secs() -> u64 {
    30
}

fn default_adaptive_medium_rps() -> f64 {
    40.0
}

fn default_adaptive_high_rps() -> f64 {
    120.0
}

fn default_adaptive_medium_latency_spread_ms() -> u64 {
    75
}

fn default_adaptive_high_latency_spread_ms() -> u64 {
    175
}

fn default_adaptive_max_latency_samples() -> usize {
    180
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProbeConfig {
    #[serde(default = "default_probe_enabled")]
    pub enabled: bool,
    #[serde(default = "default_probe_interval_secs")]
    pub interval_secs: u64,
    #[serde(default = "default_probe_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_probe_method")]
    pub method: String,
    #[serde(default = "default_probe_params")]
    pub params: Value,
}

impl Default for ProbeConfig {
    fn default() -> Self {
        Self {
            enabled: default_probe_enabled(),
            interval_secs: default_probe_interval_secs(),
            timeout_ms: default_probe_timeout_ms(),
            method: default_probe_method(),
            params: default_probe_params(),
        }
    }
}

impl ProbeConfig {
    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.interval_secs.max(1))
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms.max(1))
    }
}

fn default_probe_enabled() -> bool {
    true
}

fn default_probe_interval_secs() -> u64 {
    10
}

fn default_probe_timeout_ms() -> u64 {
    1_200
}

fn default_probe_method() -> String {
    "getHealth".to_string()
}

fn default_probe_params() -> Value {
    Value::Array(Vec::new())
}

#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_cache_enabled")]
    pub enabled: bool,
    #[serde(default = "default_cache_ttl_secs")]
    pub ttl_secs: u64,
    #[serde(default = "default_cache_capacity")]
    pub max_capacity: u64,
    #[serde(default)]
    pub cacheable_methods: Vec<String>,
    #[serde(default)]
    pub method_ttl_secs: HashMap<String, u64>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_cache_enabled(),
            ttl_secs: default_cache_ttl_secs(),
            max_capacity: default_cache_capacity(),
            cacheable_methods: Vec::new(),
            method_ttl_secs: HashMap::new(),
        }
    }
}

fn default_cache_enabled() -> bool {
    true
}

fn default_cache_ttl_secs() -> u64 {
    2
}

fn default_cache_capacity() -> u64 {
    20_000
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MethodPolicyConfig {
    #[serde(default)]
    pub overrides: HashMap<String, MethodPolicyOverride>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MethodPolicyOverride {
    #[serde(default)]
    pub cacheable_by_default: Option<bool>,
    #[serde(default)]
    pub consensus_critical: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    pub url: String,
    #[serde(default = "default_provider_weight")]
    pub weight: u32,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub shadow_mode: bool,
    #[serde(default)]
    pub shadow_warmup_secs: Option<u64>,
    #[serde(default)]
    pub shadow_min_observations: Option<u64>,
    #[serde(default)]
    pub cost_per_million_requests: Option<f64>,
}

impl ProviderConfig {
    pub fn timeout_or_default(&self, fallback: Duration) -> Duration {
        self.timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(fallback)
    }

    pub fn shadow_warmup_or_default(&self, fallback_secs: u64) -> u64 {
        self.shadow_warmup_secs.unwrap_or(fallback_secs).max(1)
    }

    pub fn shadow_min_observations_or_default(&self, fallback: u64) -> u64 {
        self.shadow_min_observations.unwrap_or(fallback).max(1)
    }
}

fn default_provider_weight() -> u32 {
    100
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn reliability_normalizes_failover_attempts() {
        let reliability = ReliabilityConfig {
            max_failover_attempts: 0,
            ..ReliabilityConfig::default()
        };

        assert_eq!(reliability.normalized_max_failover_attempts(), 1);
    }

    #[test]
    fn provider_timeout_falls_back_when_none() {
        let provider = ProviderConfig {
            name: "provider-a".to_string(),
            url: "http://localhost:8545".to_string(),
            weight: 100,
            timeout_ms: None,
            headers: HashMap::new(),
            shadow_mode: false,
            shadow_warmup_secs: None,
            shadow_min_observations: None,
            cost_per_million_requests: None,
        };

        assert_eq!(
            provider.timeout_or_default(Duration::from_millis(900)),
            Duration::from_millis(900)
        );
    }

    #[test]
    fn parses_probe_and_per_method_cache_ttls_from_toml() {
        let raw = r#"
[server]
bind_addr = "127.0.0.1:8080"

[reliability]
request_timeout_ms = 2200
hedge_delay_ms = 150
max_failover_attempts = 3
unhealthy_after_failures = 3
recovery_after_secs = 15

[reliability.adaptive_hedging]
enabled = true
min_hedge_width = 1
max_hedge_width = 3
rps_window_secs = 20
medium_rps = 25.0
high_rps = 75.0
medium_latency_spread_ms = 60
high_latency_spread_ms = 150
max_latency_samples = 100

[reliability.predictive_scoring]
enabled = true
target_latency_ms = 100
max_block_lag = 12
unknown_block_lag_factor = 0.7
unknown_rate_limit_headroom = 0.9
block_lag_poll_interval_secs = 8
block_lag_method = "getSlot"
block_lag_params = []

[reliability.shadow_mode]
enabled = true
default_warmup_secs = 420
default_min_observations = 33
mirror_max_providers = 2

[reliability.coalescing]
enabled = true

[reliability.consensus_validation]
enabled = true
sample_size = 3
fail_open = false

[reliability.cost_routing]
enabled = true
strategy = "cheapest"

[reliability.probe]
enabled = true
interval_secs = 9
timeout_ms = 650
method = "getHealth"
params = []

[cache]
enabled = true
ttl_secs = 2
max_capacity = 100
cacheable_methods = ["getSlot"]

[cache.method_ttl_secs]
getSlot = 30
getBalance = 1

[method_policy.overrides."customExperimentalMethod"]
cacheable_by_default = true
consensus_critical = true

[[providers]]
name = "provider-a"
url = "http://localhost:8545"
shadow_mode = true
shadow_warmup_secs = 60
shadow_min_observations = 5
cost_per_million_requests = 12.5
"#;

        let settings: Settings = toml::from_str(raw).expect("settings TOML should parse");
        assert_eq!(settings.reliability.probe.interval_secs, 9);
        assert_eq!(settings.reliability.probe.method, "getHealth");
        assert_eq!(settings.reliability.probe.params, json!([]));
        assert!(settings.reliability.adaptive_hedging.enabled);
        assert_eq!(settings.reliability.adaptive_hedging.max_hedge_width, 3);
        assert_eq!(settings.reliability.adaptive_hedging.high_rps, 75.0);
        assert!(settings.reliability.predictive_scoring.enabled);
        assert_eq!(settings.reliability.predictive_scoring.max_block_lag, 12);
        assert_eq!(
            settings
                .reliability
                .predictive_scoring
                .unknown_block_lag_factor,
            0.7
        );
        assert!(settings.reliability.shadow_mode.enabled);
        assert_eq!(settings.reliability.shadow_mode.default_warmup_secs, 420);
        assert_eq!(settings.reliability.shadow_mode.mirror_max_providers, 2);
        assert!(settings.reliability.coalescing.enabled);
        assert!(settings.reliability.consensus_validation.enabled);
        assert_eq!(settings.reliability.consensus_validation.sample_size, 3);
        assert!(!settings.reliability.consensus_validation.fail_open);
        assert!(settings.reliability.cost_routing.enabled);
        assert_eq!(
            settings.reliability.cost_routing.normalized_strategy(),
            "cheapest"
        );
        assert_eq!(settings.cache.method_ttl_secs.get("getSlot"), Some(&30));
        assert_eq!(settings.cache.method_ttl_secs.get("getBalance"), Some(&1));
        let override_policy = settings
            .method_policy
            .overrides
            .get("customExperimentalMethod")
            .expect("method policy override should exist");
        assert_eq!(override_policy.cacheable_by_default, Some(true));
        assert_eq!(override_policy.consensus_critical, Some(true));
        assert!(settings.providers[0].shadow_mode);
        assert_eq!(settings.providers[0].shadow_warmup_secs, Some(60));
        assert_eq!(settings.providers[0].shadow_min_observations, Some(5));
        assert_eq!(settings.providers[0].cost_per_million_requests, Some(12.5));
    }

    #[test]
    fn probe_interval_and_timeout_have_floor() {
        let probe = ProbeConfig {
            enabled: true,
            interval_secs: 0,
            timeout_ms: 0,
            method: "getHealth".to_string(),
            params: json!([]),
        };

        assert_eq!(probe.interval(), Duration::from_secs(1));
        assert_eq!(probe.timeout(), Duration::from_millis(1));
    }

    #[test]
    fn adaptive_hedging_normalization_enforces_bounds() {
        let adaptive = AdaptiveHedgingConfig {
            enabled: true,
            min_hedge_width: 0,
            max_hedge_width: 0,
            rps_window_secs: 0,
            medium_rps: 10.0,
            high_rps: 20.0,
            medium_latency_spread_ms: 50,
            high_latency_spread_ms: 100,
            max_latency_samples: 0,
        };

        assert_eq!(adaptive.normalized_min_hedge_width(), 1);
        assert_eq!(adaptive.normalized_max_hedge_width(), 1);
        assert_eq!(adaptive.normalized_rps_window_secs(), 1);
        assert_eq!(adaptive.normalized_max_latency_samples(), 5);
    }

    #[test]
    fn predictive_and_shadow_normalization_enforces_bounds() {
        let predictive = PredictiveScoringConfig {
            enabled: true,
            target_latency_ms: 0,
            max_block_lag: 0,
            unknown_block_lag_factor: 1.5,
            unknown_rate_limit_headroom: 0.0,
            block_lag_poll_interval_secs: 0,
            block_lag_method: "getSlot".to_string(),
            block_lag_params: json!([]),
        };
        let shadow = ShadowModeConfig {
            enabled: true,
            default_warmup_secs: 0,
            default_min_observations: 0,
            mirror_max_providers: 0,
        };
        let consensus = ConsensusValidationConfig {
            enabled: true,
            sample_size: 50,
            fail_open: false,
        };
        let cost = CostRoutingConfig {
            enabled: true,
            strategy: "  ".to_string(),
        };
        let provider = ProviderConfig {
            name: "provider-a".to_string(),
            url: "http://localhost:8545".to_string(),
            weight: 100,
            timeout_ms: None,
            headers: HashMap::new(),
            shadow_mode: true,
            shadow_warmup_secs: Some(0),
            shadow_min_observations: Some(0),
            cost_per_million_requests: None,
        };

        assert_eq!(predictive.normalized_target_latency_ms(), 1);
        assert_eq!(predictive.normalized_max_block_lag(), 1);
        assert_eq!(predictive.normalized_unknown_block_lag_factor(), 1.0);
        assert_eq!(predictive.normalized_unknown_rate_limit_headroom(), 0.05);
        assert_eq!(predictive.block_lag_poll_interval(), Duration::from_secs(1));
        assert_eq!(shadow.normalized_default_warmup_secs(), 1);
        assert_eq!(shadow.normalized_default_min_observations(), 1);
        assert_eq!(shadow.normalized_mirror_max_providers(), 1);
        assert_eq!(consensus.normalized_sample_size(), 3);
        assert_eq!(cost.normalized_strategy(), "balanced");
        assert_eq!(provider.shadow_warmup_or_default(300), 1);
        assert_eq!(provider.shadow_min_observations_or_default(50), 1);
    }
}
