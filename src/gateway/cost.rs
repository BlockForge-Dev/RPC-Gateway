use crate::settings::{CostRoutingConfig, ProviderConfig};

pub(super) struct CostRoutingPolicy {
    enabled: bool,
    strategy: CostRoutingStrategy,
    provider_costs: Vec<Option<f64>>,
}

impl CostRoutingPolicy {
    pub(super) fn from_config(config: &CostRoutingConfig, providers: &[ProviderConfig]) -> Self {
        Self {
            enabled: config.enabled,
            strategy: CostRoutingStrategy::from_str(config.normalized_strategy().as_str()),
            provider_costs: providers
                .iter()
                .map(|provider| provider.cost_per_million_requests)
                .collect(),
        }
    }

    pub(super) fn adjust_score(
        &self,
        provider_index: usize,
        base_score: f64,
        min_known_cost: Option<f64>,
    ) -> f64 {
        base_score * self.cost_factor(provider_index, min_known_cost)
    }

    pub(super) fn min_cost_for_indexes(&self, indexes: &[usize]) -> Option<f64> {
        indexes
            .iter()
            .filter_map(|index| self.provider_cost(*index))
            .reduce(f64::min)
    }

    pub(super) fn provider_cost(&self, provider_index: usize) -> Option<f64> {
        self.provider_costs
            .get(provider_index)
            .copied()
            .flatten()
            .filter(|cost| *cost > 0.0)
    }

    fn cost_factor(&self, provider_index: usize, min_known_cost: Option<f64>) -> f64 {
        if !self.enabled {
            return 1.0;
        }

        let Some(min_cost) = min_known_cost else {
            return 1.0;
        };

        let Some(cost) = self.provider_cost(provider_index) else {
            return 0.9;
        };
        let ratio = (min_cost / cost).clamp(0.05, 1.0);
        self.strategy.factor_from_ratio(ratio)
    }
}

#[derive(Clone, Copy)]
enum CostRoutingStrategy {
    Cheapest,
    Balanced,
    LatencyFirst,
}

impl CostRoutingStrategy {
    fn from_str(strategy: &str) -> Self {
        match strategy {
            "cheapest" | "cheapest_healthy_first" => Self::Cheapest,
            "latency_first" | "latency-first" => Self::LatencyFirst,
            _ => Self::Balanced,
        }
    }

    fn factor_from_ratio(self, ratio: f64) -> f64 {
        match self {
            Self::Cheapest => ratio,
            Self::Balanced => 0.65 + (0.35 * ratio),
            Self::LatencyFirst => 0.85 + (0.15 * ratio),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::CostRoutingPolicy;
    use crate::settings::{CostRoutingConfig, ProviderConfig};

    fn provider(name: &str, cost: Option<f64>) -> ProviderConfig {
        ProviderConfig {
            name: name.to_string(),
            url: "http://localhost:8545".to_string(),
            weight: 100,
            timeout_ms: None,
            headers: HashMap::new(),
            shadow_mode: false,
            shadow_warmup_secs: None,
            shadow_min_observations: None,
            cost_per_million_requests: cost,
        }
    }

    #[test]
    fn cheapest_strategy_penalizes_expensive_provider() {
        let policy = CostRoutingPolicy::from_config(
            &CostRoutingConfig {
                enabled: true,
                strategy: "cheapest".to_string(),
            },
            &[provider("a", Some(12.0)), provider("b", Some(3.0))],
        );
        let min_cost = policy.min_cost_for_indexes(&[0, 1]);
        let expensive = policy.adjust_score(0, 100.0, min_cost);
        let cheap = policy.adjust_score(1, 100.0, min_cost);

        assert!(cheap > expensive);
    }

    #[test]
    fn disabled_strategy_does_not_change_scores() {
        let policy = CostRoutingPolicy::from_config(
            &CostRoutingConfig {
                enabled: false,
                strategy: "cheapest".to_string(),
            },
            &[provider("a", Some(12.0)), provider("b", Some(3.0))],
        );
        let min_cost = policy.min_cost_for_indexes(&[0, 1]);
        assert_eq!(policy.adjust_score(0, 77.0, min_cost), 77.0);
    }
}
