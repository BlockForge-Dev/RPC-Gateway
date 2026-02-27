use std::{
    collections::VecDeque,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

use serde::Serialize;

use crate::settings::AdaptiveHedgingConfig;

pub(super) struct AdaptiveHedgingController {
    config: AdaptiveHedgingConfig,
    state: Mutex<AdaptiveState>,
}

impl AdaptiveHedgingController {
    pub(super) fn new(config: AdaptiveHedgingConfig, provider_count: usize) -> Self {
        let provider_count = provider_count.max(1);
        let max_latency_samples = config.normalized_max_latency_samples();
        let provider_latency_samples = (0..provider_count)
            .map(|_| VecDeque::with_capacity(max_latency_samples))
            .collect::<Vec<_>>();

        Self {
            config,
            state: Mutex::new(AdaptiveState {
                request_timestamps: VecDeque::new(),
                provider_latency_samples,
                total_hedged_requests: 0,
                hedge_win_count: 0,
                last_observed_rps: 0.0,
                last_latency_spread_ms: 0.0,
                last_decision_reason: None,
            }),
        }
    }

    pub(super) fn record_request(&self) {
        let mut state = self.lock_state();
        let now = Instant::now();
        state.request_timestamps.push_back(now);
        prune_old_timestamps(
            &mut state.request_timestamps,
            now,
            self.config.normalized_rps_window_secs(),
        );
        state.last_observed_rps =
            state.request_timestamps.len() as f64 / self.config.normalized_rps_window_secs() as f64;
    }

    pub(super) fn record_provider_latency(&self, provider_index: usize, latency: Duration) {
        let mut state = self.lock_state();
        let Some(samples) = state.provider_latency_samples.get_mut(provider_index) else {
            return;
        };
        let sample_ms = latency.as_millis() as u64;
        samples.push_back(sample_ms);
        while samples.len() > self.config.normalized_max_latency_samples() {
            samples.pop_front();
        }
    }

    pub(super) fn record_hedge_outcome(&self, preferred_provider: usize, winner_provider: usize) {
        let mut state = self.lock_state();
        state.total_hedged_requests = state.total_hedged_requests.saturating_add(1);
        if preferred_provider != winner_provider {
            state.hedge_win_count = state.hedge_win_count.saturating_add(1);
        }
    }

    pub(super) fn decide_hedge_width(&self, ranked_candidates: &[usize]) -> HedgeDecision {
        let mut state = self.lock_state();
        let now = Instant::now();
        prune_old_timestamps(
            &mut state.request_timestamps,
            now,
            self.config.normalized_rps_window_secs(),
        );
        state.last_observed_rps =
            state.request_timestamps.len() as f64 / self.config.normalized_rps_window_secs() as f64;
        state.last_latency_spread_ms =
            p95_latency_spread_ms(&state.provider_latency_samples, ranked_candidates)
                .unwrap_or(0.0);

        let available = ranked_candidates.len();
        if available == 0 {
            let decision = HedgeDecision {
                hedge_width: 0,
                observed_rps: state.last_observed_rps,
                observed_latency_spread_ms: state.last_latency_spread_ms,
                reason: HedgeReason::NoCandidates,
            };
            state.last_decision_reason = Some(decision.reason);
            return decision;
        }

        if !self.config.enabled {
            let decision = HedgeDecision {
                hedge_width: 1,
                observed_rps: state.last_observed_rps,
                observed_latency_spread_ms: state.last_latency_spread_ms,
                reason: HedgeReason::AdaptiveDisabled,
            };
            state.last_decision_reason = Some(decision.reason);
            return decision;
        }

        let min_width = self.config.normalized_min_hedge_width().min(available);
        let max_width = self.config.normalized_max_hedge_width().min(available);
        let spread = state.last_latency_spread_ms;
        let rps = state.last_observed_rps;

        let (width, reason) = if rps >= self.config.high_rps {
            (min_width, HedgeReason::HighLoad)
        } else if spread >= self.config.high_latency_spread_ms as f64 {
            (max_width, HedgeReason::HighLatencyVariance)
        } else if spread >= self.config.medium_latency_spread_ms as f64 {
            if rps >= self.config.medium_rps {
                (min_width, HedgeReason::ModerateVarianceModerateLoad)
            } else {
                (
                    (min_width + 1).min(max_width),
                    HedgeReason::ModerateVarianceLowLoad,
                )
            }
        } else {
            (min_width, HedgeReason::LowVariance)
        };

        let decision = HedgeDecision {
            hedge_width: width.max(1),
            observed_rps: rps,
            observed_latency_spread_ms: spread,
            reason,
        };
        state.last_decision_reason = Some(decision.reason);
        decision
    }

    pub(super) fn stats_snapshot(&self) -> HedgingStatsView {
        let state = self.lock_state();
        let hedge_win_rate = if state.total_hedged_requests == 0 {
            0.0
        } else {
            state.hedge_win_count as f64 / state.total_hedged_requests as f64
        };

        HedgingStatsView {
            adaptive_enabled: self.config.enabled,
            total_hedged_requests: state.total_hedged_requests,
            hedge_win_count: state.hedge_win_count,
            hedge_win_rate,
            observed_rps: state.last_observed_rps,
            observed_latency_spread_ms: state.last_latency_spread_ms,
            min_hedge_width: self.config.normalized_min_hedge_width(),
            max_hedge_width: self.config.normalized_max_hedge_width(),
            last_decision_reason: state.last_decision_reason,
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, AdaptiveState> {
        self.state.lock().unwrap_or_else(|err| err.into_inner())
    }
}

struct AdaptiveState {
    request_timestamps: VecDeque<Instant>,
    provider_latency_samples: Vec<VecDeque<u64>>,
    total_hedged_requests: u64,
    hedge_win_count: u64,
    last_observed_rps: f64,
    last_latency_spread_ms: f64,
    last_decision_reason: Option<HedgeReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum HedgeReason {
    AdaptiveDisabled,
    NoCandidates,
    HighLoad,
    HighLatencyVariance,
    ModerateVarianceLowLoad,
    ModerateVarianceModerateLoad,
    LowVariance,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct HedgeDecision {
    pub(super) hedge_width: usize,
    pub(super) observed_rps: f64,
    pub(super) observed_latency_spread_ms: f64,
    pub(super) reason: HedgeReason,
}

#[derive(Debug, Clone, Serialize)]
pub struct HedgingStatsView {
    pub adaptive_enabled: bool,
    pub total_hedged_requests: u64,
    pub hedge_win_count: u64,
    pub hedge_win_rate: f64,
    pub observed_rps: f64,
    pub observed_latency_spread_ms: f64,
    pub min_hedge_width: usize,
    pub max_hedge_width: usize,
    pub last_decision_reason: Option<HedgeReason>,
}

fn prune_old_timestamps(timestamps: &mut VecDeque<Instant>, now: Instant, window_secs: u64) {
    let window = Duration::from_secs(window_secs.max(1));
    while let Some(front) = timestamps.front() {
        if now.duration_since(*front) > window {
            timestamps.pop_front();
        } else {
            break;
        }
    }
}

fn p95_latency_spread_ms(
    samples_by_provider: &[VecDeque<u64>],
    ranked_candidates: &[usize],
) -> Option<f64> {
    let mut p95_values = Vec::new();

    for provider_index in ranked_candidates {
        let Some(samples) = samples_by_provider.get(*provider_index) else {
            continue;
        };
        if samples.is_empty() {
            continue;
        }

        let mut values = samples.iter().copied().collect::<Vec<_>>();
        values.sort_unstable();
        let idx = ((values.len() as f64 * 0.95).ceil() as usize)
            .saturating_sub(1)
            .min(values.len().saturating_sub(1));
        p95_values.push(values[idx] as f64);
    }

    if p95_values.len() < 2 {
        return None;
    }

    let min_value = p95_values
        .iter()
        .fold(f64::INFINITY, |acc, sample| acc.min(*sample));
    let max_value = p95_values
        .iter()
        .fold(f64::NEG_INFINITY, |acc, sample| acc.max(*sample));
    Some((max_value - min_value).max(0.0))
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::{AdaptiveHedgingController, HedgeReason};
    use crate::settings::AdaptiveHedgingConfig;

    fn config_for_test() -> AdaptiveHedgingConfig {
        AdaptiveHedgingConfig {
            enabled: true,
            min_hedge_width: 1,
            max_hedge_width: 4,
            rps_window_secs: 1,
            medium_rps: 5.0,
            high_rps: 10.0,
            medium_latency_spread_ms: 60,
            high_latency_spread_ms: 150,
            max_latency_samples: 100,
        }
    }

    #[test]
    fn chooses_max_width_for_high_latency_spread() {
        let controller = AdaptiveHedgingController::new(config_for_test(), 4);
        controller.record_provider_latency(0, Duration::from_millis(40));
        controller.record_provider_latency(1, Duration::from_millis(80));
        controller.record_provider_latency(2, Duration::from_millis(220));
        controller.record_provider_latency(3, Duration::from_millis(240));

        let decision = controller.decide_hedge_width(&[0, 1, 2, 3]);
        assert_eq!(decision.hedge_width, 4);
        assert_eq!(decision.reason, HedgeReason::HighLatencyVariance);
    }

    #[test]
    fn reduces_width_on_high_load_even_with_variance() {
        let controller = AdaptiveHedgingController::new(config_for_test(), 3);
        controller.record_provider_latency(0, Duration::from_millis(30));
        controller.record_provider_latency(1, Duration::from_millis(140));
        controller.record_provider_latency(2, Duration::from_millis(200));

        for _ in 0..20 {
            controller.record_request();
        }

        let decision = controller.decide_hedge_width(&[0, 1, 2]);
        assert_eq!(decision.hedge_width, 1);
        assert_eq!(decision.reason, HedgeReason::HighLoad);
    }

    #[test]
    fn moderate_variance_picks_middle_width_on_low_load() {
        let mut config = config_for_test();
        config.max_hedge_width = 3;
        let controller = AdaptiveHedgingController::new(config, 3);
        controller.record_provider_latency(0, Duration::from_millis(40));
        controller.record_provider_latency(1, Duration::from_millis(100));
        controller.record_provider_latency(2, Duration::from_millis(115));

        let decision = controller.decide_hedge_width(&[0, 1, 2]);
        assert_eq!(decision.hedge_width, 2);
        assert_eq!(decision.reason, HedgeReason::ModerateVarianceLowLoad);
    }

    #[test]
    fn old_requests_age_out_from_rps_window() {
        let controller = AdaptiveHedgingController::new(config_for_test(), 2);
        for _ in 0..8 {
            controller.record_request();
        }
        thread::sleep(Duration::from_millis(1_200));
        controller.record_request();

        let decision = controller.decide_hedge_width(&[0, 1]);
        assert!(decision.observed_rps < 3.0);
    }

    #[test]
    fn hedge_win_rate_tracks_winner_outcomes() {
        let controller = AdaptiveHedgingController::new(config_for_test(), 2);
        controller.record_hedge_outcome(0, 0);
        controller.record_hedge_outcome(0, 1);
        controller.record_hedge_outcome(0, 1);
        let snapshot = controller.stats_snapshot();

        assert_eq!(snapshot.total_hedged_requests, 3);
        assert_eq!(snapshot.hedge_win_count, 2);
        assert!((snapshot.hedge_win_rate - (2.0 / 3.0)).abs() < 0.0001);
    }
}
