use std::{
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

pub struct ProviderHealthTracker {
    unhealthy_after_failures: u32,
    recovery_after: Duration,
    state: Mutex<ProviderHealthState>,
}

impl ProviderHealthTracker {
    pub fn new(unhealthy_after_failures: u32, recovery_after: Duration) -> Self {
        Self {
            unhealthy_after_failures: unhealthy_after_failures.max(1),
            recovery_after,
            state: Mutex::new(ProviderHealthState::default()),
        }
    }

    pub fn record_success(&self, latency: Duration) {
        let mut state = self.lock_state();
        state.total_success = state.total_success.saturating_add(1);
        state.consecutive_failures = 0;
        state.last_success_unix_ms = Some(now_unix_ms());
        state.circuit_open_until = None;

        let sample_ms = latency.as_secs_f64() * 1_000.0;
        state.latency_ewma_ms = Some(match state.latency_ewma_ms {
            Some(previous) => (previous * 0.75) + (sample_ms * 0.25),
            None => sample_ms,
        });
    }

    pub fn record_failure(&self) {
        let mut state = self.lock_state();
        state.total_failures = state.total_failures.saturating_add(1);
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        state.last_failure_unix_ms = Some(now_unix_ms());

        if state.consecutive_failures >= self.unhealthy_after_failures {
            state.circuit_open_until = Some(Instant::now() + self.recovery_after);
        }
    }

    pub fn snapshot(&self, weight: u32) -> ProviderHealthSnapshot {
        let mut state = self.lock_state();
        let (healthy, remaining_open_ms) = match state.circuit_open_until {
            Some(open_until) if open_until > Instant::now() => {
                let remaining = (open_until - Instant::now()).as_millis() as u64;
                (false, Some(remaining))
            }
            Some(_) => {
                state.circuit_open_until = None;
                (true, None)
            }
            None => (true, None),
        };

        let total = state.total_success.saturating_add(state.total_failures);
        let success_rate = if total == 0 {
            1.0
        } else {
            state.total_success as f64 / total as f64
        };
        let latency_penalty = state.latency_ewma_ms.unwrap_or(100.0) / 20.0;
        let failure_penalty = state.consecutive_failures as f64 * 30.0;
        let health_penalty = if healthy { 0.0 } else { 10_000.0 };
        let score = (weight as f64 * 10.0) + (success_rate * 100.0)
            - latency_penalty
            - failure_penalty
            - health_penalty;

        ProviderHealthSnapshot {
            healthy,
            consecutive_failures: state.consecutive_failures,
            total_success: state.total_success,
            total_failures: state.total_failures,
            last_success_unix_ms: state.last_success_unix_ms,
            last_failure_unix_ms: state.last_failure_unix_ms,
            latency_ewma_ms: state.latency_ewma_ms,
            circuit_open_remaining_ms: remaining_open_ms,
            score,
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, ProviderHealthState> {
        self.state.lock().unwrap_or_else(|err| err.into_inner())
    }
}

#[derive(Default)]
struct ProviderHealthState {
    consecutive_failures: u32,
    total_success: u64,
    total_failures: u64,
    last_success_unix_ms: Option<u128>,
    last_failure_unix_ms: Option<u128>,
    latency_ewma_ms: Option<f64>,
    circuit_open_until: Option<Instant>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderHealthSnapshot {
    pub healthy: bool,
    pub consecutive_failures: u32,
    pub total_success: u64,
    pub total_failures: u64,
    pub last_success_unix_ms: Option<u128>,
    pub last_failure_unix_ms: Option<u128>,
    pub latency_ewma_ms: Option<f64>,
    pub circuit_open_remaining_ms: Option<u64>,
    pub score: f64,
}

pub fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn opens_circuit_after_threshold() {
        let tracker = ProviderHealthTracker::new(2, Duration::from_secs(30));
        tracker.record_failure();
        assert!(tracker.snapshot(100).healthy);

        tracker.record_failure();
        let snapshot = tracker.snapshot(100);
        assert!(!snapshot.healthy);
        assert!(snapshot.circuit_open_remaining_ms.is_some());
    }

    #[test]
    fn success_resets_failure_state() {
        let tracker = ProviderHealthTracker::new(1, Duration::from_secs(30));
        tracker.record_failure();
        assert!(!tracker.snapshot(100).healthy);

        tracker.record_success(Duration::from_millis(20));
        let snapshot = tracker.snapshot(100);
        assert!(snapshot.healthy);
        assert_eq!(snapshot.consecutive_failures, 0);
        assert_eq!(snapshot.total_success, 1);
    }

    #[test]
    fn circuit_closes_after_recovery_window() {
        let tracker = ProviderHealthTracker::new(1, Duration::from_millis(15));
        tracker.record_failure();
        assert!(!tracker.snapshot(100).healthy);

        thread::sleep(Duration::from_millis(30));
        assert!(tracker.snapshot(100).healthy);
    }
}
