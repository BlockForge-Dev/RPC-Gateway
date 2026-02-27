use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use dashmap::{DashMap, mapref::entry::Entry};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Notify};

pub(super) struct RequestCoalescer<T, E> {
    enabled: bool,
    in_flight: DashMap<String, Arc<InFlightRequest<T, E>>>,
    leader_count: AtomicU64,
    fanout_count: AtomicU64,
}

impl<T, E> RequestCoalescer<T, E>
where
    T: Clone,
    E: Clone,
{
    pub(super) fn new(enabled: bool) -> Self {
        Self {
            enabled,
            in_flight: DashMap::new(),
            leader_count: AtomicU64::new(0),
            fanout_count: AtomicU64::new(0),
        }
    }

    pub(super) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(super) fn stats(&self) -> CoalescingStatsView {
        CoalescingStatsView {
            enabled: self.enabled,
            in_flight: self.in_flight.len(),
            leader_count: self.leader_count.load(Ordering::Relaxed),
            fanout_count: self.fanout_count.load(Ordering::Relaxed),
        }
    }

    pub(super) async fn run_or_join<F, Fut>(
        &self,
        key: String,
        operation: F,
    ) -> (Result<T, E>, bool)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if !self.enabled {
            return (operation().await, false);
        }

        let (in_flight, is_leader) = match self.in_flight.entry(key.clone()) {
            Entry::Occupied(existing) => (Arc::clone(existing.get()), false),
            Entry::Vacant(vacant) => {
                let state = Arc::new(InFlightRequest::new());
                vacant.insert(Arc::clone(&state));
                (state, true)
            }
        };

        if is_leader {
            self.leader_count.fetch_add(1, Ordering::Relaxed);
            let result = operation().await;
            {
                let mut slot = in_flight.result.lock().await;
                *slot = Some(result.clone());
            }
            self.in_flight.remove(&key);
            in_flight.notify.notify_waiters();
            return (result, false);
        }

        self.fanout_count.fetch_add(1, Ordering::Relaxed);
        loop {
            if let Some(done) = in_flight.result.lock().await.clone() {
                return (done, true);
            }
            in_flight.notify.notified().await;
        }
    }
}

struct InFlightRequest<T, E> {
    result: Mutex<Option<Result<T, E>>>,
    notify: Notify,
}

impl<T, E> InFlightRequest<T, E> {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            notify: Notify::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CoalescingStatsView {
    pub enabled: bool,
    pub in_flight: usize,
    pub leader_count: u64,
    pub fanout_count: u64,
}

pub(super) fn request_coalescing_key(method: &str, body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    let digest = hex::encode(hasher.finalize());
    format!("{}:{digest}", method.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio::{sync::oneshot, time::sleep};

    use super::RequestCoalescer;

    #[tokio::test]
    async fn coalescer_fans_out_to_waiters() {
        let coalescer = Arc::new(RequestCoalescer::<u64, String>::new(true));
        let execution_count = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = oneshot::channel();

        let c1 = Arc::clone(&coalescer);
        let e1 = Arc::clone(&execution_count);
        let first = tokio::spawn(async move {
            c1.run_or_join("k".to_string(), || async move {
                e1.fetch_add(1, Ordering::SeqCst);
                let _ = started_tx.send(());
                sleep(Duration::from_millis(20)).await;
                Ok(7)
            })
            .await
        });

        let _ = started_rx.await;

        let c2 = Arc::clone(&coalescer);
        let e2 = Arc::clone(&execution_count);
        let second = tokio::spawn(async move {
            c2.run_or_join("k".to_string(), || async move {
                e2.fetch_add(1, Ordering::SeqCst);
                Ok(9)
            })
            .await
        });

        let (result_a, joined_a) = first.await.expect("task should complete");
        let (result_b, joined_b) = second.await.expect("task should complete");

        assert_eq!(result_a.expect("call should succeed"), 7);
        assert_eq!(result_b.expect("call should succeed"), 7);
        assert_ne!(joined_a, joined_b);
        let stats = coalescer.stats();
        assert_eq!(stats.leader_count, 1);
        assert_eq!(stats.fanout_count, 1);
        assert_eq!(execution_count.load(Ordering::SeqCst), 1);
    }
}
