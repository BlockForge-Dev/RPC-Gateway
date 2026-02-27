use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use crate::{health::now_unix_ms, settings::CacheConfig};

pub(super) struct CachePolicy {
    enabled: bool,
    default_ttl: Duration,
    cacheable_methods: Vec<String>,
    method_ttl_secs: HashMap<String, u64>,
}

impl CachePolicy {
    pub(super) fn from_config(config: &CacheConfig) -> Self {
        Self {
            enabled: config.enabled,
            default_ttl: Duration::from_secs(config.ttl_secs.max(1)),
            cacheable_methods: config
                .cacheable_methods
                .iter()
                .map(|method| method.to_ascii_lowercase())
                .collect(),
            method_ttl_secs: config
                .method_ttl_secs
                .iter()
                .map(|(method, ttl)| (method.to_ascii_lowercase(), *ttl))
                .collect(),
        }
    }

    pub(super) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(super) fn plan(
        &self,
        body: &[u8],
        method: Option<&str>,
        cacheable_by_default: bool,
    ) -> Option<CachePlan> {
        if !self.enabled {
            return None;
        }

        let method = method?;
        let ttl = self.ttl_for_method(method, cacheable_by_default)?;
        let method_normalized = method.to_ascii_lowercase();

        let mut hasher = Sha256::new();
        hasher.update(body);
        let digest = hex::encode(hasher.finalize());

        Some(CachePlan {
            key: format!("{method_normalized}:{digest}"),
            ttl,
        })
    }

    fn ttl_for_method(&self, method: &str, cacheable_by_default: bool) -> Option<Duration> {
        let method_lowercase = method.to_ascii_lowercase();

        if let Some(method_ttl_secs) = self.method_ttl_secs.get(&method_lowercase) {
            if *method_ttl_secs == 0 {
                return None;
            }
            return Some(Duration::from_secs(*method_ttl_secs));
        }

        if !self.cacheable_methods.is_empty() {
            if self
                .cacheable_methods
                .iter()
                .any(|cacheable| cacheable == &method_lowercase)
            {
                return Some(self.default_ttl);
            }
            return None;
        }

        if cacheable_by_default {
            return Some(self.default_ttl);
        }

        None
    }
}

pub(super) struct CachePlan {
    pub(super) key: String,
    pub(super) ttl: Duration,
}

pub(super) struct ResponseCache {
    entries: RwLock<HashMap<String, CachedResponse>>,
    max_capacity: usize,
}

impl ResponseCache {
    pub(super) fn new(max_capacity: u64) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            max_capacity: max_capacity.max(1) as usize,
        }
    }

    pub(super) async fn get(&self, key: &str) -> Option<CachedResponse> {
        let now = now_unix_ms();

        {
            let entries = self.entries.read().await;
            if let Some(value) = entries.get(key)
                && value.expires_at_unix_ms > now
            {
                return Some(value.clone());
            }
        }

        let mut entries = self.entries.write().await;
        if let Some(value) = entries.get(key)
            && value.expires_at_unix_ms <= now
        {
            entries.remove(key);
        }

        None
    }

    pub(super) async fn insert(&self, key: String, body: Bytes, provider: String, ttl: Duration) {
        let mut entries = self.entries.write().await;
        let now = now_unix_ms();

        if entries.len() >= self.max_capacity {
            entries.retain(|_, existing| existing.expires_at_unix_ms > now);

            if entries.len() >= self.max_capacity
                && let Some(oldest_key) = entries
                    .iter()
                    .min_by_key(|(_, existing)| existing.cached_at_unix_ms)
                    .map(|(entry_key, _)| entry_key.clone())
            {
                entries.remove(&oldest_key);
            }
        }

        entries.insert(
            key,
            CachedResponse {
                body,
                provider,
                cached_at_unix_ms: now,
                expires_at_unix_ms: now.saturating_add(ttl.as_millis()),
            },
        );
    }
}

#[derive(Clone)]
pub(super) struct CachedResponse {
    pub(super) body: Bytes,
    pub(super) provider: String,
    pub(super) cached_at_unix_ms: u128,
    expires_at_unix_ms: u128,
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use bytes::Bytes;
    use tokio::time;

    use super::{CachePolicy, ResponseCache};
    use crate::settings::CacheConfig;

    #[test]
    fn cache_policy_uses_per_method_ttl() {
        let mut method_ttl_secs = HashMap::new();
        method_ttl_secs.insert("getSlot".to_string(), 33);
        let policy = CachePolicy::from_config(&CacheConfig {
            enabled: true,
            ttl_secs: 2,
            max_capacity: 100,
            cacheable_methods: vec![],
            method_ttl_secs,
        });

        let plan = policy
            .plan(
                br#"{"jsonrpc":"2.0","method":"getSlot","params":[]}"#,
                Some("GETSLOT"),
                true,
            )
            .expect("cache plan should be present");
        assert_eq!(plan.ttl, Duration::from_secs(33));
    }

    #[test]
    fn cache_policy_honors_allowlist_with_default_ttl() {
        let policy = CachePolicy::from_config(&CacheConfig {
            enabled: true,
            ttl_secs: 5,
            max_capacity: 100,
            cacheable_methods: vec!["getSlot".to_string()],
            method_ttl_secs: HashMap::new(),
        });

        assert!(
            policy
                .plan(
                    br#"{"jsonrpc":"2.0","method":"getSlot","params":[]}"#,
                    Some("getSlot"),
                    false,
                )
                .is_some()
        );
        assert!(
            policy
                .plan(
                    br#"{"jsonrpc":"2.0","method":"getBalance","params":["11111111111111111111111111111111"]}"#,
                    Some("getBalance")
                    ,
                    true,
                )
                .is_none()
        );
    }

    #[test]
    fn cache_policy_zero_ttl_override_disables_method_cache() {
        let mut method_ttl_secs = HashMap::new();
        method_ttl_secs.insert("getBalance".to_string(), 0);

        let policy = CachePolicy::from_config(&CacheConfig {
            enabled: true,
            ttl_secs: 5,
            max_capacity: 100,
            cacheable_methods: vec!["getBalance".to_string()],
            method_ttl_secs,
        });

        assert!(
            policy
                .plan(
                    br#"{"jsonrpc":"2.0","method":"getBalance","params":["11111111111111111111111111111111"]}"#,
                    Some("getBalance")
                    ,
                    true,
                )
                .is_none()
        );
    }

    #[test]
    fn cache_policy_defaults_unknown_method_to_non_cacheable() {
        let policy = CachePolicy::from_config(&CacheConfig {
            enabled: true,
            ttl_secs: 5,
            max_capacity: 100,
            cacheable_methods: vec![],
            method_ttl_secs: HashMap::new(),
        });

        assert!(
            policy
                .plan(
                    br#"{"jsonrpc":"2.0","method":"customExperimentalMethod","params":[]}"#,
                    Some("customExperimentalMethod"),
                    false,
                )
                .is_none()
        );
    }

    #[tokio::test]
    async fn response_cache_expires_entries_by_ttl() {
        let cache = ResponseCache::new(10);
        cache
            .insert(
                "k".to_string(),
                Bytes::from_static(br#"{"ok":true}"#),
                "provider-a".to_string(),
                Duration::from_millis(10),
            )
            .await;

        assert!(cache.get("k").await.is_some());
        time::sleep(Duration::from_millis(20)).await;
        assert!(cache.get("k").await.is_none());
    }

    #[tokio::test]
    async fn response_cache_evicts_oldest_when_full() {
        let cache = ResponseCache::new(2);
        cache
            .insert(
                "a".to_string(),
                Bytes::from_static(br#"{"ok":1}"#),
                "provider-a".to_string(),
                Duration::from_secs(10),
            )
            .await;
        time::sleep(Duration::from_millis(2)).await;
        cache
            .insert(
                "b".to_string(),
                Bytes::from_static(br#"{"ok":2}"#),
                "provider-a".to_string(),
                Duration::from_secs(10),
            )
            .await;
        time::sleep(Duration::from_millis(2)).await;
        cache
            .insert(
                "c".to_string(),
                Bytes::from_static(br#"{"ok":3}"#),
                "provider-a".to_string(),
                Duration::from_secs(10),
            )
            .await;

        assert!(cache.get("a").await.is_none());
        assert!(cache.get("b").await.is_some());
        assert!(cache.get("c").await.is_some());
    }
}
