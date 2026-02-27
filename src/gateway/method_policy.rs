use std::collections::HashMap;

use crate::settings::{MethodPolicyConfig, MethodPolicyOverride};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SolanaMethodPolicy {
    pub known: bool,
    pub cacheable_by_default: bool,
    pub consensus_critical: bool,
}

impl SolanaMethodPolicy {
    pub const fn unknown() -> Self {
        Self {
            known: false,
            cacheable_by_default: false,
            consensus_critical: false,
        }
    }

    pub const fn cacheable(consensus_critical: bool) -> Self {
        Self {
            known: true,
            cacheable_by_default: true,
            consensus_critical,
        }
    }

    pub const fn non_cacheable(consensus_critical: bool) -> Self {
        Self {
            known: true,
            cacheable_by_default: false,
            consensus_critical,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SolanaMethodPolicyTable {
    overrides: HashMap<String, SolanaMethodPolicy>,
}

impl SolanaMethodPolicyTable {
    pub fn from_config(config: &MethodPolicyConfig) -> Self {
        let overrides = config
            .overrides
            .iter()
            .map(|(method, policy_override)| {
                (
                    method.to_ascii_lowercase(),
                    apply_override(solana_method_policy(method), policy_override),
                )
            })
            .collect();

        Self { overrides }
    }

    pub fn policy_for(&self, method: &str) -> SolanaMethodPolicy {
        let method_lowercase = method.to_ascii_lowercase();
        self.overrides
            .get(&method_lowercase)
            .copied()
            .unwrap_or_else(|| solana_method_policy(method))
    }

    pub fn policy_for_opt(&self, method: Option<&str>) -> SolanaMethodPolicy {
        method
            .map(|method_name| self.policy_for(method_name))
            .unwrap_or_else(SolanaMethodPolicy::unknown)
    }
}

fn apply_override(
    base: SolanaMethodPolicy,
    policy_override: &MethodPolicyOverride,
) -> SolanaMethodPolicy {
    SolanaMethodPolicy {
        known: base.known
            || policy_override.cacheable_by_default.is_some()
            || policy_override.consensus_critical.is_some(),
        cacheable_by_default: policy_override
            .cacheable_by_default
            .unwrap_or(base.cacheable_by_default),
        consensus_critical: policy_override
            .consensus_critical
            .unwrap_or(base.consensus_critical),
    }
}

pub fn solana_method_policy(method: &str) -> SolanaMethodPolicy {
    match method.to_ascii_lowercase().as_str() {
        // Non-mutating methods safe to cache briefly.
        "gethealth"
        | "getversion"
        | "getslot"
        | "getslotleader"
        | "getblockheight"
        | "getepochinfo"
        | "getepochschedule"
        | "getgenesishash"
        | "getidentity"
        | "getclusternodes"
        | "getsupply"
        | "gettokensupply"
        | "getsignaturestatuses"
        | "getsignaturesforaddress"
        | "getrecentprioritizationfees"
        | "getminimumbalanceforrentexemption" => SolanaMethodPolicy::cacheable(false),

        // Critical data paths often used by bots/wallets where correctness matters.
        "getbalance"
        | "getaccountinfo"
        | "getmultipleaccounts"
        | "getprogramaccounts"
        | "gettokenaccountbalance"
        | "getlatestblockhash"
        | "getblock"
        | "gettransaction"
        | "isblockhashvalid" => SolanaMethodPolicy::cacheable(true),

        // Mutating or highly dynamic methods should not be cached by default.
        "sendtransaction" | "sendrawtransaction" | "requestairdrop" | "simulatetransaction" => {
            SolanaMethodPolicy::non_cacheable(false)
        }

        _ => SolanaMethodPolicy::unknown(),
    }
}

pub fn solana_method_policy_opt(method: Option<&str>) -> SolanaMethodPolicy {
    method
        .map(solana_method_policy)
        .unwrap_or_else(SolanaMethodPolicy::unknown)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{SolanaMethodPolicyTable, solana_method_policy, solana_method_policy_opt};
    use crate::settings::{MethodPolicyConfig, MethodPolicyOverride};

    #[test]
    fn known_read_methods_are_cacheable() {
        let policy = solana_method_policy("getSlot");
        assert!(policy.known);
        assert!(policy.cacheable_by_default);
        assert!(!policy.consensus_critical);
    }

    #[test]
    fn critical_methods_are_marked_consensus_critical() {
        let policy = solana_method_policy("getBalance");
        assert!(policy.known);
        assert!(policy.cacheable_by_default);
        assert!(policy.consensus_critical);
    }

    #[test]
    fn write_methods_are_not_cacheable() {
        let policy = solana_method_policy("sendTransaction");
        assert!(policy.known);
        assert!(!policy.cacheable_by_default);
        assert!(!policy.consensus_critical);
    }

    #[test]
    fn unknown_methods_default_to_safe_non_cacheable() {
        let policy = solana_method_policy("customExperimentalMethod");
        assert!(!policy.known);
        assert!(!policy.cacheable_by_default);
        assert!(!policy.consensus_critical);
    }

    #[test]
    fn missing_method_defaults_to_safe_non_cacheable() {
        let policy = solana_method_policy_opt(None);
        assert!(!policy.known);
        assert!(!policy.cacheable_by_default);
        assert!(!policy.consensus_critical);
    }

    #[test]
    fn policy_table_overrides_known_method() {
        let mut overrides = HashMap::new();
        overrides.insert(
            "getBalance".to_string(),
            MethodPolicyOverride {
                cacheable_by_default: Some(false),
                consensus_critical: Some(false),
            },
        );
        let table = SolanaMethodPolicyTable::from_config(&MethodPolicyConfig { overrides });

        let policy = table.policy_for("getBalance");
        assert!(policy.known);
        assert!(!policy.cacheable_by_default);
        assert!(!policy.consensus_critical);
    }

    #[test]
    fn policy_table_can_classify_unknown_method() {
        let mut overrides = HashMap::new();
        overrides.insert(
            "customMethod".to_string(),
            MethodPolicyOverride {
                cacheable_by_default: Some(true),
                consensus_critical: Some(true),
            },
        );
        let table = SolanaMethodPolicyTable::from_config(&MethodPolicyConfig { overrides });

        let policy = table.policy_for("customMethod");
        assert!(policy.known);
        assert!(policy.cacheable_by_default);
        assert!(policy.consensus_critical);
    }

    #[test]
    fn policy_table_lookup_is_case_insensitive() {
        let mut overrides = HashMap::new();
        overrides.insert(
            "customMethod".to_string(),
            MethodPolicyOverride {
                cacheable_by_default: Some(true),
                consensus_critical: Some(false),
            },
        );
        let table = SolanaMethodPolicyTable::from_config(&MethodPolicyConfig { overrides });

        let policy = table.policy_for("CuStOmMeThOd");
        assert!(policy.known);
        assert!(policy.cacheable_by_default);
        assert!(!policy.consensus_critical);
    }

    #[test]
    fn policy_table_defaults_to_builtin_policy_without_override() {
        let table = SolanaMethodPolicyTable::from_config(&MethodPolicyConfig::default());
        let policy = table.policy_for("getSlot");
        assert!(policy.known);
        assert!(policy.cacheable_by_default);
        assert!(!policy.consensus_critical);
    }
}
