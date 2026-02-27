use serde_json::Value;

use super::method_policy::solana_method_policy_opt;

pub fn extract_rpc_method(body: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(body).ok()?;

    match value {
        Value::Object(object) => object
            .get("method")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        Value::Array(mut requests) => requests.drain(..).find_map(|item| {
            item.get("method")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        }),
        _ => None,
    }
}

pub fn should_cache_hint(method: Option<&str>) -> bool {
    solana_method_policy_opt(method).cacheable_by_default
}

pub fn is_consensus_critical_method(method: Option<&str>) -> bool {
    solana_method_policy_opt(method).consensus_critical
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_method_from_single_request() {
        let body = br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#;
        assert_eq!(extract_rpc_method(body).as_deref(), Some("getSlot"));
    }

    #[test]
    fn extracts_method_from_batch() {
        let body = br#"[{"jsonrpc":"2.0","id":1,"method":"getBalance","params":["Fj9s..."]},{"jsonrpc":"2.0","id":2,"method":"getSlot","params":[]}]"#;
        assert_eq!(extract_rpc_method(body).as_deref(), Some("getBalance"));
    }

    #[test]
    fn method_hint_detects_write_calls() {
        assert!(!should_cache_hint(Some("sendTransaction")));
        assert!(!should_cache_hint(Some("requestAirdrop")));
        assert!(should_cache_hint(Some("getBalance")));
        assert!(!should_cache_hint(None));
        assert!(is_consensus_critical_method(Some("getBalance")));
        assert!(!is_consensus_critical_method(Some("getSlot")));
    }
}
