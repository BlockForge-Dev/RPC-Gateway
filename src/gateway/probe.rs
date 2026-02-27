use anyhow::Context;
use bytes::Bytes;
use serde_json::json;

use crate::settings::ProbeConfig;

pub(super) fn build_probe_payload(probe: &ProbeConfig) -> anyhow::Result<Bytes> {
    let payload = json!({
        "jsonrpc": "2.0",
        "id": "rpc-gateway-probe",
        "method": probe.method,
        "params": probe.params,
    });

    let body = serde_json::to_vec(&payload).context("failed to serialize probe payload")?;
    Ok(Bytes::from(body))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn builds_probe_payload_with_method_and_params() {
        let probe = ProbeConfig {
            enabled: true,
            interval_secs: 10,
            timeout_ms: 800,
            method: "getHealth".to_string(),
            params: json!([]),
        };

        let payload = build_probe_payload(&probe).expect("probe payload should serialize");
        let value: serde_json::Value =
            serde_json::from_slice(&payload).expect("probe payload should be valid JSON");

        assert_eq!(
            value.get("method").and_then(serde_json::Value::as_str),
            Some("getHealth")
        );
        assert_eq!(value.get("params"), Some(&json!([])));
    }
}
