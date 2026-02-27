use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use serde_json::Value;

#[derive(Clone)]
pub(super) struct ConsensusCandidate {
    pub provider: String,
    pub body: Bytes,
    pub provider_index: usize,
    pub latency: Duration,
}

pub(super) struct ConsensusDecision {
    pub winner: ConsensusCandidate,
    pub agreement: usize,
    pub participants: usize,
    pub majority: bool,
}

pub(super) fn decide_consensus(
    method: Option<&str>,
    candidates: Vec<ConsensusCandidate>,
) -> Option<ConsensusDecision> {
    if candidates.is_empty() {
        return None;
    }

    let participants = candidates.len();
    let mut grouped: HashMap<String, Vec<ConsensusCandidate>> = HashMap::new();

    for candidate in candidates {
        let fingerprint = consensus_fingerprint(method, &candidate.body)?;
        grouped.entry(fingerprint).or_default().push(candidate);
    }

    let mut winning_group = grouped.into_iter().max_by_key(|(_, group)| group.len())?.1;
    let agreement = winning_group.len();
    winning_group.sort_by_key(|candidate| candidate.latency);
    let winner = winning_group.into_iter().next()?;

    Some(ConsensusDecision {
        winner,
        agreement,
        participants,
        majority: agreement > participants / 2,
    })
}

fn consensus_fingerprint(method: Option<&str>, body: &[u8]) -> Option<String> {
    let payload: Value = serde_json::from_slice(body).ok()?;
    let method = method.unwrap_or_default().to_ascii_lowercase();

    let selected = match method.as_str() {
        "getbalance" => payload
            .pointer("/result/value")
            .cloned()
            .or_else(|| payload.get("result").cloned())
            .or_else(|| payload.get("error").cloned())?,
        "gettokenaccountbalance" => payload
            .pointer("/result/value/amount")
            .cloned()
            .or_else(|| payload.pointer("/result/value").cloned())
            .or_else(|| payload.get("result").cloned())
            .or_else(|| payload.get("error").cloned())?,
        "getlatestblockhash" => payload
            .pointer("/result/value/blockhash")
            .cloned()
            .or_else(|| payload.get("result").cloned())
            .or_else(|| payload.get("error").cloned())?,
        "getblock" => payload
            .pointer("/result/blockhash")
            .cloned()
            .or_else(|| payload.get("result").cloned())
            .or_else(|| payload.get("error").cloned())?,
        "isblockhashvalid" => payload
            .pointer("/result/value")
            .cloned()
            .or_else(|| payload.get("result").cloned())
            .or_else(|| payload.get("error").cloned())?,
        _ => payload
            .get("result")
            .cloned()
            .or_else(|| payload.get("error").cloned())?,
    };

    serde_json::to_string(&selected).ok()
}

#[cfg(test)]
mod tests {
    use super::{ConsensusCandidate, decide_consensus};
    use bytes::Bytes;
    use std::time::Duration;

    fn candidate(provider: &str, body: &str, latency_ms: u64) -> ConsensusCandidate {
        ConsensusCandidate {
            provider: provider.to_string(),
            body: Bytes::from(body.to_string()),
            provider_index: 0,
            latency: Duration::from_millis(latency_ms),
        }
    }

    #[test]
    fn balances_agree_even_with_different_context_slot() {
        let decision = decide_consensus(
            Some("getBalance"),
            vec![
                candidate(
                    "a",
                    r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":99},"value":500}}"#,
                    20,
                ),
                candidate(
                    "b",
                    r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":100},"value":500}}"#,
                    10,
                ),
                candidate(
                    "c",
                    r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":100},"value":777}}"#,
                    5,
                ),
            ],
        )
        .expect("consensus should produce decision");

        assert!(decision.majority);
        assert_eq!(decision.agreement, 2);
        assert_eq!(decision.participants, 3);
        assert_eq!(decision.winner.provider, "b");
    }

    #[test]
    fn no_majority_detected_when_all_results_differ() {
        let decision = decide_consensus(
            Some("getBalance"),
            vec![
                candidate(
                    "a",
                    r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":99},"value":1}}"#,
                    20,
                ),
                candidate(
                    "b",
                    r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":100},"value":2}}"#,
                    10,
                ),
                candidate(
                    "c",
                    r#"{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":101},"value":3}}"#,
                    5,
                ),
            ],
        )
        .expect("decision should still be produced");

        assert!(!decision.majority);
        assert_eq!(decision.agreement, 1);
    }
}
