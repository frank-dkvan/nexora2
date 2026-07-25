//! Regression: `on_property_change` is edge-triggered, so a double trigger for
//! the same node is idempotent — no double count, no duplicate broadcast.
//!
//! This guards a real correctness property the distributed PG-wire write path
//! relies on: on the coordinator node, a locally-owned node can be evaluated
//! twice for the same write — once by the GraphService mutation callback, once
//! by the cluster write path's `trigger_standing_queries_distributed`. Because
//! the manager only emits on a state *transition*, the second call is a no-op,
//! so the double trigger neither double-counts matches nor double-broadcasts.

use nexora_id::{NexoraId, PropertyValue};
use nexora_standing_query::pattern::{FilterCondition, StandingQueryPattern};
use nexora_standing_query::{ResultType, StandingQueryManager};
use std::collections::HashMap;

#[tokio::test]
async fn double_trigger_same_state_is_idempotent() {
    let m = StandingQueryManager::new(64);
    let sq = m
        .register(
            "vip",
            StandingQueryPattern::property(
                "vip",
                FilterCondition::Equals(PropertyValue::Boolean(true)),
            ),
        )
        .await;
    let mut rx = m.subscribe();

    let qid = NexoraId::from_bytes(b"n1".to_vec());
    let mut props = HashMap::new();
    props.insert("vip".to_string(), PropertyValue::Boolean(true));

    // First trigger: transitions to matching → one Matched broadcast.
    let a = m
        .on_property_change(&qid, "vip", &PropertyValue::Boolean(true), &props)
        .await;
    // Second trigger (same state): no transition → no broadcast, count unchanged.
    let b = m
        .on_property_change(&qid, "vip", &PropertyValue::Boolean(true), &props)
        .await;

    assert_eq!(a, 1, "first trigger matches");
    assert_eq!(b, 0, "repeat trigger for same state emits nothing");
    assert_eq!(m.match_count(sq).await, 1, "match count is 1, not 2");

    // Exactly one Matched result on the broadcast channel.
    let first = rx.try_recv().expect("one broadcast");
    assert!(matches!(first.result_type, ResultType::Matched));
    assert!(rx.try_recv().is_err(), "no second broadcast for the repeat");
}
