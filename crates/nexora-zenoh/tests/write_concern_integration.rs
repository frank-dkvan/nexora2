//! Integration tests for WriteConcern configuration and behavior.
//!
//! Note: Full RF=3 replication with WriteConcern is tested in real_cluster_smoke tests,
//! which set up the complete cluster infrastructure with fencing and replication logs.
//! These tests focus on WriteConcern logic independent of full cluster setup.

use nexora_id::NexoraId;
use nexora_zenoh::{GraphOperation, ReplicaWriter, RouterError, TcpRemoteClient, WriteConcern};
use std::sync::Arc;
use std::time::Duration;

use nexora_zenoh::replication::{FencingToken, ReplicaSet};
use nexora_zenoh::shard_map::OwnerEpoch;

/// Test that WriteConcern::All requires all replicas to acknowledge
#[tokio::test]
async fn test_write_concern_all_requires_all_replicas() {
    let client = Arc::new(TcpRemoteClient::new());
    let writer = ReplicaWriter::new(client.clone())
        .with_write_concern(WriteConcern::All)
        .with_timeout(Duration::from_millis(500));

    let replica_set = ReplicaSet::new(
        0,
        "127.0.0.1:9990".to_string(),
        vec!["127.0.0.1:9991".to_string(), "127.0.0.1:9992".to_string()],
    );
    writer.register_replica_set(replica_set);

    let qid = NexoraId::from_bytes(b"test".to_vec());
    let op = GraphOperation::SetProperty {
        qid,
        key: "key".into(),
        value: serde_json::json!(42),
    };

    let token = FencingToken::new(0, OwnerEpoch::new());
    let result = writer.quorum_write(0, &token, op).await;

    // All followers are unreachable, so should fail (requires 3/3 but only owner = 1).
    // Quorum failure surfaces as Err(RouterError::QuorumFailed) rather than Ok(Failed).
    match result {
        Err(RouterError::QuorumFailed { acked, required }) => {
            assert_eq!(required, 3); // WriteConcern::All requires all 3
            assert_eq!(acked, 1); // Only owner acked
        }
        other => panic!("expected Err(QuorumFailed), got {:?}", other),
    }
}

/// Test that WriteConcern::One succeeds with only owner
#[tokio::test]
async fn test_write_concern_one_succeeds_with_only_owner() {
    let client = Arc::new(TcpRemoteClient::new());
    let writer = ReplicaWriter::new(client.clone())
        .with_write_concern(WriteConcern::One)
        .with_timeout(Duration::from_millis(500));

    let replica_set = ReplicaSet::new(
        0,
        "127.0.0.1:9993".to_string(),
        vec!["127.0.0.1:9994".to_string(), "127.0.0.1:9995".to_string()],
    );
    writer.register_replica_set(replica_set);

    let qid = NexoraId::from_bytes(b"test".to_vec());
    let op = GraphOperation::SetProperty {
        qid,
        key: "key".into(),
        value: serde_json::json!("value"),
    };

    let token = FencingToken::new(0, OwnerEpoch::new());
    let status = writer.quorum_write(0, &token, op).await.unwrap();

    // WriteConcern::One should succeed with only owner (1/3)
    match status {
        nexora_zenoh::replication::WriteStatus::CommittedQuorum { acked, total } => {
            assert_eq!(total, 3);
            assert_eq!(acked, 1); // Only owner
        }
        other => panic!("expected CommittedQuorum, got {:?}", other),
    }
}

/// Test that WriteConcern::Majority requires majority of replicas
#[tokio::test]
async fn test_write_concern_majority_requires_two_of_three() {
    let client = Arc::new(TcpRemoteClient::new());
    let writer = ReplicaWriter::new(client.clone())
        .with_write_concern(WriteConcern::Majority)
        .with_timeout(Duration::from_millis(500));

    let replica_set = ReplicaSet::new(
        0,
        "127.0.0.1:9996".to_string(),
        vec!["127.0.0.1:9997".to_string(), "127.0.0.1:9998".to_string()],
    );
    writer.register_replica_set(replica_set);

    let qid = NexoraId::from_bytes(b"test".to_vec());
    let op = GraphOperation::SetProperty {
        qid,
        key: "key".into(),
        value: serde_json::json!(42),
    };

    let token = FencingToken::new(0, OwnerEpoch::new());
    let result = writer.quorum_write(0, &token, op).await;

    // All followers unreachable, so should fail (requires 2/3 but only owner = 1).
    // Quorum failure surfaces as Err(RouterError::QuorumFailed) rather than Ok(Failed).
    match result {
        Err(RouterError::QuorumFailed { acked, required }) => {
            assert_eq!(required, 2); // WriteConcern::Majority requires 2 out of 3
            assert_eq!(acked, 1); // Only owner acked
        }
        other => panic!("expected Err(QuorumFailed), got {:?}", other),
    }
}

/// Test WriteConcern::min_acks calculation
#[test]
fn test_write_concern_min_acks_calculation() {
    // Majority of 3 = 2
    assert_eq!(WriteConcern::Majority.min_acks(3), 2);

    // Majority of 5 = 3
    assert_eq!(WriteConcern::Majority.min_acks(5), 3);

    // All of 3 = 3
    assert_eq!(WriteConcern::All.min_acks(3), 3);

    // One of 3 = 1
    assert_eq!(WriteConcern::One.min_acks(3), 1);

    // Majority of 1 = 1
    assert_eq!(WriteConcern::Majority.min_acks(1), 1);
}
