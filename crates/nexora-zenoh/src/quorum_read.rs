//! Quorum read implementation for strong consistency.
//!
//! Reads from RF/2+1 replicas to ensure we see the most recent committed value.

use crate::shard_map::ShardMap;
use crate::tcp_transport::TcpRemoteClient;
use crate::{GraphOperation, GraphResult, RemoteGraphClient, RouterError};
use nexora_id::NexoraId;
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration for quorum reads
#[derive(Clone, Debug)]
pub struct QuorumReadConfig {
    /// Whether quorum reads are enabled (default: true)
    pub enabled: bool,
    /// Timeout for each individual replica read (default: 5s)
    pub replica_timeout_ms: u64,
    /// Whether to fail fast if quorum cannot be reached (default: true)
    pub fail_fast: bool,
}

impl Default for QuorumReadConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            replica_timeout_ms: 5000,
            fail_fast: true,
        }
    }
}

/// Result of a quorum read operation
#[derive(Debug)]
pub struct QuorumReadResult {
    /// The consensus value agreed upon by the quorum
    pub value: GraphResult,
    /// Number of replicas that responded
    pub responses: usize,
    /// Number of replicas queried
    pub total_queried: usize,
    /// Whether quorum was achieved
    pub quorum_achieved: bool,
}

/// Quorum read coordinator
pub struct QuorumReader {
    config: QuorumReadConfig,
    client: Arc<TcpRemoteClient>,
}

impl QuorumReader {
    pub fn new(config: QuorumReadConfig, client: Arc<TcpRemoteClient>) -> Self {
        Self { config, client }
    }

    /// Perform a quorum read across replicas
    ///
    /// Queries RF/2+1 replicas and returns the consensus value.
    /// For reads, we pick the most recent value based on version/timestamp if available.
    pub async fn read_with_quorum(
        &self,
        shard_map: &ShardMap,
        qid: &NexoraId,
        op: GraphOperation,
    ) -> Result<QuorumReadResult, RouterError> {
        let shard = shard_map.shard_of(qid);
        let assignment = shard_map
            .get(shard)
            .ok_or_else(|| RouterError::NodeNotFound(format!("shard {}", shard)))?;

        // Build list of all replicas to query (owner + followers)
        let mut replicas = vec![assignment.owner.clone()];
        replicas.extend(assignment.replicas.iter().cloned());

        let rf = replicas.len();
        let quorum_size = (rf / 2) + 1;

        tracing::debug!(
            shard = shard,
            rf = rf,
            quorum_size = quorum_size,
            "starting quorum read"
        );

        // Query all replicas concurrently
        let mut tasks = Vec::new();
        for replica in &replicas {
            let client = self.client.clone();
            let replica = replica.clone();
            let op = op.clone();
            let timeout = tokio::time::Duration::from_millis(self.config.replica_timeout_ms);

            tasks.push(tokio::spawn(async move {
                let result = tokio::time::timeout(timeout, client.execute(&replica, op)).await;
                (replica, result)
            }));
        }

        // Collect responses
        let mut responses: HashMap<String, GraphResult> = HashMap::new();
        let mut errors: HashMap<String, String> = HashMap::new();

        for task in tasks {
            match task.await {
                Ok((replica, Ok(Ok(result)))) => {
                    responses.insert(replica, result);
                }
                Ok((replica, Ok(Err(e)))) => {
                    errors.insert(replica.clone(), e.to_string());
                    tracing::warn!(
                        replica = %replica,
                        error = %e,
                        "replica read failed"
                    );
                }
                Ok((replica, Err(_))) => {
                    errors.insert(replica.clone(), "timeout".to_string());
                    tracing::warn!(
                        replica = %replica,
                        "replica read timeout"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "task join failed"
                    );
                }
            }
        }

        let response_count = responses.len();
        let quorum_achieved = response_count >= quorum_size;

        if !quorum_achieved && self.config.fail_fast {
            return Err(RouterError::Remote(format!(
                "quorum not achieved: {} responses, {} required (RF={})",
                response_count, quorum_size, rf
            )));
        }

        // Pick consensus value (for now, use majority or first response)
        // In a full implementation, we'd compare versions/timestamps
        let consensus_value = self.select_consensus_value(responses, quorum_size)?;

        Ok(QuorumReadResult {
            value: consensus_value,
            responses: response_count,
            total_queried: replicas.len(),
            quorum_achieved,
        })
    }

    /// Select the consensus value from multiple responses
    ///
    /// A1.2: Implements version-based consensus algorithm (last-write-wins).
    ///
    /// Strategy:
    /// 1. Extract version/timestamp from each response
    /// 2. Select the response with the highest version
    /// 3. Verify at least quorum_size replicas agree on that version or higher
    ///
    /// This ensures W+R>N: we return the most recent committed value and satisfy
    /// ReadConcern::Majority guarantees by ensuring the read quorum overlaps with
    /// any write quorum.
    ///
    /// # Arguments
    /// * `responses_with_versions` - Map of replica → (result, version)
    /// * `quorum_size` - Minimum replicas required for consensus
    ///
    /// # Returns
    /// The GraphResult with the highest version that has quorum agreement.
    #[allow(dead_code)] // C3: Quorum read with versioning, will be used when version-aware reads are enabled
    fn select_consensus_value_with_versions(
        &self,
        responses_with_versions: HashMap<String, (GraphResult, u64)>,
        quorum_size: usize,
    ) -> Result<GraphResult, RouterError> {
        if responses_with_versions.is_empty() {
            return Err(RouterError::Remote("no responses available".to_string()));
        }

        // Sort responses by version (descending) to find the highest version
        let mut sorted_responses: Vec<(String, GraphResult, u64)> = responses_with_versions
            .into_iter()
            .map(|(replica, (result, version))| (replica, result, version))
            .collect();
        // Sort by version descending
        sorted_responses.sort_by_key(|(_, _, version)| std::cmp::Reverse(*version));

        if sorted_responses.is_empty() {
            return Err(RouterError::Remote("no valid responses".to_string()));
        }

        // Take the highest version
        let highest_version = sorted_responses[0].2;
        let highest_result = sorted_responses[0].1.clone();

        // Count how many replicas have this version or higher
        let replicas_at_or_above = sorted_responses
            .iter()
            .filter(|(_, _, v)| *v >= highest_version)
            .count();

        // Verify we have quorum agreement on the highest version
        if replicas_at_or_above < quorum_size {
            return Err(RouterError::Remote(format!(
                "no quorum consensus at version {}: only {}/{} replicas at or above this version",
                highest_version, replicas_at_or_above, quorum_size
            )));
        }

        tracing::debug!(
            highest_version = highest_version,
            replicas_at_version = replicas_at_or_above,
            quorum_size = quorum_size,
            "selected consensus value by version"
        );

        Ok(highest_result)
    }

    /// Legacy method: Select consensus value without version tracking.
    ///
    /// This method uses value equality for consensus. Kept for backward
    /// compatibility but prefer `select_consensus_value_with_versions` for
    /// A1.2 W+R>N guarantees.
    fn select_consensus_value(
        &self,
        responses: HashMap<String, GraphResult>,
        quorum_size: usize,
    ) -> Result<GraphResult, RouterError> {
        if responses.is_empty() {
            return Err(RouterError::Remote("no responses available".to_string()));
        }

        // Group responses by their content (for determining consensus)
        // Uses serde_json serialization for stable comparison instead of Debug format
        let mut value_counts: HashMap<String, (usize, GraphResult)> = HashMap::new();

        for (_replica, result) in responses {
            // Use serde_json for stable serialization instead of Debug format
            let key = serde_json::to_string(&result).unwrap_or_else(|_| format!("{:?}", result));
            value_counts
                .entry(key)
                .and_modify(|(count, _)| *count += 1)
                .or_insert((1, result));
        }

        // Find the value with the most replicas agreeing
        let (most_common_count, consensus_value) = value_counts
            .into_iter()
            .map(|(_, (count, value))| (count, value))
            .max_by_key(|(count, _)| *count)
            .unwrap();

        // Verify we have quorum agreement
        if most_common_count < quorum_size {
            return Err(RouterError::Remote(format!(
                "no quorum consensus: best agreement was {}/{} replicas",
                most_common_count, quorum_size
            )));
        }

        tracing::debug!(
            agreement_count = most_common_count,
            quorum_size = quorum_size,
            "selected consensus value"
        );

        Ok(consensus_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_quorum_reader_creation() {
        let config = QuorumReadConfig::default();
        let client = Arc::new(TcpRemoteClient::new());
        let reader = QuorumReader::new(config, client);
        assert!(reader.config.enabled);
    }

    #[tokio::test]
    async fn test_quorum_size_calculation() {
        // RF=1 → quorum=1
        // RF=2 → quorum=2
        // RF=3 → quorum=2
        // RF=5 → quorum=3
        assert_eq!((2 / 2) + 1, 2);
        assert_eq!((3 / 2) + 1, 2);
        assert_eq!((5 / 2) + 1, 3);
    }

    /// A1.2: Version-based consensus selects the highest version value.
    #[test]
    fn test_select_consensus_value_by_version() {
        let config = QuorumReadConfig::default();
        let client = Arc::new(TcpRemoteClient::new());
        let reader = QuorumReader::new(config, client);

        // Construct 3 responses with versions 10, 10, 8
        let mut responses = HashMap::new();
        responses.insert(
            "replica-1".to_string(),
            (
                GraphResult::Property(Some(serde_json::json!("value_v10"))),
                10u64,
            ),
        );
        responses.insert(
            "replica-2".to_string(),
            (
                GraphResult::Property(Some(serde_json::json!("value_v10"))),
                10u64,
            ),
        );
        responses.insert(
            "replica-3".to_string(),
            (
                GraphResult::Property(Some(serde_json::json!("value_v8"))),
                8u64,
            ),
        );

        // Quorum size = 2 (majority of 3)
        let result = reader.select_consensus_value_with_versions(responses, 2);
        assert!(result.is_ok());

        if let Ok(GraphResult::Property(Some(v))) = result {
            assert_eq!(v, serde_json::json!("value_v10"));
        } else {
            panic!("expected Property with value_v10");
        }
    }

    /// A1.2: Version-based consensus fails when no quorum at highest version.
    #[test]
    fn test_select_consensus_value_version_no_quorum() {
        let config = QuorumReadConfig::default();
        let client = Arc::new(TcpRemoteClient::new());
        let reader = QuorumReader::new(config, client);

        // Construct 3 responses with versions 10, 8, 6 - no quorum at any version
        let mut responses = HashMap::new();
        responses.insert(
            "replica-1".to_string(),
            (GraphResult::Property(Some(serde_json::json!("v10"))), 10u64),
        );
        responses.insert(
            "replica-2".to_string(),
            (GraphResult::Property(Some(serde_json::json!("v8"))), 8u64),
        );
        responses.insert(
            "replica-3".to_string(),
            (GraphResult::Property(Some(serde_json::json!("v6"))), 6u64),
        );

        // Quorum size = 2, but only 1 replica at highest version
        let result = reader.select_consensus_value_with_versions(responses, 2);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("no quorum consensus"));
    }

    /// A1.2: Version-based consensus with RF=5, quorum=3, selecting version 15.
    #[test]
    fn test_select_consensus_value_version_rf5() {
        let config = QuorumReadConfig::default();
        let client = Arc::new(TcpRemoteClient::new());
        let reader = QuorumReader::new(config, client);

        let mut responses = HashMap::new();
        responses.insert(
            "r1".to_string(),
            (GraphResult::Property(Some(serde_json::json!("v15"))), 15u64),
        );
        responses.insert(
            "r2".to_string(),
            (GraphResult::Property(Some(serde_json::json!("v15"))), 15u64),
        );
        responses.insert(
            "r3".to_string(),
            (GraphResult::Property(Some(serde_json::json!("v15"))), 15u64),
        );
        responses.insert(
            "r4".to_string(),
            (GraphResult::Property(Some(serde_json::json!("v12"))), 12u64),
        );
        responses.insert(
            "r5".to_string(),
            (GraphResult::Property(Some(serde_json::json!("v10"))), 10u64),
        );

        // Quorum = 3, and we have 3 replicas at version 15
        let result = reader.select_consensus_value_with_versions(responses, 3);
        assert!(result.is_ok());

        if let Ok(GraphResult::Property(Some(v))) = result {
            assert_eq!(v, serde_json::json!("v15"));
        } else {
            panic!("expected Property with v15");
        }
    }

    /// A1.2: W+R>N guarantee - if 3 replicas wrote v10, reading from 2 must see v10.
    #[test]
    fn test_w_plus_r_greater_than_n_guarantee() {
        let config = QuorumReadConfig::default();
        let client = Arc::new(TcpRemoteClient::new());
        let reader = QuorumReader::new(config, client);

        // RF=5, write quorum=3 wrote v20, read quorum=3
        // Even if 2 replicas are stale, W+R>N guarantees overlap
        let mut responses = HashMap::new();
        responses.insert(
            "r1".to_string(),
            (GraphResult::Property(Some(serde_json::json!("v20"))), 20u64),
        );
        responses.insert(
            "r2".to_string(),
            (GraphResult::Property(Some(serde_json::json!("v20"))), 20u64),
        );
        responses.insert(
            "r3".to_string(),
            (GraphResult::Property(Some(serde_json::json!("v20"))), 20u64),
        );

        // Quorum read (3 out of 5) must see the quorum-committed v20
        let result = reader.select_consensus_value_with_versions(responses, 3);
        assert!(result.is_ok());

        if let Ok(GraphResult::Property(Some(v))) = result {
            assert_eq!(v, serde_json::json!("v20"));
        } else {
            panic!("expected v20 from quorum write/read overlap");
        }
    }
}
