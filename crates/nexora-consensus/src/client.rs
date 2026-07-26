//! Core consensus client trait.

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Result;
use crate::types::{LogIndex, NodeId};

/// Abstraction over consensus protocols for distributed coordination.
///
/// This trait provides a unified interface for consensus operations,
/// primarily targeting leader election and replicated log commitment.
///
/// # Implementation Notes
///
/// - All write operations (like [`commit`](Self::commit)) require the node to be the leader
/// - Leadership can change at any time due to network partitions or failures
/// - Callers should handle [`ConsensusError::NotLeader`](crate::ConsensusError::NotLeader)
///   and retry on the new leader
#[async_trait]
pub trait ConsensusClient: Send + Sync {
    /// Check if this node is currently the leader.
    ///
    /// # Returns
    ///
    /// - `Ok(true)` if this node is the leader
    /// - `Ok(false)` if another node is the leader
    /// - `Err(_)` if leadership status cannot be determined
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use nexora_consensus::ConsensusClient;
    /// # async fn example(client: &impl ConsensusClient) -> Result<(), Box<dyn std::error::Error>> {
    /// if client.is_leader().await? {
    ///     println!("I am the leader!");
    /// } else {
    ///     println!("I am a follower");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    async fn is_leader(&self) -> Result<bool>;

    /// Get the current leader's node ID, if known.
    ///
    /// # Returns
    ///
    /// - `Some(node_id)` if a leader is known
    /// - `None` if no leader is currently elected or known
    async fn current_leader(&self) -> Result<Option<NodeId>>;

    /// Commit data to the replicated log.
    ///
    /// This operation is only valid on the leader node. If called on a follower,
    /// it returns [`ConsensusError::NotLeader`](crate::ConsensusError::NotLeader).
    ///
    /// # Arguments
    ///
    /// - `data`: Arbitrary bytes to be replicated across the cluster
    ///
    /// # Returns
    ///
    /// The log index at which the data was committed.
    ///
    /// # Errors
    ///
    /// - [`ConsensusError::NotLeader`](crate::ConsensusError::NotLeader) if this node is not the leader
    /// - [`ConsensusError::Raft`](crate::ConsensusError::Raft) if the Raft operation fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use nexora_consensus::ConsensusClient;
    /// # use bytes::Bytes;
    /// # async fn example(client: &impl ConsensusClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let data = Bytes::from("important data");
    /// let log_index = client.commit(data).await?;
    /// println!("Committed at index: {}", log_index);
    /// # Ok(())
    /// # }
    /// ```
    async fn commit(&self, data: Bytes) -> Result<LogIndex>;

    /// Get this node's ID in the cluster.
    fn node_id(&self) -> NodeId;

    /// Shutdown the consensus node gracefully.
    ///
    /// This should stop all background tasks and close network connections.
    async fn shutdown(&self) -> Result<()>;
}
