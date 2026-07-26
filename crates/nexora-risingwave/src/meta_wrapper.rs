//! RisingWave Meta node wrapper.
//!
//! Phase 3: Simplified implementation with placeholder logic.
//! Phase 4: Full integration with vendor/risingwave Meta node.

use crate::error::{Result, RisingWaveError};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Wrapper around RisingWave Meta node.
///
/// The Meta node is responsible for:
/// - Cluster metadata management
/// - DDL execution coordination
/// - Catalog management
/// - Leader election (in HA mode)
pub struct MetaNode {
    addr: SocketAddr,
    state: Arc<RwLock<MetaState>>,
}

#[derive(Debug)]
struct MetaState {
    running: bool,
    is_leader: bool,
}

impl MetaNode {
    /// Create a new Meta node wrapper.
    ///
    /// # Arguments
    ///
    /// - `addr`: The address to bind the Meta node to
    ///
    /// # Example
    ///
    /// ```rust
    /// use nexora_risingwave::meta_wrapper::MetaNode;
    ///
    /// let meta = MetaNode::new("127.0.0.1:5690".parse().unwrap());
    /// ```
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            state: Arc::new(RwLock::new(MetaState {
                running: false,
                is_leader: true, // Single node is always leader
            })),
        }
    }

    /// Start the Meta node.
    ///
    /// In Phase 3, this is a simplified implementation that just marks
    /// the node as running. Phase 4 will start the actual RisingWave
    /// Meta node process.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if the Meta node started successfully
    /// - `Err(_)` if startup failed
    pub async fn start(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if state.running {
            return Err(RisingWaveError::MetaStartFailed(
                "meta node already running".to_string(),
            ));
        }

        info!("Starting RisingWave Meta node on {}", self.addr);

        // Phase 3: Placeholder
        // Phase 4: Will start actual Meta node:
        //   - Initialize RisingWave MetaService
        //   - Start gRPC server
        //   - Initialize catalog
        //   - Start leader election (if HA enabled)

        state.running = true;
        Ok(())
    }

    /// Stop the Meta node gracefully.
    pub async fn stop(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if !state.running {
            return Ok(());
        }

        info!("Stopping RisingWave Meta node");

        // Phase 3: Placeholder
        // Phase 4: Will stop actual Meta node

        state.running = false;
        Ok(())
    }

    /// Check if this Meta node is the leader.
    ///
    /// In single-node mode, always returns true.
    /// In HA mode, returns true only if this node won the election.
    pub async fn is_leader(&self) -> bool {
        self.state.read().await.is_leader
    }

    /// Get the Meta node address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Check if the Meta node is running.
    pub async fn is_running(&self) -> bool {
        self.state.read().await.running
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_meta_lifecycle() {
        let meta = MetaNode::new("127.0.0.1:15690".parse().unwrap());

        assert!(!meta.is_running().await);
        assert_eq!(meta.addr().port(), 15690);

        meta.start().await.unwrap();
        assert!(meta.is_running().await);
        assert!(meta.is_leader().await);

        meta.stop().await.unwrap();
        assert!(!meta.is_running().await);
    }

    #[tokio::test]
    async fn test_meta_double_start() {
        let meta = MetaNode::new("127.0.0.1:15691".parse().unwrap());

        meta.start().await.unwrap();
        let result = meta.start().await;
        assert!(result.is_err());
    }
}
