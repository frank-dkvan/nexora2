//! Zenoh liveliness-based node discovery.
//!
//! Uses Zenoh's built-in liveliness tokens for automatic node discovery
//! and failure detection, replacing the custom TCP heartbeat protocol.
//!
//! Key expression design:
//!   - `cluster/node/{node_id}/alive` — liveliness token per node
//!   - `cluster/node/**` — subscriber pattern to discover all nodes
//!
//! When a node starts, it declares a liveliness token. Other nodes
//! subscribed to `cluster/node/**` receive:
//!   - `SampleKind::Put` → node is alive
//!   - `SampleKind::Delete` → node is dead (dropped token or crash)

#![cfg(feature = "zenoh")]

use crate::discovery::{ClusterRegistry, NodeInfo};
use crate::RouterError;
use std::sync::Arc;
use zenoh::key_expr::KeyExpr;
use zenoh::sample::SampleKind;
use zenoh::Session;

/// Key expression prefix for cluster liveliness.
const CLUSTER_NODE_PREFIX: &str = "cluster/node";

/// Build the liveliness key expression for a node.
fn node_alive_key(node_id: &str) -> String {
    format!("{CLUSTER_NODE_PREFIX}/{node_id}/alive")
}

/// Extract node_id from a liveliness key expression.
/// Expected format: `cluster/node/{node_id}/alive`
fn extract_node_id(key: &str) -> Option<String> {
    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() == 4 && parts[0] == "cluster" && parts[1] == "node" && parts[3] == "alive" {
        Some(parts[2].to_string())
    } else {
        None
    }
}

/// Zenoh-based node discovery using liveliness tokens.
///
/// Each node:
/// 1. Declares a liveliness token on `cluster/node/{node_id}/alive`
/// 2. Subscribes to `cluster/node/**` to detect other nodes
///
/// When a liveliness token is dropped (node crash or graceful shutdown),
/// the subscriber receives a `SampleKind::Delete` event, triggering failover.
pub struct ZenohDiscovery {
    session: Arc<Session>,
    node_id: String,
    /// The liveliness token, kept alive to maintain presence.
    _token: Option<zenoh::liveliness::LivelinessToken>,
    /// The subscriber task handle.
    _subscriber_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Callback type for node failure events.
pub type FailoverCallback = Arc<dyn Fn(String) + Send + Sync>;

impl ZenohDiscovery {
    /// Create new discovery for a node.
    pub fn new(session: Arc<Session>, node_id: String) -> Self {
        Self {
            session,
            node_id,
            _token: None,
            _subscriber_handle: None,
        }
    }

    /// Start discovery: declare liveliness token and subscribe to peer events.
    ///
    /// `registry` is updated as nodes come and go.
    /// `on_failure` is called when a node's liveliness token is dropped.
    pub async fn start(
        &mut self,
        registry: Arc<ClusterRegistry>,
        on_failure: FailoverCallback,
    ) -> Result<(), RouterError> {
        // 1. Declare liveliness token for this node
        let key = node_alive_key(&self.node_id);
        tracing::info!("Declaring liveliness token on '{key}'");
        let token = self
            .session
            .liveliness()
            .declare_token(
                KeyExpr::try_from(&key as &str)
                    .map_err(|e| RouterError::Remote(format!("zenoh key expr: {e}")))?,
            )
            .await
            .map_err(|e| RouterError::Remote(format!("zenoh liveliness token: {e}")))?;

        self._token = Some(token);

        // 2. Subscribe to all node liveliness events
        let sub_key = format!("{CLUSTER_NODE_PREFIX}/**");
        tracing::info!("Subscribing to liveliness on '{sub_key}'");

        let subscriber = self
            .session
            .liveliness()
            .declare_subscriber(
                KeyExpr::try_from(sub_key.as_str())
                    .map_err(|e| RouterError::Remote(format!("zenoh key expr: {e}")))?,
            )
            .history(true)
            .await
            .map_err(|e| RouterError::Remote(format!("zenoh liveliness subscriber: {e}")))?;

        let self_node_id = self.node_id.clone();
        self._subscriber_handle = Some(tokio::spawn(async move {
            while let Ok(sample) = subscriber.recv_async().await {
                let key_str = sample.key_expr().as_str();
                let Some(node_id) = extract_node_id(key_str) else {
                    continue;
                };

                match sample.kind() {
                    SampleKind::Put => {
                        tracing::info!("Node alive: {node_id}");
                        registry
                            .register(NodeInfo {
                                id: node_id.clone(),
                                address: String::new(), // address learned via ShardMap
                                roles: vec!["compute".into(), "voter".into()],
                                last_heartbeat_ms: current_millis(),
                                alive: true,
                            })
                            .await;
                    }
                    SampleKind::Delete => {
                        if node_id == self_node_id {
                            // Skip self-delete events
                            continue;
                        }
                        tracing::warn!("Node failed: {node_id}");
                        registry.mark_dead(&node_id).await;
                        on_failure(node_id);
                    }
                }
            }
        }));

        Ok(())
    }

    /// Stop discovery: drop liveliness token and cancel subscriber.
    ///
    /// Dropping the liveliness token causes other nodes to receive a
    /// `SampleKind::Delete` event, triggering failover.
    pub async fn stop(&mut self) {
        // Drop the liveliness token — triggers Delete event on peers
        if let Some(token) = self._token.take() {
            drop(token);
        }
        // Abort the subscriber task
        if let Some(handle) = self._subscriber_handle.take() {
            handle.abort();
        }
        tracing::info!("ZenohDiscovery stopped for node {}", self.node_id);
    }
}

/// Get current time in milliseconds.
fn current_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_node_alive_key() {
        assert_eq!(node_alive_key("node-1"), "cluster/node/node-1/alive");
    }

    #[test]
    fn test_extract_node_id_valid() {
        assert_eq!(
            extract_node_id("cluster/node/node-1/alive"),
            Some("node-1".into())
        );
    }

    #[test]
    fn test_extract_node_id_invalid() {
        assert_eq!(extract_node_id("cluster/node/node-1"), None);
        assert_eq!(extract_node_id("other/node-1/alive"), None);
        assert_eq!(extract_node_id("cluster/node/node-1/status"), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_zenoh_discovery_node_alive_and_dead() {
        let config = zenoh::Config::default();
        let session = Arc::new(zenoh::open(config).await.unwrap());

        let registry = Arc::new(ClusterRegistry::new());
        let failover_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fc = failover_called.clone();
        let on_failure: FailoverCallback = Arc::new(move |_node_id| {
            fc.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        // Node A starts discovery
        let mut discovery_a = ZenohDiscovery::new(session.clone(), "disc-node-a".into());
        discovery_a
            .start(registry.clone(), on_failure)
            .await
            .unwrap();

        // Give time for subscription to establish
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Node B declares a liveliness token
        let key_b = node_alive_key("disc-node-b");
        let token_b = session
            .liveliness()
            .declare_token(KeyExpr::try_from(key_b.as_str()).unwrap())
            .await
            .unwrap();

        // Wait for node A to discover node B
        tokio::time::sleep(Duration::from_millis(500)).await;

        let alive = registry.alive_nodes().await;
        let has_b = alive.iter().any(|n| n.id == "disc-node-b");
        assert!(has_b, "Node B should be discovered as alive");

        // Drop node B's token (simulate failure)
        drop(token_b);

        // Wait for node A to detect failure
        tokio::time::sleep(Duration::from_millis(500)).await;

        assert!(
            failover_called.load(std::sync::atomic::Ordering::SeqCst),
            "Failover callback should be called"
        );
    }
}
