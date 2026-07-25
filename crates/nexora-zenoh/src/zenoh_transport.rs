//! Eclipse Zenoh transport layer for distributed graph operations.
//!
//! Uses Zenoh's query/reply pattern for request-response graph operations.
//! Each node declares a queryable on `graph/node/{node_id}` and receives
//! GraphOperation payloads, replying with GraphResult.
//!
//! Key expression design:
//!   - `graph/node/{node_id}` — queryable for graph operations on a specific node
//!   - Payload: JSON-serialized GraphOperation
//!   - Reply: JSON-serialized GraphResult
//!
//! This is the real Eclipse Zenoh integration, replacing the TCP transport
//! with Zenoh's P2P routing, automatic discovery, and multi-transport support.

#![cfg(feature = "zenoh")]

use crate::{GraphOperation, GraphResult, RemoteGraphClient, RouterError};
use std::sync::Arc;
use std::time::Duration;
use zenoh::bytes::ZBytes;
use zenoh::key_expr::KeyExpr;
use zenoh::query::QueryTarget;
use zenoh::Session;

use crate::tcp_transport::GraphHandler;

/// Key expression prefix for graph operations.
const GRAPH_KEY_PREFIX: &str = "graph/node";

/// Build the Zenoh key expression for a target node.
fn node_key_expr(node_id: &str) -> String {
    format!("{GRAPH_KEY_PREFIX}/{node_id}")
}

/// Zenoh-based graph server.
///
/// Declares a queryable on `graph/node/{node_id}` and dispatches incoming
/// queries to a GraphHandler. The handler processes GraphOperation payloads
/// and replies with GraphResult.
pub struct ZenohGraphServer {
    session: Arc<Session>,
    node_id: String,
    /// The queryable declaration, kept alive to maintain the subscription.
    _queryable: Option<zenoh::query::Queryable<zenoh::handlers::DefaultHandler>>,
}

impl ZenohGraphServer {
    /// Create a new Zenoh graph server.
    ///
    /// The server will respond to queries on `graph/node/{node_id}`.
    /// Does NOT start serving yet — call `start()` to begin.
    pub fn new(session: Arc<Session>, node_id: String) -> Self {
        Self {
            session,
            node_id,
            _queryable: None,
        }
    }

    /// Start serving graph operations.
    ///
    /// Declares a queryable and spawns a task to handle incoming queries.
    pub async fn start(&mut self, handler: Arc<dyn GraphHandler>) -> Result<(), RouterError> {
        let key_expr = node_key_expr(&self.node_id);
        tracing::info!("ZenohGraphServer declaring queryable on '{key_expr}'");

        let queryable = self
            .session
            .declare_queryable(&key_expr)
            .await
            .map_err(|e| RouterError::Remote(format!("zenoh queryable declare: {e}")))?;

        let node_id = self.node_id.clone();
        tokio::spawn(async move {
            while let Ok(query) = queryable.recv_async().await {
                let handler = handler.clone();
                let nid = node_id.clone();
                tokio::spawn(handle_query(query, handler, nid));
            }
        });

        Ok(())
    }
}

/// Handle a single Zenoh query: deserialize GraphOperation, dispatch, reply.
async fn handle_query(query: zenoh::query::Query, handler: Arc<dyn GraphHandler>, node_id: String) {
    // Extract payload from the query
    let op: GraphOperation = match query.payload() {
        Some(payload) => {
            let bytes = payload.to_bytes();
            match serde_json::from_slice(&bytes) {
                Ok(op) => op,
                Err(e) => {
                    tracing::warn!("Failed to deserialize GraphOperation: {e}");
                    let err_msg = serde_json::to_vec(&serde_json::json!({
                        "error": format!("deserialize: {e}")
                    }))
                    .unwrap_or_default();
                    let _ = query
                        .reply(
                            KeyExpr::try_from(node_key_expr(&node_id))
                                .unwrap_or_else(|_| KeyExpr::try_from("graph/node/error").unwrap()),
                            ZBytes::from(err_msg),
                        )
                        .await;
                    return;
                }
            }
        }
        None => {
            tracing::warn!("Query received with no payload");
            return;
        }
    };

    // Dispatch to handler
    let result = handler.handle(&node_id, op).await;

    // Serialize and reply
    let reply_key = match KeyExpr::try_from(node_key_expr(&node_id)) {
        Ok(k) => k,
        Err(e) => {
            tracing::error!("Failed to create reply key: {e}");
            return;
        }
    };

    match result {
        Ok(graph_result) => {
            let payload = match serde_json::to_vec(&graph_result) {
                Ok(b) => ZBytes::from(b),
                Err(e) => {
                    tracing::error!("Failed to serialize GraphResult: {e}");
                    return;
                }
            };
            if let Err(e) = query.reply(reply_key, payload).await {
                tracing::warn!("Failed to send reply: {e}");
            }
        }
        Err(e) => {
            let err_payload = serde_json::to_vec(&serde_json::json!({
                "error": e.to_string()
            }))
            .unwrap_or_default();
            let _ = query.reply(reply_key, ZBytes::from(err_payload)).await;
        }
    }
}

/// Zenoh-based remote graph client.
///
/// Implements `RemoteGraphClient` by using `session.get()` to route
/// GraphOperation payloads to the target node's queryable.
pub struct ZenohRemoteClient {
    session: Arc<Session>,
    /// Operation timeout
    op_timeout: Duration,
}

impl ZenohRemoteClient {
    /// Create a new Zenoh remote client.
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            session,
            op_timeout: Duration::from_secs(10),
        }
    }

    /// Set the operation timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.op_timeout = timeout;
        self
    }
}

impl RemoteGraphClient for ZenohRemoteClient {
    fn execute<'a>(
        &'a self,
        target_node: &'a str,
        op: GraphOperation,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<GraphResult, RouterError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let key = node_key_expr(target_node);
            let payload =
                serde_json::to_vec(&op).map_err(|e| RouterError::Serialization(e.to_string()))?;

            tracing::trace!("Zenoh get '{key}' with GraphOperation");

            let replies = self
                .session
                .get(&key)
                .target(QueryTarget::BestMatching)
                .timeout(self.op_timeout)
                .payload(ZBytes::from(payload))
                .await
                .map_err(|e| RouterError::Remote(format!("zenoh get: {e}")))?;

            // Receive the first reply
            let reply = replies
                .recv_async()
                .await
                .map_err(|_| RouterError::Timeout)?;

            match reply.result() {
                Ok(sample) => {
                    let bytes = sample.payload().to_bytes();
                    let result: GraphResult = serde_json::from_slice(&bytes)
                        .map_err(|e| RouterError::Serialization(e.to_string()))?;
                    Ok(result)
                }
                Err(err) => {
                    let err_msg = err
                        .payload()
                        .try_to_string()
                        .unwrap_or_else(|e| e.to_string().into());
                    Err(RouterError::Remote(err_msg.to_string()))
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A simple echo handler for testing.
    struct EchoHandler;

    #[async_trait::async_trait]
    impl GraphHandler for EchoHandler {
        async fn handle(
            &self,
            _target: &str,
            op: GraphOperation,
        ) -> Result<GraphResult, RouterError> {
            match op {
                GraphOperation::GetProperty { key, .. } => {
                    Ok(GraphResult::Property(Some(serde_json::json!(key))))
                }
                GraphOperation::SetProperty { .. } => Ok(GraphResult::Status {
                    ok: true,
                    message: "ok".into(),
                }),
                GraphOperation::AddEdge { .. } => Ok(GraphResult::Status {
                    ok: true,
                    message: "edge ok".into(),
                }),
                GraphOperation::ExecuteCypher { query } => Ok(GraphResult::CypherRows {
                    columns: vec!["query".into()],
                    rows: vec![vec![serde_json::json!(query)]],
                }),
                GraphOperation::GetEdges { qid, edge_type } => {
                    let et = edge_type.unwrap_or_default();
                    Ok(GraphResult::Property(Some(serde_json::json!([{
                        "edge_type": et,
                        "direction": "out",
                        "target": qid.to_hex(),
                    }]))))
                }
                GraphOperation::GetAllProperties { qid } => {
                    Ok(GraphResult::Property(Some(serde_json::json!({
                        "_qid": qid.to_hex(),
                    }))))
                }
                GraphOperation::FencedWrite { .. } => Ok(GraphResult::Status {
                    ok: true,
                    message: "fenced write ok".into(),
                }),
                GraphOperation::ExportShard { .. } => {
                    Ok(GraphResult::Property(Some(serde_json::json!(null))))
                }
                GraphOperation::ExportDelta { .. } => {
                    Ok(GraphResult::Property(Some(serde_json::json!(null))))
                }
            }
        }
    }

    async fn create_test_session() -> Arc<Session> {
        let config = zenoh::Config::default();
        let session = zenoh::open(config).await.unwrap();
        Arc::new(session)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_zenoh_transport_get_property() {
        let session = create_test_session().await;
        let handler = Arc::new(EchoHandler);

        let mut server = ZenohGraphServer::new(session.clone(), "test-node-1".into());
        server.start(handler).await.unwrap();

        // Give Zenoh time to establish the queryable
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = ZenohRemoteClient::new(session.clone());
        let qid = nexora_id::NexoraId::from_bytes(b"test".to_vec());
        let result = client
            .execute(
                "test-node-1",
                GraphOperation::GetProperty {
                    qid: qid.clone(),
                    key: "speed".into(),
                },
            )
            .await
            .unwrap();

        match result {
            GraphResult::Property(Some(v)) => assert_eq!(v, serde_json::json!("speed")),
            other => panic!("expected Property(Some(\"speed\")), got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_zenoh_transport_set_property() {
        let session = create_test_session().await;
        let handler = Arc::new(EchoHandler);

        let mut server = ZenohGraphServer::new(session.clone(), "test-node-2".into());
        server.start(handler).await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = ZenohRemoteClient::new(session.clone());
        let qid = nexora_id::NexoraId::from_bytes(b"n1".to_vec());
        let result = client
            .execute(
                "test-node-2",
                GraphOperation::SetProperty {
                    qid,
                    key: "name".into(),
                    value: serde_json::json!("Alice"),
                },
            )
            .await
            .unwrap();

        match result {
            GraphResult::Status { ok, message } => {
                assert!(ok);
                assert_eq!(message, "ok");
            }
            other => panic!("expected Status, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_zenoh_transport_execute_cypher() {
        let session = create_test_session().await;
        let handler = Arc::new(EchoHandler);

        let mut server = ZenohGraphServer::new(session.clone(), "test-node-3".into());
        server.start(handler).await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = ZenohRemoteClient::new(session.clone());
        let result = client
            .execute(
                "test-node-3",
                GraphOperation::ExecuteCypher {
                    query: "MATCH (n) RETURN n".into(),
                },
            )
            .await
            .unwrap();

        match result {
            GraphResult::CypherRows { columns, rows } => {
                assert_eq!(columns, vec!["query"]);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], serde_json::json!("MATCH (n) RETURN n"));
            }
            other => panic!("expected CypherRows, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_zenoh_transport_get_edges() {
        let session = create_test_session().await;
        let handler = Arc::new(EchoHandler);

        let mut server = ZenohGraphServer::new(session.clone(), "test-node-4".into());
        server.start(handler).await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = ZenohRemoteClient::new(session.clone());
        let qid = nexora_id::NexoraId::from_bytes(b"node-edges".to_vec());
        let result = client
            .execute(
                "test-node-4",
                GraphOperation::GetEdges {
                    qid,
                    edge_type: Some("KNOWS".into()),
                },
            )
            .await
            .unwrap();

        match result {
            GraphResult::Property(Some(v)) => {
                let arr = v.as_array().expect("expected array");
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0]["edge_type"], serde_json::json!("KNOWS"));
            }
            other => panic!("expected Property(Some(array)), got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_zenoh_transport_multiple_operations() {
        let session = create_test_session().await;
        let handler = Arc::new(EchoHandler);

        let mut server = ZenohGraphServer::new(session.clone(), "test-node-5".into());
        server.start(handler).await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = ZenohRemoteClient::new(session.clone());

        // Send 5 sequential requests
        for i in 0..5 {
            let qid = nexora_id::NexoraId::from_bytes(format!("n{i}").into_bytes());
            let result = client
                .execute(
                    "test-node-5",
                    GraphOperation::GetProperty {
                        qid,
                        key: format!("key-{i}"),
                    },
                )
                .await
                .unwrap();

            match result {
                GraphResult::Property(Some(v)) => {
                    assert_eq!(v, serde_json::json!(format!("key-{i}")));
                }
                other => panic!("result {i}: expected Property, got {other:?}"),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_zenoh_transport_concurrent_operations() {
        let session = create_test_session().await;
        let handler = Arc::new(EchoHandler);

        let mut server = ZenohGraphServer::new(session.clone(), "test-node-6".into());
        server.start(handler).await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = Arc::new(ZenohRemoteClient::new(session.clone()));

        let mut handles = Vec::new();
        for i in 0..10 {
            let c = client.clone();
            handles.push(tokio::spawn(async move {
                let qid = nexora_id::NexoraId::from_bytes(format!("n{i}").into_bytes());
                c.execute(
                    "test-node-6",
                    GraphOperation::GetProperty {
                        qid,
                        key: format!("key-{i}"),
                    },
                )
                .await
                .unwrap()
            }));
        }

        let results: Vec<_> = futures::future::join_all(handles).await;
        for (i, r) in results.into_iter().enumerate() {
            let r = r.unwrap();
            match r {
                GraphResult::Property(Some(v)) => {
                    assert_eq!(v, serde_json::json!(format!("key-{i}")));
                }
                other => panic!("result {i}: expected Property, got {other:?}"),
            }
        }
    }
}
