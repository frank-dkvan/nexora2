//! TCP transport layer for distributed graph operations.
//!
//! Replaces Zenoh with a simple, debuggable length-prefixed JSON protocol.
//!
//! Wire format:
//!   Handshake (4 bytes, sent once by the client immediately on connect):
//!     [0x4E, 0x58, major, minor]  — 'N','X' magic + wire version
//!   Request:  4B BE length | JSON (target_node: String, op: GraphOperation)
//!   Response: 4B BE length | JSON (Result<GraphResult, String>)
//!
//! Version negotiation (E4): the client writes a 4-byte magic+version header
//! right after `TcpStream::connect` (before the first request frame).  The
//! server reads and validates those 4 bytes before entering the request loop.
//! If the magic is wrong or the major version differs, the server closes the
//! connection immediately.  This avoids touching the `WireRequest`/
//! `WireResponse` enums (which have exhaustive match arms in many places) and
//! keeps the change to two small call sites.
//!
//! Design doc §6.2: Zenoh routing — this TCP layer provides the same
//! routing semantics with zero external dependencies beyond tokio.

use crate::{GraphOperation, GraphResult, RemoteGraphClient, RouterError};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};

/// 4-byte magic+version header written by the client immediately on connect.
/// Layout: [0x4E, 0x58, MAJOR, MINOR]  ('N', 'X' = Nexora).
/// The server rejects any connection where the magic bytes differ or the major
/// version doesn't match NEXORA_WIRE_VERSION.
pub const NEXORA_WIRE_MAGIC: [u8; 2] = [0x4E, 0x58]; // 'N', 'X'
/// Current wire protocol major version.  Bump when introducing a breaking
/// change to the frame format or the JSON envelope.  The server accepts only
/// connections whose major byte equals this value.
pub const NEXORA_WIRE_VERSION: u8 = 1;
/// Wire protocol minor version.  Informational only — no compatibility check.
pub const NEXORA_WIRE_MINOR: u8 = 0;

/// Length-prefix frame: write 4B BE length + payload.
async fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> Result<(), std::io::Error> {
    let len = payload.len() as u32;
    stream.write_u32(len).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;
    Ok(())
}

/// Length-prefix frame: read 4B BE length + payload.
async fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, std::io::Error> {
    let len = stream.read_u32().await? as usize;
    if len > 64 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame too large: {len}"),
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Tagged request on the wire. The transport multiplexes two independent
/// planes over the *same* TCP channel:
///
/// - [`WireRequest::Graph`] — a data-plane [`GraphOperation`] (the original,
///   only, payload before A2).
/// - [`WireRequest::Raft`] — a control-plane consensus RPC, carried as opaque
///   bytes. The transport never interprets these; it hands them to a
///   registered [`RaftRpcHandler`]. This keeps openraft entirely out of the
///   transport layer and off the data-plane [`GraphOperation`] enum — the
///   control plane (consensus over metadata) and the data plane (user graph
///   ops) stay cleanly separated, just sharing a socket.
///
/// Raft payloads are `Vec<u8>` and thus serialize as a JSON number array under
/// this length-prefixed-JSON protocol — inefficient per byte, but control-plane
/// traffic is small and infrequent (metadata DDL, heartbeats), so reusing the
/// existing stack beats introducing a second wire format.
#[derive(serde::Serialize, serde::Deserialize)]
enum WireRequest {
    Graph {
        target_node: String,
        op: GraphOperation,
    },
    Raft(Vec<u8>),
}

/// Tagged response on the wire, mirroring [`WireRequest`].
#[derive(serde::Serialize, serde::Deserialize)]
enum WireResponse {
    /// Result of a graph operation (`ok` + `result`/`error`).
    Graph {
        ok: bool,
        result: Option<GraphResult>,
        error: Option<String>,
    },
    /// Opaque serialized Raft RPC response bytes.
    Raft(Vec<u8>),
    /// A transport-level fault that isn't specific to either plane — a
    /// malformed request, or a Raft RPC arriving at a server with no
    /// [`RaftRpcHandler`] registered.
    Fault(String),
}

/// Trait for handling graph operations on the server side.
/// Implemented by GraphServiceAdapter to bridge to the local GraphService.
#[async_trait::async_trait]
pub trait GraphHandler: Send + Sync {
    async fn handle(
        &self,
        target_node: &str,
        op: GraphOperation,
    ) -> Result<GraphResult, RouterError>;
}

/// Trait for handling control-plane Raft RPCs on the server side.
///
/// Implemented (in `control_raft_network.rs`, A2-4) by the adapter that decodes
/// the opaque payload into an openraft `append_entries`/`vote`/`install_snapshot`
/// request, dispatches it to the local `Raft` node, and re-encodes the response.
/// The transport treats the payload as bytes end to end.
#[async_trait::async_trait]
pub trait RaftRpcHandler: Send + Sync {
    /// Handle one serialized Raft RPC, returning the serialized response.
    /// An `Err` is surfaced to the caller as a transport fault.
    async fn handle_raft(&self, payload: Vec<u8>) -> Result<Vec<u8>, String>;
}

/// TCP server that accepts graph operation requests and dispatches to a handler.
///
/// Optionally also serves control-plane Raft RPCs (A2-4) if a
/// [`RaftRpcHandler`] is registered via [`TcpGraphServer::with_raft_handler`].
/// A server with no Raft handler answers Raft requests with a transport fault,
/// so the two planes can be enabled independently.
pub struct TcpGraphServer {
    handler: Arc<dyn GraphHandler>,
    raft_handler: Option<Arc<dyn RaftRpcHandler>>,
    listen_addr: String,
    shutdown: Arc<tokio::sync::Notify>,
}

impl TcpGraphServer {
    pub fn new(handler: Arc<dyn GraphHandler>, listen_addr: String) -> Self {
        Self {
            handler,
            raft_handler: None,
            listen_addr,
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Attach a control-plane Raft RPC handler so this server also serves
    /// consensus traffic over the same socket. Builder-style so existing
    /// call sites (data-plane only) are unchanged.
    pub fn with_raft_handler(mut self, raft_handler: Arc<dyn RaftRpcHandler>) -> Self {
        self.raft_handler = Some(raft_handler);
        self
    }

    /// Start listening. Returns the actual bound address.
    pub async fn start(&self) -> Result<String, std::io::Error> {
        let listener = TcpListener::bind(&self.listen_addr).await?;
        let addr = listener.local_addr()?.to_string();
        tracing::info!("TcpGraphServer listening on {addr}");

        let handler = self.handler.clone();
        let raft_handler = self.raft_handler.clone();
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, peer)) => {
                                tracing::debug!("Connection from {peer}");
                                let h = handler.clone();
                                let rh = raft_handler.clone();
                                tokio::spawn(handle_connection(stream, h, rh));
                            }
                            Err(e) => {
                                tracing::error!("Accept error: {e}");
                                break;
                            }
                        }
                    }
                    _ = shutdown.notified() => {
                        tracing::info!("TcpGraphServer shutting down");
                        break;
                    }
                }
            }
        });

        Ok(addr)
    }

    /// Signal the server to stop.
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }
}

/// Handle a single TCP connection: read tagged requests, dispatch to the graph
/// or Raft handler, write tagged responses.
///
/// E4 version negotiation: the first 4 bytes on every new connection MUST be
/// the magic+version header.  An incompatible client (wrong magic or wrong
/// major version) gets its connection closed immediately, preventing it from
/// accidentally sending garbled frames to the request loop.
async fn handle_connection(
    mut stream: TcpStream,
    handler: Arc<dyn GraphHandler>,
    raft_handler: Option<Arc<dyn RaftRpcHandler>>,
) {
    // --- version handshake ---
    let mut hdr = [0u8; 4];
    if stream.read_exact(&mut hdr).await.is_err() {
        // Client closed before sending the header — nothing to do.
        return;
    }
    if hdr[0] != NEXORA_WIRE_MAGIC[0] || hdr[1] != NEXORA_WIRE_MAGIC[1] {
        tracing::warn!(
            "version handshake: bad magic {:02x}{:02x}, closing connection",
            hdr[0],
            hdr[1]
        );
        return;
    }
    if hdr[2] != NEXORA_WIRE_VERSION {
        tracing::warn!(
            "version handshake: client major version {} != server {}, closing connection",
            hdr[2],
            NEXORA_WIRE_VERSION
        );
        return;
    }
    // Minor version is informational only — no check needed.

    loop {
        let frame = match read_frame(&mut stream).await {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => {
                tracing::warn!("Read error: {e}");
                break;
            }
        };

        let request: WireRequest = match serde_json::from_slice(&frame) {
            Ok(req) => req,
            Err(e) => {
                let resp = WireResponse::Fault(format!("invalid request: {e}"));
                let resp_bytes = serde_json::to_vec(&resp).unwrap_or_default();
                let _ = write_frame(&mut stream, &resp_bytes).await;
                continue;
            }
        };

        let resp = match request {
            WireRequest::Graph { target_node, op } => {
                match handler.handle(&target_node, op).await {
                    Ok(r) => WireResponse::Graph {
                        ok: true,
                        result: Some(r),
                        error: None,
                    },
                    Err(e) => WireResponse::Graph {
                        ok: false,
                        result: None,
                        error: Some(e.to_string()),
                    },
                }
            }
            WireRequest::Raft(payload) => match &raft_handler {
                Some(rh) => match rh.handle_raft(payload).await {
                    Ok(bytes) => WireResponse::Raft(bytes),
                    Err(e) => WireResponse::Fault(e),
                },
                None => WireResponse::Fault("no raft handler registered".into()),
            },
        };

        let resp_bytes = match serde_json::to_vec(&resp) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("Failed to serialize response: {e}");
                break;
            }
        };

        if write_frame(&mut stream, &resp_bytes).await.is_err() {
            break;
        }
    }
}

/// Connection pool entry.
struct PooledConn {
    stream: TcpStream,
    last_used: std::time::Instant,
}

/// TCP client that implements RemoteGraphClient.
/// Maintains a connection pool per target node for efficiency.
pub struct TcpRemoteClient {
    /// target_node -> connection pool
    pools: RwLock<HashMap<String, Mutex<Vec<PooledConn>>>>,
    /// target_node -> address (host:port)
    node_addresses: RwLock<HashMap<String, String>>,
    /// Connection timeout
    connect_timeout: std::time::Duration,
    /// Operation timeout
    op_timeout: std::time::Duration,
    /// Max connections per node
    max_pool_size: usize,
}

impl TcpRemoteClient {
    pub fn new() -> Self {
        Self {
            pools: RwLock::new(HashMap::new()),
            node_addresses: RwLock::new(HashMap::new()),
            connect_timeout: std::time::Duration::from_secs(5),
            op_timeout: std::time::Duration::from_secs(10),
            max_pool_size: 4,
        }
    }

    /// Register a node's network address.
    pub async fn register_node(&self, node_id: &str, address: &str) {
        self.node_addresses
            .write()
            .await
            .insert(node_id.to_string(), address.to_string());
    }

    /// Register multiple nodes at once.
    pub async fn register_nodes(&self, nodes: &[(String, String)]) {
        let mut addrs = self.node_addresses.write().await;
        for (id, addr) in nodes {
            addrs.insert(id.clone(), addr.clone());
        }
    }

    /// Get the address for a node.
    async fn get_address(&self, node_id: &str) -> Result<String, RouterError> {
        self.node_addresses
            .read()
            .await
            .get(node_id)
            .cloned()
            .ok_or_else(|| RouterError::NodeNotFound(node_id.to_string()))
    }

    /// Acquire a connection from the pool or create a new one.
    async fn acquire_conn(&self, node_id: &str) -> Result<TcpStream, RouterError> {
        let addr = self.get_address(node_id).await?;

        // Try to get a pooled connection
        let pool_lock = self.pools.read().await;
        if let Some(mutex) = pool_lock.get(node_id) {
            let mut pool = mutex.lock().await;
            while let Some(conn) = pool.pop() {
                // Check if connection is still fresh (< 30s old)
                if conn.last_used.elapsed() < std::time::Duration::from_secs(30) {
                    return Ok(conn.stream);
                }
                // Stale connection, try next
            }
        }
        drop(pool_lock);

        // Create new connection
        let mut stream = tokio::time::timeout(self.connect_timeout, TcpStream::connect(&addr))
            .await
            .map_err(|_| RouterError::Timeout)?
            .map_err(|e| RouterError::Remote(format!("connect to {addr} failed: {e}")))?;

        // E4 version negotiation: send the 4-byte magic+version header immediately
        // after connecting so the server can reject incompatible clients early.
        let hdr = [
            NEXORA_WIRE_MAGIC[0],
            NEXORA_WIRE_MAGIC[1],
            NEXORA_WIRE_VERSION,
            NEXORA_WIRE_MINOR,
        ];
        stream
            .write_all(&hdr)
            .await
            .map_err(|e| RouterError::Remote(format!("version handshake write failed: {e}")))?;
        stream
            .flush()
            .await
            .map_err(|e| RouterError::Remote(format!("version handshake flush failed: {e}")))?;

        Ok(stream)
    }

    /// Return a connection to the pool.
    async fn release_conn(&self, node_id: &str, stream: TcpStream) {
        let pool_lock = self.pools.read().await;
        if let Some(mutex) = pool_lock.get(node_id) {
            let mut pool = mutex.lock().await;
            if pool.len() < self.max_pool_size {
                pool.push(PooledConn {
                    stream,
                    last_used: std::time::Instant::now(),
                });
            }
            // If pool is full, the connection is dropped (closed)
        }
        drop(pool_lock);
    }

    /// Ensure a pool exists for a node.
    async fn ensure_pool(&self, node_id: &str) {
        let mut pools = self.pools.write().await;
        pools
            .entry(node_id.to_string())
            .or_insert_with(|| Mutex::new(Vec::new()));
    }

    /// Execute a single request-response over a TCP stream.
    async fn execute_on_stream(
        &self,
        mut stream: TcpStream,
        target_node: &str,
        op: GraphOperation,
    ) -> Result<(GraphResult, TcpStream), RouterError> {
        let request = WireRequest::Graph {
            target_node: target_node.to_string(),
            op,
        };
        let req_bytes =
            serde_json::to_vec(&request).map_err(|e| RouterError::Serialization(e.to_string()))?;

        write_frame(&mut stream, &req_bytes)
            .await
            .map_err(|e| RouterError::Remote(format!("write: {e}")))?;

        let resp_frame = tokio::time::timeout(self.op_timeout, read_frame(&mut stream))
            .await
            .map_err(|_| RouterError::Timeout)?
            .map_err(|e| RouterError::Remote(format!("read: {e}")))?;

        let resp: WireResponse = serde_json::from_slice(&resp_frame)
            .map_err(|e| RouterError::Serialization(e.to_string()))?;

        let result = match resp {
            WireResponse::Graph {
                ok: true, result, ..
            } => result.ok_or_else(|| RouterError::Remote("empty result".into())),
            WireResponse::Graph {
                ok: false, error, ..
            } => Err(RouterError::Remote(
                error.unwrap_or_else(|| "unknown error".into()),
            )),
            WireResponse::Fault(e) => Err(RouterError::Remote(e)),
            WireResponse::Raft(_) => Err(RouterError::Remote(
                "unexpected raft response to graph request".into(),
            )),
        }?;

        Ok((result, stream))
    }

    /// Send a control-plane Raft RPC (opaque bytes) to `target_node` and return
    /// the peer's serialized response bytes. Reuses the same connection pool and
    /// wire framing as graph ops (A2-4). The caller (openraft `RaftNetwork`
    /// impl) owns encoding/decoding; the transport only shuttles bytes.
    pub async fn send_raft(
        &self,
        target_node: &str,
        payload: Vec<u8>,
        timeout: std::time::Duration,
    ) -> Result<Vec<u8>, RouterError> {
        self.ensure_pool(target_node).await;
        let mut stream = self.acquire_conn(target_node).await?;

        let request = WireRequest::Raft(payload);
        let req_bytes =
            serde_json::to_vec(&request).map_err(|e| RouterError::Serialization(e.to_string()))?;

        write_frame(&mut stream, &req_bytes)
            .await
            .map_err(|e| RouterError::Remote(format!("write: {e}")))?;

        let resp_frame = tokio::time::timeout(timeout, read_frame(&mut stream))
            .await
            .map_err(|_| RouterError::Timeout)?
            .map_err(|e| RouterError::Remote(format!("read: {e}")))?;

        let resp: WireResponse = serde_json::from_slice(&resp_frame)
            .map_err(|e| RouterError::Serialization(e.to_string()))?;

        match resp {
            WireResponse::Raft(bytes) => {
                // Only pool the connection back on the success path, matching
                // the graph path's "don't reuse a broken stream" policy.
                self.release_conn(target_node, stream).await;
                Ok(bytes)
            }
            WireResponse::Fault(e) => Err(RouterError::Remote(e)),
            WireResponse::Graph { .. } => Err(RouterError::Remote(
                "unexpected graph response to raft request".into(),
            )),
        }
    }
}

impl Default for TcpRemoteClient {
    fn default() -> Self {
        Self::new()
    }
}

impl RemoteGraphClient for TcpRemoteClient {
    fn execute<'a>(
        &'a self,
        target_node: &'a str,
        op: GraphOperation,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<GraphResult, RouterError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.ensure_pool(target_node).await;
            let stream = self.acquire_conn(target_node).await?;
            match self.execute_on_stream(stream, target_node, op).await {
                Ok((result, stream)) => {
                    self.release_conn(target_node, stream).await;
                    Ok(result)
                }
                Err(e) => {
                    // On error, don't return the broken connection to the pool
                    Err(e)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A simple echo handler for testing the transport layer.
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
                GraphOperation::ExportDigest { .. } => {
                    Ok(GraphResult::Property(Some(serde_json::json!(null))))
                }
                GraphOperation::Ping => Ok(GraphResult::Status {
                    ok: true,
                    message: "pong".into(),
                }),
            }
        }
    }

    #[tokio::test]
    async fn test_tcp_server_start_and_shutdown() {
        let handler = Arc::new(EchoHandler);
        let server = TcpGraphServer::new(handler, "127.0.0.1:0".to_string());
        let addr = server.start().await.unwrap();
        assert!(addr.starts_with("127.0.0.1:"));
        server.shutdown();
    }

    #[tokio::test]
    async fn test_tcp_remote_client_get_property() {
        let handler = Arc::new(EchoHandler);
        let server = TcpGraphServer::new(handler, "127.0.0.1:0".to_string());
        let addr = server.start().await.unwrap();

        let client = TcpRemoteClient::new();
        client.register_node("node-1", &addr).await;

        let qid = nexora_id::NexoraId::from_bytes(b"test".to_vec());
        let result = client
            .execute(
                "node-1",
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

    #[tokio::test]
    async fn test_tcp_remote_client_set_property() {
        let handler = Arc::new(EchoHandler);
        let server = TcpGraphServer::new(handler, "127.0.0.1:0".to_string());
        let addr = server.start().await.unwrap();

        let client = TcpRemoteClient::new();
        client.register_node("node-1", &addr).await;

        let qid = nexora_id::NexoraId::from_bytes(b"n1".to_vec());
        let result = client
            .execute(
                "node-1",
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

    #[tokio::test]
    async fn test_tcp_remote_client_connection_reuse() {
        let handler = Arc::new(EchoHandler);
        let server = TcpGraphServer::new(handler, "127.0.0.1:0".to_string());
        let addr = server.start().await.unwrap();

        let client = TcpRemoteClient::new();
        client.register_node("node-1", &addr).await;

        // First request creates a connection
        let qid = nexora_id::NexoraId::from_bytes(b"a".to_vec());
        let r1 = client
            .execute(
                "node-1",
                GraphOperation::GetProperty {
                    qid: qid.clone(),
                    key: "k1".into(),
                },
            )
            .await
            .unwrap();

        // Second request should reuse the pooled connection
        let r2 = client
            .execute(
                "node-1",
                GraphOperation::GetProperty {
                    qid,
                    key: "k2".into(),
                },
            )
            .await
            .unwrap();

        assert_eq!(r1, GraphResult::Property(Some(serde_json::json!("k1"))));
        assert_eq!(r2, GraphResult::Property(Some(serde_json::json!("k2"))));
    }

    #[tokio::test]
    async fn test_tcp_remote_client_node_not_found() {
        let client = TcpRemoteClient::new();
        let qid = nexora_id::NexoraId::from_bytes(b"x".to_vec());
        let result = client
            .execute(
                "unknown-node",
                GraphOperation::GetProperty {
                    qid,
                    key: "k".into(),
                },
            )
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            RouterError::NodeNotFound(n) => assert_eq!(n, "unknown-node"),
            other => panic!("expected NodeNotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_tcp_remote_client_execute_cypher() {
        let handler = Arc::new(EchoHandler);
        let server = TcpGraphServer::new(handler, "127.0.0.1:0".to_string());
        let addr = server.start().await.unwrap();

        let client = TcpRemoteClient::new();
        client.register_node("node-1", &addr).await;

        let result = client
            .execute(
                "node-1",
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

    #[tokio::test]
    async fn test_tcp_multiple_clients() {
        let handler = Arc::new(EchoHandler);
        let server = TcpGraphServer::new(handler, "127.0.0.1:0".to_string());
        let addr = server.start().await.unwrap();

        let client = Arc::new(TcpRemoteClient::new());
        client.register_node("node-1", &addr).await;

        let mut handles = Vec::new();
        for i in 0..5 {
            let c = client.clone();
            handles.push(tokio::spawn(async move {
                let qid = nexora_id::NexoraId::from_bytes(format!("n{i}").into_bytes());
                c.execute(
                    "node-1",
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

    /// E4: A client that sends an incompatible major version in the handshake
    /// header must have its connection rejected by the server (connection closed
    /// before the first request frame is processed).
    #[tokio::test]
    async fn version_handshake_rejects_incompatible_client() {
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpStream as RawStream;

        let handler = Arc::new(EchoHandler);
        let server = TcpGraphServer::new(handler, "127.0.0.1:0".to_string());
        let addr = server.start().await.unwrap();

        // Give the server a moment to start listening.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Connect with a bad major version (99 instead of 1).
        let mut stream = RawStream::connect(&addr).await.unwrap();
        let bad_hdr = [NEXORA_WIRE_MAGIC[0], NEXORA_WIRE_MAGIC[1], 99u8, 0u8];
        stream.write_all(&bad_hdr).await.unwrap();
        stream.flush().await.unwrap();

        // The server should close the connection: a subsequent read attempt must
        // return EOF or a connection-reset error (not a valid response frame).
        let mut buf = vec![0u8; 8];
        let result = stream.read(&mut buf).await;
        match result {
            Ok(0) => {}  // clean EOF — expected
            Err(_) => {} // connection reset — also acceptable
            Ok(n) => panic!("expected server to close connection, got {n} bytes"),
        }
    }

    /// E4: A compatible client (correct magic + version 1) connects and executes
    /// a request successfully.
    #[tokio::test]
    async fn version_handshake_accepts_compatible_client() {
        let handler = Arc::new(EchoHandler);
        let server = TcpGraphServer::new(handler, "127.0.0.1:0".to_string());
        let addr = server.start().await.unwrap();

        let client = TcpRemoteClient::new();
        client.register_node("n", &addr).await;

        let qid = nexora_id::NexoraId::from_bytes(b"v".to_vec());
        let result = client
            .execute(
                "n",
                GraphOperation::GetProperty {
                    qid,
                    key: "x".into(),
                },
            )
            .await;
        assert!(
            result.is_ok(),
            "compatible client should succeed: {result:?}"
        );
    }
}
