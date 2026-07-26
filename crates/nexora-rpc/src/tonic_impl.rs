//! Tonic (gRPC) implementation of RPC traits.
//!
//! This is a simplified implementation for Phase 2. Full gRPC service
//! definitions and implementations will be added in Phase 4.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;
use tracing::info;

use crate::client::RpcClient;
use crate::error::{Result, RpcError};
use crate::server::RpcServer;

/// Tonic-based RPC server implementation.
///
/// This is a simplified version for Phase 2 that provides the basic
/// RpcServer interface. Full gRPC service implementation will be added
/// in Phase 4 when we integrate with RisingWave.
pub struct TonicRpcServer {
    addr: SocketAddr,
    state: Arc<RwLock<ServerState>>,
}

#[derive(Debug)]
struct ServerState {
    running: bool,
}

impl TonicRpcServer {
    /// Create a new Tonic RPC server.
    ///
    /// # Arguments
    ///
    /// - `addr`: The address to bind to
    ///
    /// # Example
    ///
    /// ```rust
    /// use nexora_rpc::TonicRpcServer;
    ///
    /// let server = TonicRpcServer::new("127.0.0.1:5690".parse().unwrap());
    /// ```
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            state: Arc::new(RwLock::new(ServerState { running: false })),
        }
    }
}

#[async_trait]
impl RpcServer for TonicRpcServer {
    async fn start(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if state.running {
            return Err(RpcError::ServerStart("server already running".to_string()));
        }

        info!("Starting Tonic RPC server on {}", self.addr);
        state.running = true;

        // In Phase 4, this will actually start a tonic server
        // For now, just mark as running
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if !state.running {
            return Ok(());
        }

        info!("Stopping Tonic RPC server");
        state.running = false;
        Ok(())
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        Some(self.addr)
    }

    fn is_running(&self) -> bool {
        // Use try_read to avoid blocking
        self.state.try_read().map(|s| s.running).unwrap_or(false)
    }
}

/// Tonic-based RPC client implementation.
///
/// This is a simplified version for Phase 2 that provides the basic
/// RpcClient interface. Full gRPC client implementation will be added
/// in Phase 4 when we integrate with RisingWave.
#[derive(Clone)]
pub struct TonicRpcClient {
    endpoint: String,
    state: Arc<RwLock<ClientState>>,
}

#[derive(Debug)]
struct ClientState {
    connected: bool,
}

impl TonicRpcClient {
    /// Connect to a Tonic RPC server.
    ///
    /// # Arguments
    ///
    /// - `endpoint`: The server endpoint (e.g., "http://127.0.0.1:5690")
    ///
    /// # Returns
    ///
    /// A connected client.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use nexora_rpc::TonicRpcClient;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = TonicRpcClient::connect("http://127.0.0.1:5690").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint = endpoint.into();
        info!("Connecting to Tonic RPC server at {}", endpoint);

        // In Phase 4, this will actually connect via tonic
        // For now, just create the client structure
        Ok(Self {
            endpoint,
            state: Arc::new(RwLock::new(ClientState { connected: true })),
        })
    }

    /// Get the endpoint this client is connected to.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[async_trait]
impl RpcClient for TonicRpcClient {
    async fn call(&self, method: &str, request: Bytes) -> Result<Bytes> {
        let state = self.state.read().await;
        if !state.connected {
            return Err(RpcError::ConnectionFailed("not connected".to_string()));
        }

        info!(
            "RPC call: method={}, request_size={}",
            method,
            request.len()
        );

        // In Phase 4, this will actually make a gRPC call
        // For now, just echo back the request as a placeholder
        Ok(request)
    }

    async fn is_healthy(&self) -> Result<bool> {
        let state = self.state.read().await;
        Ok(state.connected)
    }

    async fn close(&self) -> Result<()> {
        let mut state = self.state.write().await;
        info!("Closing Tonic RPC client");
        state.connected = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_lifecycle() {
        let server = TonicRpcServer::new("127.0.0.1:15690".parse().unwrap());

        assert!(!server.is_running());
        assert_eq!(
            server.local_addr(),
            Some("127.0.0.1:15690".parse().unwrap())
        );

        server.start().await.unwrap();
        assert!(server.is_running());

        server.stop().await.unwrap();
        assert!(!server.is_running());
    }

    #[tokio::test]
    async fn test_client_lifecycle() {
        let client = TonicRpcClient::connect("http://127.0.0.1:15691")
            .await
            .unwrap();

        assert_eq!(client.endpoint(), "http://127.0.0.1:15691");
        assert!(client.is_healthy().await.unwrap());

        client.close().await.unwrap();
        assert!(!client.is_healthy().await.unwrap());
    }

    #[tokio::test]
    async fn test_client_call() {
        let client = TonicRpcClient::connect("http://127.0.0.1:15692")
            .await
            .unwrap();

        let request = Bytes::from("test request");
        let response = client.call("test_method", request.clone()).await.unwrap();

        // In Phase 2, it echoes back
        assert_eq!(response, request);
    }

    #[tokio::test]
    async fn test_client_clone() {
        let client1 = TonicRpcClient::connect("http://127.0.0.1:15693")
            .await
            .unwrap();

        let client2 = client1.clone();
        assert_eq!(client2.endpoint(), "http://127.0.0.1:15693");
        assert!(client2.is_healthy().await.unwrap());
    }
}
