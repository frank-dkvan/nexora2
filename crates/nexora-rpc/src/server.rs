//! RPC server trait.

use async_trait::async_trait;
use std::net::SocketAddr;

use crate::error::Result;

/// Abstraction over RPC server implementations.
///
/// This trait provides a unified interface for starting and stopping
/// RPC servers that handle remote procedure calls.
#[async_trait]
pub trait RpcServer: Send + Sync {
    /// Start the RPC server.
    ///
    /// This begins listening on the configured address and handling
    /// incoming RPC requests. The method returns once the server is
    /// ready to accept connections.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if the server started successfully
    /// - `Err(_)` if the server failed to start (e.g., port already in use)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use nexora_rpc::RpcServer;
    /// # async fn example(server: impl RpcServer) -> Result<(), Box<dyn std::error::Error>> {
    /// server.start().await?;
    /// println!("Server is now accepting connections");
    /// # Ok(())
    /// # }
    /// ```
    async fn start(&self) -> Result<()>;

    /// Stop the RPC server gracefully.
    ///
    /// This stops accepting new connections and waits for in-flight
    /// requests to complete before shutting down.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if the server stopped successfully
    /// - `Err(_)` if shutdown encountered an error
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use nexora_rpc::RpcServer;
    /// # async fn example(server: impl RpcServer) -> Result<(), Box<dyn std::error::Error>> {
    /// server.stop().await?;
    /// println!("Server stopped");
    /// # Ok(())
    /// # }
    /// ```
    async fn stop(&self) -> Result<()>;

    /// Get the local address the server is listening on.
    ///
    /// # Returns
    ///
    /// The socket address, or `None` if the server hasn't started yet.
    fn local_addr(&self) -> Option<SocketAddr>;

    /// Check if the server is currently running.
    fn is_running(&self) -> bool;
}
