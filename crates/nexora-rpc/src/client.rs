//! RPC client trait.

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Result;

/// Abstraction over RPC client implementations.
///
/// This trait provides a unified interface for making remote procedure
/// calls to RPC servers.
#[async_trait]
pub trait RpcClient: Send + Sync + Clone {
    /// Send a request and receive a response.
    ///
    /// This is a generic method for making RPC calls. The request and
    /// response are passed as byte buffers, allowing flexibility in
    /// serialization formats.
    ///
    /// # Arguments
    ///
    /// - `method`: The RPC method name
    /// - `request`: Serialized request data
    ///
    /// # Returns
    ///
    /// The serialized response data.
    ///
    /// # Errors
    ///
    /// - [`RpcError::CallFailed`](crate::RpcError::CallFailed) if the RPC call fails
    /// - [`RpcError::Timeout`](crate::RpcError::Timeout) if the call times out
    /// - [`RpcError::Transport`](crate::RpcError::Transport) for network errors
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use nexora_rpc::RpcClient;
    /// # use bytes::Bytes;
    /// # async fn example(client: impl RpcClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let request = Bytes::from("request data");
    /// let response = client.call("my_method", request).await?;
    /// println!("Got response: {} bytes", response.len());
    /// # Ok(())
    /// # }
    /// ```
    async fn call(&self, method: &str, request: Bytes) -> Result<Bytes>;

    /// Check if the connection is healthy.
    ///
    /// # Returns
    ///
    /// - `Ok(true)` if the connection is healthy
    /// - `Ok(false)` if the connection is unhealthy but can be retried
    /// - `Err(_)` if the health check failed
    async fn is_healthy(&self) -> Result<bool>;

    /// Close the client connection.
    ///
    /// This gracefully closes the connection to the server.
    async fn close(&self) -> Result<()>;
}
