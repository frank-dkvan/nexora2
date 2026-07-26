//! RPC abstraction layer for distributed communication.
//!
//! Provides unified [`RpcServer`] and [`RpcClient`] traits that abstract over
//! different RPC protocols. The primary implementation uses [tonic](https://docs.rs/tonic)
//! for gRPC communication.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │  Application Layer (Nexora, RisingWave) │
//! └────────────┬───────────────┬────────────┘
//!              │               │
//!              v               v
//! ┌─────────────────┐ ┌─────────────────┐
//! │   RpcServer     │ │   RpcClient     │
//! └────────┬────────┘ └────────┬────────┘
//!          │                   │
//!          v                   v
//! ┌─────────────────────────────────────────┐
//! │    Tonic gRPC (Protocol Buffers)        │
//! └─────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ## Server
//!
//! ```rust,no_run
//! use nexora_rpc::{RpcServer, TonicRpcServer};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let server = TonicRpcServer::new("127.0.0.1:5690".parse()?);
//! server.start().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Client
//!
//! ```rust,no_run
//! use nexora_rpc::{RpcClient, TonicRpcClient};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = TonicRpcClient::connect("http://127.0.0.1:5690").await?;
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod error;
pub mod server;
pub mod tonic_impl;

pub use client::RpcClient;
pub use error::{RpcError, Result};
pub use server::RpcServer;
pub use tonic_impl::{TonicRpcClient, TonicRpcServer};
