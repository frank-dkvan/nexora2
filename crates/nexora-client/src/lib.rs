//! # nexora-client
//!
//! A typed async HTTP client SDK for the [nexora](https://github.com/frank-dkvan/nexora)
//! streaming graph database.
//!
//! ## Quick Start
//!
//! ```no_run
//! use nexora_client::NexoraClient;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let client = NexoraClient::builder()
//!     .base_url("http://localhost:8080")
//!     .bearer_token("my-token")
//!     .build()?;
//!
//! // Execute a Cypher query
//! let result = client.execute_cypher("CREATE (n:Person {name: 'Alice'}) RETURN n").await?;
//! println!("Columns: {:?}", result.columns);
//!
//! // Check health
//! let health = client.health().await?;
//! println!("Status: {}", health.status);
//! # Ok(())
//! # }
//! ```
//!
//! ## Features
//!
//! - **Async** — built on `reqwest` + `tokio`
//! - **Typed** — all request/response types are strongly typed via `serde`
//! - **Authenticated** — Bearer token support
//! - **Retryable** — automatic retries with exponential backoff for transient failures
//! - **Configurable** — custom base URL, timeout, and max retries
//!
//! ## API Coverage
//!
//! The client covers all nexora API endpoints:
//!
//! | Category | Endpoints |
//! |----------|-----------|
//! | Health | health, readiness, liveness |
//! | Query | Cypher, SQL, EXPLAIN |
//! | Graph | property CRUD, edges, time-travel |
//! | Standing Queries | list, create, get, delete |
//! | Vector Search | index, search, get, delete |
//! | Ingest | file ingest, list, cancel |
//! | Streams | list, start Kafka, stop |
//! | Recipes | list, create, get, delete, execute, runs |
//! | UDFs | register, list, execute, delete |
//! | Materialized Views | create, list, get, drop, query, refresh, link-sq |
//! | Storage | status, migrate |
//! | System | info, config |
//! | Auth | token generation |
//! | Cluster | stats, Raft status |
//! | Metrics | JSON, Prometheus |

pub mod client;
pub mod error;
pub mod types;

pub use client::{NexoraClient, NexoraClientBuilder};
pub use error::NexoraClientError;
pub use types::*;
