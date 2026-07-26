//! RisingWave integration for Nexora 2.
//!
//! Provides a high-level wrapper around RisingWave components for advanced
//! stream processing capabilities.
//!
//! # Phase 3 Implementation
//!
//! This is a **simplified Phase 3 implementation** that defines the API and
//! provides basic structure. Full RisingWave integration (Meta, Frontend,
//! Compute nodes) will be added in Phase 4.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │      RisingWaveModule (this crate)      │
//! ├─────────────────────────────────────────┤
//! │  ┌─────────┐  ┌──────────┐  ┌────────┐ │
//! │  │  Meta   │  │ Frontend │  │ Compute│ │
//! │  │  Node   │  │   Node   │  │  Node  │ │
//! │  └─────────┘  └──────────┘  └────────┘ │
//! └────────┬──────────────┬─────────────────┘
//!          │              │
//!          v              v
//!   nexora-consensus  nexora-rpc
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use nexora_risingwave::{RisingWaveModule, RisingWaveConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create configuration
//! let config = RisingWaveConfig::new()
//!     .with_meta_addr("127.0.0.1:5690".parse()?)
//!     .with_frontend_addr("127.0.0.1:4566".parse()?);
//!
//! // Start RisingWave module
//! let rw = RisingWaveModule::start(config).await?;
//!
//! // Execute DDL
//! rw.execute_ddl("CREATE SOURCE my_source WITH (...)").await?;
//! rw.execute_ddl("CREATE MATERIALIZED VIEW my_mv AS SELECT ...").await?;
//!
//! // Query materialized view
//! let rows = rw.query_mv("SELECT * FROM my_mv").await?;
//!
//! // Shutdown
//! rw.shutdown().await?;
//! # Ok(())
//! # }
//! ```

pub mod catalog;
pub mod config;
pub mod ddl_parser;
pub mod error;
pub mod event_sink;
pub mod frontend_wrapper;
pub mod meta_wrapper;
pub mod module;

pub use catalog::{CatalogClient, ColumnInfo, MaterializedViewInfo, SourceInfo};
pub use config::RisingWaveConfig;
pub use ddl_parser::{ColumnDef, DdlParser, ParsedSchema};
pub use error::{Result, RisingWaveError};
pub use event_sink::{Change, ColumnValue, EventLogSink, Row};
pub use module::RisingWaveModule;
