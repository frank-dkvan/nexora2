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
//! │      EventStreamingModule (this crate)      │
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
//! use nexora_risingwave::{EventStreamingModule, EventStreamingConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create configuration
//! let config = EventStreamingConfig::new()
//!     .with_meta_addr("127.0.0.1:5690".parse()?)
//!     .with_frontend_addr("127.0.0.1:4566".parse()?);
//!
//! // Start RisingWave module
//! let rw = EventStreamingModule::start(config).await?;
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
pub mod event_streaming_trait;
pub mod frontend_wrapper;
pub mod meta_wrapper;
pub mod module;

// Phase 8: Library mode - RisingWave compiled in-process (single-node)
#[cfg(feature = "library")]
pub mod library;

#[cfg(feature = "library")]
pub mod library_client;

#[cfg(feature = "library")]
pub mod library_module;

// Phase 9: Distributed library mode - Multi-node cluster with in-process RisingWave
#[cfg(feature = "library")]
pub mod distributed_library_config;

#[cfg(feature = "library")]
pub mod distributed_library_meta;

// Phase 7: Embedded RisingWave (process-based)
#[cfg(feature = "embedded")]
pub mod embedded_process;

// Phase 8: Distributed Embedded RisingWave (HA cluster)
#[cfg(feature = "embedded")]
pub mod distributed;

pub use catalog::{CatalogClient, ColumnInfo, MaterializedViewInfo, SourceInfo};
pub use config::EventStreamingConfig;
pub use ddl_parser::{ColumnDef, DdlParser, ParsedSchema};
pub use error::{Result, EventStreamingError};
pub use event_sink::{Change, ColumnValue, EventLogSink, Row};
pub use event_streaming_trait::EventStreamingOperations;
pub use module::EventStreamingModule;

#[cfg(feature = "library")]
pub use library::{EmbeddedLibrary, EmbeddedLibraryConfig};

#[cfg(feature = "library")]
pub use library_client::LibraryClient;

#[cfg(feature = "library")]
pub use library_module::LibraryEventStreamingModule;

#[cfg(feature = "library")]
pub use distributed_library_config::{
    DistributedLibraryConfig, MetaNodeConfig as DistributedMetaNodeConfig,
    FrontendNodeConfig as DistributedFrontendNodeConfig,
    ComputeNodeConfig as DistributedComputeNodeConfig, MetaBackend as DistributedMetaBackend,
};

#[cfg(feature = "library")]
pub use distributed_library_meta::{DistributedMetaCluster, MetaClusterState, RaftState};

#[cfg(feature = "embedded")]
pub use embedded_process::{
    EmbeddedEventStreaming, EmbeddedConfig, EmbeddedState,
    MetaConfig, MetaBackend, FrontendConfig, ComputeConfig,
};

#[cfg(feature = "embedded")]
pub use distributed::{
    DistributedEmbeddedEventStreaming, DistributedConfig,
    MetaNodeConfig, FrontendNodeConfig, ComputeNodeConfig,
    ClusterHealth, NodeHealth,
};
