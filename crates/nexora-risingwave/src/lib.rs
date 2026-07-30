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

#[cfg(feature = "library")]
pub mod distributed_library_frontend;

#[cfg(feature = "library")]
pub mod distributed_library_compute;

// Phase 7: Embedded RisingWave (process-based)
#[cfg(feature = "embedded")]
pub mod embedded_process;

// Phase 8: Distributed Embedded RisingWave (HA cluster)
#[cfg(feature = "embedded")]
pub mod distributed;

pub use catalog::{CatalogClient, ColumnInfo, MaterializedViewInfo, SourceInfo};
pub use config::EventStreamingConfig;
pub use ddl_parser::{ColumnDef, DdlParser, ParsedSchema};
pub use error::{EventStreamingError, Result};
pub use event_sink::{Change, ColumnValue, EventLogSink, Row};
pub use event_streaming_trait::{EventStreamingOperations, IcebergTable};
pub use module::EventStreamingModule;

#[cfg(feature = "library")]
pub use library::{EmbeddedLibrary, EmbeddedLibraryConfig};

#[cfg(feature = "library")]
pub use library_client::LibraryClient;

#[cfg(feature = "library")]
pub use library_module::LibraryEventStreamingModule;

#[cfg(feature = "library")]
pub use distributed_library_config::{
    ComputeNodeConfig as DistributedComputeNodeConfig, DistributedLibraryConfig,
    FrontendNodeConfig as DistributedFrontendNodeConfig, MetaBackend as DistributedMetaBackend,
    MetaNodeConfig as DistributedMetaNodeConfig,
};

#[cfg(feature = "library")]
pub use distributed_library_meta::{DistributedMetaCluster, MetaClusterState, RaftState};

#[cfg(feature = "library")]
pub use distributed_library_frontend::{DistributedFrontendPool, FrontendHealth, FrontendNode};

#[cfg(feature = "library")]
pub use distributed_library_compute::{
    ComputeHealth, ComputeNode, DistributedComputeCluster, FragmentAssignment, FragmentScheduler,
};

// Re-export simplified names for nexora-app
#[cfg(feature = "library")]
pub use distributed_library_config::{
    ComputeNodeConfig, FrontendNodeConfig, MetaBackend, MetaNodeConfig,
};

/// Start a distributed library mode cluster.
///
/// Returns (Meta cluster, Frontend pool, Compute cluster).
#[cfg(feature = "library")]
pub async fn start_distributed_library_cluster(
    config: DistributedLibraryConfig,
) -> Result<(
    DistributedMetaCluster,
    DistributedFrontendPool,
    DistributedComputeCluster,
)> {
    use std::sync::Arc;

    // Start Meta cluster
    let meta = DistributedMetaCluster::start(config.clone()).await?;

    // Start Frontend pool
    let frontend = DistributedFrontendPool::new(config.clone()).await?;

    // Start Compute cluster
    let compute = Arc::new(DistributedComputeCluster::new(config).await?);
    compute.register_compute_node().await?;

    // Start background tasks
    let _frontend_health = frontend.start_health_check();
    let _compute_heartbeat = compute.start_heartbeat();

    Ok((
        meta,
        frontend,
        Arc::try_unwrap(compute).unwrap_or_else(|arc| (*arc).clone()),
    ))
}

#[cfg(feature = "embedded")]
pub use embedded_process::{
    ComputeConfig, EmbeddedConfig, EmbeddedEventStreaming, EmbeddedState, FrontendConfig,
    MetaBackend, MetaConfig,
};

#[cfg(feature = "embedded")]
pub use distributed::{
    ClusterHealth, ComputeNodeConfig, DistributedConfig, DistributedEmbeddedEventStreaming,
    FrontendNodeConfig, MetaNodeConfig, NodeHealth,
};
