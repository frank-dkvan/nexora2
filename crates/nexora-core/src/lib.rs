//! Nexora Core — the streaming graph engine.
//!
//! This crate implements the core graph computation model:
//! - **NodeTask**: Each graph node runs as an independent async task
//! - **GraphShard**: Manages a collection of nodes, handles lifecycle
//! - **GraphService**: Orchestrates shards, persistence, and standing queries
//! - **NodeEvent**: The event sourcing model for node mutations
//! - **Persistence**: The `NamespacedPersistenceAgent` trait hierarchy
//! - **PropertyIndex**: Three-tier hybrid index for fast property queries
//! - **LabelIndex**: Fast label-based node lookups
//! - **QueryOptimizer**: Cost-based query optimization
//! - **MaterializedView**: Persistent storage for computed query results
//! - **IncrementalAggregation**: Efficient COUNT/SUM/AVG updates

pub mod backup;
pub mod control_plane_store;
pub mod domain_package;
pub mod edge_index;
pub mod event;
pub mod evidence_ref;
pub mod flatbuf_codec;
pub mod graph;
pub mod incremental_aggregation;
pub mod index;
pub mod label_index;
pub mod materialized_view;
pub mod ontology_manager;
pub mod persistor;
pub mod query_optimizer;
pub mod query_pool;
pub mod raw_event;
pub mod snapshot_manifest;
pub mod wal;

#[cfg(feature = "encrypt")]
pub mod encryption;

// Re-export the most important types
pub use backup::{BackupArtifact, BackupEdge, BackupNode, BackupPayload};
pub use control_plane_store::{
    ControlPlaneStore, ControlStoreError, InMemoryControlPlaneStore, Namespace,
    RocksDbControlPlaneStore,
};
pub use domain_package::{
    DomainColumnDef, DomainLoader, DomainMV, DomainPackage, DomainQuery, DomainSchema, EdgeMapping,
    EdgeTypeDef, EventMapping, IndexDef, LabelDef, NodeMapping, PropertyDef,
};
pub use edge_index::{EdgeIndex, EdgeIndexStats};
pub use event::{DomainIndexEvent, NodeChangeEvent, NodeEvent};
pub use evidence_ref::{EvidenceRef, EvidenceStoreType};
pub use graph::{
    BatchDurability, GraphError, GraphService, GraphServiceConfig, MutationOp, NodeCommand,
    NodeTask, WriteBatchOptions,
};
pub use index::{IndexConfig, IndexError, IndexStats, PropertyIndex};
pub use label_index::{LabelIndex, LabelIndexStats};
pub use persistor::{InMemoryPersistor, NamespacedPersistenceAgent};
pub use query_optimizer::{ExecutionPlan, FilterPredicate, IndexStatistics, QueryOptimizer};
pub use raw_event::RawEvent;
pub use snapshot_manifest::{ChecksumKind, SnapshotKind, SnapshotManifest};

#[cfg(feature = "encrypt")]
pub use encryption::EncryptionConfig;
