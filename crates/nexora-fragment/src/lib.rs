//! Fragment-based time-sharded immutble graph storage.
//!
//! Each fragment represents an immutble time window of graph events.
//! Inspired by TileDB's fragment model: __t1_t2_uuid naming.
//!
//! Architecture:
//! ```text
//!   Graph events → Batched by time window → Fragment
//!   Fragment = { metadata, node_tiles, edge_tiles, property_columns }
//!   Fragment naming: <start_us>_<end_us>_<uuid>
//!   Read: filter fragments by time range → merge relevant tiles
//! ```

/// Columnar cold-layer format with predicate + projection pushdown (OLAP).
pub mod columnar;
/// B8: background fragment consolidation — auto-merges small fragments (TileDB-style).
pub mod consolidator;
/// Unique identifier for a fragment, encoding time range and a UUID.
pub mod fragment_id;
/// Pre-computed fragment statistics used for query optimization.
pub mod metadata;
/// graph→fragment sealing pipeline: accumulates mutations into time-windowed
/// fragments and seals them into the tiered store.
pub mod sealer;
/// Physical storage layer for fragment data (tiles, properties, edges).
pub mod store;
/// Bridge: fragment bodies stored in a tiered object store (hot/warm/cold), so
/// aged fragments sink to shared storage and are read back on demand.
pub mod tiered_store;
/// Time-travel query engine: snapshots, consolidation, historical views.
pub mod time_travel;

/// Re-export: columnar cold-layer format with predicate/projection pushdown.
pub use columnar::{ColumnStats, ColumnarFragment, Predicate, PredicateOp};
/// Re-export: background fragment consolidator and its strategy config.
pub use consolidator::{ConsolidationConfig, FragmentConsolidator};
/// Re-export: a unique fragment identifier combining time window and UUID.
pub use fragment_id::FragmentId;
/// Re-export: metadata and property statistics for a fragment.
pub use metadata::FragmentMetadata;
/// Re-export: the graph→fragment sealing pipeline and its event type.
pub use sealer::{FragmentSealer, SealEvent};
/// Re-export: the core fragment storage engine.
pub use store::FragmentStore;
/// Re-export: the tiered fragment store bridging fragment lifecycle to storage tiers.
pub use tiered_store::TieredFragmentStore;
pub use time_travel::{
    consolidate_fragments, execute_time_travel, EdgeSnapshot, NodeSnapshot, TimeTravelQuery,
    TimeTravelResult,
};
