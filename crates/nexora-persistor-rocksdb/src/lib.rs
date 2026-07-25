//! RocksDB persistence backend for Nexora.
//!
//! Implements `NamespacedPersistenceAgent` using RocksDB as the storage engine.
//! This is the "hot tier" in the three-tier storage architecture.
//!
//! # Column Families
//!
//! Mirrors the reference implementation's 8 column families:
//! - `node-events`: NodeChangeEvent journal
//! - `domain-index-events`: DomainIndexEvent journal
//! - `snapshots`: Node snapshots
//! - `standing-queries`: Standing query definitions (future)
//! - `standing-query-states`: SQ intermediate states (future)
//! - `meta-data`: Metadata key-value
//! - `domain-graph-nodes`: DomainGraphNode storage (future)
//!
//! # Key Encoding
//!
//! Keys are byte-ordered to preserve lexicographic sort order:
//! `[2-byte QID length][NexoraId bytes][8-byte EventTime]`
//!
//! This matches the `RocksDbPersistor` key encoding for data compatibility.

mod persistor;
mod property_index;

pub use persistor::RocksDbPersistor;
pub use property_index::PersistentPropertyIndex;
