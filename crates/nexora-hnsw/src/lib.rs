//! NOTE: This crate is planned for future integration.
//! HNSW (Hierarchical Navigable Small World) vector index for real-time
//! graph similarity search.
//!
//! This crate provides:
//! - **Distance functions**: cosine similarity, Euclidean distance, inner product
//! - **HNSW index**: a configurable approximate nearest-neighbor index
//!
//! # Example
//!
//! ```rust,no_run
//! use nexora_hnsw::{HnswIndex, HnswConfig, Distance};
//!
//! let mut config = HnswConfig::default();
//! config.dim = 3;
//! config.distance = Distance::L2;
//! let mut index = HnswIndex::new(config);
//!
//! let node_id = nexora_id::NexoraId::new_random();
//! index.insert(node_id, vec![1.0, 2.0, 3.0]);
//! let results = index.search_knn(&[1.1, 2.1, 3.1], 5);
//! ```

pub mod distance;
pub mod index;

pub use distance::{cosine_similarity, euclidean_distance, inner_product, Distance};
pub use index::{HnswConfig, HnswIndex};
