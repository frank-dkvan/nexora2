//! Higher-level Nexora value types for graph elements.
//!
//! This crate builds on `nexora-id` to define the graph element types used
//! throughout the Nexora engine: edges, property keys (Symbols), and the
//! representation of graph node state.

mod edge;
mod symbol;

pub use edge::{EdgeDirection, HalfEdge};
pub use symbol::Symbol;

// Re-export nexora-id types for convenience
pub use nexora_id::{EventTime, NexoraId, PropertyValue};
