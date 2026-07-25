//! NexoraId — the fundamental node identifier in the Nexora graph.
//!
//! A NexoraId is an opaque byte sequence that uniquely identifies a graph node.
//! It is the primary key for all graph operations: property storage, edge references,
//! event journaling, snapshot identification, and standing query state tracking.
//!
//! The ID is typically generated from a namespace-specific UUID provider, but can
//! also be derived from external identifiers (e.g., device serial numbers, user IDs).

mod event_time;
mod nexora_id;
mod property_value;

pub use event_time::EventTime;
pub use nexora_id::NexoraId;
pub use property_value::PropertyValue;

pub mod blob;
pub use blob::BlobRef;
