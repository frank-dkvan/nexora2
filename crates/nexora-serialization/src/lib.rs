//! Nexora serialization — FlatBuffers code generation and binary codecs.
//!
//! This crate provides:
//! 1. Auto-generated Rust types from the FlatBuffers schemas in `fbs/`
//! 2. The `PackedFlatBufferBinaryFormat` codec (matching the reference implementation)
//! 3. Event time encoding/decoding for the persistence layer
//! 4. `EventCodec` for encoding/decoding node events and snapshots
//!
//! # FlatBuffers Compatibility
//!
//! The `.fbs` schemas are shared with the reference implementation. The generated
//! Rust code can read data written by the version and vice versa.

pub mod capnp_codec;
pub mod codec;
pub mod event_codec;

// Include the auto-generated FlatBuffers code
#[allow(unused_imports, dead_code, clippy::all)]
pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/flatbuffers_generated/mod.rs"));
}

// Re-export commonly used types
pub use codec::{NexoraCodecError, PackedCodec};
pub use event_codec::{EventCodec, SCHEMA_VERSION};
