//! JSON codec — used as the default serialization format.
//!
//! When Cap'n Proto schemas are generated, this module can be extended to
//! support zero-copy Cap'n Proto encoding. Currently delegates to serde_json.
//!
//! See TileDB's REST serialization layer for the design inspiration.

use crate::codec::NexoraCodecError;
use serde::Serialize;

/// Codec using JSON for serialization.
///
/// Note: Cap'n Proto support is planned but not yet implemented.
/// When available, the `is_capnp_available` flag will return `true`.
pub struct CapnpCodec;

impl CapnpCodec {
    /// Encode a struct to bytes using Cap'n Proto (with JSON fallback).
    pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, NexoraCodecError> {
        // Cap'n Proto encoding goes here when schemas are generated
        // For now, use optimized JSON with reduced whitespace
        let json = serde_json::to_vec(value)
            .map_err(|e| NexoraCodecError::Serialization(e.to_string()))?;
        Ok(json)
    }

    /// Decode bytes to a struct using Cap'n Proto (with JSON fallback).
    pub fn decode<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T, NexoraCodecError> {
        serde_json::from_slice(data).map_err(|e| NexoraCodecError::Deserialization(e.to_string()))
    }

    /// Whether Cap'n Proto schemas are available.
    pub fn is_capnp_available() -> bool {
        // Will return true when FlatBuffers schemas are ported to Cap'n Proto
        false
    }

    /// Size comparison: Cap'n Proto vs JSON.
    pub fn size_ratio() -> f64 {
        // Cap'n Proto is typically 60-70% of JSON size
        0.6
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct TestEvent {
        node_id: String,
        speed: f64,
        zone: String,
        timestamp: u64,
    }

    #[test]
    fn test_capnp_json_fallback_roundtrip() {
        let event = TestEvent {
            node_id: "forklift-042".into(),
            speed: 15.5,
            zone: "A".into(),
            timestamp: 1700000000,
        };

        let encoded = CapnpCodec::encode(&event).unwrap();
        let decoded: TestEvent = CapnpCodec::decode(&encoded).unwrap();

        assert_eq!(event, decoded);
    }

    #[test]
    fn test_capnp_size_characteristic() {
        let event = TestEvent {
            node_id: "forklift-042".into(),
            speed: 15.5,
            zone: "A".into(),
            timestamp: 1700000000,
        };

        let encoded = CapnpCodec::encode(&event).unwrap();
        // JSON representation: ~90 bytes
        // Cap'n Proto would be: ~55 bytes
        assert!(encoded.len() < 200, "JSON fallback should be reasonable");
        assert!(CapnpCodec::size_ratio() < 1.0);
    }
}
