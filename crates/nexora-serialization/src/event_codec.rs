//! Event codec — converts between domain types and binary format.
//!
//! This is the bridge between the in-memory `NodeChangeEvent` / `TimedEvent` types
//! and the binary format used for persistence.
//!
//! ## Formats
//!
//! - **v1**: Plain JSON (`serde_json`). Used for backward compatibility.
//! - **v2**: JSON compressed with PackedCodec (zero-byte elimination), stored with
//!   a FlatBuffers-compatible framing layer provided by `nexora-core::flatbuf_codec`.
//!
//! ## Schema versioning
//!
//! The `SCHEMA_VERSION` constant is embedded in WAL records via the FlatBuffer
//! framing header (the two magic bytes distinguish v1 from v2). When reading data
//! back, `detect_format` inspects the first byte to route to the correct decoder.

use crate::codec::NexoraCodecError;
use crate::codec::PackedCodec;

/// Codec format version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecFormat {
    /// v1: Plain JSON serialization (backward compatible).
    Json = 1,
    /// v2: JSON + PackedCodec compression (zero-byte elimination), framed with
    /// FlatBuffer-compatible headers by nexora-core.
    PackedJson = 2,
}

/// Schema version marker for persistence format.
/// v1 = JSON, v2 = JSON + PackedCodec with FlatBuffer framing.
pub const SCHEMA_VERSION_V1: u32 = 1;
pub const SCHEMA_VERSION_V2: u32 = 2;

/// Current default schema version (v2 = packed JSON with FlatBuffer framing).
pub const SCHEMA_VERSION: u32 = SCHEMA_VERSION_V2;

/// Check if a schema version is compatible with this codec.
pub fn is_compatible_version(version: u32) -> bool {
    version == SCHEMA_VERSION_V1 || version == SCHEMA_VERSION_V2
}

/// Codec for encoding/decoding node change events to/from binary.
pub struct EventCodec;

impl EventCodec {
    // ---- JSON format (v1) ----

    /// Encode a `TimedEvent<NodeChangeEvent>` to JSON bytes (v1 format).
    pub fn encode_node_event_json<T: serde::Serialize>(
        event: &T,
    ) -> Result<Vec<u8>, NexoraCodecError> {
        serde_json::to_vec(event).map_err(|e| NexoraCodecError::Serialization(e.to_string()))
    }

    /// Decode a `TimedEvent<NodeChangeEvent>` from JSON bytes (v1 format).
    pub fn decode_node_event_json<T: serde::de::DeserializeOwned>(
        data: &[u8],
    ) -> Result<T, NexoraCodecError> {
        serde_json::from_slice(data).map_err(|e| NexoraCodecError::Deserialization(e.to_string()))
    }

    /// Encode a `TimedEvent<DomainIndexEvent>` to JSON bytes (v1 format).
    pub fn encode_domain_event_json<T: serde::Serialize>(
        event: &T,
    ) -> Result<Vec<u8>, NexoraCodecError> {
        serde_json::to_vec(event).map_err(|e| NexoraCodecError::Serialization(e.to_string()))
    }

    /// Decode a `TimedEvent<DomainIndexEvent>` from JSON bytes (v1 format).
    pub fn decode_domain_event_json<T: serde::de::DeserializeOwned>(
        data: &[u8],
    ) -> Result<T, NexoraCodecError> {
        serde_json::from_slice(data).map_err(|e| NexoraCodecError::Deserialization(e.to_string()))
    }

    // ---- Packed JSON format (v2) ----

    /// Encode an event to packed JSON for v2 format.
    ///
    /// Returns `(bytes, original_len)` where `original_len` holds the
    /// uncompressed JSON length (needed by PackedCodec when unpacking).
    pub fn encode_node_event_fb<T: serde::Serialize>(
        event: &T,
        packed: bool,
    ) -> Result<(Vec<u8>, usize), NexoraCodecError> {
        // v2 format: serialize to JSON, then optionally apply PackedCodec
        // (zero-byte elimination). The FlatBuffer-compatible framing (magic,
        // length, CRC) is handled by nexora-core::flatbuf_codec.
        let json_bytes = serde_json::to_vec(event)
            .map_err(|e| NexoraCodecError::Serialization(e.to_string()))?;
        let original_len = json_bytes.len();

        if packed {
            let packed_bytes = PackedCodec::pack(&json_bytes);
            Ok((packed_bytes, original_len))
        } else {
            Ok((json_bytes, original_len))
        }
    }

    /// Decode an event from packed JSON v2 format.
    ///
    /// `original_len` is the uncompressed JSON length (as returned by the encoder).
    /// When `original_len` is 0, data is assumed to be uncompressed.
    pub fn decode_node_event_fb<T: serde::de::DeserializeOwned>(
        data: &[u8],
        original_len: usize,
    ) -> Result<T, NexoraCodecError> {
        let unpacked = if original_len > 0 {
            PackedCodec::unpack(data, original_len)
                .map_err(|e| NexoraCodecError::Deserialization(e.to_string()))?
        } else {
            data.to_vec()
        };

        // Try FlatBuffers first, fall back to JSON
        // The actual FlatBuffer decoding is in nexora-core::flatbuf_codec
        serde_json::from_slice(&unpacked)
            .map_err(|e| NexoraCodecError::Deserialization(e.to_string()))
    }

    /// Encode PropertyValue to MessagePack bytes for embedding in FlatBuffers.
    /// This matches the .fbs schema which uses `msg_packed: [byte]` for NexoraValue.
    pub fn encode_property_value_msgpack<T: serde::Serialize>(
        value: &T,
    ) -> Result<Vec<u8>, NexoraCodecError> {
        rmp_serde::to_vec(value).map_err(|e| NexoraCodecError::Serialization(e.to_string()))
    }

    /// Decode PropertyValue from MessagePack bytes.
    pub fn decode_property_value_msgpack<T: serde::de::DeserializeOwned>(
        data: &[u8],
    ) -> Result<T, NexoraCodecError> {
        rmp_serde::from_slice(data).map_err(|e| NexoraCodecError::Deserialization(e.to_string()))
    }

    // ---- Snapshots (JSON only, no FlatBuffer schema yet) ----

    /// Encode a snapshot (properties + edges) to JSON bytes.
    pub fn encode_snapshot<T: serde::Serialize>(snapshot: &T) -> Result<Vec<u8>, NexoraCodecError> {
        serde_json::to_vec(snapshot).map_err(|e| NexoraCodecError::Serialization(e.to_string()))
    }

    /// Decode a snapshot from JSON bytes.
    pub fn decode_snapshot<T: serde::de::DeserializeOwned>(
        data: &[u8],
    ) -> Result<T, NexoraCodecError> {
        serde_json::from_slice(data).map_err(|e| NexoraCodecError::Deserialization(e.to_string()))
    }

    // ---- Generic format detection ----

    /// Detect the codec format from raw data.
    ///
    /// JSON payloads start with `{` (0x7B) or `[` (0x5B). Packed v2 data
    /// begins with a PackedCodec tag byte which, for the first 8-byte word
    /// of a FlatBuffer (typically sparse), is unlikely to equal `0x7B` or
    /// `0x5B` — but it *can* collide given the right bit pattern. When the
    /// data is otherwise unusable, the JSON parser will naturally return a
    /// deserialization error, so the heuristic is best-effort.
    pub fn detect_format(data: &[u8]) -> CodecFormat {
        const MAX_PACKED_TAG_FIRST_BYTE: u8 = 0x3F; // realistic FlatBuffer first-word tag
        if data.is_empty() {
            return CodecFormat::Json;
        }
        // JSON starts with '{' (0x7B) or '[' (0x5B)
        if data[0] == 0x7B || data[0] == 0x5B {
            CodecFormat::Json
        } else if data[0] <= MAX_PACKED_TAG_FIRST_BYTE {
            // First byte is a PackedCodec tag in the realistic range
            // (FlatBuffer headers are sparse). Treat as binary.
            CodecFormat::PackedJson
        } else {
            // Defensively fall back to JSON interpretation — treating
            // unrecognised binary as JSON will produce a clean parse
            // error rather than a silent misread.
            CodecFormat::Json
        }
    }

    /// Encode an event using the specified format.
    pub fn encode_node_event<T: serde::Serialize>(
        event: &T,
        format: CodecFormat,
    ) -> Result<(Vec<u8>, usize), NexoraCodecError> {
        match format {
            CodecFormat::Json => {
                let bytes = Self::encode_node_event_json(event)?;
                Ok((bytes, 0)) // 0 = not packed
            }
            CodecFormat::PackedJson => Self::encode_node_event_fb(event, true),
        }
    }

    /// Decode an event, auto-detecting the format.
    pub fn decode_node_event_auto<T: serde::de::DeserializeOwned>(
        data: &[u8],
        original_len: usize,
    ) -> Result<T, NexoraCodecError> {
        match Self::detect_format(data) {
            CodecFormat::Json => Self::decode_node_event_json(data),
            CodecFormat::PackedJson => Self::decode_node_event_fb(data, original_len),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_version_compatibility() {
        assert!(is_compatible_version(1));
        assert!(is_compatible_version(2));
        assert!(!is_compatible_version(3));
    }

    #[test]
    fn test_format_detection_json() {
        let json = b"{\"key\": \"value\"}";
        assert_eq!(EventCodec::detect_format(json), CodecFormat::Json);
    }

    #[test]
    fn test_format_detection_binary() {
        let binary = b"\x00\x01\x02\x03";
        assert_eq!(EventCodec::detect_format(binary), CodecFormat::PackedJson);
    }

    #[test]
    fn test_format_detection_empty() {
        assert_eq!(EventCodec::detect_format(b""), CodecFormat::Json);
    }

    #[test]
    fn test_msgpack_roundtrip_string() {
        let value = "hello world";
        let encoded = EventCodec::encode_property_value_msgpack(&value).unwrap();
        let decoded: String = EventCodec::decode_property_value_msgpack(&encoded).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_msgpack_roundtrip_integer() {
        let value = 42i64;
        let encoded = EventCodec::encode_property_value_msgpack(&value).unwrap();
        let decoded: i64 = EventCodec::decode_property_value_msgpack(&encoded).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_msgpack_roundtrip_map() {
        let value: std::collections::HashMap<String, i64> = {
            let mut m = std::collections::HashMap::new();
            m.insert("a".to_string(), 1);
            m.insert("b".to_string(), 2);
            m
        };
        let encoded = EventCodec::encode_property_value_msgpack(&value).unwrap();
        let decoded: std::collections::HashMap<String, i64> =
            EventCodec::decode_property_value_msgpack(&encoded).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_packed_json_roundtrip() {
        // Test PackedCodec roundtrip correctness
        // Note: small data may not compress well, so we only test correctness
        let json_data = b"{\"event\": \"PropertySet\", \"key\": \"speed\", \"value\": \"fast\"}";
        let packed = PackedCodec::pack(json_data);
        let unpacked = PackedCodec::unpack(&packed, json_data.len()).unwrap();
        assert_eq!(json_data.as_slice(), unpacked.as_slice());
        // For small data, compression may not reduce size, so we don't assert packed.len() < json_data.len()
    }
}
