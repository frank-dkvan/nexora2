//! Binary codec for Nexora persistence — Cap'n Proto-style packing + FlatBuffers.
//!
//! The reference implementation uses `PackedFlatBufferBinaryFormat` which:
//! 1. Serializes data into a FlatBuffer
//! 2. Applies Cap'n Proto stream packing (zero-byte compression)
//!
//! This module provides the Rust equivalent for reading and writing
//! data compatible with the persistence layer.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum NexoraCodecError {
    #[error("FlatBuffers encoding error: {0}")]
    FlatBuffer(String),
    #[error("Packing error: {0}")]
    Packing(String),
    #[error("Unpacking error: {0}")]
    Unpacking(String),
    #[error("Invalid data: {0}")]
    Invalid(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Deserialization error: {0}")]
    Deserialization(String),
}

/// Cap'n Proto-style stream packing codec.
///
/// The packing algorithm works on 8-byte words:
/// - A 1-byte tag precedes each word, where each bit indicates whether
///   the corresponding byte in the word is non-zero.
/// - If the tag is 0x00 (all bytes zero), the word is compressed to just the tag byte.
/// - If the tag is 0xFF (all bytes non-zero), the raw bytes follow the tag.
/// - Otherwise, only the non-zero bytes are emitted after the tag.
///
/// This provides significant compression for FlatBuffers data which often
/// has many zero bytes (default values, alignment padding).
pub struct PackedCodec;

impl PackedCodec {
    /// Pack a byte slice using Cap'n Proto stream packing.
    ///
    /// This matches the `Packing.pack()` implementation.
    pub fn pack(data: &[u8]) -> Vec<u8> {
        let mut output = Vec::with_capacity(data.len() / 2 + 8);
        let mut pos = 0;

        while pos < data.len() {
            let word_end = (pos + 8).min(data.len());
            let word = &data[pos..word_end];
            let word_len = word.len();

            // Compute tag: each bit indicates non-zero byte
            let mut tag: u8 = 0;
            for (i, &byte) in word.iter().enumerate() {
                if byte != 0 {
                    tag |= 1 << i;
                }
            }

            output.push(tag);

            if tag == 0 {
                // All zeros — just the tag byte, skip the word
            } else if tag == 0xFF && word_len == 8 {
                // All non-zero — emit all bytes inline
                output.extend_from_slice(word);
            } else {
                // Mixed — emit only non-zero bytes
                for (i, &byte) in word.iter().enumerate() {
                    if tag & (1 << i) != 0 {
                        output.push(byte);
                    }
                }
            }

            pos += 8;
        }

        output
    }

    /// Unpack a Cap'n Proto packed byte slice back to the original data.
    ///
    /// `original_len` is needed because the packed format doesn't encode the
    /// trailing zero bytes of the last word.
    pub fn unpack(packed: &[u8], original_len: usize) -> Result<Vec<u8>, NexoraCodecError> {
        let mut output = Vec::with_capacity(original_len);
        let mut pos = 0;

        while pos < packed.len() && output.len() < original_len {
            let tag = packed[pos];
            pos += 1;

            let word_start = output.len();

            if tag == 0 {
                // All zeros — push 8 zero bytes
                let zeros = 8.min(original_len - output.len());
                output.extend(std::iter::repeat_n(0u8, zeros));
            } else if tag == 0xFF {
                // All non-zero — copy 8 bytes directly
                let available = packed.len().saturating_sub(pos);
                let count = 8.min(available).min(original_len - output.len());
                if count < 8 && output.len() + 8 <= original_len {
                    return Err(NexoraCodecError::Unpacking(
                        "Unexpected end of packed data for 0xFF tag".into(),
                    ));
                }
                output.extend_from_slice(&packed[pos..pos + count]);
                pos += count;
            } else {
                // Mixed — read only non-zero bytes and reconstruct
                for i in 0..8 {
                    if output.len() >= original_len {
                        break;
                    }
                    if tag & (1 << i) != 0 {
                        if pos >= packed.len() {
                            return Err(NexoraCodecError::Unpacking(
                                "Unexpected end of packed data".into(),
                            ));
                        }
                        output.push(packed[pos]);
                        pos += 1;
                    } else {
                        output.push(0);
                    }
                }
            }

            // Safety: don't emit more than original_len
            let _ = word_start;
        }

        if output.len() != original_len {
            return Err(NexoraCodecError::Unpacking(format!(
                "packed data ended after {} of {original_len} output bytes",
                output.len()
            )));
        }
        if pos != packed.len() {
            return Err(NexoraCodecError::Unpacking(
                "packed data contains trailing bytes".into(),
            ));
        }
        output.truncate(original_len);
        Ok(output)
    }

    /// Calculate the compression ratio for a packed result.
    pub fn compression_ratio(original_len: usize, packed_len: usize) -> f64 {
        if packed_len == 0 {
            return 1.0;
        }
        original_len as f64 / packed_len as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_unpack_all_zeros() {
        let data = vec![0u8; 100];
        let packed = PackedCodec::pack(&data);
        // 13 words (13 tags) — all tags are 0x00
        assert_eq!(packed.len(), 13);
        let unpacked = PackedCodec::unpack(&packed, 100).unwrap();
        assert_eq!(data, unpacked);
    }

    #[test]
    fn test_pack_unpack_all_nonzero() {
        let data: Vec<u8> = (1..=32).collect();
        let packed = PackedCodec::pack(&data);
        // 4 words × (1 tag + 8 bytes) = 36 bytes
        assert_eq!(packed.len(), 36);
        let unpacked = PackedCodec::unpack(&packed, 32).unwrap();
        assert_eq!(data, unpacked);
    }

    #[test]
    fn test_pack_unpack_mixed() {
        let data = vec![0, 0, 5, 0, 0, 3, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        let packed = PackedCodec::pack(&data);
        let unpacked = PackedCodec::unpack(&packed, 16).unwrap();
        assert_eq!(data, unpacked);
    }

    #[test]
    fn test_pack_unpack_single_byte() {
        let data = vec![42u8];
        let packed = PackedCodec::pack(&data);
        let unpacked = PackedCodec::unpack(&packed, 1).unwrap();
        assert_eq!(data, unpacked);
    }

    #[test]
    fn test_pack_unpack_empty() {
        let data = vec![];
        let packed = PackedCodec::pack(&data);
        assert!(packed.is_empty());
        let unpacked = PackedCodec::unpack(&packed, 0).unwrap();
        assert!(unpacked.is_empty());
    }

    #[test]
    fn test_compression_ratio_sparse() {
        // Mostly zeros — should compress well
        let mut data = vec![0u8; 1000];
        data[100] = 1;
        data[500] = 2;
        data[900] = 3;
        let packed = PackedCodec::pack(&data);
        let ratio = PackedCodec::compression_ratio(data.len(), packed.len());
        assert!(ratio > 3.0, "Expected ratio > 3, got {ratio}");
    }

    #[test]
    fn test_compression_ratio_dense() {
        // All non-zero — should be slightly larger (tag overhead)
        let data: Vec<u8> = (0..100).map(|i| (i % 255 + 1) as u8).collect();
        let packed = PackedCodec::pack(&data);
        let ratio = PackedCodec::compression_ratio(data.len(), packed.len());
        // Worst case: 10/8 = 1.25x overhead
        assert!(ratio < 1.0, "Dense data should not compress");
    }

    #[test]
    fn test_roundtrip_various_sizes() {
        for size in [1, 2, 7, 8, 9, 15, 16, 100, 1024, 65536] {
            let data: Vec<u8> = (0..size).map(|i| (i * 37 + 13) as u8).collect();
            let packed = PackedCodec::pack(&data);
            let unpacked = PackedCodec::unpack(&packed, size).unwrap();
            assert_eq!(data, unpacked, "Roundtrip failed for size {size}");
        }
    }

    #[test]
    fn test_unpack_rejects_truncated_input() {
        assert!(PackedCodec::unpack(&[], 8).is_err());
        assert!(PackedCodec::unpack(&[0], 16).is_err());
        assert!(PackedCodec::unpack(&[0xFF, 1, 2], 8).is_err());
    }
}
