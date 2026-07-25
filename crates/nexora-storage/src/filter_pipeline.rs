//! B10: Filter Pipeline — pluggable compression/encryption chain (借鉴 TileDB).
//!
//! A [`FilterPipeline`] is an ordered list of [`Filter`]s applied to a byte
//! buffer on write (`encode`) and reversed on read (`decode`). Each filter
//! transforms bytes → bytes, so filters compose: e.g. shuffle → compress →
//! encrypt on the way out, decrypt → decompress → unshuffle on the way in.
//!
//! Unlike the WAL's hard-coded Zstd+AES, this is configurable: a user picks the
//! chain (and its order) per use case — a cold-tier archive might use
//! `[BitShuffle, Zstd(19), Aes256Gcm]` for max ratio + at-rest encryption, while
//! a hot cache might use `[Zstd(1)]` for speed, or nothing at all.
//!
//! ## Ordering contract
//!
//! `decode` runs the filters in REVERSE order, each filter's `decode` undoing
//! its `encode`. So a pipeline `[A, B, C]` produces `C(B(A(data)))` on encode
//! and recovers `data` via `A⁻¹(B⁻¹(C⁻¹(...)))` on decode. Encryption therefore
//! belongs LAST in the list (outermost) so compression sees plaintext (ciphertext
//! doesn't compress), and integrity/checksum — if used — should wrap the final
//! bytes.

use std::fmt;

/// Error from a filter's encode/decode step.
#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    #[error("compression error: {0}")]
    Compression(String),
    #[error("decompression error: {0}")]
    Decompression(String),
    #[error("encryption error: {0}")]
    Encryption(String),
    #[error("decryption error: {0}")]
    Decryption(String),
    #[error("corrupt data: {0}")]
    Corrupt(String),
}

/// A single reversible byte transform in a [`FilterPipeline`].
///
/// `encode` and `decode` must be exact inverses: `decode(encode(x)) == x` for
/// all `x`. Filters are `Send + Sync` so a pipeline can be shared across tasks.
pub trait Filter: Send + Sync {
    /// Transform bytes on the write path.
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, FilterError>;
    /// Reverse the transform on the read path.
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, FilterError>;
    /// Short name for logging/debugging.
    fn name(&self) -> &str;
}

/// An ordered chain of filters applied on write and reversed on read.
#[derive(Default)]
pub struct FilterPipeline {
    filters: Vec<Box<dyn Filter>>,
}

impl fmt::Debug for FilterPipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilterPipeline")
            .field(
                "filters",
                &self.filters.iter().map(|f| f.name()).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl FilterPipeline {
    /// An empty pipeline — `encode`/`decode` return the input unchanged.
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
        }
    }

    /// Append a filter to the chain (builder style). Order matters: the first
    /// added runs first on encode, last on decode.
    // Named `add` for a fluent builder; it is not `std::ops::Add` (takes a
    // boxed filter, returns Self by value), so the trait-confusion lint is a
    // false positive here.
    #[allow(clippy::should_implement_trait)]
    pub fn add(mut self, filter: Box<dyn Filter>) -> Self {
        self.filters.push(filter);
        self
    }

    /// Number of filters in the chain.
    pub fn len(&self) -> usize {
        self.filters.len()
    }

    /// Whether the chain is empty (a no-op pipeline).
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    /// Apply every filter in order on the write path.
    pub fn encode(&self, data: &[u8]) -> Result<Vec<u8>, FilterError> {
        let mut buf = data.to_vec();
        for filter in &self.filters {
            buf = filter.encode(&buf)?;
        }
        Ok(buf)
    }

    /// Reverse every filter (in reverse order) on the read path.
    pub fn decode(&self, data: &[u8]) -> Result<Vec<u8>, FilterError> {
        let mut buf = data.to_vec();
        for filter in self.filters.iter().rev() {
            buf = filter.decode(&buf)?;
        }
        Ok(buf)
    }

    /// The filter names in chain order (for logging / schema recording).
    pub fn filter_names(&self) -> Vec<&str> {
        self.filters.iter().map(|f| f.name()).collect()
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Built-in filters
// ─────────────────────────────────────────────────────────────────────────

/// Zstandard compression at a configurable level (1 = fast, 19+ = max ratio).
pub struct ZstdFilter {
    level: i32,
}

impl ZstdFilter {
    /// New Zstd filter at `level` (clamped to zstd's valid 1..=22 range).
    pub fn new(level: i32) -> Self {
        Self {
            level: level.clamp(1, 22),
        }
    }
}

impl Filter for ZstdFilter {
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, FilterError> {
        zstd::stream::encode_all(data, self.level)
            .map_err(|e| FilterError::Compression(e.to_string()))
    }
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, FilterError> {
        zstd::stream::decode_all(data).map_err(|e| FilterError::Decompression(e.to_string()))
    }
    fn name(&self) -> &str {
        "zstd"
    }
}

/// AES-256-GCM authenticated encryption. Each `encode` prepends a fresh random
/// 12-byte nonce; `decode` reads it back. Authentication (GCM tag) detects
/// tampering/corruption on decode.
pub struct Aes256GcmFilter {
    key: [u8; 32],
}

impl Aes256GcmFilter {
    /// New filter with a 32-byte key.
    pub fn new(key: [u8; 32]) -> Self {
        Self { key }
    }
}

impl Filter for Aes256GcmFilter {
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, FilterError> {
        use aes_gcm::aead::{rand_core::RngCore, Aead, KeyInit, OsRng};
        use aes_gcm::{Aes256Gcm, Key, Nonce};

        let key = Key::<Aes256Gcm>::from_slice(&self.key);
        let cipher = Aes256Gcm::new(key);
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(nonce, data)
            .map_err(|e| FilterError::Encryption(e.to_string()))?;
        // Layout: [nonce(12)][ciphertext+tag]
        let mut out = Vec::with_capacity(12 + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, FilterError> {
        use aes_gcm::aead::{Aead, KeyInit};
        use aes_gcm::{Aes256Gcm, Key, Nonce};

        if data.len() < 12 {
            return Err(FilterError::Corrupt("ciphertext shorter than nonce".into()));
        }
        let (nonce_bytes, ciphertext) = data.split_at(12);
        let key = Key::<Aes256Gcm>::from_slice(&self.key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(nonce_bytes);
        cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| FilterError::Decryption(e.to_string()))
    }
    fn name(&self) -> &str {
        "aes256gcm"
    }
}

/// Byte-shuffle: regroups bytes by position across fixed-size elements so runs
/// of similar high bytes sit together, improving downstream compression on
/// columnar numeric data (借鉴 TileDB BitShuffle, byte-granularity variant).
///
/// The element width is stored in a 1-byte header so `decode` can reverse it.
/// A width of 1 (or data not a multiple of width) degrades to identity, so it's
/// always safe to place before a compressor.
pub struct ByteShuffleFilter {
    element_width: usize,
}

impl ByteShuffleFilter {
    /// New shuffle for `element_width`-byte elements (e.g. 8 for i64/f64).
    pub fn new(element_width: usize) -> Self {
        Self {
            element_width: element_width.clamp(1, 255),
        }
    }
}

impl Filter for ByteShuffleFilter {
    fn encode(&self, data: &[u8]) -> Result<Vec<u8>, FilterError> {
        let w = self.element_width;
        let mut out = Vec::with_capacity(1 + data.len());
        // Only shuffle when the buffer splits cleanly into `w`-byte elements
        // AND w > 1; otherwise record width 1 (identity) so decode is trivial.
        if w <= 1 || data.is_empty() || !data.len().is_multiple_of(w) {
            out.push(1);
            out.extend_from_slice(data);
            return Ok(out);
        }
        out.push(w as u8);
        let n = data.len() / w;
        // Position-major: all byte-0s, then all byte-1s, ...
        for byte_pos in 0..w {
            for elem in 0..n {
                out.push(data[elem * w + byte_pos]);
            }
        }
        Ok(out)
    }
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, FilterError> {
        if data.is_empty() {
            return Err(FilterError::Corrupt(
                "shuffle payload missing width header".into(),
            ));
        }
        let w = data[0] as usize;
        let body = &data[1..];
        if w <= 1 {
            return Ok(body.to_vec());
        }
        if !body.len().is_multiple_of(w) {
            return Err(FilterError::Corrupt(
                "shuffle body not a multiple of width".into(),
            ));
        }
        let n = body.len() / w;
        let mut out = vec![0u8; body.len()];
        for byte_pos in 0..w {
            for elem in 0..n {
                out[elem * w + byte_pos] = body[byte_pos * n + elem];
            }
        }
        Ok(out)
    }
    fn name(&self) -> &str {
        "byteshuffle"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_data() -> Vec<u8> {
        // Repetitive data so compression actually shrinks it.
        let mut v = Vec::new();
        for i in 0..1000u32 {
            v.extend_from_slice(&(i % 16).to_le_bytes());
        }
        v
    }

    #[test]
    fn empty_pipeline_is_identity() {
        let p = FilterPipeline::new();
        let data = b"hello world".to_vec();
        assert_eq!(p.encode(&data).unwrap(), data);
        assert_eq!(p.decode(&data).unwrap(), data);
        assert!(p.is_empty());
    }

    #[test]
    fn zstd_roundtrip_and_shrinks() {
        let p = FilterPipeline::new().add(Box::new(ZstdFilter::new(3)));
        let data = sample_data();
        let encoded = p.encode(&data).unwrap();
        assert!(encoded.len() < data.len(), "compressible data must shrink");
        assert_eq!(p.decode(&encoded).unwrap(), data, "zstd roundtrip");
    }

    #[test]
    fn aes_roundtrip_and_hides_plaintext() {
        let p = FilterPipeline::new().add(Box::new(Aes256GcmFilter::new([7u8; 32])));
        let data = b"secret payload that must be encrypted".to_vec();
        let encoded = p.encode(&data).unwrap();
        assert_ne!(encoded, data, "ciphertext must differ from plaintext");
        assert!(
            !encoded.windows(6).any(|w| w == b"secret"),
            "plaintext must not leak"
        );
        assert_eq!(p.decode(&encoded).unwrap(), data, "aes roundtrip");
    }

    #[test]
    fn aes_fresh_nonce_per_encode() {
        let f = Aes256GcmFilter::new([1u8; 32]);
        let data = b"same input".to_vec();
        // Random nonce → two encodes of the same input differ.
        assert_ne!(f.encode(&data).unwrap(), f.encode(&data).unwrap());
    }

    #[test]
    fn aes_detects_tampering() {
        let f = Aes256GcmFilter::new([2u8; 32]);
        let mut encoded = f.encode(b"authentic").unwrap();
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF; // flip a bit in the GCM tag region
        assert!(
            f.decode(&encoded).is_err(),
            "GCM must reject tampered ciphertext"
        );
    }

    #[test]
    fn byteshuffle_roundtrip() {
        let f = ByteShuffleFilter::new(8);
        let data: Vec<u8> = (0..80u8).collect(); // 10 elements of width 8
        let encoded = f.encode(&data).unwrap();
        assert_eq!(f.decode(&encoded).unwrap(), data, "shuffle roundtrip");
    }

    #[test]
    fn byteshuffle_identity_on_unaligned() {
        let f = ByteShuffleFilter::new(8);
        let data = b"abc".to_vec(); // 3 bytes, not a multiple of 8 → identity
        let encoded = f.encode(&data).unwrap();
        assert_eq!(f.decode(&encoded).unwrap(), data);
    }

    #[test]
    fn full_chain_shuffle_compress_encrypt() {
        // The realistic cold-tier chain: shuffle → compress → encrypt.
        let p = FilterPipeline::new()
            .add(Box::new(ByteShuffleFilter::new(4)))
            .add(Box::new(ZstdFilter::new(9)))
            .add(Box::new(Aes256GcmFilter::new([42u8; 32])));
        assert_eq!(p.len(), 3);
        assert_eq!(p.filter_names(), vec!["byteshuffle", "zstd", "aes256gcm"]);

        let data = sample_data();
        let encoded = p.encode(&data).unwrap();
        let decoded = p.decode(&encoded).unwrap();
        assert_eq!(decoded, data, "full chain roundtrip must recover original");
    }

    #[test]
    fn wrong_key_fails_decode() {
        let enc = FilterPipeline::new().add(Box::new(Aes256GcmFilter::new([1u8; 32])));
        let dec = FilterPipeline::new().add(Box::new(Aes256GcmFilter::new([9u8; 32])));
        let encoded = enc.encode(b"data").unwrap();
        assert!(dec.decode(&encoded).is_err(), "wrong key must fail");
    }
}
