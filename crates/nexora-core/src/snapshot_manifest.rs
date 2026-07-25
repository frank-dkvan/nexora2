//! B1: Unified snapshot manifest primitive.
//!
//! A single manifest type that any snapshot (node state, shard export, stream
//! checkpoint, database backup) embeds to get a consistency cut + integrity
//! check. "Don't build four snapshot systems — build one primitive, reuse it
//! five ways." (roadmap Track B)
//!
//! The manifest captures:
//! - a consistency cut (last_tx_id / timestamp): the logical point the snapshot
//!   corresponds to, so recovery knows exactly what's included
//! - integrity (checksum + byte length): detect truncation/corruption, the
//!   "manifest last" torn-write guard
//! - provenance (format version, kind): forward/backward compat + typing

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Checksum algorithm used for a manifest's payload integrity check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumKind {
    /// CRC32 (fast, adequate for torn-write detection).
    Crc32,
    /// Blake3 (cryptographic strength, for backup integrity).
    Blake3,
}

/// The kind of artifact a manifest describes (provenance/typing).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotKind {
    /// A single node's state snapshot.
    Node,
    /// A cluster shard export (state transfer).
    Shard,
    /// A stream consistency checkpoint (offset-aligned).
    StreamCheckpoint,
    /// A full database backup.
    DatabaseBackup,
}

/// Unified snapshot manifest — the consistency cut + integrity envelope.
///
/// Written as the LAST entry of a snapshot artifact (so a torn write leaves the
/// manifest absent/short → detected as incomplete on load). Reusable across all
/// snapshot kinds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    /// Manifest format version (for forward/backward compat).
    pub format_version: u32,
    /// What this snapshot describes.
    pub kind: SnapshotKind,
    /// Consistency cut: the last transaction/sequence id included. Recovery
    /// replays only what comes AFTER this point.
    pub last_tx_id: u64,
    /// Wall-clock creation time (millis since UNIX epoch).
    pub created_at_ms: u64,
    /// Length in bytes of the snapshot payload the manifest covers.
    pub payload_len: u64,
    /// Checksum algorithm.
    pub checksum_kind: ChecksumKind,
    /// Checksum of the payload (hex-encoded). Verified on load.
    pub checksum: String,
}

impl SnapshotManifest {
    pub const CURRENT_FORMAT_VERSION: u32 = 1;

    /// Build a manifest for `payload`, computing its checksum with `kind`.
    pub fn for_payload(
        snapshot_kind: SnapshotKind,
        last_tx_id: u64,
        payload: &[u8],
        checksum_kind: ChecksumKind,
    ) -> Self {
        let checksum = compute_checksum(payload, checksum_kind);
        Self {
            format_version: Self::CURRENT_FORMAT_VERSION,
            kind: snapshot_kind,
            last_tx_id,
            created_at_ms: now_ms(),
            payload_len: payload.len() as u64,
            checksum_kind,
            checksum,
        }
    }

    /// Verify `payload` matches this manifest's length + checksum. Returns Err
    /// with a description on mismatch (truncation or corruption).
    pub fn verify(&self, payload: &[u8]) -> Result<(), String> {
        if payload.len() as u64 != self.payload_len {
            return Err(format!(
                "snapshot payload length mismatch: manifest={}, actual={}",
                self.payload_len,
                payload.len()
            ));
        }
        let actual = compute_checksum(payload, self.checksum_kind);
        if actual != self.checksum {
            return Err(format!(
                "snapshot checksum mismatch: manifest={}, actual={}",
                self.checksum, actual
            ));
        }
        Ok(())
    }
}

/// Compute checksum of `payload` using the specified algorithm.
fn compute_checksum(payload: &[u8], kind: ChecksumKind) -> String {
    match kind {
        ChecksumKind::Crc32 => {
            // Use crc32fast (same as WAL in nexora-core/wal/log.rs).
            // CRC-32/ISO-HDLC polynomial (0xEDB88320, reflected).
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(payload);
            format!("{:08x}", hasher.finalize())
        }
        ChecksumKind::Blake3 => {
            // Blake3 for cryptographic-strength integrity (backups).
            let hash = blake3::hash(payload);
            hash.to_hex().to_string()
        }
    }
}

/// Get current time in milliseconds since UNIX epoch.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before UNIX epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_verify_roundtrip() {
        let payload = b"test snapshot data";
        let manifest =
            SnapshotManifest::for_payload(SnapshotKind::Node, 42, payload, ChecksumKind::Crc32);

        // Verify should pass with correct payload
        assert!(manifest.verify(payload).is_ok());
    }

    #[test]
    fn manifest_detects_truncation() {
        let payload = b"test snapshot data with some length";
        let manifest =
            SnapshotManifest::for_payload(SnapshotKind::Shard, 100, payload, ChecksumKind::Crc32);

        // Truncate the payload
        let truncated = &payload[..10];
        let result = manifest.verify(truncated);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("length mismatch"));
    }

    #[test]
    fn manifest_detects_corruption() {
        let payload = b"original data";
        let manifest = SnapshotManifest::for_payload(
            SnapshotKind::StreamCheckpoint,
            200,
            payload,
            ChecksumKind::Crc32,
        );

        // Corrupt one byte
        let mut corrupted = payload.to_vec();
        corrupted[5] = corrupted[5].wrapping_add(1);

        let result = manifest.verify(&corrupted);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("checksum mismatch"));
    }

    #[test]
    fn manifest_crc32_and_blake3() {
        let payload = b"test data for both algorithms";

        // CRC32
        let manifest_crc = SnapshotManifest::for_payload(
            SnapshotKind::DatabaseBackup,
            1000,
            payload,
            ChecksumKind::Crc32,
        );
        assert!(manifest_crc.verify(payload).is_ok());
        assert_eq!(manifest_crc.checksum_kind, ChecksumKind::Crc32);
        // CRC32 hex is 8 chars
        assert_eq!(manifest_crc.checksum.len(), 8);

        // Blake3
        let manifest_blake = SnapshotManifest::for_payload(
            SnapshotKind::DatabaseBackup,
            1000,
            payload,
            ChecksumKind::Blake3,
        );
        assert!(manifest_blake.verify(payload).is_ok());
        assert_eq!(manifest_blake.checksum_kind, ChecksumKind::Blake3);
        // Blake3 hex is 64 chars (32 bytes * 2)
        assert_eq!(manifest_blake.checksum.len(), 64);

        // Different checksums
        assert_ne!(manifest_crc.checksum, manifest_blake.checksum);
    }

    #[test]
    fn manifest_serde_roundtrip() {
        let payload = b"serialization test";
        let manifest = SnapshotManifest::for_payload(
            SnapshotKind::StreamCheckpoint,
            999,
            payload,
            ChecksumKind::Blake3,
        );

        // Serialize to JSON
        let json = serde_json::to_string(&manifest).unwrap();

        // Deserialize back
        let deserialized: SnapshotManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(manifest, deserialized);
        assert_eq!(
            deserialized.format_version,
            SnapshotManifest::CURRENT_FORMAT_VERSION
        );
        assert_eq!(deserialized.kind, SnapshotKind::StreamCheckpoint);
        assert_eq!(deserialized.last_tx_id, 999);
    }

    #[test]
    fn manifest_fields_captured_correctly() {
        let payload = b"field capture test";
        let manifest =
            SnapshotManifest::for_payload(SnapshotKind::Shard, 12345, payload, ChecksumKind::Crc32);

        assert_eq!(manifest.format_version, 1);
        assert_eq!(manifest.kind, SnapshotKind::Shard);
        assert_eq!(manifest.last_tx_id, 12345);
        assert_eq!(manifest.payload_len, payload.len() as u64);
        assert!(manifest.created_at_ms > 0);
    }
}
