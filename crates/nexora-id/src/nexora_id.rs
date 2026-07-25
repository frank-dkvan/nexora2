use serde::Serialize;
use std::fmt;
use xxhash_rust::xxh3::xxh3_64;

/// Opaque identifier for a graph node.
///
/// Internally stored as a byte vector. The Nexora uses 16-byte UUIDs,
/// but the wire format supports arbitrary byte sequences for compatibility
/// with external ID systems.
///
/// # Deterministic Sharding
///
/// `shard_key()` returns a stable `u64` derived from the byte content via xxHash3.
/// This is used by `GraphShard` to route operations to the correct shard without
/// needing to hash the full bytes each time.
#[derive(Clone)]
pub struct NexoraId {
    bytes: Vec<u8>,
    /// Cached hash for shard routing. Computed once on construction.
    shard_key: u64,
}

// Implement PartialEq/Eq/Hash based only on bytes (shard_key is a derived cache)
impl PartialEq for NexoraId {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}
impl Eq for NexoraId {}

impl std::hash::Hash for NexoraId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

impl Serialize for NexoraId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.bytes)
    }
}

impl<'de> serde::Deserialize<'de> for NexoraId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = serde::Deserialize::deserialize(deserializer)?;
        Ok(Self::from_bytes(bytes))
    }
}

impl NexoraId {
    /// Create a NexoraId from raw bytes.
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        let shard_key = xxh3_64(&bytes);
        Self { bytes, shard_key }
    }

    /// Create a NexoraId from a hex-encoded string (the wire format used by the HTTP API).
    pub fn from_hex(hex_str: &str) -> Result<Self, hex::FromHexError> {
        let bytes = hex::decode(hex_str)?;
        Ok(Self::from_bytes(bytes))
    }

    /// Create a NexoraId from a 128-bit UUID (the standard generation method).
    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self::from_bytes(uuid.as_bytes().to_vec())
    }

    /// Generate a new random NexoraId.
    pub fn new_random() -> Self {
        Self::from_uuid(uuid::Uuid::new_v4())
    }

    /// The raw byte content of this ID.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Hex-encoded representation (used in API responses and key expressions).
    pub fn to_hex(&self) -> String {
        hex::encode(&self.bytes)
    }

    /// Stable hash key for shard routing.
    ///
    /// This is a xxHash3 of the byte content, computed once at construction time.
    /// Used by `GraphShard` to determine which shard owns this node:
    /// `shard_id = qid.shard_key() % num_shards`
    pub fn shard_key(&self) -> u64 {
        self.shard_key
    }

    /// The length of the ID in bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether this ID is empty (should not happen in practice).
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl fmt::Debug for NexoraId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NexoraId({})", self.to_hex())
    }
}

impl fmt::Display for NexoraId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl AsRef<[u8]> for NexoraId {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_bytes_roundtrip() {
        let bytes = vec![0x01, 0x02, 0x03, 0x04];
        let qid = NexoraId::from_bytes(bytes.clone());
        assert_eq!(qid.as_bytes(), &bytes);
        assert_eq!(qid.to_hex(), "01020304");
    }

    #[test]
    fn test_from_hex() {
        let qid = NexoraId::from_hex("a1b2c3d4").unwrap();
        assert_eq!(qid.as_bytes(), &[0xa1, 0xb2, 0xc3, 0xd4]);
    }

    #[test]
    fn test_from_uuid() {
        let uuid = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let qid = NexoraId::from_uuid(uuid);
        assert_eq!(qid.len(), 16);
        assert_eq!(qid.to_hex(), "550e8400e29b41d4a716446655440000");
    }

    #[test]
    fn test_random_ids_are_unique() {
        // UID-005: 1000 random IDs must all be unique
        let mut ids = std::collections::HashSet::new();
        for _ in 0..1000 {
            let qid = NexoraId::new_random();
            assert_eq!(qid.len(), 16);
            assert!(ids.insert(qid), "duplicate NexoraId detected");
        }
    }

    #[test]
    fn test_shard_key_deterministic() {
        let qid = NexoraId::from_bytes(vec![1, 2, 3, 4]);
        let key1 = qid.shard_key();
        let key2 = qid.shard_key();
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_shard_key_different_for_different_ids() {
        let qid1 = NexoraId::from_bytes(vec![1, 2, 3]);
        let qid2 = NexoraId::from_bytes(vec![4, 5, 6]);
        // Not guaranteed to be different for all inputs, but extremely likely
        assert_ne!(qid1.shard_key(), qid2.shard_key());
    }

    #[test]
    fn test_serde_roundtrip() {
        let qid = NexoraId::new_random();
        let json = serde_json::to_string(&qid).unwrap();
        let restored: NexoraId = serde_json::from_str(&json).unwrap();
        assert_eq!(qid, restored);
    }

    #[test]
    fn test_display_shows_hex() {
        let qid = NexoraId::from_bytes(vec![0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(format!("{qid}"), "deadbeef");
    }

    // ====== UID-003: from_hex 无效输入 ======
    #[test]
    fn test_from_hex_invalid() {
        let result = NexoraId::from_hex("zzzz");
        assert!(result.is_err());
    }

    // ====== UID-010: PartialEq 只比较 bytes ======
    #[test]
    fn test_partial_eq_ignores_shard_key() {
        let bytes = vec![1, 2, 3, 4];
        let qid1 = NexoraId::from_bytes(bytes.clone());
        let qid2 = NexoraId::from_bytes(bytes);
        // shard_key 可能相同也可能不同，但 PartialEq 只比较 bytes
        assert_eq!(qid1, qid2);
    }

    // ====== UID-011: HashMap 查找一致性 ======
    #[test]
    fn test_hashmap_lookup_consistency() {
        let mut map = std::collections::HashMap::new();
        let qid = NexoraId::from_bytes(vec![5, 6, 7, 8]);
        map.insert(qid.clone(), 42);
        // 通过相同 bytes 创建的 NexoraId 应能找到
        let qid2 = NexoraId::from_bytes(vec![5, 6, 7, 8]);
        assert_eq!(map.get(&qid2), Some(&42));
    }

    // ====== UID-013: 空 bytes ======
    #[test]
    fn test_empty_nexora_id() {
        let qid = NexoraId::from_bytes(vec![]);
        assert!(qid.is_empty());
        assert_eq!(qid.len(), 0);
        assert_eq!(qid.to_hex(), "");
    }
}
