//! RocksDB-backed offset store (feature `rocksdb-offsets`) for durable
//! checkpoint persistence across restarts.
//!
//! This implementation of [`OffsetStore`] writes both numeric offsets and string
//! checkpoints (e.g. Kinesis sequence numbers) to a local RocksDB, keyed by
//! `(topic, partition)`. The database persists to disk, so committed offsets
//! survive process restarts — Kafka/File/Kinesis/Pulsar sources resume exactly
//! where they left off.
//!
//! Two column families keep numeric and string checkpoints in separate
//! namespaces: `CF_NUMERIC_OFFSETS` and `CF_STRING_OFFSETS`. Both are keyed by
//! `{topic}:{partition}` (string). The value for numeric is a BE `u64`; for
//! string it's the raw checkpoint bytes (UTF-8).

use crate::OffsetStore;
use rust_rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::path::Path;
use std::sync::Arc;

const CF_NUMERIC_OFFSETS: &str = "numeric_offsets";
const CF_STRING_OFFSETS: &str = "string_offsets";

/// RocksDB-backed offset store, suitable for production deployments that need
/// durable checkpoint persistence.
pub struct RocksDbOffsetStore {
    db: Arc<DB>,
}

impl RocksDbOffsetStore {
    /// Open or create a RocksDB at the given path with the offset column families.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_numeric = ColumnFamilyDescriptor::new(CF_NUMERIC_OFFSETS, Options::default());
        let cf_string = ColumnFamilyDescriptor::new(CF_STRING_OFFSETS, Options::default());

        let db = DB::open_cf_descriptors(&opts, path, vec![cf_numeric, cf_string])
            .map_err(|e| format!("RocksDB open: {e}"))?;

        Ok(Self { db: Arc::new(db) })
    }

    fn key(topic: &str, partition: &str) -> String {
        format!("{topic}:{partition}")
    }
}

#[async_trait::async_trait]
impl OffsetStore for RocksDbOffsetStore {
    async fn save(&self, topic: &str, partition: &str, offset: u64) -> Result<(), String> {
        let cf = self
            .db
            .cf_handle(CF_NUMERIC_OFFSETS)
            .ok_or_else(|| format!("CF {CF_NUMERIC_OFFSETS} not found"))?;
        let key = Self::key(topic, partition);
        self.db
            .put_cf(cf, key.as_bytes(), offset.to_be_bytes())
            .map_err(|e| format!("RocksDB put: {e}"))
    }

    async fn load(&self, topic: &str, partition: &str) -> Result<Option<u64>, String> {
        let cf = self
            .db
            .cf_handle(CF_NUMERIC_OFFSETS)
            .ok_or_else(|| format!("CF {CF_NUMERIC_OFFSETS} not found"))?;
        let key = Self::key(topic, partition);
        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(bytes)) if bytes.len() == 8 => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes);
                Ok(Some(u64::from_be_bytes(buf)))
            }
            Ok(Some(_)) => Err("corrupt numeric offset (not 8 bytes)".into()),
            Ok(None) => Ok(None),
            Err(e) => Err(format!("RocksDB get: {e}")),
        }
    }

    async fn load_all(
        &self,
    ) -> Result<Vec<(String, String, u64, chrono::DateTime<chrono::Utc>)>, String> {
        let cf = self
            .db
            .cf_handle(CF_NUMERIC_OFFSETS)
            .ok_or_else(|| format!("CF {CF_NUMERIC_OFFSETS} not found"))?;
        let now = chrono::Utc::now();
        let mut result = Vec::new();

        let iter = self.db.iterator_cf(cf, rust_rocksdb::IteratorMode::Start);
        for item in iter {
            let (key_bytes, val_bytes) = item.map_err(|e| format!("RocksDB iter: {e}"))?;
            let key = String::from_utf8_lossy(&key_bytes).to_string();
            let mut parts = key.splitn(2, ':');
            let topic = parts.next().unwrap_or("").to_string();
            let partition = parts.next().unwrap_or("").to_string();
            if val_bytes.len() == 8 {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&val_bytes);
                let offset = u64::from_be_bytes(buf);
                result.push((topic, partition, offset, now));
            }
        }
        Ok(result)
    }

    async fn save_str(&self, topic: &str, partition: &str, value: &str) -> Result<(), String> {
        let cf = self
            .db
            .cf_handle(CF_STRING_OFFSETS)
            .ok_or_else(|| format!("CF {CF_STRING_OFFSETS} not found"))?;
        let key = Self::key(topic, partition);
        self.db
            .put_cf(cf, key.as_bytes(), value.as_bytes())
            .map_err(|e| format!("RocksDB put_str: {e}"))
    }

    async fn load_str(&self, topic: &str, partition: &str) -> Result<Option<String>, String> {
        let cf = self
            .db
            .cf_handle(CF_STRING_OFFSETS)
            .ok_or_else(|| format!("CF {CF_STRING_OFFSETS} not found"))?;
        let key = Self::key(topic, partition);
        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(bytes)) => {
                let s =
                    String::from_utf8(bytes).map_err(|e| format!("non-UTF8 checkpoint: {e}"))?;
                Ok(Some(s))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(format!("RocksDB get_str: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn numeric_offset_roundtrip() {
        let dir = TempDir::new().unwrap();
        let store = RocksDbOffsetStore::open(dir.path()).unwrap();
        store.save("t1", "0", 42).await.unwrap();
        assert_eq!(store.load("t1", "0").await.unwrap(), Some(42));
        assert_eq!(store.load("t1", "1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn string_checkpoint_roundtrip() {
        let dir = TempDir::new().unwrap();
        let store = RocksDbOffsetStore::open(dir.path()).unwrap();
        let seq = "49590338271490256608559692538361571095921575989136588898";
        store.save_str("stream", "shard-0", seq).await.unwrap();
        assert_eq!(
            store
                .load_str("stream", "shard-0")
                .await
                .unwrap()
                .as_deref(),
            Some(seq)
        );
    }

    #[tokio::test]
    async fn numeric_and_string_namespaces_independent() {
        let dir = TempDir::new().unwrap();
        let store = RocksDbOffsetStore::open(dir.path()).unwrap();
        store.save("t", "p", 7).await.unwrap();
        store.save_str("t", "p", "seq-999").await.unwrap();
        assert_eq!(store.load("t", "p").await.unwrap(), Some(7));
        assert_eq!(
            store.load_str("t", "p").await.unwrap().as_deref(),
            Some("seq-999")
        );
    }

    #[tokio::test]
    async fn persists_across_close_and_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let store = RocksDbOffsetStore::open(dir.path()).unwrap();
            store.save("kafka", "3", 1234).await.unwrap();
            store
                .save_str("kinesis", "shard-0", "seq-abc")
                .await
                .unwrap();
        }
        // Reopen same path.
        let store = RocksDbOffsetStore::open(dir.path()).unwrap();
        assert_eq!(store.load("kafka", "3").await.unwrap(), Some(1234));
        assert_eq!(
            store
                .load_str("kinesis", "shard-0")
                .await
                .unwrap()
                .as_deref(),
            Some("seq-abc")
        );
    }

    #[tokio::test]
    async fn load_all_lists_numeric_offsets() {
        let dir = TempDir::new().unwrap();
        let store = RocksDbOffsetStore::open(dir.path()).unwrap();
        store.save("t1", "0", 10).await.unwrap();
        store.save("t1", "1", 20).await.unwrap();
        store.save("t2", "0", 30).await.unwrap();
        let all = store.load_all().await.unwrap();
        assert_eq!(all.len(), 3);
        assert!(all
            .iter()
            .any(|(t, p, o, _)| t == "t1" && p == "0" && *o == 10));
        assert!(all
            .iter()
            .any(|(t, p, o, _)| t == "t1" && p == "1" && *o == 20));
        assert!(all
            .iter()
            .any(|(t, p, o, _)| t == "t2" && p == "0" && *o == 30));
    }
}
