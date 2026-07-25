//! NOTE: This crate is planned for future integration.
//! Tiered storage abstraction — Hot/Warm/Cold layers.
//!
//! Inspired by TileDB's VFS (Virtual File System) and RisingWave's Hummock.
//!
//! Layer model:
//! ```text
//!   Hot  (RocksDB)     → <1ms,  active graph state
//!   Warm (S3/Parquet)  → <100ms, archived fragments
//!   Cold (Iceberg)     → <30s,  historical analytics
//! ```

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

/// Storage tier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTier {
    /// Local SSD (RocksDB) — active state
    Hot,
    /// Object store (S3/GCS/Azure) — archived fragments
    Warm,
    /// Columnar format (Iceberg/Parquet) — analytics
    Cold,
}

/// Object metadata returned by the store.
#[derive(Clone, Debug)]
pub struct ObjectMeta {
    pub path: String,
    pub size: u64,
    pub tier: StorageTier,
    pub last_modified_ms: u64,
    pub content_type: Option<String>,
}

/// Abstract storage backend — like TileDB's VFS trait.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Store an object at the given path.
    async fn put(&self, path: &str, data: Bytes) -> Result<(), StorageError>;

    /// Retrieve an object by path.
    async fn get(&self, path: &str) -> Result<Bytes, StorageError>;

    /// Get object metadata without reading the full object.
    async fn head(&self, path: &str) -> Result<ObjectMeta, StorageError>;

    /// List objects with a prefix.
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StorageError>;

    /// Delete an object.
    async fn delete(&self, path: &str) -> Result<(), StorageError>;

    /// Check if an object exists.
    async fn exists(&self, path: &str) -> Result<bool, StorageError>;

    /// The tier this backend serves.
    fn tier(&self) -> StorageTier;

    /// Name of this backend (for logging).
    fn name(&self) -> &str;
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("backend error: {0}")]
    Backend(String),
}

/// Local filesystem storage backend (Hot/Warm).
pub struct LocalStorage {
    base_path: PathBuf,
    tier: StorageTier,
}

impl LocalStorage {
    pub fn new(base_path: impl Into<PathBuf>, tier: StorageTier) -> Self {
        let path = base_path.into();
        std::fs::create_dir_all(&path).ok();
        Self {
            base_path: path,
            tier,
        }
    }
}

#[async_trait]
impl StorageBackend for LocalStorage {
    async fn put(&self, path: &str, data: Bytes) -> Result<(), StorageError> {
        let full = self.base_path.join(path);
        if let Some(parent) = full.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&full, &data).await?;
        tracing::debug!("Wrote {} bytes to {}", data.len(), full.display());
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Bytes, StorageError> {
        let full = self.base_path.join(path);
        let data = tokio::fs::read(&full).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(path.to_string())
            } else {
                StorageError::Io(e)
            }
        })?;
        Ok(Bytes::from(data))
    }

    async fn head(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        let full = self.base_path.join(path);
        let meta = tokio::fs::metadata(&full).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(path.to_string())
            } else {
                StorageError::Io(e)
            }
        })?;
        Ok(ObjectMeta {
            path: path.to_string(),
            size: meta.len(),
            tier: self.tier,
            last_modified_ms: meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            content_type: None,
        })
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StorageError> {
        let dir = self.base_path.join(prefix);
        if !dir.exists() {
            return Ok(vec![]);
        }
        let mut entries = tokio::fs::read_dir(&dir).await?;
        let mut results = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path_str = entry
                .path()
                .strip_prefix(&self.base_path)
                .unwrap_or(&entry.path())
                .to_string_lossy()
                .to_string();
            let meta = entry.metadata().await?;
            results.push(ObjectMeta {
                path: path_str,
                size: meta.len(),
                tier: self.tier,
                last_modified_ms: 0,
                content_type: None,
            });
        }
        Ok(results)
    }

    async fn delete(&self, path: &str) -> Result<(), StorageError> {
        let full = self.base_path.join(path);
        tokio::fs::remove_file(&full).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(path.to_string())
            } else {
                StorageError::Io(e)
            }
        })?;
        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool, StorageError> {
        Ok(self.base_path.join(path).exists())
    }

    fn tier(&self) -> StorageTier {
        self.tier
    }
    fn name(&self) -> &str {
        "local"
    }
}

/// Memory storage backend (for testing).
pub struct MemoryStorage {
    data: Arc<tokio::sync::RwLock<std::collections::HashMap<String, Bytes>>>,
    tier: StorageTier,
}

impl MemoryStorage {
    pub fn new(tier: StorageTier) -> Self {
        Self {
            data: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            tier,
        }
    }
}

#[async_trait]
impl StorageBackend for MemoryStorage {
    async fn put(&self, path: &str, data: Bytes) -> Result<(), StorageError> {
        self.data.write().await.insert(path.to_string(), data);
        Ok(())
    }
    async fn get(&self, path: &str) -> Result<Bytes, StorageError> {
        self.data
            .read()
            .await
            .get(path)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(path.to_string()))
    }
    async fn head(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        let map = self.data.read().await;
        let data = map
            .get(path)
            .ok_or_else(|| StorageError::NotFound(path.to_string()))?;
        Ok(ObjectMeta {
            path: path.to_string(),
            size: data.len() as u64,
            tier: self.tier,
            last_modified_ms: 0,
            content_type: None,
        })
    }
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StorageError> {
        let map = self.data.read().await;
        Ok(map
            .keys()
            .filter(|k| k.starts_with(prefix))
            .map(|k| ObjectMeta {
                path: k.clone(),
                size: map.get(k).map(|d| d.len() as u64).unwrap_or(0),
                tier: self.tier,
                last_modified_ms: 0,
                content_type: None,
            })
            .collect())
    }
    async fn delete(&self, path: &str) -> Result<(), StorageError> {
        self.data.write().await.remove(path);
        Ok(())
    }
    async fn exists(&self, path: &str) -> Result<bool, StorageError> {
        Ok(self.data.read().await.contains_key(path))
    }
    fn tier(&self) -> StorageTier {
        self.tier
    }
    fn name(&self) -> &str {
        "memory"
    }
}

pub mod filter_pipeline;
pub mod s3;
#[cfg(feature = "opendal-backend")]
pub mod s3_opendal;
pub mod tiered;

pub use filter_pipeline::{
    Aes256GcmFilter, ByteShuffleFilter, Filter, FilterError, FilterPipeline, ZstdFilter,
};
pub use s3::{MockS3Storage, S3Config, S3Storage};
#[cfg(feature = "opendal-backend")]
pub use s3_opendal::S3StorageOpenDal;
pub use tiered::{LifecycleRule, TieredStore};

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_storage_roundtrip() {
        let store = MemoryStorage::new(StorageTier::Hot);
        store.put("test.txt", Bytes::from("hello")).await.unwrap();
        let data = store.get("test.txt").await.unwrap();
        assert_eq!(data, Bytes::from("hello"));
        assert!(store.exists("test.txt").await.unwrap());
        store.delete("test.txt").await.unwrap();
        assert!(!store.exists("test.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_local_storage_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStorage::new(dir.path(), StorageTier::Warm);
        store
            .put("data/test.bin", Bytes::from("world"))
            .await
            .unwrap();
        let data = store.get("data/test.bin").await.unwrap();
        assert_eq!(data, Bytes::from("world"));
    }

    #[tokio::test]
    async fn test_list_objects() {
        let store = MemoryStorage::new(StorageTier::Hot);
        store.put("a/1.txt", Bytes::from("1")).await.unwrap();
        store.put("a/2.txt", Bytes::from("2")).await.unwrap();
        store.put("b/3.txt", Bytes::from("3")).await.unwrap();
        let list = store.list("a/").await.unwrap();
        assert_eq!(list.len(), 2);
    }
}
