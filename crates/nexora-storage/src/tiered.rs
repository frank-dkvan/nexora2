//! Tiered store manager — orchestrates Hot/Warm/Cold storage tiers
//! with automatic lifecycle management.
//!
//! ```text
//!   Write → Hot (RocksDB)
//!   Age > 1h → Warm (S3)
//!   Age > 30d → Cold (Iceberg/Parquet)
//! ```

use crate::{ObjectMeta, StorageBackend, StorageError, StorageTier};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Lifecycle rule for automatic tier migration.
#[derive(Clone, Debug)]
pub struct LifecycleRule {
    /// Source tier
    pub from: StorageTier,
    /// Target tier
    pub to: StorageTier,
    /// Age in milliseconds after which to migrate
    pub age_ms: u64,
}

impl LifecycleRule {
    pub fn hot_to_warm() -> Self {
        Self {
            from: StorageTier::Hot,
            to: StorageTier::Warm,
            age_ms: 3_600_000, // 1 hour
        }
    }

    pub fn warm_to_cold() -> Self {
        Self {
            from: StorageTier::Warm,
            to: StorageTier::Cold,
            age_ms: 2_592_000_000, // 30 days
        }
    }
}

/// Manages multiple storage tiers with lifecycle policies.
pub struct TieredStore {
    hot: Arc<dyn StorageBackend>,
    warm: Arc<dyn StorageBackend>,
    cold: Arc<dyn StorageBackend>,
    rules: Vec<LifecycleRule>,
}

impl TieredStore {
    /// Create a new tiered store with the given backends and default lifecycle rules.
    pub fn new(
        hot: Arc<dyn StorageBackend>,
        warm: Arc<dyn StorageBackend>,
        cold: Arc<dyn StorageBackend>,
    ) -> Self {
        Self {
            hot,
            warm,
            cold,
            rules: vec![LifecycleRule::hot_to_warm(), LifecycleRule::warm_to_cold()],
        }
    }

    /// Create with custom lifecycle rules.
    pub fn with_rules(
        hot: Arc<dyn StorageBackend>,
        warm: Arc<dyn StorageBackend>,
        cold: Arc<dyn StorageBackend>,
        rules: Vec<LifecycleRule>,
    ) -> Self {
        Self {
            hot,
            warm,
            cold,
            rules,
        }
    }

    /// Write to the hot tier.
    pub async fn put(&self, path: &str, data: bytes::Bytes) -> Result<(), StorageError> {
        self.hot.put(path, data).await
    }

    /// Read from any tier (tries hot → warm → cold).
    pub async fn get(&self, path: &str) -> Result<bytes::Bytes, StorageError> {
        // Try hot first
        if let Ok(data) = self.hot.get(path).await {
            return Ok(data);
        }
        // Try warm
        if let Ok(data) = self.warm.get(path).await {
            return Ok(data);
        }
        // Try cold
        self.cold.get(path).await
    }

    /// Check existence across all tiers.
    pub async fn exists(&self, path: &str) -> bool {
        self.hot.exists(path).await.unwrap_or(false)
            || self.warm.exists(path).await.unwrap_or(false)
            || self.cold.exists(path).await.unwrap_or(false)
    }

    /// Delete from all tiers.
    pub async fn delete(&self, path: &str) -> Result<(), StorageError> {
        let _ = self.hot.delete(path).await;
        let _ = self.warm.delete(path).await;
        let _ = self.cold.delete(path).await;
        Ok(())
    }

    /// List objects across all tiers.
    pub async fn list_all(&self, prefix: &str) -> Vec<ObjectMeta> {
        let mut results = Vec::new();
        if let Ok(list) = self.hot.list(prefix).await {
            results.extend(list);
        }
        if let Ok(list) = self.warm.list(prefix).await {
            results.extend(list);
        }
        if let Ok(list) = self.cold.list(prefix).await {
            results.extend(list);
        }
        results
    }

    /// Run lifecycle migration: move objects that have aged past the threshold.
    pub async fn run_lifecycle(&self) -> Result<usize, StorageError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let mut migrated = 0;

        for rule in &self.rules {
            let source = self.get_backend(rule.from);
            let target = self.get_backend(rule.to);

            if let Ok(objects) = source.list("").await {
                for obj in objects {
                    if now > obj.last_modified_ms && (now - obj.last_modified_ms) > rule.age_ms {
                        // Migrate: read from source, write to target, delete from source
                        if let Ok(data) = source.get(&obj.path).await {
                            if target.put(&obj.path, data).await.is_ok() {
                                let _ = source.delete(&obj.path).await;
                                migrated += 1;
                                tracing::info!(
                                    "Migrated {} from {:?} to {:?}",
                                    obj.path,
                                    rule.from,
                                    rule.to
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(migrated)
    }

    /// Explicitly promote an object from warm/cold to hot.
    pub async fn promote(&self, path: &str) -> Result<(), StorageError> {
        if self.hot.exists(path).await.unwrap_or(false) {
            return Ok(()); // Already hot
        }

        let data = if let Ok(d) = self.warm.get(path).await {
            d
        } else if let Ok(d) = self.cold.get(path).await {
            d
        } else {
            return Err(StorageError::NotFound(path.to_string()));
        };

        self.hot.put(path, data).await
    }

    /// Explicitly demote an object from hot to warm.
    pub async fn demote(&self, path: &str) -> Result<(), StorageError> {
        let data = self.hot.get(path).await?;
        self.warm.put(path, data).await?;
        self.hot.delete(path).await?;
        Ok(())
    }

    /// Which tier currently holds an object, checked hot → warm → cold. Returns
    /// `None` if the object is not present in any tier. Used by callers (e.g. the
    /// fragment lifecycle) to report where a fragment's data physically lives.
    pub async fn tier_of(&self, path: &str) -> Option<StorageTier> {
        if self.hot.exists(path).await.unwrap_or(false) {
            Some(StorageTier::Hot)
        } else if self.warm.exists(path).await.unwrap_or(false) {
            Some(StorageTier::Warm)
        } else if self.cold.exists(path).await.unwrap_or(false) {
            Some(StorageTier::Cold)
        } else {
            None
        }
    }

    /// Get the backend for a specific tier.
    fn get_backend(&self, tier: StorageTier) -> &Arc<dyn StorageBackend> {
        match tier {
            StorageTier::Hot => &self.hot,
            StorageTier::Warm => &self.warm,
            StorageTier::Cold => &self.cold,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStorage;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_tiered_put_get() {
        let hot = Arc::new(MemoryStorage::new(StorageTier::Hot));
        let warm = Arc::new(MemoryStorage::new(StorageTier::Warm));
        let cold = Arc::new(MemoryStorage::new(StorageTier::Cold));

        let store = TieredStore::new(hot, warm, cold);

        store.put("test.bin", Bytes::from("hello")).await.unwrap();
        assert!(store.exists("test.bin").await);

        let data = store.get("test.bin").await.unwrap();
        assert_eq!(data, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_demote_and_promote() {
        let hot = Arc::new(MemoryStorage::new(StorageTier::Hot));
        let warm = Arc::new(MemoryStorage::new(StorageTier::Warm));
        let cold = Arc::new(MemoryStorage::new(StorageTier::Cold));

        let store = TieredStore::new(hot.clone(), warm.clone(), cold);

        store
            .put("data.parquet", Bytes::from("parquet data"))
            .await
            .unwrap();
        assert!(hot.exists("data.parquet").await.unwrap());

        // Demote to warm
        store.demote("data.parquet").await.unwrap();
        assert!(!hot.exists("data.parquet").await.unwrap());
        assert!(warm.exists("data.parquet").await.unwrap());

        // Promote back to hot
        store.promote("data.parquet").await.unwrap();
        assert!(hot.exists("data.parquet").await.unwrap());
    }

    #[tokio::test]
    async fn test_get_from_warm() {
        let hot = Arc::new(MemoryStorage::new(StorageTier::Hot));
        let warm = Arc::new(MemoryStorage::new(StorageTier::Warm));
        let cold = Arc::new(MemoryStorage::new(StorageTier::Cold));

        // Pre-populate warm tier
        warm.put("warm-only.bin", Bytes::from("from warm"))
            .await
            .unwrap();

        let store = TieredStore::new(hot, warm, cold);
        let data = store.get("warm-only.bin").await.unwrap();
        assert_eq!(data, Bytes::from("from warm"));
    }

    #[tokio::test]
    async fn test_get_from_cold() {
        let hot = Arc::new(MemoryStorage::new(StorageTier::Hot));
        let warm = Arc::new(MemoryStorage::new(StorageTier::Warm));
        let cold = Arc::new(MemoryStorage::new(StorageTier::Cold));

        // Pre-populate cold tier only
        cold.put("cold-only.bin", Bytes::from("from cold"))
            .await
            .unwrap();

        let store = TieredStore::new(hot, warm, cold);
        let data = store.get("cold-only.bin").await.unwrap();
        assert_eq!(data, Bytes::from("from cold"));
    }

    #[tokio::test]
    async fn test_delete_across_tiers() {
        let hot = Arc::new(MemoryStorage::new(StorageTier::Hot));
        let warm = Arc::new(MemoryStorage::new(StorageTier::Warm));
        let cold = Arc::new(MemoryStorage::new(StorageTier::Cold));

        hot.put("multi.bin", Bytes::from("hot")).await.unwrap();
        warm.put("multi.bin", Bytes::from("warm")).await.unwrap();
        cold.put("multi.bin", Bytes::from("cold")).await.unwrap();

        let store = TieredStore::new(hot, warm, cold);
        assert!(store.exists("multi.bin").await);

        store.delete("multi.bin").await.unwrap();
        assert!(!store.exists("multi.bin").await);
    }

    #[tokio::test]
    async fn test_lifecycle_migration() {
        let hot = Arc::new(MemoryStorage::new(StorageTier::Hot));
        let warm = Arc::new(MemoryStorage::new(StorageTier::Warm));
        let cold = Arc::new(MemoryStorage::new(StorageTier::Cold));

        // Put an old object in hot (with timestamp 0 = very old)
        hot.put("old.bin", Bytes::from("old data")).await.unwrap();

        // Use a very short age threshold for testing
        let rules = vec![LifecycleRule {
            from: StorageTier::Hot,
            to: StorageTier::Warm,
            age_ms: 1, // 1ms — anything will qualify
        }];

        let store = TieredStore::with_rules(hot, warm, cold, rules);
        let migrated = store.run_lifecycle().await.unwrap();
        assert!(migrated >= 1);
    }
}
