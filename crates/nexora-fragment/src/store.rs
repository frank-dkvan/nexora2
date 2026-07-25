//! Fragment store: manages fragment lifecycle (write, read, list, consolidate).
//!
//! The store maintains a registry of fragments and provides time-range-based
//! query routing. Each fragment is an immutble directory in the storage backend.

use crate::fragment_id::FragmentId;
use crate::metadata::FragmentMetadata;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tokio::sync::RwLock;

/// Error type for fragment operations.
#[derive(Debug, thiserror::Error)]
pub enum FragmentError {
    #[error("fragment not found: {0}")]
    NotFound(FragmentId),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(String),
}

/// Manages a collection of time-sharded fragments.
pub struct FragmentStore {
    base_dir: PathBuf,
    fragments: tokio::sync::RwLock<BTreeMap<FragmentId, FragmentMetadata>>,
}

impl FragmentStore {
    /// Open or create a fragment store.
    pub fn new(base_dir: impl Into<PathBuf>, _namespace: &str) -> Self {
        let dir = base_dir.into();
        std::fs::create_dir_all(&dir).ok();

        Self {
            base_dir: dir,
            fragments: RwLock::new(BTreeMap::new()),
        }
    }

    /// Get the base directory for all fragments.
    pub fn base_dir(&self) -> &PathBuf {
        &self.base_dir
    }

    /// Register a new fragment.
    pub async fn register(&self, meta: FragmentMetadata) -> Result<(), FragmentError> {
        self.fragments.write().await.insert(meta.id.clone(), meta);
        Ok(())
    }

    /// List fragments overlapping a time range.
    pub async fn query_range(&self, start_us: u64, end_us: u64) -> Vec<FragmentMetadata> {
        self.fragments
            .read()
            .await
            .values()
            .filter(|m| m.id.overlaps(start_us, end_us))
            .cloned()
            .collect()
    }

    /// List fragments that might contain a specific NexoraId.
    pub async fn query_qid(&self, qid: &nexora_id::NexoraId) -> Vec<FragmentMetadata> {
        self.fragments
            .read()
            .await
            .values()
            .filter(|m| m.might_contain(qid))
            .cloned()
            .collect()
    }

    /// List all fragments in the store.
    pub async fn list_all(&self) -> Vec<FragmentMetadata> {
        self.fragments.read().await.values().cloned().collect()
    }

    /// Count total fragments.
    pub async fn count(&self) -> usize {
        self.fragments.read().await.len()
    }

    /// Remove a fragment.
    pub async fn remove(&self, id: &FragmentId) -> Result<(), FragmentError> {
        self.fragments.write().await.remove(id);
        Ok(())
    }

    /// Get the fragment directory path.
    pub fn fragment_dir(&self, id: &FragmentId) -> PathBuf {
        self.base_dir.join(id.to_string())
    }

    /// Create a fragment directory.
    pub fn create_fragment_dir(&self, id: &FragmentId) -> Result<PathBuf, FragmentError> {
        let dir = self.fragment_dir(id);
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fragment_id::FragmentId;
    use uuid::Uuid;

    fn make_store() -> FragmentStore {
        let dir = tempfile::tempdir().unwrap();
        FragmentStore::new(dir.path(), "test")
    }

    #[tokio::test]
    async fn test_fragment_registry() {
        let store = make_store();

        let f1 = FragmentMetadata::new(
            FragmentId {
                start_us: 1000,
                end_us: 2000,
                uuid: Uuid::new_v4(),
            },
            "test".into(),
        );
        let f2 = FragmentMetadata::new(
            FragmentId {
                start_us: 5000,
                end_us: 6000,
                uuid: Uuid::new_v4(),
            },
            "test".into(),
        );

        store.register(f1).await.unwrap();
        store.register(f2).await.unwrap();

        assert_eq!(store.count().await, 2);

        // Query range that overlaps only f1
        let results = store.query_range(500, 1500).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id.start_us, 1000);

        // Query range that overlaps both
        let results = store.query_range(1500, 5500).await;
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_fragment_dir_creation() {
        let store = make_store();
        let fid = FragmentId {
            start_us: 1000,
            end_us: 2000,
            uuid: Uuid::new_v4(),
        };

        let dir = store.create_fragment_dir(&fid).unwrap();
        assert!(dir.exists());
        assert!(dir.to_string_lossy().contains(&fid.to_string()));
    }
}
