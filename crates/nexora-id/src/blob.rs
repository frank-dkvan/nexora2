//! Blob reference type for unstructured data (ReductStore compatible).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    pub bucket: String,
    pub entry: String,
    pub timestamp_us: u64,
    pub size: u64,
    pub content_type: String,
    pub labels: HashMap<String, String>,
}

impl BlobRef {
    pub fn new(
        bucket: &str,
        entry: &str,
        timestamp_us: u64,
        size: u64,
        content_type: &str,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            entry: entry.into(),
            timestamp_us,
            size,
            content_type: content_type.into(),
            labels: HashMap::new(),
        }
    }

    pub fn storage_path(&self) -> String {
        format!("{}/{}/{}", self.bucket, self.entry, self.timestamp_us)
    }

    pub fn memory_size(&self) -> usize {
        256 + self
            .labels
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>()
    }
}

impl std::fmt::Display for BlobRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlobRef({}/{}, {}B)", self.bucket, self.entry, self.size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_ref() {
        let br = BlobRef::new(
            "videos",
            "cam-01",
            1700000000000000,
            1024 * 1024,
            "video/mp4",
        );
        assert_eq!(br.bucket, "videos");
        assert_eq!(br.storage_path(), "videos/cam-01/1700000000000000");
    }
}
