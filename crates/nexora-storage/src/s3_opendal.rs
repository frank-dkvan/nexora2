//! opendal-based S3 backend — spike for unifying nexora's storage layer.
//!
//! Implements the same [`StorageBackend`] trait as the self-written [`S3Storage`]
//! (`s3.rs`), but delegates all I/O to OpenDAL's S3 service. This is the
//! "converge onto opendal" path: opendal brings range read, multipart upload,
//! retry/timeout layers, and list pagination for free — the exact features the
//! hand-written SigV4 client lacks.
//!
//! Reuses [`S3Config`] verbatim so callers/config are unchanged.

use async_trait::async_trait;
use bytes::Bytes;
use opendal::{services::S3, Operator};

use crate::{s3::S3Config, ObjectMeta, StorageBackend, StorageError, StorageTier};

/// S3-compatible storage backend backed by OpenDAL.
pub struct S3StorageOpenDal {
    op: Operator,
    tier: StorageTier,
    name: String,
}

impl S3StorageOpenDal {
    /// Build from the existing [`S3Config`]. `tier` labels which layer this
    /// backend serves (Hot/Warm/Cold) so it drops into `TieredStore` unchanged.
    pub fn new(config: S3Config, tier: StorageTier) -> Result<Self, StorageError> {
        let mut builder = S3::default()
            .bucket(&config.bucket)
            .region(&config.region)
            .endpoint(&config.endpoint)
            .access_key_id(&config.access_key)
            .secret_access_key(&config.secret_key);

        // Prefix maps to opendal's root (all keys are relative to it).
        if let Some(prefix) = &config.prefix {
            builder = builder.root(prefix);
        }
        // MinIO/self-hosted require path-style; AWS uses virtual-host by default.
        if !config.path_style {
            builder = builder.enable_virtual_host_style();
        }
        // Don't read ~/.aws or env — use exactly the creds we were handed.
        builder = builder.disable_config_load();

        let op = Operator::new(builder)
            .map_err(|e| StorageError::Backend(format!("opendal S3 init: {e}")))?
            .finish();

        Ok(Self {
            op,
            tier,
            name: format!("opendal-s3://{}", config.bucket),
        })
    }
}

/// Map opendal errors onto the crate's StorageError, preserving NotFound.
fn map_err(e: opendal::Error) -> StorageError {
    if e.kind() == opendal::ErrorKind::NotFound {
        StorageError::NotFound(e.to_string())
    } else {
        StorageError::Backend(e.to_string())
    }
}

#[async_trait]
impl StorageBackend for S3StorageOpenDal {
    async fn put(&self, path: &str, data: Bytes) -> Result<(), StorageError> {
        // opendal's writer transparently uses multipart for large objects.
        self.op.write(path, data).await.map_err(map_err)?;
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Bytes, StorageError> {
        let buf = self.op.read(path).await.map_err(map_err)?;
        Ok(buf.to_bytes())
    }

    async fn head(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        let meta = self.op.stat(path).await.map_err(map_err)?;
        Ok(ObjectMeta {
            path: path.to_string(),
            size: meta.content_length(),
            tier: self.tier,
            last_modified_ms: meta
                .last_modified()
                .map(|t| t.as_millisecond() as u64)
                .unwrap_or(0),
            content_type: meta.content_type().map(|s| s.to_string()),
        })
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StorageError> {
        // opendal's list handles pagination internally (no 1000-object cap).
        let entries = self.op.list(prefix).await.map_err(map_err)?;
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            let m = entry.metadata();
            if m.is_dir() {
                continue;
            }
            out.push(ObjectMeta {
                path: entry.path().to_string(),
                size: m.content_length(),
                tier: self.tier,
                last_modified_ms: m
                    .last_modified()
                    .map(|t| t.as_millisecond() as u64)
                    .unwrap_or(0),
                content_type: m.content_type().map(|s| s.to_string()),
            });
        }
        Ok(out)
    }

    async fn delete(&self, path: &str) -> Result<(), StorageError> {
        self.op.delete(path).await.map_err(map_err)?;
        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool, StorageError> {
        self.op.exists(path).await.map_err(map_err)
    }

    fn tier(&self) -> StorageTier {
        self.tier
    }

    fn name(&self) -> &str {
        &self.name
    }
}
