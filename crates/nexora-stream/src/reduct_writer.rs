//! F3.2: ReductStore 直写工具 — 将二进制 blob 上传到 ReductStore，返回 BlobRef。
//!
//! ReductStore 的 HTTP API (v1):
//!   POST /api/v1/b/{bucket}/{entry}?ts={timestamp_us}
//!   Headers: Content-Type: application/octet-stream
//!            Authorization: Bearer {token}
//!   Body: raw bytes
//!   Response: 204 No Content on success
//!
//! 文档：https://docs.reduct.store/http-api/entry-api

use nexora_id::BlobRef;

/// HTTP client for writing blobs to a ReductStore instance.
///
/// Thread-safe: `reqwest::Client` is clone-able and internally pooled.
/// Multiple tasks can share a single `ReductBlobWriter` via `Arc`.
pub struct ReductBlobWriter {
    /// Base URL, e.g. `"http://localhost:8383"`. No trailing slash.
    base_url: String,
    /// Optional Bearer token for `Authorization` header.
    token: Option<String>,
    /// Shared HTTP client (connection pool).
    client: reqwest::Client,
}

impl ReductBlobWriter {
    /// Create a new writer pointed at `base_url`.
    ///
    /// `token` is sent as `Authorization: Bearer <token>` when provided.
    pub fn new(base_url: &str, token: Option<&str>) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            token: token.map(str::to_owned),
            client: reqwest::Client::new(),
        }
    }

    /// Upload `data` to `bucket/entry` with the given `timestamp_us`.
    ///
    /// On HTTP 204 success, returns a `BlobRef` describing the stored object.
    /// Returns `Err(String)` for any HTTP or transport error.
    pub async fn write_blob(
        &self,
        bucket: &str,
        entry: &str,
        timestamp_us: u64,
        data: &[u8],
        content_type: &str,
    ) -> Result<BlobRef, String> {
        let url = format!(
            "{}/api/v1/b/{}/{}?ts={}",
            self.base_url, bucket, entry, timestamp_us
        );

        let mut req = self
            .client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, content_type)
            .body(data.to_vec());

        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }

        let response = req
            .send()
            .await
            .map_err(|e| format!("ReductStore HTTP error: {e}"))?;

        let status = response.status();
        if status == reqwest::StatusCode::NO_CONTENT || status == reqwest::StatusCode::OK {
            Ok(BlobRef::new(
                bucket,
                entry,
                timestamp_us,
                data.len() as u64,
                content_type,
            ))
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(format!("ReductStore write failed: HTTP {status} — {body}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reduct_writer_constructs_correctly() {
        let writer = ReductBlobWriter::new("http://localhost:8383", Some("my-token"));
        assert_eq!(writer.base_url, "http://localhost:8383");
        assert_eq!(writer.token.as_deref(), Some("my-token"));
    }

    #[test]
    fn reduct_writer_no_token() {
        let writer = ReductBlobWriter::new("http://localhost:8383", None);
        assert!(writer.token.is_none());
    }

    #[test]
    fn reduct_writer_strips_trailing_slash() {
        let writer = ReductBlobWriter::new("http://localhost:8383/", None);
        assert_eq!(writer.base_url, "http://localhost:8383");
    }

    /// Validate that BlobRef is constructed with correct fields from write_blob's
    /// parameters (without hitting a real server — only the constructor path).
    #[test]
    fn blob_ref_construction_from_write_params() {
        let bucket = "videos";
        let entry = "cam-01";
        let ts: u64 = 1_700_000_000_000_000;
        let data: &[u8] = b"fake_video_bytes";
        let content_type = "video/mp4";

        // Mirrors the BlobRef construction inside write_blob on success.
        let blob_ref = BlobRef::new(bucket, entry, ts, data.len() as u64, content_type);
        assert_eq!(blob_ref.bucket, "videos");
        assert_eq!(blob_ref.entry, "cam-01");
        assert_eq!(blob_ref.timestamp_us, ts);
        assert_eq!(blob_ref.size, data.len() as u64);
        assert_eq!(blob_ref.content_type, "video/mp4");
    }
}
