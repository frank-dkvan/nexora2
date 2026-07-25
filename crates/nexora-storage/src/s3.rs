//! S3-compatible object storage backend.
//!
//! Works with any S3-compatible API: AWS S3, MinIO, GCS (S3 interop),
//! Cloudflare R2, etc.
//!
//! Uses plain HTTP REST calls signed with AWS Signature Version 4 — no AWS SDK
//! dependency. TLS is provided by rustls (via reqwest) to match the app stack.

use async_trait::async_trait;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{ObjectMeta, StorageBackend, StorageError, StorageTier};

type HmacSha256 = Hmac<Sha256>;

/// SHA-256 hex of an empty body — used as the payload hash for bodyless requests.
const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Configuration for an S3-compatible storage backend.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3Config {
    /// Endpoint URL (e.g., "https://s3.us-east-1.amazonaws.com" or "http://localhost:9000")
    pub endpoint: String,
    /// Bucket name
    pub bucket: String,
    /// Region
    pub region: String,
    /// Access key ID
    pub access_key: String,
    /// Secret access key
    pub secret_key: String,
    /// Optional prefix within the bucket (virtual directory)
    pub prefix: Option<String>,
    /// Use path-style addressing (true for MinIO, false for AWS S3)
    pub path_style: bool,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:9000".into(),
            bucket: "nexora".into(),
            region: "us-east-1".into(),
            access_key: String::new(),
            secret_key: String::new(),
            prefix: None,
            path_style: true,
        }
    }
}
/// S3-compatible storage backend over plain HTTP + AWS SigV4.
pub struct S3Storage {
    config: S3Config,
    client: reqwest::Client,
}

impl S3Storage {
    pub fn new(config: S3Config) -> Self {
        Self {
            client: reqwest::Client::new(),
            config,
        }
    }

    /// Get the full object key (prefix + path), without a leading slash.
    fn key(&self, path: &str) -> String {
        let path = path.trim_start_matches('/');
        match &self.config.prefix {
            Some(p) => format!("{}/{}", p.trim_matches('/'), path),
            None => path.to_string(),
        }
    }

    /// Host portion of the endpoint (no scheme), used in the signed Host header.
    fn host(&self) -> String {
        self.config
            .endpoint
            .split("://")
            .nth(1)
            .unwrap_or(&self.config.endpoint)
            .trim_end_matches('/')
            .to_string()
    }

    /// Build the request URL and the canonical URI path for a given object key.
    ///
    /// Path-style (MinIO): `{endpoint}/{bucket}/{key}`.
    /// Virtual-host style (AWS): `{scheme}://{bucket}.{host}/{key}`.
    /// Returns `(url, canonical_uri)` where `canonical_uri` is the URI-encoded
    /// path that SigV4 signs.
    fn build_url(&self, key: &str) -> (String, String) {
        let encoded_key = uri_encode(key, false);
        if self.config.path_style {
            let base = self.config.endpoint.trim_end_matches('/');
            let canonical_uri = format!("/{}/{}", self.config.bucket, encoded_key);
            (format!("{base}{canonical_uri}"), canonical_uri)
        } else {
            let (scheme, host) = match self.config.endpoint.split_once("://") {
                Some((s, h)) => (s, h.trim_end_matches('/')),
                None => ("https", self.config.endpoint.trim_end_matches('/')),
            };
            let canonical_uri = format!("/{encoded_key}");
            (
                format!("{scheme}://{}.{host}{canonical_uri}", self.config.bucket),
                canonical_uri,
            )
        }
    }

    /// Host header value, accounting for virtual-host addressing.
    fn signed_host(&self) -> String {
        if self.config.path_style {
            self.host()
        } else {
            format!("{}.{}", self.config.bucket, self.host())
        }
    }
}
/// AWS SigV4 URI-encoding. Encodes everything except unreserved characters.
/// When `encode_slash` is false, `/` is left as-is (correct for path segments).
fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Derive the SigV4 signing key: HMAC chain over date → region → service → "aws4_request".
fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

impl S3Storage {
    /// Build a fully SigV4-signed request for the given method/key/body.
    ///
    /// Implements the canonical request → string-to-sign → signature flow for
    /// AWS Signature Version 4 (service "s3"), signing the `host`,
    /// `x-amz-content-sha256`, and `x-amz-date` headers.
    fn signed_request(
        &self,
        method: reqwest::Method,
        key: &str,
        body: &[u8],
    ) -> reqwest::RequestBuilder {
        let (url, canonical_uri) = self.build_url(key);
        let host = self.signed_host();
        let payload_hash = if body.is_empty() {
            EMPTY_PAYLOAD_SHA256.to_string()
        } else {
            sha256_hex(body)
        };

        // Timestamps: amz-date (YYYYMMDDTHHMMSSZ) and short date (YYYYMMDD).
        let now = chrono::Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        // Canonical request. No query string (empty). Signed headers must be
        // sorted lowercase and match the header set sent on the wire.
        let canonical_headers =
            format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_request = format!(
            "{method}\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );

        // String to sign.
        let service = "s3";
        let scope = format!("{date_stamp}/{}/{service}/aws4_request", self.config.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );

        // Signature + Authorization header.
        let key_bytes = signing_key(
            &self.config.secret_key,
            &date_stamp,
            &self.config.region,
            service,
        );
        let signature = hex::encode(hmac_sha256(&key_bytes, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.config.access_key
        );

        let mut req = self
            .client
            .request(method, &url)
            .header("host", host)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", authorization);
        if !body.is_empty() {
            req = req.body(body.to_vec());
        }
        req
    }

    /// Build a SigV4-signed ListObjectsV2 request. Unlike `signed_request`, this
    /// signs a non-empty query string (`list-type=2&prefix=...`), which must be
    /// part of the canonical request in sorted, URI-encoded form.
    fn signed_list_request(&self, full_prefix: &str) -> reqwest::RequestBuilder {
        let host = self.signed_host();
        // Canonical URI is the bucket root ("/" or "/{bucket}/" for path-style).
        let (url_base, canonical_uri) = if self.config.path_style {
            let base = self.config.endpoint.trim_end_matches('/');
            let uri = format!("/{}/", self.config.bucket);
            (format!("{base}{uri}"), uri)
        } else {
            let (scheme, h) = self
                .config
                .endpoint
                .split_once("://")
                .unwrap_or(("https", &self.config.endpoint));
            (
                format!(
                    "{scheme}://{}.{}/",
                    self.config.bucket,
                    h.trim_end_matches('/')
                ),
                "/".to_string(),
            )
        };

        // Canonical query string: params sorted by key, values URI-encoded.
        let encoded_prefix = uri_encode(full_prefix, true);
        let canonical_query = format!("list-type=2&prefix={encoded_prefix}");
        let url = format!("{url_base}?{canonical_query}");

        let payload_hash = EMPTY_PAYLOAD_SHA256.to_string();
        let now = chrono::Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        let canonical_headers =
            format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_request = format!(
            "GET\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );

        let service = "s3";
        let scope = format!("{date_stamp}/{}/{service}/aws4_request", self.config.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let key_bytes = signing_key(
            &self.config.secret_key,
            &date_stamp,
            &self.config.region,
            service,
        );
        let signature = hex::encode(hmac_sha256(&key_bytes, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.config.access_key
        );

        self.client
            .get(&url)
            .header("host", host)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", authorization)
    }
}
/// Minimal parser for the `<Key>` and `<Size>`/`<LastModified>` entries in an
/// S3 ListObjectsV2 XML response. Avoids pulling in a full XML crate for the
/// only response shape we consume.
fn parse_list_v2_xml(xml: &str) -> Vec<(String, u64, u64)> {
    let mut out = Vec::new();
    // Each object lives in a <Contents>...</Contents> block.
    for block in xml.split("<Contents>").skip(1) {
        let block = match block.split_once("</Contents>") {
            Some((b, _)) => b,
            None => block,
        };
        let key = extract_tag(block, "Key");
        if key.is_empty() {
            continue;
        }
        let size = extract_tag(block, "Size").parse::<u64>().unwrap_or(0);
        // LastModified is ISO-8601; convert to epoch ms, best-effort.
        let last_modified_ms =
            chrono::DateTime::parse_from_rfc3339(&extract_tag(block, "LastModified"))
                .map(|dt| dt.timestamp_millis() as u64)
                .unwrap_or(0);
        out.push((key, size, last_modified_ms));
    }
    out
}

/// Extract the text content of the first `<tag>...</tag>` in `xml`.
fn extract_tag(xml: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    if let Some(start) = xml.find(&open) {
        let after = &xml[start + open.len()..];
        if let Some(end) = after.find(&close) {
            return after[..end].to_string();
        }
    }
    String::new()
}

#[async_trait]
impl StorageBackend for S3Storage {
    async fn put(&self, path: &str, data: Bytes) -> Result<(), StorageError> {
        let key = self.key(path);
        let resp = self
            .signed_request(reqwest::Method::PUT, &key, &data)
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("S3 PUT request failed: {e}")))?;
        check_status(resp, path, "PUT").await.map(|_| ())
    }

    async fn get(&self, path: &str) -> Result<Bytes, StorageError> {
        let key = self.key(path);
        let resp = self
            .signed_request(reqwest::Method::GET, &key, &[])
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("S3 GET request failed: {e}")))?;
        let resp = check_status(resp, path, "GET").await?;
        resp.bytes()
            .await
            .map_err(|e| StorageError::Backend(format!("S3 GET body read failed: {e}")))
    }

    async fn head(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        let key = self.key(path);
        let resp = self
            .signed_request(reqwest::Method::HEAD, &key, &[])
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("S3 HEAD request failed: {e}")))?;
        let resp = check_status(resp, path, "HEAD").await?;
        let size = resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let last_modified_ms = resp
            .headers()
            .get(reqwest::header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| chrono::DateTime::parse_from_rfc2822(v).ok())
            .map(|dt| dt.timestamp_millis() as u64)
            .unwrap_or(0);
        Ok(ObjectMeta {
            path: path.to_string(),
            size,
            tier: StorageTier::Warm,
            last_modified_ms,
            content_type: Some("application/octet-stream".into()),
        })
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StorageError> {
        // ListObjectsV2: GET on the bucket root with ?list-type=2&prefix=.
        // signed_list_request builds the URL and SigV4 signature (query string
        // included in the canonical request).
        let full_prefix = self.key(prefix);
        let resp = self
            .signed_list_request(&full_prefix)
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("S3 LIST request failed: {e}")))?;
        let resp = check_status(resp, prefix, "LIST").await?;
        let xml = resp
            .text()
            .await
            .map_err(|e| StorageError::Backend(format!("S3 LIST body read failed: {e}")))?;
        Ok(parse_list_v2_xml(&xml)
            .into_iter()
            .map(|(key, size, ts)| ObjectMeta {
                path: key,
                size,
                tier: StorageTier::Warm,
                last_modified_ms: ts,
                content_type: Some("application/octet-stream".into()),
            })
            .collect())
    }

    async fn delete(&self, path: &str) -> Result<(), StorageError> {
        let key = self.key(path);
        let resp = self
            .signed_request(reqwest::Method::DELETE, &key, &[])
            .send()
            .await
            .map_err(|e| StorageError::Backend(format!("S3 DELETE request failed: {e}")))?;
        // S3 DELETE returns 204 on success and also 204 for a missing key, so we
        // treat any 2xx as success without a NotFound distinction.
        check_status(resp, path, "DELETE").await.map(|_| ())
    }

    async fn exists(&self, path: &str) -> Result<bool, StorageError> {
        match self.head(path).await {
            Ok(_) => Ok(true),
            Err(StorageError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn tier(&self) -> StorageTier {
        StorageTier::Warm
    }

    fn name(&self) -> &str {
        "s3"
    }
}

/// Map an HTTP response to a `StorageError`: 404 → NotFound, other non-2xx →
/// Backend(status + body). Returns the response unchanged on success.
async fn check_status(
    resp: reqwest::Response,
    path: &str,
    op: &str,
) -> Result<reqwest::Response, StorageError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(StorageError::NotFound(path.to_string()));
    }
    let body = resp.text().await.unwrap_or_default();
    Err(StorageError::Backend(format!(
        "S3 {op} {path} failed: HTTP {status}: {body}"
    )))
}
/// In-memory mock of an S3-compatible store, for tests that don't want a live
/// backend. Matches S3 key/prefix semantics. This is the former `S3Storage`
/// mock, kept explicitly test-only so production code can never silently use it.
pub struct MockS3Storage {
    config: S3Config,
    objects: tokio::sync::RwLock<std::collections::HashMap<String, (Bytes, u64)>>,
}

impl MockS3Storage {
    pub fn new(config: S3Config) -> Self {
        Self {
            config,
            objects: tokio::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }

    fn key(&self, path: &str) -> String {
        let path = path.trim_start_matches('/');
        match &self.config.prefix {
            Some(p) => format!("{}/{}", p.trim_matches('/'), path),
            None => path.to_string(),
        }
    }
}

#[async_trait]
impl StorageBackend for MockS3Storage {
    async fn put(&self, path: &str, data: Bytes) -> Result<(), StorageError> {
        let key = self.key(path);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.objects.write().await.insert(key, (data, now));
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Bytes, StorageError> {
        let key = self.key(path);
        self.objects
            .read()
            .await
            .get(&key)
            .map(|(data, _)| data.clone())
            .ok_or_else(|| StorageError::NotFound(path.to_string()))
    }

    async fn head(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        let key = self.key(path);
        let map = self.objects.read().await;
        let (data, ts) = map
            .get(&key)
            .ok_or_else(|| StorageError::NotFound(path.to_string()))?;
        Ok(ObjectMeta {
            path: path.to_string(),
            size: data.len() as u64,
            tier: StorageTier::Warm,
            last_modified_ms: *ts,
            content_type: Some("application/octet-stream".into()),
        })
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, StorageError> {
        let full_prefix = self.key(prefix);
        let map = self.objects.read().await;
        Ok(map
            .iter()
            .filter(|(k, _)| k.starts_with(&full_prefix))
            .map(|(k, (data, ts))| ObjectMeta {
                path: k.clone(),
                size: data.len() as u64,
                tier: StorageTier::Warm,
                last_modified_ms: *ts,
                content_type: Some("application/octet-stream".into()),
            })
            .collect())
    }

    async fn delete(&self, path: &str) -> Result<(), StorageError> {
        let key = self.key(path);
        self.objects
            .write()
            .await
            .remove(&key)
            .ok_or_else(|| StorageError::NotFound(path.to_string()))?;
        Ok(())
    }

    async fn exists(&self, path: &str) -> Result<bool, StorageError> {
        let key = self.key(path);
        Ok(self.objects.read().await.contains_key(&key))
    }

    fn tier(&self) -> StorageTier {
        StorageTier::Warm
    }

    fn name(&self) -> &str {
        "s3-mock"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== SigV4 primitive tests (no network) =====

    #[test]
    fn test_sha256_hex_empty() {
        // The well-known SHA-256 of the empty string, used as the empty payload hash.
        assert_eq!(sha256_hex(b""), EMPTY_PAYLOAD_SHA256);
    }

    #[test]
    fn test_uri_encode_preserves_unreserved_and_slash() {
        assert_eq!(uri_encode("abc-_.~/def", false), "abc-_.~/def");
        // With encode_slash, the slash becomes %2F.
        assert_eq!(uri_encode("a/b", true), "a%2Fb");
        // Spaces and other chars are percent-encoded.
        assert_eq!(uri_encode("a b", false), "a%20b");
    }

    #[test]
    fn test_sigv4_signing_key_matches_aws_example() {
        // AWS SigV4 documented test vector.
        // secret=wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY, date=20150830,
        // region=us-east-1, service=iam → known signing key (hex).
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            hex::encode(key),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
        );
    }

    #[test]
    fn test_key_applies_prefix() {
        let store = S3Storage::new(S3Config {
            prefix: Some("data".into()),
            ..Default::default()
        });
        assert_eq!(store.key("fragment-1.wal"), "data/fragment-1.wal");
        // Leading slash on input is trimmed.
        assert_eq!(store.key("/x.bin"), "data/x.bin");
    }

    #[test]
    fn test_build_url_path_style() {
        let store = S3Storage::new(S3Config {
            endpoint: "http://localhost:9000".into(),
            bucket: "nexora".into(),
            path_style: true,
            ..Default::default()
        });
        let (url, uri) = store.build_url("a/b.bin");
        assert_eq!(url, "http://localhost:9000/nexora/a/b.bin");
        assert_eq!(uri, "/nexora/a/b.bin");
    }

    #[test]
    fn test_build_url_virtual_host_style() {
        let store = S3Storage::new(S3Config {
            endpoint: "https://s3.us-east-1.amazonaws.com".into(),
            bucket: "nexora".into(),
            path_style: false,
            ..Default::default()
        });
        let (url, uri) = store.build_url("a/b.bin");
        assert_eq!(url, "https://nexora.s3.us-east-1.amazonaws.com/a/b.bin");
        assert_eq!(uri, "/a/b.bin");
    }

    #[test]
    fn test_parse_list_v2_xml() {
        let xml = r#"<?xml version="1.0"?>
        <ListBucketResult>
          <Contents>
            <Key>data/a.parquet</Key>
            <LastModified>2024-01-15T10:30:00.000Z</LastModified>
            <Size>1024</Size>
          </Contents>
          <Contents>
            <Key>data/b.parquet</Key>
            <LastModified>2024-01-16T11:00:00.000Z</LastModified>
            <Size>2048</Size>
          </Contents>
        </ListBucketResult>"#;
        let items = parse_list_v2_xml(xml);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, "data/a.parquet");
        assert_eq!(items[0].1, 1024);
        assert!(items[0].2 > 0, "LastModified should parse to epoch ms");
        assert_eq!(items[1].0, "data/b.parquet");
        assert_eq!(items[1].1, 2048);
    }

    // ===== MockS3Storage behavior tests =====

    #[tokio::test]
    async fn test_mock_put_get() {
        let store = MockS3Storage::new(S3Config {
            prefix: Some("data".into()),
            ..Default::default()
        });
        store
            .put("fragment-1.wal", Bytes::from("wal data"))
            .await
            .unwrap();
        let data = store.get("fragment-1.wal").await.unwrap();
        assert_eq!(data, Bytes::from("wal data"));
    }

    #[tokio::test]
    async fn test_mock_list_with_prefix() {
        let store = MockS3Storage::new(S3Config {
            prefix: Some("fragments".into()),
            ..Default::default()
        });
        store
            .put("2024/01/data.parquet", Bytes::from("a"))
            .await
            .unwrap();
        store
            .put("2024/01/meta.json", Bytes::from("b"))
            .await
            .unwrap();
        store
            .put("2024/02/data.parquet", Bytes::from("c"))
            .await
            .unwrap();
        let list = store.list("2024/01/").await.unwrap();
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn test_mock_delete_and_not_found() {
        let store = MockS3Storage::new(S3Config::default());
        store
            .put("to-delete.bin", Bytes::from("data"))
            .await
            .unwrap();
        assert!(store.exists("to-delete.bin").await.unwrap());
        store.delete("to-delete.bin").await.unwrap();
        assert!(!store.exists("to-delete.bin").await.unwrap());
        assert!(matches!(
            store.get("nonexistent").await,
            Err(StorageError::NotFound(_))
        ));
    }
}
