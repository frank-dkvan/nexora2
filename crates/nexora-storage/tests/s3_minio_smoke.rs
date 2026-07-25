//! S3 backend smoke test against a live MinIO (or any S3-compatible) endpoint.
//!
//! Ignored by default — it needs a running server, so it doesn't gate CI unless
//! explicitly invoked. Run with:
//!
//! ```text
//! docker run -d -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
//!   -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
//! # create the bucket first (mc or the console), then:
//! S3_SMOKE_ENDPOINT=http://localhost:9000 \
//! S3_SMOKE_BUCKET=nexora-test \
//! S3_SMOKE_ACCESS_KEY=minioadmin \
//! S3_SMOKE_SECRET_KEY=minioadmin \
//!   cargo test -p nexora-storage --test s3_minio_smoke -- --ignored --nocapture
//! ```
//!
//! Exercises the full signed round-trip: put → head → get → list → delete.

use bytes::Bytes;
use nexora_storage::{S3Config, S3Storage, StorageBackend, StorageError};

fn config_from_env() -> Option<S3Config> {
    Some(S3Config {
        endpoint: std::env::var("S3_SMOKE_ENDPOINT").ok()?,
        bucket: std::env::var("S3_SMOKE_BUCKET").ok()?,
        region: std::env::var("S3_SMOKE_REGION").unwrap_or_else(|_| "us-east-1".into()),
        access_key: std::env::var("S3_SMOKE_ACCESS_KEY").ok()?,
        secret_key: std::env::var("S3_SMOKE_SECRET_KEY").ok()?,
        prefix: Some("smoke-test".into()),
        // MinIO needs path-style addressing.
        path_style: std::env::var("S3_SMOKE_PATH_STYLE")
            .map(|v| v != "false")
            .unwrap_or(true),
    })
}

#[tokio::test]
#[ignore = "requires a live S3/MinIO endpoint; set S3_SMOKE_* env vars"]
async fn s3_full_round_trip() {
    let config = match config_from_env() {
        Some(c) => c,
        None => {
            eprintln!("S3_SMOKE_* env vars not set; skipping");
            return;
        }
    };
    let store = S3Storage::new(config);

    let path = "fragment-round-trip.bin";
    let payload = Bytes::from("nexora s3 smoke payload");

    // put
    store.put(path, payload.clone()).await.expect("put failed");

    // exists + head
    assert!(store.exists(path).await.expect("exists failed"));
    let meta = store.head(path).await.expect("head failed");
    assert_eq!(meta.size, payload.len() as u64);

    // get round-trips the exact bytes
    let got = store.get(path).await.expect("get failed");
    assert_eq!(got, payload, "round-tripped bytes must match");

    // list finds it under the prefix
    let listed = store.list("").await.expect("list failed");
    assert!(
        listed.iter().any(|m| m.path.ends_with(path)),
        "list should include the object we just put; got {listed:?}"
    );

    // delete, then it's gone
    store.delete(path).await.expect("delete failed");
    assert!(!store
        .exists(path)
        .await
        .expect("exists after delete failed"));
    assert!(matches!(
        store.get(path).await,
        Err(StorageError::NotFound(_))
    ));
}
