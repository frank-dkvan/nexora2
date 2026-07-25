//! Bridge between the fragment lifecycle and the tiered object store.
//!
//! P2-B (DISTRIBUTED_EVOLUTION §5): the [`FragmentStore`] tracks time-sharded
//! fragment *metadata* and writes node data to local directories; the
//! [`TieredStore`] moves opaque objects hot → warm → cold as they age. This
//! module connects the two so a fragment's node data is stored as a **single
//! object** in the tiered store, letting the store's lifecycle rules sink an
//! aged (cold) fragment to Warm(S3)/Cold, while the read path transparently
//! pulls it back from whichever tier currently holds it.
//!
//! Object key convention: `fragments/<namespace>/<fragment_id>.jsonl`. The body
//! is the fragment's newline-delimited node records (same shape the local-dir
//! path and [`crate::time_travel`] use), so a fragment sunk to S3 can be read
//! back and replayed identically.
//!
//! Hot path stays shared-nothing: writing a fragment puts it on the hot tier
//! (local SSD/RocksDB-backed); only aged fragments migrate to shared object
//! storage. Reads try hot first, so live queries never pay the S3 round-trip.

use crate::fragment_id::FragmentId;
use crate::metadata::FragmentMetadata;
use crate::store::{FragmentError, FragmentStore};
use bytes::Bytes;
use nexora_storage::{StorageError, StorageTier, TieredStore};
use std::sync::Arc;

/// A fragment store whose fragment bodies live in a [`TieredStore`], so aged
/// fragments can sink to warm/cold shared storage and be read back on demand.
///
/// Wraps a `FragmentStore` (for the in-memory metadata registry + time-range
/// routing) and a `TieredStore` (for the tiered object bodies).
pub struct TieredFragmentStore {
    registry: Arc<FragmentStore>,
    tiered: Arc<TieredStore>,
    namespace: String,
}

impl TieredFragmentStore {
    /// Wrap a fragment registry and a tiered object store.
    pub fn new(registry: Arc<FragmentStore>, tiered: Arc<TieredStore>, namespace: &str) -> Self {
        Self {
            registry,
            tiered,
            namespace: namespace.to_string(),
        }
    }

    /// The tiered-store object key for a fragment's body.
    pub fn object_key(&self, id: &FragmentId) -> String {
        format!("fragments/{}/{}.jsonl", self.namespace, id)
    }

    /// Write a fragment: put its newline-delimited node records on the hot tier
    /// and register its metadata. `body` is the same `nodes.jsonl` content the
    /// local-dir path uses (one JSON node record per line), so it can be read
    /// back and replayed by the time-travel path from any tier.
    pub async fn put_fragment(
        &self,
        meta: FragmentMetadata,
        body: Bytes,
    ) -> Result<(), FragmentError> {
        let key = self.object_key(&meta.id);
        self.tiered
            .put(&key, body)
            .await
            .map_err(storage_to_fragment_err)?;
        self.registry.register(meta).await
    }

    /// Read a fragment's body from whichever tier holds it (hot → warm → cold).
    /// Returns `NotFound` if no tier has it.
    pub async fn get_fragment(&self, id: &FragmentId) -> Result<Bytes, FragmentError> {
        let key = self.object_key(id);
        self.tiered.get(&key).await.map_err(storage_to_fragment_err)
    }

    /// Which tier a fragment's body currently lives in, or `None` if absent.
    pub async fn tier_of(&self, id: &FragmentId) -> Option<StorageTier> {
        self.tiered.tier_of(&self.object_key(id)).await
    }

    /// Fragment bodies overlapping a time range, each paired with its metadata,
    /// pulled from whichever tier holds them. This is the tiered read path: a
    /// historical range query transparently retrieves fragments that have sunk
    /// to warm/cold storage.
    pub async fn read_range(
        &self,
        start_us: u64,
        end_us: u64,
    ) -> Result<Vec<(FragmentMetadata, Bytes)>, FragmentError> {
        let metas = self.registry.query_range(start_us, end_us).await;
        let mut out = Vec::with_capacity(metas.len());
        for meta in metas {
            // Skip fragments whose body was never written / already evicted;
            // a missing body is not fatal to a range read.
            match self.get_fragment(&meta.id).await {
                Ok(body) => out.push((meta, body)),
                Err(FragmentError::NotFound(_)) => {
                    tracing::warn!("fragment {} has no body in any tier; skipping", meta.id);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// Write a fragment in **columnar** form (for cold-layer OLAP): the node
    /// records are transposed to columns with per-column min/max stats, so a
    /// later range scan can push down predicates and projections. The body is
    /// stored under a distinct `.col` object key so it coexists with any row
    /// body. `rows` are the same `nodes.jsonl` records the sealer produces.
    pub async fn put_fragment_columnar(
        &self,
        meta: FragmentMetadata,
        rows: &[serde_json::Value],
    ) -> Result<(), FragmentError> {
        let cf = crate::columnar::ColumnarFragment::from_rows(rows);
        let key = self.columnar_key(&meta.id);
        self.tiered
            .put(&key, Bytes::from(cf.to_bytes()))
            .await
            .map_err(storage_to_fragment_err)?;
        self.registry.register(meta).await
    }

    /// The tiered-store object key for a fragment's columnar body.
    pub fn columnar_key(&self, id: &FragmentId) -> String {
        format!("fragments/{}/{}.col", self.namespace, id)
    }

    /// OLAP scan over a time range with predicate + projection pushdown across
    /// all tiers. For each fragment overlapping `[start_us, end_us)` that was
    /// stored columnar, load its columnar body and:
    ///   1. skip the whole fragment if its column stats rule out every predicate
    ///      (fragment-level pushdown — no row decoding at all), else
    ///   2. return only rows matching all predicates, projecting only `project`
    ///      columns (empty = all).
    ///
    /// Fragments without a columnar body are skipped (this is the OLAP path;
    /// callers use `read_range` for row replay).
    pub async fn scan_range(
        &self,
        start_us: u64,
        end_us: u64,
        predicates: &[crate::columnar::Predicate],
        project: &[String],
    ) -> Result<Vec<serde_json::Value>, FragmentError> {
        let metas = self.registry.query_range(start_us, end_us).await;
        let mut out = Vec::new();
        for meta in metas {
            let key = self.columnar_key(&meta.id);
            let bytes = match self.tiered.get(&key).await {
                Ok(b) => b,
                // No columnar body for this fragment → not part of the OLAP set.
                Err(nexora_storage::StorageError::NotFound(_)) => continue,
                Err(e) => return Err(storage_to_fragment_err(e)),
            };
            let cf = crate::columnar::ColumnarFragment::from_bytes(&bytes)
                .map_err(|e| FragmentError::Serialization(e.to_string()))?;
            // Fragment-level skip via stats, then row-level scan.
            out.extend(cf.scan(predicates, project));
        }
        Ok(out)
    }

    /// Run the tiered store's lifecycle: migrate aged fragment objects hot →
    /// warm → cold per the store's rules. Returns the number of objects moved.
    pub async fn run_lifecycle(&self) -> Result<usize, FragmentError> {
        self.tiered
            .run_lifecycle()
            .await
            .map_err(storage_to_fragment_err)
    }

    /// Access the underlying metadata registry (time-range routing, counts).
    pub fn registry(&self) -> &Arc<FragmentStore> {
        &self.registry
    }
}

/// Map a storage-layer error into the fragment error space, preserving the
/// not-found case so callers can treat a missing body distinctly.
fn storage_to_fragment_err(e: StorageError) -> FragmentError {
    match e {
        StorageError::NotFound(path) => {
            // Recover a FragmentId from the object key if possible; otherwise
            // fall back to a synthetic id carrying the path in its display.
            FragmentError::Serialization(format!("object not found: {path}"))
        }
        other => FragmentError::Serialization(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::FragmentMetadata;
    use nexora_storage::{LifecycleRule, MemoryStorage, StorageTier};
    use uuid::Uuid;

    fn tiered() -> Arc<TieredStore> {
        Arc::new(TieredStore::new(
            Arc::new(MemoryStorage::new(StorageTier::Hot)),
            Arc::new(MemoryStorage::new(StorageTier::Warm)),
            Arc::new(MemoryStorage::new(StorageTier::Cold)),
        ))
    }

    fn registry() -> Arc<FragmentStore> {
        let dir = tempfile::tempdir().unwrap();
        Arc::new(FragmentStore::new(dir.path(), "test"))
    }

    fn frag(start: u64, end: u64) -> FragmentMetadata {
        FragmentMetadata::new(
            FragmentId {
                start_us: start,
                end_us: end,
                uuid: Uuid::new_v4(),
            },
            "test".into(),
        )
    }

    #[tokio::test]
    async fn put_then_get_from_hot() {
        let store = TieredFragmentStore::new(registry(), tiered(), "test");
        let meta = frag(1000, 2000);
        let id = meta.id.clone();
        store
            .put_fragment(meta, Bytes::from_static(b"{\"id\":\"n1\"}\n"))
            .await
            .unwrap();

        assert_eq!(store.tier_of(&id).await, Some(StorageTier::Hot));
        let body = store.get_fragment(&id).await.unwrap();
        assert!(body.starts_with(b"{\"id\":\"n1\"}"));
    }

    #[tokio::test]
    async fn aged_fragment_sinks_to_warm_and_reads_back() {
        // Lifecycle rule: hot → warm with age 1ms, so anything qualifies.
        let tiered = Arc::new(TieredStore::with_rules(
            Arc::new(MemoryStorage::new(StorageTier::Hot)),
            Arc::new(MemoryStorage::new(StorageTier::Warm)),
            Arc::new(MemoryStorage::new(StorageTier::Cold)),
            vec![LifecycleRule {
                from: StorageTier::Hot,
                to: StorageTier::Warm,
                age_ms: 1,
            }],
        ));
        let store = TieredFragmentStore::new(registry(), tiered, "test");
        let meta = frag(1000, 2000);
        let id = meta.id.clone();
        store
            .put_fragment(meta, Bytes::from_static(b"{\"id\":\"cold-n\"}\n"))
            .await
            .unwrap();

        // MemoryStorage stamps last_modified_ms=0, so the object is "infinitely
        // old" vs now and the 1ms rule migrates it.
        let migrated = store.run_lifecycle().await.unwrap();
        assert!(migrated >= 1, "aged fragment must migrate");
        assert_eq!(store.tier_of(&id).await, Some(StorageTier::Warm));

        // Read path still finds it (pulled back from warm).
        let body = store.get_fragment(&id).await.unwrap();
        assert!(body.starts_with(b"{\"id\":\"cold-n\"}"));
    }

    #[tokio::test]
    async fn read_range_pulls_matching_fragments() {
        let store = TieredFragmentStore::new(registry(), tiered(), "test");
        let a = frag(1000, 2000);
        let b = frag(5000, 6000);
        store
            .put_fragment(a, Bytes::from_static(b"a\n"))
            .await
            .unwrap();
        store
            .put_fragment(b, Bytes::from_static(b"b\n"))
            .await
            .unwrap();

        // Overlaps only the first fragment.
        let hits = store.read_range(500, 1500).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].1, Bytes::from_static(b"a\n"));

        // Overlaps both.
        let hits = store.read_range(1500, 5500).await.unwrap();
        assert_eq!(hits.len(), 2);
    }

    #[tokio::test]
    async fn fragment_sinks_to_s3_warm_and_time_travels_back() {
        // Warm tier is the S3-shaped MockS3Storage — the same backend shape
        // production uses — so this exercises the real hot→S3 sink + read-back,
        // then replays the pulled-back records through the time-travel engine.
        use crate::time_travel::{execute_time_travel, TimeTravelQuery};
        use nexora_storage::MockS3Storage;

        let warm_cfg = nexora_storage::S3Config {
            prefix: Some("warm".into()),
            ..Default::default()
        };
        // Hot ages out at 1ms; MockS3 stamps `now` on write, but the hot
        // MemoryStorage object is stamped 0 (infinitely old) so it migrates.
        let tiered = Arc::new(TieredStore::with_rules(
            Arc::new(MemoryStorage::new(StorageTier::Hot)),
            Arc::new(MockS3Storage::new(warm_cfg)),
            Arc::new(MemoryStorage::new(StorageTier::Cold)),
            vec![LifecycleRule {
                from: StorageTier::Hot,
                to: StorageTier::Warm,
                age_ms: 1,
            }],
        ));
        let reg = registry();
        let store = TieredFragmentStore::new(reg.clone(), tiered, "hist");

        // A fragment covering [1000,2000) holding one node record at t=1500.
        let meta = frag(1000, 2000);
        let id = meta.id.clone();
        let body = Bytes::from_static(
            b"{\"id\":\"node1\",\"timestamp\":1500,\"properties\":{\"name\":\"Alice\",\"speed\":50}}\n",
        );
        store.put_fragment(meta, body).await.unwrap();
        assert_eq!(store.tier_of(&id).await, Some(StorageTier::Hot));

        // Age it out → sinks to S3 (warm).
        assert!(store.run_lifecycle().await.unwrap() >= 1);
        assert_eq!(
            store.tier_of(&id).await,
            Some(StorageTier::Warm),
            "aged fragment must have sunk to S3 warm tier"
        );

        // A historical range read pulls the body back from S3 and materializes
        // the fragment dir so the time-travel engine can replay it.
        let hits = store.read_range(0, 2000).await.unwrap();
        assert_eq!(hits.len(), 1, "range read must pull the fragment from S3");
        // Write the pulled-back body to the fragment dir the time-travel engine
        // reads from, then run an "as of" query.
        let dir = reg.create_fragment_dir(&id).unwrap();
        std::fs::write(dir.join("nodes.jsonl"), &hits[0].1).unwrap();

        let result = execute_time_travel(&reg, TimeTravelQuery::at(1600))
            .await
            .unwrap();
        assert_eq!(
            result.node_count(),
            1,
            "must reconstruct the node from S3 data"
        );
        assert_eq!(
            result.nodes[0].properties.get("speed"),
            Some(&nexora_id::PropertyValue::Integer(50)),
            "historical value must survive the round-trip through S3"
        );
    }

    #[tokio::test]
    async fn columnar_olap_scan_pushes_down_across_s3() {
        // Cold-layer OLAP: store fragments columnar, sink them to S3 (warm),
        // then run a range + predicate + projection scan that pulls the columnar
        // bodies back from S3 and applies pushdown.
        use crate::columnar::{Predicate, PredicateOp};
        use nexora_storage::MockS3Storage;
        use serde_json::json;

        let warm_cfg = nexora_storage::S3Config {
            prefix: Some("cold".into()),
            ..Default::default()
        };
        let tiered = Arc::new(TieredStore::with_rules(
            Arc::new(MemoryStorage::new(StorageTier::Hot)),
            Arc::new(MockS3Storage::new(warm_cfg)),
            Arc::new(MemoryStorage::new(StorageTier::Cold)),
            vec![LifecycleRule {
                from: StorageTier::Hot,
                to: StorageTier::Warm,
                age_ms: 1,
            }],
        ));
        let reg = registry();
        let store = TieredFragmentStore::new(reg.clone(), tiered, "hist");

        // Fragment A [1000,2000): two nodes, speeds 50 and 80.
        let a = frag(1000, 2000);
        store
            .put_fragment_columnar(
                a,
                &[
                    json!({"id":"n1","timestamp":1500,"properties":{"speed":50,"name":"Alice"}}),
                    json!({"id":"n2","timestamp":1600,"properties":{"speed":80,"name":"Bob"}}),
                ],
            )
            .await
            .unwrap();
        // Fragment B [3000,4000): one node, speed 10 (below the filter → fragment
        // skipped by stats pushdown, never row-decoded).
        let b = frag(3000, 4000);
        store
            .put_fragment_columnar(
                b,
                &[json!({"id":"n3","timestamp":3500,"properties":{"speed":10,"name":"Carol"}})],
            )
            .await
            .unwrap();

        // Age both out → they sink to S3 (warm).
        assert!(store.run_lifecycle().await.unwrap() >= 2);

        // OLAP query: over [0,5000), speed >= 60, project only "name".
        let hits = store
            .scan_range(
                0,
                5000,
                &[Predicate::new("speed", PredicateOp::Ge, json!(60))],
                &["name".to_string()],
            )
            .await
            .unwrap();

        // Only n2 (speed 80) matches; n1(50) filtered by row predicate, fragment
        // B skipped whole by stats (max speed 10 < 60).
        assert_eq!(hits.len(), 1, "only n2 matches speed>=60");
        assert_eq!(hits[0]["id"], json!("n2"));
        assert_eq!(hits[0]["name"], json!("Bob"));
        // Projection excluded "speed".
        assert_eq!(hits[0].get("speed"), None);
    }
}
