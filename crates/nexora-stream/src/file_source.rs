//! File ingestion source (poll + commit) — reads JSON Lines from a file.
//!
//! This is the poll+commit counterpart to the legacy push-channel file source,
//! bringing file ingest onto the same [`IngestionSource`] contract as Kafka and
//! future connectors. Each line is a JSON object; `poll` returns up to
//! `max_batch` lines' worth of [`IngestRecord`]s (one per top-level field, node
//! id taken from `id_field`), and the offset is the 0-based line index.
//!
//! Durability of *reads*: `commit` persists the last-processed line to an
//! [`OffsetStore`]; on `connect` the source skips already-committed lines, so a
//! restart resumes rather than replaying the whole file.

use crate::{
    IngestBatch, IngestRecord, IngestionError, IngestionSource, IngestionStats, OffsetStore,
    SourceOffset,
};
use nexora_id::NexoraId;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::sync::Mutex;

/// Configuration for a [`FileSource`].
#[derive(Clone, Debug)]
pub struct FileSourceConfig {
    /// Path to the JSON Lines file.
    pub path: PathBuf,
    /// Logical topic/stream name (used for offset keys and stats).
    pub topic: String,
    /// JSON field holding the node id (hex NexoraId, else hashed from string).
    pub id_field: String,
    /// Optional JSON field whose value becomes the node's graph *label* (in
    /// addition to remaining a property). E.g. `label_field: "type"` turns
    /// `{"type":"Forklift",…}` into a `:Forklift`-labelled node, so label scans,
    /// Standing Query `LabelFilter`s, and the pg-wire table catalog see it.
    pub label_field: Option<String>,
    /// Optional JSON field holding the record's event time, for event-time
    /// processing (out-of-order-safe last-writer-wins + windowing). RFC 3339
    /// string or integer epoch interpreted per `event_time_unit`. When unset or
    /// absent, the record carries no event time (arrival order).
    pub event_time_field: Option<String>,
    /// How a numeric `event_time_field` is interpreted: seconds, milliseconds,
    /// microseconds, or RFC 3339 string. Defaults to microseconds.
    pub event_time_unit: crate::EventTimeUnit,
    /// Max lines returned per `poll`.
    pub max_batch: usize,
}

impl Default for FileSourceConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            topic: "file".into(),
            id_field: "id".into(),
            label_field: None,
            event_time_field: None,
            event_time_unit: crate::EventTimeUnit::default(),
            max_batch: 256,
        }
    }
}

/// Live reader state, created on `connect`.
struct FileState {
    lines: Lines<BufReader<tokio::fs::File>>,
    /// 0-based index of the next line to read.
    next_line: u64,
    /// Set once the underlying reader is exhausted.
    eof: bool,
}

/// A file ingestion source implementing [`IngestionSource`].
pub struct FileSource {
    config: FileSourceConfig,
    offset_store: Option<Arc<dyn OffsetStore>>,
    state: Mutex<Option<FileState>>,
    stats: Mutex<IngestionStats>,
    stopped: AtomicBool,
}

impl FileSource {
    /// Create a file source with no persisted offset (always reads from line 0).
    pub fn new(config: FileSourceConfig) -> Self {
        Self {
            config,
            offset_store: None,
            state: Mutex::new(None),
            stats: Mutex::new(IngestionStats::default()),
            stopped: AtomicBool::new(false),
        }
    }

    /// Attach an offset store so the source resumes from the last committed line
    /// on `connect` and persists progress on `commit`.
    pub fn with_offset_store(mut self, store: Arc<dyn OffsetStore>) -> Self {
        self.offset_store = Some(store);
        self
    }

    /// Signal the source to stop polling.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }

    /// The single logical partition of a file source.
    const PARTITION: &'static str = "0";
}

#[async_trait::async_trait]
impl IngestionSource for FileSource {
    async fn connect(&self) -> Result<(), IngestionError> {
        let file = tokio::fs::File::open(&self.config.path)
            .await
            .map_err(|e| IngestionError::Connection(format!("open {:?}: {e}", self.config.path)))?;
        let mut lines = BufReader::new(file).lines();

        // Resume: skip lines already committed (offset = last-processed index,
        // so skip 0..=offset → next_line = offset + 1).
        let mut next_line = 0u64;
        if let Some(store) = &self.offset_store {
            if let Ok(Some(committed)) = store.load(&self.config.topic, Self::PARTITION).await {
                let skip = committed + 1;
                for _ in 0..skip {
                    match lines.next_line().await {
                        Ok(Some(_)) => {}
                        Ok(None) => break, // file shorter than committed offset
                        Err(e) => return Err(IngestionError::Connection(format!("seek: {e}"))),
                    }
                }
                next_line = skip;
            }
        }

        let mut guard = self.state.lock().await;
        *guard = Some(FileState {
            lines,
            next_line,
            eof: false,
        });
        Ok(())
    }

    async fn poll(&self) -> Result<Option<IngestBatch>, IngestionError> {
        if self.stopped.load(Ordering::Relaxed) {
            return Ok(None);
        }

        let mut guard = self.state.lock().await;
        let state = guard.as_mut().ok_or(IngestionError::NotConnected)?;
        if state.eof {
            return Ok(None);
        }

        let mut records = Vec::new();
        let mut raw_events = Vec::new(); // 阶段 1: 收集 RawEvent
        let offset_start = state.next_line;
        let mut last_line = offset_start;

        while (records.len() < self.config.max_batch) || records.is_empty()
        /* ensure progress on wide lines */
        {
            match state.lines.next_line().await {
                Ok(Some(line)) => {
                    let line_idx = state.next_line;
                    state.next_line += 1;
                    last_line = line_idx;
                    if line.trim().is_empty() {
                        continue;
                    }

                    // 解析 JSON payload
                    let json_value: serde_json::Value = match serde_json::from_str(&line) {
                        Ok(v) => v,
                        Err(e) => {
                            // Skip malformed lines but count them, so ingest error
                            // observability is preserved (offset still advances).
                            let mut stats = self.stats.lock().await;
                            stats.errors += 1;
                            drop(stats);
                            tracing::warn!("Skipping invalid JSON at line {}: {}", line_idx, e);
                            continue;
                        }
                    };

                    // 构造 RawEvent (事件优先模式)
                    let event_time_us = if let Some(obj) = json_value.as_object() {
                        crate::extract_event_time(
                            obj,
                            self.config.event_time_field.as_deref(),
                            self.config.event_time_unit,
                            None,
                        )
                        .map(|dt| dt.timestamp_micros() as u64)
                        .unwrap_or_else(|| chrono::Utc::now().timestamp_micros() as u64)
                    } else {
                        chrono::Utc::now().timestamp_micros() as u64
                    };

                    let raw_event = nexora_core::RawEvent::new(
                        event_time_us,
                        chrono::Utc::now().timestamp_micros() as u64, // ingest_time_us
                        "file",                                        // source
                        self.config.topic.clone(),                     // topic
                        None,                                          // partition
                        Some(line_idx as i64),                         // offset
                        None,                                          // subject
                        json_value.clone(),                            // payload
                    );
                    raw_events.push(raw_event);

                    // 原有的 IngestRecord 构造(保持兼容)
                    match line_to_records(
                        &line,
                        &self.config.id_field,
                        self.config.label_field.as_deref(),
                        self.config.event_time_field.as_deref(),
                        self.config.event_time_unit,
                    ) {
                        Ok(mut recs) => records.append(&mut recs),
                        Err(e) => {
                            // Skip malformed lines but keep advancing the offset.
                            let mut stats = self.stats.lock().await;
                            stats.errors += 1;
                            tracing::warn!(line = line_idx, error = %e, "file ingest: skip bad line");
                        }
                    }
                    // Stop once we have at least a batch worth of records.
                    if records.len() >= self.config.max_batch {
                        break;
                    }
                }
                Ok(None) => {
                    state.eof = true;
                    break;
                }
                Err(e) => return Err(IngestionError::Poll(format!("read: {e}"))),
            }
        }

        if records.is_empty() {
            return Ok(None);
        }

        let len = records.len() as u64;
        {
            let mut stats = self.stats.lock().await;
            stats.records_ingested += len;
            stats.batches_processed += 1;
        }

        Ok(Some(IngestBatch {
            records,
            partition: Self::PARTITION.to_string(),
            offset_start,
            offset_end: last_line,
            topic: self.config.topic.clone(),
            raw_events: Some(raw_events), // 阶段 1: 事件优先模式
        }))
    }

    async fn commit(&self, offset: &SourceOffset) -> Result<(), IngestionError> {
        {
            let mut stats = self.stats.lock().await;
            stats.last_offset = Some(offset.clone());
        }
        if let Some(store) = &self.offset_store {
            store
                .save(&offset.topic, &offset.partition, offset.offset)
                .await
                .map_err(IngestionError::Commit)?;
        }
        Ok(())
    }

    fn stats(&self) -> IngestionStats {
        self.stats.try_lock().map(|s| s.clone()).unwrap_or_default()
    }

    fn topics(&self) -> Vec<String> {
        vec![self.config.topic.clone()]
    }

    async fn close(&self) -> Result<(), IngestionError> {
        self.stopped.store(true, Ordering::Relaxed);
        let mut guard = self.state.lock().await;
        *guard = None;
        Ok(())
    }
}

/// Parse one JSONL line into records — one per top-level field (skipping the id
/// field), node id from `id_field`. Mirrors the Kafka source's object handling.
///
/// When `label_field` is set and present in the object, an extra label record is
/// emitted (in addition to keeping that field as a property), so the value both
/// registers as a graph label and stays queryable as a property.
fn line_to_records(
    line: &str,
    id_field: &str,
    label_field: Option<&str>,
    event_time_field: Option<&str>,
    event_time_unit: crate::EventTimeUnit,
) -> Result<Vec<IngestRecord>, String> {
    let json: serde_json::Value =
        serde_json::from_str(line).map_err(|e| format!("JSON parse: {e}"))?;
    let obj = json
        .as_object()
        .ok_or_else(|| "line is not a JSON object".to_string())?;

    let qid = extract_id(obj, id_field)?;
    // File records have no transport-level timestamp, so the event time comes
    // solely from the configured payload field (if any).
    let timestamp = crate::extract_event_time(obj, event_time_field, event_time_unit, None);

    let mut records = Vec::new();

    // Emit a label record first (if configured and the field is a non-empty
    // string), so the node is labelled before its properties are applied.
    if let Some(lf) = label_field {
        if let Some(serde_json::Value::String(label)) = obj.get(lf) {
            if !label.is_empty() {
                records.push(IngestRecord {
                    qid: qid.clone(),
                    key: String::new(),
                    value: serde_json::Value::Null,
                    edge_type: None,
                    edge_target: None,
                    timestamp,
                    label: Some(label.clone()),
                });
            }
        }
    }

    for (key, value) in obj {
        if key == id_field {
            continue;
        }
        records.push(IngestRecord {
            qid: qid.clone(),
            key: key.clone(),
            value: value.clone(),
            edge_type: None,
            edge_target: None,
            timestamp,
            label: None,
        });
    }
    Ok(records)
}

fn extract_id(
    obj: &serde_json::Map<String, serde_json::Value>,
    id_field: &str,
) -> Result<NexoraId, String> {
    let id_val = obj
        .get(id_field)
        .ok_or_else(|| format!("missing id field '{id_field}'"))?;
    match id_val {
        serde_json::Value::String(s) => {
            Ok(NexoraId::from_hex(s)
                .unwrap_or_else(|_| NexoraId::from_bytes(s.as_bytes().to_vec())))
        }
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(|i| NexoraId::from_bytes(i.to_be_bytes().to_vec()))
            .ok_or_else(|| "id must be string or integer".to_string()),
        _ => Err("id must be string or integer".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryOffsetStore;
    use std::io::Write;

    fn write_jsonl(lines: &[&str]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        for l in lines {
            writeln!(f, "{l}").unwrap();
        }
        f.flush().unwrap();
        f
    }

    fn config(path: PathBuf, max_batch: usize) -> FileSourceConfig {
        FileSourceConfig {
            path,
            topic: "file-test".into(),
            id_field: "id".into(),
            label_field: None,
            event_time_field: None,
            event_time_unit: crate::EventTimeUnit::default(),
            max_batch,
        }
    }

    /// With `label_field` set, each object yields an extra label record whose
    /// `label` is that field's value (and the field also stays a property).
    #[tokio::test]
    async fn label_field_emits_label_record() {
        let f = write_jsonl(&[r#"{"id":"a","type":"Forklift","speed":10}"#]);
        let mut cfg = config(f.path().to_path_buf(), 256);
        cfg.label_field = Some("type".into());
        let src = FileSource::new(cfg);
        src.connect().await.unwrap();

        let batch = src.poll().await.unwrap().unwrap();
        // Records: 1 label + type property + speed property = 3.
        let label_recs: Vec<_> = batch.records.iter().filter(|r| r.label.is_some()).collect();
        assert_eq!(label_recs.len(), 1, "one label record emitted");
        assert_eq!(label_recs[0].label.as_deref(), Some("Forklift"));
        // The `type` field also remains as a property (still queryable).
        assert!(
            batch
                .records
                .iter()
                .any(|r| r.key == "type" && r.label.is_none()),
            "type stays a property too"
        );
    }

    /// Reads all lines; each object yields one record per non-id field.
    #[tokio::test]
    async fn reads_all_records() {
        let f = write_jsonl(&[r#"{"id":"a","x":1,"y":2}"#, r#"{"id":"b","x":3}"#]);
        let src = FileSource::new(config(f.path().to_path_buf(), 256));
        src.connect().await.unwrap();

        let batch = src.poll().await.unwrap().unwrap();
        // a → x,y (2) ; b → x (1) = 3 records
        assert_eq!(batch.records.len(), 3);
        assert_eq!(batch.offset_start, 0);
        assert_eq!(batch.offset_end, 1, "last processed line index");

        // Exhausted.
        assert!(src.poll().await.unwrap().is_none());
    }

    /// max_batch bounds records per poll.
    #[tokio::test]
    async fn respects_max_batch() {
        // 5 single-field objects → 5 records; max_batch 2.
        let f = write_jsonl(&[
            r#"{"id":"1","v":1}"#,
            r#"{"id":"2","v":2}"#,
            r#"{"id":"3","v":3}"#,
            r#"{"id":"4","v":4}"#,
            r#"{"id":"5","v":5}"#,
        ]);
        let src = FileSource::new(config(f.path().to_path_buf(), 2));
        src.connect().await.unwrap();

        let b1 = src.poll().await.unwrap().unwrap();
        assert_eq!(b1.records.len(), 2);
        let b2 = src.poll().await.unwrap().unwrap();
        assert_eq!(b2.records.len(), 2);
        let b3 = src.poll().await.unwrap().unwrap();
        assert_eq!(b3.records.len(), 1);
        assert!(src.poll().await.unwrap().is_none());
    }

    /// Malformed lines are skipped, not fatal; offset still advances.
    #[tokio::test]
    async fn skips_bad_lines() {
        let f = write_jsonl(&[r#"{"id":"a","v":1}"#, r#"not json"#, r#"{"id":"b","v":2}"#]);
        let src = FileSource::new(config(f.path().to_path_buf(), 256));
        src.connect().await.unwrap();
        let batch = src.poll().await.unwrap().unwrap();
        assert_eq!(batch.records.len(), 2, "two good lines, bad one skipped");
        assert_eq!(src.stats().errors, 1);
    }

    /// After committing an offset, a fresh source resumes past it.
    #[tokio::test]
    async fn resumes_from_committed_offset() {
        let f = write_jsonl(&[
            r#"{"id":"1","v":1}"#,
            r#"{"id":"2","v":2}"#,
            r#"{"id":"3","v":3}"#,
            r#"{"id":"4","v":4}"#,
        ]);
        let store: Arc<dyn OffsetStore> = Arc::new(InMemoryOffsetStore::new());

        // First run: consume 2 lines, commit offset = 1 (last-processed index).
        {
            let src =
                FileSource::new(config(f.path().to_path_buf(), 2)).with_offset_store(store.clone());
            src.connect().await.unwrap();
            let b = src.poll().await.unwrap().unwrap();
            assert_eq!(b.offset_end, 1);
            src.commit(&SourceOffset {
                topic: "file-test".into(),
                partition: "0".into(),
                offset: b.offset_end,
                committed_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        }

        // Second run: resume — should start at line 2, not replay 0..1.
        {
            let src = FileSource::new(config(f.path().to_path_buf(), 256))
                .with_offset_store(store.clone());
            src.connect().await.unwrap();
            let b = src.poll().await.unwrap().unwrap();
            assert_eq!(b.offset_start, 2, "resume past committed line 1");
            assert_eq!(b.records.len(), 2, "only lines 2 and 3 remain");
        }
    }
}
