# Phase 6: Event Pipeline Integration

**Status**: 📝 Planning  
**Estimated Duration**: 1 week  
**Prerequisites**: Phase 5 complete (CLI, config, initialization, HTTP API, HA testing)

## Overview

Phase 6 connects RisingWave's materialized views back to `nexora-eventlog`, completing the event processing pipeline. This enables enriched, transformed events from SQL materialized views to flow into the graph database.

## Goals

1. **RisingWave → EventLog Bridge**: Stream MV changes to Iceberg tables
2. **Change Data Capture (CDC)**: Subscribe to materialized view updates
3. **Event Projection**: Map enriched events to graph nodes/edges
4. **End-to-End Pipeline**: Complete flow from source → RisingWave → EventLog → Graph
5. **Performance Validation**: Measure throughput and latency

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                    Phase 6: Event Pipeline                      │
└────────────────────────────────────────────────────────────────┘

External Sources (Kafka/Pulsar/MQTT)
         │
         ▼
┌─────────────────────┐
│  RisingWave Engine  │
│  (Event Streaming)  │
├─────────────────────┤
│ CREATE SOURCE       │ ← Direct connector (no nexora-stream)
│ CREATE MV           │ ← SQL transformations
└──────┬──────────────┘
       │ MV Change Stream (CDC)
       ▼
┌─────────────────────┐
│  EventLogSink       │ ← NEW (Phase 6)
│  (Bridge Layer)     │
├─────────────────────┤
│ - Subscribe to MV   │
│ - Convert rows      │
│ - Write to Iceberg  │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│  nexora-eventlog    │
│  (Apache Iceberg)   │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│  GraphStreaming     │ ← Future: Graph projection layer
│  (Projection)       │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│  nexora-core        │
│  (Graph Database)   │
└─────────────────────┘
```

## Key Components

### 1. EventLogSink (NEW)

**Location**: `crates/nexora-risingwave/src/event_sink.rs`

**Purpose**: Bridge RisingWave materialized view changes to nexora-eventlog

**API**:
```rust
pub struct EventLogSink {
    event_store: Arc<IcebergEventLogStore>,
    risingwave: Arc<EventStreamingModule>,
}

impl EventLogSink {
    /// Create new sink
    pub fn new(
        event_store: Arc<IcebergEventLogStore>,
        risingwave: Arc<EventStreamingModule>,
    ) -> Self;

    /// Start syncing MV changes to event log
    pub async fn start_sync(
        &self,
        mv_name: &str,
        target_table: &str,
    ) -> Result<SyncHandle>;

    /// Stop syncing
    pub async fn stop_sync(&self, handle: SyncHandle) -> Result<()>;
}

pub struct SyncHandle {
    task: JoinHandle<Result<()>>,
    shutdown_tx: mpsc::Sender<()>,
}

/// Materialized view change event
#[derive(Debug)]
pub enum MvChange {
    Insert(Row),
    Update { old: Row, new: Row },
    Delete(Row),
}

pub struct Row {
    columns: Vec<Column>,
    values: Vec<Value>,
}
```

**Implementation Flow**:
```rust
impl EventLogSink {
    pub async fn start_sync(
        &self,
        mv_name: &str,
        target_table: &str,
    ) -> Result<SyncHandle> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        
        let event_store = self.event_store.clone();
        let risingwave = self.risingwave.clone();
        let mv_name = mv_name.to_string();
        let target_table = target_table.to_string();
        
        let task = tokio::spawn(async move {
            // Subscribe to MV changes via PostgreSQL LISTEN/NOTIFY
            let mut change_stream = risingwave
                .subscribe_mv_changes(&mv_name)
                .await?;
            
            loop {
                tokio::select! {
                    Some(change) = change_stream.next() => {
                        match change {
                            MvChange::Insert(row) => {
                                let event = row_to_event(&row)?;
                                event_store.append(&target_table, event).await?;
                            }
                            MvChange::Update { old, new } => {
                                // Handle update as delete + insert
                                let event = row_to_event(&new)?;
                                event_store.append(&target_table, event).await?;
                            }
                            MvChange::Delete(row) => {
                                // Soft delete or tombstone event
                                let tombstone = create_tombstone(&row)?;
                                event_store.append(&target_table, tombstone).await?;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
            
            Ok(())
        });
        
        Ok(SyncHandle { task, shutdown_tx })
    }
}

/// Convert RisingWave row to Iceberg event
fn row_to_event(row: &Row) -> Result<CloudEvent> {
    let mut data = serde_json::Map::new();
    
    for (col, val) in row.columns.iter().zip(row.values.iter()) {
        data.insert(col.name.clone(), value_to_json(val)?);
    }
    
    Ok(CloudEvent::new(
        Uuid::new_v4().to_string(),
        "com.nexora.risingwave.mv_change",
        serde_json::Value::Object(data),
    ))
}
```

---

### 2. MV Change Subscription

**RisingWave CDC Mechanism**: Use PostgreSQL LISTEN/NOTIFY or polling

**Option A: PostgreSQL LISTEN/NOTIFY** (Recommended)
```sql
-- In RisingWave, create trigger function (if supported)
CREATE FUNCTION notify_mv_change() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('mv_changes', json_build_object(
    'mv_name', TG_TABLE_NAME,
    'operation', TG_OP,
    'data', row_to_json(NEW)
  )::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach to materialized view
CREATE TRIGGER enriched_cargo_events_notify
AFTER INSERT OR UPDATE OR DELETE ON enriched_cargo_events
FOR EACH ROW EXECUTE FUNCTION notify_mv_change();
```

**Option B: Polling** (Fallback)
```rust
async fn poll_mv_changes(
    frontend: &FrontendConnection,
    mv_name: &str,
    last_timestamp: &mut SystemTime,
) -> Result<Vec<MvChange>> {
    let sql = format!(
        "SELECT * FROM {} WHERE _row_id > $1 ORDER BY _row_id",
        mv_name
    );
    
    let rows = frontend.query(&sql, &[&last_timestamp]).await?;
    
    // Convert rows to MvChange::Insert
    Ok(rows.into_iter().map(MvChange::Insert).collect())
}
```

---

### 3. Graph Projection (Future - Phase 7)

**Concept**: `GraphStreaming` layer projects enriched events to graph nodes/edges

```rust
// Future: crates/nexora-core/src/graph_streaming.rs
pub struct GraphStreaming {
    graph_service: Arc<GraphService>,
    event_store: Arc<IcebergEventLogStore>,
}

impl GraphStreaming {
    /// Project event to graph based on mapping rules
    pub async fn project_event(
        &self,
        event: &CloudEvent,
        mapping: &ProjectionMapping,
    ) -> Result<()> {
        match mapping.target {
            ProjectionTarget::Node { label, id_field } => {
                let node_id = event.data[id_field].as_str()?;
                self.graph_service.upsert_node(label, node_id, event.data).await?;
            }
            ProjectionTarget::Edge { from_field, to_field, label } => {
                let from = event.data[from_field].as_str()?;
                let to = event.data[to_field].as_str()?;
                self.graph_service.create_edge(from, label, to, event.data).await?;
            }
        }
        Ok(())
    }
}
```

---

## Use Case Example

### End-to-End: Logistics Cargo Tracking

**Step 1: Create Kafka Source in RisingWave**
```sql
CREATE SOURCE raw_cargo_events WITH (
    connector = 'kafka',
    topic = 'logistics.raw_events',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;
```

**Step 2: Create Materialized View with Enrichment**
```sql
CREATE MATERIALIZED VIEW enriched_cargo_events AS
SELECT 
    c.cargo_id,
    c.status,
    c.location_code,
    l.city,
    l.country,
    c.temperature,
    c.event_time,
    CASE 
        WHEN c.temperature > 25 THEN 'ALERT'
        ELSE 'NORMAL'
    END as alert_status
FROM raw_cargo_events c
LEFT JOIN location_lookup l ON c.location_code = l.code;
```

**Step 3: Start EventLogSink**
```rust
// In nexora-app startup
let sink = EventLogSink::new(event_store.clone(), event_streaming.clone());
let handle = sink.start_sync("enriched_cargo_events", "cargo_events").await?;

// Store handle for shutdown
app_state.event_sinks.push(handle);
```

**Step 4: Events Flow to Iceberg**
```
Kafka Message:
{
  "cargo_id": "CARGO-123",
  "status": "IN_TRANSIT",
  "location_code": "LAX",
  "temperature": 28,
  "event_time": "2026-08-02T10:00:00Z"
}

↓ (RisingWave enriches)

Materialized View Row:
{
  "cargo_id": "CARGO-123",
  "status": "IN_TRANSIT",
  "location_code": "LAX",
  "city": "Los Angeles",
  "country": "USA",
  "temperature": 28,
  "event_time": "2026-08-02T10:00:00Z",
  "alert_status": "ALERT"
}

↓ (EventLogSink converts)

Iceberg Event (CloudEvent):
{
  "specversion": "1.0",
  "id": "uuid-...",
  "type": "com.nexora.cargo.enriched",
  "source": "risingwave://enriched_cargo_events",
  "time": "2026-08-02T10:00:01Z",
  "data": {
    "cargo_id": "CARGO-123",
    "status": "IN_TRANSIT",
    "city": "Los Angeles",
    "country": "USA",
    "temperature": 28,
    "alert_status": "ALERT"
  }
}

↓ (Future: GraphStreaming projects)

Graph Nodes:
- (Cargo:CARGO-123 {status: "IN_TRANSIT", temperature: 28, alert: "ALERT"})
- (Location:LAX {city: "Los Angeles", country: "USA"})

Graph Edge:
- (Cargo:CARGO-123)-[:LOCATED_AT]->(Location:LAX)
```

---

## Implementation Plan

### Task 6.1: EventLogSink Core (2 days)

**Files**:
- `crates/nexora-risingwave/src/event_sink.rs` (NEW)
- `crates/nexora-risingwave/src/lib.rs` (MODIFIED)

**Tasks**:
- [ ] Implement `EventLogSink` struct
- [ ] Implement `start_sync()` with background task
- [ ] Implement `stop_sync()` for graceful shutdown
- [ ] Implement `row_to_event()` conversion
- [ ] Add `MvChange` enum (Insert/Update/Delete)
- [ ] Add unit tests

**Success Criteria**:
- ✅ Sink can subscribe to MV changes
- ✅ Rows convert to CloudEvents correctly
- ✅ Events written to Iceberg tables

---

### Task 6.2: MV Change Subscription (1 day)

**Files**:
- `crates/nexora-risingwave/src/frontend_wrapper.rs` (MODIFIED)

**Tasks**:
- [ ] Research RisingWave CDC capabilities
- [ ] Implement LISTEN/NOTIFY if available
- [ ] Fallback to polling if LISTEN/NOTIFY unavailable
- [ ] Add `subscribe_mv_changes()` to `EventStreamingModule`
- [ ] Add integration tests

**Success Criteria**:
- ✅ MV inserts trigger notifications
- ✅ Polling works as fallback
- ✅ Subscription survives connection loss

---

### Task 6.3: App Integration (1 day)

**Files**:
- `crates/nexora-app/src/main.rs` (MODIFIED)
- `crates/nexora-app/src/handlers/event_streaming.rs` (MODIFIED)

**Tasks**:
- [ ] Add `AppState.event_sinks: Vec<SyncHandle>`
- [ ] Start EventLogSink on app startup (if configured)
- [ ] Add HTTP endpoint: `POST /api/event-streaming/sync/start`
- [ ] Add HTTP endpoint: `POST /api/event-streaming/sync/stop`
- [ ] Add HTTP endpoint: `GET /api/event-streaming/sync/status`
- [ ] Add graceful shutdown of all sinks

**API Example**:
```bash
# Start syncing MV to EventLog
curl -X POST http://localhost:8080/api/event-streaming/sync/start \
  -H "Content-Type: application/json" \
  -d '{
    "mv_name": "enriched_cargo_events",
    "target_table": "cargo_events"
  }'

# Check sync status
curl http://localhost:8080/api/event-streaming/sync/status

# Stop syncing
curl -X POST http://localhost:8080/api/event-streaming/sync/stop \
  -H "Content-Type: application/json" \
  -d '{"mv_name": "enriched_cargo_events"}'
```

**Success Criteria**:
- ✅ Sync starts automatically on app startup
- ✅ HTTP API allows dynamic sync control
- ✅ Graceful shutdown stops all sinks

---

### Task 6.4: End-to-End Testing (2 days)

**Files**:
- `crates/nexora-app/tests/phase6_event_pipeline_test.rs` (NEW)

**Test Cases**:
1. **test_kafka_to_risingwave_to_eventlog**
   - Start Kafka (Docker)
   - Create RisingWave source
   - Create materialized view
   - Start EventLogSink
   - Publish Kafka messages
   - Verify events in Iceberg table

2. **test_mv_enrichment_pipeline**
   - Create lookup table in RisingWave
   - Create MV with JOIN
   - Verify enriched data in EventLog

3. **test_mv_aggregation_pipeline**
   - Create MV with GROUP BY
   - Verify aggregated events in EventLog

4. **test_sink_restart_recovery**
   - Start sink
   - Stop sink
   - Insert MV rows
   - Restart sink
   - Verify no data loss

**Success Criteria**:
- ✅ End-to-end pipeline works
- ✅ No data loss
- ✅ Performance acceptable (<1s latency P95)

---

### Task 6.5: Performance Benchmarking (1 day)

**Metrics to Measure**:
- End-to-end latency (Kafka → RisingWave → EventLog)
- Throughput (events/sec)
- EventLogSink overhead
- Memory usage

**Target Performance**:
- E2E latency: <1s (P95)
- Throughput: >1000 events/sec
- Sink overhead: <10% CPU
- Memory: <100MB per sink

**Tools**:
- `criterion` for benchmarks
- `flamegraph` for profiling
- Grafana for monitoring

---

## Configuration

### nexora.toml Updates

```toml
[event_streaming]
enabled = true
mode = "single"
meta_addr = "127.0.0.1:5690"
frontend_addr = "127.0.0.1:4566"

# NEW: Event sink configuration
[event_streaming.sinks]
enabled = true

[[event_streaming.sinks.sync]]
mv_name = "enriched_cargo_events"
target_table = "cargo_events"
auto_start = true

[[event_streaming.sinks.sync]]
mv_name = "user_activity_counts"
target_table = "user_events"
auto_start = false  # Manual start via API
```

---

## Deliverables

- [ ] `crates/nexora-risingwave/src/event_sink.rs` (~300 LOC)
- [ ] `subscribe_mv_changes()` in `EventStreamingModule`
- [ ] HTTP API endpoints for sync control
- [ ] End-to-end test suite
- [ ] Performance benchmarks
- [ ] Documentation: `PHASE6_EVENT_PIPELINE_COMPLETE.md`

---

## Success Criteria

Phase 6 is complete when:
- ✅ EventLogSink implemented and tested
- ✅ MV changes flow to Iceberg tables
- ✅ HTTP API for sync management
- ✅ End-to-end test passes
- ✅ Performance targets met
- ✅ Documentation complete

---

## Known Limitations

1. **RisingWave CDC Support**: Need to verify if RisingWave supports PostgreSQL LISTEN/NOTIFY for MVs
   - If not, fallback to polling (higher latency)
   
2. **Schema Evolution**: MV schema changes not handled automatically
   - Manual intervention required if MV structure changes
   
3. **Backpressure**: No backpressure mechanism if EventLog writes slow
   - Risk of buffering in memory

4. **Exactly-Once Semantics**: Current design is at-least-once
   - Duplicate events possible on restart

---

## Next Steps (Phase 7)

After Phase 6, Phase 7 will focus on:
1. **GraphStreaming Layer**: Automatic graph projection from events
2. **Projection Mappings**: User-defined event → node/edge rules
3. **Incremental Updates**: Efficient graph updates from event stream
4. **Schema Registry**: Manage event schemas and projection mappings

---

**Last Updated**: 2026-08-02  
**Document Version**: 1.0  
**Status**: Planning - Ready to Start After Phase 5.4 Decision
