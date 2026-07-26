# Phase 6: Event Pipeline Integration - Implementation Plan

**Status**: ⏳ Ready to Start  
**Estimated Duration**: 1 week (40 hours)  
**Dependencies**: Phase 1-5 Complete ✅

## Overview

Phase 6 completes the RisingWave integration by connecting the full event processing pipeline. This enables the advanced path: **Kafka → RisingWave SQL MV → EventLogStore → Graph**, allowing complex SQL transformations and enrichment before graph ingestion.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Event Pipeline Paths                      │
├──────────────────────────────────────────────────────────────┤
│  Path A (Simple - Existing):                                 │
│    Kafka → nexora-stream → nexora-eventlog → nexora-core    │
│                                                               │
│  Path B (Advanced - NEW):                                    │
│    Kafka → RisingWave (SQL MV) → EventLogSink →             │
│           nexora-eventlog → nexora-core                      │
└──────────────────────────────────────────────────────────────┘
```

## Implementation Tasks

### Task 1: EventLogSink Bridge (8 hours)

**Goal**: Stream RisingWave MV changes to EventLogStore

**File**: `crates/nexora-risingwave/src/event_sink.rs`

```rust
use nexora_eventlog::EventLogStore;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

/// Bridge between RisingWave materialized views and Nexora EventLogStore
pub struct EventLogSink {
    event_store: Arc<EventLogStore>,
    risingwave: Arc<RisingWaveModule>,
}

impl EventLogSink {
    pub fn new(
        event_store: Arc<EventLogStore>,
        risingwave: Arc<RisingWaveModule>,
    ) -> Self {
        Self { event_store, risingwave }
    }

    /// Subscribe to MV changes and stream to EventLogStore
    pub async fn start_sync(&self, mv_name: &str, topic: &str) -> Result<()> {
        let mut rx = self.risingwave.subscribe_mv(mv_name).await?;
        
        while let Some(change) = rx.recv().await {
            match change {
                Change::Insert(row) => {
                    let event = self.row_to_event(&row)?;
                    self.event_store.append(topic, event).await?;
                }
                Change::Update { old, new } => {
                    // Handle updates (delete old, insert new)
                    let event = self.row_to_event(&new)?;
                    self.event_store.append(topic, event).await?;
                }
                Change::Delete(row) => {
                    // Mark as deleted or skip (depends on use case)
                    tracing::debug!("Skipping delete for row: {:?}", row);
                }
            }
        }
        Ok(())
    }

    /// Convert RisingWave Row to Nexora Event (JSON)
    fn row_to_event(&self, row: &Row) -> Result<serde_json::Value> {
        // Extract columns and build JSON object
        let mut obj = serde_json::Map::new();
        for (col_name, col_value) in row.columns() {
            obj.insert(col_name.to_string(), self.value_to_json(col_value)?);
        }
        Ok(serde_json::Value::Object(obj))
    }

    fn value_to_json(&self, value: &ColumnValue) -> Result<serde_json::Value> {
        // Convert RisingWave types to JSON
        Ok(match value {
            ColumnValue::Int32(v) => json!(v),
            ColumnValue::Int64(v) => json!(v),
            ColumnValue::Float64(v) => json!(v),
            ColumnValue::String(v) => json!(v),
            ColumnValue::Boolean(v) => json!(v),
            ColumnValue::Timestamp(v) => json!(v.to_rfc3339()),
            ColumnValue::Null => serde_json::Value::Null,
        })
    }
}
```

**Testing**:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_event_sink_basic() {
        // Mock RisingWave module with test MV
        // Mock EventLogStore
        // Verify events land in store
    }
}
```

### Task 2: MV Change Subscription (6 hours)

**Goal**: Implement CDC-like streaming from RisingWave MVs

**File**: `crates/nexora-risingwave/src/lib.rs` (extend)

```rust
#[derive(Debug, Clone)]
pub enum Change {
    Insert(Row),
    Update { old: Row, new: Row },
    Delete(Row),
}

#[derive(Debug, Clone)]
pub struct Row {
    columns: Vec<(String, ColumnValue)>,
}

#[derive(Debug, Clone)]
pub enum ColumnValue {
    Int32(i32),
    Int64(i64),
    Float64(f64),
    String(String),
    Boolean(bool),
    Timestamp(chrono::DateTime<chrono::Utc>),
    Null,
}

impl RisingWaveModule {
    /// Subscribe to materialized view changes (CDC-like)
    pub async fn subscribe_mv(&self, mv_name: &str) -> Result<Receiver<Change>> {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        
        // Connect to RisingWave Frontend
        let frontend = self.frontend.clone();
        let mv_name = mv_name.to_string();
        
        tokio::spawn(async move {
            // Query MV periodically or use RisingWave CDC connector
            // For now: simple polling approach
            loop {
                // Poll MV for new rows (watermark-based)
                // Convert to Change events
                // Send via tx
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        
        Ok(rx)
    }
}
```

### Task 3: SQL DDL → Domain Package Parser (8 hours)

**Goal**: Auto-generate DomainPackages from CREATE MATERIALIZED VIEW

**File**: `crates/nexora-risingwave/src/ddl_parser.rs`

```rust
use nexora_core::domain_package::DomainPackage;
use sqlparser::ast::{Statement, CreateTable, ColumnDef};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

pub struct DdlParser;

impl DdlParser {
    /// Parse CREATE MATERIALIZED VIEW and generate DomainPackage
    pub fn parse_create_mv(sql: &str) -> Result<DomainPackage> {
        let dialect = PostgreSqlDialect {};
        let ast = Parser::parse_sql(&dialect, sql)?;
        
        for stmt in ast {
            if let Statement::CreateView { name, columns, query, materialized, .. } = stmt {
                if !materialized {
                    return Err(anyhow::anyhow!("Not a materialized view"));
                }
                
                let view_name = name.to_string();
                let schema = Self::extract_schema(&columns, query)?;
                
                return Ok(DomainPackage {
                    name: view_name.clone(),
                    version: "1.0.0".to_string(),
                    description: format!("Auto-generated from MV: {}", view_name),
                    entities: vec![Self::schema_to_entity(&view_name, &schema)],
                    mappings: vec![],
                    relationships: vec![],
                });
            }
        }
        
        Err(anyhow::anyhow!("No CREATE MATERIALIZED VIEW found"))
    }
    
    fn extract_schema(
        columns: &[ColumnDef],
        query: &Query,
    ) -> Result<Vec<(String, String)>> {
        // Extract column names and types from SELECT projection
        let mut schema = Vec::new();
        
        for col in columns {
            let name = col.name.to_string();
            let typ = Self::sql_type_to_nexora_type(&col.data_type)?;
            schema.push((name, typ));
        }
        
        Ok(schema)
    }
    
    fn sql_type_to_nexora_type(sql_type: &DataType) -> Result<String> {
        Ok(match sql_type {
            DataType::Int(_) | DataType::Integer(_) => "integer".to_string(),
            DataType::BigInt(_) => "long".to_string(),
            DataType::Float(_) | DataType::Double => "double".to_string(),
            DataType::Varchar(_) | DataType::Text => "string".to_string(),
            DataType::Boolean => "boolean".to_string(),
            DataType::Timestamp(_, _) => "timestamp".to_string(),
            _ => "string".to_string(), // Fallback
        })
    }
    
    fn schema_to_entity(name: &str, schema: &[(String, String)]) -> Entity {
        Entity {
            name: name.to_string(),
            properties: schema
                .iter()
                .map(|(name, typ)| Property {
                    name: name.clone(),
                    property_type: typ.clone(),
                    required: false,
                })
                .collect(),
        }
    }
}
```

**Testing**:
```rust
#[test]
fn test_parse_create_mv() {
    let sql = r#"
        CREATE MATERIALIZED VIEW enriched_cargo AS
        SELECT cargo_id, status, location, city
        FROM raw_events
        LEFT JOIN locations ON raw_events.location = locations.code
    "#;
    
    let pkg = DdlParser::parse_create_mv(sql).unwrap();
    assert_eq!(pkg.name, "enriched_cargo");
    assert_eq!(pkg.entities.len(), 1);
}
```

### Task 4: Catalog Introspection (6 hours)

**Goal**: Query RisingWave system catalog for sources and MVs

**File**: `crates/nexora-risingwave/src/catalog.rs`

```rust
use tokio_postgres::{Client, NoTls};

pub struct CatalogClient {
    pg_client: Client,
}

impl CatalogClient {
    pub async fn connect(frontend_addr: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(
            &format!("host={} port=4566 user=root dbname=dev", frontend_addr),
            NoTls,
        ).await?;
        
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        
        Ok(Self { pg_client: client })
    }
    
    /// List all sources from RisingWave catalog
    pub async fn list_sources(&self) -> Result<Vec<SourceInfo>> {
        let rows = self.pg_client
            .query(
                "SELECT name, connector, schema_name FROM rw_sources",
                &[],
            )
            .await?;
        
        let mut sources = Vec::new();
        for row in rows {
            sources.push(SourceInfo {
                name: row.get(0),
                connector: row.get(1),
                schema: row.get(2),
            });
        }
        Ok(sources)
    }
    
    /// List all materialized views
    pub async fn list_materialized_views(&self) -> Result<Vec<MaterializedViewInfo>> {
        let rows = self.pg_client
            .query(
                "SELECT name, definition, schema_name FROM rw_materialized_views",
                &[],
            )
            .await?;
        
        let mut mvs = Vec::new();
        for row in rows {
            mvs.push(MaterializedViewInfo {
                name: row.get(0),
                definition: row.get(1),
                schema: row.get(2),
            });
        }
        Ok(mvs)
    }
}

#[derive(Debug, Clone)]
pub struct SourceInfo {
    pub name: String,
    pub connector: String,
    pub schema: String,
}

#[derive(Debug, Clone)]
pub struct MaterializedViewInfo {
    pub name: String,
    pub definition: String,
    pub schema: String,
}
```

**Update handlers**:
```rust
// crates/nexora-app/src/handlers/risingwave.rs

pub async fn list_sources(
    State(state): State<AppState>,
) -> Result<Json<Vec<RisingWaveSource>>, ApiError> {
    let rw = state.risingwave.as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("risingwave".to_string()))?;
    
    let sources = rw.list_sources().await
        .map_err(|e| ApiError::Internal(format!("Failed to list sources: {}", e)))?;
    
    let response = sources
        .iter()
        .map(|s| RisingWaveSource {
            name: s.name.clone(),
            connector: s.connector.clone(),
            status: "active".to_string(),
        })
        .collect();
    
    Ok(Json(response))
}

pub async fn list_materialized_views(
    State(state): State<AppState>,
) -> Result<Json<Vec<RisingWaveMaterializedView>>, ApiError> {
    let rw = state.risingwave.as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("risingwave".to_string()))?;
    
    let mvs = rw.list_materialized_views().await
        .map_err(|e| ApiError::Internal(format!("Failed to list MVs: {}", e)))?;
    
    let response = mvs
        .iter()
        .map(|mv| RisingWaveMaterializedView {
            name: mv.name.clone(),
            definition: mv.definition.clone(),
            status: "active".to_string(),
        })
        .collect();
    
    Ok(Json(response))
}
```

### Task 5: Integration Tests (10 hours)

**File**: `crates/nexora-risingwave/tests/e2e_pipeline.rs`

```rust
use nexora_risingwave::*;
use nexora_eventlog::EventLogStore;
use testcontainers::*;

#[tokio::test]
#[ignore] // Requires Docker
async fn test_kafka_risingwave_eventlog_pipeline() {
    // 1. Start Kafka container
    let kafka = clients::Cli::default()
        .run(images::kafka::Kafka::default());
    
    // 2. Start RisingWave (Meta + Frontend + Compute)
    let rw_config = RisingWaveConfig::new()
        .with_meta_addr("127.0.0.1:5690".parse().unwrap())
        .with_frontend_addr("127.0.0.1:4566".parse().unwrap());
    let risingwave = RisingWaveModule::start(rw_config).await.unwrap();
    
    // 3. Create EventLogStore
    let event_store = Arc::new(
        EventLogStore::new_with_config(
            nexora_eventlog::StorageConfig::local_fs("./test_events")
        ).await.unwrap()
    );
    
    // 4. Create Kafka source in RisingWave
    risingwave.execute_ddl(r#"
        CREATE SOURCE test_events WITH (
            connector = 'kafka',
            topic = 'test_topic',
            properties.bootstrap.server = 'localhost:9092'
        ) FORMAT PLAIN ENCODE JSON
    "#).await.unwrap();
    
    // 5. Create materialized view
    risingwave.execute_ddl(r#"
        CREATE MATERIALIZED VIEW enriched_events AS
        SELECT id, status, timestamp
        FROM test_events
        WHERE status = 'active'
    "#).await.unwrap();
    
    // 6. Start EventLogSink
    let sink = EventLogSink::new(event_store.clone(), Arc::new(risingwave));
    let sink_handle = tokio::spawn(async move {
        sink.start_sync("enriched_events", "test.enriched").await
    });
    
    // 7. Publish test event to Kafka
    // ... (use rdkafka producer)
    
    // 8. Wait for event to flow through pipeline
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // 9. Verify event landed in EventLogStore
    let events = event_store.scan_table("test.enriched").await.unwrap();
    assert!(!events.is_empty());
    
    // Cleanup
    sink_handle.abort();
}
```

### Task 6: Performance Benchmarks (6 hours)

**File**: `crates/nexora-risingwave/benches/pipeline_overhead.rs`

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_direct_ingestion(c: &mut Criterion) {
    // Measure: Kafka → nexora-stream → EventLogStore
    c.bench_function("direct_ingestion", |b| {
        b.iter(|| {
            // Ingest 1000 events directly
            black_box(/* ... */)
        })
    });
}

fn bench_risingwave_pipeline(c: &mut Criterion) {
    // Measure: Kafka → RisingWave → EventLogStore
    c.bench_function("risingwave_pipeline", |b| {
        b.iter(|| {
            // Ingest 1000 events via RisingWave
            black_box(/* ... */)
        })
    });
}

criterion_group!(benches, bench_direct_ingestion, bench_risingwave_pipeline);
criterion_main!(benches);
```

### Task 7: Documentation (2 hours)

**File**: `docs/USER_GUIDE_RISINGWAVE.md`

```markdown
# RisingWave Integration User Guide

## Overview

RisingWave integration enables advanced SQL-based stream processing
before graph ingestion. Use it for:
- Complex SQL transformations (joins, aggregations, window functions)
- Multi-stream temporal joins
- Real-time data enrichment
- CDC-like change streaming

## Quick Start

### 1. Enable RisingWave Feature

Build with RisingWave support:
```bash
cargo build --release --features risingwave
```

### 2. Start Nexora with RisingWave

```bash
cargo run --release --features risingwave -- \
  --enable-risingwave \
  --risingwave-meta-addr 127.0.0.1:5690 \
  --risingwave-frontend-addr 127.0.0.1:4566
```

### 3. Create Kafka Source

```bash
curl -X POST http://localhost:8080/api/risingwave/ddl \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "CREATE SOURCE raw_events WITH (connector = '\''kafka'\'', topic = '\''events'\'', properties.bootstrap.server = '\''kafka:9092'\'') FORMAT PLAIN ENCODE JSON"
  }'
```

### 4. Create Materialized View

```bash
curl -X POST http://localhost:8080/api/risingwave/ddl \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "CREATE MATERIALIZED VIEW enriched AS SELECT id, status FROM raw_events WHERE status = '\''active'\''"
  }'
```

### 5. Query Results

```bash
curl -X POST http://localhost:8080/api/risingwave/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM enriched LIMIT 10"}'
```

## Use Cases

### Use Case 1: Stream Enrichment

```sql
-- Join event stream with lookup table
CREATE MATERIALIZED VIEW enriched_cargo AS
SELECT 
    e.cargo_id,
    e.status,
    l.city,
    l.country
FROM raw_cargo_events e
LEFT JOIN location_lookup l ON e.location_code = l.code;
```

### Use Case 2: Temporal Joins

```sql
-- Join streams based on time windows
CREATE MATERIALIZED VIEW matched_events AS
SELECT 
    a.order_id,
    a.timestamp AS order_time,
    b.timestamp AS shipment_time
FROM orders a
JOIN shipments b
    ON a.order_id = b.order_id
    AND b.timestamp BETWEEN a.timestamp AND a.timestamp + INTERVAL '1' HOUR;
```

### Use Case 3: Aggregations

```sql
-- Tumbling window aggregation
CREATE MATERIALIZED VIEW order_stats_5min AS
SELECT 
    window_start,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM TUMBLE(orders, timestamp, INTERVAL '5' MINUTE)
GROUP BY window_start;
```

## Performance Tuning

### When to Use RisingWave

✅ Use RisingWave when:
- Need complex SQL transformations
- Multi-stream joins required
- Real-time aggregations needed
- SQL expertise in team

❌ Use Direct Path when:
- Simple event-to-graph mapping
- <10ms latency requirement
- Memory-constrained (<2GB available)
- No SQL transformation needed

### Performance Tips

1. **Index materialized views** for faster queries
2. **Use tumbling windows** instead of sliding for aggregations
3. **Limit join fanout** to avoid memory pressure
4. **Monitor RisingWave metrics** via /api/risingwave/status

## Troubleshooting

### RisingWave not starting
- Check ports 5690 (Meta) and 4566 (Frontend) are available
- Verify --enable-risingwave flag is set
- Check logs: `RUST_LOG=nexora_risingwave=debug cargo run ...`

### Events not flowing to graph
- Verify EventLogSink is running: check /api/risingwave/status
- Check MV definition: curl /api/risingwave/materialized_views
- Verify Kafka connectivity

### High latency
- Measure overhead: `cargo bench --bench pipeline_overhead`
- Consider direct path for simple transformations
- Tune RisingWave parallelism
```

## Deliverables

- [ ] `crates/nexora-risingwave/src/event_sink.rs` (EventLogSink)
- [ ] `crates/nexora-risingwave/src/lib.rs` (subscribe_mv method)
- [ ] `crates/nexora-risingwave/src/ddl_parser.rs` (SQL → DomainPackage)
- [ ] `crates/nexora-risingwave/src/catalog.rs` (Introspection)
- [ ] `crates/nexora-app/src/handlers/risingwave.rs` (Updated handlers)
- [ ] `crates/nexora-risingwave/tests/e2e_pipeline.rs` (Integration test)
- [ ] `crates/nexora-risingwave/benches/pipeline_overhead.rs` (Benchmarks)
- [ ] `docs/USER_GUIDE_RISINGWAVE.md` (User documentation)
- [ ] `docs/RISINGWAVE_PHASE6_REPORT.md` (Implementation report)

## Success Criteria

✅ EventLogSink streaming MV changes → EventLogStore  
✅ SQL DDL auto-generates DomainPackages  
✅ Catalog introspection returns real sources/MVs  
✅ End-to-end test passing (Kafka → RisingWave → Graph)  
✅ Performance benchmark: <10ms overhead for RisingWave path  
✅ User documentation complete

## Timeline

| Day | Task | Hours | Status |
|-----|------|-------|--------|
| 1 | EventLogSink implementation | 8 | ⏳ |
| 2 | MV subscription + catalog | 12 | ⏳ |
| 3 | DDL parser | 8 | ⏳ |
| 4 | Integration tests (Part 1) | 6 | ⏳ |
| 5 | Integration tests (Part 2) + benchmarks | 10 | ⏳ |
| 5 | Documentation | 2 | ⏳ |

**Total**: 40 hours (1 week)

## Dependencies

✅ Phase 1: Repository Setup (Complete)  
✅ Phase 2: Shared Infrastructure (Complete)  
✅ Phase 3: RisingWave Wrapper (Complete)  
✅ Phase 4: Raft HA Extension (Complete)  
✅ Phase 5: App Integration (Complete)  
⏳ Phase 6: Event Pipeline (Ready to start)

---

**Created**: 2026-07-26  
**Status**: Planning Complete - Awaiting User Approval
