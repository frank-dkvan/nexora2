# RisingWave Iceberg Sink Integration with Nexora EventLog

## Executive Summary

RisingWave **already has a built-in Iceberg sink** with full support for REST catalogs, which means Phase 4's "EventLogSink implementation" task does not require writing a sink from scratch. Instead, we need to configure RisingWave's existing Iceberg sink to write to nexora-eventlog's Iceberg tables.

**Key Finding**: RisingWave's Iceberg sink supports:
- ✅ REST catalog (compatible with nexora-eventlog's Lakekeeper/Polaris setup)
- ✅ S3-compatible object storage (MinIO, AWS S3)
- ✅ Append-only and upsert modes
- ✅ OAuth2 authentication for REST catalogs
- ✅ Automatic table creation
- ✅ Schema evolution

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Event Pipeline (Phase 4)                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Kafka/Kinesis (Raw Events)                                      │
│      │                                                            │
│      ├──► RisingWave CREATE SOURCE                               │
│      │         │                                                  │
│      │         ├──► Materialized Views (SQL transformations)     │
│      │         │                                                  │
│      │         └──► CREATE SINK INTO nexora_events (            │
│      │                  connector='iceberg',                     │
│      │                  catalog.type='rest',                     │
│      │                  catalog.uri='http://localhost:8181/..', │
│      │                  warehouse.path='s3://nexora-events/'     │
│      │              )                                             │
│      │                   │                                        │
│      │                   └──► Iceberg Table (nexora-eventlog)    │
│      │                            │                               │
│      └────────────────────────────┴──► nexora-core (Graph)       │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## RisingWave Iceberg Sink Configuration

### Basic REST Catalog Configuration

```sql
CREATE SINK enriched_events_sink
FROM enriched_events_mv
WITH (
    connector = 'iceberg',
    
    -- Catalog configuration (matches nexora-eventlog REST backend)
    catalog.type = 'rest',
    catalog.uri = 'http://localhost:8181/catalog',
    catalog.name = 'nexora',
    warehouse.path = 's3://nexora-events/',
    
    -- S3 configuration (same as nexora-eventlog)
    s3.endpoint = 'http://localhost:9000',
    s3.region = 'us-east-1',
    s3.access.key = 'minioadmin',
    s3.secret.key = 'minioadmin',
    s3.path_style_access = 'true',
    
    -- Table location
    database.name = 'nexora_db',
    table.name = 'events',
    
    -- Write mode
    type = 'append-only',
    force_append_only = 'true'
);
```

### Configuration Mapping

| nexora-eventlog Config | RisingWave Sink Property | Example Value |
|------------------------|-------------------------|---------------|
| `storage.backend = "rest"` | `catalog.type = 'rest'` | `'rest'` |
| `rest.uri` | `catalog.uri` | `'http://localhost:8181/catalog'` |
| `rest.warehouse` | `warehouse.path` | `'s3://nexora-events/'` |
| `rest.s3_endpoint` | `s3.endpoint` | `'http://localhost:9000'` |
| `rest.s3_region` | `s3.region` | `'us-east-1'` |
| `rest.s3_access_key` | `s3.access.key` | `'minioadmin'` |
| `rest.s3_secret_key` | `s3.secret.key` | `'minioadmin'` |
| `rest.s3_path_style` | `s3.path_style_access` | `'true'` |

## Schema Compatibility

### nexora-eventlog Event Schema

```rust
// crates/nexora-eventlog/src/lib.rs
pub struct Event {
    pub event_id: String,        // UUID
    pub event_type: String,      // e.g. "cargo.moved"
    pub timestamp: i64,          // Unix timestamp (millis)
    pub payload: serde_json::Value,  // JSON object
    pub source: Option<String>,  // Optional source system
}
```

### RisingWave Materialized View Schema

```sql
CREATE MATERIALIZED VIEW enriched_events_mv AS
SELECT
    event_id::VARCHAR AS event_id,
    event_type::VARCHAR AS event_type,
    timestamp::BIGINT AS timestamp,
    payload::JSONB AS payload,
    source::VARCHAR AS source
FROM raw_events_source;
```

### Iceberg Table Schema (Auto-Created)

When RisingWave creates the Iceberg table, it will map SQL types to Iceberg types:

| RisingWave Type | Iceberg Type | Arrow Type |
|-----------------|--------------|------------|
| `VARCHAR` | `string` | `Utf8` |
| `BIGINT` | `long` | `Int64` |
| `JSONB` | `string` | `Utf8` (serialized JSON) |

**Important**: nexora-eventlog's `EventLogStore::append()` method serializes events to Arrow record batches with this schema:

```rust
Schema::new(vec![
    Field::new("event_id", DataType::Utf8, false),
    Field::new("event_type", DataType::Utf8, false),
    Field::new("timestamp", DataType::Int64, false),
    Field::new("payload", DataType::Utf8, false),  // JSON serialized
    Field::new("source", DataType::Utf8, true),
])
```

RisingWave's Iceberg sink will write the same schema automatically.

## Integration Steps

### Step 1: Configure nexora-eventlog with REST Catalog

Update `nexora.toml`:

```toml
[event_store]
backend = "rest"
rest_uri = "http://localhost:8181/catalog"
rest_warehouse = "nexora"
s3_endpoint = "http://localhost:9000"
s3_bucket = "nexora-events"
s3_region = "us-east-1"
s3_access_key = "minioadmin"
s3_secret_key = "minioadmin"
s3_path_style = true
```

### Step 2: Start RisingWave with Library Mode

```bash
cargo build --release --features event-first,event-streaming,library

./target/release/nexora --config nexora.toml
```

### Step 3: Create Source in RisingWave

```sql
-- Connect via psql
psql -h localhost -p 4566 -U root -d dev

-- Create Kafka source
CREATE SOURCE raw_cargo_events (
    cargo_id VARCHAR,
    status VARCHAR,
    location VARCHAR,
    event_time TIMESTAMP
) WITH (
    connector = 'kafka',
    topic = 'logistics.raw_events',
    properties.bootstrap.server = 'localhost:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;
```

### Step 4: Create Materialized View with Transformations

```sql
CREATE MATERIALIZED VIEW enriched_cargo_events AS
SELECT
    gen_random_uuid()::VARCHAR AS event_id,
    'cargo.' || status AS event_type,
    extract(epoch from event_time)::BIGINT * 1000 AS timestamp,
    json_build_object(
        'cargo_id', cargo_id,
        'status', status,
        'location', location
    )::VARCHAR AS payload,
    'kafka-source' AS source
FROM raw_cargo_events;
```

### Step 5: Create Iceberg Sink to nexora-eventlog

```sql
CREATE SINK nexora_events_sink
FROM enriched_cargo_events
WITH (
    connector = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://localhost:8181/catalog',
    warehouse.path = 's3://nexora-events/',
    s3.endpoint = 'http://localhost:9000',
    s3.region = 'us-east-1',
    s3.access.key = 'minioadmin',
    s3.secret.key = 'minioadmin',
    s3.path_style_access = 'true',
    database.name = 'nexora_db',
    table.name = 'events',
    type = 'append-only',
    force_append_only = 'true',
    create_table_if_not_exists = 'true'
);
```

### Step 6: Verify Data Flow

```bash
# Check RisingWave sink status
psql -h localhost -p 4566 -U root -d dev -c "SHOW SINKS;"

# Query nexora-eventlog via HTTP API
curl http://localhost:8080/api/events/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM events LIMIT 10"}'

# Query graph nodes created from events
curl http://localhost:8080/api/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (c:Cargo) RETURN count(c)"}'
```

## Advanced Configuration

### Authentication (OAuth2)

For production REST catalogs with OAuth2:

```sql
CREATE SINK nexora_events_sink
FROM enriched_cargo_events
WITH (
    connector = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'https://catalog.example.com/v1',
    catalog.credential = 'client_id:client_secret',
    catalog.oauth2_server_uri = 'https://auth.example.com/token',
    catalog.scope = 'catalog:rw',
    -- ... other options
);
```

### Upsert Mode (Deduplication)

If you need deduplication by event_id:

```sql
CREATE SINK nexora_events_sink
FROM enriched_cargo_events
WITH (
    connector = 'iceberg',
    -- ... catalog config ...
    type = 'upsert',
    primary_key = 'event_id'
);
```

### Partitioning

Partition by event date for better query performance:

```sql
CREATE SINK nexora_events_sink
FROM enriched_cargo_events
WITH (
    connector = 'iceberg',
    -- ... catalog config ...
    partition_columns = 'event_date',  -- Add event_date column to MV
    type = 'append-only'
);
```

### Compaction

Enable automatic small file compaction:

```sql
CREATE SINK nexora_events_sink
FROM enriched_cargo_events
WITH (
    connector = 'iceberg',
    -- ... catalog config ...
    enable_compaction = 'true',
    compaction_interval_sec = '3600',  -- Compact every hour
    'compaction.target_file_size_mb' = '128'
);
```

## Testing

### Unit Test: Schema Compatibility

```rust
// crates/nexora-risingwave/tests/iceberg_sink_test.rs
#[tokio::test]
async fn test_risingwave_nexora_schema_compatibility() {
    // 1. Create EventLogStore with REST catalog
    let storage_config = StorageConfig::rest(
        "http://localhost:8181/catalog",
        "nexora",
        "http://localhost:9000",
        "us-east-1",
        "minioadmin",
        "minioadmin",
        true,
    );
    let event_store = EventLogStore::new(storage_config).await.unwrap();

    // 2. Write event via nexora-eventlog
    let event = Event {
        event_id: "test-123".to_string(),
        event_type: "cargo.moved".to_string(),
        timestamp: 1234567890000,
        payload: json!({"cargo_id": "C001"}),
        source: Some("test".to_string()),
    };
    event_store.append("events", event).await.unwrap();

    // 3. Query via RisingWave (after sink setup)
    // psql query or HTTP API call here
    
    // 4. Verify row count matches
}
```

### Integration Test: Full Pipeline

```bash
#!/bin/bash
# scripts/test-risingwave-iceberg-pipeline.sh

set -e

echo "Starting full pipeline test..."

# 1. Start services
docker-compose up -d kafka minio lakekeeper

# 2. Start Nexora with RisingWave
cargo run --release --features event-first,event-streaming,library -- \
  --config nexora.toml &
NEXORA_PID=$!

sleep 10

# 3. Create RisingWave source and sink
psql -h localhost -p 4566 -U root -d dev <<EOF
CREATE SOURCE test_events (event_id VARCHAR, event_type VARCHAR, ts BIGINT, data VARCHAR)
WITH (connector='kafka', topic='test', properties.bootstrap.server='localhost:9092')
FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW test_mv AS SELECT * FROM test_events;

CREATE SINK test_sink FROM test_mv
WITH (connector='iceberg', catalog.type='rest',
      catalog.uri='http://localhost:8181/catalog',
      warehouse.path='s3://nexora-events/',
      s3.endpoint='http://localhost:9000',
      s3.region='us-east-1',
      s3.access.key='minioadmin',
      s3.secret.key='minioadmin',
      s3.path_style_access='true',
      database.name='nexora_db', table.name='events',
      type='append-only', create_table_if_not_exists='true');
EOF

# 4. Send test events to Kafka
echo '{"event_id":"1","event_type":"test","ts":123,"data":"hello"}' | \
  kafka-console-producer --broker-list localhost:9092 --topic test

# 5. Wait for sink to process
sleep 5

# 6. Query via nexora-eventlog
RESULT=$(curl -s http://localhost:8080/api/events/query \
  -H "Content-Type: application/json" \
  -d '{"query":"SELECT COUNT(*) FROM events"}')

echo "Result: $RESULT"

# 7. Cleanup
kill $NEXORA_PID
docker-compose down

echo "Pipeline test complete!"
```

## Troubleshooting

### Issue 1: Schema Mismatch

**Symptom**: RisingWave sink fails with "schema mismatch" error

**Cause**: Materialized view schema doesn't match nexora-eventlog's expected schema

**Fix**: Ensure MV has exactly these columns:
- `event_id VARCHAR`
- `event_type VARCHAR`
- `timestamp BIGINT`
- `payload VARCHAR` (JSON serialized)
- `source VARCHAR`

### Issue 2: REST Catalog Connection Failed

**Symptom**: `CREATE SINK` fails with "failed to connect to catalog"

**Cause**: Lakekeeper/Polaris not running or wrong URI

**Fix**:
```bash
# Check catalog is accessible
curl http://localhost:8181/catalog/v1/config

# Check nexora.toml has correct rest_uri
grep rest_uri nexora.toml
```

### Issue 3: S3 Permission Denied

**Symptom**: Sink writes fail with "Access Denied" S3 error

**Cause**: RisingWave's S3 credentials don't match MinIO

**Fix**: Ensure `s3.access.key` and `s3.secret.key` match nexora.toml:
```sql
SHOW CREATE SINK nexora_events_sink;
-- Compare credentials with nexora.toml [event_store] section
```

### Issue 4: Duplicate Events

**Symptom**: Same event appears multiple times in nexora-eventlog

**Cause**: Sink retries on transient failures

**Fix**: Use upsert mode with `primary_key = 'event_id'`:
```sql
ALTER SINK nexora_events_sink SET type = 'upsert', primary_key = 'event_id';
```

## Performance Tuning

### Throughput Optimization

```sql
CREATE SINK nexora_events_sink
FROM enriched_cargo_events
WITH (
    connector = 'iceberg',
    -- ... catalog config ...
    
    -- Increase commit interval (default 10s)
    commit_checkpoint_interval = 60,
    
    -- Larger row group size for Parquet
    'write.parquet.row_group_size' = '262144',
    
    -- Compression
    'write.parquet.compression' = 'zstd'
);
```

### Memory Optimization

```sql
CREATE SINK nexora_events_sink
FROM enriched_cargo_events
WITH (
    connector = 'iceberg',
    -- ... catalog config ...
    
    -- Smaller row group size
    'write.parquet.row_group_size' = '65536',
    
    -- More frequent commits (reduces memory)
    commit_checkpoint_interval = 5
);
```

## Phase 4 Updated Task List

Based on the discovery that RisingWave already has a complete Iceberg sink, the Phase 4 tasks are simplified:

| Task | Original Scope | Updated Scope | Status |
|------|---------------|---------------|--------|
| 1. EventLogSink Implementation | Write custom Rust sink | ~~Not needed~~ Configure RisingWave sink | ⏳ Pending |
| 2. Schema Mapping | Design Arrow ↔ RisingWave conversion | ~~Not needed~~ Verify auto-mapping | ⏳ Pending |
| 3. REST Catalog Integration | Implement REST client | ~~Not needed~~ Test connectivity | ⏳ Pending |
| 4. End-to-End Test | Build full test harness | Bash script + SQL DDL | ⏳ Pending |
| 5. GraphStreaming | EventLog → Graph projection | Design and implement | ⏳ Pending |

**Estimated Time**: 2-3 days (down from original 1 week)

## Next Steps

1. **Test REST Catalog Connectivity** (30 minutes)
   - Start Lakekeeper with same config as nexora-eventlog
   - Verify RisingWave can connect to REST catalog
   - Test `CREATE SINK` with minimal config

2. **Schema Validation** (1 hour)
   - Write integration test that writes via nexora-eventlog
   - Create RisingWave sink to same table
   - Verify Arrow schemas match

3. **End-to-End Pipeline** (4 hours)
   - Implement `scripts/test-risingwave-iceberg-pipeline.sh`
   - Set up Kafka → RisingWave → Iceberg flow
   - Verify events reach nexora-core graph

4. **GraphStreaming Implementation** (1-2 days)
   - Design `GraphStreaming` trait
   - Implement EventLog → Graph projection
   - Handle CDC (insert/update/delete)

---

**Last Updated**: 2026-07-30  
**Status**: Phase 4 scope clarified, implementation pending  
**Key Finding**: RisingWave's built-in Iceberg sink eliminates need for custom EventLogSink
