# Phase 4 Architecture Decision: RisingWave Native REST Catalog Support

## Decision

**Selected: Use RisingWave's Native REST Catalog Client (Built-in)**

Date: 2026-07-30  
Status: ✅ Revised - RisingWave has built-in REST catalog support

## Key Finding

**RisingWave has native Iceberg REST catalog support via `iceberg_catalog_rest::RestCatalogBuilder`.**

Investigation confirmed:
1. RisingWave connector includes `iceberg_catalog_rest` module
2. Supports `catalog.type = 'rest'` with `catalog.uri` configuration
3. RisingWave manages Iceberg metadata internally (iceberg_tables DB table)
4. RisingWave can connect to any standard Iceberg REST catalog
5. RisingWave provides `HostedIcebergCatalogService` (gRPC, port 5690) for internal queries

**User Decision**: Use RisingWave's native REST catalog capabilities instead of deploying external Lakekeeper.

## Rationale

RisingWave already has everything needed for Phase 4 integration:

### 1. Built-in REST Catalog Client

```rust
// vendor/risingwave/src/connector/src/connector_common/iceberg/mod.rs
CatalogBuildPlan::NativeRest(iceberg_configs) => {
    let catalog = iceberg_catalog_rest::RestCatalogBuilder::default()
        .load("rest", iceberg_configs)
        .await?;
    Ok(Arc::new(catalog))
}
```

**Configuration in RisingWave**:
```sql
CREATE CONNECTION nexora_catalog_conn WITH (
    type = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://nexora-app:8080/api/iceberg/catalog',
    warehouse.path = 'nexora-warehouse',
    s3.endpoint = 'http://localhost:9000',
    s3.region = 'us-east-1',
    s3.access.key = 'minioadmin',
    s3.secret.key = 'minioadmin',
    s3.path.style.access = 'true'
);
```

### 2. Internal Metadata Management

- `iceberg_tables` database table tracks all Iceberg sinks
- Automatic compaction and snapshot expiration
- `HostedIcebergCatalogService` gRPC API (port 5690) for internal queries

### 3. Simplified Architecture

**Before (External Lakekeeper)**:
```
6 services: Kafka + MinIO + PostgreSQL + Lakekeeper + RisingWave + nexora-app
```

**After (RisingWave Native)**:
```
3 services: Kafka + MinIO + nexora-app (with embedded RisingWave)
```

No need for:
- ❌ PostgreSQL (Lakekeeper backend)
- ❌ Lakekeeper service
- ❌ Additional configuration complexity

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│          RisingWave Native REST Catalog Architecture            │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  nexora-app (port 8080)                                          │
│  ├─ Embedded RisingWave Library                                  │
│  │  ├─ Meta (catalog metadata in SQLite/etcd)                   │
│  │  ├─ Frontend (SQL interface, port 4566)                      │
│  │  └─ Compute (stream processing)                              │
│  │                                                                │
│  └─ Iceberg REST Catalog HTTP Endpoints (NEW)                   │
│     ├─ GET  /api/iceberg/catalog/v1/config                      │
│     ├─ GET  /api/iceberg/catalog/v1/namespaces                  │
│     ├─ POST /api/iceberg/catalog/v1/namespaces/{ns}/tables      │
│     └─ ... (full Iceberg REST spec)                             │
│                                                                   │
├─────────────────────────────────────────────────────────────────┤
│                    Data Flow                                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Path A: nexora-eventlog                                         │
│  ├─ Write events via Iceberg Rust SDK                           │
│  ├─ Connect to nexora-app REST catalog (same process!)          │
│  └─ Write data files to S3                                       │
│                                                                   │
│  Path B: RisingWave Sink                                         │
│  ├─ Materialized View → CREATE SINK                             │
│  ├─ Connect to nexora-app REST catalog (internal call)          │
│  └─ Write data files to S3                                       │
│                                                                   │
│  Both paths share:                                                │
│  ├─ Same catalog metadata (RisingWave Meta SQLite/etcd)         │
│  ├─ Same S3 bucket (s3://nexora-events/)                        │
│  └─ Same Iceberg table namespace                                │
│                                                                   │
│  RisingWave Internal Operations:                                 │
│  ├─ iceberg_tables DB table (tracks all sinks)                  │
│  ├─ Automatic compaction (via Meta scheduler)                   │
│  ├─ Snapshot cleanup (via Meta maintenance)                     │
│  └─ HostedIcebergCatalogService (gRPC, port 5690, internal)     │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Benefits

### 1. Zero Additional Services

**No external dependencies**:
- ✅ No PostgreSQL database for catalog metadata
- ✅ No Lakekeeper service to deploy and maintain
- ✅ Everything runs in nexora-app process (with embedded RisingWave)

### 2. Unified Metadata Management

- ✅ Single source of truth: RisingWave Meta's `iceberg_tables` table
- ✅ ACID transactions via Meta's existing SQLite/etcd backend
- ✅ Built-in compaction and maintenance
- ✅ No metadata synchronization issues

### 3. Performance Benefits

**Catalog operations are in-process**:
- ✅ No network latency for catalog calls from RisingWave sink
- ✅ nexora-eventlog connects to localhost:8080 (same machine)
- ✅ Typical catalog latency: <1ms (in-process) vs ~10ms (external service)

### 4. Standard Iceberg Ecosystem Compatibility

**Full REST Specification Support via nexora-app endpoints**:
- ✅ Compatible with all Iceberg clients (Spark, Trino, DuckDB, Flink)
- ✅ OAuth2/JWT authentication via nexora-app (future)
- ✅ Multi-tenancy via namespace isolation
- ✅ Standard Iceberg REST catalog v1 spec

### 5. Simplified Operations

- ✅ Single binary to deploy (nexora-app with embedded RisingWave)
- ✅ Single configuration file (nexora.toml)
- ✅ Fewer moving parts to monitor
- ✅ Easier development and testing

## Implementation Approach

### Option 1: Bridge RisingWave Meta to HTTP REST (Recommended)

Expose RisingWave's `iceberg_tables` metadata via standard Iceberg REST HTTP endpoints in nexora-app.

**Implementation**:
```rust
// crates/nexora-app/src/handlers/iceberg_catalog.rs

/// GET /api/iceberg/catalog/v1/config
pub async fn get_catalog_config(
    State(state): State<Arc<AppState>>,
) -> Result<Json<IcebergCatalogConfig>, ApiError> {
    // Return standard Iceberg REST catalog config
    Ok(Json(IcebergCatalogConfig {
        overrides: HashMap::new(),
        defaults: HashMap::new(),
    }))
}

/// GET /api/iceberg/catalog/v1/namespaces
pub async fn list_namespaces(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ListNamespacesResponse>, ApiError> {
    // Query RisingWave Meta's iceberg_tables for unique namespaces
    let risingwave = state.risingwave.as_ref()
        .ok_or(ApiError::FeatureNotEnabled("event-streaming"))?;
    
    let tables = risingwave.list_iceberg_tables().await?;
    let namespaces: HashSet<_> = tables.iter()
        .map(|t| t.table_namespace.clone())
        .collect();
    
    Ok(Json(ListNamespacesResponse {
        namespaces: namespaces.into_iter().collect(),
    }))
}

/// GET /api/iceberg/catalog/v1/namespaces/{namespace}/tables/{table}
pub async fn load_table(
    State(state): State<Arc<AppState>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<Json<LoadTableResponse>, ApiError> {
    let risingwave = state.risingwave.as_ref()
        .ok_or(ApiError::FeatureNotEnabled("event-streaming"))?;
    
    let iceberg_table = risingwave
        .get_iceberg_table(&namespace, &table)
        .await?;
    
    Ok(Json(LoadTableResponse {
        metadata_location: iceberg_table.metadata_location,
        metadata: iceberg_table.metadata,
        config: HashMap::new(),
    }))
}
```

**Advantages**:
- ✅ Leverages RisingWave's existing metadata management
- ✅ No duplication of catalog state
- ✅ Minimal code (~500 lines for REST endpoint adapters)
- ✅ RisingWave handles compaction/maintenance automatically

### Option 2: Implement Full REST Catalog in nexora-eventlog

Build a standalone Iceberg REST catalog in nexora-eventlog, separate from RisingWave.

**Disadvantages**:
- ❌ Duplicate metadata storage (RisingWave + nexora-eventlog)
- ❌ Synchronization complexity
- ❌ More code to maintain (~2000+ lines)
- ❌ Need to implement compaction separately

**Verdict**: Option 1 is strongly preferred.

## Configuration

### docker-compose.yml (Development Setup)

```yaml
version: '3.8'

services:
  # MinIO (S3-compatible storage)
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio_data:/data

  # Kafka
  kafka:
    image: apache/kafka:latest
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"

  # Nexora (with embedded RisingWave)
  nexora:
    build: .
    ports:
      - "8080:8080"  # HTTP API + Iceberg REST catalog
      - "4566:4566"  # RisingWave Frontend (psql)
    environment:
      RUST_LOG: info
    volumes:
      - ./nexora.toml:/app/nexora.toml
      - nexora_data:/data
    depends_on:
      - minio
      - kafka

volumes:
  minio_data:
  nexora_data:
```

**Note**: Only 3 services needed (Kafka + MinIO + nexora-app).

### nexora.toml (Updated)

```toml
[server]
host = "0.0.0.0"
port = 8080

[storage]
backend = "rocksdb"
data_dir = "/data/nexora/graph"

[event_store]
backend = "rest"
# Point to nexora-app's own Iceberg REST catalog endpoints
rest_uri = "http://localhost:8080/api/iceberg/catalog"
rest_warehouse = "nexora"
s3_endpoint = "http://localhost:9000"
s3_bucket = "nexora-events"
s3_region = "us-east-1"
s3_access_key = "minioadmin"
s3_secret_key = "minioadmin"
s3_path_style = true

[event_streaming]
enabled = true

[event_streaming.library]
enabled = true
data_dir = "/data/nexora/risingwave"

[event_streaming.library.meta]
listen_addr = "0.0.0.0:5690"
backend = "sqlite"
sqlite_path = "/data/nexora/risingwave/meta.db"

[event_streaming.library.frontend]
listen_addr = "0.0.0.0:4566"

[event_streaming.library.compute]
listen_addr = "0.0.0.0:5688"
parallelism = 8
```

### RisingWave Sink DDL

```sql
-- Connect to RisingWave
psql -h localhost -p 4566 -U root -d dev

-- Create Iceberg connection pointing to nexora-app REST catalog
CREATE CONNECTION nexora_catalog_conn WITH (
    type = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://localhost:8080/api/iceberg/catalog',
    warehouse.path = 'nexora-warehouse',
    s3.endpoint = 'http://localhost:9000',
    s3.region = 'us-east-1',
    s3.access.key = 'minioadmin',
    s3.secret.key = 'minioadmin',
    s3.path_style.access = 'true'
);

-- Create sink writing to nexora-app catalog
CREATE SINK nexora_events_sink
FROM enriched_cargo_events
WITH (
    connector = 'iceberg',
    connection_name = 'nexora_catalog_conn',
    database.name = 'nexora_db',
    table.name = 'events',
    type = 'append-only',
    force_append_only = 'true',
    create_table_if_not_exists = 'true'
);
```

## Phase 4 Implementation Plan (Revised)

### Task 1: Implement Iceberg REST Catalog HTTP Endpoints (3 hours)

**Goal**: Add standard Iceberg REST catalog v1 endpoints to nexora-app

**Steps**:
1. Create `crates/nexora-app/src/handlers/iceberg_catalog.rs`:
   - `GET /api/iceberg/catalog/v1/config`
   - `GET /api/iceberg/catalog/v1/namespaces`
   - `POST /api/iceberg/catalog/v1/namespaces`
   - `GET /api/iceberg/catalog/v1/namespaces/{namespace}/tables`
   - `GET /api/iceberg/catalog/v1/namespaces/{namespace}/tables/{table}`
   - `POST /api/iceberg/catalog/v1/namespaces/{namespace}/tables`

2. Bridge to RisingWave's HostedIcebergCatalogService (gRPC):
   ```rust
   // Query RisingWave Meta's iceberg_tables
   let tables = risingwave
       .hosted_iceberg_catalog_client()
       .list_iceberg_tables(ListIcebergTablesRequest {})
       .await?;
   ```

3. Map gRPC responses to Iceberg REST JSON format

4. Add to nexora-app router:
   ```rust
   .nest("/api/iceberg/catalog", iceberg_catalog_routes())
   ```

**Deliverables**:
- `crates/nexora-app/src/handlers/iceberg_catalog.rs` (~500 lines)
- HTTP endpoints returning Iceberg REST v1 JSON
- Integration test verifying endpoints

**Acceptance Criteria**:
- ✅ `curl http://localhost:8080/api/iceberg/catalog/v1/config` returns valid JSON
- ✅ Endpoints match Iceberg REST catalog specification
- ✅ RisingWave sink can connect via `catalog.uri = 'http://localhost:8080/api/iceberg/catalog'`

### Task 2: Configure nexora-eventlog to Use Local REST Catalog (2 hours)

**Goal**: Point nexora-eventlog to nexora-app's REST catalog endpoints

**Steps**:
1. Update nexora.toml:
   ```toml
   [event_store]
   backend = "rest"
   rest_uri = "http://localhost:8080/api/iceberg/catalog"
   ```

2. Write integration test:
   ```rust
   // crates/nexora-eventlog/tests/local_catalog_integration.rs
   #[tokio::test]
   async fn test_local_rest_catalog_write() {
       // Start nexora-app with event-streaming feature
       // Create EventLogStore with REST backend pointing to localhost:8080
       // Write test events
       // Verify via catalog HTTP API
   }
   ```

3. Verify table creation in RisingWave Meta:
   ```bash
   psql -h localhost -p 4566 -U root -d dev \
     -c "SELECT * FROM rw_catalog.rw_iceberg_tables;"
   ```

**Acceptance Criteria**:
- ✅ EventLogStore can create tables via nexora-app REST catalog
- ✅ Events written to S3
- ✅ Metadata visible in RisingWave Meta's iceberg_tables
- ✅ Integration test passes

### Task 3: Configure RisingWave Sink to Use Local Catalog (1 hour)

**Goal**: Test RisingWave sink writing to same catalog as nexora-eventlog

**Steps**:
1. Start nexora with library features:
   ```bash
   cargo run --features event-first,event-streaming,library
   ```

2. Connect via psql and create connection:
   ```sql
   CREATE CONNECTION nexora_catalog_conn WITH (
       type = 'iceberg',
       catalog.type = 'rest',
       catalog.uri = 'http://localhost:8080/api/iceberg/catalog',
       warehouse.path = 'nexora-warehouse',
       s3.endpoint = 'http://localhost:9000',
       s3.region = 'us-east-1',
       s3.access.key = 'minioadmin',
       s3.secret.key = 'minioadmin',
       s3.path_style.access = 'true'
   );
   ```

3. Create test source and sink:
   ```sql
   CREATE SOURCE test_events WITH (
       connector = 'kafka',
       topic = 'test',
       properties.bootstrap.server = 'localhost:9092'
   ) FORMAT PLAIN ENCODE JSON;
   
   CREATE MATERIALIZED VIEW test_mv AS SELECT * FROM test_events;
   
   CREATE SINK test_sink FROM test_mv WITH (
       connector = 'iceberg',
       connection_name = 'nexora_catalog_conn',
       database.name = 'nexora_db',
       table.name = 'events',
       type = 'append-only',
       create_table_if_not_exists = 'true'
   );
   ```

**Acceptance Criteria**:
- ✅ Connection created successfully
- ✅ Sink created without errors
- ✅ SHOW SINKS lists test_sink
- ✅ No errors in RisingWave logs

### Task 4: End-to-End Pipeline Test (3 hours)

**Goal**: Kafka → RisingWave → nexora-app catalog → nexora-eventlog → Graph

**Steps**:
1. Create test script `scripts/test-phase4-pipeline.sh`:
   ```bash
   #!/bin/bash
   
   # 1. Start all services
   docker-compose up -d
   cargo run --features event-first,event-streaming,library &
   
   # 2. Create RisingWave source + MV + sink
   psql -h localhost -p 4566 < setup_pipeline.sql
   
   # 3. Send test events to Kafka
   echo '{"cargo_id": "C001", "status": "shipped"}' | \
     kafka-console-producer --topic test --broker-list localhost:9092
   
   # 4. Wait for processing
   sleep 10
   
   # 5. Verify in catalog
   curl http://localhost:8080/api/iceberg/catalog/v1/namespaces/nexora_db/tables/events
   
   # 6. Verify in nexora
   curl http://localhost:8080/api/events/query
   
   # 7. Verify in graph
   curl -X POST http://localhost:8080/api/query/cypher \
     -d '{"query": "MATCH (c:Cargo) WHERE c.id = \"C001\" RETURN c"}'
   ```

2. Run full pipeline test
3. Measure latency (Kafka → Graph)

**Acceptance Criteria**:
- ✅ Events flow from Kafka to Graph
- ✅ Data visible in catalog HTTP API
- ✅ RisingWave sink writing successfully
- ✅ nexora-eventlog reading from same catalog
- ✅ Graph nodes created correctly
- ✅ End-to-end latency <10 seconds

### Task 5: Documentation (1 hour)

**Goal**: Document simplified architecture and configuration

**Steps**:
1. Update RISINGWAVE_ICEBERG_INTEGRATION.md
2. Update DISTRIBUTED_DEPLOYMENT.md
3. Add troubleshooting section

**Deliverables**:
- Updated architecture diagrams
- Configuration examples
- API endpoint reference
- Troubleshooting guide

## Success Criteria

### Phase 4 Complete When:

- [ ] Task 1: Iceberg REST catalog HTTP endpoints implemented in nexora-app
- [ ] Task 2: nexora-eventlog integration test passes with local catalog
- [ ] Task 3: RisingWave sink configured and tested with local catalog
- [ ] Task 4: End-to-end pipeline test passes (Kafka → Graph)
- [ ] Task 5: Documentation updated

### Performance Targets:

- Event ingestion: >10k events/sec
- Sink latency: <5 seconds (Kafka to Iceberg)
- Query latency: <100ms (graph queries)
- Catalog operations: <1ms (in-process via HTTP to localhost)

### Architecture Validation:

- ✅ Zero external catalog services (no Lakekeeper, no PostgreSQL)
- ✅ Single unified metadata store (RisingWave Meta)
- ✅ Standard Iceberg REST compatibility
- ✅ All existing tests still pass

## Timeline

| Task | Estimated Time | Status |
|------|---------------|--------|
| 1. Implement REST catalog HTTP endpoints | 3 hours | ⏳ Pending |
| 2. Configure nexora-eventlog | 2 hours | ⏳ Pending |
| 3. Configure RisingWave sink | 1 hour | ⏳ Pending |
| 4. End-to-end pipeline test | 3 hours | ⏳ Pending |
| 5. Documentation | 1 hour | ⏳ Pending |
| **Total** | **10 hours (~1.5 days)** | **Phase 4** |

**Reduced from original estimate**: 15.5 hours → 10 hours (no external services to deploy)

## Next Steps

**Immediate**: Start Task 1 - Implement Iceberg REST catalog HTTP endpoints

1. Create handler file:
   ```bash
   touch crates/nexora-app/src/handlers/iceberg_catalog.rs
   ```

2. Add module to `crates/nexora-app/src/handlers/mod.rs`:
   ```rust
   #[cfg(feature = "event-streaming")]
   pub mod iceberg_catalog;
   ```

3. Implement core endpoints:
   - Start with GET /v1/config (simplest)
   - Then GET /v1/namespaces (query RisingWave Meta)
   - Then table operations

4. Wire into nexora-app router:
   ```rust
   #[cfg(feature = "event-streaming")]
   app = app.nest("/api/iceberg/catalog", iceberg_catalog::routes());
   ```

---

**Document Version**: 3.0 (Revised - RisingWave Native)  
**Date**: 2026-07-30  
**Decision Owner**: Nexora Development Team  
**Approved**: Yes (Native RisingWave REST catalog support)
