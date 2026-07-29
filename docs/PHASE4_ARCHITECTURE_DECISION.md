# Phase 4 Architecture Decision: Use RisingWave as Unified Catalog

## Decision

**Selected: Option 1 - Use RisingWave as Unified Iceberg REST Catalog**

Date: 2026-07-30  
Status: ✅ Approved

## Rationale

RisingWave hosts its own Iceberg REST catalog, eliminating the need for external catalog services like Lakekeeper or Polaris. Using RisingWave as the unified catalog simplifies the architecture significantly.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Unified Catalog Architecture                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  RisingWave Meta Node (Single Catalog Server)                    │
│  ├─ gRPC Server (port 5690)                                      │
│  │  ├─ ClusterService                                            │
│  │  ├─ DdlService                                                │
│  │  └─ HostedIcebergCatalogService                              │
│  │                                                                │
│  ├─ Iceberg REST Catalog (HTTP port TBD)                        │
│  │  ├─ Table creation/deletion                                   │
│  │  ├─ Schema evolution                                          │
│  │  ├─ Metadata management                                       │
│  │  └─ Token/auth support                                        │
│  │                                                                │
│  └─ Catalog Database (SQLite/PostgreSQL)                        │
│     └─ iceberg_tables (metadata storage)                        │
│                                                                   │
├─────────────────────────────────────────────────────────────────┤
│                    Data Flow                                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Path A: nexora-eventlog                                         │
│  ├─ Write events via Iceberg Rust SDK                           │
│  ├─ Connect to RisingWave REST Catalog                          │
│  └─ Write data files to S3                                       │
│                                                                   │
│  Path B: RisingWave Sink                                         │
│  ├─ Materialized View → CREATE SINK                             │
│  ├─ Connect to RisingWave REST Catalog (internal)               │
│  └─ Write data files to S3                                       │
│                                                                   │
│  Both paths share:                                                │
│  ├─ Same catalog metadata (RisingWave Meta)                     │
│  ├─ Same S3 bucket (s3://nexora-events/)                        │
│  └─ Same Iceberg table namespace                                │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Benefits

### 1. Simplified Deployment

**Before** (with external Lakekeeper):
```
Services Required:
├─ MinIO (S3)
├─ Kafka (event streams)
├─ PostgreSQL (Lakekeeper backend)
├─ Lakekeeper (REST catalog)
├─ RisingWave (Meta + Frontend + Compute)
└─ nexora-app

Total: 6 services
```

**After** (unified catalog):
```
Services Required:
├─ MinIO (S3)
├─ Kafka (event streams)
├─ RisingWave (Meta + Frontend + Compute + REST Catalog)
└─ nexora-app

Total: 4 services (-33%)
```

### 2. Single Source of Truth

- All Iceberg metadata in one place (RisingWave Meta database)
- No catalog synchronization issues
- Consistent schema versions across all writers
- Simplified monitoring and debugging

### 3. Automatic Maintenance

RisingWave handles:
- ✅ **Compaction** - Small file merging
- ✅ **Snapshot expiration** - Old snapshot cleanup
- ✅ **Manifest rewriting** - Metadata optimization
- ✅ **Orphan file cleanup** - Unreferenced data removal

No need for external cron jobs or manual maintenance.

### 4. Better Integration

- nexora-eventlog and RisingWave sink share the same catalog
- RisingWave can query tables created by nexora-eventlog
- Unified monitoring and metrics
- Consistent S3 credential management

## Drawbacks and Mitigations

### Drawback 1: Tight Coupling

**Issue**: nexora-eventlog requires RisingWave to be running

**Mitigation**:
- In library mode, RisingWave starts with nexora-app (no separate process)
- If RisingWave crashes, nexora-app can fall back to local SQLite catalog
- Document the dependency clearly in deployment guides

### Drawback 2: REST Catalog Spec Compliance Unknown

**Issue**: Need to verify RisingWave's REST catalog is fully Iceberg-compliant

**Mitigation**:
- Phase 4 Task 1: Test REST catalog endpoints with curl
- Phase 4 Task 2: Validate against Iceberg REST spec
- If non-compliant, contribute fixes to RisingWave upstream

### Drawback 3: Limited External Tool Access

**Issue**: External tools (Spark, Trino) must connect to RisingWave's catalog

**Mitigation**:
- RisingWave README confirms external tool support
- Document REST catalog endpoint for external connections
- Test with DuckDB/Trino before production

## Configuration

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
# RisingWave's REST catalog endpoint (TBD: find actual port)
rest_uri = "http://localhost:8080/iceberg/v1"
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

-- Create sink writing to RisingWave's own catalog
CREATE SINK nexora_events_sink
FROM enriched_cargo_events
WITH (
    connector = 'iceberg',
    
    -- Use RisingWave's internal catalog
    catalog.type = 'rest',
    catalog.uri = 'http://localhost:8080/iceberg/v1',
    warehouse.path = 's3://nexora-events/',
    
    -- S3 config (matches nexora.toml)
    s3.endpoint = 'http://localhost:9000',
    s3.region = 'us-east-1',
    s3.access.key = 'minioadmin',
    s3.secret.key = 'minioadmin',
    s3.path_style_access = 'true',
    
    -- Table location (same namespace as nexora-eventlog)
    database.name = 'nexora_db',
    table.name = 'events',
    
    -- Write mode
    type = 'append-only',
    force_append_only = 'true',
    create_table_if_not_exists = 'true'
);
```

## Phase 4 Implementation Plan (Revised)

### Task 1: Locate REST Catalog Endpoint (1 hour)

**Goal**: Find RisingWave's REST catalog HTTP port and base path

**Steps**:
1. Start RisingWave in library mode
2. Check logs for REST catalog startup messages
3. Inspect Meta node configuration
4. Test endpoints with curl

**Expected Output**:
```bash
# Should work:
curl http://localhost:<port>/v1/config
curl http://localhost:<port>/v1/namespaces
```

### Task 2: Validate REST Catalog Compliance (2 hours)

**Goal**: Verify RisingWave's catalog implements Iceberg REST spec

**Steps**:
1. Test all Iceberg REST endpoints
2. Compare with [Iceberg REST spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
3. Document any deviations or missing features
4. Create compatibility test suite

**Acceptance Criteria**:
- ✅ List namespaces works
- ✅ Create/drop tables works
- ✅ Schema evolution works
- ✅ Snapshot management works

### Task 3: Integrate nexora-eventlog (3 hours)

**Goal**: Configure nexora-eventlog to use RisingWave's catalog

**Steps**:
1. Update nexora.toml with RisingWave REST URI
2. Write integration test:
   ```rust
   // crates/nexora-eventlog/tests/risingwave_catalog_test.rs
   #[tokio::test]
   async fn test_risingwave_catalog_integration() {
       // Start RisingWave library
       // Configure EventLogStore with RisingWave REST catalog
       // Write events
       // Query via RisingWave SQL
       // Verify data consistency
   }
   ```
3. Test schema compatibility
4. Verify table appears in RisingWave's catalog

**Acceptance Criteria**:
- ✅ EventLogStore can write to RisingWave catalog
- ✅ Tables visible in `SHOW TABLES` via RisingWave
- ✅ Data queryable via RisingWave SQL
- ✅ Schema matches expected format

### Task 4: End-to-End Pipeline Test (4 hours)

**Goal**: Kafka → RisingWave MV → Iceberg Sink → Graph

**Steps**:
1. Create test script:
   ```bash
   #!/bin/bash
   # scripts/test-unified-catalog-pipeline.sh
   
   # Start services (Kafka, MinIO)
   docker-compose up -d
   
   # Start nexora with RisingWave
   cargo run --features event-first,event-streaming,library
   
   # Create Kafka source
   psql -h localhost -p 4566 <<EOF
   CREATE SOURCE raw_events (...) WITH (connector='kafka', ...);
   CREATE MATERIALIZED VIEW enriched_events AS SELECT ...;
   CREATE SINK nexora_sink FROM enriched_events WITH (connector='iceberg', ...);
   EOF
   
   # Send test events
   kafka-console-producer --topic test < test_events.json
   
   # Wait and verify
   sleep 10
   curl http://localhost:8080/api/events/query
   curl http://localhost:8080/api/query/cypher
   ```
2. Run end-to-end test
3. Verify data flows through all stages
4. Check graph nodes created correctly

**Acceptance Criteria**:
- ✅ Events flow from Kafka to RisingWave
- ✅ Sink writes to catalog successfully
- ✅ nexora-eventlog can read the same tables
- ✅ Graph nodes created from events

### Task 5: Documentation (2 hours)

**Goal**: Document unified catalog architecture

**Steps**:
1. Update DISTRIBUTED_DEPLOYMENT.md
2. Add REST catalog configuration examples
3. Document external tool access
4. Create troubleshooting guide

**Deliverables**:
- Updated deployment guide
- Configuration reference
- External tool integration guide

## Success Criteria

### Phase 4 Complete When:

- [x] Architecture decision documented (this file)
- [ ] REST catalog endpoint located and tested
- [ ] Iceberg REST spec compliance verified
- [ ] nexora-eventlog integration test passes
- [ ] End-to-end pipeline test passes
- [ ] Documentation updated

### Performance Targets:

- Event ingestion: >10k events/sec
- Sink latency: <5 seconds (Kafka to Iceberg)
- Query latency: <100ms (graph queries)
- Catalog operations: <1 second (table creation)

## Rollback Plan

If RisingWave's REST catalog is **not** Iceberg-compliant:

### Plan B: Fall Back to Option 2

1. Deploy Lakekeeper as external catalog
2. Configure nexora-eventlog to use Lakekeeper
3. Configure RisingWave sink to use Lakekeeper
4. Document the external catalog requirement

### Plan C: Contribute Upstream

1. Identify missing REST endpoints in RisingWave
2. Submit PR to risingwave-labs/risingwave
3. Wait for upstream fix
4. Update vendor/risingwave with patched version

## Timeline

| Task | Estimated Time | Status |
|------|---------------|--------|
| 1. Locate REST endpoint | 1 hour | ⏳ Pending |
| 2. Validate REST compliance | 2 hours | ⏳ Pending |
| 3. Integrate nexora-eventlog | 3 hours | ⏳ Pending |
| 4. End-to-end pipeline test | 4 hours | ⏳ Pending |
| 5. Documentation | 2 hours | ⏳ Pending |
| **Total** | **12 hours (~1.5 days)** | **Phase 4** |

## Next Steps

**Immediate**: Start Task 1 - Locate REST catalog endpoint

1. Build nexora with library features:
   ```bash
   cargo build --release --features event-first,event-streaming,library
   ```

2. Start with detailed logging:
   ```bash
   RUST_LOG=info,risingwave_meta=debug ./target/release/nexora --config nexora.toml
   ```

3. Search logs for:
   - "REST catalog"
   - "Iceberg"
   - "HTTP server started"
   - Port bindings

4. Test with curl:
   ```bash
   curl http://localhost:<found-port>/v1/config
   ```

---

**Document Version**: 1.0  
**Date**: 2026-07-30  
**Decision Owner**: Nexora Development Team  
**Approved**: Yes (方案 1 已选定)
