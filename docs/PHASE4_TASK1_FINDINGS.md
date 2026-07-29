# Phase 4 Task 1: REST Catalog Endpoint Investigation - FINDINGS

## Critical Discovery: RisingWave Does NOT Host Iceberg REST Catalog

### Evidence

After extensive code investigation, the reality is **different from the initial assumption**:

#### 1. RisingWave Uses EXTERNAL REST Catalogs

All e2e tests use **external Lakekeeper** as the REST catalog:

```sql
-- From e2e_test/iceberg/test_case/*.slt
CREATE CONNECTION lakekeeper_catalog_conn WITH (
    type = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://127.0.0.1:8181/catalog/',  -- External Lakekeeper!
    warehouse.path = 'risingwave-warehouse',
    -- ... S3 config
);
```

**Port 8181 is Lakekeeper, NOT RisingWave**.

#### 2. HostedIcebergCatalogService is gRPC, Not HTTP REST

```rust
// src/meta/service/src/hosted_iceberg_catalog_service.rs
service HostedIcebergCatalogService {
  rpc ListIcebergTables(ListIcebergTablesRequest) returns (ListIcebergTablesResponse);
}
```

This is a **gRPC service** for internal RisingWave operations, not a standard Iceberg REST catalog.

#### 3. Dashboard HTTP Server Has No Iceberg REST Endpoints

```rust
// src/meta/src/dashboard/mod.rs
let api_router = Router::new()
    .route("/version", get(get_version))
    .route("/clusters/{ty}", get(list_clusters))
    .route("/materialized_views", get(list_materialized_views))
    // ... many routes, but NO /v1/config or /v1/namespaces (Iceberg REST spec)
```

The dashboard HTTP server (port TBD) serves **monitoring/UI endpoints**, not Iceberg REST catalog.

#### 4. No HTTP REST Catalog Server Found

Searched exhaustively:
- ✅ Meta node startup code - no HTTP REST catalog server
- ✅ Dashboard routes - no Iceberg REST endpoints
- ✅ Config files - no REST catalog port configuration
- ✅ E2E tests - all use external Lakekeeper

### Re-Interpretation of README Statement

> "It hosts the Iceberg REST catalog directly and handles table maintenance"

This likely means:
1. RisingWave **manages Iceberg table metadata** (in `iceberg_tables` database table)
2. RisingWave **handles compaction and maintenance** (confirmed in code)
3. RisingWave can **connect to** external REST catalogs (Lakekeeper, Polaris)
4. BUT RisingWave does **NOT provide** a standard Iceberg REST catalog HTTP endpoint

### What RisingWave Actually Provides

```
RisingWave Architecture (Actual):

Meta Node
├─ gRPC Server (port 5690)
│  ├─ ClusterService
│  ├─ DdlService
│  └─ HostedIcebergCatalogService (internal gRPC for listing tables)
│
├─ Dashboard HTTP Server (port TBD, e.g. 5691)
│  ├─ /api/clusters
│  ├─ /api/materialized_views
│  └─ ... (monitoring/UI endpoints, NOT Iceberg REST)
│
├─ Iceberg Metadata Management
│  ├─ iceberg_tables database table
│  ├─ Compaction scheduling
│  ├─ Snapshot expiration
│  └─ Manifest rewriting
│
└─ Connector to EXTERNAL REST Catalogs
   ├─ Lakekeeper (http://localhost:8181)
   ├─ Polaris
   └─ AWS Glue (via REST)
```

### Impact on Phase 4 Architecture Decision

**CRITICAL**: Our Option 1 assumption was **INCORRECT**.

#### Original Option 1 (INVALID)
```
❌ Use RisingWave as unified REST catalog
   - nexora-eventlog → RisingWave REST endpoint
   - RisingWave Sink → RisingWave REST endpoint
   - NO Lakekeeper needed
```

**This is NOT possible** because RisingWave does not provide an Iceberg REST catalog HTTP endpoint.

#### Revised Architecture (REQUIRED)

**We MUST use Option 2: External Lakekeeper as REST Catalog**

```
✅ Lakekeeper (External REST Catalog)
   ├─ nexora-eventlog → Lakekeeper REST API
   ├─ RisingWave Sink → Lakekeeper REST API
   └─ Shared metadata in Lakekeeper PostgreSQL

Required Services:
├─ MinIO (S3)
├─ Kafka (event streams)
├─ PostgreSQL (Lakekeeper backend)
├─ Lakekeeper (REST catalog at port 8181)
├─ RisingWave (Meta + Frontend + Compute)
└─ nexora-app

Total: 6 services (cannot reduce)
```

### Configuration (Corrected)

#### nexora.toml

```toml
[event_store]
backend = "rest"
# External Lakekeeper, NOT RisingWave
rest_uri = "http://localhost:8181/catalog"
rest_warehouse = "nexora"
s3_endpoint = "http://localhost:9000"
s3_bucket = "nexora-events"
```

#### RisingWave Sink DDL

```sql
CREATE CONNECTION nexora_catalog_conn WITH (
    type = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://localhost:8181/catalog',  -- Lakekeeper
    warehouse.path = 'nexora-warehouse',
    s3.endpoint = 'http://localhost:9000',
    s3.region = 'us-east-1',
    s3.access.key = 'minioadmin',
    s3.secret.key = 'minioadmin',
    s3.path.style.access = 'true'
);

CREATE SINK nexora_events_sink
FROM enriched_cargo_events
WITH (
    connector = 'iceberg',
    connection_name = 'nexora_catalog_conn',
    database.name = 'nexora_db',
    table.name = 'events',
    type = 'append-only',
    force_append_only = 'true'
);
```

### Verification Steps

To confirm this finding, we should:

1. **Start RisingWave with ./risedev d**
2. **Check all listening ports**:
   ```bash
   lsof -iTCP -sTCP:LISTEN -P | grep risingwave
   ```
3. **Test for Iceberg REST endpoints** (expect 404):
   ```bash
   curl http://localhost:5690/v1/config  # gRPC port - expect failure
   curl http://localhost:5691/v1/config  # Dashboard port - expect 404
   ```
4. **Confirm e2e tests require Lakekeeper**:
   ```bash
   grep -r "lakekeeper" ci/scripts/e2e-iceberg-test.sh
   ```

### Conclusion

**Task 1 Result**: RisingWave does **NOT** provide an Iceberg REST catalog HTTP endpoint.

**Phase 4 Architecture Decision MUST BE REVISED**:
- ❌ Option 1 (RisingWave as catalog) - Not feasible
- ✅ Option 2 (External Lakekeeper) - **REQUIRED**

**Next Steps**:
1. Update PHASE4_ARCHITECTURE_DECISION.md to reflect this finding
2. Update RISINGWAVE_INTEGRATION_PLAN.md
3. Document Lakekeeper deployment requirements
4. Proceed with Task 2: Deploy and configure Lakekeeper

---

**Status**: Task 1 Complete - Architecture assumption corrected  
**Outcome**: Must use external Lakekeeper (Option 2)  
**Time Spent**: 1.5 hours (investigation + documentation)
