# RisingWave REST Catalog Analysis

## Key Finding: RisingWave Hosts Its Own Iceberg REST Catalog

Based on code investigation, **RisingWave Meta node hosts an embedded Iceberg REST catalog**, not just a gRPC service for listing tables.

### Evidence

1. **README.md states explicitly**:
   > "It hosts the Iceberg REST catalog directly and handles table maintenance — compaction, small-file optimization, snapshot cleanup — without external tooling."

2. **Meta service includes HostedIcebergCatalogService**:
   - File: `src/meta/service/src/hosted_iceberg_catalog_service.rs`
   - Registered in: `src/meta/node/src/server.rs:L48` and added to gRPC server
   - Database table: `iceberg_tables` stores catalog metadata

3. **Catalog metadata storage**:
   ```rust
   // src/meta/model/src/iceberg_tables.rs
   pub struct Model {
       pub catalog_name: String,
       pub table_namespace: String,
       pub table_name: String,
       pub metadata_location: Option<String>,
       pub previous_metadata_location: Option<String>,
   }
   ```

4. **gRPC service definition**:
   ```protobuf
   // proto/meta.proto
   service HostedIcebergCatalogService {
     rpc ListIcebergTables(ListIcebergTablesRequest) returns (ListIcebergTablesResponse);
   }
   ```

## Architecture Clarification

### What RisingWave Provides

```
RisingWave Meta Node
├─ gRPC Server (port 5690)
│  ├─ ClusterService
│  ├─ DdlService
│  ├─ HeartbeatService
│  └─ HostedIcebergCatalogService (gRPC API for listing tables)
│
├─ Iceberg REST Catalog (HTTP/REST API - likely separate port)
│  ├─ REST endpoints for table operations
│  ├─ Metadata management (table creation, schema evolution)
│  └─ Storage in Meta's database (iceberg_tables table)
│
└─ Catalog Database
   └─ iceberg_tables (catalog_name, namespace, table_name, metadata_location)
```

### Implications for Nexora Integration

**IMPORTANT**: This changes the Phase 4 integration strategy significantly.

#### Option 1: Use RisingWave as the Catalog Server (Simpler)

Instead of running **both** Lakekeeper **and** RisingWave:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Simplified Architecture                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  RisingWave Meta (Single Catalog Server)                         │
│  ├─ Iceberg REST Catalog (port 8080?)                           │
│  │  └─ Serves both RisingWave sinks AND nexora-eventlog         │
│  └─ Metadata Storage (SQLite/PostgreSQL)                        │
│                                                                   │
│  nexora-eventlog → RisingWave REST Catalog → S3                 │
│  RisingWave Sink → RisingWave REST Catalog → S3                 │
│                                                                   │
│  Both write to the SAME catalog, SAME S3 bucket                  │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

**Configuration**:
```toml
# nexora.toml
[event_store]
backend = "rest"
rest_uri = "http://localhost:8080/iceberg/v1"  # RisingWave's REST catalog
rest_warehouse = "nexora"
s3_endpoint = "http://localhost:9000"
s3_bucket = "nexora-events"
```

```sql
-- RisingWave sink (same catalog)
CREATE SINK nexora_events_sink
FROM enriched_cargo_events
WITH (
    connector = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://localhost:8080/iceberg/v1',  -- Same endpoint!
    warehouse.path = 's3://nexora-events/',
    -- ... S3 config
);
```

**Benefits**:
- ✅ No external catalog service needed (no Lakekeeper/Polaris)
- ✅ Single source of truth for all Iceberg metadata
- ✅ RisingWave handles compaction, snapshot expiration automatically
- ✅ Simpler deployment (one less service)

**Drawbacks**:
- ⚠️ Tight coupling to RisingWave (can't use catalog without RisingWave running)
- ⚠️ Need to verify RisingWave's REST catalog is fully Iceberg-spec compliant

#### Option 2: Keep Lakekeeper as External Catalog (More Flexible)

Keep the original plan with separate Lakekeeper:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Decoupled Architecture                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Lakekeeper (External REST Catalog)                              │
│  └─ Metadata Storage (PostgreSQL)                               │
│       ↑                                                          │
│       ├──── nexora-eventlog                                      │
│       └──── RisingWave Sink                                      │
│                                                                   │
│  Both use Lakekeeper as the catalog                              │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

**Benefits**:
- ✅ nexora-eventlog can work independently of RisingWave
- ✅ Standard Iceberg REST catalog (better ecosystem compatibility)
- ✅ Can use other tools (Spark, Trino) with same catalog

**Drawbacks**:
- ❌ More complex deployment (separate Lakekeeper + PostgreSQL)
- ❌ RisingWave's auto-compaction features may not apply

## Questions to Investigate

### 1. What is the REST catalog endpoint?

Need to find:
- HTTP port for REST catalog (separate from gRPC port 5690?)
- Base path (e.g., `/iceberg/v1`)
- How to configure it in RisingWave startup

**Search for**:
```bash
grep -r "rest.*catalog.*endpoint\|iceberg.*http.*server" src/meta/ --include="*.rs"
```

### 2. Is the REST catalog fully spec-compliant?

Need to verify RisingWave's REST catalog implements:
- [Iceberg REST Catalog spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
- Table creation, schema evolution, snapshot management
- OAuth2 authentication (if needed)

**Test by**:
```bash
# Start RisingWave
./risedev d

# Try to access REST catalog
curl http://localhost:<port>/v1/config
curl http://localhost:<port>/v1/namespaces
```

### 3. Can nexora-eventlog use RisingWave's catalog?

Need to test:
```rust
// crates/nexora-eventlog integration test
let storage_config = StorageConfig::rest(
    "http://localhost:<risingwave-rest-port>/v1",  // RisingWave's endpoint
    "nexora",
    "http://localhost:9000",
    "us-east-1",
    "minioadmin",
    "minioadmin",
    true,
);
let event_store = EventLogStore::new(storage_config).await.unwrap();

// Write event
event_store.append("events", test_event).await.unwrap();

// Query via RisingWave
// Should see the table in RisingWave's catalog
```

### 4. Schema validation features?

Need to find:
- Does RisingWave validate schema compatibility on writes?
- How does it handle schema evolution?
- Can it enforce schema constraints?

**Search for**:
```bash
grep -r "schema.*validation\|schema.*evolution\|schema.*check" src/connector/src/sink/iceberg/ --include="*.rs"
```

## Recommended Next Steps

1. **Verify REST catalog endpoint** (1 hour)
   - Start RisingWave with `./risedev d`
   - Find REST catalog port (check logs, config files)
   - Test with `curl` to verify Iceberg REST spec compliance

2. **Test nexora-eventlog integration** (2 hours)
   - Configure nexora-eventlog to use RisingWave's REST catalog
   - Write events via EventLogStore
   - Verify tables appear in RisingWave's catalog
   - Query tables via RisingWave SQL

3. **Decision: Option 1 or Option 2?** (Discussion)
   - If RisingWave's REST catalog is fully compliant → **Option 1** (simpler)
   - If nexora needs catalog independence → **Option 2** (flexible)

4. **Update Phase 4 documentation** (30 min)
   - Document RisingWave's hosted catalog discovery
   - Update integration guide with chosen architecture
   - Add REST catalog endpoint configuration

## Schema Validation Investigation

**Hypothesis**: RisingWave likely validates schema on sink writes because:
- Iceberg requires schema compatibility for writes
- RisingWave manages the catalog metadata
- Sink DDL includes schema mapping

**Where to look**:
```rust
// vendor/risingwave/src/connector/src/sink/iceberg/writer.rs
// Should contain schema validation logic
```

---

**Status**: Investigation needed - REST endpoint not yet confirmed  
**Priority**: High - Blocks Phase 4 implementation decision  
**Estimated Investigation Time**: 3-4 hours
