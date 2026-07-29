# Phase 4 Architecture Decision: Use External Lakekeeper as Catalog

## Decision

**Selected: Option 2 - Use External Lakekeeper as Iceberg REST Catalog**

Date: 2026-07-30  
Status: ✅ Approved (Revised after Task 1 investigation)

## Critical Finding from Task 1

**RisingWave does NOT provide an Iceberg REST catalog HTTP endpoint.**

Initial assumption based on README statement was incorrect. Investigation revealed:
1. RisingWave manages Iceberg metadata internally (iceberg_tables DB table)
2. RisingWave handles compaction and maintenance
3. RisingWave **connects to** external REST catalogs (Lakekeeper, Polaris)
4. RisingWave does **NOT host** a standard Iceberg REST catalog server

All RisingWave e2e tests use external Lakekeeper at port 8181.

**Reference**: [Task 1 Findings](PHASE4_TASK1_FINDINGS.md)

## Rationale

Since RisingWave cannot serve as a catalog, we must use an external Iceberg REST catalog service. Lakekeeper is the standard choice, providing full Iceberg REST specification compliance.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              External Lakekeeper Architecture                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Lakekeeper (Iceberg REST Catalog)                               │
│  ├─ HTTP Server (port 8181)                                      │
│  │  ├─ /v1/config                                                │
│  │  ├─ /v1/namespaces                                            │
│  │  ├─ /v1/namespaces/{namespace}/tables                        │
│  │  └─ ... (full Iceberg REST spec)                             │
│  │                                                                │
│  └─ PostgreSQL Backend                                           │
│     └─ Iceberg metadata (schemas, snapshots, manifests)         │
│                                                                   │
├─────────────────────────────────────────────────────────────────┤
│                    Data Flow                                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Path A: nexora-eventlog                                         │
│  ├─ Write events via Iceberg Rust SDK                           │
│  ├─ Connect to Lakekeeper REST Catalog                          │
│  └─ Write data files to S3                                       │
│                                                                   │
│  Path B: RisingWave Sink                                         │
│  ├─ Materialized View → CREATE SINK                             │
│  ├─ Connect to Lakekeeper REST Catalog (same as Path A)         │
│  └─ Write data files to S3                                       │
│                                                                   │
│  Both paths share:                                                │
│  ├─ Same catalog metadata (Lakekeeper PostgreSQL)               │
│  ├─ Same S3 bucket (s3://nexora-events/)                        │
│  └─ Same Iceberg table namespace                                │
│                                                                   │
│  RisingWave Internal Operations:                                 │
│  ├─ iceberg_tables DB table (tracks sinks)                      │
│  ├─ Automatic compaction (reads from Lakekeeper)                │
│  └─ Snapshot cleanup (writes via Lakekeeper)                    │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Benefits

### 1. Standard Iceberg Ecosystem Compatibility

**Full REST Specification Support**:
- ✅ Lakekeeper implements complete Iceberg REST catalog spec
- ✅ Compatible with all Iceberg clients (Spark, Trino, DuckDB, Flink)
- ✅ OAuth2/JWT authentication support
- ✅ Multi-tenancy via warehouse isolation

### 2. Independent Operation

- nexora-eventlog can run without RisingWave
- RisingWave can run without nexora-eventlog
- Both write to shared catalog independently
- No tight coupling between components

### 3. Production-Grade Catalog

**Lakekeeper provides**:
- ✅ Distributed catalog metadata (PostgreSQL backend)
- ✅ High availability (PostgreSQL clustering)
- ✅ ACID transactions for metadata updates
- ✅ Multi-writer concurrency control
- ✅ Auditing and access control

### 4. Proven Architecture

- RisingWave's own e2e tests use Lakekeeper
- Well-documented integration patterns
- Active community support
- Clear upgrade paths

## Drawbacks and Mitigations

### Drawback 1: Additional Service Dependency

**Issue**: Requires deploying and maintaining Lakekeeper + PostgreSQL

**Mitigation**:
- Lakekeeper is lightweight (single binary, minimal resources)
- PostgreSQL is already commonly deployed infrastructure
- Docker Compose simplifies local development setup
- Helm charts available for production Kubernetes deployment

### Drawback 2: More Services to Monitor

**Issue**: 6 services instead of a potentially simpler stack

**Reality Check**:
- This is the **only viable option** (RisingWave does not provide REST catalog)
- Industry-standard architecture (same as RisingWave's own testing setup)
- Each service has a clear, focused responsibility
- Standard monitoring with Prometheus/Grafana applies to all

### Drawback 3: Network Latency for Catalog Operations

**Issue**: Extra hop to Lakekeeper for metadata operations

**Mitigation**:
- Catalog operations are infrequent (table creation, schema updates)
- Data path (file reads/writes) goes directly to S3 (no catalog hop)
- Lakekeeper caches metadata for read operations
- Typical catalog latency: <10ms on local network

## Configuration

### docker-compose.yml (Development Setup)

```yaml
version: '3.8'

services:
  # PostgreSQL for Lakekeeper
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: lakekeeper
      POSTGRES_USER: lakekeeper
      POSTGRES_PASSWORD: lakekeeper
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  # Lakekeeper (Iceberg REST Catalog)
  lakekeeper:
    image: lakekeeper/lakekeeper:latest
    depends_on:
      - postgres
    environment:
      DATABASE_URL: postgresql://lakekeeper:lakekeeper@postgres:5432/lakekeeper
      RUST_LOG: info
    ports:
      - "8181:8181"

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

volumes:
  postgres_data:
  minio_data:
```

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
# Lakekeeper REST catalog
rest_uri = "http://localhost:8181/catalog"
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

-- Create Iceberg connection pointing to Lakekeeper
CREATE CONNECTION nexora_catalog_conn WITH (
    type = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://localhost:8181/catalog',
    warehouse.path = 'nexora-warehouse',
    s3.endpoint = 'http://localhost:9000',
    s3.region = 'us-east-1',
    s3.access.key = 'minioadmin',
    s3.secret.key = 'minioadmin',
    s3.path_style.access = 'true'
);

-- Create sink writing to Lakekeeper catalog
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

### Task 1: Locate REST Catalog Endpoint (COMPLETED ✅)

**Result**: Confirmed RisingWave does NOT provide REST catalog endpoint

**Finding**: Must use external Lakekeeper

**Reference**: [Task 1 Findings](PHASE4_TASK1_FINDINGS.md)

### Task 2: Deploy Lakekeeper Stack (3 hours)

**Goal**: Set up Lakekeeper + PostgreSQL + MinIO + Kafka

**Steps**:
1. Create `docker-compose.yml` (see Configuration section)
2. Start services:
   ```bash
   docker-compose up -d
   ```
3. Verify Lakekeeper is running:
   ```bash
   curl http://localhost:8181/v1/config
   ```
4. Create MinIO bucket:
   ```bash
   mc alias set local http://localhost:9000 minioadmin minioadmin
   mc mb local/nexora-events
   ```

**Acceptance Criteria**:
- ✅ All services healthy in `docker-compose ps`
- ✅ Lakekeeper responds to /v1/config
- ✅ PostgreSQL accepts connections
- ✅ MinIO bucket created

### Task 3: Integrate nexora-eventlog with Lakekeeper (3 hours)

**Goal**: Configure nexora-eventlog to use Lakekeeper catalog

**Steps**:
1. Update nexora.toml with Lakekeeper URI
2. Write integration test:
   ```rust
   // crates/nexora-eventlog/tests/lakekeeper_integration.rs
   #[tokio::test]
   async fn test_lakekeeper_catalog_write() {
       // Start docker-compose stack
       // Create EventLogStore with REST backend
       // Write test events
       // Verify via Lakekeeper API
   }
   ```
3. Run test and verify table creation in PostgreSQL:
   ```bash
   psql -h localhost -U lakekeeper -d lakekeeper \
     -c "SELECT * FROM iceberg_tables;"
   ```

**Acceptance Criteria**:
- ✅ EventLogStore can create tables in Lakekeeper
- ✅ Events written to S3
- ✅ Metadata visible in Lakekeeper PostgreSQL
- ✅ Integration test passes

### Task 4: Configure RisingWave Sink (2 hours)

**Goal**: Connect RisingWave sink to Lakekeeper

**Steps**:
1. Start nexora with library features
2. Connect via psql and create connection:
   ```sql
   CREATE CONNECTION nexora_catalog_conn WITH (...);
   ```
3. Create test source and materialized view:
   ```sql
   CREATE SOURCE test_events WITH (
       connector = 'kafka',
       topic = 'test',
       properties.bootstrap.server = 'localhost:9092'
   ) FORMAT PLAIN ENCODE JSON;
   
   CREATE MATERIALIZED VIEW enriched_events AS
   SELECT * FROM test_events WHERE value IS NOT NULL;
   ```
4. Create Iceberg sink:
   ```sql
   CREATE SINK nexora_sink FROM enriched_events WITH (...);
   ```

**Acceptance Criteria**:
- ✅ Connection created successfully
- ✅ Sink created without errors
- ✅ SHOW SINKS lists nexora_sink

### Task 5: End-to-End Pipeline Test (4 hours)

**Goal**: Kafka → RisingWave → Lakekeeper → nexora-eventlog → Graph

**Steps**:
1. Create test script:
   ```bash
   #!/bin/bash
   # scripts/test-phase4-pipeline.sh
   
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
   
   # 5. Verify in Lakekeeper
   curl http://localhost:8181/v1/namespaces/nexora_db/tables/events
   
   # 6. Verify in nexora
   curl http://localhost:8080/api/events/query
   
   # 7. Verify in graph
   curl -X POST http://localhost:8080/api/query/cypher \
     -d '{"query": "MATCH (c:Cargo) WHERE c.id = \"C001\" RETURN c"}'
   ```
2. Run full pipeline test
3. Verify data at each stage
4. Measure latency (Kafka → Graph)

**Acceptance Criteria**:
- ✅ Events flow from Kafka to Graph
- ✅ Data visible in Lakekeeper catalog
- ✅ RisingWave sink writing successfully
- ✅ nexora-eventlog reading from same tables
- ✅ Graph nodes created correctly
- ✅ End-to-end latency <10 seconds

### Task 6: Documentation (2 hours)

**Goal**: Document Lakekeeper deployment and configuration

**Steps**:
1. Update DISTRIBUTED_DEPLOYMENT.md with Lakekeeper section
2. Create LAKEKEEPER_SETUP.md with detailed guide
3. Update RISINGWAVE_ICEBERG_INTEGRATION.md
4. Add troubleshooting section for common issues

**Deliverables**:
- Lakekeeper deployment guide
- Configuration reference
- Troubleshooting checklist
- Architecture diagrams

## Success Criteria

### Phase 4 Complete When:

- [x] Task 1: REST catalog investigation completed
- [x] Architecture decision revised based on findings
- [ ] Task 2: Lakekeeper stack deployed and verified
- [ ] Task 3: nexora-eventlog integration test passes
- [ ] Task 4: RisingWave sink configured and tested
- [ ] Task 5: End-to-end pipeline test passes
- [ ] Task 6: Documentation updated

### Performance Targets:

- Event ingestion: >10k events/sec
- Sink latency: <5 seconds (Kafka to Iceberg)
- Query latency: <100ms (graph queries)
- Catalog operations: <1 second (table creation via Lakekeeper)

## Rollback Plan

There is **no alternative** to using an external REST catalog, since:
1. RisingWave does not provide REST catalog endpoint (confirmed)
2. Iceberg requires a catalog for metadata management
3. Local SQLite catalog does not support multi-writer scenarios

If Lakekeeper has issues:

### Plan B: Use Apache Polaris

1. Deploy Polaris instead of Lakekeeper
2. Update catalog.uri to Polaris endpoint
3. Polaris provides same REST catalog interface

### Plan C: Use AWS Glue (Production Only)

1. Configure AWS Glue as Iceberg catalog
2. Update nexora.toml with Glue endpoint
3. Requires AWS credentials and permissions

## Timeline

| Task | Estimated Time | Status |
|------|---------------|--------|
| 1. Locate REST endpoint | 1.5 hours | ✅ Complete |
| 2. Deploy Lakekeeper stack | 3 hours | ⏳ Pending |
| 3. Integrate nexora-eventlog | 3 hours | ⏳ Pending |
| 4. Configure RisingWave sink | 2 hours | ⏳ Pending |
| 5. End-to-end pipeline test | 4 hours | ⏳ Pending |
| 6. Documentation | 2 hours | ⏳ Pending |
| **Total** | **15.5 hours (~2 days)** | **Phase 4** |

## Next Steps

**Immediate**: Start Task 2 - Deploy Lakekeeper stack

1. Create docker-compose.yml in project root:
   ```bash
   # Copy configuration from this document
   vim docker-compose.yml
   ```

2. Start services:
   ```bash
   docker-compose up -d
   ```

3. Verify Lakekeeper:
   ```bash
   curl http://localhost:8181/v1/config
   ```

4. Initialize MinIO bucket:
   ```bash
   docker run --rm --network host \
     minio/mc alias set local http://localhost:9000 minioadmin minioadmin
   docker run --rm --network host \
     minio/mc mb local/nexora-events
   ```

---

**Document Version**: 2.0 (Revised after Task 1)  
**Date**: 2026-07-30  
**Decision Owner**: Nexora Development Team  
**Approved**: Yes (Option 2 - External Lakekeeper required)
