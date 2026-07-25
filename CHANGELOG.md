# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-07-18

### Added

#### Out-of-order event processing (Track F4/F5)
- Event-time last-writer-wins (LWW) for property writes **and** removes: each
  property carries the event time of the mutation that last decided its state,
  so out-of-order arrivals converge to the event-time-latest value instead of
  the arrival-order-latest one.
- Per-operation event times: a single ingest batch coalescing several records
  for one node resolves each write against its own record's timestamp.
- Per-property tombstones with event time: a late remove cannot delete a newer
  value, and a stale set below a remove's tombstone cannot resurrect it. Live
  commit and WAL-replay paths implement the same LWW decision.
- `EventTimeUnit` (`Seconds` / `Millis` / `Micros` (default) / `Rfc3339`):
  numeric event-time fields are parsed by an explicit configured unit; string
  fields always parse as RFC 3339.
- Event-time extraction wired through all ingest sources (file, Kafka, Kinesis,
  MQTT, WebSocket, Zenoh), the CLI (`--event-time-field`), and bulk ingest.
- Watermark engine in `nexora-stream`: `WatermarkGenerator` (monotonic
  watermark) + `TumblingWindow` (stateful buckets with allowed-lateness);
  `IngestionPipeline` advances the watermark per batch.
- F1.2–F1.4 Global Checkpoint (per-shard flush with real counts, RocksDB
  checkpoint store, crash-recovery offset alignment) and F2 exactly-once
  end-to-end verification.
- F1.0 Fragment Time Travel with MVCC-overwrite semantics.
- Fragment consolidation strategy with background trigger (B8); pluggable
  compression/encryption filter pipeline (B10); VFS abstraction verified (B9).

#### Distributed & correctness (Track A/C)
- Two-phase commit and WAL torn-write repair primitives (A1.1/A4).
- Strict W+R>N read/write consistency with majority quorum (A1.2).
- Failover catch-up protocol + auto-failover integration (A1.3).
- Per-namespace divergence isolation (quarantine tracker): a single namespace's
  apply failure no longer fences the whole node (A6).
- Bounded-concurrency query pool with caller-runs backpressure (D6).
- Offset-aligned streaming checkpoint (B2), unified snapshot primitive with
  CRC32 manifest (B1), database backup/restore/PITR basics (B3).

#### Performance (Track D — complete)
- Batch-concurrent BFS via `buffer_unordered` frontier expansion (D1).
- Dedicated adjacency-list traversal path (D2).
- Zero-copy wake via MessagePack node snapshots (B4).
- Topology-aware residency: hubs resist LRU eviction (D4).
- Concurrent shard flush and multi-hop path-join parallelization (B6/C5).

#### Operations (Track E)
- Failover alerting hooks and honest EXPERIMENTAL banner (E3/E6).

### Changed
- `SnapshotData` gains a `property_times` field, appended last to preserve
  MessagePack positional backward-compatibility with pre-0.3.0 snapshots.
- Workspace and all member crates now share a single version (`version.workspace
  = true`); `nexora-cli`, `nexora-client`, and `nexora-pgwire` no longer pin
  their own version.

### Known limitations
- Data-plane writes use best-effort quorum, not the (implemented but un-wired)
  two-phase commit path; matters only in the RF>1 + quorum-failure window, where
  idempotent replay + anti-entropy converge (TD-1). Write path carries no
  request_id idempotency key yet — required before introducing non-idempotent
  ops (TD-2).
- Per-property tombstones are retained without reclamation this release; a
  low-watermark GC is deferred until a watermark consumer exists.
- The tumbling-window engine advances a watermark but its window output has no
  production consumer yet (not wired to Standing Queries / materialized views).

## [0.2.0] - 2026-07-15

### Added
- Production-readiness hardening across security (P0 fixes: quorum-write
  rollback, replication-log error handling, WHERE short-circuit, SQL-injection
  guards, resource-exhaustion protection, default auth/key safety), operations
  (Prometheus metrics endpoint, ops tooling), and testing (end-to-end and
  grey-release harnesses).
- Distributed Cypher/SQL over multi-node clusters.
- Standing Query state management; Checkpoint and exactly-once primitives;
  watermark propagation; distributed write idempotency, anti-entropy repair, and
  shard load balancing.
- Dashboard P0/P1/P2 enhancements.

### Fixed
- Cypher path-variable syntax; dashboard graph visualization and CSP/CDN white
  screen; assorted P0/P1 blockers.

## [0.1.0] - 2026-07-04

### Added

#### Core Engine
- Streaming graph database engine with sharded architecture (default 256 shards)
- LRU-based node eviction per shard (configurable `--max-nodes-per-shard`)
- Async actor-based node processing with channel-based message passing
- Property change callbacks for real-time event-driven processing
- Time-travel query support via history retention

#### Query Languages
- **Cypher** support with 99% clause coverage (`CREATE`, `MATCH`, `MERGE`, `DELETE`, `SET`, `RETURN`, `WITH`, `WHERE`, `ORDER BY`, `LIMIT`, `SKIP`, `UNION`, `UNWIND`, `CASE WHEN`, `CALL`, `FOREACH`)
- **SQL** query support with `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, `JOIN` on graph data
- SQL DDL support for materialized view creation (`CREATE MATERIALIZED VIEW`)
- Query optimizer with cost-based plan selection and `EXPLAIN` output
- Query rewriter for Cypher-to-internal-plan translation

#### HTTP API & Server
- RESTful HTTP API (v2) with OpenAPI 3.0 specification at `/api/v2/openapi.json`
- Swagger UI documentation at `/api/v2/docs`
- WebSocket endpoints for live query streaming (`/api/v2/ws/query`, `/api/v2/ws/sq/{id}`)
- Health check endpoints: `/api/v2/health`, `/api/v2/health/ready`, `/api/v2/health/live`
- Prometheus metrics endpoint at `/metrics`
- JSON metrics endpoint at `/api/v2/metrics`
- React SPA dashboard served at `/dashboard`
- Graceful shutdown with in-flight request draining
- Request body size limiting (16 MB default)
- Request ID tracking middleware
- CORS configuration via `--cors-origin`

#### Standing Queries
- Standing Query Manager with persistent subscription-based pattern matching
- Real-time property-change-triggered evaluation
- Standing Query to Materialized View bridge for automatic view maintenance
- WebSocket-based live result streaming
- Admin API for CRUD operations on standing queries

#### Materialized Views
- Incremental materialized view engine backed by RocksDB
- SQL DDL-based view creation (`CREATE MATERIALIZED VIEW`)
- Automatic refresh on standing query matches
- View linking API (`/api/v2/materialized-views/{id}/link-sq`)
- Manual refresh API endpoint
- Query materialized view data via REST API

#### Vector Search
- HNSW (Hierarchical Navigable Small World) approximate nearest neighbor index
- Vector indexing API (`/api/v2/vector/index`)
- Similarity search API (`/api/v2/vector/search`)
- Per-node vector management (GET/DELETE)
- Configurable HNSW parameters (M, ef_construction, ef_search)

#### User-Defined Functions (UDF)
- Native UDF registry for built-in functions
- **Wasm**-based UDF support with sandboxed execution
- **Python**-based UDF support via embedded runtime
- UDF management API: register, list, execute, delete
- File-based UDF loading from `--udf-dir` directory

#### Stream Ingestion
- **Kafka** streaming ingestion source with consumer group support
- CLI-driven Kafka configuration (`--kafka-brokers`, `--kafka-topic`, `--kafka-group-id`)
- File-based batch ingestion API
- Stream source management API (`/api/v2/streams`)
- Recipe-based ingestion pipelines with execution tracking

#### Persistence & Recovery
- **RocksDB** persistent storage backend (`--rocksdb-path`)
- **WAL (Write-Ahead Log)** for crash recovery with automatic replay on startup
- In-memory storage mode for ephemeral/testing workloads (`--no-rocksdb`)
- Configurable WAL directory (`--wal-dir`)
- WAL encryption with AES-256-GCM (`--encrypt-wal`)

#### Distributed Cluster Mode
- Multi-node cluster operation via TCP and Zenoh transport (`--cluster`)
- Node identity management (`--node-id`)
- Inter-node graph operation listener (`--cluster-listen-addr`)
- Heartbeat protocol for liveness detection (`--cluster-heartbeat-addr`)
- Peer discovery with bootstrap configuration (`--peer`)
- Cluster stats API endpoint (`/api/v2/cluster/stats`)

#### Raft Consensus
- Raft consensus protocol for strong consistency (`--raft-port`)
- Log replication with quorum-based commit
- Leader election with configurable heartbeat and election timeouts
- Raft stats API endpoint (`/api/v2/cluster/raft`)
- Batched log entry replication for throughput optimization

#### Security
- **TLS/HTTPS** support with PEM certificate loading (`--tls-cert`, `--tls-key`)
- Self-signed certificate generation for development (`--gen-tls-cert`)
- **RBAC** (Role-Based Access Control) with Admin and Operator roles
- **JWT** authentication with HMAC-SHA256 signing (`--require-auth`, `--auth-secret`)
- Rate limiting with token bucket algorithm (configurable rate and burst)
- Security headers middleware
- Audit logging middleware
- Path traversal protection for file ingestion (`--allow-ingest-dir`)
- Private key file permission enforcement (0o600 on Unix)

#### Storage Tiering
- Tiered storage with Hot/Warm/Cold lifecycle management
- **S3** backend support for warm/cold tiers (`--storage-backend=s3`, `--s3-bucket`, `--s3-region`)
- Local filesystem tiered storage (`--storage-backend=local`)
- Configurable lifecycle rules for automatic tier migration (`--s3-cold-after-days`)
- Storage migration and status API endpoints

#### Data-at-Rest Encryption
- AES-256-GCM WAL encryption (`--encrypt-wal`)
- Multiple key provisioning methods:
  - CLI flag: `--encryption-key` (64 hex characters)
  - Key file: `--encryption-key-file`
  - Environment variable: `NEXORA_ENCRYPTION_KEY`

#### SDK & Client Libraries
- **Python SDK** for Python application integration
- **TypeScript SDK** for Node.js and browser integration
- **Rust client SDK** for native Rust application integration

#### Deployment & Operations
- **Docker** deployment with Dockerfile and docker-compose.yml
- Docker Compose cluster deployment (docker-compose-cluster.yml)
- Grafana dashboard configurations
- Three run profiles: `lite-ephemeral`, `single-durable`, `clustered`
- Profile validation and auto-derivation from flags

#### Testing
- 907+ unit and integration tests across the workspace
- 22 crates in the workspace
- Benchmark suite with criterion

### Changed

- Adopted async/await (Tokio runtime) as the sole execution model
- Sharded graph architecture replaces single-threaded model for horizontal scaling
- HTTP API versioned under `/api/v2/` namespace
- RocksDB used as the default persistent storage backend (InMemory is opt-in)
- WAL enabled by default for crash recovery (opt-out via `--no-wal`)
- Rate limiting enabled by default (configurable via `--rate-limit-rate`, `--rate-limit-burst`)
- CORS defaults to permissive (`*`) for development; configurable for production

### Fixed

- WAL replay correctly recovers in-flight mutations after crash
- Graceful shutdown waits for all in-flight request handlers to drain before final persistence flush
- Node LRU eviction respects active edge references to prevent dangling edges
- Concurrent property updates are serialized per-node via actor message queue
- WebSocket connections receive shutdown notification before server closes

### Security

- HMAC-SHA256 JWT token signing for authenticated API access
- AES-256-GCM encryption for WAL data at rest
- TLS 1.2+ for HTTPS and WSS connections
- RBAC enforces Admin-only access to standing query management endpoints
- Rate limiting mitigates abuse and DoS at the application layer
- Path traversal prevention on file ingestion endpoints
- Private key files created with 0o600 permissions
- Security headers (X-Content-Type-Options, X-Frame-Options, etc.) applied to all responses
- Audit logging records all API requests with request IDs

### Performance

- Sharded graph engine enables parallel node processing across 256 shards by default
- HNSW vector index provides O(log N) approximate nearest neighbor search
- Incremental materialized view updates avoid full recomputation
- Batched Raft log replication reduces per-entry RPC overhead
- LRU eviction caps memory usage per shard with configurable limits
- RocksDB compaction tuned for mixed read/write graph workloads
- Tokio async runtime maximizes I/O concurrency for HTTP and cluster traffic
- Channel-based actor model eliminates lock contention on hot paths

### Documentation

- OpenAPI 3.0 specification auto-generated and served at `/api/v2/openapi.json`
- Swagger UI available at `/api/v2/docs`
- API tutorial guide (`docs/api-tutorial.md`)
- Cluster operations guide (`docs/cluster-ops.md`)
- Performance tuning guide (`docs/performance-tuning.md`)
- Cypher support reference (`CYPHER_SUPPORT.md`)
- Cypher clause order documentation (`CYPHER_CLAUSE_ORDER.md`)
- Contributing guidelines (`CONTRIBUTING.md`)
- Benchmark guide (`BENCHMARK_GUIDE.md`)
- Design-to-implementation mapping (`docs/design-implementation-mapping.md`)
