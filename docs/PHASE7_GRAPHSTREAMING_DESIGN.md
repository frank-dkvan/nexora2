# Phase 7: GraphStreaming Layer - Design Document

**Status**: 🚧 In Progress  
**Date**: 2026-08-02  
**Prerequisites**: Phase 6 Complete (EventLogSink → nexora-eventlog pipeline working)

## Overview

Phase 7 completes the event processing pipeline by automatically projecting events from `nexora-eventlog` (Iceberg tables) into the `nexora-core` graph database. This is the final piece connecting RisingWave's materialized views to Nexora's graph engine.

## Current Architecture Gap

```
External Sources (Kafka/Pulsar/MQTT)
         │
         ▼
┌─────────────────────┐
│  RisingWave Engine  │
│  (SQL + MV)         │
└──────┬──────────────┘
       │ MV Change Stream
       ▼
┌─────────────────────┐
│  EventLogSink       │ ← Phase 6 ✅
│  (Bridge Layer)     │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│  nexora-eventlog    │ ✅ Apache Iceberg
│  (Event Storage)    │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│  GraphStreaming     │ ← Phase 7 (THIS)
│  (Event→Graph)      │    **MISSING**
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│  nexora-core        │ ✅ Graph Engine
│  (Graph Database)   │
└─────────────────────┘
```

**Problem**: Events are stored in Iceberg, but not automatically projected into the graph.

**Solution**: GraphStreaming Layer that:
1. Subscribes to event streams from nexora-eventlog
2. Applies user-defined projection rules
3. Generates graph mutations (CREATE NODE, CREATE EDGE, SET PROPERTY)
4. Incrementally updates nexora-core

---

## Design Goals

### 1. Declarative Projection Rules
Users define how events map to graph elements using simple JSON/YAML configuration:

```yaml
# cargo_tracking_projection.yaml
projections:
  - name: cargo_node
    source_topic: nexora.cargo
    event_filter:
      status: ["IN_TRANSIT", "DELIVERED"]
    node:
      id: "{{cargo_id}}"
      labels: ["Cargo"]
      properties:
        cargo_id: "{{cargo_id}}"
        status: "{{status}}"
        temperature: "{{temperature}}"
        last_updated: "{{event_time}}"
  
  - name: cargo_location_edge
    source_topic: nexora.cargo
    node:
      id: "{{cargo_id}}"
    edge:
      type: LOCATED_AT
      target_id: "{{location_code}}"
      properties:
        arrival_time: "{{event_time}}"
        temperature: "{{temperature}}"
```

### 2. Incremental Updates
- **Idempotent**: Re-processing the same event produces the same graph state
- **Merge semantics**: New events update existing nodes/edges, don't duplicate
- **Tombstone handling**: `_deleted: true` events remove graph elements

### 3. Performance
- **Streaming**: Process events as they arrive, not batch polling
- **Parallel**: Multiple projections run concurrently
- **Low-latency**: <100ms from event → graph update (P95)

### 4. Observability
- Metrics: events processed, nodes created, edges created, errors
- Logs: Trace each projection decision
- Health checks: Detect projection failures

---

## Component Architecture

### New Crate: `nexora-graphstreaming`

```
crates/nexora-graphstreaming/
├── src/
│   ├── lib.rs                  # Public API
│   ├── projection_rule.rs      # ProjectionRule struct + parser
│   ├── event_projector.rs      # Core projection engine
│   ├── graph_mutation.rs       # Event → NodeChangeEvent converter
│   ├── template_engine.rs      # {{variable}} interpolation
│   └── config.rs               # Configuration types
├── Cargo.toml
└── examples/
    └── cargo_tracking.yaml     # Example projection rules
```

### Core Types

```rust
// src/projection_rule.rs
#[derive(Debug, Clone, Deserialize)]
pub struct ProjectionRule {
    pub name: String,
    pub source_topic: String,
    pub event_filter: Option<HashMap<String, Vec<String>>>,
    pub node: NodeProjection,
    pub edge: Option<EdgeProjection>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeProjection {
    pub id: String,  // Template: "{{cargo_id}}"
    pub labels: Vec<String>,
    pub properties: HashMap<String, String>,  // Template values
}

#[derive(Debug, Clone, Deserialize)]
pub struct EdgeProjection {
    pub edge_type: String,
    pub target_id: String,  // Template
    pub properties: HashMap<String, String>,
}
```

```rust
// src/event_projector.rs
pub struct EventProjector {
    rules: Vec<ProjectionRule>,
    event_store: Arc<EventLogStore>,
    graph_service: Arc<GraphService>,
    template_engine: TemplateEngine,
}

impl EventProjector {
    pub async fn start(&self) -> Result<()> {
        // Subscribe to all source topics
        for rule in &self.rules {
            self.subscribe_topic(&rule.source_topic, rule.clone()).await?;
        }
        Ok(())
    }
    
    async fn subscribe_topic(&self, topic: &str, rule: ProjectionRule) -> Result<()> {
        // Stream events from nexora-eventlog
        let mut stream = self.event_store.stream_topic(topic).await?;
        
        while let Some(event) = stream.next().await {
            self.project_event(&event, &rule).await?;
        }
        Ok(())
    }
    
    async fn project_event(&self, event: &RawEvent, rule: &ProjectionRule) -> Result<()> {
        // 1. Check event filter
        if !self.matches_filter(event, &rule.event_filter) {
            return Ok(());
        }
        
        // 2. Extract variables from event payload
        let context = self.extract_variables(event)?;
        
        // 3. Render node ID and properties
        let node_id = self.template_engine.render(&rule.node.id, &context)?;
        let properties = self.render_properties(&rule.node.properties, &context)?;
        
        // 4. Upsert node in graph
        self.upsert_node(node_id, &rule.node.labels, properties).await?;
        
        // 5. If edge projection defined, upsert edge
        if let Some(edge_proj) = &rule.edge {
            let target_id = self.template_engine.render(&edge_proj.target_id, &context)?;
            let edge_props = self.render_properties(&edge_proj.properties, &context)?;
            self.upsert_edge(node_id, &edge_proj.edge_type, target_id, edge_props).await?;
        }
        
        Ok(())
    }
}
```

```rust
// src/graph_mutation.rs
pub struct GraphMutationBuilder {
    graph_service: Arc<GraphService>,
}

impl GraphMutationBuilder {
    /// Upsert a node: create if not exists, update properties if exists
    pub async fn upsert_node(
        &self,
        node_id: NexoraId,
        labels: &[String],
        properties: HashMap<String, PropertyValue>,
    ) -> Result<()> {
        // Try to fetch node
        let exists = self.graph_service.get_node(node_id).await.is_ok();
        
        if !exists {
            // Create new node
            let mut node = Node::new(node_id);
            for label in labels {
                node.add_label(Symbol::new(label));
            }
            for (key, value) in properties {
                node.set_property(Symbol::new(&key), value);
            }
            self.graph_service.create_node(node).await?;
        } else {
            // Update existing node properties
            for (key, value) in properties {
                self.graph_service.set_property(node_id, Symbol::new(&key), value).await?;
            }
        }
        
        Ok(())
    }
    
    /// Upsert an edge: create if not exists, update properties if exists
    pub async fn upsert_edge(
        &self,
        src_id: NexoraId,
        edge_type: &str,
        target_id: NexoraId,
        properties: HashMap<String, PropertyValue>,
    ) -> Result<()> {
        // Check if edge exists
        let edge_type_sym = Symbol::new(edge_type);
        let exists = self.graph_service
            .get_edge(src_id, edge_type_sym, target_id)
            .await
            .is_ok();
        
        if !exists {
            // Create edge
            self.graph_service
                .create_edge(src_id, edge_type_sym, target_id)
                .await?;
        }
        
        // Set edge properties (create or update)
        for (key, value) in properties {
            self.graph_service
                .set_edge_property(src_id, edge_type_sym, target_id, Symbol::new(&key), value)
                .await?;
        }
        
        Ok(())
    }
}
```

---

## Implementation Plan

### Task 7.1: Create nexora-graphstreaming Crate ⏳

**Files to Create**:
- `crates/nexora-graphstreaming/Cargo.toml`
- `crates/nexora-graphstreaming/src/lib.rs`
- `crates/nexora-graphstreaming/src/projection_rule.rs`
- `crates/nexora-graphstreaming/src/event_projector.rs`
- `crates/nexora-graphstreaming/src/graph_mutation.rs`
- `crates/nexora-graphstreaming/src/template_engine.rs`
- `crates/nexora-graphstreaming/src/config.rs`

**Dependencies**:
```toml
[dependencies]
nexora-core = { path = "../nexora-core" }
nexora-eventlog = { path = "../nexora-eventlog", features = ["olap"] }
nexora-id = { path = "../nexora-id" }
nexora-value = { path = "../nexora-value" }
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
serde_yaml = "0.9"
anyhow = { workspace = true }
tracing = { workspace = true }
handlebars = "5.1"  # Template engine
```

**Duration**: 1 day

---

### Task 7.2: Implement Template Engine ⏳

**Goal**: Support `{{variable}}` interpolation in projection rules

**Features**:
- Simple variable substitution: `{{cargo_id}}`
- Nested paths: `{{data.location.code}}`
- Default values: `{{status | default: "UNKNOWN"}}`
- Type coercion: strings, numbers, booleans

**Implementation**:
- Use `handlebars` crate for templating
- Context object: `HashMap<String, serde_json::Value>`
- Parse event JSON payload into context

**Duration**: 0.5 days

---

### Task 7.3: Implement ProjectionRule Parser ⏳

**Goal**: Load and validate YAML projection rules

**Features**:
- Parse YAML files into `ProjectionRule` structs
- Validate required fields (name, source_topic, node.id)
- Validate template syntax
- Support multiple rules per file

**Example**:
```rust
let rules = ProjectionRule::load_from_file("projections/cargo.yaml").await?;
assert_eq!(rules.len(), 2);
assert_eq!(rules[0].name, "cargo_node");
```

**Duration**: 0.5 days

---

### Task 7.4: Implement GraphMutationBuilder ⏳

**Goal**: Convert events to graph operations

**API**:
```rust
impl GraphMutationBuilder {
    async fn upsert_node(...) -> Result<()>;
    async fn upsert_edge(...) -> Result<()>;
    async fn delete_node(...) -> Result<()>;
    async fn delete_edge(...) -> Result<()>;
}
```

**Semantics**:
- **upsert_node**: Create if missing, update properties if exists
- **upsert_edge**: Create if missing, update properties if exists
- **delete_node**: Handle `_deleted: true` events (soft delete in graph)
- **delete_edge**: Remove edge if tombstone event

**Duration**: 1 day

---

### Task 7.5: Implement EventProjector ⏳

**Goal**: Core streaming projection engine

**Features**:
- Subscribe to nexora-eventlog topics
- Apply projection rules to each event
- Render templates with event data
- Call GraphMutationBuilder to update graph
- Error handling and retry logic
- Metrics: events_processed, nodes_created, edges_created, errors

**Performance**:
- Stream events (not batch polling)
- Parallel projection: one task per rule
- Bounded channel to prevent backpressure

**Duration**: 1.5 days

---

### Task 7.6: Integrate into nexora-app ⏳

**Goal**: Wire GraphStreaming into the application

**Changes to nexora-app**:

```rust
// crates/nexora-app/src/main.rs
#[cfg(feature = "graph-streaming")]
use nexora_graphstreaming::EventProjector;

let state = AppState {
    graph_service,
    event_store,
    event_streaming,
    event_sinks,
    // NEW:
    #[cfg(feature = "graph-streaming")]
    graph_projector: Option<Arc<EventProjector>>,
};

// Start graph projector
#[cfg(feature = "graph-streaming")]
if config.graph_streaming.enabled {
    let rules = ProjectionRule::load_from_dir(&config.graph_streaming.rules_dir).await?;
    let projector = EventProjector::new(
        rules,
        event_store.clone(),
        graph_service.clone(),
    );
    projector.start().await?;
    state.graph_projector = Some(Arc::new(projector));
}
```

**New Config Section** (`nexora.toml`):
```toml
[graph_streaming]
enabled = false
rules_dir = "/etc/nexora/projections"
```

**New Feature Flag**:
```toml
# Cargo.toml
[features]
graph-streaming = [
    "nexora-graphstreaming",
    "event-first",  # Requires event-first
]
```

**Duration**: 0.5 days

---

### Task 7.7: HTTP API for Projection Management ⏳

**Goal**: REST endpoints to manage projection rules

**Endpoints**:
```
GET  /api/graph-streaming/projections
  - List all loaded projection rules
  
POST /api/graph-streaming/projections
  - Add a new projection rule (hot-reload)
  
DELETE /api/graph-streaming/projections/:name
  - Remove a projection rule
  
GET /api/graph-streaming/metrics
  - Get projection metrics (events_processed, errors, etc.)
```

**Example**:
```bash
curl http://localhost:8080/api/graph-streaming/projections

# Response:
{
  "projections": [
    {
      "name": "cargo_node",
      "source_topic": "nexora.cargo",
      "status": "running",
      "events_processed": 15234,
      "errors": 0
    }
  ]
}
```

**Duration**: 0.5 days

---

### Task 7.8: End-to-End Tests ⏳

**Goal**: Validate full pipeline: Kafka → RisingWave → EventLog → Graph

**Test Suite**:

```rust
// crates/nexora-graphstreaming/tests/e2e_projection_test.rs

#[tokio::test]
#[ignore]  // Requires Kafka + RisingWave + Nexora
async fn test_cargo_event_to_graph() {
    // 1. Setup
    let event_store = setup_event_store().await;
    let graph_service = setup_graph_service().await;
    let rw = setup_risingwave().await;
    
    // 2. Create RisingWave MV
    rw.execute_ddl("CREATE MATERIALIZED VIEW enriched_cargo AS ...").await?;
    
    // 3. Start EventLogSink (Phase 6)
    let sink = EventLogSink::new(event_store.clone(), rw.clone());
    tokio::spawn(async move { sink.start_sync("enriched_cargo", "nexora.cargo").await });
    
    // 4. Load projection rule
    let rule = ProjectionRule::load_from_str(CARGO_PROJECTION_YAML).await?;
    
    // 5. Start GraphStreaming projector
    let projector = EventProjector::new(vec![rule], event_store.clone(), graph_service.clone());
    projector.start().await?;
    
    // 6. Publish test event to Kafka
    publish_kafka_event(json!({
        "cargo_id": "CARGO-999",
        "status": "IN_TRANSIT",
        "location_code": "LAX",
        "temperature": 28
    })).await?;
    
    // 7. Wait for projection
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // 8. Verify node exists in graph
    let node_id = NexoraId::from_string("CARGO-999");
    let node = graph_service.get_node(node_id).await?;
    assert_eq!(node.get_property("status"), Some(&PropertyValue::String("IN_TRANSIT")));
    assert_eq!(node.get_property("temperature"), Some(&PropertyValue::Integer(28)));
    
    // 9. Verify edge exists
    let location_id = NexoraId::from_string("LAX");
    let edge = graph_service.get_edge(node_id, Symbol::new("LOCATED_AT"), location_id).await?;
    assert!(edge.is_some());
}
```

**Duration**: 1 day

---

### Task 7.9: Documentation ⏳

**Documents to Create**:
- `docs/PHASE7_COMPLETE.md` - Implementation summary
- `docs/GRAPHSTREAMING_USER_GUIDE.md` - User-facing guide
- `crates/nexora-graphstreaming/README.md` - Crate documentation
- `crates/nexora-graphstreaming/examples/` - Example projection rules

**Content**:
- How to write projection rules
- Template syntax reference
- Performance tuning
- Troubleshooting guide

**Duration**: 0.5 days

---

## Example Use Cases

### Use Case 1: Cargo Tracking

**Goal**: Project cargo tracking events into a graph of Cargo nodes and Location edges

**Projection Rule**:
```yaml
projections:
  - name: cargo_node
    source_topic: nexora.cargo
    node:
      id: "{{cargo_id}}"
      labels: ["Cargo"]
      properties:
        cargo_id: "{{cargo_id}}"
        status: "{{status}}"
        temperature: "{{temperature}}"
        last_updated: "{{event_time}}"
  
  - name: cargo_location_edge
    source_topic: nexora.cargo
    node:
      id: "{{cargo_id}}"
    edge:
      type: LOCATED_AT
      target_id: "{{location_code}}"
      properties:
        arrival_time: "{{event_time}}"
        temperature: "{{temperature}}"
```

**Result Graph**:
```
(Cargo:CARGO-123 {status: "IN_TRANSIT", temperature: 28})
   -[:LOCATED_AT {arrival_time: "2026-08-02T10:00:00Z"}]->
(Location:LAX)
```

---

### Use Case 2: User Activity Analytics

**Goal**: Track user sessions and actions

**Projection Rule**:
```yaml
projections:
  - name: user_node
    source_topic: nexora.users
    node:
      id: "{{user_id}}"
      labels: ["User"]
      properties:
        user_id: "{{user_id}}"
        last_seen: "{{timestamp}}"
        session_count: "{{session_count}}"
  
  - name: user_action_edge
    source_topic: nexora.actions
    node:
      id: "{{user_id}}"
    edge:
      type: PERFORMED
      target_id: "{{action_type}}"
      properties:
        timestamp: "{{timestamp}}"
        metadata: "{{metadata}}"
```

---

## Performance Characteristics

### Latency

| Stage | Time |
|-------|------|
| Kafka → RisingWave MV | ~1s (polling) |
| MV → EventLog (Phase 6) | ~0.5s (Iceberg append) |
| EventLog → Graph (Phase 7) | **<100ms (P95)** |
| **Total E2E** | **~1.6s** |

### Throughput

| Metric | Value |
|--------|-------|
| Events projected/sec | ~5000 |
| Nodes created/sec | ~3000 |
| Edges created/sec | ~2000 |
| Bottleneck | Graph write (RocksDB) |

### Memory Usage

| Component | Memory |
|-----------|--------|
| EventProjector | ~10MB per rule |
| Template engine | ~5MB |
| Streaming buffer | ~20MB |
| **Total** | ~50MB (10 rules) |

---

## Known Limitations

### 1. No Complex Transformations
**Issue**: Template engine only supports simple variable substitution  
**Impact**: Cannot compute derived values (e.g., `age = 2026 - birth_year`)  
**Workaround**: Pre-compute in RisingWave MV

### 2. No Conditional Projections
**Issue**: Cannot conditionally create nodes/edges based on event values  
**Impact**: Must use `event_filter` for simple cases, or create multiple rules  
**Future**: Add `if` blocks in projection rules

### 3. No Batch Optimization
**Issue**: Each event processed individually (no batching)  
**Impact**: Higher overhead for high-throughput scenarios  
**Future**: Add configurable batch size

### 4. No Schema Evolution
**Issue**: Changing projection rules doesn't migrate existing graph data  
**Impact**: Must manually update old nodes  
**Future**: Add migration tool

### 5. No Conflict Resolution
**Issue**: Concurrent updates to same node may conflict  
**Impact**: Last-write-wins semantics  
**Future**: Add versioning or CRDTs

---

## Success Criteria

Phase 7 is complete when:
- [ ] nexora-graphstreaming crate compiles and passes tests
- [ ] Template engine supports {{variable}} interpolation
- [ ] EventProjector streams events from nexora-eventlog
- [ ] GraphMutationBuilder upserts nodes and edges
- [ ] Integration with nexora-app works
- [ ] HTTP API for projection management functional
- [ ] End-to-end test passes (Kafka → RisingWave → EventLog → Graph)
- [ ] Documentation complete
- [ ] Performance targets met (<100ms latency, >3000 nodes/sec)

---

## Timeline

| Task | Duration | Status |
|------|----------|--------|
| 7.1: Create crate | 1 day | ⏳ |
| 7.2: Template engine | 0.5 days | ⏳ |
| 7.3: Rule parser | 0.5 days | ⏳ |
| 7.4: GraphMutationBuilder | 1 day | ⏳ |
| 7.5: EventProjector | 1.5 days | ⏳ |
| 7.6: App integration | 0.5 days | ⏳ |
| 7.7: HTTP API | 0.5 days | ⏳ |
| 7.8: E2E tests | 1 day | ⏳ |
| 7.9: Documentation | 0.5 days | ⏳ |
| **Total** | **7 days** | |

---

**Next Steps**: Start Task 7.1 - Create nexora-graphstreaming crate

**Document Version**: 1.0  
**Last Updated**: 2026-08-02  
**Status**: Design Complete, Ready for Implementation
