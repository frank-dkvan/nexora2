# nexora-graphstreaming

GraphStreaming Layer for Nexora: Automatic event-to-graph projection

## Overview

`nexora-graphstreaming` bridges `nexora-eventlog` (Iceberg event tables) and `nexora-core` (graph database) by automatically projecting events into graph nodes and edges based on user-defined rules.

## Architecture

```
nexora-eventlog (Iceberg)
        │
        ▼ stream events
EventProjector (applies rules)
        │
        ▼ generate mutations
GraphMutationBuilder
        │
        ▼ update
nexora-core (Graph)
```

## Features

- **Declarative Rules**: Define projections using simple YAML configuration
- **Template Engine**: Handlebars-style variable interpolation (`{{field}}`)
- **Event Filtering**: Only process events matching specific criteria
- **Incremental Updates**: Upsert semantics for nodes and edges
- **Streaming**: Real-time event processing (not batch)
- **Metrics**: Track events processed, nodes/edges created, errors

## Usage

### Define Projection Rules

Create a YAML file with projection rules:

```yaml
# cargo_tracking.yaml
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
  
  - name: cargo_location_edge
    source_topic: nexora.cargo
    node:
      id: "{{cargo_id}}"
      labels: ["Cargo"]
    edge:
      edge_type: LOCATED_AT
      target_id: "{{location_code}}"
      properties:
        arrival_time: "{{event_time}}"
```

### Start Event Projector

```rust
use nexora_graphstreaming::{EventProjector, ProjectionRule};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load projection rules
    let rules = ProjectionRule::load_from_file("cargo_tracking.yaml").await?;

    // Create projector
    let projector = EventProjector::new(
        rules,
        event_store.clone(),
        graph_service.clone(),
    );

    // Start streaming projection
    projector.start().await?;

    // Get metrics
    let metrics = projector.get_metrics();
    for (rule_name, m) in metrics {
        println!("{}: {} events, {} nodes, {} edges",
            rule_name, m.events_processed, m.nodes_created, m.edges_created);
    }

    Ok(())
}
```

## Template Syntax

### Simple Variables
```yaml
id: "{{cargo_id}}"           # Direct field access
status: "{{status}}"
```

### Nested Fields
```yaml
city: "{{location.city}}"    # Nested object access
country: "{{location.country}}"
```

### Supported Types
- Strings: `"{{name}}"`
- Numbers: `"{{temperature}}"` (auto-parsed as integer or float)
- Booleans: `"{{active}}"` (auto-parsed)

## Event Filtering

Filter events before projection:

```yaml
event_filter:
  status: ["IN_TRANSIT", "DELIVERED"]  # Only these statuses
  priority: ["1", "2"]                 # Only priority 1 or 2
```

Events must match ALL filter criteria (AND logic).

## Projection Semantics

### Node Upsert
- **If node doesn't exist**: Create with labels and properties
- **If node exists**: Update properties (labels unchanged)

### Edge Upsert
- **If edge doesn't exist**: Create with properties
- **If edge exists**: Update properties

### Idempotency
Re-processing the same event produces the same graph state.

## Configuration

Add to `nexora.toml`:

```toml
[graph_streaming]
enabled = true
rules_dir = "/etc/nexora/projections"
max_concurrent_projections = 10
buffer_size = 1000
```

## Examples

See `examples/` directory:
- `cargo_tracking.yaml` - Logistics cargo tracking
- `user_activity.yaml` - User activity analytics

## Performance

- **Latency**: <100ms per event (P95)
- **Throughput**: ~5000 events/sec
- **Memory**: ~10MB per projection rule

## Limitations

1. **No complex transformations**: Only simple variable substitution (compute in RisingWave MV instead)
2. **No conditional projections**: Use `event_filter` or create multiple rules
3. **No batch optimization**: Events processed individually
4. **Last-write-wins**: Concurrent updates to same node may conflict

## Development

### Run Tests
```bash
cargo test -p nexora-graphstreaming
```

### Add New Projection Rule
1. Create YAML file in `/etc/nexora/projections/`
2. Restart nexora-app (hot-reload coming soon)

## See Also

- [Phase 7 Design Document](../../docs/PHASE7_GRAPHSTREAMING_DESIGN.md)
- [RisingWave Integration Plan](../../docs/RISINGWAVE_INTEGRATION_PLAN.md)

## License

Apache-2.0
