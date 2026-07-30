# Event Streaming Library Mode - Distributed Deployment

## Overview

This document describes the distributed deployment architecture for Event Streaming library mode, where RisingWave components are compiled into the Nexora binary but run as separate processes for distributed deployments.

## Architecture

### Single-Node Mode (Current)

```
┌─────────────────────────────────────────┐
│         Nexora Process                   │
│  ┌───────────────────────────────────┐  │
│  │   RisingWave (library mode)       │  │
│  │   ┌──────┐ ┌────────┐ ┌────────┐ │  │
│  │   │ Meta │ │Frontend│ │Compute │ │  │
│  │   └──────┘ └────────┘ └────────┘ │  │
│  └───────────────────────────────────┘  │
│  ┌───────────────────────────────────┐  │
│  │       Nexora Core (Graph)         │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

### Distributed Mode (New)

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  Node 1          │    │  Node 2          │    │  Node 3          │
│  ┌────────────┐  │    │  ┌────────────┐  │    │  ┌────────────┐  │
│  │ Meta       │◄─┼────┼─►│ Meta       │◄─┼────┼─►│ Meta       │  │
│  │ (leader)   │  │    │  │ (follower) │  │    │  │ (follower) │  │
│  └────────────┘  │    │  └────────────┘  │    │  └────────────┘  │
│  ┌────────────┐  │    │  ┌────────────┐  │    │  ┌────────────┐  │
│  │ Frontend   │  │    │  │ Frontend   │  │    │  │ Frontend   │  │
│  └────────────┘  │    │  └────────────┘  │    │  └────────────┘  │
│  ┌────────────┐  │    │  ┌────────────┐  │    │  ┌────────────┐  │
│  │ Compute    │  │    │  │ Compute    │  │    │  │ Compute    │  │
│  └────────────┘  │    │  └────────────┘  │    │  └────────────┘  │
│  ┌────────────┐  │    │  ┌────────────┐  │    │  ┌────────────┐  │
│  │ Nexora     │  │    │  │ Nexora     │  │    │  │ Nexora     │  │
│  │ Core       │  │    │  │ Core       │  │    │  │ Core       │  │
│  └────────────┘  │    │  └────────────┘  │    │  └────────────┘  │
└──────────────────┘    └──────────────────┘    └──────────────────┘
```

## CLI Parameters

### Node Role Selection

```bash
# Start specific RisingWave components (library mode)
--library-event-streaming-role <ROLE>

# Valid roles (comma-separated):
#   meta      - Meta service (cluster coordinator)
#   compute   - Compute service (stream processing)
#   frontend  - Frontend service (SQL interface)
#   compactor - Compactor service (storage optimization)
#   all       - All services (single-node mode, default)
```

### Examples

#### Single-Node Deployment (Current)

```bash
# All components in one process
./nexora --enable-event-streaming --library-event-streaming
```

#### Distributed 3-Node Deployment (New)

**Node 1: Meta + Frontend + Compute**
```bash
./nexora \
  --enable-event-streaming \
  --library-event-streaming \
  --library-event-streaming-role meta,frontend,compute \
  --event-streaming-meta-addr 192.168.1.10:5690 \
  --event-streaming-frontend-addr 192.168.1.10:4566 \
  --library-compute-addr 192.168.1.10:5688 \
  --library-meta-backend etcd \
  --library-meta-etcd-endpoints http://192.168.1.10:2379,http://192.168.1.11:2379,http://192.168.1.12:2379
```

**Node 2: Meta + Frontend + Compute**
```bash
./nexora \
  --enable-event-streaming \
  --library-event-streaming \
  --library-event-streaming-role meta,frontend,compute \
  --event-streaming-meta-addr 192.168.1.11:5690 \
  --event-streaming-frontend-addr 192.168.1.11:4566 \
  --library-compute-addr 192.168.1.11:5688 \
  --library-meta-backend etcd \
  --library-meta-etcd-endpoints http://192.168.1.10:2379,http://192.168.1.11:2379,http://192.168.1.12:2379
```

**Node 3: Meta + Frontend + Compute**
```bash
./nexora \
  --enable-event-streaming \
  --library-event-streaming \
  --library-event-streaming-role meta,frontend,compute \
  --event-streaming-meta-addr 192.168.1.12:5690 \
  --event-streaming-frontend-addr 192.168.1.12:4566 \
  --library-compute-addr 192.168.1.12:5688 \
  --library-meta-backend etcd \
  --library-meta-etcd-endpoints http://192.168.1.10:2379,http://192.168.1.11:2379,http://192.168.1.12:2379
```

#### Specialized Role Deployment

**Dedicated Meta Cluster (3 nodes)**
```bash
# Node 1
./nexora \
  --enable-event-streaming \
  --library-event-streaming \
  --library-event-streaming-role meta \
  --event-streaming-meta-addr 192.168.1.10:5690 \
  --library-meta-backend etcd \
  --library-meta-etcd-endpoints http://etcd:2379

# Node 2, 3 similar...
```

**Dedicated Compute Nodes**
```bash
./nexora \
  --enable-event-streaming \
  --library-event-streaming \
  --library-event-streaming-role compute \
  --library-compute-addr 192.168.1.20:5688 \
  --library-meta-address http://192.168.1.10:5690,http://192.168.1.11:5690,http://192.168.1.12:5690
```

**Dedicated Frontend Nodes**
```bash
./nexora \
  --enable-event-streaming \
  --library-event-streaming \
  --library-event-streaming-role frontend \
  --event-streaming-frontend-addr 192.168.1.30:4566 \
  --library-meta-address http://192.168.1.10:5690,http://192.168.1.11:5690,http://192.168.1.12:5690
```

## Implementation Plan

### Phase 1: Extend CLI Parameters

Add new CLI parameters to `nexora-app/src/main.rs`:

```rust
/// Event Streaming library mode - node roles to start
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[arg(long, requires = "library_event_streaming", value_delimiter = ',')]
library_event_streaming_role: Vec<String>,

/// Compute node listen address
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[arg(long)]
library_compute_addr: Option<String>,

/// Meta backend: mem (single-node) or etcd (distributed)
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[arg(long, default_value = "mem")]
library_meta_backend: String,

/// Meta service address(es) for compute/frontend to connect
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[arg(long, value_delimiter = ',')]
library_meta_address: Vec<String>,

/// Etcd endpoints for distributed meta (required when backend=etcd)
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[arg(long, value_delimiter = ',')]
library_meta_etcd_endpoints: Vec<String>,

/// Compactor node listen address
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[arg(long)]
library_compactor_addr: Option<String>,

/// Prometheus listener for RisingWave metrics
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[arg(long)]
library_prometheus_addr: Option<String>,

/// RisingWave config file path
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[arg(long)]
library_config_path: Option<PathBuf>,
```

### Phase 2: Extend `EmbeddedLibraryConfig`

Modify `nexora-risingwave/src/library.rs`:

```rust
#[derive(Debug, Clone)]
pub enum NodeRole {
    Meta,
    Compute,
    Frontend,
    Compactor,
}

#[derive(Debug, Clone)]
pub enum MetaBackend {
    /// In-memory (single-node only)
    Mem,
    /// Etcd (distributed)
    Etcd { endpoints: Vec<String> },
}

#[derive(Debug, Clone)]
pub struct EmbeddedLibraryConfig {
    /// Node roles to start (empty = all roles for single-node mode)
    pub roles: Vec<NodeRole>,
    
    /// Meta service configuration
    pub meta_addr: Option<String>,
    pub meta_backend: MetaBackend,
    
    /// Frontend configuration
    pub frontend_listen_addr: Option<String>,
    
    /// Compute configuration
    pub compute_addr: Option<String>,
    pub compute_meta_address: Vec<String>,
    
    /// Compactor configuration
    pub compactor_addr: Option<String>,
    
    /// Shared configuration
    pub store_directory: Option<PathBuf>,
    pub in_memory: bool,
    pub config_path: Option<PathBuf>,
    pub prometheus_listener_addr: Option<String>,
}
```

### Phase 3: Implement Distributed Start Logic

Add distributed mode support in `nexora-risingwave/src/library.rs`:

```rust
impl EmbeddedLibrary {
    pub fn start(config: EmbeddedLibraryConfig) -> Result<Self> {
        install_process_globals();
        
        // Determine mode
        let is_single_node = config.roles.is_empty() || 
                            (config.roles.len() > 1 && config.roles.contains(&NodeRole::Meta));
        
        if is_single_node {
            // Current single-node path
            Self::start_single_node(config)
        } else {
            // New distributed path
            Self::start_distributed(config)
        }
    }
    
    fn start_single_node(config: EmbeddedLibraryConfig) -> Result<Self> {
        // Existing implementation
        ...
    }
    
    fn start_distributed(config: EmbeddedLibraryConfig) -> Result<Self> {
        // Build StandaloneOpts based on roles
        let standalone_opts = build_standalone_opts(&config)?;
        
        let shutdown = CancellationToken::new();
        let task_token = shutdown.clone();
        let task = tokio::spawn(async move {
            standalone(standalone_opts, task_token).await;
        });
        
        Ok(Self {
            shutdown,
            task,
            frontend_listen_addr: config.frontend_listen_addr
                .unwrap_or_else(|| "127.0.0.1:4566".to_string()),
        })
    }
}

fn build_standalone_opts(config: &EmbeddedLibraryConfig) -> Result<ParsedStandaloneOpts> {
    let mut meta_opts = None;
    let mut compute_opts = None;
    let mut frontend_opts = None;
    let mut compactor_opts = None;
    
    for role in &config.roles {
        match role {
            NodeRole::Meta => {
                meta_opts = Some(build_meta_opts(config)?);
            }
            NodeRole::Compute => {
                compute_opts = Some(build_compute_opts(config)?);
            }
            NodeRole::Frontend => {
                frontend_opts = Some(build_frontend_opts(config)?);
            }
            NodeRole::Compactor => {
                compactor_opts = Some(build_compactor_opts(config)?);
            }
        }
    }
    
    Ok(ParsedStandaloneOpts {
        meta_opts,
        compute_opts,
        frontend_opts,
        compactor_opts,
    })
}
```

### Phase 4: Configuration File Support

Add TOML configuration support in `nexora.toml`:

```toml
[event_streaming.library]
# Node roles to start (meta, compute, frontend, compactor)
# Empty = single-node mode (all roles)
roles = ["meta", "frontend", "compute"]

# Meta service configuration
meta_addr = "192.168.1.10:5690"
meta_backend = "etcd"  # "mem" or "etcd"
meta_etcd_endpoints = [
    "http://192.168.1.10:2379",
    "http://192.168.1.11:2379",
    "http://192.168.1.12:2379"
]

# Frontend configuration
frontend_addr = "192.168.1.10:4566"

# Compute configuration
compute_addr = "192.168.1.10:5688"
# Meta addresses for compute to connect (required in distributed mode)
compute_meta_address = [
    "http://192.168.1.10:5690",
    "http://192.168.1.11:5690",
    "http://192.168.1.12:5690"
]

# Compactor configuration (optional)
compactor_addr = "192.168.1.10:6660"

# Shared configuration
data_dir = "./nexora-data/event-streaming"
prometheus_addr = "0.0.0.0:1250"
config_path = "./risingwave.toml"
```

## Testing

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_parse_node_roles() {
        let roles = vec!["meta".to_string(), "compute".to_string()];
        let parsed = parse_node_roles(&roles).unwrap();
        assert_eq!(parsed.len(), 2);
        assert!(parsed.contains(&NodeRole::Meta));
        assert!(parsed.contains(&NodeRole::Compute));
    }
    
    #[test]
    fn test_single_node_config() {
        let config = EmbeddedLibraryConfig::new()
            .with_frontend_listen_addr("127.0.0.1:4566")
            .in_memory();
        
        assert!(config.roles.is_empty());  // Single-node
        assert_eq!(config.meta_backend, MetaBackend::Mem);
    }
    
    #[test]
    fn test_distributed_config() {
        let config = EmbeddedLibraryConfig::new()
            .with_roles(vec![NodeRole::Meta, NodeRole::Frontend])
            .with_meta_backend(MetaBackend::Etcd {
                endpoints: vec!["http://etcd:2379".to_string()]
            });
        
        assert_eq!(config.roles.len(), 2);
        assert!(matches!(config.meta_backend, MetaBackend::Etcd { .. }));
    }
}
```

### Integration Tests

```bash
# Test 1: Start single-node library mode
cargo test --features event-streaming,library test_single_node_library

# Test 2: Start distributed meta node
cargo test --features event-streaming,library test_distributed_meta_node

# Test 3: Start distributed compute node
cargo test --features event-streaming,library test_distributed_compute_node

# Test 4: Full 3-node cluster
cargo test --features event-streaming,library test_three_node_cluster
```

## Migration Path

### From Current Single-Node

No changes required. Default behavior remains single-node:

```bash
# Works exactly as before
./nexora --enable-event-streaming --library-event-streaming
```

### To Distributed Mode

Add role parameter:

```bash
# Explicit single-node (all roles)
./nexora --enable-event-streaming --library-event-streaming \
  --library-event-streaming-role all

# Distributed (specific roles)
./nexora --enable-event-streaming --library-event-streaming \
  --library-event-streaming-role meta,frontend,compute \
  --library-meta-backend etcd \
  --library-meta-etcd-endpoints http://etcd:2379
```

## Benefits

1. **No External Binary**: RisingWave compiled into Nexora, no separate `risingwave` process
2. **Flexible Deployment**: Single-node for dev, distributed for production
3. **Unified Management**: One binary, one configuration, one deployment
4. **Resource Efficiency**: Shared runtime, no IPC overhead
5. **Simplified Operations**: No version mismatch, easier debugging

## Limitations

1. **Memory Overhead**: All RisingWave code linked even if not used
2. **Single Binary Size**: ~100MB+ (vs ~40MB without RisingWave)
3. **Rust Toolchain**: Requires nightly (for RisingWave compilation)

## Next Steps

1. Implement Phase 1: CLI parameters (1 day)
2. Implement Phase 2: Config structs (1 day)
3. Implement Phase 3: Distributed logic (2 days)
4. Implement Phase 4: TOML config (1 day)
5. Testing and documentation (2 days)

Total estimate: **1 week**
