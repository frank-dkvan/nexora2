# CLI Parameter Simplification for Event Streaming

## Problem Statement

Current CLI parameters for Event Streaming are too complex and obscure:

```bash
# Current (complex)
./nexora \
  --enable-event-streaming \
  --library-event-streaming \
  --event-streaming-meta-addr 127.0.0.1:5690 \
  --event-streaming-frontend-addr 127.0.0.1:4566 \
  --library-meta-backend etcd \
  --library-meta-etcd-endpoints http://etcd:2379
```

**Issues:**
1. Too many flags (5+ flags just to enable)
2. Redundant prefixes (`event-streaming-`, `library-`)
3. Not intuitive (hard to remember)
4. Mixing concerns (enable + mode + addresses)
5. No clear distinction between modes

## Proposed Simplification

### Design Principles

1. **Profile-based**: Use deployment profiles instead of individual flags
2. **Sensible defaults**: Zero-config for development
3. **Progressive disclosure**: Simple for basic use, detailed for advanced
4. **Single source of truth**: Config file first, CLI flags for overrides only

### Simplified CLI

#### Single Flag for Most Use Cases

```bash
# Development (single-node, in-memory)
./nexora --event-streams dev

# Production (distributed, persistent)
./nexora --event-streams production

# Custom (from config file)
./nexora --event-streams config:nexora.toml
```

#### Profile Definitions

**Built-in Profiles:**

```yaml
# Profile: dev (default when --event-streams is used without argument)
dev:
  mode: library          # Embedded RisingWave
  deployment: single     # All components in one process
  storage: memory        # In-memory only
  frontend: 127.0.0.1:4566
  meta: 127.0.0.1:5690
  
# Profile: production
production:
  mode: library
  deployment: distributed
  storage: persistent
  backend: etcd          # Requires external etcd
  data_dir: ./nexora-data/event-streaming
  
# Profile: external
external:
  mode: client           # Connect to external RisingWave cluster
  meta: ${EVENT_STREAMING_META}     # From env var
  frontend: ${EVENT_STREAMING_FRONTEND}
```

### Comparison: Before vs After

#### Development Mode

**Before (7 flags):**
```bash
./nexora \
  --enable-event-streaming \
  --library-event-streaming \
  --event-streaming-meta-addr 127.0.0.1:5690 \
  --event-streaming-frontend-addr 127.0.0.1:4566 \
  --library-meta-backend mem \
  --rocksdb-path ./data \
  --no-rocksdb  # In-memory
```

**After (1 flag):**
```bash
./nexora --event-streams dev
# or just
./nexora --event-streams
```

#### Production Distributed Mode

**Before (9+ flags):**
```bash
./nexora \
  --enable-event-streaming \
  --library-event-streaming \
  --library-event-streaming-role meta,frontend,compute \
  --event-streaming-meta-addr 192.168.1.10:5690 \
  --event-streaming-frontend-addr 192.168.1.10:4566 \
  --library-compute-addr 192.168.1.10:5688 \
  --library-meta-backend etcd \
  --library-meta-etcd-endpoints http://etcd1:2379,http://etcd2:2379,http://etcd3:2379 \
  --rocksdb-path /data/nexora
```

**After (1 flag + config file):**
```bash
./nexora --event-streams production
```

With `nexora.toml`:
```toml
[event_streaming.production]
mode = "library"
deployment = "distributed"
roles = ["meta", "frontend", "compute"]

[event_streaming.production.addresses]
meta = "192.168.1.10:5690"
frontend = "192.168.1.10:4566"
compute = "192.168.1.10:5688"

[event_streaming.production.backend]
type = "etcd"
endpoints = [
    "http://etcd1:2379",
    "http://etcd2:2379",
    "http://etcd3:2379"
]
```

#### External RisingWave Cluster

**Before (4 flags):**
```bash
./nexora \
  --enable-event-streaming \
  --event-streaming-meta-addr external-rw.example.com:5690 \
  --event-streaming-frontend-addr external-rw.example.com:4566
```

**After (1 flag):**
```bash
./nexora --event-streams external
```

Or with inline override:
```bash
./nexora --event-streams "external:meta=rw.example.com:5690,frontend=rw.example.com:4566"
```

### Implementation Plan

#### Phase 1: Add Profile Support (Keep Old Flags)

Add new `--event-streams` flag alongside existing flags:

```rust
/// Event Streaming deployment profile (dev|production|external|config:PATH)
#[arg(long, value_name = "PROFILE")]
event_streams: Option<String>,
```

**Backward compatibility:** Old flags still work, new flag takes precedence.

#### Phase 2: Profile Parser

```rust
#[derive(Debug, Clone)]
pub enum EventStreamingProfile {
    Dev,
    Production,
    External,
    Custom(PathBuf),
}

impl EventStreamingProfile {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "dev" | "" => Ok(Self::Dev),
            "production" | "prod" => Ok(Self::Production),
            "external" | "ext" => Ok(Self::External),
            s if s.starts_with("config:") => {
                let path = s.strip_prefix("config:").unwrap();
                Ok(Self::Custom(PathBuf::from(path)))
            }
            _ => Err(anyhow!("Invalid profile: {}", s)),
        }
    }
    
    pub fn to_config(&self, base_config: &Config) -> EventStreamingConfig {
        match self {
            Self::Dev => EventStreamingConfig {
                enabled: true,
                mode: Mode::Library,
                deployment: Deployment::SingleNode,
                meta_addr: "127.0.0.1:5690".to_string(),
                frontend_addr: "127.0.0.1:4566".to_string(),
                backend: Backend::Memory,
                ..Default::default()
            },
            Self::Production => {
                // Load from [event_streaming.production] in nexora.toml
                base_config.event_streaming.production.clone()
            },
            Self::External => {
                // Load from [event_streaming.external] in nexora.toml
                // or from environment variables
                EventStreamingConfig {
                    enabled: true,
                    mode: Mode::Client,
                    meta_addr: env::var("EVENT_STREAMING_META")
                        .unwrap_or_else(|_| "localhost:5690".to_string()),
                    frontend_addr: env::var("EVENT_STREAMING_FRONTEND")
                        .unwrap_or_else(|_| "localhost:4566".to_string()),
                    ..Default::default()
                }
            },
            Self::Custom(path) => {
                // Load custom config file
                load_custom_config(path)
            }
        }
    }
}
```

#### Phase 3: Config File Schema

Update `nexora.toml.example`:

```toml
# =============================================================================
# Event Streaming Configuration
# =============================================================================
# Event Streaming provides SQL-based stream processing: continuous queries,
# materialized views, and stream joins over event streams.
#
# Three deployment modes:
#   1. library/single    - All components in-process (dev/testing)
#   2. library/distributed - Distributed cluster (production)
#   3. client            - Connect to external RisingWave cluster
# =============================================================================

# -----------------------------------------------------------------------------
# Quick Start Profiles
# -----------------------------------------------------------------------------
# Use with: ./nexora --event-streams <profile>

[event_streaming.profiles.dev]
# Development profile (default)
# Single-node, in-memory, auto-starts on 127.0.0.1
enabled = true
mode = "library"
deployment = "single"
storage = "memory"

[event_streaming.profiles.production]
# Production profile
# Distributed cluster with persistent storage
enabled = true
mode = "library"
deployment = "distributed"
storage = "persistent"
data_dir = "./nexora-data/event-streaming"

[event_streaming.profiles.production.cluster]
# Roles to run on this node (meta, compute, frontend, compactor)
roles = ["meta", "frontend", "compute"]

[event_streaming.profiles.production.addresses]
# Service listen addresses
meta = "0.0.0.0:5690"
frontend = "0.0.0.0:4566"
compute = "0.0.0.0:5688"

[event_streaming.profiles.production.backend]
# Meta backend: etcd for distributed mode
type = "etcd"
endpoints = [
    "http://etcd1:2379",
    "http://etcd2:2379",
    "http://etcd3:2379"
]

[event_streaming.profiles.external]
# External RisingWave cluster
enabled = true
mode = "client"
meta = "${EVENT_STREAMING_META:-localhost:5690}"
frontend = "${EVENT_STREAMING_FRONTEND:-localhost:4566}"

# -----------------------------------------------------------------------------
# Advanced: Manual Configuration (overrides profiles)
# -----------------------------------------------------------------------------
# [event_streaming]
# enabled = true
# mode = "library"  # library or client
# meta_addr = "127.0.0.1:5690"
# frontend_addr = "127.0.0.1:4566"
# ...
```

#### Phase 4: Deprecation Path

1. **v2.1** (Current): Old flags work, new `--event-streams` added
2. **v2.2** (3 months): Both work, deprecation warning for old flags
3. **v2.3** (6 months): Old flags removed

Deprecation warning:
```
WARNING: Flag --enable-event-streaming is deprecated.
         Use --event-streams dev instead.
         Old flags will be removed in v2.3.
```

### Enhanced Help Text

```bash
$ ./nexora --help

Event Streaming Options:
  --event-streams <PROFILE>
          Event Streaming deployment profile
          
          Profiles:
            dev         Development (single-node, in-memory)
            production  Production (distributed, persistent)
            external    External RisingWave cluster
            config:PATH Custom config file
          
          [default: disabled]
          
          Examples:
            --event-streams dev
            --event-streams production
            --event-streams config:./my-config.toml
            --event-streams "external:meta=rw.example.com:5690"

Advanced Event Streaming Options (override profile):
  --event-streams-meta <ADDR>
          Override meta service address
          
  --event-streams-frontend <ADDR>
          Override frontend service address
          
  --event-streams-backend <TYPE>
          Override meta backend (mem|etcd)
```

### Migration Guide

#### For Users

**Simple cases (dev/testing):**
```bash
# Old
./nexora --enable-event-streaming --library-event-streaming

# New
./nexora --event-streams dev
```

**Production (distributed):**
```bash
# Old
./nexora \
  --enable-event-streaming \
  --library-event-streaming \
  --library-event-streaming-role meta,frontend,compute \
  ... (8 more flags)

# New
./nexora --event-streams production
```

Then edit `nexora.toml`:
```toml
[event_streaming.profiles.production]
mode = "library"
deployment = "distributed"
roles = ["meta", "frontend", "compute"]
...
```

#### For Operators

**Docker Compose:**
```yaml
# Before
services:
  nexora:
    command: >
      nexora
      --enable-event-streaming
      --library-event-streaming
      --event-streaming-meta-addr ${META_ADDR}
      --event-streaming-frontend-addr ${FRONTEND_ADDR}
      ...

# After
services:
  nexora:
    command: nexora --event-streams production
    volumes:
      - ./nexora.toml:/etc/nexora/nexora.toml
    environment:
      - NEXORA_CONFIG=/etc/nexora/nexora.toml
```

**Kubernetes:**
```yaml
# Before
args:
  - --enable-event-streaming
  - --library-event-streaming
  - --library-event-streaming-role=meta,frontend,compute
  - --event-streaming-meta-addr=$(POD_IP):5690
  ... (many more)

# After
args:
  - --event-streams=production
env:
  - name: EVENT_STREAMING_META
    value: "$(POD_IP):5690"
  - name: EVENT_STREAMING_FRONTEND
    value: "$(POD_IP):4566"
volumeMounts:
  - name: config
    mountPath: /etc/nexora
```

### Summary

**Benefits:**
1. **90% reduction in CLI flags** for common cases
2. **Profile-based** approach familiar to developers
3. **Config-file first** for production deployments
4. **Environment variable** support for cloud-native
5. **Backward compatible** with deprecation path

**Implementation effort:** ~3 days
- Day 1: Profile parser + config schema
- Day 2: Integration with existing code
- Day 3: Tests + documentation

**Next steps after single-node testing:**
1. Implement profile-based CLI (this simplification)
2. Then implement distributed mode
3. Both use the same simplified interface
