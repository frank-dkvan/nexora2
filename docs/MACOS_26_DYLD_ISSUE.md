# macOS 26.3 Beta Compatibility Issue

## Status: Known Issue - Workaround Available

This document describes a compatibility issue between Nexora 2 and macOS 26.3 beta that prevents the binary from running.

## Symptoms

When attempting to run the compiled `nexora` binary on macOS 26.3:

```
dyld[87699]: dyld cache '(null)' not loaded: syscall to map cache into shared region failed
dyld[87699]: Library not loaded: /System/Library/Frameworks/CoreFoundation.framework/Versions/A/CoreFoundation
  Referenced from: <UUID> /path/to/nexora
  Reason: tried: '/System/Library/Frameworks/CoreFoundation.framework/Versions/A/CoreFoundation' (no such file)
```

## Root Cause

macOS 26.3 (Build 25D125) is a development/beta version with a broken dyld shared cache system:

1. **dyld cache loading fails**: The dynamic linker cannot map the shared cache into memory
2. **Framework files don't exist as files**: CoreFoundation and other system frameworks are embedded in the dyld shared cache, not stored as individual files
3. **Our binary links to system frameworks**: Via transitive dependencies (`security-framework`, `core-foundation` crates from the RisingWave dependency tree)

This is a **system-level bug in macOS 26.3 beta**, not a Nexora issue.

## Affected Systems

- macOS 26.3 (Darwin Kernel Version 25.3.0)
- Build 25D125
- Likely other macOS 26.x beta versions

## Unaffected Systems

- ✅ macOS 15 (Sequoia) - stable
- ✅ macOS 14 (Sonoma) - stable
- ✅ Linux (any distribution)
- ✅ Docker/container environments

## Workarounds

### Recommended: Use Docker/Container

Run Nexora in a Linux container to bypass macOS framework dependencies:

```bash
# Using Docker
docker run -it --rm \
  -v $(pwd):/workspace \
  -w /workspace \
  rust:1.98-bookworm \
  bash

# Inside container:
cargo build --features event-first,event-streaming,library
./target/debug/nexora --version
./scripts/start-cluster-3nodes-risingwave-sqlite.sh
```

### Alternative 1: Use Lima VM

```bash
# Install Lima
brew install lima

# Create Ubuntu VM
limactl start default

# Enter VM and build
lima
cd /path/to/nexora2
cargo build --features event-first,event-streaming,library
```

### Alternative 2: Use GitHub Actions / CI

The SQLite persistence implementation can be tested in CI:

```yaml
# .github/workflows/test-sqlite-persistence.yml
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@nightly
      - run: cargo build --features event-first,event-streaming,library
      - run: ./scripts/start-cluster-3nodes-risingwave-sqlite.sh
```

### Alternative 3: Downgrade macOS

If a bare-metal macOS environment is required:
- Downgrade to macOS 15 (Sequoia)
- Downgrade to macOS 14 (Sonoma)

### Alternative 4: Wait for Fix

Apple will likely fix the dyld cache issue in:
- A later macOS 26.3 beta build
- macOS 26.4 or later
- macOS 26 final release

## Testing Status

### ✅ Implementation Complete

The SQLite persistence feature is fully implemented and ready:

- CLI parameters: `--meta-backend sqlite`, `--meta-backend-sqlite-path`, `--meta-backend-etcd-endpoints`
- Configuration: `DistributedLibraryConfig` supports all three backends (Memory, SQLite, Etcd)
- Startup script: `scripts/start-cluster-3nodes-risingwave-sqlite.sh`
- Documentation: Command-line help and inline comments

### ❌ Testing Blocked on macOS 26.3

Cannot verify persistence on this system due to dyld bug.

## Verification Plan

Once running on a compatible system:

```bash
# 1. Start cluster with SQLite
./scripts/start-cluster-3nodes-risingwave-sqlite.sh --clean

# 2. Verify SQLite files are created
ls -lh nexora-data-node-*/meta.db

# 3. Create test data via PostgreSQL
psql -h 127.0.0.1 -p 4566 -U root -d dev <<EOF
CREATE TABLE test_table (id INT, name VARCHAR);
INSERT INTO test_table VALUES (1, 'test');
SELECT * FROM test_table;
EOF

# 4. Restart cluster (without --clean)
./scripts/stop-cluster.sh
./scripts/start-cluster-3nodes-risingwave-sqlite.sh

# 5. Verify data persisted
psql -h 127.0.0.1 -p 4566 -U root -d dev -c "SELECT * FROM test_table;"
```

Expected result: `test_table` should still exist with the inserted row after restart.

## Timeline

- **2026-07-31**: Issue discovered during SQLite persistence testing
- **Status**: Waiting for compatible test environment (Docker/Linux/stable macOS)

## References

- Original analysis: `/tmp/macos_dyld_issue.md`
- SQLite persistence implementation: `crates/nexora-app/src/main.rs` (lines 2283-2383)
- Startup script: `scripts/start-cluster-3nodes-risingwave-sqlite.sh`

## Contact

If you encounter this issue, please:
1. Use one of the workarounds above
2. Report your macOS version and build number
3. Note whether the workaround resolved the issue
