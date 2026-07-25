#!/usr/bin/env bash
# =============================================================================
# Benchmark Runner for nexora Graph Database
#
# Runs all nexora-core benchmarks, collects results, and generates a
# Markdown report with results tables.
#
# Usage:
#   ./benches/run_benchmarks.sh           # run all benchmarks
#   ./benches/run_benchmarks.sh --quick   # run with reduced sample sizes
#
# Prerequisites:
#   - cargo (Rust toolchain)
#   - python3 (for result parsing and report generation)
# =============================================================================

set -euo pipefail

# ---- Configuration ----------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CRATE_DIR="$PROJECT_ROOT/crates/nexora-core"
REPORT_DIR="$PROJECT_ROOT/target/bench-reports"
REPORT_FILE="$REPORT_DIR/benchmark_report.md"
CRITERION_DIR="$PROJECT_ROOT/target/criterion"
COMPARISON_JSON="$PROJECT_ROOT/target/comparison_results.json"

QUICK_MODE=false
if [[ "${1:-}" == "--quick" ]]; then
    QUICK_MODE=true
fi

# ---- Helpers ----------------------------------------------------------------

log() {
    echo "[bench-runner] $*" >&2
}

# ---- Sanity checks ----------------------------------------------------------

if ! command -v cargo &>/dev/null; then
    echo "ERROR: cargo not found. Please install the Rust toolchain." >&2
    exit 1
fi

if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 not found. Please install Python 3." >&2
    exit 1
fi

mkdir -p "$REPORT_DIR"

# ---- Run benchmarks ---------------------------------------------------------

cd "$PROJECT_ROOT"

BENCHES=(
    "throughput"
    "concurrent_writes"
    "tpc_graph"
    "comparison"
)

# Also include optional benches that may exist
OPTIONAL_BENCHES=(
    "query_benchmark"
    "index_benchmark"
    "label_benchmark"
    "wal_sync_benchmark"
)

for bench in "${BENCHES[@]}"; do
    log "Running benchmark: $bench"
    if $QUICK_MODE; then
        # Use --sample-size for quicker runs (criterion accepts this)
        cargo bench -p nexora-core --bench "$bench" -- --sample-size 10 2>&1 || {
            log "WARNING: benchmark '$bench' failed, continuing..."
        }
    else
        cargo bench -p nexora-core --bench "$bench" 2>&1 || {
            log "WARNING: benchmark '$bench' failed, continuing..."
        }
    fi
    echo "---"
done

for bench in "${OPTIONAL_BENCHES[@]}"; do
    log "Running optional benchmark: $bench"
    cargo bench -p nexora-core --bench "$bench" 2>&1 || {
        log "WARNING: optional benchmark '$bench' failed or not found, continuing..."
    }
    echo "---"
done

# ---- Generate Markdown report -----------------------------------------------

log "Generating Markdown report..."

python3 "$SCRIPT_DIR/generate_report.py" \
    --criterion-dir "$CRITERION_DIR" \
    --comparison-json "$COMPARISON_JSON" \
    --output "$REPORT_FILE" \
    || {
        log "WARNING: report generation failed. Raw criterion data is in $CRITERION_DIR"
        log "Comparison JSON (if generated) is in $COMPARISON_JSON"
        exit 0
    }

log "Report generated: $REPORT_FILE"
log "Done!"
