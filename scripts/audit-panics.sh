#!/usr/bin/env bash
set -euo pipefail

# P1-1 Panic Audit Script
# Categorize all panic!/expect/unwrap instances into:
# 1. Test code (acceptable)
# 2. Unreachable code (convert to unreachable!())
# 3. Hotpath code (MUST fix to Result)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_FILE="$PROJECT_ROOT/panic_audit.txt"

cd "$PROJECT_ROOT"

echo "=== Nexora 2.0 Panic Audit ===" > "$OUTPUT_FILE"
echo "Generated: $(date)" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

# Count totals
TOTAL=$(grep -rn "panic!\|\.expect(\|\.unwrap()" crates/ --include="*.rs" | wc -l | xargs)
TEST_CODE=$(grep -rn "panic!\|\.expect(\|\.unwrap()" crates/ --include="*.rs" | grep -E "(tests?/|test\.rs|#\[cfg\(test\)\]|benches?/|examples?/)" | wc -l | xargs)
PRODUCTION=$((TOTAL - TEST_CODE))

echo "📊 Summary:" >> "$OUTPUT_FILE"
echo "  Total instances: $TOTAL" >> "$OUTPUT_FILE"
echo "  Test code: $TEST_CODE (acceptable)" >> "$OUTPUT_FILE"
echo "  Production code: $PRODUCTION (needs review)" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

# Critical hotpaths to audit
echo "🔥 HOTPATH FILES (Priority for fixing):" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

HOTPATHS=(
    "crates/nexora-core/src/graph_service.rs"
    "crates/nexora-core/src/index_optimizer.rs"
    "crates/nexora-cypher/src/executor.rs"
    "crates/nexora-cypher/src/planner.rs"
    "crates/nexora-raft/src/replication.rs"
    "crates/nexora-raft/src/consensus.rs"
    "crates/nexora-eventlog/src/store.rs"
    "crates/nexora-eventlog/src/writer.rs"
    "crates/nexora-stream/src/kafka_source.rs"
    "crates/nexora-app/src/http_server.rs"
)

for file in "${HOTPATHS[@]}"; do
    if [ -f "$PROJECT_ROOT/$file" ]; then
        COUNT=$(grep -n "panic!\|\.expect(\|\.unwrap()" "$file" 2>/dev/null | wc -l | xargs)
        if [ "$COUNT" -gt 0 ]; then
            echo "  ❌ $file: $COUNT instances" >> "$OUTPUT_FILE"
            grep -n "panic!\|\.expect(\|\.unwrap()" "$file" | sed 's/^/      /' >> "$OUTPUT_FILE"
            echo "" >> "$OUTPUT_FILE"
        else
            echo "  ✅ $file: 0 instances" >> "$OUTPUT_FILE"
        fi
    fi
done

echo "" >> "$OUTPUT_FILE"
echo "🔍 ALL PRODUCTION CODE INSTANCES:" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

# Find all non-test instances
grep -rn "panic!\|\.expect(\|\.unwrap()" crates/ --include="*.rs" \
    | grep -v "tests/" \
    | grep -v "test.rs" \
    | grep -v "#\[cfg(test)\]" \
    | grep -v "benches/" \
    | grep -v "examples/" \
    | sort >> "$OUTPUT_FILE"

echo ""
echo "✅ Audit complete. Results written to: $OUTPUT_FILE"
echo ""
echo "📌 Next steps:"
echo "  1. Review hotpath files and convert to Result-based error handling"
echo "  2. Convert unreachable cases to unreachable!() macro"
echo "  3. Keep test code as-is (acceptable)"
