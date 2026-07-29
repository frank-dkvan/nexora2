#!/usr/bin/env bash
set -euo pipefail

# Check RisingWave Build Progress
# Shows current compilation status and any errors

LOG_FILE="${1:-/tmp/risingwave_rebuild2.log}"

if [ ! -f "$LOG_FILE" ]; then
    echo "Error: Log file $LOG_FILE not found"
    exit 1
fi

echo "=== RisingWave Build Status ==="
echo ""

# Count compiled crates
COMPILED=$(grep -c "Compiling" "$LOG_FILE" 2>/dev/null || echo "0")
echo "Crates compiled: $COMPILED"

# Check if finished
if grep -q "Finished" "$LOG_FILE" 2>/dev/null; then
    echo "Status: ✅ BUILD COMPLETE"
    grep "Finished" "$LOG_FILE" | tail -1
    exit 0
fi

# Check for errors
ERROR_COUNT=$(grep -c "^error\[" "$LOG_FILE" 2>/dev/null || echo "0")
if [ "$ERROR_COUNT" -gt 0 ]; then
    echo "Status: ❌ BUILD FAILED"
    echo "Errors found: $ERROR_COUNT"
    echo ""
    echo "=== First 3 Errors ==="
    grep -A 5 "^error\[" "$LOG_FILE" | head -20
else
    echo "Status: 🔄 COMPILING..."
    echo ""
    echo "=== Last 5 Compiled Crates ==="
    grep "Compiling" "$LOG_FILE" | tail -5
fi

echo ""
echo "=== Warnings ==="
WARNING_COUNT=$(grep -c "^warning:" "$LOG_FILE" 2>/dev/null || echo "0")
echo "Total warnings: $WARNING_COUNT"

if [ "$WARNING_COUNT" -gt 0 ]; then
    echo ""
    echo "Sample warnings:"
    grep "^warning:" "$LOG_FILE" | head -3
fi
