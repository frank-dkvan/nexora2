#!/bin/bash
# Check RisingWave build progress

OUTPUT_FILE="/private/tmp/claude-501/-Users-frank-aiCoding-nexora2/ec55058c-59ee-4d8b-be8e-96ee64475cb0/tasks/bsgid1gf2.output"

echo "=== RisingWave Build Progress ==="
echo ""
echo "Total crates compiled:"
grep "Compiling" "$OUTPUT_FILE" 2>/dev/null | wc -l | tr -d ' '
echo ""
echo "RisingWave-specific crates:"
grep "Compiling risingwave" "$OUTPUT_FILE" 2>/dev/null | wc -l | tr -d ' '
echo ""
echo "Latest 10 compilations:"
tail -10 "$OUTPUT_FILE" 2>/dev/null
echo ""
echo "Any errors?"
grep -c "^error" "$OUTPUT_FILE" 2>/dev/null || echo "0"
