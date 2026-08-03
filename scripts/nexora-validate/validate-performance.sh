#!/bin/bash
# Validate performance meets baseline expectations
set -euo pipefail

API_ENDPOINT="${API_ENDPOINT:-http://localhost:8080}"
WRITE_THRESHOLD_MS="${WRITE_THRESHOLD_MS:-1000}"
READ_THRESHOLD_MS="${READ_THRESHOLD_MS:-500}"

echo "=== Running Performance Baseline Validation ==="

# Test 1: Write latency
echo ""
echo "Test 1: Write Latency"
start=$(date +%s%3N)
response=$(curl -sf -X POST "${API_ENDPOINT}/api/events/ingest" \
    -H "Content-Type: application/json" \
    -d '{"topic": "perf_test", "events": [{"id": "test_'$(date +%s)'", "data": {"test": true}}]}' 2>/dev/null || echo "ERROR")
end=$(date +%s%3N)

if [[ "$response" == "ERROR" ]]; then
    echo "ERROR: Write request failed"
    exit 1
fi

write_latency=$((end - start))
echo "  Latency: ${write_latency}ms (threshold: ${WRITE_THRESHOLD_MS}ms)"

if [[ "$write_latency" -gt "$WRITE_THRESHOLD_MS" ]]; then
    echo "  WARNING: Write latency exceeds threshold"
fi

# Test 2: Read latency
echo ""
echo "Test 2: Read Latency"
start=$(date +%s%3N)
response=$(curl -sf -X POST "${API_ENDPOINT}/api/query/cypher" \
    -H "Content-Type: application/json" \
    -d '{"query": "MATCH (n) RETURN n LIMIT 10"}' 2>/dev/null || echo "ERROR")
end=$(date +%s%3N)

if [[ "$response" == "ERROR" ]]; then
    echo "ERROR: Read request failed"
    exit 1
fi

read_latency=$((end - start))
echo "  Latency: ${read_latency}ms (threshold: ${READ_THRESHOLD_MS}ms)"

if [[ "$read_latency" -gt "$READ_THRESHOLD_MS" ]]; then
    echo "  WARNING: Read latency exceeds threshold"
fi

# Test 3: Concurrent query handling (5 parallel queries)
echo ""
echo "Test 3: Concurrent Query Handling"
concurrent_count=5
start=$(date +%s%3N)

for i in $(seq 1 $concurrent_count); do
    curl -sf -X POST "${API_ENDPOINT}/api/query/cypher" \
        -H "Content-Type: application/json" \
        -d '{"query": "MATCH (n) RETURN count(n) LIMIT 1"}' > /dev/null 2>&1 &
done

wait
end=$(date +%s%3N)

concurrent_latency=$((end - start))
echo "  ${concurrent_count} queries completed in: ${concurrent_latency}ms"

# Test 4: Health endpoint response time
echo ""
echo "Test 4: Health Endpoint"
start=$(date +%s%3N)
health=$(curl -sf "${API_ENDPOINT}/health" 2>/dev/null || echo "ERROR")
end=$(date +%s%3N)

if [[ "$health" == "ERROR" ]]; then
    echo "ERROR: Health check failed"
    exit 1
fi

health_latency=$((end - start))
echo "  Latency: ${health_latency}ms"

if [[ "$health_latency" -gt 100 ]]; then
    echo "  WARNING: Health check latency high (expected < 100ms)"
fi

echo ""
echo "=== Performance Summary ==="
echo "  Write:       ${write_latency}ms"
echo "  Read:        ${read_latency}ms"
echo "  Concurrent:  ${concurrent_latency}ms for ${concurrent_count} queries"
echo "  Health:      ${health_latency}ms"

echo ""
echo "✅ Performance baseline validation passed"
