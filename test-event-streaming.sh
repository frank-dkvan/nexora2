#!/bin/bash
# Test Nexora Event Streaming endpoints
# Run this after Nexora is started with library mode

BASE_URL="http://127.0.0.1:8080"

echo "=== Testing Nexora Event Streaming Endpoints ==="
echo ""

# 1. Check basic health
echo "1. Health check:"
curl -s ${BASE_URL}/api/health | jq . || echo "Failed"
echo ""

# 2. Check Event Streaming status
echo "2. Event Streaming status:"
curl -s ${BASE_URL}/api/event-streaming/status | jq . || echo "Failed"
echo ""

# 3. Create a test source (simple demo - will fail without real Kafka)
echo "3. Create test materialized view (demo SQL):"
curl -s -X POST ${BASE_URL}/api/event-streaming/ddl \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "CREATE MATERIALIZED VIEW test_mv AS SELECT 1 as id, '\''hello'\'' as name"
  }' | jq . || echo "Failed (expected if no source)"
echo ""

# 4. List sources
echo "4. List sources:"
curl -s ${BASE_URL}/api/event-streaming/sources | jq . || echo "Failed"
echo ""

# 5. List materialized views
echo "5. List materialized views:"
curl -s ${BASE_URL}/api/event-streaming/materialized_views | jq . || echo "Failed"
echo ""

# 6. Query the test MV
echo "6. Query test materialized view:"
curl -s -X POST ${BASE_URL}/api/event-streaming/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM test_mv"
  }' | jq . || echo "Failed"
echo ""

echo "=== Test complete ==="
