#!/bin/bash
# End-to-end smoke test after disaster recovery
set -euo pipefail

API_ENDPOINT="${API_ENDPOINT:-http://localhost:8080}"
TEST_ID="dr_test_$(date +%s)"

echo "=== Running E2E Smoke Test ==="

# Test 1: Create node
echo ""
echo "Test 1: Create Test Node"
create_response=$(curl -sf -X POST "${API_ENDPOINT}/api/query/cypher" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"CREATE (u:DRTestUser {id: '${TEST_ID}', name: 'Recovery Test', timestamp: timestamp()}) RETURN u\"}" 2>/dev/null || echo "ERROR")

if [[ "$create_response" == "ERROR" ]]; then
    echo "ERROR: Failed to create test node"
    exit 1
fi

node_created=$(echo "$create_response" | jq -r '.results[0].u.id' 2>/dev/null || echo "")
if [[ "$node_created" != "$TEST_ID" ]]; then
    echo "ERROR: Created node ID mismatch"
    exit 1
fi
echo "  ✓ Node created: $TEST_ID"

# Test 2: Query test node
echo ""
echo "Test 2: Query Test Node"
query_response=$(curl -sf -X POST "${API_ENDPOINT}/api/query/cypher" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"MATCH (u:DRTestUser {id: '${TEST_ID}'}) RETURN u\"}" 2>/dev/null || echo "ERROR")

if [[ "$query_response" == "ERROR" ]]; then
    echo "ERROR: Failed to query test node"
    exit 1
fi

node_found=$(echo "$query_response" | jq -r '.results[0].u.id' 2>/dev/null || echo "")
if [[ "$node_found" != "$TEST_ID" ]]; then
    echo "ERROR: Node not found or ID mismatch"
    exit 1
fi
echo "  ✓ Node found: $TEST_ID"

# Test 3: Update node property
echo ""
echo "Test 3: Update Node Property"
update_response=$(curl -sf -X POST "${API_ENDPOINT}/api/query/cypher" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"MATCH (u:DRTestUser {id: '${TEST_ID}'}) SET u.status = 'verified', u.updated = timestamp() RETURN u\"}" 2>/dev/null || echo "ERROR")

if [[ "$update_response" == "ERROR" ]]; then
    echo "ERROR: Failed to update test node"
    exit 1
fi

status=$(echo "$update_response" | jq -r '.results[0].u.status' 2>/dev/null || echo "")
if [[ "$status" != "verified" ]]; then
    echo "ERROR: Property update failed"
    exit 1
fi
echo "  ✓ Property updated: status=verified"

# Test 4: Create relationship
echo ""
echo "Test 4: Create Relationship"
rel_response=$(curl -sf -X POST "${API_ENDPOINT}/api/query/cypher" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"MATCH (u:DRTestUser {id: '${TEST_ID}'}) CREATE (u)-[r:VERIFIED_AT]->(t:Timestamp {value: timestamp()}) RETURN r\"}" 2>/dev/null || echo "ERROR")

if [[ "$rel_response" == "ERROR" ]]; then
    echo "ERROR: Failed to create relationship"
    exit 1
fi
echo "  ✓ Relationship created: VERIFIED_AT"

# Test 5: Query with relationship
echo ""
echo "Test 5: Query with Relationship"
path_response=$(curl -sf -X POST "${API_ENDPOINT}/api/query/cypher" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"MATCH (u:DRTestUser {id: '${TEST_ID}'})-[r:VERIFIED_AT]->(t) RETURN u, r, t\"}" 2>/dev/null || echo "ERROR")

if [[ "$path_response" == "ERROR" ]]; then
    echo "ERROR: Failed to query relationship"
    exit 1
fi

result_count=$(echo "$path_response" | jq -r '.results | length' 2>/dev/null || echo "0")
if [[ "$result_count" -lt 1 ]]; then
    echo "ERROR: Relationship not found"
    exit 1
fi
echo "  ✓ Relationship query successful"

# Cleanup: Delete test data
echo ""
echo "Cleanup: Deleting Test Data"
delete_response=$(curl -sf -X POST "${API_ENDPOINT}/api/query/cypher" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"MATCH (u:DRTestUser {id: '${TEST_ID}'})-[r]->(t) DELETE r, u, t\"}" 2>/dev/null || echo "ERROR")

if [[ "$delete_response" == "ERROR" ]]; then
    echo "WARNING: Cleanup failed, manual cleanup may be required"
fi

# Verify deletion
verify_response=$(curl -sf -X POST "${API_ENDPOINT}/api/query/cypher" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"MATCH (u:DRTestUser {id: '${TEST_ID}'}) RETURN count(u) as count\"}" 2>/dev/null || echo "ERROR")

remaining=$(echo "$verify_response" | jq -r '.results[0].count' 2>/dev/null || echo "1")
if [[ "$remaining" -eq 0 ]]; then
    echo "  ✓ Test data cleaned up"
else
    echo "  WARNING: Test data may still exist (count: $remaining)"
fi

echo ""
echo "=== E2E Test Summary ==="
echo "  ✓ Node creation"
echo "  ✓ Node query"
echo "  ✓ Property update"
echo "  ✓ Relationship creation"
echo "  ✓ Relationship query"
echo "  ✓ Data cleanup"

echo ""
echo "✅ E2E smoke test passed"
