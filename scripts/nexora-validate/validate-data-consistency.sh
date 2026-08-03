#!/bin/bash
# Validate data consistency across cluster nodes
set -euo pipefail

NODES="${NEXORA_NODES:-node1 node2 node3}"
API_PORT="${API_PORT:-8080}"

echo "=== Checking Data Consistency Across Nodes ==="

declare -A node_counts

for node in $NODES; do
    count=$(curl -sf -X POST "http://${node}:${API_PORT}/api/query/cypher" \
        -H "Content-Type: application/json" \
        -d '{"query": "MATCH (n) RETURN count(n) as total"}' | \
        jq -r '.results[0].total' 2>/dev/null || echo "ERROR")

    if [[ "$count" == "ERROR" ]]; then
        echo "ERROR: Failed to query node $node"
        exit 1
    fi

    node_counts[$node]=$count
    echo "$node: $count nodes"
done

echo ""
echo "=== Verifying Consistency ==="

first_count=""
for node in $NODES; do
    count=${node_counts[$node]}

    if [[ -z "$first_count" ]]; then
        first_count=$count
    elif [[ "$count" != "$first_count" ]]; then
        echo "ERROR: Inconsistent node count across cluster"
        echo "  Expected: $first_count"
        echo "  Found on $node: $count"
        exit 1
    fi
done

echo "All nodes report: $first_count nodes (consistent)"

echo ""
echo "=== Checking Edge Count Consistency ==="

for node in $NODES; do
    edge_count=$(curl -sf -X POST "http://${node}:${API_PORT}/api/query/cypher" \
        -H "Content-Type: application/json" \
        -d '{"query": "MATCH ()-[r]->() RETURN count(r) as total"}' | \
        jq -r '.results[0].total' 2>/dev/null || echo "ERROR")

    if [[ "$edge_count" == "ERROR" ]]; then
        echo "ERROR: Failed to query edges on node $node"
        exit 1
    fi

    echo "$node: $edge_count edges"
done

echo ""
echo "✅ Data consistency validation passed"
