#!/bin/bash
# Validate Nexora cluster health after recovery
set -euo pipefail

NODES="${NEXORA_NODES:-node1 node2 node3}"
ADMIN_PORT="${ADMIN_PORT:-8080}"

echo "=== Checking Raft Cluster Health ==="

declare -A node_states
leader_count=0

for node in $NODES; do
    if ! status=$(curl -sf "http://${node}:${ADMIN_PORT}/admin/raft/status" 2>/dev/null); then
        echo "ERROR: Cannot reach node $node"
        exit 1
    fi

    state=$(echo "$status" | jq -r '.state')
    node_states[$node]=$state
    echo "$node: $state"

    if [[ "$state" == "Leader" ]]; then
        ((leader_count++))
    elif [[ "$state" != "Follower" ]]; then
        echo "ERROR: $node in unexpected state: $state"
        exit 1
    fi
done

echo ""
echo "=== Verifying Leader Election ==="
if [[ "$leader_count" -ne 1 ]]; then
    echo "ERROR: Expected exactly 1 leader, found $leader_count"
    exit 1
fi

echo "Leader count: $leader_count (OK)"

echo ""
echo "=== Checking Node Connectivity ==="
for node in $NODES; do
    if ! curl -sf "http://${node}:${ADMIN_PORT}/health" > /dev/null; then
        echo "ERROR: Health check failed for $node"
        exit 1
    fi
    echo "$node: healthy"
done

echo ""
echo "✅ Cluster health validation passed"
