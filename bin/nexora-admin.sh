#!/bin/bash
# nexora-admin — Operations CLI for Nexora-RS
# Usage: ./nexora-admin.sh <command> [options]
# Commands: status, slow-queries, backup, reindex, health, nodes, sample

set -euo pipefail

# Configuration
HOST="${NEXORA_HOST:-http://localhost:8080}"
AUTH="${NEXORA_AUTH_SECRET:-}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Helper functions
api_get() {
    local path="$1"
    if [ -n "$AUTH" ]; then
        curl -s -H "Authorization: Bearer $AUTH" "$HOST$path"
    else
        curl -s "$HOST$path"
    fi
}

api_post() {
    local path="$1"
    if [ -n "$AUTH" ]; then
        curl -s -X POST -H "Authorization: Bearer $AUTH" "$HOST$path"
    else
        curl -s -X POST "$HOST$path"
    fi
}

print_json() {
    if command -v python3 &> /dev/null; then
        python3 -m json.tool 2>/dev/null || cat
    else
        cat
    fi
}

# Commands
cmd_status() {
    echo -e "${CYAN}=== Nexora-RS System Status ===${NC}"
    api_get "/api/v2/admin/status" | print_json
    echo
}

cmd_health() {
    echo -e "${CYAN}=== Health Check ===${NC}"
    api_get "/api/v2/health" | print_json
    echo
}

cmd_slow_queries() {
    echo -e "${CYAN}=== Slow Query Analysis ===${NC}"
    api_get "/api/v2/admin/slow-queries" | print_json
    echo
    echo -e "${YELLOW}Note: Slow queries are logged at tracing target 'slow_query'.${NC}"
    echo -e "${YELLOW}Check application logs for detailed slow query entries.${NC}"
    echo
}

cmd_backup() {
    echo -e "${CYAN}=== Triggering Backup ===${NC}"
    api_post "/api/v2/admin/backup" | print_json
    echo
}

cmd_reindex() {
    echo -e "${CYAN}=== Triggering HNSW Reindex ===${NC}"
    api_post "/api/v2/admin/reindex" | print_json
    echo
}

cmd_nodes() {
    echo -e "${CYAN}=== Node Count ===${NC}"
    api_get "/api/v2/admin/status" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    g = data.get('graph', {})
    print(f\"Active nodes: {g.get('active_nodes', 'N/A')}\")
    print(f\"Shard count:  {g.get('shard_count', 'N/A')}\")
    print(f\"Max per shard: {g.get('max_nodes_per_shard', 'N/A')}\")
except: print('Failed to parse response')
" 2>/dev/null || api_get "/api/v2/admin/status"
    echo
}

cmd_sample() {
    local dataset="${1:-}"
    if [ -z "$dataset" ]; then
        echo -e "${CYAN}Available datasets:${NC}"
        api_get "/api/v2/sample-data" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for ds in data.get('datasets', []):
        print(f\"  {ds['id']:20s} - {ds['name']} ({ds['node_count']} nodes)\")
    print()
    for r in data.get('recipes', []):
        print(f\"  {r['id']:20s} - {r['name']}\")
except: print('Failed to parse')
" 2>/dev/null || api_get "/api/v2/sample-data"
        echo
        echo "Usage: $0 sample <dataset-name>"
        echo "       $0 sample social-network"
        return
    fi
    echo -e "${CYAN}Loading sample data: $dataset${NC}"
    api_post "/api/v2/sample-data/$dataset" | print_json
    echo
}

cmd_metrics() {
    echo -e "${CYAN}=== Prometheus Metrics ===${NC}"
    api_get "/metrics" 2>/dev/null || echo "Metrics endpoint not available"
    echo
}

cmd_help() {
    cat << EOF
${CYAN}nexora-admin${NC} — Operations CLI for Nexora-RS

${GREEN}Usage:${NC} $0 <command> [options]

${GREEN}Commands:${NC}
  status          Show detailed system status
  health          Health check (uptime, version, node count)
  slow-queries    Show slow query analysis
  backup          Trigger WAL backup (flush + checkpoint)
  reindex         Trigger HNSW vector reindex
  nodes           Show graph node statistics
  sample [name]   Load sample data (list available if no name given)
  metrics         Show Prometheus metrics

${GREEN}Environment:${NC}
  NEXORA_HOST          Server URL (default: http://localhost:8080)
  NEXORA_AUTH_SECRET   Auth secret (if auth is enabled)

${GREEN}Examples:${NC}
  $0 status
  $0 sample social-network
  $0 slow-queries
  NEXORA_HOST=http://prod:8080 $0 status

EOF
}

# Main
case "${1:-help}" in
    status)        cmd_status ;;
    health)        cmd_health ;;
    slow-queries)  cmd_slow_queries ;;
    backup)        cmd_backup ;;
    reindex)       cmd_reindex ;;
    nodes)         cmd_nodes ;;
    sample)        cmd_sample "${2:-}" ;;
    metrics)       cmd_metrics ;;
    help|--help|-h) cmd_help ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        echo ""
        cmd_help
        exit 1
        ;;
esac
