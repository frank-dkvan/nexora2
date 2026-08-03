#!/bin/bash
# Nexora 72-Hour Stability Load Test
# Sustained load: 1,000 writes/s + 5,000 reads/s
# Version: 1.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DURATION="${TEST_DURATION:-259200}"  # 72 hours in seconds
API_ENDPOINT="${API_ENDPOINT:-http://localhost:8080}"
WRITE_RATE="${WRITE_RATE:-1000}"  # writes per second
READ_RATE="${READ_RATE:-5000}"    # reads per second
RESULTS_DIR="${RESULTS_DIR:-/tmp/nexora-load-test}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${RESULTS_DIR}/stability-test.log"
}

log_success() {
    echo -e "${GREEN}✅ $*${NC}" | tee -a "${RESULTS_DIR}/stability-test.log"
}

log_error() {
    echo -e "${RED}❌ $*${NC}" | tee -a "${RESULTS_DIR}/stability-test.log"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $*${NC}" | tee -a "${RESULTS_DIR}/stability-test.log"
}

setup() {
    log "Setting up stability test environment..."

    mkdir -p "${RESULTS_DIR}"
    mkdir -p "${RESULTS_DIR}/metrics"

    # Check prerequisites
    command -v jq >/dev/null 2>&1 || { log_error "jq is required but not installed"; exit 1; }
    command -v bc >/dev/null 2>&1 || { log_error "bc is required but not installed"; exit 1; }

    # Verify API is reachable
    if ! curl -sf "${API_ENDPOINT}/health" > /dev/null; then
        log_error "API endpoint not reachable: ${API_ENDPOINT}"
        exit 1
    fi

    log_success "Environment ready"
}

# Write workload generator
write_workload() {
    local worker_id=$1
    local duration=$2
    local rate=$3
    local interval=$(echo "scale=6; 1.0 / $rate" | bc)

    local count=0
    local start_time=$(date +%s)
    local end_time=$((start_time + duration))

    while [ $(date +%s) -lt $end_time ]; do
        local req_start=$(date +%s%3N)

        # Generate write request
        local event_id="load_test_w${worker_id}_${count}"
        local payload=$(cat <<EOF
{
  "topic": "load_test",
  "events": [{
    "id": "${event_id}",
    "data": {
      "worker": ${worker_id},
      "count": ${count},
      "timestamp": $(date +%s)
    }
  }]
}
EOF
)

        # Send request
        response=$(curl -sf -X POST "${API_ENDPOINT}/api/events/ingest" \
            -H "Content-Type: application/json" \
            -d "$payload" 2>/dev/null || echo "ERROR")

        local req_end=$(date +%s%3N)
        local latency=$((req_end - req_start))

        # Record metrics
        if [[ "$response" == "ERROR" ]]; then
            echo "${req_start},write,${latency},error" >> "${RESULTS_DIR}/metrics/write_worker_${worker_id}.csv"
        else
            echo "${req_start},write,${latency},success" >> "${RESULTS_DIR}/metrics/write_worker_${worker_id}.csv"
        fi

        ((count++))

        # Rate limiting
        sleep "$interval" 2>/dev/null || true
    done

    log "Write worker ${worker_id} completed: ${count} requests"
}

# Read workload generator
read_workload() {
    local worker_id=$1
    local duration=$2
    local rate=$3
    local interval=$(echo "scale=6; 1.0 / $rate" | bc)

    local count=0
    local start_time=$(date +%s)
    local end_time=$((start_time + duration))

    # Sample queries
    local queries=(
        "MATCH (n) RETURN count(n) as total"
        "MATCH (n) RETURN n LIMIT 100"
        "MATCH (n)-[r]->(m) RETURN count(r) as edges"
        "MATCH (n) WHERE n.worker IS NOT NULL RETURN n LIMIT 50"
    )

    while [ $(date +%s) -lt $end_time ]; do
        local req_start=$(date +%s%3N)

        # Select random query
        local query_idx=$((RANDOM % ${#queries[@]}))
        local query="${queries[$query_idx]}"

        # Send request
        response=$(curl -sf -X POST "${API_ENDPOINT}/api/query/cypher" \
            -H "Content-Type: application/json" \
            -d "{\"query\": \"${query}\"}" 2>/dev/null || echo "ERROR")

        local req_end=$(date +%s%3N)
        local latency=$((req_end - req_start))

        # Record metrics
        if [[ "$response" == "ERROR" ]]; then
            echo "${req_start},read,${latency},error" >> "${RESULTS_DIR}/metrics/read_worker_${worker_id}.csv"
        else
            echo "${req_start},read,${latency},success" >> "${RESULTS_DIR}/metrics/read_worker_${worker_id}.csv"
        fi

        ((count++))

        # Rate limiting
        sleep "$interval" 2>/dev/null || true
    done

    log "Read worker ${worker_id} completed: ${count} requests"
}

# Monitor system metrics
monitor_metrics() {
    local duration=$1
    local interval=60  # Sample every minute

    local start_time=$(date +%s)
    local end_time=$((start_time + duration))

    while [ $(date +%s) -lt $end_time ]; do
        local timestamp=$(date +%s)

        # Collect metrics from API
        health=$(curl -sf "${API_ENDPOINT}/health" 2>/dev/null || echo "{}")

        # System metrics (if available)
        cpu_usage=$(top -l 1 | grep "CPU usage" | awk '{print $3}' | sed 's/%//' || echo "0")
        mem_usage=$(ps aux | grep nexora | awk '{sum+=$4} END {print sum}' || echo "0")

        # Record
        echo "${timestamp},${cpu_usage},${mem_usage}" >> "${RESULTS_DIR}/metrics/system.csv"

        sleep "$interval"
    done
}

# Generate final report
generate_report() {
    log "Generating test report..."

    local report_file="${RESULTS_DIR}/stability-test-report-${TIMESTAMP}.md"

    cat > "$report_file" <<EOF
# Nexora 72-Hour Stability Test Report

**Test Date**: $(date +'%Y-%m-%d %H:%M:%S')
**Duration**: ${TEST_DURATION} seconds (72 hours)
**Target Load**: ${WRITE_RATE} writes/s + ${READ_RATE} reads/s

---

## Test Configuration

| Parameter | Value |
|-----------|-------|
| API Endpoint | ${API_ENDPOINT} |
| Write Rate Target | ${WRITE_RATE} req/s |
| Read Rate Target | ${READ_RATE} req/s |
| Test Duration | ${TEST_DURATION}s (72h) |
| Total Expected Writes | $(( WRITE_RATE * TEST_DURATION )) |
| Total Expected Reads | $(( READ_RATE * TEST_DURATION )) |

---

## Results Summary

EOF

    # Aggregate write metrics
    if ls ${RESULTS_DIR}/metrics/write_worker_*.csv >/dev/null 2>&1; then
        cat ${RESULTS_DIR}/metrics/write_worker_*.csv > ${RESULTS_DIR}/metrics/all_writes.csv

        local total_writes=$(wc -l < ${RESULTS_DIR}/metrics/all_writes.csv)
        local failed_writes=$(grep -c "error" ${RESULTS_DIR}/metrics/all_writes.csv || echo 0)
        local success_writes=$((total_writes - failed_writes))
        local write_success_rate=$(echo "scale=2; ($success_writes * 100.0) / $total_writes" | bc)

        cat >> "$report_file" <<EOF
### Write Operations

| Metric | Value |
|--------|-------|
| Total Requests | ${total_writes} |
| Successful | ${success_writes} |
| Failed | ${failed_writes} |
| Success Rate | ${write_success_rate}% |

EOF
    fi

    # Aggregate read metrics
    if ls ${RESULTS_DIR}/metrics/read_worker_*.csv >/dev/null 2>&1; then
        cat ${RESULTS_DIR}/metrics/read_worker_*.csv > ${RESULTS_DIR}/metrics/all_reads.csv

        local total_reads=$(wc -l < ${RESULTS_DIR}/metrics/all_reads.csv)
        local failed_reads=$(grep -c "error" ${RESULTS_DIR}/metrics/all_reads.csv || echo 0)
        local success_reads=$((total_reads - failed_reads))
        local read_success_rate=$(echo "scale=2; ($success_reads * 100.0) / $total_reads" | bc)

        cat >> "$report_file" <<EOF
### Read Operations

| Metric | Value |
|--------|-------|
| Total Requests | ${total_reads} |
| Successful | ${success_reads} |
| Failed | ${failed_reads} |
| Success Rate | ${read_success_rate}% |

---

## Conclusion

EOF
    fi

    # Determine pass/fail
    if (( $(echo "$write_success_rate >= 99.9" | bc -l) )) && (( $(echo "$read_success_rate >= 99.9" | bc -l) )); then
        cat >> "$report_file" <<EOF
✅ **PASS**: System maintained 99.9%+ success rate for 72 hours.

**Recommendation**: System is production-ready for this load profile.
EOF
    else
        cat >> "$report_file" <<EOF
❌ **FAIL**: Success rate below 99.9% threshold.

**Recommendation**: Investigate errors and re-test after fixes.
EOF
    fi

    log_success "Report generated: ${report_file}"
}

main() {
    log "========================================"
    log "Starting 72-Hour Stability Test"
    log "========================================"

    setup

    log "Launching workload generators..."

    # Start write workers (distribute load across workers)
    local write_workers=10
    local writes_per_worker=$((WRITE_RATE / write_workers))

    for i in $(seq 1 $write_workers); do
        write_workload "$i" "$TEST_DURATION" "$writes_per_worker" &
    done

    # Start read workers
    local read_workers=20
    local reads_per_worker=$((READ_RATE / read_workers))

    for i in $(seq 1 $read_workers); do
        read_workload "$i" "$TEST_DURATION" "$reads_per_worker" &
    done

    # Start monitoring
    monitor_metrics "$TEST_DURATION" &

    log "All workers started. Test running for ${TEST_DURATION} seconds..."
    log "Progress can be monitored in: ${RESULTS_DIR}/metrics/"

    # Wait for all workers
    wait

    log_success "All workers completed"

    # Generate report
    generate_report

    log "========================================"
    log_success "72-Hour Stability Test Complete"
    log "========================================"
}

# Trap signals for clean shutdown
trap 'log_error "Test interrupted"; exit 1' INT TERM

main "$@"
