#!/bin/bash
# Nexora Stress Test - Find Performance Limits
# Ramp from 100 to 10,000 QPS
# Version: 1.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
API_ENDPOINT="${API_ENDPOINT:-http://localhost:8080}"
START_QPS="${START_QPS:-100}"
MAX_QPS="${MAX_QPS:-10000}"
STEP_QPS="${STEP_QPS:-100}"
STEP_DURATION="${STEP_DURATION:-60}"  # seconds per step
RESULTS_DIR="${RESULTS_DIR:-/tmp/nexora-stress-test}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${RESULTS_DIR}/stress-test.log"
}

log_success() {
    echo -e "${GREEN}✅ $*${NC}" | tee -a "${RESULTS_DIR}/stress-test.log"
}

log_error() {
    echo -e "${RED}❌ $*${NC}" | tee -a "${RESULTS_DIR}/stress-test.log"
}

setup() {
    log "Setting up stress test environment..."

    mkdir -p "${RESULTS_DIR}"
    mkdir -p "${RESULTS_DIR}/qps_steps"

    # Check prerequisites
    command -v jq >/dev/null 2>&1 || { log_error "jq is required"; exit 1; }
    command -v bc >/dev/null 2>&1 || { log_error "bc is required"; exit 1; }

    # Verify API
    if ! curl -sf "${API_ENDPOINT}/health" > /dev/null; then
        log_error "API endpoint not reachable: ${API_ENDPOINT}"
        exit 1
    fi

    log_success "Environment ready"
}

# Run load at specific QPS
run_qps_step() {
    local qps=$1
    local duration=$2
    local step_file="${RESULTS_DIR}/qps_steps/qps_${qps}.csv"

    log "Testing QPS: ${qps} for ${duration}s"

    local interval=$(echo "scale=6; 1.0 / $qps" | bc)
    local workers=$(( qps < 1000 ? 10 : qps / 100 ))
    local reqs_per_worker=$(( qps / workers ))

    # Launch workers
    for i in $(seq 1 $workers); do
        (
            local start=$(date +%s)
            local end=$((start + duration))
            local count=0

            while [ $(date +%s) -lt $end ]; do
                local req_start=$(date +%s%3N)

                # Mixed workload (70% reads, 30% writes)
                if (( RANDOM % 10 < 7 )); then
                    # Read
                    response=$(curl -sf -X POST "${API_ENDPOINT}/api/query/cypher" \
                        -H "Content-Type: application/json" \
                        -d '{"query": "MATCH (n) RETURN count(n) LIMIT 1"}' 2>/dev/null || echo "ERROR")
                    op_type="read"
                else
                    # Write
                    response=$(curl -sf -X POST "${API_ENDPOINT}/api/events/ingest" \
                        -H "Content-Type: application/json" \
                        -d "{\"topic\": \"stress_test\", \"events\": [{\"id\": \"s${qps}_w${i}_${count}\"}]}" 2>/dev/null || echo "ERROR")
                    op_type="write"
                fi

                local req_end=$(date +%s%3N)
                local latency=$((req_end - req_start))

                # Record
                if [[ "$response" == "ERROR" ]]; then
                    echo "${req_start},${op_type},${latency},error" >> "${step_file}.worker_${i}"
                else
                    echo "${req_start},${op_type},${latency},success" >> "${step_file}.worker_${i}"
                fi

                ((count++))
                sleep "$interval" 2>/dev/null || true
            done
        ) &
    done

    # Wait for workers
    wait

    # Aggregate results
    cat ${step_file}.worker_* > "${step_file}" 2>/dev/null || true
    rm -f ${step_file}.worker_* 2>/dev/null || true

    # Calculate metrics
    local total=$(wc -l < "${step_file}" 2>/dev/null || echo 0)
    local errors=$(grep -c "error" "${step_file}" 2>/dev/null || echo 0)
    local success=$((total - errors))
    local success_rate=$(echo "scale=2; ($success * 100.0) / $total" | bc 2>/dev/null || echo "0")

    # Latency percentiles
    local p50=$(awk -F',' '{print $3}' "${step_file}" | sort -n | awk '{a[NR]=$1} END {print a[int(NR*0.5)]}')
    local p95=$(awk -F',' '{print $3}' "${step_file}" | sort -n | awk '{a[NR]=$1} END {print a[int(NR*0.95)]}')
    local p99=$(awk -F',' '{print $3}' "${step_file}" | sort -n | awk '{a[NR]=$1} END {print a[int(NR*0.99)]}')

    # Record summary
    echo "${qps},${total},${success},${errors},${success_rate},${p50},${p95},${p99}" >> "${RESULTS_DIR}/stress_summary.csv"

    log "  Total: ${total}, Success: ${success} (${success_rate}%), p50: ${p50}ms, p95: ${p95}ms, p99: ${p99}ms"

    # Check if system is degrading
    if (( $(echo "$success_rate < 95" | bc -l) )); then
        log_error "Success rate dropped below 95% at ${qps} QPS"
        return 1
    fi

    if (( $(echo "$p99 > 1000" | bc -l) )); then
        log_warning "p99 latency exceeds 1000ms at ${qps} QPS"
    fi

    return 0
}

generate_report() {
    log "Generating stress test report..."

    local report_file="${RESULTS_DIR}/stress-test-report-${TIMESTAMP}.md"

    cat > "$report_file" <<EOF
# Nexora Stress Test Report

**Test Date**: $(date +'%Y-%m-%d %H:%M:%S')
**Test Type**: Progressive load increase
**Range**: ${START_QPS} - ${MAX_QPS} QPS
**Step Size**: ${STEP_QPS} QPS
**Step Duration**: ${STEP_DURATION}s

---

## Test Results

| QPS | Total Reqs | Success | Errors | Success % | p50 (ms) | p95 (ms) | p99 (ms) |
|-----|-----------|---------|--------|-----------|----------|----------|----------|
EOF

    # Add results from summary file
    if [ -f "${RESULTS_DIR}/stress_summary.csv" ]; then
        while IFS=',' read -r qps total success errors rate p50 p95 p99; do
            echo "| ${qps} | ${total} | ${success} | ${errors} | ${rate} | ${p50} | ${p95} | ${p99} |" >> "$report_file"
        done < "${RESULTS_DIR}/stress_summary.csv"
    fi

    cat >> "$report_file" <<EOF

---

## Performance Analysis

### Maximum Sustained Load

EOF

    # Find max QPS where success rate >= 99%
    local max_qps=$(awk -F',' '$5 >= 99.0 {print $1}' "${RESULTS_DIR}/stress_summary.csv" | tail -1 || echo "N/A")

    cat >> "$report_file" <<EOF
**Maximum QPS with 99%+ success rate**: ${max_qps} QPS

### Latency Breakdown

EOF

    # Find when latency starts degrading
    local good_p99=$(awk -F',' '$8 <= 100 {print $1}' "${RESULTS_DIR}/stress_summary.csv" | tail -1 || echo "N/A")

    cat >> "$report_file" <<EOF
**Sustained p99 < 100ms**: Up to ${good_p99} QPS

### Bottleneck Identification

EOF

    if [ "$max_qps" = "N/A" ]; then
        cat >> "$report_file" <<EOF
❌ **System did not maintain 99% success rate at any tested load.**

**Recommendation**: Investigate errors and optimize before production deployment.
EOF
    elif (( max_qps < 1000 )); then
        cat >> "$report_file" <<EOF
⚠️  **Maximum capacity below target** (${max_qps} QPS < 1000 QPS target).

**Recommendations**:
- Profile CPU/memory usage
- Check database write amplification
- Optimize query execution paths
- Consider horizontal scaling
EOF
    elif (( max_qps < 5000 )); then
        cat >> "$report_file" <<EOF
✅ **Acceptable capacity** (${max_qps} QPS).

**Recommendations**:
- Monitor resource usage trends
- Implement auto-scaling triggers at 70% capacity
- Regular capacity testing
EOF
    else
        cat >> "$report_file" <<EOF
✅ **Excellent capacity** (${max_qps} QPS).

**System can handle**:
- 5x normal load (assuming 1K QPS baseline)
- Traffic spikes without degradation
- Production workload with headroom

**Recommendations**:
- Deploy with confidence
- Set up monitoring and alerting at 60% capacity
EOF
    fi

    cat >> "$report_file" <<EOF

---

## Raw Data

Full metrics available in: \`${RESULTS_DIR}/qps_steps/\`

**Analysis completed**: $(date +'%Y-%m-%d %H:%M:%S')
EOF

    log_success "Report generated: ${report_file}"
}

main() {
    log "========================================"
    log "Starting Stress Test"
    log "========================================"

    setup

    # Initialize summary file
    echo "qps,total,success,errors,success_rate,p50,p95,p99" > "${RESULTS_DIR}/stress_summary.csv"

    local current_qps=$START_QPS
    local max_reached=false

    while [ $current_qps -le $MAX_QPS ]; do
        if ! run_qps_step "$current_qps" "$STEP_DURATION"; then
            log_error "System degraded at ${current_qps} QPS"
            log "Maximum sustainable QPS: $((current_qps - STEP_QPS))"
            break
        fi

        current_qps=$((current_qps + STEP_QPS))

        # Brief cooldown between steps
        sleep 5
    done

    if [ $current_qps -gt $MAX_QPS ]; then
        log_success "System maintained performance up to ${MAX_QPS} QPS"
    fi

    generate_report

    log "========================================"
    log_success "Stress Test Complete"
    log "========================================"
}

trap 'log_error "Test interrupted"; exit 1' INT TERM

main "$@"
