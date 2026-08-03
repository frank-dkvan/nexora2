#!/bin/bash
# Nexora Disaster Recovery - Master Validation Script
# Runs all validation checks after recovery
# Version: 1.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="/var/log/nexora/validation-$(date +%Y%m%d_%H%M%S).log"
FAILED_CHECKS=0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    local msg="[$(date +'%Y-%m-%d %H:%M:%S')] $*"
    echo "$msg" | tee -a "$LOG_FILE"
}

log_success() {
    echo -e "${GREEN}✅ $*${NC}" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "${RED}❌ $*${NC}" | tee -a "$LOG_FILE"
    ((FAILED_CHECKS++))
}

log_warning() {
    echo -e "${YELLOW}⚠️  $*${NC}" | tee -a "$LOG_FILE"
}

run_check() {
    local check_name="$1"
    local check_script="$2"

    log "Running: $check_name"

    if bash "$check_script" >> "$LOG_FILE" 2>&1; then
        log_success "$check_name passed"
        return 0
    else
        log_error "$check_name failed"
        return 1
    fi
}

main() {
    log "========================================"
    log "Nexora Disaster Recovery Validation"
    log "========================================"

    # 1. Cluster Health
    run_check "Cluster Health" "${SCRIPT_DIR}/validate-cluster-health.sh"

    # 2. Data Consistency
    run_check "Data Consistency" "${SCRIPT_DIR}/validate-data-consistency.sh"

    # 3. Event Log Integrity
    run_check "Event Log Integrity" "${SCRIPT_DIR}/validate-event-log.sh"

    # 4. Performance Baseline
    run_check "Performance Baseline" "${SCRIPT_DIR}/validate-performance.sh"

    # 5. End-to-End Test
    run_check "E2E Smoke Test" "${SCRIPT_DIR}/e2e-smoke-test.sh"

    log "========================================"
    if [ $FAILED_CHECKS -eq 0 ]; then
        log_success "All validation checks passed!"
        log "Full validation log: $LOG_FILE"
        exit 0
    else
        log_error "$FAILED_CHECKS validation check(s) failed"
        log "Review log for details: $LOG_FILE"
        exit 1
    fi
}

main "$@"
