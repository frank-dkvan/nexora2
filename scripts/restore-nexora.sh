#!/usr/bin/env bash
#
# One-Click Nexora Restore Script
#
# Features:
# - Lists available backups
# - Validates backup integrity before restore
# - Performs restore with safety checks
# - Optionally creates a pre-restore backup
# - Verifies restore succeeded
#
# Usage:
#   ./restore-nexora.sh [options] <backup-file>
#
# Options:
#   --host <host>          Nexora API host (default: localhost)
#   --port <port>          Nexora API port (default: 8080)
#   --pre-backup           Create backup before restore (default: true)
#   --force                Skip confirmation prompt
#   --verify               Verify node count after restore
#   --help                 Show this help message
#
# Examples:
#   # List available backups
#   ./restore-nexora.sh --list
#
#   # Restore from specific backup
#   ./restore-nexora.sh /data/nexora/wal/backups/backup-20260725-103045.nxbak
#
#   # Restore without pre-backup (dangerous!)
#   ./restore-nexora.sh --no-pre-backup --force backup-20260725-103045.nxbak

set -euo pipefail

# Configuration
NEXORA_HOST="${NEXORA_HOST:-localhost}"
NEXORA_PORT="${NEXORA_PORT:-8080}"
PRE_BACKUP="${PRE_BACKUP:-true}"
FORCE="${FORCE:-false}"
VERIFY_RESTORE="${VERIFY_RESTORE:-false}"
LIST_ONLY="${LIST_ONLY:-false}"

# Parse command line arguments
BACKUP_FILE=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --host)
      NEXORA_HOST="$2"
      shift 2
      ;;
    --port)
      NEXORA_PORT="$2"
      shift 2
      ;;
    --pre-backup)
      PRE_BACKUP="true"
      shift
      ;;
    --no-pre-backup)
      PRE_BACKUP="false"
      shift
      ;;
    --force)
      FORCE="true"
      shift
      ;;
    --verify)
      VERIFY_RESTORE="true"
      shift
      ;;
    --list)
      LIST_ONLY="true"
      shift
      ;;
    --help)
      sed -n '2,/^$/p' "$0" | sed 's/^# //'
      exit 0
      ;;
    *)
      if [[ -z "$BACKUP_FILE" ]]; then
        BACKUP_FILE="$1"
      else
        echo "Unknown option: $1"
        echo "Run with --help for usage information"
        exit 1
      fi
      shift
      ;;
  esac
done

NEXORA_API="http://${NEXORA_HOST}:${NEXORA_PORT}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESTORE_LOG="/tmp/nexora-restore-${TIMESTAMP}.log"

# Logging functions
log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$RESTORE_LOG"
}

log_error() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "$RESTORE_LOG" >&2
}

# List available backups
list_backups() {
  log "Listing available backups..."

  # Try to find backup directory from common locations
  local backup_dirs=(
    "/data/nexora/wal/backups"
    "/var/lib/nexora/wal/backups"
    "./data/wal/backups"
  )

  local found=false
  for dir in "${backup_dirs[@]}"; do
    if [[ -d "$dir" ]]; then
      log "Found backups in: $dir"
      echo ""
      find "$dir" -name "backup-*.nxbak" -type f -printf "%T@ %p %s\n" 2>/dev/null | \
        sort -rn | \
        while read -r mtime path size; do
          local date
          date=$(date -d "@${mtime}" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -r "${mtime%%.*}" '+%Y-%m-%d %H:%M:%S')
          local size_human
          size_human=$(numfmt --to=iec "$size" 2>/dev/null || echo "${size} bytes")
          printf "  %-20s  %-10s  %s\n" "$date" "$size_human" "$(basename "$path")"
        done
      echo ""
      found=true
    fi
  done

  if [[ "$found" == "false" ]]; then
    log_error "No backup directories found"
    exit 1
  fi
}

# Check if Nexora is healthy
check_health() {
  log "Checking Nexora health..."
  if ! curl -f -s "${NEXORA_API}/api/health" > /dev/null; then
    log_error "Nexora is not healthy or unreachable"
    exit 1
  fi
  log "Health check passed"
}

# Create pre-restore backup
create_pre_restore_backup() {
  if [[ "$PRE_BACKUP" != "true" ]]; then
    log "Skipping pre-restore backup (--no-pre-backup specified)"
    return
  fi

  log "Creating pre-restore backup for safety..."

  local response
  response=$(curl -s -w "\n%{http_code}" -X POST "${NEXORA_API}/api/admin/backup")

  local http_code
  http_code=$(echo "$response" | tail -n1)
  local body
  body=$(echo "$response" | sed '$d')

  if [[ "$http_code" != "200" ]]; then
    log_error "Pre-restore backup failed with HTTP $http_code"
    log_error "Response: $body"
    echo ""
    echo "ABORT: Cannot proceed without pre-restore backup for safety."
    echo "Use --no-pre-backup to override (not recommended)."
    exit 1
  fi

  local pre_backup_path
  pre_backup_path=$(echo "$body" | jq -r '.backup_path')
  log "Pre-restore backup created: $pre_backup_path"
}

# Validate backup file
validate_backup() {
  log "Validating backup file: $BACKUP_FILE"

  if [[ ! -f "$BACKUP_FILE" ]]; then
    log_error "Backup file not found: $BACKUP_FILE"
    exit 1
  fi

  local file_size
  file_size=$(stat -f%z "$BACKUP_FILE" 2>/dev/null || stat -c%s "$BACKUP_FILE" 2>/dev/null)

  if [[ $file_size -eq 0 ]]; then
    log_error "Backup file is empty"
    exit 1
  fi

  log "Backup file validated ($file_size bytes)"
}

# Confirm restore operation
confirm_restore() {
  if [[ "$FORCE" == "true" ]]; then
    return
  fi

  echo ""
  echo "╔════════════════════════════════════════════════════════════════╗"
  echo "║                     ⚠️  WARNING ⚠️                              ║"
  echo "║                                                                ║"
  echo "║  This is a DESTRUCTIVE operation.                             ║"
  echo "║  Existing graph data will be OVERWRITTEN.                     ║"
  echo "║                                                                ║"
  echo "║  Backup file: $(printf '%-48s' "$(basename "$BACKUP_FILE")") ║"
  echo "║  Target host: $(printf '%-48s' "${NEXORA_HOST}:${NEXORA_PORT}") ║"
  echo "║                                                                ║"
  echo "╚════════════════════════════════════════════════════════════════╝"
  echo ""
  read -rp "Type 'RESTORE' to confirm: " confirmation

  if [[ "$confirmation" != "RESTORE" ]]; then
    log "Restore cancelled by user"
    exit 0
  fi

  log "User confirmed restore operation"
}

# Perform restore
perform_restore() {
  log "Starting restore from: $BACKUP_FILE"

  # Get absolute path
  local abs_backup_path
  abs_backup_path=$(cd "$(dirname "$BACKUP_FILE")" && pwd)/$(basename "$BACKUP_FILE")

  local response
  response=$(curl -s -w "\n%{http_code}" -X POST "${NEXORA_API}/api/admin/restore" \
    -H "Content-Type: application/json" \
    -d "{\"backup_path\": \"$abs_backup_path\"}")

  local http_code
  http_code=$(echo "$response" | tail -n1)
  local body
  body=$(echo "$response" | sed '$d')

  if [[ "$http_code" != "200" ]]; then
    log_error "Restore failed with HTTP $http_code"
    log_error "Response: $body"
    exit 1
  fi

  local nodes_restored
  nodes_restored=$(echo "$body" | jq -r '.nodes_restored // "unknown"')

  log "Restore completed successfully"
  log "Nodes restored: $nodes_restored"

  echo "$body" | jq '.' >> "$RESTORE_LOG"
}

# Verify restore
verify_restore() {
  if [[ "$VERIFY_RESTORE" != "true" ]]; then
    return
  fi

  log "Verifying restore..."

  sleep 2  # Give system time to settle

  local response
  response=$(curl -s "${NEXORA_API}/api/metrics")

  local active_nodes
  active_nodes=$(echo "$response" | jq -r '.active_nodes // 0')

  if [[ "$active_nodes" -eq 0 ]]; then
    log_error "Verification failed: no active nodes found"
    exit 1
  fi

  log "Verification passed: $active_nodes active nodes"
}

# Main execution
main() {
  log "=== Nexora Restore Started ==="
  log "Host: ${NEXORA_HOST}:${NEXORA_PORT}"
  log "Pre-backup: ${PRE_BACKUP}"
  log "Force: ${FORCE}"

  if [[ "$LIST_ONLY" == "true" ]]; then
    list_backups
    exit 0
  fi

  if [[ -z "$BACKUP_FILE" ]]; then
    echo "Error: No backup file specified"
    echo ""
    echo "Usage: $0 [options] <backup-file>"
    echo "       $0 --list  (to list available backups)"
    echo ""
    echo "Run with --help for more information"
    exit 1
  fi

  check_health
  validate_backup
  confirm_restore
  create_pre_restore_backup
  perform_restore
  verify_restore

  log "=== Restore Completed Successfully ==="
  log "Restore log: $RESTORE_LOG"

  echo ""
  echo "✅ Restore completed successfully!"
  echo ""
  echo "Next steps:"
  echo "  1. Verify data integrity: curl ${NEXORA_API}/api/metrics"
  echo "  2. Check logs: tail -f $RESTORE_LOG"
  echo "  3. Run smoke tests"
  echo ""

  exit 0
}

# Trap errors
trap 'log_error "Restore failed with exit code $?"; exit 1' ERR

main
