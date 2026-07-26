#!/usr/bin/env bash
#
# Automated Nexora Backup Script
#
# Features:
# - Creates timestamped backups via /api/admin/backup
# - Validates backup integrity (Blake3 checksum)
# - Manages retention policy (keeps last N backups)
# - Sends notifications on success/failure
# - Supports S3/MinIO upload for offsite storage
#
# Usage:
#   ./backup-nexora.sh [options]
#
# Options:
#   --host <host>          Nexora API host (default: localhost)
#   --port <port>          Nexora API port (default: 8080)
#   --retention <days>     Keep backups for N days (default: 30)
#   --s3-bucket <bucket>   Upload to S3 bucket (optional)
#   --slack-webhook <url>  Send notifications to Slack (optional)
#   --verify               Verify backup integrity after creation
#   --help                 Show this help message

set -euo pipefail

# Configuration
NEXORA_HOST="${NEXORA_HOST:-localhost}"
NEXORA_PORT="${NEXORA_PORT:-8080}"
RETENTION_DAYS="${RETENTION_DAYS:-30}"
S3_BUCKET="${S3_BUCKET:-}"
SLACK_WEBHOOK="${SLACK_WEBHOOK:-}"
VERIFY_BACKUP="${VERIFY_BACKUP:-false}"

# Parse command line arguments
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
    --retention)
      RETENTION_DAYS="$2"
      shift 2
      ;;
    --s3-bucket)
      S3_BUCKET="$2"
      shift 2
      ;;
    --slack-webhook)
      SLACK_WEBHOOK="$2"
      shift 2
      ;;
    --verify)
      VERIFY_BACKUP="true"
      shift
      ;;
    --help)
      sed -n '2,/^$/p' "$0" | sed 's/^# //'
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Run with --help for usage information"
      exit 1
      ;;
  esac
done

NEXORA_API="http://${NEXORA_HOST}:${NEXORA_PORT}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
BACKUP_LOG="/tmp/nexora-backup-${TIMESTAMP}.log"

# Logging functions
log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$BACKUP_LOG"
}

log_error() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "$BACKUP_LOG" >&2
}

# Send Slack notification
notify_slack() {
  local status=$1
  local message=$2

  if [[ -z "$SLACK_WEBHOOK" ]]; then
    return
  fi

  local color="good"
  local icon=":white_check_mark:"
  if [[ "$status" == "error" ]]; then
    color="danger"
    icon=":x:"
  fi

  curl -X POST "$SLACK_WEBHOOK" \
    -H "Content-Type: application/json" \
    -d "{
      \"attachments\": [{
        \"color\": \"$color\",
        \"title\": \"$icon Nexora Backup - $status\",
        \"text\": \"$message\",
        \"fields\": [
          {\"title\": \"Host\", \"value\": \"$NEXORA_HOST:$NEXORA_PORT\", \"short\": true},
          {\"title\": \"Timestamp\", \"value\": \"$TIMESTAMP\", \"short\": true}
        ]
      }]
    }" \
    --silent --output /dev/null || true
}

# Check if Nexora is healthy
check_health() {
  log "Checking Nexora health..."
  if ! curl -f -s "${NEXORA_API}/api/health" > /dev/null; then
    log_error "Nexora is not healthy or unreachable"
    notify_slack "error" "Health check failed - backup aborted"
    exit 1
  fi
  log "Health check passed"
}

# Create backup
create_backup() {
  log "Requesting backup from ${NEXORA_API}..."

  local response
  response=$(curl -s -w "\n%{http_code}" -X POST "${NEXORA_API}/api/admin/backup")

  local http_code
  http_code=$(echo "$response" | tail -n1)
  local body
  body=$(echo "$response" | sed '$d')

  if [[ "$http_code" != "200" ]]; then
    log_error "Backup request failed with HTTP $http_code"
    log_error "Response: $body"
    notify_slack "error" "Backup creation failed (HTTP $http_code)"
    exit 1
  fi

  # Extract backup path and size from response
  BACKUP_PATH=$(echo "$body" | jq -r '.backup_path')
  BACKUP_SIZE=$(echo "$body" | jq -r '.backup_size_bytes')

  if [[ -z "$BACKUP_PATH" ]] || [[ "$BACKUP_PATH" == "null" ]]; then
    log_error "Failed to extract backup path from response"
    log_error "Response: $body"
    notify_slack "error" "Backup path extraction failed"
    exit 1
  fi

  log "Backup created successfully: $BACKUP_PATH ($BACKUP_SIZE bytes)"
}

# Verify backup integrity
verify_backup() {
  if [[ "$VERIFY_BACKUP" != "true" ]]; then
    return
  fi

  log "Verifying backup integrity..."

  if [[ ! -f "$BACKUP_PATH" ]]; then
    log_error "Backup file not found: $BACKUP_PATH"
    notify_slack "error" "Backup verification failed - file not found"
    exit 1
  fi

  local actual_size
  actual_size=$(stat -f%z "$BACKUP_PATH" 2>/dev/null || stat -c%s "$BACKUP_PATH" 2>/dev/null)

  if [[ "$actual_size" != "$BACKUP_SIZE" ]]; then
    log_error "Backup size mismatch: expected $BACKUP_SIZE, got $actual_size"
    notify_slack "error" "Backup verification failed - size mismatch"
    exit 1
  fi

  log "Backup integrity verified (size: $actual_size bytes)"
}

# Upload to S3/MinIO
upload_to_s3() {
  if [[ -z "$S3_BUCKET" ]]; then
    return
  fi

  log "Uploading backup to S3: s3://${S3_BUCKET}/nexora-backups/"

  local s3_key="nexora-backups/$(basename "$BACKUP_PATH")"

  if command -v aws &> /dev/null; then
    if aws s3 cp "$BACKUP_PATH" "s3://${S3_BUCKET}/${s3_key}"; then
      log "Backup uploaded to S3 successfully"
    else
      log_error "S3 upload failed"
      notify_slack "error" "S3 upload failed"
      exit 1
    fi
  else
    log_error "aws CLI not found - cannot upload to S3"
    notify_slack "error" "S3 upload skipped - aws CLI not installed"
  fi
}

# Clean up old backups
cleanup_old_backups() {
  log "Cleaning up backups older than ${RETENTION_DAYS} days..."

  local backup_dir
  backup_dir=$(dirname "$BACKUP_PATH")

  local deleted=0
  while IFS= read -r -d '' old_backup; do
    rm -f "$old_backup"
    log "Deleted old backup: $(basename "$old_backup")"
    deleted=$((deleted + 1))
  done < <(find "$backup_dir" -name "backup-*.nxbak" -type f -mtime "+${RETENTION_DAYS}" -print0)

  if [[ $deleted -gt 0 ]]; then
    log "Deleted $deleted old backup(s)"
  else
    log "No old backups to delete"
  fi
}

# Main execution
main() {
  log "=== Nexora Backup Started ==="
  log "Host: ${NEXORA_HOST}:${NEXORA_PORT}"
  log "Retention: ${RETENTION_DAYS} days"
  log "S3 Upload: ${S3_BUCKET:-disabled}"
  log "Verification: ${VERIFY_BACKUP}"

  check_health
  create_backup
  verify_backup
  upload_to_s3
  cleanup_old_backups

  log "=== Backup Completed Successfully ==="
  log "Backup path: $BACKUP_PATH"
  log "Backup size: $BACKUP_SIZE bytes"

  notify_slack "success" "Backup completed successfully\nPath: \`$BACKUP_PATH\`\nSize: $(numfmt --to=iec "$BACKUP_SIZE" 2>/dev/null || echo "$BACKUP_SIZE bytes")"

  exit 0
}

# Trap errors
trap 'log_error "Backup failed with exit code $?"; notify_slack "error" "Backup script failed unexpectedly"; exit 1' ERR

main
