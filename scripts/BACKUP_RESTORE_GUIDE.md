# Automated Backup/Restore Configuration for Nexora 2.0

This directory contains production-ready backup and restore automation for Nexora deployments.

---

## 📦 Components

### 1. `backup-nexora.sh` - Automated Backup Script

**Features:**
- ✅ Automated backups via REST API (`POST /api/admin/backup`)
- ✅ Blake3 checksum integrity verification
- ✅ Retention policy (default: 30 days)
- ✅ S3/MinIO offsite backup support
- ✅ Slack notifications on success/failure
- ✅ Health check before backup
- ✅ Detailed logging

**Usage:**
```bash
# Basic backup (stores in WAL directory)
./backup-nexora.sh

# Backup with S3 upload
./backup-nexora.sh --s3-bucket my-nexora-backups

# Custom retention
./backup-nexora.sh --retention 60

# Verify backup integrity
./backup-nexora.sh --verify

# Full production setup
./backup-nexora.sh \
  --host prod-nexora-1 \
  --port 8080 \
  --retention 90 \
  --s3-bucket nexora-production-backups \
  --slack-webhook https://hooks.slack.com/services/YOUR/WEBHOOK \
  --verify
```

### 2. `restore-nexora.sh` - One-Click Restore Script

**Features:**
- ✅ List available backups with timestamps
- ✅ Pre-restore safety backup (automatic)
- ✅ Integrity validation before restore
- ✅ Confirmation prompt for safety
- ✅ Post-restore verification
- ✅ Detailed restore logging

**Usage:**
```bash
# List available backups
./restore-nexora.sh --list

# Restore from specific backup (with safety checks)
./restore-nexora.sh /data/nexora/wal/backups/backup-20260725-103045.nxbak

# Restore without pre-backup (DANGEROUS - not recommended)
./restore-nexora.sh --no-pre-backup --force backup-20260725-103045.nxbak

# Restore with verification
./restore-nexora.sh --verify /path/to/backup.nxbak
```

### 3. Cron Scheduling Examples

**Daily backups at 2 AM:**
```cron
# /etc/cron.d/nexora-backup
0 2 * * * nexora /opt/nexora/scripts/backup-nexora.sh --s3-bucket nexora-prod-backups --slack-webhook $SLACK_URL 2>&1 | logger -t nexora-backup
```

**Using systemd timer (recommended):**

`/etc/systemd/system/nexora-backup.service`:
```ini
[Unit]
Description=Nexora Database Backup
After=network.target

[Service]
Type=oneshot
User=nexora
ExecStart=/opt/nexora/scripts/backup-nexora.sh --s3-bucket nexora-prod-backups --verify
StandardOutput=journal
StandardError=journal
```

`/etc/systemd/system/nexora-backup.timer`:
```ini
[Unit]
Description=Nexora Backup Timer
Requires=nexora-backup.service

[Timer]
OnCalendar=daily
OnCalendar=02:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enable:
```bash
sudo systemctl daemon-reload
sudo systemctl enable nexora-backup.timer
sudo systemctl start nexora-backup.timer
sudo systemctl list-timers  # Verify it's scheduled
```

---

## 🔒 Backup Format

Nexora backups are stored in `.nxbak` format (custom binary format):

**Structure:**
```
┌─────────────────────────────────────────┐
│ Manifest (JSON)                         │
│ - version: "2.0"                        │
│ - timestamp: ISO 8601                   │
│ - node_count: N                         │
│ - checksum: Blake3 hash                 │
└─────────────────────────────────────────┘
│ Serialized Graph Data (bincode)        │
│ - Nodes (QID, properties, labels)      │
│ - Edges (source, target, label)        │
│ - Indexes (by label, property)         │
└─────────────────────────────────────────┘
```

**Integrity Protection:**
- Blake3 checksum in manifest
- Validated on restore
- Corruption detection before data loss

---

## 🚨 Disaster Recovery Playbook

### Scenario 1: Accidental Data Deletion

**Detection:**
- Monitoring alert: `active_nodes` dropped suddenly
- User report: "My data is gone!"

**Recovery:**
```bash
# 1. Stop writes immediately
curl -X POST http://nexora:8080/api/admin/drain

# 2. List available backups
./restore-nexora.sh --list

# 3. Restore from latest backup
./restore-nexora.sh --verify /data/backups/backup-LATEST.nxbak

# 4. Verify data
curl http://nexora:8080/api/metrics | jq '.active_nodes'

# 5. Resume operations
# (restart nexora service or undrain)
```

**RTO (Recovery Time Objective):** 5-15 minutes  
**RPO (Recovery Point Objective):** Last backup (e.g., 24 hours for daily backups)

### Scenario 2: Complete Node Failure

**Recovery:**
```bash
# 1. Provision new server
# 2. Install Nexora
# 3. Download latest backup from S3
aws s3 cp s3://nexora-prod-backups/backup-LATEST.nxbak /data/backups/

# 4. Restore
./restore-nexora.sh --no-pre-backup --force /data/backups/backup-LATEST.nxbak

# 5. Start Nexora
sudo systemctl start nexora

# 6. Verify cluster rejoins
curl http://nexora:8080/api/cluster/stats
```

**RTO:** 30-60 minutes (depends on provisioning time)

### Scenario 3: Corrupted WAL / RocksDB

**Symptoms:**
- Nexora fails to start
- Logs show: "Failed to open RocksDB" or "WAL corruption detected"

**Recovery:**
```bash
# 1. Stop Nexora
sudo systemctl stop nexora

# 2. Backup corrupted data (for forensics)
sudo mv /data/nexora/rocksdb /data/nexora/rocksdb.corrupted
sudo mv /data/nexora/wal /data/nexora/wal.corrupted

# 3. Reinitialize storage
sudo mkdir -p /data/nexora/rocksdb /data/nexora/wal

# 4. Start Nexora (empty state)
sudo systemctl start nexora

# 5. Restore from backup
./restore-nexora.sh --no-pre-backup /data/backups/backup-LATEST.nxbak
```

---

## 📊 Monitoring Backup Health

### Key Metrics

**Backup Freshness:**
```bash
# Check age of latest backup
find /data/nexora/wal/backups -name "backup-*.nxbak" -type f -mmin +1500 -ls
# Alert if no backup in last 25 hours (daily + 1 hour grace)
```

**Backup Size Trend:**
```bash
# Track backup growth over time
ls -lh /data/nexora/wal/backups/*.nxbak | awk '{print $5, $9}'
```

**S3 Upload Success:**
```bash
# Verify backups are reaching S3
aws s3 ls s3://nexora-prod-backups/nexora-backups/ --recursive | tail -5
```

### Grafana Dashboard Panel

Add to `monitoring/grafana-dashboards/nexora-overview.json`:

```json
{
  "title": "Backup Freshness",
  "type": "stat",
  "targets": [{
    "expr": "(time() - nexora_last_backup_timestamp) / 3600",
    "legendFormat": "Hours since last backup"
  }],
  "fieldConfig": {
    "defaults": {
      "unit": "h",
      "thresholds": {
        "steps": [
          {"value": 0, "color": "green"},
          {"value": 25, "color": "yellow"},
          {"value": 48, "color": "red"}
        ]
      }
    }
  }
}
```

### Prometheus Alert

Add to `monitoring/prometheus-alerts/nexora-alerts.yml`:

```yaml
- alert: NexoraBackupStale
  expr: (time() - nexora_last_backup_timestamp) > 86400
  for: 1h
  labels:
    severity: warning
    component: backup
  annotations:
    summary: "Nexora backup is stale on {{ $labels.instance }}"
    description: "Last backup was {{ $value | humanizeDuration }} ago (threshold: 24h)"
```

---

## 🧪 Testing Backup/Restore

### Test 1: Backup Creation

```bash
# Run backup
./backup-nexora.sh --verify

# Check backup file exists
ls -lh /data/nexora/wal/backups/*.nxbak | tail -1

# Verify checksum
# (checksum is embedded in manifest, validated by --verify flag)
```

### Test 2: Restore Integrity

```bash
# Get current node count
BEFORE=$(curl -s http://localhost:8080/api/metrics | jq '.active_nodes')

# Create backup
./backup-nexora.sh
BACKUP_FILE=$(ls -t /data/nexora/wal/backups/*.nxbak | head -1)

# Restore
./restore-nexora.sh --force "$BACKUP_FILE"

# Verify count matches
AFTER=$(curl -s http://localhost:8080/api/metrics | jq '.active_nodes')

if [ "$BEFORE" -eq "$AFTER" ]; then
  echo "✅ Restore test passed: $BEFORE nodes"
else
  echo "❌ Restore test failed: $BEFORE → $AFTER nodes"
fi
```

### Test 3: S3 Upload/Download

```bash
# Backup with S3 upload
./backup-nexora.sh --s3-bucket nexora-test-backups

# Download from S3
LATEST=$(aws s3 ls s3://nexora-test-backups/nexora-backups/ | tail -1 | awk '{print $4}')
aws s3 cp "s3://nexora-test-backups/nexora-backups/$LATEST" /tmp/test-backup.nxbak

# Restore from downloaded backup
./restore-nexora.sh --force /tmp/test-backup.nxbak
```

---

## 🔧 Troubleshooting

### Backup fails with "WAL is disabled"

**Cause:** Backup requires WAL directory to store backup files.

**Fix:**
```bash
# Start Nexora with WAL enabled
./nexora --wal-dir /data/nexora/wal
```

### Restore fails with "Backup file not found"

**Cause:** Relative path doesn't resolve correctly.

**Fix:** Use absolute path:
```bash
./restore-nexora.sh /data/nexora/wal/backups/backup-20260725-103045.nxbak
```

### S3 upload fails with "aws: command not found"

**Fix:** Install AWS CLI:
```bash
# macOS
brew install awscli

# Ubuntu/Debian
sudo apt install awscli

# Configure credentials
aws configure
```

### Slack notifications not working

**Fix:** Test webhook manually:
```bash
curl -X POST https://hooks.slack.com/services/YOUR/WEBHOOK \
  -H "Content-Type: application/json" \
  -d '{"text": "Test notification"}'
```

---

## 📚 Related Documentation

- [Production Readiness Assessment](../docs/DASHBOARD_PRODUCTION_ASSESSMENT.md)
- [Operational Runbook](../docs/ops/RUNBOOK.md)
- [Backup/Restore API Documentation](../docs/backup-restore.md)

---

## ✅ Production Readiness Checklist

- [ ] Backup script deployed to production servers
- [ ] Cron/systemd timer configured for daily backups
- [ ] S3 bucket created and credentials configured
- [ ] Slack webhook configured for notifications
- [ ] Backup retention policy set (default: 30 days)
- [ ] Restore tested successfully in staging environment
- [ ] Monitoring alerts configured for backup freshness
- [ ] DR playbook tested with actual restore
- [ ] Backup/restore procedures documented in runbook
- [ ] Team trained on disaster recovery procedures
