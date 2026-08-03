# Nexora 2.0 Disaster Recovery Manual

**Version**: 1.0  
**Last Updated**: 2026-08-03  
**Next Review**: 2026-09-03  
**Owner**: Operations Team

---

## Table of Contents

1. [Overview](#overview)
2. [RTO/RPO Definitions](#rtorpo-definitions)
3. [Architecture Overview](#architecture-overview)
4. [Backup Strategy](#backup-strategy)
5. [Recovery Procedures](#recovery-procedures)
6. [Validation Steps](#validation-steps)
7. [Drill Schedule](#drill-schedule)
8. [Emergency Contacts](#emergency-contacts)

---

## Overview

### Purpose

This manual provides comprehensive procedures for recovering Nexora 2.0 from various disaster scenarios, including:
- Hardware failures
- Data corruption
- Network partitions
- Complete datacenter outage
- Human error (accidental deletion)

### Scope

**In Scope**:
- Single node failure recovery
- Multi-node cluster failure
- Data corruption scenarios
- Backup and restore procedures
- Network partition recovery

**Out of Scope**:
- Application-level bugs (see incident response playbook)
- Performance degradation (see operations manual)
- Security incidents (see security incident response)

### Prerequisites

**Required Access**:
- SSH access to all Nexora nodes
- S3/MinIO credentials for Iceberg storage
- Database admin credentials
- Monitoring system access

**Required Tools**:
- `nexora-cli` (admin tool)
- AWS CLI or MinIO client (`mc`)
- `rsync` for file transfers
- `jq` for JSON processing

---

## RTO/RPO Definitions

### Recovery Time Objective (RTO)

**Target RTO: 30 minutes**

Time from disaster declaration to service restoration.

| Scenario | Target RTO | Actual RTO (tested) |
|----------|------------|---------------------|
| Single node failure | 5 minutes | TBD (P1-8) |
| Multi-node failure (< quorum loss) | 15 minutes | TBD (P1-8) |
| Complete cluster failure | 30 minutes | TBD (P1-8) |
| Data corruption | 30 minutes | TBD (P1-8) |

### Recovery Point Objective (RPO)

**Target RPO: 1 minute**

Maximum acceptable data loss.

| Component | RPO | Mechanism |
|-----------|-----|-----------|
| Graph state | 1 minute | RocksDB WAL + Raft log |
| Event log | 0 (zero data loss) | Iceberg immutable files |
| Raft consensus state | 1 minute | Raft log replication |

---

## Architecture Overview

### Component Topology

```
┌─────────────────────────────────────────────────────────────┐
│                    Load Balancer                             │
│                   (HAProxy / Nginx)                          │
└────────────┬───────────────┬────────────────┬───────────────┘
             │               │                │
        ┌────▼────┐     ┌────▼────┐     ┌────▼────┐
        │ Node 1  │     │ Node 2  │     │ Node 3  │
        │ (Leader)│     │(Follower)│    │(Follower)│
        └────┬────┘     └────┬────┘     └────┬────┘
             │               │                │
             │    Raft Replication (TCP)     │
             └───────────────┴────────────────┘
                           │
                    ┌──────▼──────────┐
                    │   Shared Storage │
                    │  (S3 / MinIO)    │
                    │  Iceberg Tables  │
                    └──────────────────┘
```

### Data Storage Layers

1. **RocksDB (Local)**
   - Location: `/data/nexora/graph/`
   - Contains: Graph nodes, edges, properties
   - Backup: WAL + periodic snapshots

2. **Iceberg Event Log (S3)**
   - Location: `s3://nexora-events/`
   - Contains: Immutable event log
   - Backup: S3 versioning + cross-region replication

3. **Raft Log (Local)**
   - Location: `/data/nexora/raft/`
   - Contains: Consensus state, cluster config
   - Backup: Replicated across quorum

---

## Backup Strategy

### Automated Backups

#### 1. RocksDB Checkpoints

**Frequency**: Every 15 minutes  
**Retention**: 48 hours (192 checkpoints)  
**Location**: `/data/nexora/checkpoints/`

**Implementation**:
```bash
# Automated via systemd timer
0,15,30,45 * * * * /usr/local/bin/nexora-backup.sh checkpoint
```

**Script** (`/usr/local/bin/nexora-backup.sh`):
```bash
#!/bin/bash
set -euo pipefail

CHECKPOINT_DIR="/data/nexora/checkpoints"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CHECKPOINT_PATH="${CHECKPOINT_DIR}/${TIMESTAMP}"

# Create checkpoint via admin API
curl -X POST http://localhost:8080/admin/checkpoint \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -d "{\"path\": \"${CHECKPOINT_PATH}\"}"

# Upload to S3
aws s3 sync "${CHECKPOINT_PATH}" \
  "s3://nexora-backups/checkpoints/${HOSTNAME}/${TIMESTAMP}/" \
  --storage-class STANDARD_IA

# Cleanup old checkpoints (keep 48 hours)
find "${CHECKPOINT_DIR}" -type d -mtime +2 -exec rm -rf {} \;
```

#### 2. Iceberg Snapshots

**Frequency**: Continuous (immutable writes)  
**Retention**: 30 days (configurable)  
**Location**: `s3://nexora-events/snapshots/`

**Configuration** (`nexora.toml`):
```toml
[event_store]
backend = "rest"
rest_uri = "http://localhost:8181/catalog"
snapshot_retention_days = 30
```

**Iceberg guarantees**:
- All writes are atomic
- Snapshots are immutable
- Time-travel queries supported
- No data loss on failure

---

#### 3. Raft Log Backup

**Frequency**: Continuous replication  
**Retention**: Last 1000 entries per node  
**Location**: Replicated across quorum

**No manual backup needed** - Raft automatically:
- Replicates logs to all followers
- Commits entries to majority
- Snapshots compacted logs

---

## Recovery Procedures

### Scenario 1: Single Node Failure

**RTO**: 5 minutes  
**RPO**: 0 (no data loss)

#### Symptoms
- Node unreachable via monitoring
- Raft cluster reports missing follower
- Queries still succeed (quorum maintained)

#### Recovery Steps

**Step 1: Assess Impact**
```bash
# Check cluster status
curl http://node2:8080/admin/raft/status | jq .

# Expected output:
# {
#   "state": "Leader",
#   "members": [
#     {"id": "node1", "status": "Unreachable"},  # Failed node
#     {"id": "node2", "status": "Healthy"},
#     {"id": "node3", "status": "Healthy"}
#   ],
#   "quorum": "Maintained"
# }
```

**Step 2: Isolate Failed Node**
```bash
# Remove from load balancer
ssh loadbalancer "systemctl reload haproxy"
# OR
# Remove from DNS round-robin
```

**Step 3: Diagnose Failure**
```bash
# Check hardware
ssh node1 "dmesg | tail -50"

# Check disk space
ssh node1 "df -h"

# Check service logs
ssh node1 "journalctl -u nexora -n 100"
```

**Step 4A: Restart Service (if software issue)**
```bash
ssh node1 "systemctl restart nexora"

# Wait for rejoin (30-60 seconds)
watch -n 5 'curl -s http://node2:8080/admin/raft/status | jq .state'
```

**Step 4B: Restore from Backup (if data corruption)**
```bash
# Stop service
ssh node1 "systemctl stop nexora"

# Restore latest checkpoint
ssh node1 "
  rm -rf /data/nexora/graph/*
  aws s3 sync s3://nexora-backups/checkpoints/node1/latest/ /data/nexora/graph/
"

# Restart and rejoin
ssh node1 "systemctl start nexora"
```

**Step 5: Verify Recovery**
```bash
# Check node health
curl http://node1:8080/health | jq .

# Check Raft status
curl http://node1:8080/admin/raft/status | jq .state

# Run smoke test
curl -X POST http://node1:8080/api/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) RETURN count(n) as total"}'
```

**Step 6: Re-enable in Load Balancer**
```bash
ssh loadbalancer "systemctl reload haproxy"
```

---

### Scenario 2: Multi-Node Failure (Quorum Loss)

**RTO**: 15 minutes  
**RPO**: 1 minute

#### Symptoms
- 2+ nodes unreachable
- Cluster cannot elect leader
- All writes fail (reads may work from stale data)

#### Recovery Steps

**Step 1: Emergency Assessment**
```bash
# Check which nodes are alive
for node in node1 node2 node3; do
  echo "=== $node ==="
  ping -c 1 $node && echo "REACHABLE" || echo "UNREACHABLE"
done

# Check Raft state on surviving node(s)
curl http://surviving-node:8080/admin/raft/status
```

**Step 2: Force Leader Election (if 1 node alive)**
```bash
# DANGEROUS: Only use if quorum permanently lost
curl -X POST http://surviving-node:8080/admin/raft/force-leader \
  -H "Authorization: Bearer ${ADMIN_TOKEN}"

# This makes surviving node a single-node cluster
# Data may be stale by up to RPO (1 minute)
```

**Step 3: Restore Failed Nodes**

For each failed node:
```bash
# Restore from latest checkpoint
ssh node1 "
  systemctl stop nexora
  rm -rf /data/nexora/graph/*
  rm -rf /data/nexora/raft/*
  
  # Restore checkpoint
  aws s3 sync s3://nexora-backups/checkpoints/node1/latest/ /data/nexora/graph/
  
  systemctl start nexora
"
```

**Step 4: Rebuild Cluster**
```bash
# Add recovered nodes back to cluster
curl -X POST http://leader:8080/admin/raft/add-member \
  -d '{"id": "node2", "address": "node2:8080"}'

curl -X POST http://leader:8080/admin/raft/add-member \
  -d '{"id": "node3", "address": "node3:8080"}'

# Wait for replication to complete
watch -n 5 'curl -s http://leader:8080/admin/raft/status | jq .members'
```

**Step 5: Verify Cluster Health**
```bash
# Check all nodes report same state
for node in node1 node2 node3; do
  echo "=== $node ==="
  curl -s http://$node:8080/admin/raft/status | jq .
done

# Run consistency check
./scripts/verify-cluster-consistency.sh
```

---

### Scenario 3: Complete Cluster Failure

**RTO**: 30 minutes  
**RPO**: 1 minute

#### Symptoms
- All nodes unreachable
- Total service outage
- Possible datacenter failure

#### Recovery Steps

**Step 1: Assess Datacenter Status**
```bash
# Check infrastructure
ping -c 5 datacenter-gateway
ssh bastion "systemctl status network"

# Check if storage accessible
aws s3 ls s3://nexora-backups/ || echo "S3 UNREACHABLE"
```

**Step 2: Provision New Cluster (if datacenter lost)**
```bash
# Deploy 3 new nodes in different AZ
terraform apply -target=module.nexora_cluster

# Install Nexora on each node
ansible-playbook -i inventory/dr.yml playbooks/install-nexora.yml
```

**Step 3: Restore from Backups**

On **Node 1** (will be initial leader):
```bash
# Restore latest checkpoint
aws s3 sync s3://nexora-backups/checkpoints/node1/latest/ /data/nexora/graph/

# Initialize as single-node cluster
/usr/local/bin/nexora \
  --data-dir /data/nexora \
  --raft-id node1 \
  --raft-addr node1:8081 \
  --bootstrap-cluster true
```

**Step 4: Verify Data Integrity**
```bash
# Check graph data
curl http://node1:8080/api/query/cypher \
  -d '{"query": "MATCH (n) RETURN count(n) as nodes"}'

# Check event log
curl http://node1:8080/api/events/topics | jq .
```

**Step 5: Add Follower Nodes**
```bash
# Start node2 and node3
ssh node2 "/usr/local/bin/nexora --data-dir /data/nexora --raft-id node2 --raft-addr node2:8081"
ssh node3 "/usr/local/bin/nexora --data-dir /data/nexora --raft-id node3 --raft-addr node3:8081"

# Add to cluster
curl -X POST http://node1:8080/admin/raft/add-member \
  -d '{"id": "node2", "address": "node2:8081"}'
curl -X POST http://node1:8080/admin/raft/add-member \
  -d '{"id": "node3", "address": "node3:8081"}'
```

**Step 6: Rebuild Event Log State (if needed)**
```bash
# Event log is in S3 - automatically available
# Verify Iceberg catalog accessible
curl http://localhost:8181/v1/namespaces
```

---

### Scenario 4: Data Corruption

**RTO**: 30 minutes  
**RPO**: 15 minutes (last checkpoint)

#### Symptoms
- RocksDB errors in logs: "Corruption: ..."
- Queries return inconsistent results
- Service crashes on startup

#### Recovery Steps

**Step 1: Identify Corruption Scope**
```bash
# Check RocksDB integrity
/usr/local/bin/nexora-tools check-db /data/nexora/graph/

# Check Raft log
/usr/local/bin/nexora-tools check-raft /data/nexora/raft/

# Sample output:
# ERROR: SST file corrupted: 000123.sst
# ERROR: Block checksum mismatch at offset 4096
```

**Step 2: Stop Affected Node**
```bash
systemctl stop nexora
```

**Step 3: Restore from Checkpoint**
```bash
# Backup corrupted data for forensics
mv /data/nexora/graph /data/nexora/graph.corrupted.$(date +%s)

# Restore latest valid checkpoint
LATEST_CHECKPOINT=$(aws s3 ls s3://nexora-backups/checkpoints/$(hostname)/ | sort | tail -1 | awk '{print $NF}')
aws s3 sync "s3://nexora-backups/checkpoints/$(hostname)/${LATEST_CHECKPOINT}" /data/nexora/graph/
```

**Step 4: Replay Missing Events (if needed)**
```bash
# Calculate checkpoint timestamp
CHECKPOINT_TIME=$(echo $LATEST_CHECKPOINT | sed 's/_/ /')

# Query Iceberg for events after checkpoint
curl http://localhost:8181/v1/namespaces/nexora/tables/events/snapshots | \
  jq --arg ts "$CHECKPOINT_TIME" '.snapshots[] | select(.timestamp > $ts)'

# Replay events (automatic on startup)
systemctl start nexora
```

**Step 5: Verify Data Consistency**
```bash
# Wait for replay to complete
tail -f /var/log/nexora/nexora.log | grep "Replay complete"

# Run integrity check
/usr/local/bin/nexora-tools verify-consistency http://localhost:8080
```

---

### Scenario 5: Network Partition (Split Brain)

**RTO**: 5 minutes  
**RPO**: 0 (no data loss with proper quorum)

#### Symptoms
- Cluster reports multiple leaders
- Different nodes return different data
- Network monitoring shows connectivity issues

#### Recovery Steps

**Step 1: Identify Partition**
```bash
# Check Raft state on all nodes
for node in node1 node2 node3; do
  echo "=== $node ==="
  curl -s http://$node:8080/admin/raft/status | jq '{state: .state, term: .term}'
done

# Expected during partition:
# node1: {state: "Leader", term: 42}
# node2: {state: "Follower", term: 42}
# node3: {state: "Candidate", term: 43}  # Isolated, trying to elect
```

**Step 2: Verify Network Connectivity**
```bash
# Test node-to-node connectivity
ansible nexora -m ping

# Check network routes
traceroute node3
```

**Step 3: Restore Network**
```bash
# Fix network issue (firewall, routing, etc.)
# Example: Restore firewall rule
ssh node3 "iptables -A INPUT -p tcp --dport 8081 -j ACCEPT"
```

**Step 4: Wait for Auto-Recovery**
```bash
# Raft automatically resolves:
# - Isolated node discovers it's partitioned (no heartbeats)
# - Rejoins cluster as follower
# - Synchronizes from current leader

# Monitor until resolved (usually < 30 seconds)
watch -n 2 'curl -s http://node3:8080/admin/raft/status | jq .state'
```

**Step 5: Manual Intervention (if auto-recovery fails)**
```bash
# Restart isolated node to force rejoin
ssh node3 "systemctl restart nexora"

# If still stuck, remove and re-add to cluster
curl -X POST http://leader:8080/admin/raft/remove-member -d '{"id": "node3"}'
curl -X POST http://leader:8080/admin/raft/add-member -d '{"id": "node3", "address": "node3:8081"}'
```

---

## Validation Steps

### Post-Recovery Checklist

After any recovery procedure, validate the following:

#### 1. Cluster Health
```bash
#!/bin/bash
# Script: validate-cluster-health.sh

set -euo pipefail

echo "=== Checking Raft Cluster ==="
for node in node1 node2 node3; do
  status=$(curl -s http://$node:8080/admin/raft/status | jq -r .state)
  echo "$node: $status"
  
  if [[ "$status" != "Leader" && "$status" != "Follower" ]]; then
    echo "ERROR: $node in unexpected state: $status"
    exit 1
  fi
done

echo "=== Verifying Leader Election ==="
leader_count=$(for node in node1 node2 node3; do
  curl -s http://$node:8080/admin/raft/status | jq -r .state
done | grep -c "Leader")

if [[ "$leader_count" -ne 1 ]]; then
  echo "ERROR: Expected 1 leader, found $leader_count"
  exit 1
fi

echo "✅ Cluster health: OK"
```

#### 2. Data Consistency
```bash
#!/bin/bash
# Script: validate-data-consistency.sh

set -euo pipefail

echo "=== Checking Node Count Consistency ==="
for node in node1 node2 node3; do
  count=$(curl -s -X POST http://$node:8080/api/query/cypher \
    -H "Content-Type: application/json" \
    -d '{"query": "MATCH (n) RETURN count(n) as total"}' | jq -r '.results[0].total')
  
  echo "$node: $count nodes"
  
  if [[ -z "$expected_count" ]]; then
    expected_count=$count
  elif [[ "$count" -ne "$expected_count" ]]; then
    echo "ERROR: Inconsistent node count across cluster"
    exit 1
  fi
done

echo "✅ Data consistency: OK"
```

#### 3. Event Log Integrity
```bash
#!/bin/bash
# Script: validate-event-log.sh

set -euo pipefail

echo "=== Checking Iceberg Tables ==="
tables=$(curl -s http://localhost:8181/v1/namespaces/nexora/tables | jq -r '.tables[].name')

for table in $tables; do
  snapshot_count=$(curl -s http://localhost:8181/v1/namespaces/nexora/tables/$table/snapshots | jq '.snapshots | length')
  echo "$table: $snapshot_count snapshots"
  
  if [[ "$snapshot_count" -eq 0 ]]; then
    echo "ERROR: Table $table has no snapshots"
    exit 1
  fi
done

echo "✅ Event log integrity: OK"
```

#### 4. Performance Baseline
```bash
#!/bin/bash
# Script: validate-performance.sh

set -euo pipefail

echo "=== Running Performance Smoke Test ==="

# Write latency test
start=$(date +%s%3N)
curl -s -X POST http://localhost:8080/api/events/ingest \
  -H "Content-Type: application/json" \
  -d '{"topic": "perf_test", "events": [{"id": "test1", "data": "test"}]}'
end=$(date +%s%3N)
write_latency=$((end - start))

echo "Write latency: ${write_latency}ms"
if [[ "$write_latency" -gt 1000 ]]; then
  echo "WARNING: Write latency high (expected < 1000ms)"
fi

# Read latency test
start=$(date +%s%3N)
curl -s -X POST http://localhost:8080/api/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n:User) RETURN n LIMIT 10"}'
end=$(date +%s%3N)
read_latency=$((end - start))

echo "Read latency: ${read_latency}ms"
if [[ "$read_latency" -gt 500 ]]; then
  echo "WARNING: Read latency high (expected < 500ms)"
fi

echo "✅ Performance baseline: OK"
```

#### 5. End-to-End Test
```bash
#!/bin/bash
# Script: e2e-smoke-test.sh

set -euo pipefail

echo "=== Running E2E Smoke Test ==="

# Create test data
curl -X POST http://localhost:8080/api/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "CREATE (u:TestUser {id: \"recovery_test_$(date +%s)\", name: \"Test\"}) RETURN u"}'

# Query test data
result=$(curl -s -X POST http://localhost:8080/api/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (u:TestUser) WHERE u.id STARTS WITH \"recovery_test_\" RETURN count(u) as count"}' | jq -r '.results[0].count')

if [[ "$result" -lt 1 ]]; then
  echo "ERROR: E2E test failed - no test data found"
  exit 1
fi

# Cleanup
curl -X POST http://localhost:8080/api/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (u:TestUser) WHERE u.id STARTS WITH \"recovery_test_\" DELETE u"}'

echo "✅ E2E test: OK"
```

---

## Drill Schedule

### Quarterly Recovery Drills

**Purpose**: Ensure team readiness and validate recovery procedures

#### Q1 Drill: Single Node Failure

**Date**: First Monday of Q1  
**Duration**: 1 hour  
**Scenario**: Simulated node1 hardware failure

**Steps**:
1. Kill nexora process on node1
2. Execute Scenario 1 recovery procedure
3. Validate all checks pass
4. Document time taken vs RTO target
5. Update runbook with lessons learned

**Success Criteria**:
- Recovery completed within 5 minutes
- Zero data loss validated
- All validation scripts pass

#### Q2 Drill: Quorum Loss

**Date**: First Monday of Q2  
**Duration**: 2 hours  
**Scenario**: Simulated network partition causing quorum loss

**Steps**:
1. Use `iptables` to isolate 2 nodes
2. Execute Scenario 2 recovery procedure
3. Validate cluster re-forms correctly
4. Document any issues encountered

**Success Criteria**:
- Recovery completed within 15 minutes
- Data loss ≤ 1 minute
- Cluster stability after recovery

#### Q3 Drill: Data Corruption

**Date**: First Monday of Q3  
**Duration**: 2 hours  
**Scenario**: Simulated RocksDB corruption

**Steps**:
1. Stop nexora and corrupt SST file
2. Execute Scenario 4 recovery procedure
3. Validate data integrity after restore
4. Verify event replay mechanism

**Success Criteria**:
- Recovery completed within 30 minutes
- Data loss ≤ 15 minutes (last checkpoint)
- All data integrity checks pass

#### Q4 Drill: Complete Datacenter Failover

**Date**: First Monday of Q4  
**Duration**: 4 hours  
**Scenario**: Simulated datacenter outage with DR site activation

**Steps**:
1. Shutdown all production nodes
2. Execute Scenario 3 recovery in DR datacenter
3. Validate full service restoration
4. Test switchback to primary datacenter

**Success Criteria**:
- Recovery completed within 30 minutes
- All services operational in DR site
- Successful switchback without data loss

### Drill Documentation Template

After each drill, complete this template:

```markdown
# Disaster Recovery Drill Report

**Date**: YYYY-MM-DD  
**Scenario**: [Single Node / Quorum Loss / etc.]  
**Participants**: [List names]  
**Start Time**: HH:MM  
**End Time**: HH:MM  
**Total Duration**: X minutes

## Objectives
- [ ] Objective 1
- [ ] Objective 2

## Timeline
| Time | Action | Notes |
|------|--------|-------|
| 00:00 | Started drill | |
| 00:05 | Detected failure | |
| 00:10 | Started recovery | |
| 00:25 | Service restored | |
| 00:30 | Validation complete | |

## Metrics
- **Actual RTO**: X minutes (Target: Y minutes)
- **Actual RPO**: X minutes (Target: Y minutes)
- **Data Loss**: X MB / Y records

## Issues Encountered
1. Issue 1 description
2. Issue 2 description

## Lessons Learned
1. What worked well
2. What needs improvement

## Action Items
- [ ] Update documentation (Owner: X, Due: YYYY-MM-DD)
- [ ] Fix automation script (Owner: Y, Due: YYYY-MM-DD)

## Sign-off
- Drill Lead: [Name] - [Date]
- Operations Manager: [Name] - [Date]
```

---

## Emergency Contacts

### On-Call Rotation

| Role | Primary | Secondary | Phone | Email |
|------|---------|-----------|-------|-------|
| Operations Lead | John Doe | Jane Smith | +1-555-0100 | ops@nexora.io |
| Database Admin | Alice Wang | Bob Chen | +1-555-0101 | dba@nexora.io |
| Platform Engineer | Carol Liu | Dave Kim | +1-555-0102 | platform@nexora.io |
| Engineering Manager | Eve Park | Frank Zhang | +1-555-0103 | eng-mgr@nexora.io |

### Escalation Path

1. **L1**: On-call engineer (responds within 15 minutes)
2. **L2**: Operations lead (responds within 30 minutes)
3. **L3**: Engineering manager (responds within 1 hour)
4. **L4**: VP Engineering (emergency only)

### Vendor Contacts

| Vendor | Purpose | Support URL | Emergency Phone |
|--------|---------|-------------|-----------------|
| AWS | S3 storage | https://console.aws.amazon.com/support | +1-800-AWS-SUPPORT |
| Confluent | Kafka managed service | https://support.confluent.io | +1-888-456-3210 |
| PagerDuty | Alerting | https://support.pagerduty.com | +1-844-700-DUTY |

---

## Appendix A: Backup Script

**Location**: `/usr/local/bin/nexora-backup.sh`

```bash
#!/bin/bash
# Nexora Backup Script
# Version: 1.0
# Usage: nexora-backup.sh [checkpoint|full]

set -euo pipefail

BACKUP_TYPE="${1:-checkpoint}"
CHECKPOINT_DIR="/data/nexora/checkpoints"
GRAPH_DIR="/data/nexora/graph"
RAFT_DIR="/data/nexora/raft"
S3_BUCKET="s3://nexora-backups"
HOSTNAME=$(hostname)
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

create_checkpoint() {
    log "Creating checkpoint..."
    
    CHECKPOINT_PATH="${CHECKPOINT_DIR}/${TIMESTAMP}"
    mkdir -p "${CHECKPOINT_PATH}"
    
    # Create RocksDB checkpoint via API
    curl -X POST http://localhost:8080/admin/checkpoint \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -d "{\"path\": \"${CHECKPOINT_PATH}\"}" \
        -o /tmp/checkpoint_result.json
    
    if ! jq -e '.success' /tmp/checkpoint_result.json > /dev/null; then
        log "ERROR: Checkpoint creation failed"
        cat /tmp/checkpoint_result.json
        exit 1
    fi
    
    log "Checkpoint created: ${CHECKPOINT_PATH}"
    
    # Upload to S3
    log "Uploading to S3..."
    aws s3 sync "${CHECKPOINT_PATH}" \
        "${S3_BUCKET}/checkpoints/${HOSTNAME}/${TIMESTAMP}/" \
        --storage-class STANDARD_IA
    
    log "Upload complete"
    
    # Cleanup old local checkpoints (keep 48 hours)
    log "Cleaning old checkpoints..."
    find "${CHECKPOINT_DIR}" -type d -mtime +2 -exec rm -rf {} \; 2>/dev/null || true
    
    log "Backup complete: ${TIMESTAMP}"
}

full_backup() {
    log "Starting full backup..."
    
    # Stop service for consistent backup
    systemctl stop nexora
    
    # Backup graph data
    log "Backing up graph data..."
    tar -czf "/tmp/nexora_graph_${TIMESTAMP}.tar.gz" -C "${GRAPH_DIR}" .
    aws s3 cp "/tmp/nexora_graph_${TIMESTAMP}.tar.gz" \
        "${S3_BUCKET}/full/${HOSTNAME}/"
    
    # Backup Raft state
    log "Backing up Raft state..."
    tar -czf "/tmp/nexora_raft_${TIMESTAMP}.tar.gz" -C "${RAFT_DIR}" .
    aws s3 cp "/tmp/nexora_raft_${TIMESTAMP}.tar.gz" \
        "${S3_BUCKET}/full/${HOSTNAME}/"
    
    # Restart service
    systemctl start nexora
    
    # Cleanup temp files
    rm -f "/tmp/nexora_graph_${TIMESTAMP}.tar.gz"
    rm -f "/tmp/nexora_raft_${TIMESTAMP}.tar.gz"
    
    log "Full backup complete: ${TIMESTAMP}"
}

case "$BACKUP_TYPE" in
    checkpoint)
        create_checkpoint
        ;;
    full)
        full_backup
        ;;
    *)
        echo "Usage: $0 [checkpoint|full]"
        exit 1
        ;;
esac
```

---

## Appendix B: Recovery Script

**Location**: `/usr/local/bin/nexora-restore.sh`

```bash
#!/bin/bash
# Nexora Restore Script
# Version: 1.0
# Usage: nexora-restore.sh [checkpoint|full] <timestamp>

set -euo pipefail

RESTORE_TYPE="${1:-checkpoint}"
TIMESTAMP="${2:-latest}"
CHECKPOINT_DIR="/data/nexora/checkpoints"
GRAPH_DIR="/data/nexora/graph"
RAFT_DIR="/data/nexora/raft"
S3_BUCKET="s3://nexora-backups"
HOSTNAME=$(hostname)

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

restore_checkpoint() {
    log "Restoring checkpoint: ${TIMESTAMP}"
    
    # Stop service
    log "Stopping Nexora..."
    systemctl stop nexora
    
    # Backup current state
    if [ -d "${GRAPH_DIR}" ]; then
        log "Backing up current state..."
        mv "${GRAPH_DIR}" "${GRAPH_DIR}.pre-restore.$(date +%s)"
    fi
    
    # Download from S3
    log "Downloading checkpoint from S3..."
    if [ "${TIMESTAMP}" = "latest" ]; then
        TIMESTAMP=$(aws s3 ls "${S3_BUCKET}/checkpoints/${HOSTNAME}/" | sort | tail -1 | awk '{print $NF}' | sed 's|/||')
    fi
    
    mkdir -p "${GRAPH_DIR}"
    aws s3 sync "${S3_BUCKET}/checkpoints/${HOSTNAME}/${TIMESTAMP}/" "${GRAPH_DIR}/"
    
    # Fix permissions
    chown -R nexora:nexora "${GRAPH_DIR}"
    
    # Start service
    log "Starting Nexora..."
    systemctl start nexora
    
    # Wait for service to be ready
    log "Waiting for service..."
    for i in {1..30}; do
        if curl -s http://localhost:8080/health | jq -e '.status == "healthy"' > /dev/null; then
            log "Service ready"
            break
        fi
        sleep 2
    done
    
    log "Restore complete: ${TIMESTAMP}"
}

restore_full() {
    log "Restoring full backup: ${TIMESTAMP}"
    
    # Stop service
    log "Stopping Nexora..."
    systemctl stop nexora
    
    # Backup current state
    for dir in "${GRAPH_DIR}" "${RAFT_DIR}"; do
        if [ -d "$dir" ]; then
            log "Backing up $(basename $dir)..."
            mv "$dir" "${dir}.pre-restore.$(date +%s)"
        fi
    done
    
    # Download and extract
    log "Downloading full backup from S3..."
    aws s3 cp "${S3_BUCKET}/full/${HOSTNAME}/nexora_graph_${TIMESTAMP}.tar.gz" /tmp/
    aws s3 cp "${S3_BUCKET}/full/${HOSTNAME}/nexora_raft_${TIMESTAMP}.tar.gz" /tmp/
    
    log "Extracting graph data..."
    mkdir -p "${GRAPH_DIR}"
    tar -xzf "/tmp/nexora_graph_${TIMESTAMP}.tar.gz" -C "${GRAPH_DIR}"
    
    log "Extracting Raft state..."
    mkdir -p "${RAFT_DIR}"
    tar -xzf "/tmp/nexora_raft_${TIMESTAMP}.tar.gz" -C "${RAFT_DIR}"
    
    # Fix permissions
    chown -R nexora:nexora "${GRAPH_DIR}" "${RAFT_DIR}"
    
    # Cleanup
    rm -f "/tmp/nexora_graph_${TIMESTAMP}.tar.gz"
    rm -f "/tmp/nexora_raft_${TIMESTAMP}.tar.gz"
    
    # Start service
    log "Starting Nexora..."
    systemctl start nexora
    
    log "Full restore complete: ${TIMESTAMP}"
}

case "$RESTORE_TYPE" in
    checkpoint)
        restore_checkpoint
        ;;
    full)
        restore_full
        ;;
    *)
        echo "Usage: $0 [checkpoint|full] <timestamp>"
        exit 1
        ;;
esac
```

---

## Appendix C: Validation Scripts

All validation scripts should be placed in `/usr/local/bin/nexora-validate/`

Run all validations:
```bash
/usr/local/bin/nexora-validate/run-all.sh
```

Individual scripts referenced in [Validation Steps](#validation-steps) section.

---

## Document Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-08-03 | Claude | Initial version |
| | | | Next update after Q1 drill |

---

**END OF DISASTER RECOVERY MANUAL**

For questions or updates, contact: ops@nexora.io

