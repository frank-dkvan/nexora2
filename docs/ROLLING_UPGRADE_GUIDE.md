# Nexora 2.0 Rolling Upgrade Support

## Overview

Rolling upgrades allow updating a Nexora cluster **without downtime** by upgrading nodes one at a time while maintaining cluster availability.

---

## Version Compatibility Matrix

### Supported Version Skew

| Scenario | Supported | Notes |
|----------|-----------|-------|
| **Same minor version** (e.g., 2.0.1 ↔ 2.0.2) | ✅ Fully supported | Patch releases are always compatible |
| **Adjacent minor versions** (e.g., 2.0.x ↔ 2.1.x) | ✅ Supported (N-1 policy) | Mixed-version cluster OK for 24h max |
| **Two minor versions** (e.g., 2.0.x ↔ 2.2.x) | ⚠️ Not supported | Upgrade through 2.1.x first |
| **Major version** (e.g., 2.x ↔ 3.x) | ❌ Not supported | Requires full cluster shutdown |

### Protocol Compatibility

Nexora uses **protocol versioning** to ensure compatibility:

```rust
// crates/nexora-raft/src/protocol.rs
pub const PROTOCOL_VERSION: u32 = 2;
pub const MIN_PROTOCOL_VERSION: u32 = 2;
```

- **Wire protocol**: gRPC with protobuf (backward compatible by default)
- **Raft messages**: Versioned with fallback to common subset
- **Event log format**: Iceberg schema evolution (forward/backward compatible)
- **RocksDB format**: Stable across minor versions

---

## Rolling Upgrade Procedure

### Prerequisites

1. ✅ Cluster is healthy (no partitions, all nodes reachable)
2. ✅ Replication lag < 1 second on all shards
3. ✅ No ongoing failovers or rebalancing
4. ✅ Backup created before upgrade starts
5. ✅ New version tested in staging environment

### Step-by-Step Process

#### Phase 1: Preparation (5-10 minutes)

```bash
# 1. Verify cluster health
curl http://node-1:8080/api/cluster/stats | jq '{
  active_nodes: .active_nodes,
  raft_leader: .raft_leader,
  replication_lag_max_ms: .replication_lag_max_ms
}'

# Expected output:
# {
#   "active_nodes": 3,
#   "raft_leader": "node-1",
#   "replication_lag_max_ms": 15
# }

# 2. Create pre-upgrade backup
./scripts/backup-nexora.sh --verify

# 3. Enable elevated monitoring
# Set Prometheus scrape interval to 5s (from 15s)
# Watch Grafana dashboard: monitoring/grafana-dashboards/nexora-replication.json
```

#### Phase 2: Upgrade Followers First (10-15 minutes per node)

**Rule:** Always upgrade followers before the leader to minimize leadership disruption.

```bash
# 1. Identify Raft leader
LEADER=$(curl -s http://node-1:8080/api/cluster/raft | jq -r '.leader')
echo "Current leader: $LEADER"

# 2. Upgrade non-leader nodes first (e.g., node-2)
ssh node-2

# 3. Drain node (optional, for zero-disruption)
curl -X POST http://localhost:8080/api/admin/drain
# Wait for drain to complete (30s-2min)

# 4. Stop service
sudo systemctl stop nexora

# 5. Backup old binary
sudo cp /usr/local/bin/nexora /usr/local/bin/nexora.old

# 6. Deploy new binary
sudo cp nexora-v2.1.0 /usr/local/bin/nexora
sudo chmod +x /usr/local/bin/nexora

# 7. Start service
sudo systemctl start nexora

# 8. Verify node rejoined cluster
curl http://localhost:8080/api/health/ready
curl http://node-1:8080/api/cluster/stats | jq '.active_nodes'
# Should show 3 nodes

# 9. Check logs for version mismatch warnings
sudo journalctl -u nexora -f | grep -i "version\|protocol"
# Expected: "INFO: Mixed version cluster detected: 2.0.5 (node-2), 2.0.4 (node-1, node-3)"

# 10. Monitor replication lag
curl http://localhost:8080/metrics | grep deepstreaming_replication_lag_ms
# Should be < 100ms

# 11. Repeat for node-3
```

#### Phase 3: Upgrade Leader Last (10-15 minutes)

**Critical:** Upgrading the leader triggers a leadership election (5-15 second disruption).

```bash
# 1. Verify followers are healthy on new version
for node in node-2 node-3; do
  echo "=== $node ==="
  curl -s http://$node:8080/api/health/ready
  curl -s http://$node:8080/api/system/info | jq '{version: .version, uptime_secs: .uptime_secs}'
done

# 2. Drain leader (node-1)
curl -X POST http://node-1:8080/api/admin/drain

# 3. Stop service
ssh node-1
sudo systemctl stop nexora

# At this moment: Leadership election happens (~5-15 seconds)
# New leader will be elected from node-2 or node-3

# 4. Deploy new binary
sudo cp nexora-v2.1.0 /usr/local/bin/nexora
sudo chmod +x /usr/local/bin/nexora

# 5. Start service
sudo systemctl start nexora

# 6. Verify cluster converged
curl http://node-1:8080/api/cluster/stats | jq '{
  active_nodes: .active_nodes,
  raft_leader: .raft_leader,
  version_distribution: .version_distribution
}'

# Expected output:
# {
#   "active_nodes": 3,
#   "raft_leader": "node-2",  # New leader elected
#   "version_distribution": {
#     "2.1.0": 3
#   }
# }
```

#### Phase 4: Post-Upgrade Validation (10-15 minutes)

```bash
# 1. Run smoke tests
./test-queries.sh

# 2. Verify all features work
# - Cypher query
curl -X POST http://node-1:8080/api/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) RETURN count(n) as total"}' | jq .

# - SQL query
curl -X POST http://node-1:8080/api/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT COUNT(*) FROM nodes"}' | jq .

# - Standing query
curl http://node-1:8080/api/standing-queries | jq '.[] | {id, status}'

# 3. Check metrics for anomalies
curl http://node-1:8080/api/metrics | jq '{
  active_nodes: .active_nodes,
  query_error_rate: (.errors_total / .queries_total),
  replication_lag_max_ms: .replication_lag_max_ms
}'

# 4. Verify event log (if event-first enabled)
curl -X POST http://node-1:8080/api/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT COUNT(*) FROM events"}' | jq .

# 5. Monitor for 1 hour
# Watch Grafana dashboard for:
# - No spike in error rate
# - Replication lag remains low
# - No failover events
# - CPU/memory stable
```

---

## Rollback Procedure

If issues are detected after upgrading, **rollback immediately**:

```bash
# 1. Stop problematic node
ssh node-X
sudo systemctl stop nexora

# 2. Restore old binary
sudo cp /usr/local/bin/nexora.old /usr/local/bin/nexora

# 3. Start service
sudo systemctl start nexora

# 4. Verify cluster health
curl http://node-1:8080/api/cluster/stats
```

**Full Rollback (if entire cluster is upgraded):**

```bash
# 1. Stop all nodes
for node in node-{1..3}; do
  ssh $node "sudo systemctl stop nexora"
done

# 2. Restore old binaries on all nodes
for node in node-{1..3}; do
  ssh $node "sudo cp /usr/local/bin/nexora.old /usr/local/bin/nexora"
done

# 3. Restore from pre-upgrade backup (if data corruption)
./scripts/restore-nexora.sh --no-pre-backup --force /data/backups/pre-upgrade.nxbak

# 4. Start all nodes
for node in node-{1..3}; do
  ssh $node "sudo systemctl start nexora"
done
```

---

## Mixed-Version Cluster Behavior

### Supported Operations

During a rolling upgrade, the following operations **continue working**:

- ✅ Cypher queries (read/write)
- ✅ SQL queries (read-only)
- ✅ Standing queries (existing SQs continue matching)
- ✅ Event ingestion
- ✅ Replication (Raft log sync)
- ✅ Failover (automatic shard recovery)
- ✅ Read repair and anti-entropy

### Unsupported Operations

The following operations are **blocked** during a mixed-version cluster:

- ❌ Adding new nodes (`POST /api/cluster/add-node`)
- ❌ Removing nodes (`POST /api/cluster/remove-node`)
- ❌ Rebalancing shards
- ❌ Schema migrations (wait for uniform version)
- ❌ Creating new standing queries (may fail if leader/follower mismatch)

**Mixed-version safety:** Nexora detects mixed versions and rejects unsafe operations:

```json
{
  "error": "Mixed version cluster detected (2.0.4, 2.1.0). Operation 'add-node' requires uniform version.",
  "versions": ["2.0.4", "2.1.0"],
  "recommendation": "Complete rolling upgrade before adding nodes."
}
```

---

## Upgrade Testing in Staging

### Test Matrix

| Test ID | Scenario | Expected Result |
|---------|----------|-----------------|
| **T1** | Upgrade 1 follower, run queries | ✅ No errors, lag < 100ms |
| **T2** | Upgrade 2 followers, run queries | ✅ No errors, lag < 100ms |
| **T3** | Upgrade leader, verify election | ✅ New leader elected in < 15s |
| **T4** | Mixed-version: try add-node | ❌ Rejected with error |
| **T5** | Mixed-version: trigger failover | ✅ Failover succeeds |
| **T6** | Rollback 1 node | ✅ Node rejoins with old version |
| **T7** | Full upgrade, run smoke tests | ✅ All tests pass |

### Automated Test Script

```bash
#!/usr/bin/env bash
# test-rolling-upgrade.sh

set -euo pipefail

echo "=== Rolling Upgrade Test ==="

# T1: Upgrade node-2 (follower)
echo "T1: Upgrading node-2..."
ssh node-2 "sudo systemctl stop nexora && \
  sudo cp /tmp/nexora-v2.1.0 /usr/local/bin/nexora && \
  sudo systemctl start nexora"
sleep 10
curl -f http://node-1:8080/api/cluster/stats | jq '.active_nodes' | grep -q 3
echo "✅ T1 passed"

# T2: Upgrade node-3 (follower)
echo "T2: Upgrading node-3..."
ssh node-3 "sudo systemctl stop nexora && \
  sudo cp /tmp/nexora-v2.1.0 /usr/local/bin/nexora && \
  sudo systemctl start nexora"
sleep 10
curl -f http://node-1:8080/api/cluster/stats | jq '.active_nodes' | grep -q 3
echo "✅ T2 passed"

# T3: Upgrade node-1 (leader)
echo "T3: Upgrading leader..."
ssh node-1 "sudo systemctl stop nexora && \
  sudo cp /tmp/nexora-v2.1.0 /usr/local/bin/nexora && \
  sudo systemctl start nexora"
sleep 15
NEW_LEADER=$(curl -s http://node-2:8080/api/cluster/raft | jq -r '.leader')
echo "New leader: $NEW_LEADER"
[[ "$NEW_LEADER" != "node-1" ]] || { echo "❌ Leader did not change"; exit 1; }
echo "✅ T3 passed"

# T4: Verify all nodes on new version
echo "T4: Verifying versions..."
for node in node-{1..3}; do
  VERSION=$(curl -s http://$node:8080/api/system/info | jq -r '.version')
  echo "  $node: $VERSION"
  [[ "$VERSION" == "2.1.0" ]] || { echo "❌ Version mismatch"; exit 1; }
done
echo "✅ T4 passed"

echo "=== All tests passed ==="
```

---

## Known Upgrade Issues

### Issue 1: RocksDB Column Family Changes

**Versions affected:** 2.0.x → 2.1.0

**Symptom:** Node fails to start with "Unknown column family" error

**Workaround:**
```bash
# RocksDB migration is automatic, but requires WAL replay
# Ensure WAL is not corrupted before upgrade
```

### Issue 2: Event Log Schema Evolution

**Versions affected:** 2.0.x (local catalog) → 2.1.0 (REST catalog)

**Symptom:** Event queries fail after upgrade

**Workaround:**
```bash
# Upgrade catalog first:
# 1. Migrate SQLite catalog to Lakekeeper
# 2. Then upgrade Nexora nodes
```

### Issue 3: Standing Query Format Change

**Versions affected:** 2.0.x → 2.1.0

**Symptom:** Existing standing queries stop matching after upgrade

**Workaround:**
```bash
# Re-register standing queries after upgrade
curl -X DELETE http://node-1:8080/api/standing-queries/$OLD_SQ_ID
curl -X POST http://node-1:8080/api/standing-queries \
  -H "Content-Type: application/json" \
  -d '{"query": "...", "output": "..."}'
```

---

## Version-Specific Upgrade Notes

### 2.0.4 → 2.0.5

- **Changes:** Bug fixes only, no protocol changes
- **Upgrade time:** 30 minutes (3-node cluster)
- **Downtime:** ~10 seconds during leader upgrade
- **Special notes:** None

### 2.0.x → 2.1.0

- **Changes:** Event log REST catalog support, new metrics
- **Upgrade time:** 45 minutes (3-node cluster)
- **Downtime:** ~15 seconds during leader upgrade
- **Special notes:** 
  - Existing local catalogs remain functional
  - REST catalog is opt-in (`--event-store-backend rest`)
  - New metrics exposed (see `monitoring/prometheus-alerts/nexora-alerts.yml`)

### 2.1.x → 2.2.0 (planned)

- **Changes:** TBD
- **Upgrade time:** TBD
- **Special notes:** TBD

---

## Monitoring During Upgrade

### Critical Metrics

Watch these metrics during rolling upgrade:

```promql
# Cluster size (should remain 3 during upgrade)
sum(up{job="nexora"})

# Replication lag (should stay < 100ms)
max(deepstreaming_replication_lag_ms)

# Error rate (should not spike)
rate(deepstreaming_errors_total[5m]) / rate(deepstreaming_queries_total[5m])

# Failover events (should be 0 or 1 during leader upgrade)
increase(deepstreaming_failover_total[30m])

# Leadership changes (should be 1 during leader upgrade)
changes(nexora_raft_leader_id[30m])
```

### Grafana Dashboard

Use existing dashboard: `monitoring/grafana-dashboards/nexora-replication.json`

Add annotation for upgrade events:
```bash
curl -X POST http://grafana:3000/api/annotations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $GRAFANA_API_KEY" \
  -d '{
    "dashboardId": 1,
    "time": '$(date +%s000)',
    "tags": ["upgrade", "node-2"],
    "text": "Started upgrade of node-2 to v2.1.0"
  }'
```

---

## Production Readiness Checklist

Before performing a rolling upgrade in production:

- [ ] Tested rolling upgrade in staging environment
- [ ] All tests in test matrix passed
- [ ] Backup created and verified
- [ ] Change window scheduled (e.g., 2 AM-4 AM)
- [ ] On-call engineer paged and ready
- [ ] Rollback plan reviewed with team
- [ ] Grafana dashboard open and monitored
- [ ] Slack notifications configured
- [ ] Customer notification sent (if applicable)
- [ ] Post-upgrade smoke tests prepared

---

## Related Documentation

- [Production Readiness Assessment](../docs/DASHBOARD_PRODUCTION_ASSESSMENT.md)
- [Operational Runbook](../docs/ops/RUNBOOK.md)
- [Cluster Operations Guide](../docs/cluster-ops.md)
