# Backup and Restore Guide

This guide covers backup strategies, procedures, and best practices for Nexora-RS deployments.

## Overview

Nexora-RS provides multiple layers of data durability:

| Layer | Purpose | Scope |
|-------|---------|-------|
| WAL (Write-Ahead Log) | Crash recovery | Single node — automatic |
| RocksDB SST files | Primary persistent storage | Single node — on disk |
| Raft log replication | Multi-node redundancy | Cluster — automatic |
| RocksDB snapshots | Point-in-time backups | Manual or scripted |

---

## WAL-Based Recovery

### How It Works

When WAL is enabled (default in `single-durable` and `clustered` profiles), every mutation is written to the WAL before being applied to the in-memory graph. On startup, Nexora-RS replays the WAL to restore the last consistent state.

```
Write Path: Mutation → WAL append → In-memory graph update → Periodic RocksDB flush
Recovery:   Startup → WAL replay → Graph state restored
```

### Configuration

```bash
nexora-app \
  --rocksdb-path /var/lib/nexora/data \
  --wal-dir /var/lib/nexora/wal
```

| Flag | Default | Description |
|------|---------|-------------|
| `--wal-dir` | `./nexora-data/wal` | WAL directory |
| `--no-wal` | `false` | Disable WAL (not recommended) |
| `--encrypt-wal` | `false` | Encrypt WAL with AES-256-GCM |

### Recovery Process

Recovery is fully automatic on startup:

1. Nexora-RS opens the RocksDB database
2. Reads all WAL segment files from `--wal-dir`
3. Replays each WAL record in order, reconstructing graph state
4. Logs the number of recovered records: `WAL replay: N records recovered`

```bash
# Example startup log with WAL recovery
🚀 Starting DeepStreaming...
   Mode:   single-durable (RocksDB + WAL)
   RocksDB: /var/lib/nexora/data
   WAL:     /var/lib/nexora/wal
   WAL replay: 1542 records recovered
   HTTP:   listening on 0.0.0.0:8080
```

### When WAL Recovery Triggers

- Normal restart (graceful shutdown followed by startup)
- Crash recovery (process killed, power failure, OOM kill)
- Pod restart in Kubernetes

### Limitations

- WAL only covers mutations since the last RocksDB flush/checkpoint
- WAL files are node-local — they do not replicate in single-node mode
- If WAL files are corrupted or deleted, recovery falls back to the last RocksDB checkpoint

---

## RocksDB Snapshot Backup

### Procedure

RocksDB snapshots provide a consistent point-in-time copy of the database. Use the RocksDB `ldb` tool or filesystem-level snapshot.

#### Method 1: Filesystem Snapshot (Recommended)

This method captures a consistent copy by leveraging filesystem snapshots or by stopping the node briefly.

**During planned maintenance (downtime):**

```bash
# 1. Stop the Nexora-RS node gracefully
kill -TERM $(pgrep nexora-app)
# Wait for "DeepStreaming shutdown complete" in logs

# 2. Copy the data directory
cp -a /var/lib/nexora/data /backup/nexora-$(date +%Y%m%d-%H%M%S)

# 3. Restart the node
nexora-app --rocksdb-path /var/lib/nexora/data --wal-dir /var/lib/nexora/wal
```

**Without downtime (filesystem snapshot):**

```bash
# If using LVM:
lvcreate --snapshot --size 10G --name nexora-snap /dev/vg0/nexora-data
mount /dev/vg0/nexora-snap /mnt/snap
cp -a /mnt/snap/nexora-data /backup/nexora-$(date +%Y%m%d-%H%M%S)
umount /mnt/snap
lvremove -f /dev/vg0/nexora-snap
```

#### Method 2: RocksDB Checkpoint

```bash
# Use ldb tool to create a checkpoint (does not block writes)
ldb --db=/var/lib/nexora/data checkpoint --checkpoint_dir=/backup/nexora-checkpoint-$(date +%Y%m%d)

# Compress the checkpoint
tar -czf /backup/nexora-$(date +%Y%m%d-%H%M%S).tar.gz -C /backup nexora-checkpoint-$(date +%Y%m%d)
```

#### Method 3: Kubernetes Volume Snapshot

```bash
# Create a VolumeSnapshot of the PVC
cat <<EOF | kubectl apply -f -
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: nexora-backup-$(date +%Y%m%d)
  namespace: nexora
spec:
  volumeSnapshotClassName: csi-snapclass
  source:
    persistentVolumeClaimName: data-nexora-0
EOF

# Verify snapshot
kubectl -n nexora get volumesnapshot
```

### Backup Verification

Always verify backups are restorable:

```bash
# Restore to a temporary directory and start a test instance
cp -a /backup/nexora-20260704 /tmp/nexora-test-data
nexora-app --rocksdb-path /tmp/nexora-test-data --port 9090 &

# Verify data
curl -X POST http://localhost:9090/api/v2/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) RETURN count(n) AS nodes"}'

# Clean up
kill %1
rm -rf /tmp/nexora-test-data
```

---

## Cluster Backup Strategy

### Raft Replication as Redundancy

In cluster mode with Raft consensus, data is replicated across all nodes. A 3-node cluster can tolerate 1 node failure without data loss.

```
Client → Leader → Replicate to Followers → Quorum Commit → Apply
```

**This is NOT a substitute for backups.** Raft protects against node failures but not against:
- Accidental data deletion (`DELETE` queries)
- Logical corruption (bad migration, application bug)
- Simultaneous multi-node failure
- Cluster-wide misconfiguration

### Cluster Backup Procedure

#### Option A: Backup from a Single Follower

Designate one node as the backup node and snapshot it during low-traffic periods:

```bash
# 1. Identify a follower node (not the leader)
kubectl -n nexora exec nexora-cluster-1 -- curl -s http://localhost:8080/api/v2/cluster/raft

# 2. Create a volume snapshot of that follower's PVC
cat <<EOF | kubectl apply -f -
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: nexora-cluster-backup-$(date +%Y%m%d)
  namespace: nexora
spec:
  volumeSnapshotClassName: csi-snapclass
  source:
    persistentVolumeClaimName: data-nexora-cluster-1
EOF
```

#### Option B: Coordinated Cluster Snapshot

For consistent cluster-wide backups, briefly pause writes and snapshot all nodes:

```bash
#!/bin/bash
# cluster-backup.sh — coordinated cluster backup
set -e

NAMESPACE="nexora"
DATE=$(date +%Y%m%d-%H%M%S)

echo "Creating coordinated cluster backup: $DATE"

for i in 0 1 2; do
  echo "Snapshotting nexora-cluster-$i..."
  cat <<EOF | kubectl apply -f -
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: nexora-cluster-backup-$DATE-node-$i
  namespace: $NAMESPACE
spec:
  volumeSnapshotClassName: csi-snapclass
  source:
    persistentVolumeClaimName: data-nexora-cluster-$i
EOF
done

echo "Waiting for snapshots to complete..."
kubectl -n $NAMESPACE wait volumesnapshot \
  nexora-cluster-backup-$DATE-node-0 \
  nexora-cluster-backup-$DATE-node-1 \
  nexora-cluster-backup-$DATE-node-2 \
  --for=jsonpath='{.status.readyToUse}=true' \
  --timeout=600s

echo "Cluster backup complete: $DATE"
```

#### Option C: Logical Export via API

Export graph data via the HTTP API for application-level backups:

```bash
# Export all nodes via Cypher
curl -X POST http://nexora:8080/api/v2/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) RETURN n"}' \
  | gzip > /backup/nodes-$(date +%Y%m%d).json.gz

# Export standing queries
curl http://nexora:8080/api/v2/standing-query \
  | gzip > /backup/standing-queries-$(date +%Y%m%d).json.gz

# Export materialized views
curl http://nexora:8080/api/v2/materialized-views \
  | gzip > /backup/materialized-views-$(date +%Y%m%d).json.gz
```

---

## Restore Procedures

### Single-Node Restore

```bash
# 1. Stop the running instance
kill -TERM $(pgrep nexora-app)

# 2. Move corrupted data aside (do not delete until restore is verified)
mv /var/lib/nexora/data /var/lib/nexora/data.corrupt-$(date +%Y%m%d)
mv /var/lib/nexora/wal /var/lib/nexora/wal.corrupt-$(date +%Y%m%d)

# 3. Restore from backup
tar -xzf /backup/nexora-20260704.tar.gz -C /var/lib/nexora/
# Or: cp -a /backup/nexora-20260704 /var/lib/nexora/data

# 4. Remove old WAL (the restored snapshot includes its own consistent state)
# The WAL directory will be recreated on startup
rm -rf /var/lib/nexora/wal

# 5. Start the node
nexora-app \
  --rocksdb-path /var/lib/nexora/data \
  --wal-dir /var/lib/nexora/wal

# 6. Verify
curl http://localhost:8080/api/v2/health
curl -X POST http://localhost:8080/api/v2/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n) RETURN count(n) AS nodes"}'
```

### Kubernetes PVC Restore from Snapshot

```bash
# 1. Scale down the StatefulSet
kubectl -n nexora scale statefulset nexora --replicas=0

# 2. Delete the existing PVC
kubectl -n nexora delete pvc data-nexora-0

# 3. Create a new PVC from the snapshot
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-nexora-0
  namespace: nexora
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 50Gi
  storageClassName: standard
  dataSource:
    name: nexora-backup-20260704
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
EOF

# 4. Scale the StatefulSet back up
kubectl -n nexora scale statefulset nexora --replicas=1

# 5. Verify
kubectl -n nexora logs -f nexora-0 | grep "WAL replay"
```

### Cluster Restore

#### Restoring a Single Node

In a Raft cluster, a single node can be restored from backup and will catch up to the leader:

```bash
# 1. Scale down the target pod
kubectl -n nexora scale statefulset nexora-cluster --replicas=2

# Wait for pod to terminate
kubectl -n nexora wait --for=delete pod/nexora-cluster-2 --timeout=120s

# 2. Delete and recreate PVC from snapshot (as above)
kubectl -n nexora delete pvc data-nexora-cluster-2
# Create PVC from snapshot...

# 3. Scale back up
kubectl -n nexora scale statefulset nexora-cluster --replicas=3

# The restored node will join the cluster and sync from the leader
kubectl -n nexora logs -f nexora-cluster-2 | grep "Cluster"
```

#### Restoring the Entire Cluster

Use this procedure for disaster recovery when the entire cluster is lost:

```bash
# 1. Delete the old cluster
kubectl -n nexora delete statefulset nexora-cluster

# 2. Delete all PVCs
kubectl -n nexora delete pvc -l app.kubernetes.io/name=nexora

# 3. Restore PVCs from snapshots for all 3 nodes
for i in 0 1 2; do
  cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-nexora-cluster-$i
  namespace: nexora
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
  storageClassName: standard
  dataSource:
    name: nexora-cluster-backup-20260704-node-$i
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
EOF
done

# 4. Re-deploy the cluster StatefulSet
kubectl apply -f deploy/k8s/cluster-statefulset.yaml

# 5. Verify cluster formation
kubectl -n nexora logs -f nexora-cluster-0 | grep -E "(Cluster|Raft)"
```

---

## Best Practices

### Backup Schedule

| Environment | Frequency | Retention | Method |
|-------------|-----------|-----------|--------|
| Development | On-demand | — | Filesystem copy |
| Staging | Daily | 7 days | Volume snapshot |
| Production | Every 6 hours | 30 days | Volume snapshot + logical export |
| Production (critical) | Every 1 hour | 7 days hot, 90 days cold | Volume snapshot + S3 replication |

### Backup Verification

- **Test restores monthly** — A backup you haven't restored is not a backup
- Verify node counts and key queries after restore
- Document the restore procedure and expected recovery time

### WAL Management

- Keep WAL on separate storage from RocksDB data for parallel I/O
- Monitor WAL directory size — unbounded growth may indicate RocksDB flush issues
- Enable WAL encryption (`--encrypt-wal`) for sensitive data
- **Never** delete WAL files manually while the process is running

### Encryption Key Management

- Store encryption keys separately from backups
- Use a KMS (Key Management Service) for production:
  - AWS KMS, Google Cloud KMS, Azure Key Vault
  - HashiCorp Vault
  - Kubernetes Secrets with sealed-secrets
- Document key rotation procedures
- **If the encryption key is lost, all encrypted WAL data is unrecoverable**

### Cluster-Specific Considerations

- Back up followers, not the leader, to minimize performance impact
- Ensure all nodes in a Raft cluster use the same backup point for consistency
- After a cluster restore, verify Raft consensus is healthy:
  ```bash
  curl http://nexora:8080/api/v2/cluster/raft
  ```
- Maintain at least `minAvailable: 2` in PodDisruptionBudget during backup operations

### Monitoring Backup Health

```bash
# Check backup freshness (should be < backup interval)
ls -lt /backup/nexora-* | head -5

# Monitor with cron + alerting
#!/bin/bash
# backup-check.sh
LATEST=$(find /backup -name "nexora-*.tar.gz" -mtime -1 | wc -l)
if [ "$LATEST" -eq 0 ]; then
  echo "ALERT: No backup in the last 24 hours" | mail -s "Nexora backup alert" ops@example.com
fi
```

### Disaster Recovery RTO/RPO Targets

| Scenario | RTO (Recovery Time) | RPO (Data Loss) |
|----------|---------------------|-----------------|
| Single-node crash | < 1 min (auto WAL replay) | 0 (WAL) |
| Single-node disk failure | < 15 min (PVC restore) | < 6 hours (last backup) |
| Cluster 1-node loss | 0 (no downtime) | 0 (Raft replication) |
| Cluster total failure | < 30 min (full restore) | < 6 hours (last backup) |

---

## Quick Reference

```bash
# --- Backup ---

# Filesystem backup (stop node first)
kill -TERM $(pgrep nexora-app)
cp -a /var/lib/nexora/data /backup/nexora-$(date +%Y%m%d)
nexora-app --rocksdb-path /var/lib/nexora/data --wal-dir /var/lib/nexora/wal

# K8s volume snapshot
kubectl -n nexora apply -f - <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: nexora-backup-$(date +%Y%m%d)
  namespace: nexora
spec:
  volumeSnapshotClassName: csi-snapclass
  source:
    persistentVolumeClaimName: data-nexora-0
EOF

# --- Restore ---

# Single-node restore
kill -TERM $(pgrep nexora-app)
mv /var/lib/nexora/data /var/lib/nexora/data.corrupt
cp -a /backup/nexora-20260704 /var/lib/nexora/data
rm -rf /var/lib/nexora/wal
nexora-app --rocksdb-path /var/lib/nexora/data --wal-dir /var/lib/nexora/wal

# K8s PVC restore
kubectl -n nexora scale statefulset nexora --replicas=0
kubectl -n nexora delete pvc data-nexora-0
# Create PVC from snapshot (see above)
kubectl -n nexora scale statefulset nexora --replicas=1
```
