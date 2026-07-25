# Kubernetes Deployment Guide

This directory contains Kubernetes manifests for deploying Nexora-RS in both single-node and clustered modes.

## Prerequisites

- Kubernetes 1.24+
- `kubectl` configured with cluster access
- A container image of `nexora` available in your registry
- (Optional) cert-manager for automatic TLS certificate management
- (Optional) Prometheus Operator for metrics scraping via ServiceMonitor
- (Optional) ingress-nginx controller for Ingress resources

## Quick Start

### Single-Node Durable Mode

```bash
# Apply all manifests in order
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f statefulset.yaml
kubectl apply -f service.yaml

# Verify deployment
kubectl -n nexora get pods
kubectl -n nexora port-forward svc/nexora 8080:8080

# Access the API
curl http://localhost:8080/api/v2/health
```

### 3-Node Cluster Mode

```bash
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f cluster-service.yaml
kubectl apply -f cluster-statefulset.yaml
kubectl apply -f pdb.yaml

# Watch cluster formation
kubectl -n nexora logs -f nexora-cluster-0 | grep Cluster
```

## Manifest Reference

### namespace.yaml

Creates the `nexora` namespace with standard Kubernetes labels. All resources are scoped to this namespace.

### configmap.yaml

Contains non-sensitive configuration as a ConfigMap and sensitive values as a Secret.

| Key | Default | Description |
|-----|---------|-------------|
| `NEXORA_PROFILE` | `single-durable` | Run profile: `lite-ephemeral`, `single-durable`, or `clustered` |
| `NEXORA_HOST` | `0.0.0.0` | HTTP listen address |
| `NEXORA_PORT` | `8080` | HTTP listen port |
| `NEXORA_ROCKSDB_PATH` | `/data/nexora-data` | RocksDB data directory |
| `NEXORA_WAL_DIR` | `/data/nexora-data/wal` | WAL directory |
| `NEXORA_NUM_SHARDS` | `256` | Number of graph shards |
| `NEXORA_MAX_NODES_PER_SHARD` | `10000` | Max nodes per shard before LRU eviction |
| `NEXORA_RATE_LIMIT` | `true` | Enable rate limiting |
| `NEXORA_RATE_LIMIT_RATE` | `100` | Requests per second per client IP |
| `NEXORA_RATE_LIMIT_BURST` | `200` | Burst capacity per client IP |
| `NEXORA_CORS_ORIGIN` | `*` | CORS allowed origin |
| `NEXORA_UDF_DIR` | `/data/udf` | UDF scripts directory |
| `RUST_LOG` | `info` | Log level |
| `NEXORA_REQUIRE_AUTH` | `false` | Enable JWT authentication |

**Secret keys** (override in production):

| Key | Description |
|-----|-------------|
| `NEXORA_AUTH_SECRET` | HMAC-SHA256 signing key for JWT tokens |
| `NEXORA_ENCRYPTION_KEY` | Hex-encoded 32-byte AES-256 key for WAL encryption |

> **Important:** Change `NEXORA_AUTH_SECRET` before production deployment. Generate a strong encryption key with `openssl rand -hex 32`.

### statefulset.yaml (Single-Node)

Deploys a single Nexora-RS pod with persistent storage.

- **Storage:** 50Gi PVC (`standard` storage class) mounted at `/data`
- **Probes:** startup, readiness, and liveness probes against `/api/v2/health*`
- **Graceful shutdown:** 60-second termination grace period for WAL flush
- **Resources:** 500m–2000m CPU, 1Gi–4Gi memory
- **Security:** Runs as non-root user (UID 1000)

**PVC sizing:** Adjust `storage: 50Gi` based on your graph size. RocksDB compression typically achieves 3-5x reduction.

### service.yaml

Two services:

1. **`nexora`** (ClusterIP) — Internal cluster access on port 8080
2. **`nexora-nodeport`** (NodePort 30080) — For testing and local access

> Remove the NodePort service in production; use Ingress instead.

### ingress.yaml

Configures nginx Ingress with TLS termination via cert-manager.

- **Host:** `nexora.example.com` (change to your domain)
- **TLS:** Let's Encrypt certificate via `letsencrypt-prod` cluster issuer
- **WebSocket:** Enabled for `/api/v2/ws/*` endpoints
- **Body size:** 16m (matches API body limit)
- **Timeouts:** 3600s for long-running queries

**Setup:**
```bash
# Install cert-manager (if not already installed)
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.0/cert-manager.yaml

# Create ClusterIssuer for Let's Encrypt
cat <<EOF | kubectl apply -f -
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: letsencrypt-prod
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    email: your-email@example.com
    privateKeySecretRef:
      name: letsencrypt-prod
    solvers:
      - http01:
          ingress:
            class: nginx
EOF

# Update ingress host and apply
# Edit ingress.yaml: replace nexora.example.com with your domain
kubectl apply -f ingress.yaml
```

### cluster-statefulset.yaml (3-Node Cluster)

Deploys a 3-node Nexora-RS cluster with Raft consensus.

- **Replicas:** 3 (minimum for Raft quorum)
- **Pod management:** `Parallel` (all pods start simultaneously for faster cluster formation)
- **Peer discovery:** DNS-based via headless service; each pod resolves peers at startup
- **Ports:** HTTP (8080), cluster graph (9000), heartbeat (9001), Raft (9010)
- **Storage:** 100Gi PVC per pod
- **Graceful shutdown:** 120-second termination grace period for cluster rebalancing

**How peer discovery works:**

Each pod derives its identity from its StatefulSet ordinal:
- Pod `nexora-cluster-0` → node-id `node-0`
- Pod `nexora-cluster-1` → node-id `node-1`
- Pod `nexora-cluster-2` → node-id `node-2`

The startup script constructs the peer list from the headless service DNS names:
```
nexora-cluster-{0,1,2}.nexora-cluster.nexora.svc.cluster.local
```

**Scaling:** To scale beyond 3 nodes, update `replicas` in the StatefulSet and adjust the peer loop in the startup script (currently hardcoded for 3 nodes).

### cluster-service.yaml

Headless service (`clusterIP: None`) required for StatefulSet DNS-based peer discovery.

- `publishNotReadyAddresses: true` — Peers must discover each other before becoming ready
- Exposes all four ports: HTTP, cluster graph, heartbeat, Raft

### pdb.yaml

PodDisruptionBudget ensuring at least 2 of 3 cluster nodes remain available during voluntary disruptions (node drains, upgrades).

- `minAvailable: 2` — Maintains Raft quorum (majority of 3)
- Protects against simultaneous evictions during cluster maintenance

### servicemonitor.yaml

Prometheus Operator ServiceMonitor for scraping the `/metrics` endpoint.

- **Scrape interval:** 15s
- **Labels:** Adds `pod`, `node`, and `nexora_cluster` labels to metrics
- **Selector:** Matches all services with `app.kubernetes.io/name: nexora`

**Prerequisite:** Requires Prometheus Operator installed in the cluster. The `release: prometheus` label must match your Prometheus release name.

## Production Checklist

- [ ] Change `NEXORA_AUTH_SECRET` in configmap.yaml
- [ ] Set `NEXORA_REQUIRE_AUTH` to `true`
- [ ] Generate and set `NEXORA_ENCRYPTION_KEY` for WAL encryption
- [ ] Update Ingress host to your domain
- [ ] Configure cert-manager ClusterIssuer
- [ ] Remove NodePort service (use Ingress only)
- [ ] Adjust PVC storage size for your workload
- [ ] Set appropriate resource requests/limits
- [ ] Configure `RUST_LOG` to `warn` for production (reduce log volume)
- [ ] Set up Grafana dashboards (see `/grafana` directory)
- [ ] Configure backup strategy (see `docs/backup-restore.md`)
- [ ] Review PodDisruptionBudget for your replica count

## Common Operations

### View logs
```bash
kubectl -n nexora logs -f nexora-0
kubectl -n nexora logs -f nexora-cluster-0
```

### Exec into pod
```bash
kubectl -n nexora exec -it nexora-0 -- /bin/sh
```

### Scale cluster
```bash
# Note: also update the peer loop in cluster-statefulset.yaml
kubectl -n nexora scale statefulset nexora-cluster --replicas=5
```

### Rolling restart
```bash
kubectl -n nexora rollout restart statefulset nexora
kubectl -n nexora rollout status statefulset nexora
```

### Check PVC status
```bash
kubectl -n nexora get pvc
```

### Access metrics
```bash
kubectl -n nexora port-forward svc/nexora 8080:8080
curl http://localhost:8080/metrics
```
