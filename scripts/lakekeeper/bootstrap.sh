#!/usr/bin/env bash
# Bootstrap Lakekeeper for local Nexora dev:
#   1. bootstrap the server (first-time init)
#   2. create a warehouse named "nexora" backed by the host MinIO
#
# Prereqs:
#   - MinIO running on host at localhost:9000 (minioadmin/minioadmin)
#   - Lakekeeper running via scripts/lakekeeper/docker-compose.yml (port 8181)
#
# The Iceberg REST endpoint clients use afterwards:
#   uri=http://localhost:8181/catalog  warehouse=nexora
set -euo pipefail

LK=${LK:-http://localhost:8181}
WAREHOUSE=${WAREHOUSE:-nexora}
BUCKET=${BUCKET:-nexora-events}
# From inside the Lakekeeper container the host MinIO is reachable here:
MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://host.docker.internal:9000}
MINIO_KEY=${MINIO_KEY:-minioadmin}
MINIO_SECRET=${MINIO_SECRET:-minioadmin}

echo "==> Waiting for Lakekeeper health..."
for i in $(seq 1 30); do
  if curl -sf "$LK/health" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -sf "$LK/health" >/dev/null || { echo "Lakekeeper not healthy"; exit 1; }

echo "==> Bootstrapping server (idempotent)..."
curl -s -X POST "$LK/management/v1/bootstrap" \
  -H 'Content-Type: application/json' \
  -d '{"accept-terms-of-use": true}' -o /dev/null -w "bootstrap: HTTP %{http_code}\n" || true

echo "==> Ensuring warehouse '$WAREHOUSE' exists..."
existing=$(curl -s "$LK/management/v1/warehouse" | grep -o "\"name\":\"$WAREHOUSE\"" || true)
if [ -n "$existing" ]; then
  echo "warehouse '$WAREHOUSE' already exists — skipping"
else
  curl -s -X POST "$LK/management/v1/warehouse" \
    -H 'Content-Type: application/json' \
    -d "{
      \"warehouse-name\": \"$WAREHOUSE\",
      \"storage-profile\": {
        \"type\": \"s3\",
        \"bucket\": \"$BUCKET\",
        \"region\": \"us-east-1\",
        \"endpoint\": \"$MINIO_ENDPOINT\",
        \"path-style-access\": true,
        \"flavor\": \"s3-compat\",
        \"sts-enabled\": false
      },
      \"storage-credential\": {
        \"type\": \"s3\",
        \"credential-type\": \"access-key\",
        \"aws-access-key-id\": \"$MINIO_KEY\",
        \"aws-secret-access-key\": \"$MINIO_SECRET\"
      }
    }" -w "\ncreate-warehouse: HTTP %{http_code}\n"
fi

echo "==> Verifying Iceberg REST config endpoint..."
curl -s "$LK/catalog/v1/config?warehouse=$WAREHOUSE" -o /dev/null -w "config: HTTP %{http_code}\n"

echo "==> Done. Clients: uri=$LK/catalog  warehouse=$WAREHOUSE"
