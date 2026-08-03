#!/bin/bash
# Validate Iceberg event log integrity
set -euo pipefail

CATALOG_URI="${CATALOG_URI:-http://localhost:8181}"
NAMESPACE="${NAMESPACE:-nexora}"

echo "=== Checking Iceberg Catalog Connectivity ==="

if ! curl -sf "${CATALOG_URI}/v1/config" > /dev/null; then
    echo "ERROR: Cannot reach Iceberg catalog at ${CATALOG_URI}"
    exit 1
fi
echo "Catalog reachable: ${CATALOG_URI}"

echo ""
echo "=== Listing Tables in Namespace ==="

tables=$(curl -sf "${CATALOG_URI}/v1/namespaces/${NAMESPACE}/tables" | jq -r '.tables[]?.name' 2>/dev/null || echo "")

if [[ -z "$tables" ]]; then
    echo "WARNING: No tables found in namespace ${NAMESPACE}"
    echo "This is expected for a fresh deployment"
    exit 0
fi

table_count=$(echo "$tables" | wc -l)
echo "Found $table_count table(s)"

echo ""
echo "=== Checking Table Snapshots ==="

for table in $tables; do
    echo "Checking table: $table"

    snapshots=$(curl -sf "${CATALOG_URI}/v1/namespaces/${NAMESPACE}/tables/${table}/snapshots" | \
        jq '.snapshots | length' 2>/dev/null || echo "0")

    if [[ "$snapshots" -eq 0 ]]; then
        echo "  WARNING: Table $table has no snapshots"
    else
        echo "  Snapshots: $snapshots"
    fi

    # Check metadata
    metadata=$(curl -sf "${CATALOG_URI}/v1/namespaces/${NAMESPACE}/tables/${table}" 2>/dev/null || echo "")
    if [[ -z "$metadata" ]]; then
        echo "  ERROR: Cannot read metadata for table $table"
        exit 1
    fi

    schema_id=$(echo "$metadata" | jq -r '.metadata."current-schema-id"' 2>/dev/null || echo "")
    if [[ -z "$schema_id" || "$schema_id" == "null" ]]; then
        echo "  ERROR: Invalid schema for table $table"
        exit 1
    fi
    echo "  Schema ID: $schema_id"
done

echo ""
echo "✅ Event log integrity validation passed"
