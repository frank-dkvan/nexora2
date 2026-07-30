#!/usr/bin/env bash
set -euo pipefail

# Fix all Pb-prefixed protobuf types in RisingWave
# These types were renamed by the prost protobuf generator

cd "$(dirname "$0")/../vendor/risingwave"

echo "Fixing Pb-prefixed protobuf types..."
echo ""

# List of all Pb* types to fix
# Format: "PbTypeName TypeName"
TYPES=(
    "PbAction Action"
    "PbActorCountPerParallelism ActorCountPerParallelism"
    "PbArray Array"
    "PbArrayType ArrayType"
    "PbBuffer Buffer"
    "PbClusterLimit ClusterLimit"
    "PbColIndexMapping ColIndexMapping"
    "PbColumnCatalog ColumnCatalog"
    "PbColumnDesc ColumnDesc"
    "PbColumnOrder ColumnOrder"
    "PbCompressionType CompressionType"
    "PbDataChunk DataChunk"
    "PbDataType DataType"
    "PbDatum Datum"
    "PbDirection Direction"
    "PbDistanceType DistanceType"
    "PbEngine Engine"
    "PbEventMessage EventMessage"
    "PbField Field"
    "PbHostAddress HostAddress"
    "PbInterval Interval"
    "PbJoinEncodingType JoinEncodingType"
    "PbLimit Limit"
    "PbListArrayData ListArrayData"
    "PbNullsAre NullsAre"
    "PbOp Op"
    "PbOrderType OrderType"
    "PbOverWindowCachePolicy OverWindowCachePolicy"
    "PbStreamChunk StreamChunk"
    "PbSystemParams SystemParams"
    "PbTelemetryClusterType TelemetryClusterType"
    "PbTypeName TypeName"
    "PbWorkerActorCount WorkerActorCount"
    "PbWorkerSlotMapping WorkerSlotMapping"
)

# Build perl regex for one-pass replacement
PERL_REGEX=""
for type_pair in "${TYPES[@]}"; do
    old_name=$(echo "$type_pair" | awk '{print $1}')
    new_name=$(echo "$type_pair" | awk '{print $2}')
    if [ -n "$PERL_REGEX" ]; then
        PERL_REGEX="$PERL_REGEX; "
    fi
    PERL_REGEX="${PERL_REGEX}s/\\b${old_name}\\b/${new_name}/g"
done

echo "Replacing in all Rust source files..."
find src/ -type f -name "*.rs" -print0 | xargs -0 perl -pi -e "$PERL_REGEX"

echo ""
echo "✅ Done! Fixed ${#TYPES[@]} type names."
echo ""
echo "Verification:"
grep -r "\\bPb[A-Z][a-zA-Z]*\\b" src/ --include="*.rs" | \
    grep -v "PbSecretRefMap" | \
    grep -v "// Pb" | \
    grep -v "//" | \
    wc -l | \
    awk '{if ($1 == 0) print "✅ No Pb-prefixed types remain"; else print "⚠️  " $1 " Pb-prefixed references still exist"}'
