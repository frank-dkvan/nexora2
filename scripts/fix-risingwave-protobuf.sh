#!/bin/bash
# Fix RisingWave protobuf generation issues
# This script ensures prost dependencies are correct and regenerates all protobuf files

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NEXORA_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RW_DIR="$NEXORA_ROOT/vendor/risingwave"

echo "=== RisingWave Protobuf Fix Script ==="
echo "Nexora root: $NEXORA_ROOT"
echo "RisingWave dir: $RW_DIR"
echo ""

# Step 1: Verify we're using nightly toolchain
echo "[1/5] Checking Rust toolchain..."
export PATH="$HOME/.cargo/bin:$HOME/.rustup/toolchains/nightly-2025-10-10-aarch64-apple-darwin/bin:$PATH"
rustc --version | grep "nightly-2025-10-10" || {
    echo "ERROR: nightly-2025-10-10 toolchain not active"
    exit 1
}
echo "✓ Nightly toolchain active"
echo ""

# Step 2: Verify Cargo.toml uses correct prost version
echo "[2/5] Checking prost dependencies..."
cd "$RW_DIR"
if grep -q 'prost.*git.*risingwavelabs' Cargo.toml; then
    echo "ERROR: Cargo.toml uses prost fork (should use crates.io version)"
    echo "Please restore official Cargo.toml from RisingWave v3.0.2"
    exit 1
fi
if ! grep -q 'prost.*=.*"=0.14.3"' Cargo.toml; then
    echo "ERROR: prost version is not =0.14.3"
    exit 1
fi
echo "✓ prost dependencies correct (crates.io 0.14.3)"
echo ""

# Step 3: Clean and regenerate protobuf files
echo "[3/5] Cleaning generated protobuf files..."
cd "$RW_DIR/src/prost/src"
# Keep only lib.rs and id.rs (source files)
ls *.rs | grep -v "^lib.rs$" | grep -v "^id.rs$" | xargs rm -f
echo "✓ Removed $(ls *.rs 2>/dev/null | wc -l | tr -d ' ') files (should be 2)"
echo ""

# Step 4: Rebuild risingwave_pb
echo "[4/5] Rebuilding risingwave_pb..."
cd "$RW_DIR"
cargo clean -p risingwave_pb
cargo build -p risingwave_pb
echo "✓ risingwave_pb rebuilt"
echo ""

# Step 5: Verify generated files
echo "[5/5] Verifying generated files..."
cd "$RW_DIR/src/prost/src"
FILE_COUNT=$(ls *.rs | wc -l | tr -d ' ')
if [ "$FILE_COUNT" -lt 50 ]; then
    echo "ERROR: Only $FILE_COUNT .rs files generated (expected 58+)"
    exit 1
fi

if ! grep -q "derive(prost_helpers::AnyPB)" data.rs; then
    echo "ERROR: data.rs missing AnyPB derive"
    exit 1
fi

echo "✓ Generated $FILE_COUNT protobuf files with AnyPB derives"
echo ""
echo "=== Success! ==="
echo "RisingWave protobuf files are correctly generated."
echo "Now you can build the full RisingWave binary:"
echo "  cd $RW_DIR"
echo "  cargo build -p risingwave_cmd_all --bin risingwave"
