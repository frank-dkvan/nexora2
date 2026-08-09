#!/usr/bin/env bash
set -euo pipefail

# RisingWave Build Script - Ensures correct nightly toolchain is used
# Fixes PATH issue where Homebrew Rust overrides rustup toolchains

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RISINGWAVE_DIR="$PROJECT_ROOT/vendor/risingwave"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}RisingWave Build Script${NC}"
echo "================================"

# Check if RisingWave directory exists
if [ ! -d "$RISINGWAVE_DIR" ]; then
    echo -e "${RED}Error: RisingWave directory not found at $RISINGWAVE_DIR${NC}"
    exit 1
fi

# Put the rustup proxy AHEAD of Homebrew on PATH. Homebrew ships a standalone
# stable cargo/rustc that ignores rust-toolchain.toml; if it wins on PATH the
# build fails with "profile-rustflags requires nightly" / "-Z is only accepted
# on nightly". The proxy at ~/.cargo/bin auto-reads the pinned channel from
# rust-toolchain.toml (nightly-2026-06-11), so we never hardcode the date here.
export PATH="$HOME/.cargo/bin:$PATH"

echo -e "${YELLOW}Verifying toolchain...${NC}"
echo "rustc: $(rustc --version)"
echo "cargo: $(cargo --version)"

# Verify the resolved toolchain is nightly (i.e. the proxy read the pin and
# Homebrew's stable is not shadowing it).
if ! cargo --version | grep -q "nightly"; then
    echo -e "${RED}Error: cargo did not resolve to nightly!${NC}"
    echo "Resolved cargo: $(command -v cargo) → $(cargo --version)"
    echo "Homebrew's stable cargo is likely shadowing the rustup proxy on PATH."
    exit 1
fi

echo -e "${GREEN}✓ Using pinned nightly toolchain (via rustup proxy)${NC}"
echo ""

cd "$RISINGWAVE_DIR"

# Parse command line arguments
BUILD_TYPE="${1:-release}"  # default to release
PACKAGE="${2:-risingwave_cmd_all}"
BINARY="${3:-risingwave}"

case "$BUILD_TYPE" in
    dev|debug)
        echo -e "${YELLOW}Building $PACKAGE (debug mode)...${NC}"
        cargo build -p "$PACKAGE" --bin "$BINARY"
        ;;
    release)
        echo -e "${YELLOW}Building $PACKAGE (release mode)...${NC}"
        cargo build -p "$PACKAGE" --bin "$BINARY" --release
        ;;
    *)
        echo -e "${RED}Error: Unknown build type '$BUILD_TYPE'${NC}"
        echo "Usage: $0 [dev|release] [package] [binary]"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}✓ Build completed successfully!${NC}"

# Show binary location
if [ "$BUILD_TYPE" = "release" ]; then
    BINARY_PATH="$RISINGWAVE_DIR/target/release/$BINARY"
else
    BINARY_PATH="$RISINGWAVE_DIR/target/debug/$BINARY"
fi

if [ -f "$BINARY_PATH" ]; then
    echo "Binary location: $BINARY_PATH"
    echo "Binary size: $(du -h "$BINARY_PATH" | cut -f1)"
else
    echo -e "${YELLOW}Warning: Binary not found at expected location${NC}"
fi
