#!/usr/bin/env bash
# Build Nexora release binaries

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Building Nexora Release...${NC}"

# Parse options
FEATURES=""
TARGET=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --features)
            FEATURES="$2"
            shift 2
            ;;
        --target)
            TARGET="$2"
            shift 2
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Build command
BUILD_CMD="cargo build --release"

if [[ -n "$FEATURES" ]]; then
    BUILD_CMD="$BUILD_CMD --features $FEATURES"
fi

if [[ -n "$TARGET" ]]; then
    BUILD_CMD="$BUILD_CMD --target $TARGET"
fi

echo -e "${YELLOW}Build command: $BUILD_CMD${NC}"

# Execute build
$BUILD_CMD

# Determine binary location
if [[ -n "$TARGET" ]]; then
    BIN_DIR="target/$TARGET/release"
else
    BIN_DIR="target/release"
fi

echo -e "${GREEN}Build completed!${NC}"
echo -e "Binaries:"
echo -e "  - ${GREEN}nexora${NC} (main server): $BIN_DIR/nexora"
echo -e "  - ${GREEN}nex${NC} (CLI client): $BIN_DIR/nex"
echo -e "  - ${GREEN}nexora-mcp${NC} (MCP server): $BIN_DIR/nexora-mcp"

# Check binary sizes
echo ""
echo "Binary sizes:"
ls -lh "$BIN_DIR"/nexora* "$BIN_DIR/nex" 2>/dev/null | awk '{print "  " $9 ": " $5}'

echo ""
echo -e "${GREEN}✓ Build successful${NC}"
