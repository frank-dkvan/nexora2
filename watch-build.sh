#!/bin/bash
# Monitor cargo build progress

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Nexora Build Monitor ===${NC}\n"

# Check if cargo is running
CARGO_PIDS=$(pgrep -f "cargo build.*event-streaming" || true)

if [ -z "$CARGO_PIDS" ]; then
    echo -e "${RED}✗ No cargo build process found${NC}"
    echo ""
    echo "To start compilation:"
    echo "  export PATH=\"\$HOME/.cargo/bin:\$PATH\""
    echo "  cargo build --release --features event-streaming,library"
    exit 1
fi

echo -e "${GREEN}✓ Cargo build is running (PIDs: $CARGO_PIDS)${NC}"
echo ""

# Show running time
for PID in $CARGO_PIDS; do
    if ps -p "$PID" > /dev/null 2>&1; then
        START_TIME=$(ps -o etime= -p "$PID" | tr -d ' ')
        echo -e "${YELLOW}Running for: ${START_TIME}${NC}"
    fi
done
echo ""

# Check binary
if [ -f target/release/nexora ]; then
    BINARY_SIZE=$(ls -lh target/release/nexora | awk '{print $5}')
    BINARY_TIME=$(stat -f "%Sm" target/release/nexora)
    echo -e "Binary: ${BINARY_SIZE} (last built: ${BINARY_TIME})"
else
    echo -e "Binary: ${RED}Not yet built${NC}"
fi
echo ""

# Show recent compilation activity
echo -e "${BLUE}Recent crates being compiled:${NC}"
# Get cargo metadata to show what's compiling
if command -v lsof >/dev/null 2>&1; then
    # Show files being written by cargo
    for PID in $CARGO_PIDS; do
        lsof -p "$PID" 2>/dev/null | grep -E "target/release.*\.rlib|target/release.*build" | tail -5 | awk '{print $9}' | xargs -I {} basename {} | sed 's/^/  - /'
    done | head -10
else
    echo "  (install lsof to see detailed progress)"
fi
echo ""

# Estimate progress based on dependency count
TOTAL_DEPS=$(cargo tree --features event-streaming,library --depth 0 2>/dev/null | wc -l || echo "unknown")
echo "Total dependencies: ${TOTAL_DEPS}"
echo ""

# Check if any errors in recent output
if ps aux | grep -q "cargo.*event-streaming.*library"; then
    echo -e "${GREEN}Status: Compiling...${NC}"
    echo ""
    echo "This is a large build (library mode includes entire RisingWave)."
    echo "Expected time: 10-20 minutes on first build."
    echo ""
    echo "To watch live progress:"
    echo "  tail -f <(cargo build --release --features event-streaming,library 2>&1)"
else
    echo -e "${YELLOW}Status: Unknown${NC}"
fi

echo ""
echo "Press Ctrl+C to exit (build will continue in background)"
