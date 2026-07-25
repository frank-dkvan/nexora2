#!/usr/bin/env bash
# Build Nexora Docker image

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

VERSION=${1:-"latest"}
FEATURES=${FEATURES:-""}

echo -e "${GREEN}Building Nexora Docker image...${NC}"
echo -e "Version: ${YELLOW}$VERSION${NC}"

# Build image
docker build \
    --build-arg FEATURES="$FEATURES" \
    --tag nexora:$VERSION \
    --tag nexora:latest \
    -f Dockerfile \
    .

echo -e "${GREEN}✓ Docker image built successfully${NC}"
echo -e "  nexora:$VERSION"
echo -e "  nexora:latest"
