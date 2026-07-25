#!/usr/bin/env bash
# Run all Nexora tests

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}Running Nexora Test Suite...${NC}"

# Parse options
FILTER=""
VERBOSE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --filter)
            FILTER="$2"
            shift 2
            ;;
        --verbose|-v)
            VERBOSE="--verbose"
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Run unit tests
echo -e "${YELLOW}Running unit tests...${NC}"
if [[ -n "$FILTER" ]]; then
    cargo test $VERBOSE --lib $FILTER
else
    cargo test $VERBOSE --lib
fi

# Run integration tests
echo -e "${YELLOW}Running integration tests...${NC}"
if [[ -n "$FILTER" ]]; then
    cargo test $VERBOSE --test '*' $FILTER
else
    cargo test $VERBOSE --test '*'
fi

# Run doc tests
echo -e "${YELLOW}Running doc tests...${NC}"
cargo test $VERBOSE --doc

echo -e "${GREEN}✓ All tests passed${NC}"
