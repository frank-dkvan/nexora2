#!/bin/bash
# Start Nexora with library-mode event streaming engine
# Usage: ./start-nexora-library.sh

set -e

export PATH="$HOME/.cargo/bin:$PATH"

echo "=== Starting Nexora with library-mode Event Streaming ==="
echo ""
echo "Features enabled:"
echo "  - event-streaming: SQL stream processing"
echo "  - library: RisingWave compiled in-process"
echo ""
echo "Endpoints available:"
echo "  - HTTP API: http://127.0.0.1:8080"
echo "  - Health: http://127.0.0.1:8080/api/health"
echo "  - Event Streaming status: http://127.0.0.1:8080/api/event-streaming/status"
echo ""
echo "Config: nexora.toml"
echo "Data directory: ./nexora-data/"
echo ""

# Create data directory if needed
mkdir -p ./nexora-data/event-streaming

# Run with library mode (disable auth for testing)
./target/release/nexora --config nexora.toml --enable-event-streaming --library-event-streaming --allow-unauthenticated
