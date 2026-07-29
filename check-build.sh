#!/bin/bash
# Check if Nexora binary is ready and up-to-date

BINARY="./target/release/nexora"

if [ ! -f "$BINARY" ]; then
    echo "❌ Binary not found: $BINARY"
    echo "Run: cargo build --release --features event-streaming,library"
    exit 1
fi

echo "✅ Binary exists: $BINARY"
echo "   Size: $(ls -lh $BINARY | awk '{print $5}')"
echo "   Modified: $(stat -f "%Sm" -t "%Y-%m-%d %H:%M:%S" $BINARY)"
echo ""

# Check if build is still running
if pgrep -f "cargo build.*event-streaming" > /dev/null; then
    echo "⏳ Build is still running..."
    echo ""
    echo "To check progress, run:"
    echo "   ps aux | grep 'cargo build'"
    exit 0
fi

echo "✅ No active build process"
echo ""
echo "Test the binary:"
echo "   $BINARY --version"
echo ""
echo "Start Nexora:"
echo "   ./start-nexora-library.sh"
