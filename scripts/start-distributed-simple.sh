#!/bin/bash
# 简化的分布式库模式启动脚本
# 直接使用命令行参数，不需要配置文件

set -e

echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║     RisingWave Distributed Library Mode - Simple Test                ║"
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo ""

# 清理旧数据
echo "🧹 Cleaning old data..."
rm -rf ./nexora-data/event-streaming-library 2>/dev/null || true
rm -rf ./nexora-data/meta-*.db 2>/dev/null || true

echo ""
echo "📋 Configuration:"
echo "  Mode: Distributed Library (single-node test)"
echo "  Node ID: meta-1"
echo "  Meta:     127.0.0.1:5690"
echo "  Frontend: 127.0.0.1:4566 (PostgreSQL wire)"
echo "  Compute:  127.0.0.1:5688"
echo "  HTTP API: 127.0.0.1:8080"
echo ""
echo "🚀 Starting..."
echo ""

# 启动参数说明：
# --distributed-library-event-streaming: 启用分布式库模式
# --library-node-id meta-1: 节点ID（Raft成员ID）
# --library-meta-addr: Meta监听地址
# --library-meta-advertise: Meta广播地址（其他节点用来连接的地址）
# --event-streaming-frontend-addr: Frontend地址（PostgreSQL协议）
# 单节点测试不需要 --library-meta-peer

# --- Binary preparation --------------------------------------------------
# The full-feature debug binary (event-streaming,library, ~2.2GB) has debug
# segments so large that macOS dyld refuses to map it ("dyld cache '(null)'
# not loaded: syscall to map cache into shared region failed"). Stripping the
# debug symbols drops it to ~1GB, which dyld loads fine. We strip a COPY so the
# original (with symbols, for debugging under lldb) is left intact.
BIN_SRC="./target/debug/nexora"
BIN_RUN="./target/debug/nexora-run"

if [ ! -x "$BIN_SRC" ]; then
    echo "❌ $BIN_SRC not found. Build first:"
    echo "   cargo build --features event-first,event-streaming,library"
    exit 1
fi

# Re-strip only when the source binary is newer than the stripped copy.
if [ ! -x "$BIN_RUN" ] || [ "$BIN_SRC" -nt "$BIN_RUN" ]; then
    echo "🔧 Stripping debug symbols (works around macOS dyld size limit)..."
    cp "$BIN_SRC" "$BIN_RUN"
    strip "$BIN_RUN"
    echo "   $(ls -la "$BIN_RUN" | awk '{print $5}') bytes after strip"
fi

RUST_LOG=info,nexora_risingwave=debug \
"$BIN_RUN" \
  --enable-event-streaming \
  --distributed-library-event-streaming \
  --library-node-id meta-1 \
  --library-meta-addr "0.0.0.0:5690" \
  --library-meta-advertise "127.0.0.1:5690" \
  --event-streaming-frontend-addr "127.0.0.1:4566" \
  --allow-unauthenticated \
  --host 0.0.0.0 \
  --port 8080 \
  --no-rocksdb \
  --no-wal

echo ""
echo "Server stopped."
