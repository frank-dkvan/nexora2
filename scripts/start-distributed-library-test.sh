#!/bin/bash
# RisingWave Distributed Library Mode - Quick Start Test
# 启动单节点分布式库模式用于测试

set -e

echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║     RisingWave Distributed Library Mode - Test Startup               ║"
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo ""

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 检查是否已编译
if [ ! -f "target/debug/nexora" ]; then
    echo -e "${YELLOW}⚠️  nexora binary not found, compiling...${NC}"
    cargo build --features event-first,event-streaming,library
fi

# 清理旧数据（可选）
echo -e "${GREEN}🧹 Cleaning old data...${NC}"
rm -rf ./nexora-data/event-streaming-library 2>/dev/null || true
rm -rf ./nexora-data/meta-*.db 2>/dev/null || true

# 创建临时配置文件
echo -e "${GREEN}📝 Creating configuration...${NC}"
cat > /tmp/nexora-distributed-test.toml << 'EOF'
# RisingWave Distributed Library Mode Test Configuration

[event_streaming]
enabled = true
library = true

# 分布式库模式配置
[event_streaming.distributed]
enabled = true
node_id = "meta-1"
data_dir = "./nexora-data/event-streaming-library"

# Meta 节点配置
[event_streaming.distributed.meta]
listen_addr = "0.0.0.0:5690"
advertise_addr = "127.0.0.1:5690"
backend = "sqlite"
sqlite_path = "./nexora-data/meta-1.db"
# 单节点测试：自己是 Raft leader，不需要 peers
raft_peers = []
election_timeout_ms = 1000
heartbeat_interval_ms = 300

# Frontend 节点配置
[event_streaming.distributed.frontend]
listen_addr = "0.0.0.0:4566"

# Compute 节点配置
[event_streaming.distributed.compute]
listen_addr = "0.0.0.0:5688"
parallelism = 4  # 使用 4 个并行度
EOF

echo -e "${GREEN}✅ Configuration created at /tmp/nexora-distributed-test.toml${NC}"
echo ""

# 显示配置
echo -e "${YELLOW}📋 Configuration:${NC}"
echo "  Node ID: meta-1"
echo "  Meta:     127.0.0.1:5690"
echo "  Frontend: 127.0.0.1:4566 (pgwire)"
echo "  Compute:  127.0.0.1:5688"
echo "  Backend:  SQLite (./nexora-data/meta-1.db)"
echo ""

# 启动命令
echo -e "${GREEN}🚀 Starting nexora with distributed library mode...${NC}"
echo ""

RUST_LOG=info,nexora_risingwave=debug \
./target/debug/nexora \
  --config /tmp/nexora-distributed-test.toml \
  --distributed-library-event-streaming \
  --library-node-id meta-1 \
  --library-meta-addr "0.0.0.0:5690" \
  --library-meta-advertise "127.0.0.1:5690" \
  --event-streaming-frontend-addr "127.0.0.1:4566" \
  --allow-unauthenticated \
  --host 0.0.0.0 \
  --port 8080

# 如果 Ctrl+C 退出，清理提示
echo ""
echo -e "${YELLOW}⚠️  Server stopped${NC}"
echo ""
echo "数据文件位置:"
echo "  - Meta DB: ./nexora-data/meta-1.db"
echo "  - Data dir: ./nexora-data/event-streaming-library"
echo ""
echo "如需清理: rm -rf ./nexora-data/event-streaming-library ./nexora-data/meta-*.db"
