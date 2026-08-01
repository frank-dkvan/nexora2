#!/bin/bash
# 启动 3 节点 Nexora 分布式集群（包含嵌入式 RisingWave）
# 每个节点运行独立的 RisingWave Meta + Frontend + Compute

set -e

echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║   Nexora 3-Node Cluster with Embedded RisingWave (Library Mode)      ║"
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo ""

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# 清理旧数据（可选，通过参数控制）
if [ "$1" == "--clean" ]; then
    echo -e "${YELLOW}🧹 Cleaning old cluster data...${NC}"
    rm -rf ./nexora-data-node-a ./nexora-data-node-b ./nexora-data-node-c
    rm -rf ./nexora-data/meta-*.db ./nexora-data/event-streaming-library
    echo -e "${GREEN}✅ Cleaned${NC}"
    echo ""
fi

# 检查二进制文件
if [ ! -x "./target/debug/nexora-run" ]; then
    if [ ! -x "./target/debug/nexora" ]; then
        echo -e "${RED}❌ Binary not found. Please build first:${NC}"
        echo "   cargo build --features event-first,event-streaming,library"
        exit 1
    fi
    echo -e "${YELLOW}🔧 Creating stripped binary for macOS compatibility...${NC}"
    cp ./target/debug/nexora ./target/debug/nexora-run
    strip ./target/debug/nexora-run
fi

# 创建数据目录
mkdir -p ./nexora-data-node-a ./nexora-data-node-b ./nexora-data-node-c

echo -e "${BLUE}📋 Cluster Configuration:${NC}"
echo "  Cluster Name: nexora-test"
echo "  Replication Factor: 3"
echo "  Total Shards: 16"
echo "  RisingWave: Embedded (Library Mode with Raft HA)"
echo ""
echo -e "${BLUE}🔷 Node A:${NC}"
echo "  - HTTP API:       http://127.0.0.1:8080"
echo "  - Graph:          127.0.0.1:7100"
echo "  - Heartbeat:      127.0.0.1:7101"
echo "  - RW Meta:        127.0.0.1:5690 (Leader)"
echo "  - RW Frontend:    127.0.0.1:4566 (PostgreSQL)"
echo ""
echo -e "${BLUE}🔷 Node B:${NC}"
echo "  - HTTP API:       http://127.0.0.1:8081"
echo "  - Graph:          127.0.0.1:7010"
echo "  - Heartbeat:      127.0.0.1:7011"
echo "  - RW Meta:        127.0.0.1:5691"
echo "  - RW Frontend:    127.0.0.1:4567"
echo ""
echo -e "${BLUE}🔷 Node C:${NC}"
echo "  - HTTP API:       http://127.0.0.1:8082"
echo "  - Graph:          127.0.0.1:7020"
echo "  - Heartbeat:      127.0.0.1:7021"
echo "  - RW Meta:        127.0.0.1:5692"
echo "  - RW Frontend:    127.0.0.1:4568"
echo ""

# 定义日志目录
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"

echo -e "${YELLOW}🚀 Starting cluster nodes with embedded RisingWave...${NC}"
echo "   (Logs will be saved to $LOG_DIR/)"
echo ""

# 启动 Node A（RisingWave Meta Leader）
echo -e "${GREEN}▶ Starting Node A (RisingWave Meta Leader)...${NC}"
RUST_LOG=info,nexora_raft=debug,nexora_risingwave=debug \
./target/debug/nexora-run \
  --port 8080 \
  --cluster \
  --cluster-config cluster-node-a.yaml \
  --rocksdb-path ./nexora-data-node-a \
  --enable-event-streaming \
  --distributed-library-event-streaming \
  --library-node-id meta-1 \
  --library-meta-addr "0.0.0.0:5690" \
  --library-meta-advertise "127.0.0.1:5690" \
  --library-meta-peer "meta-2@127.0.0.1:5691" \
  --library-meta-peer "meta-3@127.0.0.1:5692" \
  --event-streaming-frontend-addr "127.0.0.1:4566" \
  --event-streaming-ha \
  --allow-unauthenticated \
  > "$LOG_DIR/node-a.log" 2>&1 &
NODE_A_PID=$!
echo "   PID: $NODE_A_PID"
sleep 3

# 启动 Node B
echo -e "${GREEN}▶ Starting Node B...${NC}"
RUST_LOG=info,nexora_raft=debug,nexora_risingwave=debug \
./target/debug/nexora-run \
  --port 8081 \
  --cluster \
  --cluster-config cluster-node-b.yaml \
  --rocksdb-path ./nexora-data-node-b \
  --enable-event-streaming \
  --distributed-library-event-streaming \
  --library-node-id meta-2 \
  --library-meta-addr "0.0.0.0:5691" \
  --library-meta-advertise "127.0.0.1:5691" \
  --library-meta-peer "meta-1@127.0.0.1:5690" \
  --library-meta-peer "meta-3@127.0.0.1:5692" \
  --event-streaming-frontend-addr "127.0.0.1:4567" \
  --event-streaming-ha \
  --allow-unauthenticated \
  > "$LOG_DIR/node-b.log" 2>&1 &
NODE_B_PID=$!
echo "   PID: $NODE_B_PID"
sleep 3

# 启动 Node C
echo -e "${GREEN}▶ Starting Node C...${NC}"
RUST_LOG=info,nexora_raft=debug,nexora_risingwave=debug \
./target/debug/nexora-run \
  --port 8082 \
  --cluster \
  --cluster-config cluster-node-c.yaml \
  --rocksdb-path ./nexora-data-node-c \
  --enable-event-streaming \
  --distributed-library-event-streaming \
  --library-node-id meta-3 \
  --library-meta-addr "0.0.0.0:5692" \
  --library-meta-advertise "127.0.0.1:5692" \
  --library-meta-peer "meta-1@127.0.0.1:5690" \
  --library-meta-peer "meta-2@127.0.0.1:5691" \
  --event-streaming-frontend-addr "127.0.0.1:4568" \
  --event-streaming-ha \
  --allow-unauthenticated \
  > "$LOG_DIR/node-c.log" 2>&1 &
NODE_C_PID=$!
echo "   PID: $NODE_C_PID"

echo ""
echo -e "${YELLOW}⏳ Waiting for nodes to initialize (8 seconds)...${NC}"
sleep 8

# 检查节点健康状态
echo ""
echo -e "${BLUE}🏥 Health Check:${NC}"

check_node() {
    local port=$1
    local name=$2
    local response=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:$port/ 2>/dev/null || echo "000")

    if [ "$response" == "200" ]; then
        echo -e "  ${GREEN}✓${NC} $name (port $port): ${GREEN}Healthy${NC}"
        return 0
    else
        echo -e "  ${YELLOW}⚠${NC} $name (port $port): ${YELLOW}Starting...${NC} (HTTP $response)"
        return 1
    fi
}

all_healthy=true
check_node 8080 "Node A" || all_healthy=false
check_node 8081 "Node B" || all_healthy=false
check_node 8082 "Node C" || all_healthy=false

echo ""
if [ "$all_healthy" = true ]; then
    echo -e "${GREEN}✅ All nodes are healthy!${NC}"
else
    echo -e "${YELLOW}⚠️  Some nodes may still be starting. Check logs in $LOG_DIR/${NC}"
fi

# 保存 PID 到文件
echo "$NODE_A_PID" > "$LOG_DIR/node-a.pid"
echo "$NODE_B_PID" > "$LOG_DIR/node-b.pid"
echo "$NODE_C_PID" > "$LOG_DIR/node-c.pid"

echo ""
echo -e "${BLUE}📊 Usage:${NC}"
echo "  - Web UI:         http://127.0.0.1:8080 (or 8081, 8082)"
echo "  - PostgreSQL:     psql -h 127.0.0.1 -p 4566 -U root -d dev"
echo "  - PostgreSQL:     psql -h 127.0.0.1 -p 4567 -U root -d dev"
echo "  - PostgreSQL:     psql -h 127.0.0.1 -p 4568 -U root -d dev"
echo ""
echo -e "${BLUE}📝 Logs:${NC}"
echo "  - tail -f $LOG_DIR/node-a.log"
echo "  - tail -f $LOG_DIR/node-b.log"
echo "  - tail -f $LOG_DIR/node-c.log"
echo ""
echo -e "${BLUE}🛑 Stop Cluster:${NC}"
echo "  - ./scripts/stop-cluster.sh"
echo ""
echo -e "${GREEN}✨ Cluster with embedded RisingWave is running!${NC}"
echo -e "${GREEN}   Each node has its own RisingWave Meta (HA via Raft) + Frontend${NC}"

# 可选：实时显示所有节点日志
if [ "$2" == "--follow" ]; then
    echo ""
    echo -e "${YELLOW}📜 Following all node logs (Ctrl+C to exit)...${NC}"
    echo ""
    tail -f "$LOG_DIR"/node-*.log
fi
