#!/bin/bash
# 启动 3 节点 Nexora 分布式集群
# 用于本地测试分布式功能

set -e

echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║           Nexora 3-Node Distributed Cluster Startup                  ║"
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo ""

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 清理旧数据（可选，通过参数控制）
if [ "$1" == "--clean" ]; then
    echo -e "${YELLOW}🧹 Cleaning old cluster data...${NC}"
    rm -rf ./nexora-data-node-a ./nexora-data-node-b ./nexora-data-node-c
    echo -e "${GREEN}✅ Cleaned${NC}"
    echo ""
fi

# 检查二进制文件
if [ ! -x "./target/debug/nexora-run" ]; then
    if [ ! -x "./target/debug/nexora" ]; then
        echo -e "${RED}❌ Binary not found. Please build first:${NC}"
        echo "   cargo build --features event-first"
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
echo ""
echo -e "${BLUE}🔷 Node A:${NC}"
echo "  - HTTP API:   http://127.0.0.1:8080"
echo "  - Graph:      127.0.0.1:7100"
echo "  - Heartbeat:  127.0.0.1:7101"
echo ""
echo -e "${BLUE}🔷 Node B:${NC}"
echo "  - HTTP API:   http://127.0.0.1:8081"
echo "  - Graph:      127.0.0.1:7010"
echo "  - Heartbeat:  127.0.0.1:7011"
echo ""
echo -e "${BLUE}🔷 Node C:${NC}"
echo "  - HTTP API:   http://127.0.0.1:8082"
echo "  - Graph:      127.0.0.1:7020"
echo "  - Heartbeat:  127.0.0.1:7021"
echo ""

# 定义日志目录
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"

echo -e "${YELLOW}🚀 Starting cluster nodes...${NC}"
echo "   (Logs will be saved to $LOG_DIR/)"
echo ""

# 启动 Node A
echo -e "${GREEN}▶ Starting Node A...${NC}"
RUST_LOG=info,nexora_raft=debug \
./target/debug/nexora-run \
  --port 8080 \
  --cluster \
  --cluster-config cluster-node-a.yaml \
  --rocksdb-path ./nexora-data-node-a \
  --allow-unauthenticated \
  > "$LOG_DIR/node-a.log" 2>&1 &
NODE_A_PID=$!
echo "   PID: $NODE_A_PID"
sleep 2

# 启动 Node B
echo -e "${GREEN}▶ Starting Node B...${NC}"
RUST_LOG=info,nexora_raft=debug \
./target/debug/nexora-run \
  --port 8081 \
  --cluster \
  --cluster-config cluster-node-b.yaml \
  --rocksdb-path ./nexora-data-node-b \
  --allow-unauthenticated \
  > "$LOG_DIR/node-b.log" 2>&1 &
NODE_B_PID=$!
echo "   PID: $NODE_B_PID"
sleep 2

# 启动 Node C
echo -e "${GREEN}▶ Starting Node C...${NC}"
RUST_LOG=info,nexora_raft=debug \
./target/debug/nexora-run \
  --port 8082 \
  --cluster \
  --cluster-config cluster-node-c.yaml \
  --rocksdb-path ./nexora-data-node-c \
  --allow-unauthenticated \
  > "$LOG_DIR/node-c.log" 2>&1 &
NODE_C_PID=$!
echo "   PID: $NODE_C_PID"

echo ""
echo -e "${YELLOW}⏳ Waiting for nodes to initialize (5 seconds)...${NC}"
sleep 5

# 检查节点健康状态
echo ""
echo -e "${BLUE}🏥 Health Check:${NC}"

check_node() {
    local port=$1
    local name=$2
    local response=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:$port/health 2>/dev/null || echo "000")

    if [ "$response" == "200" ]; then
        echo -e "  ${GREEN}✓${NC} $name (port $port): ${GREEN}Healthy${NC}"
        return 0
    else
        echo -e "  ${RED}✗${NC} $name (port $port): ${RED}Unavailable${NC} (HTTP $response)"
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
echo "  - Query Node A:  curl http://127.0.0.1:8080/health"
echo "  - Query Node B:  curl http://127.0.0.1:8081/health"
echo "  - Query Node C:  curl http://127.0.0.1:8082/health"
echo ""
echo -e "${BLUE}📝 Logs:${NC}"
echo "  - tail -f $LOG_DIR/node-a.log"
echo "  - tail -f $LOG_DIR/node-b.log"
echo "  - tail -f $LOG_DIR/node-c.log"
echo ""
echo -e "${BLUE}🛑 Stop Cluster:${NC}"
echo "  - ./scripts/stop-cluster.sh"
echo "  - Or: kill $NODE_A_PID $NODE_B_PID $NODE_C_PID"
echo ""
echo -e "${GREEN}✨ Cluster is running! Press Ctrl+C to monitor logs or run stop-cluster.sh to stop.${NC}"

# 可选：实时显示所有节点日志（需要用户按 Ctrl+C 停止）
if [ "$2" == "--follow" ]; then
    echo ""
    echo -e "${YELLOW}📜 Following all node logs (Ctrl+C to exit)...${NC}"
    echo ""
    tail -f "$LOG_DIR"/node-*.log
fi
