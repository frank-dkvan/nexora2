#!/bin/bash
# 停止 Nexora 分布式集群

set -e

echo "╔═══════════════════════════════════════════════════════════════════════╗"
echo "║              Stopping Nexora Distributed Cluster                      ║"
echo "╚═══════════════════════════════════════════════════════════════════════╝"
echo ""

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

LOG_DIR="./logs"

# 从 PID 文件读取并停止进程
stop_node() {
    local pid_file=$1
    local node_name=$2

    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p $pid > /dev/null 2>&1; then
            echo -e "${YELLOW}🛑 Stopping $node_name (PID: $pid)...${NC}"
            kill $pid
            sleep 1
            if ps -p $pid > /dev/null 2>&1; then
                echo -e "${RED}   Force killing...${NC}"
                kill -9 $pid
            fi
            echo -e "${GREEN}   ✓ Stopped${NC}"
        else
            echo -e "${YELLOW}   $node_name process not running${NC}"
        fi
        rm -f "$pid_file"
    else
        echo -e "${YELLOW}   No PID file for $node_name${NC}"
    fi
}

stop_node "$LOG_DIR/node-a.pid" "Node A"
stop_node "$LOG_DIR/node-b.pid" "Node B"
stop_node "$LOG_DIR/node-c.pid" "Node C"

# 额外检查：通过进程名查找并停止任何残留的 nexora 进程
echo ""
echo -e "${YELLOW}🔍 Checking for remaining nexora processes...${NC}"
REMAINING=$(pgrep -f "nexora-run.*--cluster" || true)
if [ -n "$REMAINING" ]; then
    echo -e "${YELLOW}   Found remaining processes: $REMAINING${NC}"
    pkill -f "nexora-run.*--cluster" || true
    sleep 1
    echo -e "${GREEN}   ✓ Cleaned up${NC}"
else
    echo -e "${GREEN}   ✓ No remaining processes${NC}"
fi

echo ""
echo -e "${GREEN}✅ Cluster stopped successfully!${NC}"
echo ""
echo -e "Data directories (preserved):"
echo "  - ./nexora-data-node-a"
echo "  - ./nexora-data-node-b"
echo "  - ./nexora-data-node-c"
echo ""
echo -e "To clean all data: rm -rf ./nexora-data-node-*"
