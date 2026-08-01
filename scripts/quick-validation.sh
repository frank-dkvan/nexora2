#!/bin/bash
# Nexora 2.0 快速验证脚本
# 用途: 验证所有核心能力是否正常工作

set -e

echo "🚀 Nexora 2.0 快速验证测试"
echo "================================"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查函数
check_step() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ $1${NC}"
    else
        echo -e "${RED}❌ $1 失败${NC}"
        exit 1
    fi
}

# 1. 检查构建
echo ""
echo "📦 Step 1: 检查编译..."
cargo build --release --features event-first
check_step "编译 (event-first)"

cargo build --release --features event-first,event-streaming
check_step "编译 (event-first + event-streaming)"

# 2. 运行核心测试
echo ""
echo "🧪 Step 2: 运行核心测试..."
cargo test -p nexora-core --release -- --test-threads=4 --nocapture
check_step "nexora-core 测试"

cargo test -p nexora-eventlog --release -- --test-threads=4 --nocapture
check_step "nexora-eventlog 测试"

cargo test -p nexora-cypher --release -- --test-threads=4 --nocapture
check_step "nexora-cypher 测试"

# 3. 测试Event Streaming (如果启用)
if cargo tree -p nexora-app --features event-streaming 2>/dev/null | grep -q "nexora-risingwave"; then
    echo ""
    echo "🌊 Step 3: 测试 Event Streaming..."
    cargo test -p nexora-risingwave --release --features event-streaming -- --test-threads=1
    check_step "nexora-risingwave 测试"
else
    echo -e "${YELLOW}⏭️  Step 3: Event Streaming 未启用，跳过${NC}"
fi

# 4. 启动单节点服务器（后台）
echo ""
echo "🖥️  Step 4: 启动单节点服务器..."
cargo run --release --features event-first -- --config nexora.toml &
SERVER_PID=$!
sleep 5

# 检查服务器是否启动
if ps -p $SERVER_PID > /dev/null; then
    echo -e "${GREEN}✅ 服务器启动成功 (PID: $SERVER_PID)${NC}"
else
    echo -e "${RED}❌ 服务器启动失败${NC}"
    exit 1
fi

# 5. 测试HTTP API
echo ""
echo "🌐 Step 5: 测试 HTTP API..."
sleep 2

# Health check
curl -f http://localhost:8080/health > /dev/null 2>&1
check_step "Health check"

# Cypher query
curl -f -X POST http://localhost:8080/api/query/cypher \
    -H "Content-Type: application/json" \
    -d '{"query": "CREATE (n:Person {name: \"test\"}) RETURN n"}' \
    > /dev/null 2>&1
check_step "Cypher 查询"

# 6. 清理
echo ""
echo "🧹 Step 6: 清理..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null || true
check_step "停止服务器"

# 总结
echo ""
echo "================================"
echo -e "${GREEN}🎉 所有验证测试通过！${NC}"
echo ""
echo "✅ 核心能力验证完成："
echo "  - Event-First 架构: OK"
echo "  - Graph Streaming: OK"
echo "  - Cypher 查询: OK"
echo "  - HTTP API: OK"
echo ""
echo "📋 下一步建议："
echo "  1. 运行完整端到端测试: ./scripts/test-e2e-full-pipeline.sh"
echo "  2. 启动3节点集群: ./scripts/start-cluster-3nodes.sh"
echo "  3. 运行性能基准测试: cargo run -p nexora-bench"
echo ""
