#!/bin/bash
# ========================================
# Nexora 航空货运站演示 - 一键启动脚本
# ========================================
# 功能：构建项目 → 启动服务器 → 导入数据 → 运行演示查询

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "========================================="
echo "🛫 Nexora 航空货运站演示 - 一键启动"
echo "========================================="
echo ""

# 步骤1：检查并构建
echo "📦 步骤 1/4: 检查二进制文件..."
BINARY="${PROJECT_ROOT}/target/release/nexora-app"

if [ ! -f "$BINARY" ]; then
    echo "   二进制文件不存在，开始构建..."
    echo "   这可能需要几分钟时间..."
    echo ""

    cd "$PROJECT_ROOT"
    if ! cargo build --release -p nexora-app; then
        echo "❌ 构建失败"
        exit 1
    fi

    echo ""
    echo "✅ 构建完成"
else
    echo "✅ 二进制文件已存在"
fi

echo ""

# 步骤2：启动服务器
echo "🚀 步骤 2/4: 启动 Nexora 服务器..."

DATA_DIR="${PROJECT_ROOT}/data/demo"
mkdir -p "$DATA_DIR"

# 创建配置文件
cat > "$DATA_DIR/nexora-demo.toml" <<EOF
[server]
bind_address = "127.0.0.1:8080"
num_workers = 4

[storage]
backend = "RocksDB"
data_dir = "$DATA_DIR/rocksdb"

[raft]
node_id = 1
data_dir = "$DATA_DIR/raft"

[query]
max_pattern_depth = 10
max_execution_time_secs = 30
max_snapshot_nodes = 10_000_000
max_result_rows = 100_000

[rate_limit]
global_rate = 100_000
per_client_rate = 1_000
cleanup_interval_secs = 300

[circuit_breaker]
failure_threshold = 5
base_delay_ms = 100
max_delay_ms = 5000

[retry]
max_attempts = 3
base_delay_ms = 100
max_delay_ms = 5000
jitter_percent = 25
EOF

echo "   配置文件: $DATA_DIR/nexora-demo.toml"
echo "   数据目录: $DATA_DIR"
echo "   监听地址: http://127.0.0.1:8080"
echo ""

# 后台启动服务器
"$BINARY" --config "$DATA_DIR/nexora-demo.toml" > "$DATA_DIR/server.log" 2>&1 &
SERVER_PID=$!

echo "   服务器 PID: $SERVER_PID"
echo "   日志文件: $DATA_DIR/server.log"
echo ""

# 等待服务器启动
echo "   等待服务器就绪..."
for i in {1..30}; do
    if curl -s http://127.0.0.1:8080/health > /dev/null 2>&1; then
        echo "✅ 服务器启动成功"
        break
    fi

    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "❌ 服务器启动失败"
        echo "   查看日志: cat $DATA_DIR/server.log"
        exit 1
    fi

    sleep 1
    echo -n "."
done

echo ""
echo ""

# 步骤3：导入数据
echo "📊 步骤 3/4: 导入演示数据..."
echo ""

if [ -f "${PROJECT_ROOT}/scripts/load-demo-data.sh" ]; then
    "${PROJECT_ROOT}/scripts/load-demo-data.sh"
else
    echo "⚠️  数据导入脚本不存在，跳过此步骤"
fi

echo ""

# 步骤4：运行演示查询
echo "🔍 步骤 4/4: 运行演示查询..."
echo ""

if [ -f "${PROJECT_ROOT}/scripts/demo-queries.sh" ]; then
    "${PROJECT_ROOT}/scripts/demo-queries.sh"
else
    echo "⚠️  演示查询脚本不存在，跳过此步骤"
fi

echo ""
echo "========================================="
echo "✨ 演示环境已就绪！"
echo "========================================="
echo ""
echo "📌 服务器信息："
echo "   PID: $SERVER_PID"
echo "   地址: http://127.0.0.1:8080"
echo "   日志: $DATA_DIR/server.log"
echo "   数据: $DATA_DIR/rocksdb"
echo ""
echo "📖 接下来您可以："
echo "   1. 查看服务器日志:"
echo "      tail -f $DATA_DIR/server.log"
echo ""
echo "   2. 运行自定义查询:"
echo "      curl -X POST http://127.0.0.1:8080/api/query \\"
echo "           -H 'Content-Type: application/json' \\"
echo "           -d '{\"query\": \"MATCH (a:Airport) RETURN a LIMIT 5\"}'"
echo ""
echo "   3. 重新运行演示查询:"
echo "      ./scripts/demo-queries.sh"
echo ""
echo "   4. 查看完整文档:"
echo "      cat examples/AIR_CARGO_DEMO_README.md"
echo ""
echo "🛑 停止服务器:"
echo "   kill $SERVER_PID"
echo "   或按 Ctrl+C 结束此脚本（不会停止服务器）"
echo ""

# 将 PID 保存到文件
echo $SERVER_PID > "$DATA_DIR/server.pid"

echo "💡 提示: 服务器 PID 已保存到 $DATA_DIR/server.pid"
echo ""
