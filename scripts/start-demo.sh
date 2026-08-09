#!/bin/bash
# ========================================
# Nexora 航空货运站演示 - 快速启动脚本
# ========================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="${PROJECT_ROOT}/data/demo"
BINARY="${PROJECT_ROOT}/target/release/nexora-app"

echo "========================================="
echo "Nexora 航空货运站演示"
echo "========================================="
echo ""

# 检查二进制文件
if [ ! -f "$BINARY" ]; then
    echo "❌ 错误：未找到 nexora-app 二进制文件"
    echo "请先运行: cargo build --release -p nexora-app"
    exit 1
fi

# 创建数据目录
mkdir -p "$DATA_DIR"

# 清理旧数据（可选）
if [ -d "$DATA_DIR/rocksdb" ]; then
    echo "⚠️  发现现有数据目录"
    read -p "是否清理现有数据并重新开始? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🗑️  清理现有数据..."
        rm -rf "$DATA_DIR/rocksdb"
        rm -rf "$DATA_DIR/raft"
        echo "✅ 清理完成"
    fi
fi

# 创建临时配置文件
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
# 单节点模式，不需要集群配置

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

echo "📝 配置文件已创建: $DATA_DIR/nexora-demo.toml"
echo ""

# 启动服务器
echo "🚀 启动 Nexora 服务器..."
echo "   监听地址: http://127.0.0.1:8080"
echo "   数据目录: $DATA_DIR"
echo "   配置文件: $DATA_DIR/nexora-demo.toml"
echo ""
echo "⏳ 服务器启动中..."
echo ""

# 启动服务器（前台运行）
exec "$BINARY" --config "$DATA_DIR/nexora-demo.toml"
