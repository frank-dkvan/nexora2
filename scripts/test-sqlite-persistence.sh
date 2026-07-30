#!/bin/bash
# SQLite 持久化验证测试脚本

set -e

echo "=========================================="
echo "  SQLite 持久化验证测试"
echo "=========================================="
echo ""

# 配置
DATA_DIR="/tmp/nexora-risingwave-test-$(date +%s)"
FRONTEND_PORT=4566
TEST_DB="dev"

echo "📁 测试数据目录: $DATA_DIR"
echo "🔌 Frontend 端口: $FRONTEND_PORT"
echo ""

# 清理函数
cleanup() {
    echo ""
    echo "🧹 清理测试环境..."
    if [ -n "$NEXORA_PID" ]; then
        echo "  停止 Nexora (PID: $NEXORA_PID)..."
        kill $NEXORA_PID 2>/dev/null || true
        wait $NEXORA_PID 2>/dev/null || true
    fi
    echo "  保留数据目录用于检查: $DATA_DIR"
    echo "✅ 清理完成"
}

trap cleanup EXIT

# 检查依赖
check_dependencies() {
    echo "🔍 检查依赖..."

    if ! command -v psql &> /dev/null; then
        echo "❌ 错误: psql 未安装"
        echo "   macOS: brew install postgresql"
        echo "   Ubuntu: sudo apt-get install postgresql-client"
        exit 1
    fi

    if [ ! -f "./target/release/nexora" ]; then
        echo "❌ 错误: nexora 未编译"
        echo "   请运行: cargo build --release --features risingwave,embedded"
        exit 1
    fi

    echo "✅ 依赖检查通过"
    echo ""
}

# 启动 Nexora
start_nexora() {
    echo "🚀 启动 Nexora (3 节点 RisingWave 集群)..."

    RISINGWAVE_DATA_DIR="$DATA_DIR" ./target/release/nexora \
        --enable-event-streams \
        --embedded-event-streams \
        --event-streams-cluster \
        --port 8080 \
        --rocksdb-path "$DATA_DIR/nexora-graph" \
        > "$DATA_DIR/nexora.log" 2>&1 &

    NEXORA_PID=$!
    echo "  PID: $NEXORA_PID"
    echo "  日志: $DATA_DIR/nexora.log"
    echo ""
}

# 等待就绪
wait_for_ready() {
    echo "⏳ 等待服务就绪..."

    local max_wait=120
    local waited=0

    while [ $waited -lt $max_wait ]; do
        if psql -h 127.0.0.1 -p $FRONTEND_PORT -d $TEST_DB -c "SELECT 1" &>/dev/null; then
            echo "✅ RisingWave Frontend 就绪"
            return 0
        fi

        sleep 2
        waited=$((waited + 2))
        echo -n "."
    done

    echo ""
    echo "❌ 超时：服务未在 ${max_wait}s 内就绪"
    echo ""
    echo "最后 50 行日志:"
    tail -50 "$DATA_DIR/nexora.log"
    exit 1
}

# 测试 1: 创建表并插入数据
test_create_and_insert() {
    echo ""
    echo "=========================================="
    echo "测试 1: 创建表并插入数据"
    echo "=========================================="

    psql -h 127.0.0.1 -p $FRONTEND_PORT -d $TEST_DB <<EOF
-- 创建测试表
CREATE TABLE test_persistence (
    id INT PRIMARY KEY,
    name VARCHAR,
    created_at TIMESTAMP
);

-- 插入测试数据
INSERT INTO test_persistence VALUES
    (1, 'Alice', NOW()),
    (2, 'Bob', NOW()),
    (3, 'Charlie', NOW());

-- 查询验证
SELECT * FROM test_persistence ORDER BY id;
EOF

    if [ $? -eq 0 ]; then
        echo "✅ 测试 1 通过"
    else
        echo "❌ 测试 1 失败"
        exit 1
    fi
}

# 验证 SQLite 文件
verify_sqlite_files() {
    echo ""
    echo "=========================================="
    echo "验证 SQLite 文件存在"
    echo "=========================================="

    local found_count=0

    for node in 1 2 3; do
        local meta_db="$DATA_DIR/meta-$node/.risingwave/meta.db"
        local state_dir="$DATA_DIR/meta-$node/.risingwave/state"

        echo ""
        echo "节点 $node:"

        if [ -f "$meta_db" ]; then
            local size=$(du -h "$meta_db" | cut -f1)
            echo "  ✅ meta.db 存在 (大小: $size)"
            found_count=$((found_count + 1))
        else
            echo "  ❌ meta.db 不存在: $meta_db"
        fi

        if [ -d "$state_dir" ]; then
            echo "  ✅ state/ 目录存在"
        else
            echo "  ⚠️  state/ 目录不存在: $state_dir"
        fi
    done

    echo ""
    if [ $found_count -eq 3 ]; then
        echo "✅ 所有节点的 SQLite 文件都存在"
    else
        echo "❌ 只找到 $found_count/3 个节点的 SQLite 文件"
        exit 1
    fi
}

# 测试 2: 重启并验证数据持久化
test_restart_persistence() {
    echo ""
    echo "=========================================="
    echo "测试 2: 重启并验证数据持久化"
    echo "=========================================="

    echo "🛑 停止 Nexora..."
    kill $NEXORA_PID
    wait $NEXORA_PID 2>/dev/null || true
    NEXORA_PID=""

    sleep 3

    echo ""
    echo "🚀 重新启动 Nexora..."
    start_nexora
    wait_for_ready

    echo ""
    echo "🔍 验证数据是否持久化..."

    local result=$(psql -h 127.0.0.1 -p $FRONTEND_PORT -d $TEST_DB -t -c "SELECT COUNT(*) FROM test_persistence;")
    local count=$(echo $result | tr -d ' ')

    echo "  查询结果: $count 行"

    if [ "$count" = "3" ]; then
        echo "✅ 数据持久化成功！"

        echo ""
        echo "完整数据:"
        psql -h 127.0.0.1 -p $FRONTEND_PORT -d $TEST_DB -c "SELECT * FROM test_persistence ORDER BY id;"
    else
        echo "❌ 数据丢失！期望 3 行，实际 $count 行"
        exit 1
    fi
}

# 主流程
main() {
    check_dependencies
    start_nexora
    wait_for_ready
    test_create_and_insert
    verify_sqlite_files
    test_restart_persistence

    echo ""
    echo "=========================================="
    echo "  🎉 所有测试通过！"
    echo "=========================================="
    echo ""
    echo "SQLite 持久化验证成功："
    echo "  ✅ 3 节点集群启动正常"
    echo "  ✅ SQLite 文件已创建"
    echo "  ✅ 数据写入成功"
    echo "  ✅ 重启后数据完整"
    echo ""
    echo "数据目录: $DATA_DIR"
    echo ""
}

main "$@"
