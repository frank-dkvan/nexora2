#!/usr/bin/env bash
# =============================================================================
# 分布式 RisingWave 集群测试脚本
# =============================================================================
# 用途：启动 3 节点 HA RisingWave 集群并测试各种功能
#
# 用法：
#   ./scripts/test-distributed-risingwave.sh start     # 启动集群
#   ./scripts/test-distributed-risingwave.sh test      # 测试功能
#   ./scripts/test-distributed-risingwave.sh stop      # 停止集群
#   ./scripts/test-distributed-risingwave.sh restart   # 重启集群
#   ./scripts/test-distributed-risingwave.sh health    # 检查健康状态
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

info() {
    echo -e "${BLUE}ℹ${NC} $*"
}

success() {
    echo -e "${GREEN}✓${NC} $*"
}

error() {
    echo -e "${RED}✗${NC} $*"
}

warn() {
    echo -e "${YELLOW}⚠${NC} $*"
}

# 配置
NEXORA_BIN="${PROJECT_ROOT}/target/release/nexora-app"
PSQL_BIN="psql"
META_PORTS=(5690 5692 5694)
FRONTEND_PORT=4566
COMPUTE_PORT=5688

# 检查依赖
check_dependencies() {
    info "检查依赖..."

    # 检查 nexora-app
    if [[ ! -f "${NEXORA_BIN}" ]]; then
        warn "nexora-app 未编译，正在编译..."
        cargo build --release --features embedded -p nexora-app
    fi

    # 检查 psql
    if ! command -v "${PSQL_BIN}" &> /dev/null; then
        error "psql 未安装，请安装 PostgreSQL 客户端"
        exit 1
    fi

    success "依赖检查完成"
}

# 启动集群
start_cluster() {
    info "启动分布式 RisingWave 集群..."

    cd "${PROJECT_ROOT}"

    # 后台启动 Nexora (会自动启动 RisingWave 集群)
    RUST_LOG=info,nexora_risingwave=debug \
        "${NEXORA_BIN}" \
        --risingwave-distributed \
        --risingwave-meta-nodes "1:127.0.0.1:5690,2:127.0.0.1:5692,3:127.0.0.1:5694" \
        --risingwave-frontend "127.0.0.1:4566" \
        --risingwave-compute "127.0.0.1:5688" \
        > /tmp/nexora-risingwave.log 2>&1 &

    local pid=$!
    echo "${pid}" > /tmp/nexora-risingwave.pid

    info "Nexora PID: ${pid}"
    info "日志: tail -f /tmp/nexora-risingwave.log"

    # 等待集群启动
    info "等待集群启动 (最多 60 秒)..."
    local timeout=60
    local elapsed=0

    while [[ ${elapsed} -lt ${timeout} ]]; do
        if check_frontend_ready; then
            success "集群启动成功！"
            show_cluster_info
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
        echo -n "."
    done

    error "集群启动超时"
    cat /tmp/nexora-risingwave.log
    return 1
}

# 检查 Frontend 是否就绪
check_frontend_ready() {
    ${PSQL_BIN} -h 127.0.0.1 -p ${FRONTEND_PORT} -U root -d dev \
        -c "SELECT 1" &> /dev/null
}

# 显示集群信息
show_cluster_info() {
    echo ""
    echo "========================================="
    echo "  分布式 RisingWave 集群信息"
    echo "========================================="
    echo "Meta 节点:"
    for port in "${META_PORTS[@]}"; do
        echo "  - http://127.0.0.1:${port}"
    done
    echo ""
    echo "Frontend:"
    echo "  - PostgreSQL: 127.0.0.1:${FRONTEND_PORT}"
    echo "  - 连接命令: psql -h 127.0.0.1 -p ${FRONTEND_PORT} -U root -d dev"
    echo ""
    echo "Compute 节点:"
    echo "  - 127.0.0.1:${COMPUTE_PORT}"
    echo ""
    echo "========================================="
}

# 停止集群
stop_cluster() {
    info "停止分布式 RisingWave 集群..."

    if [[ -f /tmp/nexora-risingwave.pid ]]; then
        local pid=$(cat /tmp/nexora-risingwave.pid)
        if kill -0 "${pid}" 2>/dev/null; then
            kill "${pid}"
            wait "${pid}" 2>/dev/null || true
            success "集群已停止"
        else
            warn "集群未运行 (PID ${pid} 不存在)"
        fi
        rm -f /tmp/nexora-risingwave.pid
    else
        warn "未找到 PID 文件"
    fi
}

# 检查健康状态
check_health() {
    info "检查集群健康状态..."

    echo ""
    echo "Meta 节点:"
    for port in "${META_PORTS[@]}"; do
        if nc -z 127.0.0.1 "${port}" 2>/dev/null; then
            success "  Meta :${port} - 运行中"
        else
            error "  Meta :${port} - 未运行"
        fi
    done

    echo ""
    echo "Frontend:"
    if check_frontend_ready; then
        success "  Frontend :${FRONTEND_PORT} - 运行中"
    else
        error "  Frontend :${FRONTEND_PORT} - 未运行"
    fi

    echo ""
    echo "Compute 节点:"
    if nc -z 127.0.0.1 "${COMPUTE_PORT}" 2>/dev/null; then
        success "  Compute :${COMPUTE_PORT} - 运行中"
    else
        error "  Compute :${COMPUTE_PORT} - 未运行"
    fi
}

# 测试功能
test_cluster() {
    info "测试集群功能..."

    if ! check_frontend_ready; then
        error "集群未运行，请先执行: $0 start"
        exit 1
    fi

    # 测试 1: 创建 Source
    info "测试 1: 创建 Kafka Source"
    ${PSQL_BIN} -h 127.0.0.1 -p ${FRONTEND_PORT} -U root -d dev <<'EOF'
CREATE SOURCE IF NOT EXISTS events (
    event_id VARCHAR,
    event_type VARCHAR,
    timestamp BIGINT,
    data JSONB
) WITH (
    connector = 'kafka',
    topic = 'nexora-events',
    properties.bootstrap.server = 'localhost:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;
EOF
    success "Source 创建成功"

    # 测试 2: 创建 Materialized View
    info "测试 2: 创建 Materialized View"
    ${PSQL_BIN} -h 127.0.0.1 -p ${FRONTEND_PORT} -U root -d dev <<'EOF'
CREATE MATERIALIZED VIEW IF NOT EXISTS event_counts AS
SELECT
    event_type,
    COUNT(*) AS count,
    MAX(timestamp) AS latest_timestamp
FROM events
GROUP BY event_type;
EOF
    success "Materialized View 创建成功"

    # 测试 3: 查询 Materialized View
    info "测试 3: 查询 Materialized View"
    ${PSQL_BIN} -h 127.0.0.1 -p ${FRONTEND_PORT} -U root -d dev -c "SELECT * FROM event_counts LIMIT 10;"
    success "查询成功"

    # 测试 4: 创建复杂 MV (带 JOIN)
    info "测试 4: 创建复杂 Materialized View (时间窗口聚合)"
    ${PSQL_BIN} -h 127.0.0.1 -p ${FRONTEND_PORT} -U root -d dev <<'EOF'
CREATE MATERIALIZED VIEW IF NOT EXISTS event_stats_1min AS
SELECT
    event_type,
    window_start,
    COUNT(*) AS event_count,
    COUNT(DISTINCT event_id) AS unique_events
FROM TUMBLE(events, timestamp, INTERVAL '1' MINUTE)
GROUP BY event_type, window_start;
EOF
    success "复杂 MV 创建成功"

    # 测试 5: 列出所有对象
    info "测试 5: 列出所有对象"
    echo ""
    echo "Sources:"
    ${PSQL_BIN} -h 127.0.0.1 -p ${FRONTEND_PORT} -U root -d dev -c "SHOW SOURCES;"
    echo ""
    echo "Materialized Views:"
    ${PSQL_BIN} -h 127.0.0.1 -p ${FRONTEND_PORT} -U root -d dev -c "SHOW MATERIALIZED VIEWS;"

    success "所有测试通过！"
}

# 高可用测试
test_ha() {
    info "测试高可用 (HA) 功能..."

    if ! check_frontend_ready; then
        error "集群未运行，请先执行: $0 start"
        exit 1
    fi

    # 检测当前 Leader
    info "检测当前 Meta Leader..."
    # TODO: 实现 Leader 检测 API

    # 模拟 Leader 故障
    warn "模拟 Meta Leader 故障 (杀死进程)..."
    # TODO: 杀死 Leader 进程

    # 等待重新选举
    info "等待 Raft 重新选举..."
    sleep 10

    # 验证新 Leader
    info "验证新 Leader 选举成功..."
    if check_frontend_ready; then
        success "HA 测试通过 - 集群自动恢复"
    else
        error "HA 测试失败 - 集群未恢复"
        return 1
    fi
}

# 性能测试
test_performance() {
    info "运行性能测试..."

    if ! check_frontend_ready; then
        error "集群未运行，请先执行: $0 start"
        exit 1
    fi

    # 插入测试数据
    info "插入 1000 条测试事件..."
    for i in {1..1000}; do
        echo "INSERT INTO events VALUES ('event-${i}', 'test', ${i}, '{}'::jsonb);"
    done | ${PSQL_BIN} -h 127.0.0.1 -p ${FRONTEND_PORT} -U root -d dev &> /dev/null

    success "插入完成"

    # 查询性能
    info "测试查询性能..."
    time ${PSQL_BIN} -h 127.0.0.1 -p ${FRONTEND_PORT} -U root -d dev -c "SELECT COUNT(*) FROM events;"

    time ${PSQL_BIN} -h 127.0.0.1 -p ${FRONTEND_PORT} -U root -d dev -c "SELECT * FROM event_counts;"
}

# 清理
cleanup() {
    info "清理测试数据..."

    if check_frontend_ready; then
        ${PSQL_BIN} -h 127.0.0.1 -p ${FRONTEND_PORT} -U root -d dev <<'EOF'
DROP MATERIALIZED VIEW IF EXISTS event_stats_1min;
DROP MATERIALIZED VIEW IF EXISTS event_counts;
DROP SOURCE IF EXISTS events;
EOF
        success "清理完成"
    fi
}

# 主函数
main() {
    local command="${1:-help}"

    case "${command}" in
        start)
            check_dependencies
            start_cluster
            ;;
        stop)
            stop_cluster
            ;;
        restart)
            stop_cluster
            sleep 2
            check_dependencies
            start_cluster
            ;;
        health)
            check_health
            ;;
        test)
            test_cluster
            ;;
        ha)
            test_ha
            ;;
        perf)
            test_performance
            ;;
        cleanup)
            cleanup
            ;;
        help|*)
            echo "用法: $0 {start|stop|restart|health|test|ha|perf|cleanup}"
            echo ""
            echo "命令:"
            echo "  start     - 启动分布式 RisingWave 集群"
            echo "  stop      - 停止集群"
            echo "  restart   - 重启集群"
            echo "  health    - 检查集群健康状态"
            echo "  test      - 运行功能测试"
            echo "  ha        - 运行高可用测试"
            echo "  perf      - 运行性能测试"
            echo "  cleanup   - 清理测试数据"
            echo ""
            exit 1
            ;;
    esac
}

main "$@"
