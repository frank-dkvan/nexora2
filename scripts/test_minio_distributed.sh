#!/bin/bash
# 分布式 Nexora + MinIO 集成测试脚本
#
# 功能：
# 1. 启动 MinIO 容器
# 2. 创建测试 bucket
# 3. 运行分布式 MinIO 测试套件
# 4. 清理资源

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

log_success() {
    echo -e "${GREEN}✓${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

log_error() {
    echo -e "${RED}✗${NC} $1"
}

# 检查 Docker 是否运行
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        log_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    log_success "Docker is running"
}

# 启动 MinIO
start_minio() {
    log_info "Starting MinIO container..."

    # 检查是否已有 MinIO 容器在运行
    if docker ps -a --format '{{.Names}}' | grep -q "^nexora-minio-test$"; then
        log_warn "MinIO container 'nexora-minio-test' already exists"
        docker stop nexora-minio-test > /dev/null 2>&1 || true
        docker rm nexora-minio-test > /dev/null 2>&1 || true
    fi

    # 启动 MinIO
    docker run -d \
        --name nexora-minio-test \
        -p 9000:9000 \
        -p 9001:9001 \
        -e MINIO_ROOT_USER=minioadmin \
        -e MINIO_ROOT_PASSWORD=minioadmin \
        minio/minio server /data --console-address ":9001" > /dev/null

    log_success "MinIO container started"

    # 等待 MinIO 就绪
    log_info "Waiting for MinIO to be ready..."
    local retries=30
    local count=0
    while [ $count -lt $retries ]; do
        if curl -s http://localhost:9000/minio/health/ready > /dev/null 2>&1; then
            log_success "MinIO is ready"
            return 0
        fi
        count=$((count + 1))
        sleep 1
    done

    log_error "MinIO failed to start within 30 seconds"
    return 1
}

# 创建测试 bucket
create_bucket() {
    log_info "Creating test bucket..."

    # 使用 mc (MinIO Client) 或者 AWS CLI
    # 这里我们用简单的 curl 方式
    docker exec nexora-minio-test \
        mc alias set local http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1 || true

    docker exec nexora-minio-test \
        mc mb local/nexora-test-cluster --ignore-existing > /dev/null 2>&1 || true

    log_success "Bucket 'nexora-test-cluster' created"
}

# 运行测试
run_tests() {
    log_info "Running MinIO distributed tests..."
    echo ""

    cd "$PROJECT_ROOT"

    # 运行 MinIO 分布式测试
    cargo test --test minio_distributed_test \
        --features olap \
        --package nexora-eventlog \
        -- --nocapture --ignored

    local exit_code=$?
    echo ""

    if [ $exit_code -eq 0 ]; then
        log_success "All tests passed!"
        return 0
    else
        log_error "Some tests failed (exit code: $exit_code)"
        return $exit_code
    fi
}

# 运行单个测试
run_single_test() {
    local test_name=$1
    log_info "Running single test: $test_name"
    echo ""

    cd "$PROJECT_ROOT"

    cargo test --test minio_distributed_test \
        --features olap \
        --package nexora-eventlog \
        "$test_name" \
        -- --nocapture --ignored --exact

    return $?
}

# 显示 MinIO 信息
show_minio_info() {
    echo ""
    log_info "MinIO Access Information:"
    echo "  Console URL:  http://localhost:9001"
    echo "  API URL:      http://localhost:9000"
    echo "  Username:     minioadmin"
    echo "  Password:     minioadmin"
    echo "  Bucket:       nexora-test-cluster"
    echo ""
}

# 清理资源
cleanup() {
    log_info "Cleaning up..."

    if docker ps -a --format '{{.Names}}' | grep -q "^nexora-minio-test$"; then
        docker stop nexora-minio-test > /dev/null 2>&1
        docker rm nexora-minio-test > /dev/null 2>&1
        log_success "MinIO container removed"
    fi
}

# 保持 MinIO 运行（用于手动测试）
keep_minio_running() {
    show_minio_info
    log_warn "MinIO is still running. Use './scripts/test_minio_distributed.sh clean' to stop it."
}

# 主函数
main() {
    local command=${1:-"full"}

    echo "=========================================="
    echo "  Nexora + MinIO 分布式测试套件"
    echo "=========================================="
    echo ""

    case $command in
        "full")
            check_docker
            start_minio
            create_bucket
            show_minio_info
            run_tests
            local test_result=$?
            cleanup
            exit $test_result
            ;;

        "start")
            check_docker
            start_minio
            create_bucket
            keep_minio_running
            ;;

        "test")
            if [ -z "$2" ]; then
                run_tests
            else
                run_single_test "$2"
            fi
            ;;

        "clean")
            cleanup
            log_success "Cleanup completed"
            ;;

        "info")
            show_minio_info
            ;;

        "help")
            echo "Usage: $0 [command] [test_name]"
            echo ""
            echo "Commands:"
            echo "  full              Run full test suite (start MinIO, test, cleanup)"
            echo "  start             Start MinIO and keep it running"
            echo "  test [name]       Run tests (optionally specify test name)"
            echo "  clean             Stop and remove MinIO container"
            echo "  info              Show MinIO access information"
            echo "  help              Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                          # Run full test suite"
            echo "  $0 start                    # Start MinIO for manual testing"
            echo "  $0 test                     # Run all tests (MinIO must be running)"
            echo "  $0 test test_basic_write    # Run specific test"
            echo "  $0 clean                    # Stop MinIO"
            echo ""
            ;;

        *)
            log_error "Unknown command: $command"
            echo "Use '$0 help' for usage information"
            exit 1
            ;;
    esac
}

# 捕获 Ctrl+C
trap cleanup INT TERM

main "$@"
