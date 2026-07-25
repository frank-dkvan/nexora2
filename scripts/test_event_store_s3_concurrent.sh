#!/usr/bin/env bash
# 多节点 Event Store S3 并发写入验证脚本

set -euo pipefail

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查 MinIO 是否运行
check_minio() {
    log_info "检查 MinIO 服务..."
    if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        log_info "✅ MinIO 正在运行"
        return 0
    else
        log_warn "MinIO 未运行"
        return 1
    fi
}

# 启动 MinIO
start_minio() {
    log_info "启动 MinIO Docker 容器..."
    docker run -d \
        -p 9000:9000 \
        -p 9001:9001 \
        --name nexora-minio-test \
        -e "MINIO_ROOT_USER=minioadmin" \
        -e "MINIO_ROOT_PASSWORD=minioadmin" \
        minio/minio server /data --console-address ":9001"

    log_info "等待 MinIO 启动..."
    sleep 5

    if check_minio; then
        log_info "✅ MinIO 启动成功"
        log_info "📊 MinIO Console: http://localhost:9001"
    else
        log_error "MinIO 启动失败"
        exit 1
    fi
}

# 创建测试 bucket
create_bucket() {
    log_info "创建测试 bucket: nexora-test-events"

    # 使用 AWS CLI 创建 bucket
    aws s3 mb s3://nexora-test-events \
        --endpoint-url http://localhost:9000 \
        --region us-east-1 2>/dev/null || true

    log_info "✅ Bucket 已创建"
}

# 清理环境
cleanup() {
    log_info "清理环境..."

    # 停止并删除 MinIO 容器
    if docker ps -a | grep -q nexora-minio-test; then
        docker stop nexora-minio-test > /dev/null 2>&1 || true
        docker rm nexora-minio-test > /dev/null 2>&1 || true
        log_info "✅ MinIO 容器已删除"
    fi

    # 清理临时数据
    rm -rf /tmp/nexora-test-node-*
    log_info "✅ 临时数据已清理"
}

# 运行单元测试
run_unit_tests() {
    log_info "===================="
    log_info "运行单元测试"
    log_info "===================="

    # 1. 本地文件系统并发测试（不需要 MinIO）
    log_info "1️⃣ 本地文件系统并发写入测试"
    cargo test --package nexora-eventlog --features olap \
        --test concurrent_s3_writes_test \
        test_local_fs_concurrent_writes -- --nocapture

    if [ $? -eq 0 ]; then
        log_info "✅ 本地文件系统测试通过"
    else
        log_error "❌ 本地文件系统测试失败"
        exit 1
    fi

    # 2. S3 并发测试（需要 MinIO）
    if check_minio; then
        log_info "2️⃣ S3 两节点并发写入测试"
        cargo test --package nexora-eventlog --features olap \
            --test concurrent_s3_writes_test \
            test_two_nodes_concurrent_writes -- --nocapture --ignored

        if [ $? -eq 0 ]; then
            log_info "✅ S3 两节点测试通过"
        else
            log_error "❌ S3 两节点测试失败"
            exit 1
        fi

        log_info "3️⃣ S3 高并发写入测试（5 节点）"
        cargo test --package nexora-eventlog --features olap \
            --test concurrent_s3_writes_test \
            test_high_concurrency_writes -- --nocapture --ignored

        if [ $? -eq 0 ]; then
            log_info "✅ S3 高并发测试通过"
        else
            log_error "❌ S3 高并发测试失败"
            exit 1
        fi

        log_info "4️⃣ S3 冲突解决测试"
        cargo test --package nexora-eventlog --features olap \
            --test concurrent_s3_writes_test \
            test_conflict_resolution -- --nocapture --ignored

        if [ $? -eq 0 ]; then
            log_info "✅ S3 冲突解决测试通过"
        else
            log_error "❌ S3 冲突解决测试失败"
            exit 1
        fi
    else
        log_warn "⏭️  跳过 S3 测试（MinIO 未运行）"
    fi
}

# 主函数
main() {
    log_info "===================="
    log_info "Event Store S3 并发写入验证"
    log_info "===================="

    # 解析参数
    CLEANUP_ONLY=false
    START_MINIO=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            --cleanup)
                CLEANUP_ONLY=true
                shift
                ;;
            --with-minio)
                START_MINIO=true
                shift
                ;;
            *)
                log_error "未知参数: $1"
                echo "用法: $0 [--cleanup] [--with-minio]"
                exit 1
                ;;
        esac
    done

    # 仅清理模式
    if [ "$CLEANUP_ONLY" = true ]; then
        cleanup
        exit 0
    fi

    # 启动 MinIO（如果需要）
    if [ "$START_MINIO" = true ]; then
        if check_minio; then
            log_info "MinIO 已在运行，跳过启动"
        else
            start_minio
            create_bucket
        fi
    fi

    # 运行测试
    run_unit_tests

    # 总结
    log_info "===================="
    log_info "✅ 所有测试通过！"
    log_info "===================="

    if [ "$START_MINIO" = true ]; then
        log_info ""
        log_info "提示：运行 '$0 --cleanup' 清理 MinIO 容器"
    fi
}

# 捕获 Ctrl+C
trap cleanup EXIT

# 运行主函数
main "$@"
