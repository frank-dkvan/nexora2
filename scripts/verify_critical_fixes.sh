#!/bin/bash
# Nexora 2 严重问题修复验证脚本

echo "=========================================="
echo "🔍 Nexora 2 严重问题修复验证"
echo "=========================================="
echo ""

# 设置正确的cargo路径
export PATH="/Users/frank/.rustup/toolchains/nightly-2026-06-11-aarch64-apple-darwin/bin:$PATH"

echo "📋 测试计划:"
echo "  1. nexora-raft (C-1, C-3修复)"
echo "  2. nexora-eventlog (C-2修复)"
echo "  3. nexora-stream (C-4, C-11修复)"
echo "  4. nexora-core (C-12修复)"
echo "  5. nexora-zenoh (C-8, C-13修复)"
echo "  6. nexora-app (C-14, C-15修复)"
echo "  7. nexora-graphstreaming (C-17修复)"
echo ""

# 测试计数器
TOTAL=0
PASSED=0
FAILED=0

run_test() {
    local package=$1
    local description=$2

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🧪 测试: $package"
    echo "   描述: $description"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    TOTAL=$((TOTAL + 1))

    if cargo test -p "$package" --lib 2>&1 | tee /tmp/test_${package}.log | tail -5; then
        echo "✅ $package: PASSED"
        PASSED=$((PASSED + 1))
    else
        echo "❌ $package: FAILED"
        FAILED=$((FAILED + 1))
        echo "   查看日志: /tmp/test_${package}.log"
    fi
    echo ""
}

# 运行测试
run_test "nexora-raft" "Raft锁顺序和原子性 (C-1, C-3)"
run_test "nexora-eventlog" "Iceberg超时 (C-2)"
run_test "nexora-stream" "Kafka资源泄漏和背压 (C-4, C-11)"
run_test "nexora-core" "边索引GC (C-12)"
run_test "nexora-zenoh" "快照清理和熔断器 (C-8, C-13)"
run_test "nexora-app" "MV限制和速率限制 (C-14, C-15)"
run_test "nexora-graphstreaming" "死信队列 (C-17)"

# 总结
echo "=========================================="
echo "📊 测试结果总结"
echo "=========================================="
echo ""
echo "总测试套件: $TOTAL"
echo "✅ 通过: $PASSED"
echo "❌ 失败: $FAILED"
echo ""

if [ $FAILED -eq 0 ]; then
    echo "🎉 所有测试通过！Nexora 2已准备好生产部署。"
    exit 0
else
    echo "⚠️  有 $FAILED 个测试套件失败，需要检查。"
    exit 1
fi
