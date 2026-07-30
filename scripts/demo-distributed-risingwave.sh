#!/usr/bin/env bash
# =============================================================================
# 分布式 RisingWave 集成演示
# =============================================================================
# 自动运行演示程序，展示所有功能
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# 颜色输出
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}"
cat << 'EOF'
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║   Nexora 2.0 - 分布式 RisingWave 集成演示                     ║
║   Phase 8: Distributed Embedded RisingWave                    ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

echo ""
echo -e "${GREEN}功能特性:${NC}"
echo "  ✅ 3 节点 Meta 集群 (Raft HA)"
echo "  ✅ 1 个 Frontend 节点 (PostgreSQL 协议)"
echo "  ✅ 动态 Compute 节点 (自动并行)"
echo "  ✅ 实时流式 SQL 处理"
echo "  ✅ 集群健康监控"
echo "  ✅ 自动生命周期管理"
echo ""

echo -e "${YELLOW}准备运行...${NC}"
echo ""

# 切换到项目根目录
cd "${PROJECT_ROOT}"

# 检查编译
if [[ ! -f "target/release/examples/distributed_risingwave_demo" ]]; then
    echo "首次运行，正在编译..."
    cargo build --release --features embedded \
        -p nexora-risingwave \
        --example distributed_risingwave_demo
    echo ""
fi

# 运行演示
echo -e "${GREEN}启动演示程序...${NC}"
echo ""
echo "========================================="
echo ""

cargo run --release --features embedded \
    -p nexora-risingwave \
    --example distributed_risingwave_demo

echo ""
echo "========================================="
echo ""
echo -e "${GREEN}演示完成！${NC}"
echo ""
echo "📖 更多信息:"
echo "   - 快速入门: docs/DISTRIBUTED_RISINGWAVE_QUICKSTART.md"
echo "   - 详细指南: docs/DISTRIBUTED_RISINGWAVE_TESTING_GUIDE.md"
echo "   - 完整总结: docs/DISTRIBUTED_RISINGWAVE_COMPLETE.md"
echo ""
echo "🛠️  测试工具:"
echo "   - 测试脚本: ./scripts/test-distributed-risingwave.sh"
echo "   - 手动连接: psql -h 127.0.0.1 -p 4566 -U root -d dev"
echo ""
