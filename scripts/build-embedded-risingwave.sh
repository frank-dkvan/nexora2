#!/usr/bin/env bash
#
# 构建嵌入式 RisingWave 二进制
#
# 该脚本将 RisingWave 编译为独立的二进制文件，供 Nexora 的嵌入式模式使用。
#
# 使用方法：
#   ./scripts/build-embedded-risingwave.sh
#
# 输出：
#   bin/risingwave-embedded - RisingWave standalone 模式二进制

set -euo pipefail

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

info() {
    echo -e "${GREEN}✓${NC} $1"
}

warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

error() {
    echo -e "${RED}✗${NC} $1"
}

# 项目根目录
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

info "Building embedded RisingWave binary..."

# 1. 检查 RisingWave 源码
if [ ! -d "vendor/risingwave" ]; then
    error "RisingWave source not found in vendor/risingwave"
    error "Please run: ./scripts/init-risingwave.sh"
    exit 1
fi

# 2. 检查 nightly Rust
if ! rustup toolchain list | grep -q nightly; then
    warn "Nightly Rust not installed, installing..."
    rustup toolchain install nightly
fi

# 3. 切换到 RisingWave 目录
cd vendor/risingwave

# 4. 设置本目录使用 nightly（不影响其他项目）
rustup override set nightly
info "Using nightly Rust for RisingWave"

# 5. 编译 RisingWave (standalone 模式)
info "Compiling RisingWave (this may take 10-20 minutes)..."

# 使用 release 模式，优化大小
cargo build --release --bin risingwave \
    --no-default-features \
    --features "rw-static-link" \
    2>&1 | grep -v "warning:" || true

if [ ! -f "target/release/risingwave" ]; then
    error "Build failed: target/release/risingwave not found"
    exit 1
fi

info "Build successful!"

# 6. 复制到项目 bin 目录
cd "$PROJECT_ROOT"
mkdir -p bin

cp vendor/risingwave/target/release/risingwave bin/risingwave-embedded
chmod +x bin/risingwave-embedded

# 7. 验证二进制
BIN_SIZE=$(du -h bin/risingwave-embedded | cut -f1)
info "Binary size: $BIN_SIZE"

# 8. Strip 调试符号（可选，减小体积）
if command -v strip &> /dev/null; then
    info "Stripping debug symbols..."
    strip bin/risingwave-embedded
    STRIPPED_SIZE=$(du -h bin/risingwave-embedded | cut -f1)
    info "Stripped size: $STRIPPED_SIZE"
fi

# 9. 测试二进制
info "Testing binary..."
if bin/risingwave-embedded --version &> /dev/null; then
    VERSION=$(bin/risingwave-embedded --version | head -1)
    info "RisingWave version: $VERSION"
else
    warn "Binary test failed (this is normal if dependencies are missing)"
fi

# 10. 清理（可选）
read -p "Clean build artifacts to save disk space? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    info "Cleaning build artifacts..."
    cd vendor/risingwave
    cargo clean
    cd "$PROJECT_ROOT"
    info "Cleaned!"
fi

echo ""
info "✓ Embedded RisingWave binary ready at: bin/risingwave-embedded"
info ""
info "Usage:"
info "  nexora --enable-embedded-risingwave"
info ""
info "Or set environment variable:"
info "  export RISINGWAVE_BIN=\$PWD/bin/risingwave-embedded"
