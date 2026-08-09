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

# 2. 确保 rustup proxy 优先于 Homebrew 的 stable cargo/rustc。
#    否则 PATH 上的 Homebrew cargo 会无视 rust-toolchain.toml 的 nightly pin，
#    导致 "profile-rustflags requires nightly" / "-Z only on nightly" 报错。
export PATH="$HOME/.cargo/bin:$PATH"

# 3. 版本由 rust-toolchain.toml 统一 pin（勿硬编码日期，勿用泛型 nightly）。
#    走 proxy 会自动安装并选中 pin 的那个 nightly。
info "Toolchain: $(cargo --version)  |  $(rustc --version)"
if ! rustc --version | grep -q "nightly"; then
    error "Not using nightly. Homebrew cargo/rustc is shadowing the rustup proxy."
    error "Ensure \$HOME/.cargo/bin precedes /opt/homebrew/bin on PATH."
    exit 1
fi

# 4. 切换到 RisingWave 目录（rust-toolchain.toml 在仓库根，pin 对整个 workspace 生效）
cd vendor/risingwave

# 5. 编译 RisingWave (standalone 模式)
info "Compiling RisingWave (this may take 10-20 minutes)..."

# 使用 release 模式，优化大小。
# grep 只用于过滤 warning 噪声，绝不能吞掉 cargo 的失败：
#   - 管道中 $? 是 grep 的退出码而非 cargo 的，故读 ${PIPESTATUS[0]}；
#   - 旧写法结尾的 `|| true` 会把失败强制变成 exit 0，导致 CI 假绿。
# 这里显式取出 cargo 的真实退出码并据此判断。
set +e
cargo build --release --bin risingwave \
    --no-default-features \
    --features "rw-static-link" \
    2>&1 | grep -v "warning:"
cargo_status=${PIPESTATUS[0]}
set -e

if [ "$cargo_status" -ne 0 ]; then
    error "Build failed: cargo exited with status $cargo_status"
    exit "$cargo_status"
fi

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
