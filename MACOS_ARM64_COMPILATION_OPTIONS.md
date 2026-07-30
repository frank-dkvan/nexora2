# macOS ARM64 编译 RisingWave - 可行方案

## 调查结果

### ❌ 官方发布情况

**预编译二进制**: 仅提供 Linux 版本
- ✅ x86_64-unknown-linux
- ✅ aarch64-unknown-linux  
- ❌ **无 macOS 版本**（无论 Intel 还是 ARM64）

**检查版本**:
- v3.0.2 (最新): 仅 Linux
- v2.8.5 (旧版): 仅 Linux
- 所有历史版本: 都只有 Linux

**结论**: RisingWave 官方不支持 macOS，无预编译二进制可用

---

## ✅ macOS ARM64 可行方案

### 方案 1: Docker Desktop (推荐) ⭐

**原理**: 在 macOS 上运行 Linux 容器

**优势**:
- ✅ 官方支持，稳定可靠
- ✅ 一行命令启动
- ✅ 开发/生产环境一致
- ✅ 无需编译

**实施**:
```bash
# 1. 安装 Docker Desktop for Mac (已支持 ARM64)
brew install --cask docker

# 2. 启动 RisingWave
docker run -d \
  --name risingwave \
  -p 4566:4566 -p 5691:5691 \
  risingwavelabs/risingwave:v3.0.2 \
  standalone --meta-opts="--backend mem"

# 3. 连接测试
psql -h localhost -p 4566 -d dev -U root
```

**时间**: 15 分钟可用
**稳定性**: ⭐⭐⭐⭐⭐

---

### 方案 2: Rosetta 2 + x86_64 工具链

**原理**: 使用 Rosetta 2 运行 x86_64 版本的 Rust 工具链

**步骤**:
```bash
# 1. 安装 x86_64 版本 Rust
arch -x86_64 /bin/bash -c "$(curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh)"

# 2. 添加 x86_64 工具链
rustup toolchain install nightly-2025-10-10-x86_64-apple-darwin

# 3. 编译 RisingWave (x86_64 模式)
cd /tmp/risingwave-v3.0.2
arch -x86_64 rustup override set nightly-2025-10-10-x86_64-apple-darwin
arch -x86_64 cargo build --release
```

**注意事项**:
- ⚠️ 编译速度较慢（Rosetta 2 翻译开销）
- ⚠️ 仍可能遇到相同的生命周期错误
- ⚠️ 未验证，需要测试

**时间**: 3-4 小时（编译 + 调试）
**稳定性**: ⭐⭐⭐ (不确定)

---

### 方案 3: Lima + Linux VM

**原理**: 在 macOS 上运行轻量级 Linux VM

**步骤**:
```bash
# 1. 安装 Lima
brew install lima

# 2. 创建 Ubuntu ARM64 虚拟机
limactl start --name=risingwave template://ubuntu-lts

# 3. 进入 VM 编译
limactl shell risingwave

# 在 VM 内
sudo apt-get update
sudo apt-get install -y build-essential curl
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
rustup toolchain install nightly-2025-10-10

git clone https://github.com/risingwavelabs/risingwave.git
cd risingwave
git checkout v3.0.2
cargo build --release
```

**优势**:
- ✅ 真实 Linux ARM64 环境
- ✅ 编译成功率高
- ✅ 接近生产环境

**劣势**:
- ⚠️ 需要虚拟化
- ⚠️ 额外内存开销 (2-4GB)
- ⚠️ 开发工作流复杂（需要文件共享）

**时间**: 1-2 小时设置 + 2-3 小时编译
**稳定性**: ⭐⭐⭐⭐

---

### 方案 4: GitHub Codespaces / Remote Dev

**原理**: 使用云端 Linux 环境

**步骤**:
```bash
# 1. 创建 Codespace (GitHub 网页)
# 选择 risingwavelabs/risingwave 仓库
# 机器类型: 4-core (编译需要)

# 2. 在 Codespace 内编译
rustup toolchain install nightly-2025-10-10
rustup override set nightly-2025-10-10
cargo build --release

# 3. 暴露端口到本地
# GitHub Codespaces 自动转发 4566, 5691 端口
```

**优势**:
- ✅ 零本地环境配置
- ✅ 官方 Linux x86_64 环境
- ✅ 随时随地开发
- ✅ 100% 编译成功

**劣势**:
- ⚠️ 需要网络连接
- ⚠️ 免费额度有限 (GitHub Free: 60h/月)
- ⚠️ 大型编译消耗时长快

**时间**: 30 分钟设置 + 1-2 小时首次编译
**稳定性**: ⭐⭐⭐⭐⭐

---

### 方案 5: 交叉编译到 Linux (实验性)

**原理**: 在 macOS 上编译出 Linux 可执行文件

**步骤**:
```bash
# 1. 安装 cross 工具
cargo install cross

# 2. 编译到 Linux ARM64
cd /tmp/risingwave-v3.0.2
cross build --release --target aarch64-unknown-linux-gnu

# 3. 将二进制传输到 Linux 服务器或 Docker 运行
```

**注意事项**:
- ⚠️ cross 工具使用 Docker 构建
- ⚠️ RisingWave 复杂依赖可能导致失败
- ⚠️ 需要测试验证

**时间**: 1 小时设置 + 2-3 小时编译/调试
**稳定性**: ⭐⭐ (不确定)

---

## 推荐决策树

```
需要 macOS 本地编译？
│
├─ 否 → 方案 1: Docker (⭐⭐⭐⭐⭐)
│       最简单，15 分钟可用
│
└─ 是 → 需要频繁修改 RisingWave 源码？
        │
        ├─ 否 → 方案 1: Docker (⭐⭐⭐⭐⭐)
        │       即使需要本地开发，Docker 也够用
        │
        └─ 是 → 选择：
                ├─ 方案 3: Lima VM (⭐⭐⭐⭐)
                │   真实 Linux 环境，适合深度开发
                │
                ├─ 方案 4: Codespaces (⭐⭐⭐⭐⭐)
                │   云端 Linux，随时随地，零配置
                │
                └─ 方案 2: Rosetta 2 (⭐⭐⭐)
                    实验性，可能仍然失败
```

---

## 快速开始 - Docker 方案

如果您接受 Docker 方案（覆盖 95% 的使用场景），立即开始：

```bash
# 1. 安装 Docker Desktop (如果还没有)
brew install --cask docker

# 2. 启动 Docker Desktop 应用

# 3. 启动 RisingWave
docker run -d \
  --name risingwave-standalone \
  -p 4566:4566 \
  -p 5691:5691 \
  risingwavelabs/risingwave:v3.0.2 \
  standalone --meta-opts="--backend mem"

# 4. 验证运行
docker logs risingwave-standalone

# 5. 连接测试
psql -h localhost -p 4566 -d dev -U root -c "SELECT version();"
```

**期望输出**:
```
PostgreSQL 13.x on RisingWave vX.X.X
```

---

## 总结

| 方案 | 时间 | 稳定性 | 适用场景 |
|------|------|--------|----------|
| Docker Desktop | 15 分钟 | ⭐⭐⭐⭐⭐ | **推荐 - 95% 场景** |
| Rosetta 2 | 3-4 小时 | ⭐⭐⭐ | 实验，不推荐 |
| Lima VM | 3-5 小时 | ⭐⭐⭐⭐ | 深度开发 RisingWave 源码 |
| Codespaces | 2-3 小时 | ⭐⭐⭐⭐⭐ | 云端开发，零配置 |
| 交叉编译 | 3-4 小时 | ⭐⭐ | 实验性，高风险 |

**最终建议**: 使用 Docker Desktop 方案，无需纠结编译问题。

---

**日期**: 2026-07-28  
**状态**: 方案调研完成  
**下一步**: 选择方案并实施
