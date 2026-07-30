# RisingWave v3.0.2 编译阻塞问题

## 问题总结

**状态**: ❌ BLOCKED - 无法在 macOS 上从源代码编译 RisingWave v3.0.2

**核心错误**: 65 个生命周期/HRTB 错误在 `risingwave_meta` crate

## 尝试的解决方案

### 1. 工具链配置 ✅
- 使用 `nightly-2025-10-10` (官方指定)
- 验证 `rustc --version` 显示 `1.92.0-nightly (b925a865e 2025-10-09)`
- 结果: 工具链正确

### 2. Workspace 隔离 ✅
- 问题: Nexora 的 prost 0.13 与 RisingWave 的 prost 0.14 冲突
- 解决: 从 Nexora Cargo.toml 中注释掉所有 RisingWave 相关 crate
- 结果: 依赖冲突解决

### 3. PATH 优先级 ✅
- 问题: Homebrew 的 rustc 优先于 rustup
- 解决: export PATH="$HOME/.cargo/bin:$PATH"
- 结果: 使用正确的 rustup 工具链

### 4. 编译标志 ❌
- 尝试: 启用 -Zhigher-ranked-assumptions 在 .cargo/config.toml
- 结果: 无效，仍然 65 个错误

### 5. 官方仓库验证 ❌
- 尝试: 克隆官方 v3.0.2 标签独立编译
- 结果: 相同的 65 个编译错误

## 典型错误示例

```
error: implementation of Iterator is not general enough
  --> src/meta/src/hummock/manager/timer_task.rs:45:27

= note: Iterator would have to be implemented for 
        std::collections::hash_set::Iter<'_, TypedId<0, u32>>
= note: ...but Iterator is actually implemented for 
        std::collections::hash_set::Iter<'0, TypedId<0, u32>>, 
        for some specific lifetime '0
```

## 受影响的文件
- src/meta/src/barrier/context/context_impl.rs
- src/meta/src/barrier/worker.rs
- src/meta/src/hummock/manager/compaction/compaction_event_loop.rs
- src/meta/src/hummock/manager/timer_task.rs
- src/meta/src/rpc/ddl_controller.rs
- src/storage/src/hummock/store/local_hummock_storage.rs
- src/storage/src/memory.rs
- src/storage/src/store_impl.rs

总计: 65 个编译错误

## 验证的构建环境

```
macOS:   Darwin 25.3.0 (darwin-arm64)
rustc:   1.92.0-nightly (b925a865e 2025-10-09)
cargo:   1.92.0-nightly (0722bdc11 2025-10-03)
CPU:     Apple Silicon (ARM64)
```

## 对比：官方 CI 环境

```
OS:       Ubuntu 24.04
rustc:    nightly-2025-10-10
Linker:   lld
CPU:      x86_64
```

## 实际可行的方案

### 方案 A: Docker 容器运行 RisingWave ⭐ 推荐

优点:
- 避免源代码编译问题
- 使用官方测试过的环境 (Linux)
- 简单可靠
- 易于部署和分发

实现:
```bash
# docker-compose.yml
services:
  risingwave-standalone:
    image: risingwavelabs/risingwave:v3.0.2
    ports:
      - "4566:4566"
      - "5690:5690"
    command: standalone
```

### 方案 B: 使用预编译的 Linux 二进制 (Docker 内)

优点:
- 不需要源代码编译
- 使用官方发布的二进制
- 比完整 Docker 镜像更轻量

### 方案 C: 仅在 Linux 上支持 RisingWave 集成

优点:
- 避免跨平台编译问题
- 简化开发和测试

缺点:
- macOS 用户无法使用 RisingWave 功能
- 开发体验不一致

## 推荐实施路径

### Phase 7.1: Docker 集成 (1 周)

1. 创建 Docker Compose 配置
2. 实现 Docker 后端管理
3. 测试 Docker 启动和连接

### Phase 7.2: 进程管理 (1 周)

支持两种后端:
- Docker (macOS 和 Linux)
- Process (仅 Linux)

### Phase 7.3: 集成到 nexora-app (1 周)

通过 feature flag 和配置文件控制

## 结论

1. RisingWave v3.0.2 无法在 macOS 上从源代码编译
2. Docker 方案是最实际的集成路径
3. 库模式集成暂时不可行
4. 推荐立即切换到 Docker 方案

---

创建日期: 2026-07-28
状态: BLOCKED - 等待方案决策
