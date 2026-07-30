# Phase 7.5 快速参考

## 启动命令

### 嵌入式模式（开发/测试）
```bash
# 首次构建 RisingWave 二进制
./scripts/build-embedded-risingwave.sh

# 启动 Nexora（自动启动 RisingWave）
cargo run --release --features embedded -- \
  --enable-risingwave \
  --enable-embedded-risingwave
```

### 外部服务模式（生产）
```bash
# RisingWave 独立运行
# 启动 Nexora（连接外部服务）
cargo run --release --features risingwave -- \
  --enable-risingwave \
  --risingwave-meta-addr "risingwave-meta:5690" \
  --risingwave-frontend-addr "risingwave-frontend:4566"
```

## 健康检查

```bash
curl http://localhost:8080/api/health/risingwave
```

**响应（嵌入式）**:
```json
{
  "enabled": true,
  "connected": true,
  "embedded_info": {
    "embedded": true,
    "pid": 12345,
    "state": "Running"
  }
}
```

## 环境变量

```bash
# 指定 RisingWave 二进制路径
export RISINGWAVE_BIN=/path/to/risingwave

# 启动
cargo run --features embedded -- \
  --enable-risingwave \
  --enable-embedded-risingwave
```

## CLI 参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--enable-risingwave` | 启用 RisingWave 集成 | false |
| `--enable-embedded-risingwave` | 使用嵌入式进程 | false |
| `--risingwave-meta-addr` | Meta 节点地址 | 127.0.0.1:5690 |
| `--risingwave-frontend-addr` | Frontend 节点地址 | 127.0.0.1:4566 |

## 特性标志

```bash
# 基础（无 RisingWave）
cargo build -p nexora-app

# 外部 RisingWave
cargo build -p nexora-app --features risingwave

# 嵌入式 RisingWave
cargo build -p nexora-app --features embedded
```

## 文档

- [详细报告](RISINGWAVE_PHASE7.5_REPORT.md) - 技术实现详解
- [验证清单](RISINGWAVE_PHASE7.5_VERIFICATION.md) - 测试验证结果
- [完成总结](RISINGWAVE_PHASE7.5_SUMMARY.md) - 快速概览

## 故障排查

### 问题 1: 找不到 RisingWave 二进制

**错误**:
```
RisingWave binary not found. Please:
1. Set RISINGWAVE_BIN environment variable, or
2. Run: make build-risingwave, or
3. Install RisingWave to system PATH
```

**解决**:
```bash
# 方案 1: 构建二进制
./scripts/build-embedded-risingwave.sh

# 方案 2: 设置环境变量
export RISINGWAVE_BIN=/path/to/risingwave
```

### 问题 2: 启动超时

**错误**:
```
RisingWave startup timeout after 60s
```

**解决**:
- 检查端口是否被占用（5690, 4566）
- 查看 RisingWave 进程日志
- 增加超时时间（修改 `startup_timeout_secs`）

### 问题 3: 端口冲突

**错误**:
```
Address already in use
```

**解决**:
```bash
# 使用自定义端口
cargo run --features embedded -- \
  --enable-risingwave \
  --enable-embedded-risingwave \
  --risingwave-meta-addr "127.0.0.1:15690" \
  --risingwave-frontend-addr "127.0.0.1:14566"
```

## 性能指标

| 指标 | 值 |
|------|-----|
| 启动时间 | ~5 秒 |
| 内存占用 | ~1.7 GB |
| gRPC 延迟 | <1 ms (localhost) |
| 编译时间增量 | ~4 秒 |

## 下一步

- [ ] Phase 7.6: 完善测试和文档
- [ ] 端到端集成测试
- [ ] 用户使用指南
- [ ] 性能基准测试

---

**完成日期**: 2026-07-26  
**Phase 7.5 状态**: ✅ 完成
