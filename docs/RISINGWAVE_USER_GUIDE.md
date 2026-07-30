# RisingWave 集成用户指南

**文档版本**: 1.0  
**更新日期**: 2026-07-26  
**适用版本**: Nexora 2.0 with RisingWave Integration

---

## 目录

1. [快速开始](#快速开始)
2. [配置方式](#配置方式)
3. [部署模式](#部署模式)
4. [配置参考](#配置参考)
5. [使用示例](#使用示例)
6. [故障排查](#故障排查)
7. [性能调优](#性能调优)
8. [最佳实践](#最佳实践)

---

## 快速开始

### 前置要求

- **Rust**: 1.75+
- **特性标志**: 需要编译时启用 `risingwave` 特性
- **嵌入式模式**: 额外需要 `embedded` 特性

### 30 秒体验

```bash
# 1. 编译（包含 RisingWave 支持）
cargo build --release --features risingwave,embedded

# 2. 启动（嵌入式模式）
./target/release/nexora-app \
  --enable-risingwave \
  --enable-embedded-risingwave

# 3. 验证
curl http://localhost:8080/api/health/risingwave
```

**预期输出**:
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

---

## 配置方式

Nexora 支持三种配置方式，优先级从高到低：

### 1. CLI 参数（最高优先级）

```bash
./nexora-app \
  --enable-risingwave \
  --enable-embedded-risingwave \
  --risingwave-meta-addr 127.0.0.1:5690 \
  --risingwave-frontend-addr 127.0.0.1:4566
```

**优点**: 灵活，适合测试和临时调整  
**缺点**: 命令行冗长，不易管理

### 2. 配置文件（推荐）

创建 `nexora.toml`:

```toml
[risingwave]
enabled = true
embedded = true
meta_addr = "127.0.0.1:5690"
frontend_addr = "127.0.0.1:4566"
data_dir = "./nexora-data/risingwave"
startup_timeout_secs = 60
shutdown_timeout_secs = 30
parallelism = 4
```

启动：
```bash
./nexora-app  # 自动加载 nexora.toml
```

**优点**: 声明式，易于版本控制，生产推荐  
**缺点**: 需要重启生效

### 3. 环境变量（未实现）

当前版本不支持环境变量配置，计划在未来版本添加。

### 配置文件搜索顺序

1. `--config <path>` 指定的路径
2. 当前目录的 `./nexora.toml`
3. 使用硬编码默认值

**示例**:
```bash
# 使用自定义配置文件
./nexora-app --config /etc/nexora/production.toml
```

---

## 部署模式

### 模式 1: 嵌入式模式（开发/单机）

**特点**:
- RisingWave 作为子进程运行
- Nexora 自动管理生命周期
- 无需外部依赖
- 适合开发、测试、小规模部署

**配置**:
```toml
[risingwave]
enabled = true
embedded = true
meta_addr = "127.0.0.1:5690"
frontend_addr = "127.0.0.1:4566"
```

**启动**:
```bash
cargo run --release --features embedded
```

**资源需求**:
- 内存: ~2GB (Nexora 500MB + RisingWave 1.5GB)
- CPU: 2+ 核心
- 磁盘: 根据数据量（建议 SSD）

### 模式 2: 外部服务模式（生产）

**特点**:
- RisingWave 独立部署
- 支持 HA 和横向扩展
- Nexora 仅作为客户端连接
- 适合生产、多节点集群

**配置**:
```toml
[risingwave]
enabled = true
embedded = false
meta_addr = "10.0.1.10:5690"
frontend_addr = "10.0.1.20:4566"
```

**前置步骤**:
```bash
# 1. 部署 RisingWave 集群（参考 RisingWave 官方文档）
# 2. 验证连接
psql -h 10.0.1.20 -p 4566 -d dev -U root

# 3. 启动 Nexora（无需 embedded 特性）
cargo run --release --features risingwave
```

**资源需求**:
- Nexora: 500MB 内存
- RisingWave: 独立规划（参考 RisingWave 文档）

### 模式 3: 禁用 RisingWave（默认）

**特点**:
- 纯 Nexora 核心功能
- 最小资源占用
- 适合不需要 SQL 流处理的场景

**配置**:
```toml
# 不配置 [risingwave] 节，或显式禁用
[risingwave]
enabled = false
```

**启动**:
```bash
cargo run --release  # 无需 risingwave 特性
```

---

## 配置参考

### 完整配置示例

```toml
[risingwave]
# 是否启用 RisingWave 集成
enabled = true

# 是否使用嵌入式模式（需要 --features embedded）
embedded = true

# Meta 节点地址（集群元数据管理）
meta_addr = "127.0.0.1:5690"

# Frontend 节点地址（SQL 查询入口）
frontend_addr = "127.0.0.1:4566"

# ===== 以下仅嵌入式模式生效 =====

# RisingWave 二进制路径（可选，默认自动查找）
# 查找顺序: RISINGWAVE_BIN 环境变量 → bin/risingwave-embedded → PATH
# binary_path = "/usr/local/bin/risingwave"

# 数据目录
data_dir = "./nexora-data/risingwave"

# 启动超时（秒）
startup_timeout_secs = 60

# 关闭超时（秒）
shutdown_timeout_secs = 30

# Compute 节点并行度（默认: CPU 核心数）
parallelism = 4
```

### 配置字段详解

| 字段 | 类型 | 默认值 | 必需 | 说明 |
|------|------|--------|------|------|
| `enabled` | bool | `false` | 否 | 启用 RisingWave 集成 |
| `embedded` | bool | `false` | 否 | 使用嵌入式模式 |
| `meta_addr` | string | `"127.0.0.1:5690"` | 否 | Meta 节点地址 |
| `frontend_addr` | string | `"127.0.0.1:4566"` | 否 | Frontend 节点地址 |
| `binary_path` | string | (自动查找) | 否 | RisingWave 二进制路径 |
| `data_dir` | string | `"./nexora-data/risingwave"` | 否 | 数据目录 |
| `startup_timeout_secs` | u64 | `60` | 否 | 启动超时 |
| `shutdown_timeout_secs` | u64 | `30` | 否 | 关闭超时 |
| `parallelism` | usize | (CPU 核心数) | 否 | Compute 并行度 |

### CLI 参数映射

| CLI 参数 | 配置字段 | 说明 |
|----------|----------|------|
| `--enable-risingwave` | `enabled` | 启用 RisingWave |
| `--enable-embedded-risingwave` | `embedded` | 启用嵌入式模式 |
| `--risingwave-meta-addr` | `meta_addr` | Meta 节点地址 |
| `--risingwave-frontend-addr` | `frontend_addr` | Frontend 节点地址 |
| 无 | `binary_path` | （仅配置文件） |
| 无 | `data_dir` |（仅配置文件） |
| 无 | `startup_timeout_secs` | （仅配置文件） |
| 无 | `shutdown_timeout_secs` | （仅配置文件） |
| 无 | `parallelism` | （仅配置文件） |

---

## 使用示例

### 示例 1: 开发环境（嵌入式 + 默认配置）

```bash
# nexora.toml
[risingwave]
enabled = true
embedded = true

# 启动
cargo run --release --features embedded

# 使用 PostgreSQL 客户端连接
psql -h 127.0.0.1 -p 4566 -d dev -U root
```

### 示例 2: 生产环境（外部服务 + 自定义地址）

```toml
# production.toml
[risingwave]
enabled = true
embedded = false
meta_addr = "rw-meta.internal:5690"
frontend_addr = "rw-frontend.internal:4566"
```

```bash
# 启动
./nexora-app --config production.toml

# 健康检查
curl http://localhost:8080/api/health/risingwave
```

### 示例 3: 混合配置（配置文件 + CLI 覆盖）

```toml
# nexora.toml
[risingwave]
enabled = true
embedded = true
meta_addr = "127.0.0.1:5690"
```

```bash
# CLI 覆盖 meta_addr
./nexora-app --risingwave-meta-addr 127.0.0.1:6000

# 实际使用: 127.0.0.1:6000
```

### 示例 4: 调试模式（详细日志）

```bash
RUST_LOG=nexora_risingwave=debug,nexora_app=info \
  ./nexora-app --enable-risingwave --enable-embedded-risingwave
```

**日志输出**:
```
INFO nexora_app: 🚀 Starting DeepStreaming...
INFO nexora_app: RisingWave: starting embedded process...
DEBUG nexora_risingwave: Searching for RisingWave binary...
DEBUG nexora_risingwave: Found: /usr/local/bin/risingwave
DEBUG nexora_risingwave: Starting process with PID 12345
INFO nexora_app: RisingWave: embedded process started (PID: 12345)
```

---

## 故障排查

### 问题 1: 启动失败 "Binary not found"

**症状**:
```
ERROR Failed to start embedded RisingWave: Binary not found
```

**原因**: 找不到 RisingWave 二进制文件

**解决方案**:

1. **手动指定路径**（推荐）:
   ```toml
   [risingwave]
   binary_path = "/usr/local/bin/risingwave"
   ```

2. **设置环境变量**:
   ```bash
   export RISINGWAVE_BIN=/path/to/risingwave
   ./nexora-app
   ```

3. **安装到 PATH**:
   ```bash
   # 下载 RisingWave
   wget https://github.com/risingwavelabs/risingwave/releases/download/v3.0.2/risingwave-v3.0.2-x86_64-unknown-linux.tar.gz
   tar xzf risingwave-*.tar.gz
   sudo mv risingwave /usr/local/bin/
   chmod +x /usr/local/bin/risingwave
   ```

### 问题 2: 启动超时

**症状**:
```
ERROR Embedded RisingWave initialization failed: Startup timeout after 60 seconds
```

**原因**: 
- 系统资源不足
- 端口被占用
- 数据目录权限问题

**解决方案**:

1. **增加超时**:
   ```toml
   [risingwave]
   startup_timeout_secs = 120
   ```

2. **检查端口占用**:
   ```bash
   lsof -i :5690  # Meta 端口
   lsof -i :4566  # Frontend 端口
   ```

3. **检查资源**:
   ```bash
   free -h  # 内存（需要 >2GB 可用）
   df -h    # 磁盘空间
   ```

4. **查看日志**:
   ```bash
   # RisingWave 日志
   tail -f ./nexora-data/risingwave/log/risingwave.log
   ```

### 问题 3: 连接外部 RisingWave 失败

**症状**:
```
ERROR RisingWave initialization failed: Connection refused (os error 111)
```

**原因**: 外部 RisingWave 服务未运行或网络不通

**解决方案**:

1. **验证服务可达**:
   ```bash
   telnet rw-meta.internal 5690
   telnet rw-frontend.internal 4566
   ```

2. **使用 PostgreSQL 客户端测试**:
   ```bash
   psql -h rw-frontend.internal -p 4566 -d dev -U root
   ```

3. **检查防火墙**:
   ```bash
   sudo iptables -L -n | grep -E '5690|4566'
   ```

### 问题 4: 嵌入式进程崩溃

**症状**:
```
WARN Embedded RisingWave process exited unexpectedly
```

**原因**: OOM、段错误、或 RisingWave bug

**解决方案**:

1. **检查退出码**:
   ```bash
   # 查看 Nexora 日志
   grep "RisingWave process exited" /var/log/nexora.log
   ```

2. **查看 RisingWave 日志**:
   ```bash
   tail -100 ./nexora-data/risingwave/log/risingwave.log
   ```

3. **降低并行度**（减少内存占用）:
   ```toml
   [risingwave]
   parallelism = 2
   ```

4. **增加系统资源**:
   ```bash
   # 增加内存限制（Docker）
   docker run --memory=4g ...
   ```

### 问题 5: 配置文件不生效

**症状**: 配置文件中的设置没有被应用

**原因**: 
- CLI 参数覆盖了配置文件
- 配置文件路径错误
- 语法错误

**解决方案**:

1. **验证配置文件加载**:
   ```bash
   # 启动日志应显示
   INFO Loaded configuration from: nexora.toml
   ```

2. **检查语法**:
   ```bash
   # 使用 TOML 验证工具
   cat nexora.toml | toml-lint
   ```

3. **显式指定路径**:
   ```bash
   ./nexora-app --config ./nexora.toml
   ```

4. **移除 CLI 参数**（避免覆盖）:
   ```bash
   # ❌ 错误：CLI 会覆盖配置文件
   ./nexora-app --risingwave-meta-addr 127.0.0.1:6000

   # ✅ 正确：仅依赖配置文件
   ./nexora-app
   ```

---

## 性能调优

### 调优目标

| 场景 | 优化目标 | 推荐配置 |
|------|----------|----------|
| 低延迟 | <10ms 查询延迟 | `parallelism = CPU核心数` |
| 高吞吐 | >100k events/sec | `parallelism = 2x CPU核心数` |
| 低内存 | <2GB 总内存 | `parallelism = 2` |

### 调优参数

#### 1. Compute 并行度

**影响**: 查询并行度和内存占用

```toml
[risingwave]
# 默认: CPU 核心数
# 低内存: 2-4
# 高性能: 1.5x-2x CPU 核心数
parallelism = 8
```

**经验值**:
- 4 核 CPU: `parallelism = 4-6`
- 8 核 CPU: `parallelism = 8-12`
- 16 核 CPU: `parallelism = 16-24`

#### 2. 启动/关闭超时

**影响**: 容器编排稳定性

```toml
[risingwave]
# 生产环境: 增加超时（容忍慢启动）
startup_timeout_secs = 120
shutdown_timeout_secs = 60
```

#### 3. 数据目录位置

**影响**: I/O 性能

```toml
[risingwave]
# ❌ 机械硬盘
data_dir = "/mnt/hdd/risingwave"

# ✅ SSD（推荐）
data_dir = "/mnt/nvme/risingwave"

# ✅ 内存盘（临时测试）
data_dir = "/dev/shm/risingwave"
```

### 监控指标

#### 关键指标

```bash
# 1. RisingWave 进程资源
ps aux | grep risingwave
top -p $(pgrep risingwave)

# 2. 端口连接数
netstat -an | grep -E '5690|4566' | wc -l

# 3. 数据目录大小
du -sh ./nexora-data/risingwave

# 4. Nexora 健康检查
curl http://localhost:8080/api/health/risingwave
```

#### Prometheus 指标（计划中）

```promql
# RisingWave 进程状态
risingwave_embedded_process_up

# 内存使用
risingwave_embedded_memory_bytes

# 启动时间
risingwave_embedded_startup_seconds
```

---

## 最佳实践

### 开发环境

```toml
# nexora.dev.toml
[risingwave]
enabled = true
embedded = true
meta_addr = "127.0.0.1:5690"
frontend_addr = "127.0.0.1:4566"
data_dir = "./dev-data/risingwave"
parallelism = 2  # 低资源占用
```

**启动**:
```bash
cargo run --features embedded -- --config nexora.dev.toml
```

### 生产环境

```toml
# nexora.prod.toml
[risingwave]
enabled = true
embedded = false  # 外部集群
meta_addr = "rw-meta-lb.prod.internal:5690"
frontend_addr = "rw-frontend-lb.prod.internal:4566"
```

**部署清单**:
1. ✅ RisingWave 集群独立部署（3+ 节点 HA）
2. ✅ Nexora 仅编译 `risingwave` 特性（无 `embedded`）
3. ✅ 健康检查配置到负载均衡器
4. ✅ 监控 RisingWave 和 Nexora 连接状态

### Docker 部署

```dockerfile
# Dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release --features embedded

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates
COPY --from=builder /app/target/release/nexora-app /usr/local/bin/
COPY nexora.toml /etc/nexora/nexora.toml
EXPOSE 8080 5690 4566
CMD ["nexora-app", "--config", "/etc/nexora/nexora.toml"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  nexora:
    build: .
    ports:
      - "8080:8080"
      - "5690:5690"
      - "4566:4566"
    volumes:
      - ./nexora-data:/data
      - ./nexora.toml:/etc/nexora/nexora.toml:ro
    environment:
      RUST_LOG: info
    mem_limit: 4g
    cpus: 4
```

### Kubernetes 部署

```yaml
# nexora-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nexora
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nexora
  template:
    metadata:
      labels:
        app: nexora
    spec:
      containers:
      - name: nexora
        image: nexora:latest
        ports:
        - containerPort: 8080
          name: http
        - containerPort: 5690
          name: rw-meta
        - containerPort: 4566
          name: rw-frontend
        resources:
          requests:
            memory: "2Gi"
            cpu: "2"
          limits:
            memory: "4Gi"
            cpu: "4"
        volumeMounts:
        - name: config
          mountPath: /etc/nexora
          readOnly: true
        - name: data
          mountPath: /data
        livenessProbe:
          httpGet:
            path: /api/health
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /api/health/risingwave
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 5
      volumes:
      - name: config
        configMap:
          name: nexora-config
      - name: data
        persistentVolumeClaim:
          claimName: nexora-data
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: nexora-config
data:
  nexora.toml: |
    [risingwave]
    enabled = true
    embedded = true
    parallelism = 4
```

### 安全加固

1. **限制网络暴露**:
   ```toml
   # 仅监听本地
   meta_addr = "127.0.0.1:5690"
   frontend_addr = "127.0.0.1:4566"
   ```

2. **文件权限**:
   ```bash
   chmod 600 nexora.toml  # 配置文件
   chmod 700 ./nexora-data/risingwave  # 数据目录
   ```

3. **容器安全**:
   ```dockerfile
   # 非 root 用户运行
   RUN useradd -m -u 1000 nexora
   USER nexora
   ```

---

## 参考资源

### 官方文档

- **RisingWave**: https://docs.risingwave.com
- **Nexora**: https://github.com/frank-dkvan/nexora2

### 相关文档

- [RisingWave Phase 7 实现计划](RISINGWAVE_PHASE7_PLAN.md)
- [Phase 7.1 完成报告](RISINGWAVE_PHASE7.1_DONE.md)
- [Phase 7.3 完成报告](RISINGWAVE_PHASE7.3_DONE.md)
- [Phase 7.5 完成报告](RISINGWAVE_PHASE7.5_DONE.md)

### 社区

- GitHub Issues: https://github.com/frank-dkvan/nexora2/issues
- RisingWave Slack: https://risingwave-community.slack.com

---

**文档维护**: frank  
**最后更新**: 2026-07-26  
**版本**: 1.0
