# Nexora 2 新Session启动指南

## 为什么需要创建新Session？

**强烈建议创建独立session**，原因：

1. **工作目录完全不同**
   - 当前session：`/Users/frank/aiCoding/risingwave-3.0.2`（研究源码）
   - 新任务：`/Users/frank/aiCoding/nexora2`（全新开发）

2. **任务性质完全不同**
   - 当前：探索性研究、架构分析
   - 新任务：代码实现、构建测试、持续开发

3. **上下文干净**
   - 避免混淆RisingWave源码分析和Nexora开发
   - 新session可以专注于实施计划

4. **可以设置专属指令**
   - 在nexora2目录下创建`CLAUDE.md`
   - 定义项目特定的开发规范

---

## 准备步骤

### 1. 创建nexora2目录结构

```bash
# 创建主目录
mkdir -p /Users/frank/aiCoding/nexora2

# 创建docs目录
mkdir -p /Users/frank/aiCoding/nexora2/docs

# 复制设计文档
cp /Users/frank/Downloads/Nexora_2_RnD_Guidance_v1.1.md \
   /Users/frank/aiCoding/nexora2/docs/

# 复制实施计划
cp /Users/frank/.claude/plans/ethereal-tumbling-peacock.md \
   /Users/frank/aiCoding/nexora2/docs/IMPLEMENTATION_PLAN.md

# 复制Git Subtree升级指南
cp /tmp/git-subtree-upgrade-guide.md \
   /Users/frank/aiCoding/nexora2/docs/
```

### 2. 创建CLAUDE.md项目指令文件

将以下内容保存为 `/Users/frank/aiCoding/nexora2/CLAUDE.md`：

```markdown
# Nexora 2 开发指令

## 项目简介

Nexora 2 - Real-time Event-Driven Temporal Graph Intelligence Platform

将企业事件流实时转化为动态时态知识图谱，为AI Agent提供实时认知和推理能力。

## 核心架构

### 模块定位
- **RisingWave**：Event Stream Module（内嵌模块，非外部依赖）
- **Semantic Event Engine**：业务语义转换（CloudEvents → BusinessEvent）
- **Graph Mutation Engine**：图变化执行（BusinessEvent → GraphMutation）
- **Temporal Graph Engine**：时态图存储（Bi-temporal模型）
- **Graph Cluster Manager**：分布式图引擎（使用嵌入式Raft）

### 仓库结构
```
nexora2/
├── vendor/risingwave/          # Git Subtree: RisingWave v3.0.2
├── crates/                     # 共享基础库
│   ├── consensus/              # Raft抽象（RisingWave和Nexora共用）
│   ├── rpc/                   # gRPC通信
│   ├── storage/               # 存储抽象
│   └── protocol/              # PostgreSQL Wire Protocol
├── extensions/meta_raft/      # RisingWave Raft HA扩展
├── src/                       # Nexora核心模块
│   ├── event_stream/          # 封装RisingWave
│   ├── semantic_event/        # 语义事件引擎
│   ├── graph_mutation/        # 图变化引擎
│   ├── graph_engine/          # 分布式图引擎
│   └── temporal_graph/        # 时态图引擎
├── patches/                   # RisingWave最小化补丁
├── scripts/                   # 自动化工具
├── docker/                    # Docker配置
└── docs/                      # 文档
```

## 关键设计决策

### 1. 依赖管理策略
- **RisingWave**：Git Subtree（唯一需要这种方式，深度集成>1000行）
- **openraft**：Cargo标准依赖（实现ConsensusClient trait）
- **kuzu/nebula**：Cargo标准依赖（调用图数据库API）
- **tokio-postgres**：Cargo标准依赖（数据库客户端）
- **cloudevents-sdk**：Cargo标准依赖（事件标准）

### 2. 代码复用模式
```rust
// crates/consensus/src/lib.rs - 统一抽象
#[async_trait::async_trait]
pub trait ConsensusClient: Send + Sync + 'static {
    async fn init(&self, peers: Vec<String>) -> Result<()>;
    fn is_leader(&self) -> bool;
    async fn commit(&self, data: Bytes) -> Result<LogIndex>;
    async fn members(&self) -> Result<Vec<Member>>;
}

// extensions/meta_raft - RisingWave使用
impl ElectionClient for RisingWaveRaftElectionClient {
    fn is_leader(&self) -> bool {
        self.consensus.is_leader()  // 委托给共享实现
    }
}

// src/graph_engine - Nexora使用（同样的Raft！）
pub struct GraphClusterManager {
    consensus: Arc<dyn ConsensusClient>,  // 复用相同抽象
}
```

### 3. RisingWave升级机制
```bash
# 升级到新版本（Git自动三方合并）
git subtree pull --prefix=vendor/risingwave risingwave-upstream v3.1.0 --squash

# 重新应用补丁
./scripts/apply-patches.sh

# 测试构建
cargo check --workspace
```

## 开发规范

### Rust代码风格
- 使用`cargo fmt`格式化代码
- 使用`cargo clippy`进行静态检查
- 所有注释和文档使用英文
- 错误处理优先使用`anyhow`（应用层）或`thiserror`（库层）
- 异步代码使用`tokio`运行时

### Git Commit规范
```
feat: 新功能
fix: 修复bug
refactor: 重构代码
docs: 文档更新
test: 测试相关
chore: 构建工具或辅助工具
perf: 性能优化
```

### 统一构建命令
```bash
make build          # 完整构建
make test           # 运行所有测试
make check          # 代码风格和clippy检查
make sync           # 同步RisingWave上游
make docker         # 构建Docker镜像
make clean          # 清理构建产物
```

## 技术栈

### 核心依赖
- **Rust**: 1.75+
- **RisingWave**: v3.0.2（通过Git Subtree集成）
- **openraft**: 0.9（Raft共识协议）
- **tokio**: 1.35（异步运行时）
- **tonic**: 0.11（gRPC框架）
- **kuzu**: 0.1（图数据库）
- **cloudevents-sdk**: 0.7（事件标准）

### 外部服务（可选）
- **Kafka/Redpanda**: 事件总线
- **MinIO**: 对象存储
- **Prometheus**: 监控指标
- **Grafana**: 可视化面板

## 实施计划

参考文档：`docs/IMPLEMENTATION_PLAN.md`

### 当前阶段：Phase 1（Week 1-2）
**目标**：仓库初始化与基础架构

**交付物**：
1. `scripts/init-repo.sh` - 仓库初始化脚本
2. `Cargo.toml` - Workspace配置
3. `Makefile` - 统一构建工具
4. `.gitignore` - Git忽略规则
5. `README.md` - 项目文档
6. `.github/workflows/ci.yml` - CI配置（可选）

### 后续阶段
- **Phase 2**（Week 3-4）：共享基础库开发
- **Phase 3**（Week 5-6）：RisingWave Raft HA实现
- **Phase 4**（Week 7-12）：Nexora核心引擎开发
- **Phase 5**（Week 13-14）：集成与测试
- **Phase 6**（Week 15）：Docker化与部署

## 关键文件说明

### 初始化阶段需要创建的文件
1. **scripts/init-repo.sh**
   - 自动化仓库初始化
   - 添加RisingWave Subtree
   - 创建目录结构

2. **Cargo.toml**
   - Workspace配置
   - 统一依赖版本管理
   - Feature gates定义

3. **Makefile**
   - `build`: 编译所有crates
   - `test`: 运行测试套件
   - `check`: 代码质量检查
   - `sync`: 同步RisingWave

4. **scripts/sync-risingwave.sh**
   - 检查上游最新版本
   - 执行git subtree pull
   - 自动应用补丁

5. **scripts/apply-patches.sh**
   - 遍历patches目录
   - 应用.patch文件
   - 冲突检测

## 参考资源

### 内部文档
- **设计文档**：`docs/Nexora_2_RnD_Guidance_v1.1.md`
- **实施计划**：`docs/IMPLEMENTATION_PLAN.md`
- **Git Subtree指南**：`docs/git-subtree-upgrade-guide.md`

### 外部资源
- **RisingWave源码**：`/Users/frank/aiCoding/risingwave-3.0.2`
- **RisingWave文档**：https://docs.risingwave.com
- **openraft文档**：https://docs.rs/openraft
- **CloudEvents规范**：https://cloudevents.io

## 开发流程

### 日常开发
1. 拉取最新代码：`git pull`
2. 创建feature分支：`git checkout -b feat/xxx`
3. 开发并测试：`make build && make test`
4. 代码检查：`make check`
5. 提交：`git commit -m "feat: xxx"`
6. 推送：`git push origin feat/xxx`

### RisingWave升级流程
1. 检查新版本：`./scripts/sync-risingwave.sh --check`
2. 升级：`./scripts/sync-risingwave.sh --upgrade v3.1.0`
3. 应用补丁：`./scripts/apply-patches.sh`
4. 测试：`make test`
5. 提交：`git commit -m "chore: upgrade RisingWave to v3.1.0"`

## 测试策略

### 单元测试
```bash
# 测试特定crate
cargo test -p nexora-consensus

# 测试特定模块
cargo test --package nexora-graph-engine --lib cluster
```

### 集成测试
```bash
# 运行所有集成测试
make test-integration

# 3节点集群测试
cargo test --test cluster_test -- --test-threads=1
```

### 端到端测试
```bash
# 启动本地集群
make dev

# 运行E2E测试
cargo test --test e2e_test
```

## 故障排查

### 常见问题

**Q: RisingWave Subtree升级失败**
```bash
# 检查冲突文件
git status

# 手动解决冲突后
git add .
git commit
```

**Q: 补丁应用失败**
```bash
# 检查哪个补丁失败
./scripts/apply-patches.sh --verbose

# 手动应用特定补丁
git apply patches/001-enable-external-election.patch
```

**Q: 构建失败**
```bash
# 清理并重新构建
make clean
make build

# 检查Rust版本
rustc --version  # 需要 >= 1.75
```

## Feature Gates

### raft-ha
启用RisingWave嵌入式Raft HA支持

```toml
[features]
raft-ha = ["nexora-consensus/openraft", "extensions-meta-raft"]
```

### nexora
启用Nexora完整功能

```toml
[features]
nexora = ["raft-ha", "graph-engine", "temporal-graph"]
```

### 使用示例
```bash
# 只构建RisingWave + Raft HA
cargo build --features raft-ha

# 构建完整Nexora平台
cargo build --features nexora
```

## 部署配置

### 开发环境
```bash
# 单节点模式
./target/debug/nexora --config dev.toml
```

### 生产环境
```bash
# 3节点HA集群
docker-compose -f docker/docker-compose-ha.yml up -d
```

### 配置文件示例
```toml
# nexora.toml
[meta]
node_id = 1
peers = ["node1:5690", "node2:5690", "node3:5690"]

[storage]
data_dir = "/data/nexora"
object_store = "s3://nexora-bucket"

[graph]
backend = "kuzu"
data_dir = "/data/graph"

[event_stream]
kafka_brokers = ["kafka:9092"]
```

## 性能指标

### 目标
- 事件吞吐量：>10K events/s
- Raft选举延迟：<5秒
- 图查询P99：<100ms
- 时态查询P99：<200ms

### 监控
```bash
# Prometheus指标
curl http://localhost:9090/metrics
```

---

## 快速开始

新session启动后，直接执行：

```bash
# 开始Phase 1
请帮我创建scripts/init-repo.sh脚本，实现nexora2仓库的自动化初始化。
```
```

---

## 新Session启动提示词

### 选项1：简洁版（推荐）

当CLAUDE.md已创建时使用：

```markdown
开始实施Nexora 2项目开发。

## 当前任务
Phase 1（Week 1-2）：仓库初始化与基础架构

## 背景
项目指令和完整计划请参考：
- CLAUDE.md（项目规范）
- docs/IMPLEMENTATION_PLAN.md（15周实施计划）

## 请帮我
创建scripts/init-repo.sh脚本，实现以下功能：
1. 初始化Git仓库
2. 添加RisingWave v3.0.2作为Git Subtree
3. 创建完整目录结构
4. 生成初始Cargo.toml工作空间配置

技术栈：Rust 1.75+, RisingWave, openraft, Git Subtree
```

---

### 选项2：完整版（首次使用）

如果还未创建CLAUDE.md，使用此版本：

```markdown
我要开始实施Nexora 2项目开发。

## 项目简介
Nexora 2 - Real-time Event-Driven Temporal Graph Intelligence Platform
将企业事件流（Kafka）实时转化为动态时态知识图谱，为AI Agent提供实时认知和推理能力。

## 核心技术架构

### 1. 模块定位
- **RisingWave**：Event Stream Module（内嵌模块，非外部依赖）
- **Semantic Event Engine**：业务语义转换（CloudEvents → BusinessEvent）
- **Graph Mutation Engine**：图变化执行（BusinessEvent → GraphMutation）
- **Temporal Graph Engine**：时态图存储（Bi-temporal模型）
- **Graph Cluster Manager**：分布式图引擎

### 2. 仓库结构
```
nexora2/
├── vendor/risingwave/          # Git Subtree: RisingWave v3.0.2
├── crates/                     # 共享基础库
│   ├── consensus/              # Raft抽象（RisingWave和Nexora共用）
│   ├── rpc/                   # gRPC通信
│   ├── storage/               # 存储抽象
│   └── protocol/              # PostgreSQL Wire Protocol
├── extensions/meta_raft/      # RisingWave Raft HA扩展
├── src/                       # Nexora核心模块
│   ├── event_stream/          # 封装RisingWave
│   ├── semantic_event/        # 语义事件引擎
│   ├── graph_mutation/        # 图变化引擎
│   ├── graph_engine/          # 分布式图引擎
│   └── temporal_graph/        # 时态图引擎
├── patches/                   # RisingWave最小化补丁
└── scripts/                   # 自动化工具
```

### 3. 关键设计决策

**依赖管理策略**：
- RisingWave：Git Subtree（唯一需要这种方式，深度集成）
- openraft/kuzu/tokio-postgres：Cargo标准依赖

**代码复用模式**：
```rust
// crates/consensus定义统一trait
pub trait ConsensusClient: Send + Sync {
    fn is_leader(&self) -> bool;
    async fn commit(&self, data: Bytes) -> Result<LogIndex>;
}

// RisingWave Meta使用
impl ElectionClient for RisingWaveRaftElectionClient {
    fn is_leader(&self) -> bool {
        self.consensus.is_leader()  // 委托给共享实现
    }
}

// Nexora Graph使用（同样的Raft！）
pub struct GraphClusterManager {
    consensus: Arc<dyn ConsensusClient>,
}
```

**RisingWave升级机制**：
```bash
# 一条命令升级
git subtree pull --prefix=vendor/risingwave risingwave-upstream v3.1.0 --squash

# 自动应用补丁
./scripts/apply-patches.sh
```

## 实施计划
完整15周计划详见：docs/IMPLEMENTATION_PLAN.md

## 当前任务：Phase 1（Week 1-2）
**目标**：建立nexora2仓库基础结构，集成RisingWave v3.0.2

**交付物**：
1. scripts/init-repo.sh - 仓库初始化脚本
2. Cargo.toml - Workspace配置
3. Makefile - 统一构建命令
4. README.md - 项目文档

**技术要求**：
- Rust 1.75+
- Git Subtree管理vendor/risingwave
- Cargo Workspace管理所有crates
- Feature gates: raft-ha, nexora

## 参考文档
- Nexora设计文档：docs/Nexora_2_RnD_Guidance_v1.1.md
- RisingWave源码：/Users/frank/aiCoding/risingwave-3.0.2
- Git Subtree升级指南：docs/git-subtree-upgrade-guide.md

## 请帮我
从Phase 1第一步开始：创建scripts/init-repo.sh脚本，实现以下功能：
1. 初始化Git仓库
2. 添加RisingWave v3.0.2作为Git Subtree
3. 创建完整目录结构（crates/extensions/src/patches/scripts/docker/docs）
4. 生成初始Cargo.toml工作空间配置
5. 创建.gitignore文件
```

---

## 推荐工作流程

### Step 1: 准备环境（在当前session）
```bash
# 创建nexora2目录
mkdir -p /Users/frank/aiCoding/nexora2/docs

# 复制设计文档
cp /Users/frank/Downloads/Nexora_2_RnD_Guidance_v1.1.md \
   /Users/frank/aiCoding/nexora2/docs/

# 复制实施计划
cp /Users/frank/.claude/plans/ethereal-tumbling-peacock.md \
   /Users/frank/aiCoding/nexora2/docs/IMPLEMENTATION_PLAN.md

# 复制Git Subtree指南
cp /tmp/git-subtree-upgrade-guide.md \
   /Users/frank/aiCoding/nexora2/docs/
```

### Step 2: 创建CLAUDE.md
将上面"CLAUDE.md项目指令文件"部分的内容保存到：
`/Users/frank/aiCoding/nexora2/CLAUDE.md`

### Step 3: 开启新Session
1. 打开新的Claude Code session
2. 工作目录选择：`/Users/frank/aiCoding/nexora2`
3. 使用**简洁版提示词**启动（因为CLAUDE.md会自动加载）

### Step 4: 开始开发
新session会自动读取CLAUDE.md，然后你只需说：
```
开始Phase 1：创建scripts/init-repo.sh脚本
```

---

## 总结

✅ **推荐方案**：
1. 当前session：准备nexora2目录和CLAUDE.md
2. 新session：工作目录指向nexora2，使用简洁提示词
3. CLAUDE.md自动加载，提供完整项目上下文

✅ **优势**：
- 新session专注nexora2开发
- 避免与risingwave-3.0.2源码分析混淆
- 可持续工作15周
- 需要参考RisingWave源码时，切回当前session查看

📄 **本文件用途**：
- 保存所有启动提示词
- 记录项目配置要求
- 作为新session启动手册
