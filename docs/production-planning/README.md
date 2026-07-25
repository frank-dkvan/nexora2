# Nexora 生产级开发规划文档

**版本:** 1.0  
**生成时间:** 2026/07/05  
**维护者:** Nexora Team  
**目标:** 将 Nexora 从架构雏形完善为生产级实时对象图引擎

---

## 📚 文档索引

### 🎯 核心文档（必读）

| 文档 | 用途 | 受众 | 优先级 |
|------|------|------|--------|
| [**COMPLETION_REPORT.md**](COMPLETION_REPORT.md) | 项目现状总结与完成报告 | 所有人 | ⭐⭐⭐⭐⭐ |
| [**PRODUCTION_GAP_TODO.md**](PRODUCTION_GAP_TODO.md) | 生产级差距清单与任务分解（P0/P1/P2） | 开发团队、PM | ⭐⭐⭐⭐⭐ |

### 🏗️ 架构设计文档

| 文档 | 用途 | 受众 | 优先级 |
|------|------|------|--------|
| [**ARCHITECTURE_PRODUCTION.md**](ARCHITECTURE_PRODUCTION.md) | 生产级系统架构设计 | 架构师、Tech Lead | ⭐⭐⭐⭐⭐ |
| [**GRAPH_MODEL.md**](GRAPH_MODEL.md) | 图数据模型详细设计（P0.1 核心） | 核心开发组 | ⭐⭐⭐⭐⭐ |
| [**STANDING_QUERY_ENGINE.md**](STANDING_QUERY_ENGINE.md) | Standing Query 引擎设计与实现 | 核心开发组、业务开发 | ⭐⭐⭐⭐ |
| [**MATERIALIZED_VIEW.md**](MATERIALIZED_VIEW.md) | Materialized View 设计与实现 | 核心开发组 | ⭐⭐⭐⭐ |

### 🔌 集成与扩展文档

| 文档 | 用途 | 受众 | 优先级 |
|------|------|------|--------|
| [**DOMAIN_PACKAGES.md**](DOMAIN_PACKAGES.md) | 领域模型扩展机制 | 业务开发、架构师 | ⭐⭐⭐⭐ |
| [**DOMAIN_PACKAGE_DESIGN.md**](DOMAIN_PACKAGE_DESIGN.md) | Domain Package 详细设计方案 | 业务开发 | ⭐⭐⭐⭐ |
| [**EVENT_INGESTION.md**](EVENT_INGESTION.md) | 事件摄取系统设计 | 集成开发、DevOps | ⭐⭐⭐ |
| [**EVIDENCE_REF.md**](EVIDENCE_REF.md) | 证据引用系统设计 | 业务开发、架构师 | ⭐⭐⭐ |

### 🔒 运维与安全文档

| 文档 | 用途 | 受众 | 优先级 |
|------|------|------|--------|
| [**OBSERVABILITY.md**](OBSERVABILITY.md) | 可观测性架构（Metrics/Logs/Traces） | DevOps、SRE | ⭐⭐⭐⭐ |
| [**SECURITY.md**](SECURITY.md) | 安全架构（认证/授权/审计） | DevOps、安全团队 | ⭐⭐⭐⭐ |

### 🧪 测试与场景文档

| 文档 | 用途 | 受众 | 优先级 |
|------|------|------|--------|
| [**SCENARIO_CATALOG.md**](SCENARIO_CATALOG.md) | 12 个行业场景测试目录 | 测试团队、业务开发 | ⭐⭐⭐⭐ |

---

## 🤖 Code Agent 协同开发指南

### 为什么需要这些文档？

在多个 Code Agent 协同开发时，这些文档提供：
1. **统一的上下文** — 所有 Agent 理解当前项目状态
2. **明确的任务边界** — 避免重复工作或冲突
3. **验收标准** — 确保交付质量一致
4. **架构约束** — 保证代码符合整体设计

---

## 📋 Code Agent 使用流程

### 1️⃣ 新 Agent 启动时（上下文建立）

**第一步：阅读核心文档**
```
Agent: 我需要了解 Nexora 项目的当前状态。

指令模板：
请阅读以下文档并总结项目现状：
- docs/production-planning/COMPLETION_REPORT.md
- docs/production-planning/PRODUCTION_GAP_TODO.md

告诉我：
1. 当前生产就绪度是多少？
2. 最高优先级的任务是什么？
3. 我应该从哪个任务开始？
```

**第二步：选择任务领域**
```
Agent: 我将负责 P0.1 图模型重构任务。

指令模板：
我将负责 [任务ID]，请帮我：
1. 阅读 docs/production-planning/GRAPH_MODEL.md
2. 识别需要修改的文件
3. 生成实施计划
```

---

### 2️⃣ 开发过程中（任务执行）

**指令模板（P0.1 示例）：**
```
任务：实施 P0.1 阶段 1 - 扩展核心数据结构

上下文文档：
- docs/production-planning/GRAPH_MODEL.md 第 2-4 节
- docs/production-planning/PRODUCTION_GAP_TODO.md P0.1 部分

要求：
1. 在 crates/nexora-core/src/graph/node_task.rs 中扩展 NodeTask 结构
2. 在 crates/nexora-core/src/event.rs 中扩展 GraphMutation 枚举
3. 添加 LabelAdded/LabelRemoved/EdgePropertySet/NodeDeleted 事件
4. 保持向后兼容
5. 每次修改后运行：cargo test --workspace
6. 输出修改摘要

参考设计：
见 GRAPH_MODEL.md 第 2.1 节 NodeRecord 结构
见 GRAPH_MODEL.md 第 4.1 节 GraphMutation 枚举
```

---

### 3️⃣ 协同开发（多 Agent 场景）

**场景 1：并行开发不同任务**

```
Agent A 负责：P0.1 图模型重构
Agent B 负责：P0.3 Materialized View 填充

协同方式：
1. Agent A 先完成 P0.1（因为 P0.3 依赖新的 GraphMutation）
2. Agent B 在 P0.1 完成前，可以先阅读文档和编写测试
3. 通过 PRODUCTION_GAP_TODO.md 的"依赖关系"章节协调

依赖检查指令：
请检查 docs/production-planning/PRODUCTION_GAP_TODO.md，
确认 P0.3 是否依赖 P0.1 完成。如果依赖，我应该等待还是可以并行工作？
```

**场景 2：接力开发同一任务**

```
Agent A 完成了 P0.1 阶段 1-2
Agent B 接手 P0.1 阶段 3-4

交接指令模板：
我将接手 P0.1 阶段 3（迁移 EdgeProperty 存储）。

请帮我：
1. 检查 P0.1 阶段 1-2 的完成状态（运行测试）
2. 阅读 docs/production-planning/GRAPH_MODEL.md 第 3.2 节（EdgeProperty）
3. 找到 Agent A 留下的兼容层代码
4. 继续实施阶段 3

验证前序工作：
- NodeTask.labels 字段是否已添加？
- LabelAdded/LabelRemoved 事件是否已实现？
- 测试是否通过？
```

**场景 3：跨文档引用**

```
Agent C 负责：实施 air_cargo_terminal Domain Package

需要参考的文档：
1. docs/production-planning/DOMAIN_PACKAGE_DESIGN.md（设计方案）
2. docs/production-planning/SCENARIO_CATALOG.md 第 3 节（航空货站场景）
3. docs/production-planning/GRAPH_MODEL.md 第 7.2 节（航空货站图模式）

指令模板：
请根据以下文档实施 air_cargo_terminal Domain Package：

架构设计：docs/production-planning/DOMAIN_PACKAGE_DESIGN.md 第 2.2 节
场景需求：docs/production-planning/SCENARIO_CATALOG.md 第 3 节

创建以下文件：
- domains/air_cargo_terminal/schema.yaml
- domains/air_cargo_terminal/mappings/agv_status.yaml
- domains/air_cargo_terminal/standing_queries/agv_flight_impact.cypher
- domains/air_cargo_terminal/tests/scenario_agv_flight_test.rs

确保遵循 DOMAIN_PACKAGE_DESIGN.md 第 3 节的 Schema 格式。
```

---

### 4️⃣ 任务完成后（验收与文档更新）

**验收指令模板：**
```
我已完成 P0.1 阶段 1，请帮我验收：

验收标准（来源：docs/production-planning/PRODUCTION_GAP_TODO.md P0.1）：
- [ ] NodeRecord 结构已扩展
- [ ] GraphMutation 枚举已扩展
- [ ] cargo test --workspace --all-targets 通过
- [ ] cargo clippy 无警告
- [ ] 添加了单元测试

请检查上述标准，并生成验收报告。
```

**文档更新指令：**
```
P0.1 阶段 1 已完成，请更新文档：

1. 更新 docs/production-planning/PRODUCTION_GAP_TODO.md：
   - 将 P0.1 阶段 1 状态改为 ✅ 已完成
   
2. 更新 docs/production-planning/GRAPH_MODEL.md：
   - 在第 2.1 节添加实现状态标记
   - 更新实现进度百分比

3. 生成 Git commit message：
   feat(core): implement P0.1 phase 1 - extend core data structures
```

---

## 🔍 常见场景速查

### Scenario 1: 新 Agent 不知道从哪开始

**指令：**
```
我是新加入的 Code Agent，请根据 docs/production-planning/ 文档：
1. 告诉我项目当前状态
2. 推荐我应该负责哪个任务（基于优先级和我的能力）
3. 生成该任务的实施计划
```

---

### Scenario 2: Agent 遇到设计冲突

**指令：**
```
我在实施 P0.1 时发现与 GRAPH_MODEL.md 第 3.2 节设计不一致。

当前代码：
[粘贴代码]

文档要求：
[引用文档]

请帮我：
1. 分析冲突原因
2. 确认应该遵循哪个设计
3. 如果需要修改文档，生成更新建议
```

---

### Scenario 3: Agent 需要跨模块协调

**指令：**
```
我正在实施 P1.1 Standing Query 触发器，需要修改：
- nexora-cypher/src/write_executor.rs（调用触发器）
- nexora-standing-query/src/lib.rs（实现触发器）

请根据 docs/production-planning/STANDING_QUERY_ENGINE.md 第 4 节：
1. 确认模块边界
2. 设计接口调用方式
3. 确保不破坏现有功能
```

---

### Scenario 4: Agent 需要编写测试

**指令：**
```
我需要为 P0.1 编写场景测试。

参考文档：
- docs/production-planning/SCENARIO_CATALOG.md 第 2 节（通用场景）
- docs/production-planning/GRAPH_MODEL.md 第 7.1 节（示例数据）

请生成：
- tests/scenarios/generic_impact_test.rs
- 包含完整的测试数据加载、Standing Query 注册、验证逻辑
```

---

## 📊 任务依赖图（供 Agent 参考）

```
P0.1 图模型重构 ────┬──→ P0.2 移除快照限制
  (7-10天)          │      (5-7天)
                    │
                    ├──→ P0.3 MV 填充逻辑
                    │      (3-4天)
                    │
                    └──→ P1.1 Standing Query 触发器 ──→ P1.2 持久化 SQ 状态
                           (4-5天)                       (3天)
                                                           │
                                                           ↓
                                                        P1.3 Fixpoint 集成
                                                           (2-3天)
                                                           
P2.1 Domain Package 机制 ←─── 可并行开发 ────┐
  (5-7天)                                     │
                                              │
P2.2 MV 增量刷新 ←─── 依赖 P0.3 ─────────────┘
  (4天)
```

**Agent 协调原则：**
1. **P0.1 优先级最高** — 其他任务依赖它
2. **P0.2/P0.3 可并行** — 但 P0.3 最好在 P0.1 后开始
3. **P2.1 可独立开发** — 不阻塞主线
4. **避免同时修改同一文件** — 通过 Git 分支管理

---

## 🛠️ 实用指令模板库

### 模板 1：快速上下文建立
```
我是新启动的 Code Agent，请执行以下步骤：

1. 读取 docs/production-planning/COMPLETION_REPORT.md
2. 总结项目当前生产就绪度
3. 列出 P0 任务清单
4. 推荐我应该负责的任务（基于当前进度）
```

### 模板 2：任务实施标准流程
```
任务：[任务ID]

步骤：
1. 阅读相关文档：docs/production-planning/[相关文档].md
2. 识别需要修改的文件
3. 生成实施计划（分阶段）
4. 逐步实施并运行测试
5. 生成验收报告
6. 更新文档状态

开始实施。
```

### 模板 3：代码审查请求
```
我已完成 [任务ID]，请审查代码：

参考标准：docs/production-planning/[相关文档].md

检查项：
1. 是否符合架构设计？
2. 是否遵循编码规范？
3. 测试是否充分？
4. 是否有遗漏的边界情况？

生成审查报告。
```

### 模板 4：协同冲突解决
```
检测到与 [其他Agent/任务] 的潜在冲突：

冲突点：[文件路径]
我的任务：[任务ID]
冲突任务：[任务ID]

请根据 docs/production-planning/PRODUCTION_GAP_TODO.md 的依赖关系：
1. 确定优先级
2. 建议解决方案（等待/协调/拆分）
3. 生成协调计划
```

---

## 📝 文档维护规范

### 何时更新文档？

| 触发条件 | 需要更新的文档 | 负责人 |
|----------|----------------|--------|
| P0 任务完成 | PRODUCTION_GAP_TODO.md + 对应设计文档 | 实施 Agent |
| 架构变更 | ARCHITECTURE_PRODUCTION.md | 架构师 / Tech Lead |
| 新增 Domain | DOMAIN_PACKAGES.md + SCENARIO_CATALOG.md | 业务开发 Agent |
| 新增场景测试 | SCENARIO_CATALOG.md | 测试 Agent |
| 性能优化 | ARCHITECTURE_PRODUCTION.md 第 10 节 | 核心开发 Agent |

### 文档更新模板

```markdown
## 更新记录

**日期:** 2026/07/XX  
**更新者:** Agent Name / Human Developer  
**变更类型:** [实现状态更新 / 架构变更 / 新增场景]

**变更内容:**
- [x] 任务 P0.1 阶段 1 完成
- [x] 更新 NodeRecord 结构说明
- [x] 添加实现状态标记

**影响范围:**
- GRAPH_MODEL.md 第 2.1 节
- PRODUCTION_GAP_TODO.md P0.1 状态
```

---

## 🎓 最佳实践

### ✅ DO（推荐做法）

1. **每次开始任务前先读文档** — 避免重复工作
2. **任务完成后更新文档** — 保持文档同步
3. **遇到冲突时引用文档** — 文档是权威来源
4. **跨模块开发时检查依赖** — 参考任务依赖图
5. **每个 commit 引用任务 ID** — 例如 `feat(P0.1): ...`

### ❌ DON'T（避免做法）

1. **不要凭记忆实施** — 可能与文档设计不一致
2. **不要跳过测试** — 每个任务都有验收标准
3. **不要独自修改架构** — 必须先更新文档并评审
4. **不要忽略依赖关系** — 可能阻塞其他 Agent
5. **不要创建冗余文档** — 使用现有文档体系

---

## 🔗 快速链接

- **项目根目录:** `/Users/frank/aiCoding/nexora`
- **文档目录:** `/Users/frank/aiCoding/nexora/docs/production-planning`
- **核心代码:** `/Users/frank/aiCoding/nexora/crates`
- **测试目录:** `/Users/frank/aiCoding/nexora/crates/*/tests`

---

## 📞 获取帮助

### 文档相关问题

**Q: 我不知道该读哪个文档？**  
A: 从 [COMPLETION_REPORT.md](COMPLETION_REPORT.md) 开始，它会引导你到相关文档。

**Q: 文档与代码不一致怎么办？**  
A: 优先以文档为准（文档是设计目标），但需要报告差异并更新文档。

**Q: 我发现文档有错误？**  
A: 立即报告并提交文档更新 PR，注明错误原因和修正依据。

### 任务协调问题

**Q: 我的任务被其他 Agent 阻塞了？**  
A: 检查 PRODUCTION_GAP_TODO.md 的依赖关系图，决定是等待还是先做其他任务。

**Q: 多个 Agent 修改同一文件？**  
A: 通过 Git 分支隔离，完成后 merge 并解决冲突。优先级高的任务先 merge。

---

## 🎯 成功标准

使用这些文档的项目被认为成功，如果：

- ✅ 所有 Code Agent 在开始任务前都阅读了相关文档
- ✅ 任务实施符合文档设计（代码审查通过率 > 90%）
- ✅ 文档与代码保持同步（差异 < 5%）
- ✅ 跨 Agent 协作无冲突（合并冲突 < 10%）
- ✅ 验收标准清晰且可执行（测试通过率 > 95%）

---

**文档最后更新:** 2026/07/05  
**文档维护者:** Nexora Team  
**文档版本:** 1.0

---

## 📄 许可证

本文档库遵循项目主 LICENSE。
