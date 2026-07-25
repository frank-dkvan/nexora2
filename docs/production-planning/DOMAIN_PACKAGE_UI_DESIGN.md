# Nexora Domain Package 管理 UI 方案

**版本:** 1.0
**日期:** 2026/07/06
**状态:** 设计中

---

## 总览

Domain Package 管理通过 **Nexora Web Dashboard** 进行，用户无需直接编辑服务器上的 YAML 文件。

---

## 架构

```
Web Dashboard (React SPA)
       │
       ▼
REST API (/api/v2/domains/*)
       │
       ▼
DomainManager (nexora-core)
  ├── DomainLoader      — 加载/验证 schema
  ├── DomainRegistry    — 注册/查询 domain
  └── GraphService      — labels/edge_types 应用
```

---

## Dashboard UI 页面

### 1. Domains 总览页

| 功能 | 描述 |
|------|------|
| 列出所有已加载 domains | 名称、版本、状态、labels 数量、edge types 数量 |
| 启用/禁用 domain | 开关按钮 |
| 新建 domain | 从模板创建或导入 YAML |
| 删除 domain | 卸载并清理 |

### 2. Schema 编辑器

| 功能 | 描述 |
|------|------|
| YAML 编辑器 | 语法高亮、自动补全 |
| 实时校验 | 语法错误即时显示 |
| Labels 管理 | 可视化添加/编辑/删除 labels 和 properties |
| Edge Types 管理 | 可视化添加/编辑/删除 edge types |
| 版本控制 | 查看历史版本、回滚 |

### 3. Mappings 配置器

| 功能 | 描述 |
|------|------|
| 事件类型映射 | 选择事件 → 映射到 node/edge |
| 属性映射 | JSON Path 选择器 |
| 测试映射 | 输入样例事件，查看映射结果 |

### 4. Standing Query 管理

| 功能 | 描述 |
|------|------|
| SQ 列表 | 名称、模式、状态、命中数 |
| SQ 编辑器 | Cypher 查询编辑、语法高亮 |
| SQ 测试 | 输入测试数据，查看匹配结果 |
| 命中历史 | 时间线图表、详细 explain |

### 5. Materialized View 管理

| 功能 | 描述 |
|------|------|
| MV 列表 | 名称、源查询、行数、刷新状态 |
| MV 编辑器 | 创建/编辑 MV |
| 手动刷新 | 触发全量/增量刷新 |
| 数据预览 | 查看 MV 当前数据 |

---

## REST API 设计

```
GET    /api/v2/domains                       # 获取已加载 domains 列表
POST   /api/v2/domains                       # 加载新的 domain（multipart YAML 上传）
GET    /api/v2/domains/:name                 # 获取 domain 详情
PUT    /api/v2/domains/:name                 # 更新 domain
DELETE /api/v2/domains/:name                 # 卸载 domain
POST   /api/v2/domains/:name/validate        # 验证 schema
GET    /api/v2/domains/:name/mappings        # 获取 mappings
PUT    /api/v2/domains/:name/mappings        # 更新 mappings
POST   /api/v2/domains/:name/mappings/validate # 验证 mapping
```

---

## 实施计划

| 阶段 | 内容 | 工作量 |
|------|------|--------|
| Phase 1 | Domain REST API | 3天 |
| Phase 2 | Domain 管理页面 | 5天 |
| Phase 3 | Schema 编辑器 | 3天 |
| Phase 4 | Mappings 配置器 | 3天 |
| Phase 5 | SQ/MV 管理页面 | 3天 |
| Phase 6 | 集成测试 + 文档 | 2天 |
| **总计** | | **19天 (约 4 周)** |

---

## 优先级建议

1. **P1 — Domain REST API**（后端核心）
2. **P1 — Domain 管理页面**（基础 UI）
3. **P2 — Schema 编辑器 + Mappings 配置器**（高级 UI）
4. **P2 — SQ/MV 管理页面**（高级 UI）

---

**维护者:** Nexora Team
**最后更新:** 2026/07/06
