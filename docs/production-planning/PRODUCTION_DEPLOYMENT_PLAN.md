# Nexora 生产级部署计划

> 目标: 达到生产级应用标准  
> 首个试点: 航空货运站运控系统  
> 制定日期: 2026-07-10

---

## 一、总体策略

### 部署模式选择

**Phase 1 (试点阶段)**: 单机模式
- **适用场景**: 航空货运站运控系统 (单站点)
- **数据规模**: 预计 < 1M 节点 (货物、航班、仓位、人员)
- **并发量**: 中等 (峰值 100-200 QPS)
- **可用性要求**: 高 (但单点可接受,有备份策略)

**Phase 2 (扩展阶段)**: 分布式模式
- **适用场景**: 多货运站联网、跨行业复制
- **数据规模**: > 10M 节点
- **并发量**: 高 (> 500 QPS)
- **可用性要求**: 极高 (99.9% SLA)

---

## 二、修复计划 (4个阶段)

### 🔴 Phase 1: 紧急安全加固 (Week 1-3)

**目标**: 修复所有P0问题,达到单机生产就绪

#### 任务清单

**1.1 API安全加固** (Week 1)
- [ ] **P0-6**: 修改默认认证为启用
  - 修改 `main.rs:94-99`,默认值改为 `true`
  - 添加启动时检查,生产环境强制认证
  - 更新文档和示例配置

- [ ] **P0-7**: 禁止默认密钥
  - 添加启动检查: `auth_secret == "nexora-dev-secret-change-me"` → 拒绝启动
  - 环境变量 `NEXORA_STRICT_SECURITY=true` 强制检查
  - 生成强随机密钥的工具脚本

**1.2 查询引擎安全** (Week 1-2)
- [ ] **P0-3**: WHERE短路求值
  - 修改 `evaluator.rs:239-240`
  - 在 `evaluate_with_depth` 中特殊处理 AND/OR
  - 添加回归测试: `WHERE n.prop IS NOT NULL AND n.prop > 10`

- [ ] **P0-4**: SQL注入LIKE转义
  - 修改 `lib.rs:750-775`,在trim之前转义
  - 添加安全测试: `LIKE '%\\'%'` 等边界情况

- [ ] **P0-5**: 查询资源限制
  - 添加结果集大小限制: 默认 100k 行
  - 添加查询超时: 默认 30 秒
  - DELETE操作添加扫描限制或使用边索引

**1.3 分布式系统修复** (Week 2-3, 可并行)
- [ ] **P0-1**: Quorum写入回滚
  - 实现 two-phase commit
  - Phase 1: 复制到 followers
  - Phase 2: followers 确认后才在 owner 执行
  - 添加集成测试

- [ ] **P0-2**: 复制日志错误处理
  - `persist()` 方法返回 `Result`
  - 失败时降级为 in-memory 或拒绝写入
  - 添加监控指标: 持久化失败率

**验收标准**:
- ✅ 所有 P0 问题修复完成
- ✅ 回归测试通过
- ✅ Clippy 无警告
- ✅ 文档更新完成

---

### 🟠 Phase 2: 稳定性增强 (Week 4-7)

**目标**: 修复 P1 问题,提升系统稳定性

#### 任务清单

**2.1 存储引擎加固** (Week 4-5)
- [ ] **P1-3**: WAL 目录 fsync
  - 创建 WAL 文件后对父目录进行 fsync
  - 确保目录项持久化

- [ ] **P1-4**: Shutdown 最终 flush
  - 在 shutdown 路径中强制 final flush
  - 确保未刷新数据持久化

**2.2 API 输入验证** (Week 5)
- [ ] **P1-5**: 添加输入验证
  - 使用 `validator` crate
  - JSON 大小限制: 1MB
  - 字符串长度限制: 255 字符
  - 添加验证测试

- [ ] **P1-6**: WebSocket Token 安全
  - 优先使用 HTTP Header 传递 token
  - URL token 添加警告日志
  - URL 解码处理

**2.3 查询引擎改进** (Week 6)
- [ ] **P1-2**: NULL 语义澄清
  - 添加 NULL 转换测试
  - WHERE 子句中 `col = NULL` → `col IS NULL` 警告
  - 文档说明 SQL-Cypher NULL 差异

**2.4 分布式系统** (Week 6-7)
- [ ] **P1-1**: 控制平面原子性
  - 在单个 write lock 内完成 failover
  - 避免 health check 和 shard map 更新之间的竞态

**验收标准**:
- ✅ 所有 P1 问题修复完成
- ✅ 集成测试覆盖率 > 60%
- ✅ 性能无明显回归
- ✅ 生产配置检查清单完成

---

### 🟡 Phase 3: 性能优化 (Week 8-11)

**目标**: 修复 P2 问题,优化性能

#### 任务清单

**3.1 存储优化** (Week 8-9)
- [ ] **P2-2**: Range query 优化
  - 使用 `BTreeSet` 避免排序
  - 性能测试验证

- [ ] **P2-3**: 减少 clone 开销
  - `query_all` 使用引用计数
  - 热路径性能分析

**3.2 安全配置** (Week 9-10)
- [ ] **P2-4**: 强制 TLS
  - 生产环境拒绝 HTTP
  - 证书过期监控

- [ ] **P2-5**: CORS 收紧
  - 默认拒绝跨域
  - 需要显式配置

**3.3 代码质量** (Week 10-11)
- [ ] **P2-6**: 减少 unwrap
  - 审查关键路径 (WAL, 复制, 认证)
  - 使用 `Result` 显式错误处理
  - 添加 Clippy 规则

- [ ] **P2-7**: 修复 Clippy 警告
  - `cargo clippy --fix`
  - 手动修复剩余问题

**3.4 分布式优化** (Week 11)
- [ ] **P2-1**: Gossip 内存限制
  - 添加 `MAX_LEARNED_NODES_PER_TICK = 100`

**验收标准**:
- ✅ 性能提升 20% 以上
- ✅ 内存占用降低 15%
- ✅ Clippy 零警告
- ✅ 代码覆盖率 > 70%

---

### 🔵 Phase 4: 测试补齐 (Week 12-15)

**目标**: 达到企业级测试标准

#### 任务清单

**4.1 单元测试补齐** (Week 12-13)
- [ ] nexora-persistor-rocksdb: 至少 30 个测试
  - RocksDB 操作正确性
  - 并发访问测试
  - 错误处理测试

- [ ] nexora-cli: 至少 20 个测试
  - 命令行参数解析
  - 输出格式化
  - 错误处理

**4.2 集成测试** (Week 13-14)
- [ ] 端到端场景测试
  - 完整数据生命周期
  - 查询 → 变更 → 查询验证
  - 崩溃恢复场景

- [ ] 边界条件测试
  - NULL 值处理
  - 空字符串
  - 巨大对象 (> 1MB)

**4.3 性能测试** (Week 14-15)
- [ ] TPC 图基准测试
- [ ] 并发吞吐量测试
- [ ] 性能回归 CI 集成

**验收标准**:
- ✅ 代码覆盖率 > 80%
- ✅ 所有关键路径有测试
- ✅ CI/CD 集成完成

---

## 三、航空货运站试点部署方案

### 应用场景分析

**核心业务流程**:
```
货物入库 → 仓位分配 → 航班配载 → 出库装机 → 状态追踪
     ↓          ↓          ↓          ↓          ↓
   图节点    关联关系    时序约束    事件流     实时查询
```

**数据模型**:
```cypher
// 节点类型
(Cargo:货物 {id, weight, volume, destination, priority})
(Flight:航班 {flight_no, departure_time, capacity, status})
(Storage:仓位 {zone, row, column, capacity, occupied})
(Staff:人员 {id, name, role, shift})

// 关系类型
(Cargo)-[:STORED_IN]->(Storage)
(Cargo)-[:ASSIGNED_TO]->(Flight)
(Cargo)-[:HANDLED_BY]->(Staff)
(Flight)-[:USES]->(Storage)
```

### 部署架构

**单机高可用架构**:
```
┌─────────────────────────────────────────┐
│  前端 (Vue.js)                           │
│  - 货运管理界面                          │
│  - 实时状态监控                          │
└─────────────────┬───────────────────────┘
                  │ HTTPS + WebSocket
┌─────────────────▼───────────────────────┐
│  Nexora API Server                      │
│  - 认证: HMAC-SHA256                    │
│  - 速率限制: 200 req/s                  │
│  - 审计日志: /var/log/nexora/audit.jsonl│
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│  Nexora Core (单机模式)                  │
│  - RocksDB 持久化                       │
│  - WAL 崩溃恢复                         │
│  - 物化视图 (实时统计)                   │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│  持久化层                                │
│  - /data/nexora/rocksdb/                │
│  - /data/nexora/wal/                    │
│  - 每日增量备份 → S3                    │
└─────────────────────────────────────────┘
```

### 部署配置

**服务器规格** (阿里云/AWS):
- CPU: 8核 (推荐 16核)
- 内存: 32GB (推荐 64GB)
- 存储: 500GB SSD
- 网络: 10Gbps

**Nexora 配置** (`nexora.toml`):
```toml
[server]
host = "0.0.0.0"
port = 8080
request_body_limit_mb = 16

[storage]
rocksdb_path = "/data/nexora/rocksdb"
wal_dir = "/data/nexora/wal"
wal_sync_policy = "group"  # 高吞吐 + 持久化保证

[graph]
num_shards = 256
max_nodes_per_shard = 50000

[security]
require_auth = true
auth_secret = "${NEXORA_AUTH_SECRET}"  # 从环境变量读取
tls_cert = "/etc/nexora/cert.pem"
tls_key = "/etc/nexora/key.pem"
rate_limit = true
rate_limit_rate = 200.0
rate_limit_burst = 400
audit_log_file = "/var/log/nexora/audit.jsonl"
strict_security = true

[monitoring]
metrics_port = 9090
enable_prometheus = true

[backup]
enabled = true
schedule = "0 2 * * *"  # 每天凌晨2点
retention_days = 30
s3_bucket = "nexora-backup-cargo-station"
```

**环境变量** (`.env`):
```bash
NEXORA_AUTH_SECRET=<256-bit-random-key>
NEXORA_ENCRYPTION_KEY=<64-hex-chars>
NEXORA_STRICT_SECURITY=true
RUST_LOG=info,nexora_core=debug
AWS_ACCESS_KEY_ID=<your-key>
AWS_SECRET_ACCESS_KEY=<your-secret>
```

### Standing Queries (实时监控)

**1. 超时预警**:
```cypher
// 货物在仓超过4小时未配载
MATCH (c:Cargo)-[:STORED_IN]->(s:Storage)
WHERE NOT (c)-[:ASSIGNED_TO]->(:Flight)
  AND c.storage_time < timestamp() - 4 * 3600 * 1000
RETURN c.id, c.destination, c.priority
```

**2. 仓位利用率**:
```cypher
// 仓位占用率超过80%
MATCH (s:Storage)
WITH s, size((s)<-[:STORED_IN]-(:Cargo)) AS occupied
WHERE occupied * 1.0 / s.capacity > 0.8
RETURN s.zone, s.row, occupied, s.capacity
```

**3. 航班超载检测**:
```cypher
// 航班配载重量接近上限
MATCH (f:Flight)<-[:ASSIGNED_TO]-(c:Cargo)
WITH f, sum(c.weight) AS total_weight
WHERE total_weight > f.capacity * 0.95
RETURN f.flight_no, total_weight, f.capacity
```

### 物化视图 (实时统计)

**1. 每日货运量统计**:
```cypher
CREATE MATERIALIZED VIEW daily_cargo_stats AS
MATCH (c:Cargo)
WHERE c.created_date = date()
WITH c.destination AS dest, 
     count(c) AS count,
     sum(c.weight) AS total_weight
RETURN dest, count, total_weight
ORDER BY total_weight DESC
REFRESH INCREMENTAL
```

**2. 仓位状态汇总**:
```cypher
CREATE MATERIALIZED VIEW storage_summary AS
MATCH (s:Storage)
OPTIONAL MATCH (s)<-[:STORED_IN]-(c:Cargo)
WITH s.zone AS zone,
     count(s) AS total_slots,
     count(c) AS occupied_slots
RETURN zone, total_slots, occupied_slots,
       occupied_slots * 100.0 / total_slots AS utilization
REFRESH INCREMENTAL
```

### 数据迁移策略

**从现有系统迁移**:
1. **Phase 1**: 双写 (现有系统 + Nexora)
2. **Phase 2**: 数据对比验证 (1周)
3. **Phase 3**: 切换读流量到 Nexora
4. **Phase 4**: 停止双写,Nexora 为主

**迁移脚本** (Python SDK):
```python
from nexora_rs import NexoraClient

client = NexoraClient("https://nexora.cargo-station.com")

# 批量导入货物数据
def migrate_cargo_data(legacy_db):
    for cargo in legacy_db.query("SELECT * FROM cargo"):
        client.cypher(f"""
            CREATE (c:Cargo {{
                id: '{cargo.id}',
                weight: {cargo.weight},
                volume: {cargo.volume},
                destination: '{cargo.destination}',
                created_at: {cargo.created_at}
            }})
        """)
```

---

## 四、监控和运维

### 关键指标

**性能指标**:
- QPS (查询/秒)
- P99 延迟 (< 100ms)
- 内存占用 (< 30GB)
- 磁盘 I/O

**业务指标**:
- 货物入库速率
- 仓位利用率
- 航班配载完成度
- 超时货物数量

**系统健康**:
- WAL 持久化成功率 (> 99.9%)
- 崩溃恢复次数 (= 0)
- 认证失败率 (< 0.1%)
- 速率限制触发次数

### Prometheus 监控

**nexora-app 内置指标**:
```
nexora_active_nodes              # 当前活跃节点数
nexora_query_duration_seconds    # 查询延迟分布
nexora_wal_operations_total      # WAL 操作计数
nexora_auth_failures_total       # 认证失败次数
nexora_rate_limit_exceeded_total # 速率限制触发次数
```

**Grafana 仪表盘**:
- 系统概览 (QPS, 延迟, 内存)
- 业务监控 (货运量, 仓位利用率)
- 告警面板 (超时, 错误, 资源)

### 告警规则

**Critical**:
- WAL 持久化失败 > 1次/分钟
- 查询 P99 延迟 > 500ms
- 内存占用 > 85%
- 认证失败率 > 1%

**Warning**:
- 磁盘占用 > 80%
- 仓位利用率 > 90%
- 货物超时 > 10件

---

## 五、风险控制和应急预案

### 风险清单

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|---------|
| P0问题未完全修复 | 中 | 高 | Phase 1完成后充分测试 |
| 数据迁移失败 | 低 | 高 | 双写+验证,可回滚 |
| 性能不达标 | 中 | 中 | 压力测试,预留性能余量 |
| 人员培训不足 | 高 | 中 | 提前2周培训,文档完善 |

### 应急预案

**1. 系统崩溃**:
- 自动重启 (systemd)
- WAL 自动恢复
- 5分钟内恢复服务
- 告警通知运维

**2. 数据损坏**:
- 从最近备份恢复
- RTO: 1小时
- RPO: 24小时 (每日备份)

**3. 性能下降**:
- 检查慢查询日志
- 临时禁用非关键 Standing Queries
- 增加服务器资源

**4. 回滚现有系统**:
- 停止 Nexora 写入
- 切换流量回现有系统
- 保留 Nexora 数据用于分析

---

## 六、成功标准

### Phase 1 (Week 3)
- ✅ 所有 P0 问题修复
- ✅ 单机模式通过压力测试 (200 QPS, 1小时)
- ✅ 安全配置检查通过

### Phase 2 (Week 7)
- ✅ 所有 P1 问题修复
- ✅ 试点环境部署完成
- ✅ 数据迁移脚本就绪

### Phase 3 (Week 11)
- ✅ 性能优化完成
- ✅ 监控系统上线
- ✅ 运维文档完善

### Phase 4 (Week 15)
- ✅ 测试覆盖率 > 80%
- ✅ 航空货运站试点上线
- ✅ 运行稳定 1 个月

### 最终验收 (Week 19)
- ✅ 业务数据准确性 100%
- ✅ 系统可用性 > 99.5%
- ✅ 查询 P99 延迟 < 100ms
- ✅ 用户满意度 > 90%

---

## 七、后续扩展规划

### 行业拓展路径

**第二批试点** (6个月后):
- 物流仓储管理
- 供应链追踪
- 智能制造 MES

**第三批扩展** (1年后):
- 金融风控图谱
- 社交网络分析
- 知识图谱应用

### 技术演进

**分布式模式** (修复 P0+P1 后):
- 多货运站联网
- 跨区域数据同步
- 全局实时监控

**高级特性**:
- 图神经网络集成
- 智能配载优化
- 预测性维护

---

## 附录

### A. 团队配置

**核心开发团队** (3-4人):
- Tech Lead × 1: 架构和代码审查
- 后端工程师 × 2: P0/P1 问题修复
- 测试工程师 × 1: 测试用例编写

**试点实施团队** (2-3人):
- 解决方案架构师 × 1
- 运维工程师 × 1
- 业务分析师 × 1

### B. 关键里程碑

| 时间 | 里程碑 | 交付物 |
|------|--------|--------|
| Week 3 | Phase 1 完成 | P0修复报告 + 测试报告 |
| Week 7 | Phase 2 完成 | P1修复报告 + 部署方案 |
| Week 11 | Phase 3 完成 | 性能测试报告 + 优化报告 |
| Week 15 | Phase 4 完成 | 测试覆盖报告 + 部署指南 |
| Week 19 | 试点上线 | 上线报告 + 运维手册 |

### C. 参考文档

- [完整代码审查报告](COMPREHENSIVE_CODE_REVIEW_2026-07-10.md)
- [执行摘要](CODE_REVIEW_EXECUTIVE_SUMMARY.md)
- [已修复问题](PRODUCTION_READINESS_GAPS.md)
- [集群运维指南](docs/cluster-ops.md)

---

**文档版本**: v1.0  
**制定日期**: 2026-07-10  
**负责人**: [待指定]  
**审批人**: [待指定]
