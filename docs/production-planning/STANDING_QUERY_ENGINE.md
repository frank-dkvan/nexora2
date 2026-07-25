# Nexora Standing Query 引擎

**版本:** 1.0  
**日期:** 2026/07/05  
**状态:** 核心框架完整（60%），触发器待集成（P1.1）

---

## 1. 概述

Standing Query（持续查询/站立查询）是 Nexora 的核心能力之一，用于**实时监控图模式**并在匹配时自动触发动作。

### 1.1 典型应用场景

| 场景 | 描述 | Standing Query |
|------|------|---------------|
| **影响传播分析** | 异常发生时实时计算受影响对象 | `MATCH (e:Exception)-[:AFFECTS]->(obj)-[:DEPENDS_ON*1..3]->(downstream)` |
| **风险预警** | 关键路径阻塞时自动告警 | `MATCH (task:Task {status:'BLOCKED'})-[:AFFECTS]->(flight:Flight) WHERE flight.cutoff_time < now() + 2h` |
| **合规检测** | 实时检测违规操作 | `MATCH (user:User)-[:ACCESSED]->(zone:RestrictedZone) WHERE NOT user.clearance >= zone.level` |
| **异常检测** | 资源超限自动触发 | `MATCH (device:Device) WHERE device.temperature > 80` |
| **关系推断** | 自动建立隐含关系 | `MATCH (a)-[:COLLEAGUE]->(b)-[:COLLEAGUE]->(c) WHERE NOT (a)-[:COLLEAGUE]->(c) CREATE (a)-[:POTENTIAL_COLLEAGUE]->(c)` |

---

## 2. 架构设计

### 2.1 核心组件

```
┌─────────────────────────────────────────────────────────────┐
│                 StandingQueryManager                         │
│  • 规则注册与管理                                            │
│  • GraphMutation 订阅                                        │
│  • 增量匹配引擎                                              │
│  • 结果持久化与推送                                          │
└─────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   触发器系统                                 │
│  PropertySet | EdgeAdded | LabelAdded | NodeDeleted         │
└─────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   匹配引擎                                   │
│  • Pattern Matching (图模式匹配)                            │
│  • Incremental Evaluation (增量计算)                         │
│  • Fixpoint Integration (传递闭包)                          │
└─────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   输出系统                                   │
│  WebSocket | Kafka | HTTP Webhook | RocksDB                 │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 工作流程

```
1. 用户注册 SQ：
   REGISTER STANDING QUERY impact_analysis AS
   MATCH (e:Exception)-[:AFFECTS]->(obj)-[:DEPENDS_ON*1..3]->(downstream)
   WHERE e.severity IN ['P1','P2']
   RETURN e.id, obj.id, downstream.id

2. GraphMutation 发生：
   CREATE (e:Exception {id: 'EVT001', severity: 'P1'})-[:AFFECTS]->(obj:Object {id: 'OBJ_ROOT'})

3. 触发器检测：
   StandingQueryManager 检测到 EdgeAdded 事件

4. 增量评估：
   只评估涉及 EVT001 和 OBJ_ROOT 的 SQ 规则

5. 匹配成功：
   发现 OBJ_ROOT 有 DEPENDS_ON 下游节点

6. 结果推送：
   - WebSocket 推送给订阅客户端
   - 持久化到 RocksDB `standing-query-states` CF
   - 可选：发送到 Kafka / HTTP Webhook
```

---

## 3. 数据结构

### 3.1 StandingQuery 定义

```rust
pub struct StandingQuery {
    /// 规则 ID（全局唯一）
    pub id: String,
    
    /// 规则名称
    pub name: String,
    
    /// Cypher 模式（只支持 MATCH...WHERE...RETURN 子集）
    pub pattern: String,
    
    /// 规则版本（用于灰度和回滚）
    pub version: u64,
    
    /// 启用状态
    pub enabled: bool,
    
    /// 命名空间（隔离）
    pub namespace: Option<Symbol>,
    
    /// 租户 ID
    pub tenant_id: Option<Symbol>,
    
    /// 创建时间
    pub created_at: DateTime<Utc>,
    
    /// 最后触发时间
    pub last_triggered_at: Option<DateTime<Utc>>,
    
    /// 输出配置
    pub outputs: Vec<StandingQueryOutput>,
    
    /// 抑制配置（防止重复告警）
    pub suppression: Option<SuppressionConfig>,
}

pub enum StandingQueryOutput {
    WebSocket { channel: String },
    Kafka { topic: String },
    HttpWebhook { url: String, headers: HashMap<String, String> },
    RocksDB,  // 默认持久化
}

pub struct SuppressionConfig {
    /// 抑制窗口（秒）
    pub window_seconds: u64,
    
    /// 去重键（例如 ["e.id", "obj.id"]）
    pub dedup_keys: Vec<String>,
}
```

### 3.2 StandingQuery 匹配结果

```rust
pub struct StandingQueryMatch {
    /// 规则 ID
    pub query_id: String,
    
    /// 规则版本
    pub query_version: u64,
    
    /// 匹配时间
    pub matched_at: DateTime<Utc>,
    
    /// 匹配结果（变量绑定）
    pub bindings: HashMap<String, PropertyValue>,
    
    /// 命中解释（用于调试）
    pub explanation: MatchExplanation,
    
    /// 是否恢复（之前命中，现在不匹配了）
    pub is_recovery: bool,
}

pub struct MatchExplanation {
    /// 参与节点
    pub nodes: Vec<NexoraId>,
    
    /// 参与边
    pub edges: Vec<(NexoraId, Symbol, NexoraId)>,
    
    /// 满足的条件
    pub predicates: Vec<String>,
    
    /// 证据引用（可选）
    pub evidence_refs: Vec<String>,
}
```

---

## 4. 触发器系统（P1.1 实现）

### 4.1 触发器类型

```rust
pub enum GraphMutationTrigger {
    /// 属性变化（已实现）
    PropertySet {
        node: NexoraId,
        key: Symbol,
        value: PropertyValue,
        prev: Option<PropertyValue>,
    },
    
    /// 属性删除（已实现）
    PropertyRemoved {
        node: NexoraId,
        key: Symbol,
    },
    
    /// 边新增（P1.1 待实现）
    EdgeAdded {
        src: NexoraId,
        edge_type: Symbol,
        dst: NexoraId,
    },
    
    /// 边删除（P1.1 待实现）
    EdgeRemoved {
        src: NexoraId,
        edge_type: Symbol,
        dst: NexoraId,
    },
    
    /// 标签新增（P1.1 待实现）
    LabelAdded {
        node: NexoraId,
        label: Symbol,
    },
    
    /// 标签删除（P1.1 待实现）
    LabelRemoved {
        node: NexoraId,
        label: Symbol,
    },
    
    /// 节点删除（P1.1 待实现）
    NodeDeleted {
        node: NexoraId,
    },
}
```

### 4.2 触发逻辑

```rust
impl StandingQueryManager {
    /// 属性变化触发（已实现）
    pub async fn on_property_changed(
        &self,
        node: NexoraId,
        key: Symbol,
        value: PropertyValue,
    ) -> Result<()> {
        // 1. 查找涉及该节点+属性的 SQ 规则
        let affected_queries = self.find_affected_queries_by_node(node)?;
        
        // 2. 增量重新评估
        for query in affected_queries {
            if self.should_evaluate(query, node)? {
                let matches = self.evaluate_incremental(query, node).await?;
                
                // 3. 推送结果
                for m in matches {
                    self.push_match(m).await?;
                }
            }
        }
        
        Ok(())
    }
    
    /// 边新增触发（P1.1 待实现）
    pub async fn on_edge_added(
        &self,
        src: NexoraId,
        edge_type: Symbol,
        dst: NexoraId,
    ) -> Result<()> {
        // 1. 查找涉及该边类型的 SQ 规则
        let affected_queries = self.find_affected_queries_by_edge(edge_type)?;
        
        // 2. 增量重新评估（只评估涉及 src 或 dst 的路径）
        for query in affected_queries {
            let matches = self.evaluate_incremental_edge(query, src, dst).await?;
            
            // 3. 推送结果
            for m in matches {
                self.push_match(m).await?;
            }
        }
        
        Ok(())
    }
    
    /// 节点删除触发（P1.1 待实现）
    pub async fn cleanup_node(&self, node: NexoraId) -> Result<()> {
        // 1. 查找所有涉及该节点的活跃匹配
        let active_matches = self.find_active_matches_by_node(node)?;
        
        // 2. 发送恢复通知（is_recovery = true）
        for m in active_matches {
            let recovery = StandingQueryMatch {
                is_recovery: true,
                ..m
            };
            self.push_match(recovery).await?;
        }
        
        // 3. 清理持久化状态
        self.remove_matches_by_node(node)?;
        
        Ok(())
    }
}
```

---

## 5. 增量匹配引擎

### 5.1 全量评估 vs 增量评估

**全量评估（初始构建）：**
```rust
fn evaluate_full(&self, query: &StandingQuery) -> Result<Vec<StandingQueryMatch>> {
    // 1. 执行 Cypher 查询
    let result = self.cypher_executor.execute(&query.pattern)?;
    
    // 2. 转换为 StandingQueryMatch
    let matches = result.rows.into_iter().map(|row| {
        StandingQueryMatch {
            query_id: query.id.clone(),
            matched_at: Utc::now(),
            bindings: row,
            explanation: self.explain(query, &row)?,
            is_recovery: false,
        }
    }).collect();
    
    Ok(matches)
}
```

**增量评估（变更后）：**
```rust
fn evaluate_incremental(&self, query: &StandingQuery, changed_node: NexoraId) -> Result<Vec<StandingQueryMatch>> {
    // 1. 只评估涉及 changed_node 的子图
    let subgraph = self.extract_subgraph(changed_node, query.max_hops())?;
    
    // 2. 在子图上执行模式匹配
    let new_matches = self.match_pattern_on_subgraph(query, subgraph)?;
    
    // 3. 与旧匹配对比，找出新增/删除
    let old_matches = self.get_existing_matches(query.id)?;
    let (added, removed) = self.diff_matches(old_matches, new_matches)?;
    
    // 4. 返回新增匹配 + 恢复通知
    let mut results = added;
    results.extend(removed.into_iter().map(|m| StandingQueryMatch {
        is_recovery: true,
        ..m
    }));
    
    Ok(results)
}
```

### 5.2 Fixpoint 集成（P1.3）

对于多跳路径查询 `MATCH (a)-[:DEPENDS_ON*1..3]->(b)`，使用 Fixpoint 引擎增量计算传递闭包：

```rust
impl StandingQueryManager {
    fn integrate_fixpoint(&self, query: &StandingQuery) -> Result<()> {
        // 1. 检测查询是否包含路径模式
        if query.has_path_pattern() {
            // 2. 订阅 Fixpoint ReachabilityDelta
            let edge_type = query.extract_edge_type()?;
            self.fixpoint_engine.subscribe_reachability(edge_type, |delta| {
                // 3. Delta 包含新增/删除的可达性对
                for (src, dst) in delta.added {
                    self.on_reachability_added(query, src, dst)?;
                }
                for (src, dst) in delta.removed {
                    self.on_reachability_removed(query, src, dst)?;
                }
            })?;
        }
        Ok(())
    }
}
```

---

## 6. 持久化（P1.2）

### 6.1 RocksDB Column Families

```
standing-queries          → StandingQuery 规则定义
standing-query-states     → 活跃匹配状态
standing-query-history    → 历史命中记录（可选）
```

### 6.2 持久化策略

```rust
impl StandingQueryManager {
    /// 注册规则时持久化
    pub fn register(&mut self, query: StandingQuery) -> Result<()> {
        // 1. 持久化规则定义
        let key = format!("sq:rule:{}", query.id);
        let value = bincode::serialize(&query)?;
        self.rocksdb.put_cf("standing-queries", key, value)?;
        
        // 2. 内存注册
        self.queries.insert(query.id.clone(), query);
        
        Ok(())
    }
    
    /// 命中时持久化状态
    pub async fn push_match(&self, m: StandingQueryMatch) -> Result<()> {
        // 1. 持久化匹配状态
        let key = format!("sq:state:{}:{}", m.query_id, m.match_key());
        let value = bincode::serialize(&m)?;
        self.rocksdb.put_cf("standing-query-states", key, value)?;
        
        // 2. 推送到输出
        for output in &self.queries[&m.query_id].outputs {
            match output {
                StandingQueryOutput::WebSocket { channel } => {
                    self.ws_broadcaster.send(channel, &m).await?;
                }
                StandingQueryOutput::Kafka { topic } => {
                    self.kafka_producer.send(topic, &m).await?;
                }
                // ...
            }
        }
        
        Ok(())
    }
    
    /// 启动时从 RocksDB 恢复
    pub fn recover(&mut self) -> Result<()> {
        let cf = self.rocksdb.cf_handle("standing-queries").unwrap();
        let iter = self.rocksdb.iterator_cf(cf, IteratorMode::Start);
        
        for (key, value) in iter {
            let query: StandingQuery = bincode::deserialize(&value)?;
            self.queries.insert(query.id.clone(), query);
        }
        
        Ok(())
    }
}
```

---

## 7. 查询语言支持

### 7.1 支持的 Cypher 子集

```cypher
-- ✅ 支持：简单模式匹配
MATCH (n:Label {prop: value})
RETURN n

-- ✅ 支持：边匹配
MATCH (a)-[:EDGE_TYPE]->(b)
WHERE a.prop > 10
RETURN a, b

-- ✅ 支持：多跳路径
MATCH (a)-[:DEPENDS_ON*1..3]->(b)
RETURN a, b

-- ✅ 支持：WHERE 条件
MATCH (n:Device)
WHERE n.temperature > 80 AND n.status = 'RUNNING'
RETURN n

-- ❌ 不支持：复杂聚合
MATCH (n)
RETURN count(n), avg(n.value)  -- 不支持

-- ❌ 不支持：子查询
MATCH (n)
WHERE (n)-[:RELATED]->(:Other)  -- 不支持 WHERE 子查询

-- ❌ 不支持：OPTIONAL MATCH
OPTIONAL MATCH (n)-[:EDGE]->(m)  -- 不支持
```

### 7.2 示例规则

#### 通用影响传播
```cypher
REGISTER STANDING QUERY impact_propagation AS
MATCH (e:Exception)-[:AFFECTS]->(obj:Object)-[:DEPENDS_ON*1..3]->(downstream:Object)
WHERE e.severity IN ['P1','P2']
RETURN e.id AS exception_id,
       obj.id AS root_object_id,
       downstream.id AS impacted_object_id,
       e.severity AS severity
OUTPUT WebSocket('alerts')
```

#### 航空货站 AGV 故障影响航班
```cypher
REGISTER STANDING QUERY agv_flight_impact AS
MATCH (e:Exception)-[:AFFECTS]->(agv:Device {type:'AGV'})
MATCH (agv)-[:EXECUTING]->(task:Task)-[:MOVES]->(piece:Piece)
MATCH (piece)-[:LOADED_IN]->(uld:ULD)-[:ASSIGNED_TO]->(flight:Flight)
WHERE e.severity IN ['P1','P2']
  AND flight.cutoff_time < datetime() + duration('PT2H')
RETURN e.id, agv.id, flight.id, flight.cutoff_time
OUTPUT Kafka('flight-alerts'), WebSocket('airport-ops')
SUPPRESS 300s BY [e.id, flight.id]
```

#### IT 运维服务依赖告警
```cypher
REGISTER STANDING QUERY service_dependency_alert AS
MATCH (alert:Alert)-[:AFFECTS]->(svc:Service)
MATCH (svc)-[:DEPENDS_ON*1..3]->(dep:Service)
WHERE alert.severity IN ['critical','high']
RETURN alert.id, svc.id, dep.id, dep.status
OUTPUT Webhook('https://oncall.example.com/incidents')
```

---

## 8. 性能优化

### 8.1 索引利用

```rust
// 查询优化器自动选择索引
MATCH (n:Device {status: 'RUNNING'})  
// → 使用 Label Index + Property Index，O(1) 查找
```

### 8.2 增量计算

```rust
// 只重算受影响的子图
on_edge_added(src, edge_type, dst) {
    // 不重新执行整个查询，只评估涉及 src/dst 的路径
    let affected_queries = find_queries_with_edge_type(edge_type);
    for query in affected_queries {
        evaluate_local_subgraph(query, [src, dst]);
    }
}
```

### 8.3 抑制重复告警

```rust
pub struct SuppressionConfig {
    window_seconds: 300,           // 5 分钟窗口
    dedup_keys: ["e.id", "obj.id"] // 去重键
}

// 实现
let suppression_key = format!("{}:{}", bindings["e.id"], bindings["obj.id"]);
if !self.suppression_cache.is_suppressed(suppression_key, 300) {
    self.push_match(match);
    self.suppression_cache.mark(suppression_key, Utc::now());
}
```

---

## 9. 监控指标

### 9.1 Prometheus Metrics

```prometheus
# 规则数量
nexora_standing_queries_total{namespace="airport_cargo"}

# 评估次数
nexora_sq_evaluations_total{query_id="impact_propagation"}

# 命中次数
nexora_sq_matches_total{query_id="agv_flight_impact"}

# 评估延迟
nexora_sq_evaluation_duration_seconds{query_id="service_dependency_alert",percentile="p95"}

# 抑制次数
nexora_sq_suppressed_total{query_id="impact_propagation"}
```

### 9.2 日志示例

```json
{
  "timestamp": "2026-07-05T10:30:15.123Z",
  "level": "INFO",
  "event": "standing_query_match",
  "query_id": "agv_flight_impact",
  "query_version": 1,
  "matched_at": "2026-07-05T10:30:15.120Z",
  "bindings": {
    "e.id": "EVT001",
    "agv.id": "AGV001",
    "flight.id": "LH729"
  },
  "is_recovery": false,
  "output": ["kafka:flight-alerts", "websocket:airport-ops"]
}
```

---

## 10. 未来增强

### Phase 1（P1.1-P1.3）
- ✅ 边变化触发器
- ✅ 标签变化触发器
- ✅ 节点删除触发器
- ✅ 持久化 SQ 状态
- ✅ Fixpoint 集成

### Phase 2（P2+）
- 时间窗口聚合（`WHERE event_time > now() - 5m`）
- 状态机（多步骤规则）
- 条件触发（`ON MATCH ... WHEN ... THEN ...`）
- 动态规则热更新
- 规则灰度发布

### Phase 3（未来）
- 分布式 Standing Query（跨 Shard 协调）
- 机器学习集成（异常检测模型）
- 图神经网络嵌入

---

**维护者:** Nexora Team  
**最后更新:** 2026/07/05  
**实现进度:** 核心框架 60%，P1.1-P1.3 待完成
