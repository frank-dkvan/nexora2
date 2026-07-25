# Nexora Domain Package 设计方案

**版本:** 1.0  
**日期:** 2026/07/05  
**状态:** 设计阶段（P2.1 实施）

---

## 1. 目录结构设计

```
domains/
├── README.md                           # Domain Package 使用指南
├── generic/                            # 通用对象图（基础）
│   ├── schema.yaml                     # Schema 定义
│   ├── mappings/                       # 事件映射
│   │   └── object_status.yaml
│   ├── queries/                        # Cypher 查询示例
│   │   └── impact_analysis.cypher
│   ├── standing_queries/               # Standing Query 定义
│   │   └── impact_propagation.cypher
│   └── tests/                          # 场景测试
│       ├── fixtures/
│       │   └── sample_data.cypher
│       └── integration_test.rs
│
├── air_cargo_terminal/                 # 智慧航空货站
│   ├── README.md                       # 领域说明
│   ├── schema.yaml
│   ├── mappings/
│   │   ├── agv_status.yaml
│   │   ├── task_events.yaml
│   │   ├── flight_updates.yaml
│   │   └── exception_events.yaml
│   ├── queries/
│   │   ├── flight_risk_analysis.cypher
│   │   └── transfer_risk_check.cypher
│   ├── standing_queries/
│   │   ├── agv_flight_impact.cypher
│   │   └── transfer_miss_risk.cypher
│   ├── materialized_views/
│   │   └── flight_risk_view.cypher
│   └── tests/
│       ├── fixtures/
│       │   └── cargo_terminal_data.cypher
│       └── scenario_agv_flight_test.rs
│
├── manufacturing/                      # 智能制造
│   ├── README.md
│   ├── schema.yaml
│   ├── mappings/
│   │   ├── machine_status.yaml
│   │   ├── production_line_events.yaml
│   │   └── work_order_updates.yaml
│   ├── queries/
│   │   └── quality_trace.cypher
│   ├── standing_queries/
│   │   └── machine_order_impact.cypher
│   └── tests/
│       └── scenario_machine_order_test.rs
│
├── it_observability/                   # IT 运维
│   ├── README.md
│   ├── schema.yaml
│   ├── mappings/
│   │   ├── service_health.yaml
│   │   ├── alert_events.yaml
│   │   └── trace_spans.yaml
│   ├── queries/
│   │   └── dependency_analysis.cypher
│   ├── standing_queries/
│   │   └── service_dependency_alert.cypher
│   └── tests/
│       └── scenario_service_alert_test.rs
│
├── robotics_warehouse/                 # 机器人仓储
│   ├── README.md
│   ├── schema.yaml
│   └── ...
│
├── logistics_supply_chain/             # 物流供应链
│   ├── README.md
│   ├── schema.yaml
│   └── ...
│
├── energy_power/                       # 能源电力
│   ├── README.md
│   ├── schema.yaml
│   └── ...
│
└── safety_security/                    # 安全生产
    ├── README.md
    ├── schema.yaml
    └── ...
```

---

## 2. Schema 定义格式

### 2.1 Generic Schema

```yaml
# domains/generic/schema.yaml
domain: generic
version: 1.0
description: "通用对象图模型，所有 Domain Package 的基础"

labels:
  - name: Object
    description: "通用对象实体"
    properties:
      - name: id
        type: string
        required: true
        indexed: true
      
      - name: type
        type: string
        description: "对象类型"
      
      - name: status
        type: string
        description: "对象状态"
      
      - name: created_at
        type: datetime
      
      - name: updated_at
        type: datetime
  
  - name: Exception
    description: "异常/告警事件"
    properties:
      - name: id
        type: string
        required: true
        indexed: true
      
      - name: severity
        type: string
        enum: [P0, P1, P2, P3, P4]
        required: true
      
      - name: type
        type: string
        required: true
      
      - name: message
        type: string
      
      - name: timestamp
        type: datetime
        required: true
  
  - name: EvidenceRef
    description: "证据引用（不保存原文）"
    properties:
      - name: evidence_id
        type: string
        required: true
      
      - name: store_type
        type: string
        enum: [ReductStore, S3, MinIO, OpenSearch, Loki, ExternalUrl]
        required: true
      
      - name: bucket
        type: string
      
      - name: entry
        type: string
      
      - name: uri
        type: string
      
      - name: timestamp_start
        type: datetime
      
      - name: timestamp_end
        type: datetime

edge_types:
  - name: DEPENDS_ON
    description: "依赖关系"
    properties:
      - name: weight
        type: float
        default: 1.0
      
      - name: created_at
        type: datetime
  
  - name: AFFECTS
    description: "影响关系（异常影响对象）"
    properties:
      - name: impact_level
        type: string
        enum: [HIGH, MEDIUM, LOW]
  
  - name: HAS_EVIDENCE
    description: "关联证据引用"
    source: [Exception, Alert]
    target: [EvidenceRef]

constraints:
  - type: unique
    label: Object
    properties: [id]
  
  - type: unique
    label: Exception
    properties: [id]

indexes:
  - label: Object
    properties: [status]
  
  - label: Exception
    properties: [severity, timestamp]
```

### 2.2 Air Cargo Terminal Schema

```yaml
# domains/air_cargo_terminal/schema.yaml
domain: air_cargo_terminal
version: 1.0
extends: generic
description: "智慧航空货站领域模型"

labels:
  - name: Device
    extends: Object
    description: "设备（AGV/ETV/Forklift）"
    properties:
      - name: device_type
        type: string
        enum: [AGV, ETV, Forklift, Scanner, Robot]
        required: true
      
      - name: battery
        type: float
        description: "电池电量（百分比）"
        min: 0
        max: 100
      
      - name: location
        type: map
        description: "位置信息"
        schema:
          x: float
          y: float
          zone: string
  
  - name: Task
    description: "搬运任务"
    properties:
      - name: id
        type: string
        required: true
        indexed: true
      
      - name: status
        type: string
        enum: [PENDING, IN_PROGRESS, COMPLETED, FAILED, CANCELLED]
        required: true
      
      - name: priority
        type: integer
        min: 0
        max: 10
  
  - name: Piece
    description: "货物件"
    properties:
      - name: id
        type: string
        required: true
      
      - name: awb
        type: string
        description: "航空运单号（AWB）"
        indexed: true
      
      - name: weight
        type: float
      
      - name: volume
        type: float
  
  - name: ULD
    description: "集装器"
    properties:
      - name: id
        type: string
        required: true
      
      - name: uld_type
        type: string
        enum: [AKE, AKN, LD3, LD7, PMC, PAG]
  
  - name: Flight
    description: "航班"
    properties:
      - name: id
        type: string
        required: true
        indexed: true
      
      - name: flight_number
        type: string
      
      - name: cutoff_time
        type: datetime
        required: true
        indexed: true
      
      - name: stn
        type: string
        enum: [ARRIVAL, DEPARTURE]

edge_types:
  - name: EXECUTING
    description: "设备执行任务"
    source: [Device]
    target: [Task]
    properties:
      - name: started_at
        type: datetime
  
  - name: MOVES
    description: "任务搬运货物"
    source: [Task]
    target: [Piece]
  
  - name: LOADED_IN
    description: "货物装载到 ULD"
    source: [Piece]
    target: [ULD]
  
  - name: ASSIGNED_TO
    description: "ULD 分配给航班"
    source: [ULD]
    target: [Flight]

constraints:
  - type: unique
    label: Device
    properties: [id]
  
  - type: unique
    label: Flight
    properties: [flight_number]

business_rules:
  - name: flight_cutoff_validation
    description: "航班截载时间必须大于当前时间"
    trigger: [NodeCreated, PropertySet]
    condition: "label = 'Flight' AND cutoff_time <= NOW()"
    action: REJECT
    message: "Flight cutoff time must be in the future"
```

---

## 3. Event Mapping 示例

### 3.1 Generic Object Status Mapping

```yaml
# domains/generic/mappings/object_status.yaml
domain: generic
mapping_name: object_status_changed
version: 1.0

source:
  type: kafka
  topic: object-status-events
  schema_registry: http://schema-registry:8081

target:
  graph: nexora
  namespace: ${EVENT.domain}

mapping:
  node:
    label: Object
    id: $.object_id
    namespace: $.namespace
  
  properties:
    type: $.object_type
    status: $.status
    updated_at: $.timestamp
  
  edges:
    - condition: $.dependency_id != null
      type: DEPENDS_ON
      target:
        label: Object
        id: $.dependency_id
      properties:
        weight: $.dependency_weight

validation:
  required_fields: [object_id, status, timestamp]
  
error_handling:
  on_parse_error: dead_letter_queue
  on_validation_error: dead_letter_queue
  dlq_topic: nexora-dlq
```

### 3.2 Air Cargo Terminal AGV Status Mapping

```yaml
# domains/air_cargo_terminal/mappings/agv_status.yaml
domain: air_cargo_terminal
mapping_name: agv_status_changed
version: 1.0

source:
  type: kafka
  topic: iot-agv-status
  schema_registry: http://schema-registry:8081

target:
  graph: nexora
  namespace: airport_cargo

mapping:
  node:
    label: Device
    id: $.device_id
    namespace: airport_cargo
  
  properties:
    device_type: "AGV"
    status: $.status
    battery: $.battery_level
    location:
      x: $.location.x
      y: $.location.y
      zone: $.location.zone
    updated_at: $.timestamp
  
  edges:
    - condition: $.task_id != null AND $.status == 'EXECUTING'
      type: EXECUTING
      target:
        label: Task
        id: $.task_id
      properties:
        started_at: $.timestamp
    
    # 如果 AGV 状态不是 EXECUTING，删除 EXECUTING 边
    - condition: $.status != 'EXECUTING'
      type: EXECUTING
      action: REMOVE_ALL

evidence:
  - condition: $.camera_enabled == true
    evidence_ref:
      evidence_id: ${UUID()}
      store_type: ReductStore
      bucket: iot-equipment
      entry: agv/${$.device_id}/camera/front
      timestamp_start: ${$.timestamp - 10s}
      timestamp_end: ${$.timestamp + 10s}
      labels:
        device_id: $.device_id
        sensor: camera_front

validation:
  required_fields: [device_id, status, battery_level, timestamp]
  value_ranges:
    battery_level: [0, 100]

error_handling:
  on_parse_error: log_and_skip
  on_validation_error: dead_letter_queue
  dlq_topic: air-cargo-dlq
```

---

## 4. Standing Query 定义

### 4.1 Generic Impact Propagation

```cypher
-- domains/generic/standing_queries/impact_propagation.cypher
-- @name: generic_impact_propagation
-- @version: 1.0
-- @description: 通用对象异常影响传播分析

REGISTER STANDING QUERY generic_impact_propagation AS
MATCH (e:Exception)-[:AFFECTS]->(root:Object)
MATCH (root)-[:DEPENDS_ON*1..3]->(downstream:Object)
WHERE e.severity IN ['P0', 'P1', 'P2']
RETURN e.id AS exception_id,
       e.severity AS severity,
       e.type AS exception_type,
       root.id AS root_object_id,
       root.type AS root_object_type,
       downstream.id AS impacted_object_id,
       downstream.type AS impacted_object_type,
       length((root)-[:DEPENDS_ON*]->(downstream)) AS impact_distance

OUTPUT
  WebSocket('generic-alerts')
  RocksDB

SUPPRESS 300s BY [exception_id, impacted_object_id]

METADATA
  domain: generic
  author: Nexora Team
  tags: [impact-analysis, dependency, generic]
```

### 4.2 Air Cargo Terminal AGV Flight Impact

```cypher
-- domains/air_cargo_terminal/standing_queries/agv_flight_impact.cypher
-- @name: agv_flight_impact
-- @version: 1.0
-- @description: AGV 故障影响航班分析

REGISTER STANDING QUERY agv_flight_impact AS
MATCH (e:Exception)-[:AFFECTS]->(agv:Device {device_type:'AGV'})
MATCH (agv)-[:EXECUTING]->(task:Task)-[:MOVES]->(piece:Piece)
MATCH (piece)-[:LOADED_IN]->(uld:ULD)-[:ASSIGNED_TO]->(flight:Flight)
WHERE e.severity IN ['P0', 'P1', 'P2']
  AND flight.cutoff_time < datetime() + duration('PT2H')
RETURN e.id AS exception_id,
       e.severity AS severity,
       agv.id AS device_id,
       task.id AS task_id,
       piece.awb AS awb,
       uld.id AS uld_id,
       flight.flight_number AS flight_number,
       flight.cutoff_time AS cutoff_time,
       duration.between(datetime(), flight.cutoff_time).minutes AS minutes_to_cutoff

OUTPUT
  Kafka('flight-alerts')
  WebSocket('airport-ops')
  RocksDB

SUPPRESS 300s BY [exception_id, flight_number]

METADATA
  domain: air_cargo_terminal
  author: Nexora Team
  tags: [agv, flight, cargo, critical-path]
  sla: P1
```

---

## 5. 测试结构

### 5.1 Generic Domain Test

```rust
// domains/generic/tests/integration_test.rs

use nexora_core::*;
use nexora_domain_loader::*;

#[tokio::test]
async fn test_generic_impact_propagation() {
    // 1. 加载 generic domain schema
    let domain = DomainLoader::load("domains/generic").await?;
    let graph = GraphService::new_with_domain(domain)?;
    
    // 2. 加载测试数据
    graph.execute_cypher_file("domains/generic/tests/fixtures/sample_data.cypher").await?;
    
    // 3. 注册 Standing Query
    graph.register_standing_query_file("domains/generic/standing_queries/impact_propagation.cypher").await?;
    
    // 4. 模拟异常事件
    graph.execute_cypher("
        CREATE (e:Exception {
            id: 'EVT001',
            severity: 'P1',
            type: 'FAILURE',
            timestamp: datetime()
        })
        
        MATCH (e:Exception {id: 'EVT001'}), (obj:Object {id: 'OBJ_ROOT'})
        CREATE (e)-[:AFFECTS]->(obj)
    ").await?;
    
    // 5. 等待 Standing Query 触发
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // 6. 验证结果
    let matches = graph.get_standing_query_matches("generic_impact_propagation").await?;
    
    assert_eq!(matches.len(), 2, "应该有 2 个下游对象受影响");
    assert!(matches.iter().any(|m| m.bindings["impacted_object_id"] == "OBJ_DOWNSTREAM_1"));
    assert!(matches.iter().any(|m| m.bindings["impacted_object_id"] == "OBJ_DOWNSTREAM_2"));
    
    // 7. 验证 WebSocket 推送
    let ws_messages = graph.websocket_receiver("generic-alerts").await?;
    assert_eq!(ws_messages.len(), 2);
}
```

---

## 6. 配置加载

### 6.1 Nexora 主配置

```yaml
# nexora.yaml
domains:
  enabled:
    - generic          # 必须加载
    - air_cargo_terminal
    - manufacturing
    - it_observability
  
  paths:
    - ./domains
    - /etc/nexora/domains
    - ${NEXORA_DOMAIN_PATH}
  
  auto_load: true      # 自动加载 enabled 列表中的 domain
  
  validation:
    strict: true       # 严格模式：schema 不匹配时拒绝
    warn_only: false   # 警告模式：只记录警告但不拒绝
```

### 6.2 Domain 加载器

```rust
pub struct DomainLoader {
    registry: DomainRegistry,
}

impl DomainLoader {
    pub async fn load_domain(&mut self, domain_path: &Path) -> Result<Domain> {
        // 1. 加载 schema.yaml
        let schema_path = domain_path.join("schema.yaml");
        let schema: DomainSchema = serde_yaml::from_reader(File::open(schema_path)?)?;
        
        // 2. 验证 schema
        self.validate_schema(&schema)?;
        
        // 3. 加载 mappings
        let mappings = self.load_mappings(domain_path.join("mappings"))?;
        
        // 4. 加载 standing queries
        let standing_queries = self.load_standing_queries(domain_path.join("standing_queries"))?;
        
        // 5. 加载 materialized views
        let materialized_views = self.load_materialized_views(domain_path.join("materialized_views"))?;
        
        // 6. 构建 Domain
        let domain = Domain {
            name: schema.domain.clone(),
            version: schema.version.clone(),
            schema,
            mappings,
            standing_queries,
            materialized_views,
        };
        
        // 7. 注册到 registry
        self.registry.register(domain.clone())?;
        
        Ok(domain)
    }
}
```

---

## 7. 实施计划

### Phase 1: 基础框架（P2.1 - 5天）
- [ ] DomainSchema 数据结构
- [ ] DomainLoader 加载器
- [ ] Schema 验证器
- [ ] Domain Registry
- [ ] 配置文件解析

### Phase 2: Generic Domain（P2.1 - 2天）
- [ ] generic/schema.yaml
- [ ] generic/mappings/object_status.yaml
- [ ] generic/standing_queries/impact_propagation.cypher
- [ ] generic/tests/integration_test.rs

### Phase 3: 行业 Domains（P2.1 - 5天）
- [ ] air_cargo_terminal
- [ ] manufacturing
- [ ] it_observability
- [ ] 每个 domain 的完整测试

### Phase 4: 文档和示例（P2.1 - 2天）
- [ ] domains/README.md
- [ ] 每个 domain 的 README.md
- [ ] 使用指南
- [ ] 最佳实践

---

**维护者:** Nexora Team  
**最后更新:** 2026/07/05  
**估计工作量:** 14 天（2周）
