# Nexora 领域模型扩展机制

**版本:** 1.0  
**日期:** 2026/07/05  
**状态:** 设计完成（P2.1）

---

## 1. 设计原则

**Nexora 核心引擎必须行业无关**。所有行业特定的概念（AGV、Flight、Machine、Container 等）通过 **Domain Package** 定义。

### 1.1 为什么需要 Domain Package？

- ✅ 核心引擎保持通用性
- ✅ 不同行业可独立演进
- ✅ 易于测试和验证
- ✅ 降低耦合度

---

## 2. Domain Package 结构

```
domains/
├── generic/                    # 通用对象图（基础）
│   ├── schema.yaml
│   ├── mappings/
│   ├── queries/
│   └── tests/
├── air_cargo_terminal/         # 智慧航空货站
│   ├── schema.yaml
│   ├── mappings/
│   ├── queries/
│   ├── standing_queries/
│   └── tests/
├── manufacturing/              # 智能制造
├── robotics_warehouse/         # 机器人仓储
├── logistics_supply_chain/     # 物流供应链
├── it_observability/           # IT 运维
├── energy_power/               # 能源电力
└── safety_security/            # 安全生产
```

---

## 3. Schema 定义

### 3.1 通用对象图 Schema

```yaml
# domains/generic/schema.yaml
domain: generic
version: 1.0

labels:
  - name: Object
    description: 通用对象
    properties:
      - name: id
        type: string
        required: true
      - name: type
        type: string
      - name: status
        type: string
  
  - name: Exception
    description: 异常事件
    properties:
      - name: id
        type: string
        required: true
      - name: severity
        type: string
        enum: [P0, P1, P2, P3, P4]
      - name: type
        type: string
      - name: timestamp
        type: datetime

edge_types:
  - name: DEPENDS_ON
    description: 依赖关系
    properties:
      - name: weight
        type: float
  
  - name: AFFECTS
    description: 影响关系
    properties:
      - name: impact_level
        type: string
```

### 3.2 航空货站 Schema

```yaml
# domains/air_cargo_terminal/schema.yaml
domain: air_cargo_terminal
version: 1.0
extends: generic

labels:
  - name: Device
    extends: Object
    properties:
      - name: device_type
        type: string
        enum: [AGV, ETV, Forklift, Scanner]
      - name: battery
        type: float
      - name: location
        type: map
  
  - name: Task
    properties:
      - name: id
        type: string
        required: true
      - name: status
        type: string
        enum: [PENDING, IN_PROGRESS, COMPLETED, FAILED]
  
  - name: Piece
    properties:
      - name: id
        type: string
        required: true
      - name: awb
        type: string
      - name: weight
        type: float
  
  - name: ULD
    properties:
      - name: id
        type: string
        required: true
      - name: type
        type: string
  
  - name: Flight
    properties:
      - name: id
        type: string
        required: true
      - name: cutoff_time
        type: datetime

edge_types:
  - name: EXECUTING
    description: 设备执行任务
    source: Device
    target: Task
  
  - name: MOVES
    description: 任务搬运货物
    source: Task
    target: Piece
  
  - name: LOADED_IN
    description: 货物装载到 ULD
    source: Piece
    target: ULD
  
  - name: ASSIGNED_TO
    description: ULD 分配给航班
    source: ULD
    target: Flight
```

### 3.3 智能制造 Schema

```yaml
# domains/manufacturing/schema.yaml
domain: manufacturing
version: 1.0
extends: generic

labels:
  - name: Machine
    extends: Object
    properties:
      - name: machine_type
        type: string
        enum: [CNC, Robot, Conveyor, Press]
      - name: temperature
        type: float
      - name: speed
        type: float
  
  - name: ProductionLine
    properties:
      - name: id
        type: string
        required: true
      - name: status
        type: string
  
  - name: WorkOrder
    properties:
      - name: id
        type: string
        required: true
      - name: priority
        type: integer

edge_types:
  - name: PART_OF
    description: 设备属于产线
    source: Machine
    target: ProductionLine
  
  - name: SCHEDULED_ON
    description: 工单调度到产线
    source: WorkOrder
    target: ProductionLine
```

---

## 4. Event Mapping 配置

### 4.1 航空货站 AGV 状态映射

```yaml
# domains/air_cargo_terminal/mappings/agv_status.yaml
domain: air_cargo_terminal
source: kafka_agv_status
event_type: AGV_STATUS_CHANGED

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
  
  edges:
    - condition: $.task_id != null
      type: EXECUTING
      target:
        label: Task
        id: $.task_id
```

### 4.2 智能制造设备状态映射

```yaml
# domains/manufacturing/mappings/machine_status.yaml
domain: manufacturing
source: kafka_machine_status
event_type: MACHINE_STATUS_CHANGED

mapping:
  node:
    label: Machine
    id: $.machine_id
    namespace: manufacturing
  
  properties:
    machine_type: $.type
    status: $.status
    temperature: $.sensors.temperature
    speed: $.sensors.rpm
  
  edges:
    - type: PART_OF
      target:
        label: ProductionLine
        id: $.line_id
```

---

## 5. Standing Query 示例

### 5.1 航空货站影响传播

```cypher
# domains/air_cargo_terminal/standing_queries/agv_flight_impact.cypher
REGISTER STANDING QUERY agv_flight_impact AS
MATCH (e:Exception)-[:AFFECTS]->(agv:Device {device_type:'AGV'})
MATCH (agv)-[:EXECUTING]->(task:Task)-[:MOVES]->(piece:Piece)
MATCH (piece)-[:LOADED_IN]->(uld:ULD)-[:ASSIGNED_TO]->(flight:Flight)
WHERE e.severity IN ['P1','P2']
  AND flight.cutoff_time < datetime() + duration('PT2H')
RETURN e.id AS exception_id,
       agv.id AS device_id,
       flight.id AS flight_id,
       flight.cutoff_time AS deadline
OUTPUT Kafka('flight-alerts'), WebSocket('airport-ops')
SUPPRESS 300s BY [e.id, flight.id]
```

### 5.2 智能制造设备异常

```cypher
# domains/manufacturing/standing_queries/machine_line_impact.cypher
REGISTER STANDING QUERY machine_line_impact AS
MATCH (e:Exception)-[:AFFECTS]->(machine:Machine)
MATCH (machine)-[:PART_OF]->(line:ProductionLine)
MATCH (order:WorkOrder)-[:SCHEDULED_ON]->(line)
WHERE e.severity IN ['P1','P2']
  AND order.priority = 1
RETURN e.id, machine.id, line.id, order.id
OUTPUT Kafka('production-alerts')
```

---

## 6. 示例数据

### 6.1 航空货站初始化数据

```cypher
# domains/air_cargo_terminal/tests/fixtures/sample_data.cypher

-- 创建设备
CREATE (:Device {id: 'AGV001', device_type: 'AGV', status: 'RUNNING', namespace: 'airport_cargo'})
CREATE (:Device {id: 'AGV002', device_type: 'AGV', status: 'IDLE', namespace: 'airport_cargo'})

-- 创建任务
CREATE (:Task {id: 'TASK001', status: 'IN_PROGRESS', namespace: 'airport_cargo'})

-- 创建货物
CREATE (:Piece {id: 'PIECE001', awb: '123-45678901', weight: 25.5, namespace: 'airport_cargo'})

-- 创建 ULD
CREATE (:ULD {id: 'AKE12345', type: 'AKE', namespace: 'airport_cargo'})

-- 创建航班
CREATE (:Flight {id: 'LH729', cutoff_time: '2026-07-05T18:00:00Z', namespace: 'airport_cargo'})

-- 创建关系
MATCH (agv:Device {id: 'AGV001'}), (task:Task {id: 'TASK001'})
CREATE (agv)-[:EXECUTING {started_at: '2026-07-05T10:00:00Z'}]->(task)

MATCH (task:Task {id: 'TASK001'}), (piece:Piece {id: 'PIECE001'})
CREATE (task)-[:MOVES]->(piece)

MATCH (piece:Piece {id: 'PIECE001'}), (uld:ULD {id: 'AKE12345'})
CREATE (piece)-[:LOADED_IN]->(uld)

MATCH (uld:ULD {id: 'AKE12345'}), (flight:Flight {id: 'LH729'})
CREATE (uld)-[:ASSIGNED_TO]->(flight)
```

---

## 7. 测试

### 7.1 Domain Package 测试结构

```rust
// domains/air_cargo_terminal/tests/integration_test.rs

#[test]
fn test_agv_flight_impact_scenario() {
    // 1. 加载 schema
    let schema = load_schema("domains/air_cargo_terminal/schema.yaml")?;
    
    // 2. 加载示例数据
    execute_cypher_file("domains/air_cargo_terminal/tests/fixtures/sample_data.cypher")?;
    
    // 3. 注册 Standing Query
    register_standing_query_file("domains/air_cargo_terminal/standing_queries/agv_flight_impact.cypher")?;
    
    // 4. 模拟异常事件
    execute_cypher("
        CREATE (e:Exception {id: 'EVT001', severity: 'P1', type: 'EMERGENCY_STOP'})
        MATCH (e), (agv:Device {id: 'AGV001'})
        CREATE (e)-[:AFFECTS]->(agv)
    ")?;
    
    // 5. 验证 Standing Query 命中
    let matches = get_standing_query_matches("agv_flight_impact")?;
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].bindings["flight_id"], "LH729");
}
```

---

## 8. Domain Package 加载

### 8.1 配置文件

```yaml
# nexora.yaml
domains:
  enabled:
    - generic
    - air_cargo_terminal
    - manufacturing
  
  paths:
    - ./domains
    - /etc/nexora/domains
```

### 8.2 运行时加载

```rust
pub struct DomainRegistry {
    domains: HashMap<String, Domain>,
}

impl DomainRegistry {
    pub fn load_domain(&mut self, path: &Path) -> Result<()> {
        let schema = Self::load_schema(path.join("schema.yaml"))?;
        let mappings = Self::load_mappings(path.join("mappings"))?;
        let queries = Self::load_queries(path.join("standing_queries"))?;
        
        let domain = Domain {
            name: schema.domain.clone(),
            schema,
            mappings,
            queries,
        };
        
        self.domains.insert(domain.name.clone(), domain);
        Ok(())
    }
}
```

---

## 9. 优先级实现计划

### P2.1 — Domain Package 机制（5-7天）
- [ ] Schema 定义格式
- [ ] Mapping 配置解析
- [ ] Domain 加载与注册
- [ ] 命名空间隔离
- [ ] 测试框架

### P2.1 — 示例 Domain Packages（3-4天）
- [ ] generic（通用对象图）
- [ ] air_cargo_terminal（航空货站）
- [ ] manufacturing（智能制造）
- [ ] it_observability（IT 运维）

---

## 10. 未来增强

- Domain Package 版本管理
- Schema 迁移工具
- Domain 依赖关系（extends）
- Domain 市场（社区贡献）
- 多 Domain 联邦查询

---

**维护者:** Nexora Team  
**最后更新:** 2026/07/05
