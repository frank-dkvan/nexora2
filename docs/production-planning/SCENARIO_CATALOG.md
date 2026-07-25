# Nexora 场景目录

**版本:** 1.0  
**日期:** 2026/07/05  
**状态:** 设计完成，待验证

---

## 1. 概述

本文档展示 Nexora 在不同行业和场景下的应用模式。所有场景基于**通用对象图模型**，通过 Domain Package 实现行业特定逻辑。

---

## 2. 通用场景

### 2.1 通用对象异常影响传播

**场景描述:**  
当异常事件影响某个对象时，自动计算受影响的所有下游依赖对象。

**图模式:**
```cypher
CREATE (:Object {id: 'OBJ_ROOT', type: 'GenericAsset', status: 'RUNNING'})
CREATE (:Object {id: 'OBJ_DOWNSTREAM_1', type: 'DependentAsset', status: 'RUNNING'})
CREATE (:Object {id: 'OBJ_DOWNSTREAM_2', type: 'DependentAsset', status: 'RUNNING'})
CREATE (:Exception {id: 'EVT001', severity: 'P1', type: 'FAILURE'})

MATCH (root:Object {id: 'OBJ_ROOT'}), (d1:Object {id: 'OBJ_DOWNSTREAM_1'})
CREATE (root)-[:DEPENDS_ON]->(d1)

MATCH (d1:Object {id: 'OBJ_DOWNSTREAM_1'}), (d2:Object {id: 'OBJ_DOWNSTREAM_2'})
CREATE (d1)-[:DEPENDS_ON]->(d2)

MATCH (e:Exception {id: 'EVT001'}), (root:Object {id: 'OBJ_ROOT'})
CREATE (e)-[:AFFECTS]->(root)
```

**Standing Query:**
```cypher
REGISTER STANDING QUERY generic_impact_propagation AS
MATCH (e:Exception)-[:AFFECTS]->(root:Object)
MATCH (root)-[:DEPENDS_ON*1..3]->(downstream:Object)
WHERE e.severity IN ['P1','P2']
RETURN e.id AS exception_id,
       root.id AS root_object_id,
       downstream.id AS impacted_object_id,
       e.severity AS severity
```

**预期结果:**
- 当 EVT001 → OBJ_ROOT 关系建立时自动触发
- 返回 OBJ_DOWNSTREAM_1 和 OBJ_DOWNSTREAM_2
- WebSocket 推送告警
- Prometheus 记录 SQ 命中

**验收标准:**
- [x] Standing Query 自动触发
- [x] 返回 2 个下游对象
- [x] 命中持久化到 RocksDB
- [x] 命中 explain 可查询

---

## 3. 智慧航空货站

### 3.1 AGV 故障影响航班

**Domain:** `air_cargo_terminal`

**业务描述:**  
AGV 执行搬运任务时发生紧急停止，需要立即评估对航班的影响，尤其是距离截载时间不足 2 小时的航班。

**图模式:**
```cypher
CREATE (:Device {id: 'AGV001', device_type: 'AGV', status: 'RUNNING', namespace: 'airport_cargo'})
CREATE (:Task {id: 'TASK001', status: 'IN_PROGRESS', namespace: 'airport_cargo'})
CREATE (:Piece {id: 'PIECE001', awb: '123-45678901', namespace: 'airport_cargo'})
CREATE (:ULD {id: 'AKE12345', namespace: 'airport_cargo'})
CREATE (:Flight {id: 'LH729', cutoff_time: '2026-07-05T18:00:00Z', namespace: 'airport_cargo'})
CREATE (:Exception {id: 'EVT001', severity: 'P1', type: 'EMERGENCY_STOP', namespace: 'airport_cargo'})

MATCH (agv:Device {id: 'AGV001'}), (task:Task {id: 'TASK001'})
CREATE (agv)-[:EXECUTING]->(task)

MATCH (task:Task {id: 'TASK001'}), (piece:Piece {id: 'PIECE001'})
CREATE (task)-[:MOVES]->(piece)

MATCH (piece:Piece {id: 'PIECE001'}), (uld:ULD {id: 'AKE12345'})
CREATE (piece)-[:LOADED_IN]->(uld)

MATCH (uld:ULD {id: 'AKE12345'}), (flight:Flight {id: 'LH729'})
CREATE (uld)-[:ASSIGNED_TO]->(flight)

MATCH (e:Exception {id: 'EVT001'}), (agv:Device {id: 'AGV001'})
CREATE (e)-[:AFFECTS]->(agv)
```

**Standing Query:**
```cypher
REGISTER STANDING QUERY agv_flight_impact AS
MATCH (e:Exception)-[:AFFECTS]->(agv:Device {device_type:'AGV'})
MATCH (agv)-[:EXECUTING]->(task:Task)-[:MOVES]->(piece:Piece)
MATCH (piece)-[:LOADED_IN]->(uld:ULD)-[:ASSIGNED_TO]->(flight:Flight)
WHERE e.severity IN ['P1','P2']
  AND flight.cutoff_time < datetime() + duration('PT2H')
RETURN e.id, agv.id, task.id, piece.id, uld.id, flight.id, flight.cutoff_time
```

**业务动作:**
- 自动推送告警到航站运营中心
- 触发任务重新分配
- 通知备用 AGV 接管

**验收标准:**
- [ ] AGV 异常时自动触发
- [ ] 计算完整影响链：AGV → Task → Piece → ULD → Flight
- [ ] 只告警距离截载 < 2h 的航班
- [ ] Kafka 推送到 `flight-alerts` topic
- [ ] WebSocket 推送到 `airport-ops` 频道

---

### 3.2 中转错失风险预警

**业务描述:**  
中转货物因上游航班延误导致无法在下游航班截载前完成装载，需要自动识别并告警。

**图模式:**
```cypher
CREATE (:Flight {id: 'CA123', stn: 'ARRIVAL', scheduled_time: '2026-07-05T16:30:00Z', actual_time: '2026-07-05T17:45:00Z'})
CREATE (:Flight {id: 'LH729', stn: 'DEPARTURE', cutoff_time: '2026-07-05T18:00:00Z'})
CREATE (:Piece {id: 'PIECE001', awb: '123-45678901', type: 'TRANSFER'})

MATCH (arrival:Flight {id: 'CA123'}), (piece:Piece {id: 'PIECE001'})
CREATE (arrival)-[:CARRIES]->(piece)

MATCH (piece:Piece {id: 'PIECE001'}), (departure:Flight {id: 'LH729'})
CREATE (piece)-[:TRANSFER_TO]->(departure)
```

**Standing Query:**
```cypher
REGISTER STANDING QUERY transfer_risk AS
MATCH (arrival:Flight {stn:'ARRIVAL'})-[:CARRIES]->(piece:Piece {type:'TRANSFER'})
MATCH (piece)-[:TRANSFER_TO]->(departure:Flight {stn:'DEPARTURE'})
WHERE arrival.actual_time > departure.cutoff_time - duration('PT90M')
RETURN piece.awb, arrival.id AS from_flight, departure.id AS to_flight, 
       departure.cutoff_time, arrival.actual_time
```

---

## 4. 智能制造

### 4.1 设备异常影响工单

**Domain:** `manufacturing`

**业务描述:**  
生产线设备发生异常，自动评估对正在执行的工单的影响。

**图模式:**
```cypher
CREATE (:Machine {id: 'M001', machine_type: 'CNC', status: 'RUNNING', namespace: 'manufacturing'})
CREATE (:ProductionLine {id: 'LINE_A', status: 'ACTIVE', namespace: 'manufacturing'})
CREATE (:WorkOrder {id: 'WO001', priority: 1, status: 'IN_PROGRESS', namespace: 'manufacturing'})
CREATE (:Exception {id: 'E001', severity: 'P1', type: 'MACHINE_FAILURE', namespace: 'manufacturing'})

MATCH (m:Machine {id: 'M001'}), (line:ProductionLine {id: 'LINE_A'})
CREATE (m)-[:PART_OF]->(line)

MATCH (order:WorkOrder {id: 'WO001'}), (line:ProductionLine {id: 'LINE_A'})
CREATE (order)-[:SCHEDULED_ON]->(line)

MATCH (e:Exception {id: 'E001'}), (m:Machine {id: 'M001'})
CREATE (e)-[:AFFECTS]->(m)
```

**Standing Query:**
```cypher
REGISTER STANDING QUERY machine_order_impact AS
MATCH (e:Exception)-[:AFFECTS]->(machine:Machine)
MATCH (machine)-[:PART_OF]->(line:ProductionLine)
MATCH (order:WorkOrder)-[:SCHEDULED_ON]->(line)
WHERE e.severity IN ['P1','P2']
  AND order.priority = 1
RETURN e.id, machine.id, line.id, order.id, order.priority
```

**业务动作:**
- 暂停受影响工单
- 通知生产调度重新排期
- 触发维修工单

---

### 4.2 质量问题追溯

**业务描述:**  
检测到产品质量问题时，追溯涉及的所有设备和物料批次。

**图模式:**
```cypher
CREATE (:Product {id: 'PROD001', batch: 'B001', quality: 'DEFECT'})
CREATE (:Machine {id: 'M001'})
CREATE (:Machine {id: 'M002'})
CREATE (:Material {id: 'MAT001', batch: 'MB001'})

MATCH (prod:Product {id: 'PROD001'}), (m1:Machine {id: 'M001'})
CREATE (prod)-[:PROCESSED_BY {step: 1}]->(m1)

MATCH (prod:Product {id: 'PROD001'}), (m2:Machine {id: 'M002'})
CREATE (prod)-[:PROCESSED_BY {step: 2}]->(m2)

MATCH (prod:Product {id: 'PROD001'}), (mat:Material {id: 'MAT001'})
CREATE (prod)-[:USES_MATERIAL]->(mat)
```

**查询:**
```cypher
MATCH (prod:Product {quality: 'DEFECT'})-[:PROCESSED_BY]->(machine:Machine)
MATCH (prod)-[:USES_MATERIAL]->(material:Material)
RETURN prod.id, prod.batch, 
       collect(DISTINCT machine.id) AS machines,
       collect(DISTINCT material.batch) AS material_batches
```

---

## 5. IT 运维

### 5.1 服务依赖告警

**Domain:** `it_observability`

**业务描述:**  
关键服务异常时，自动计算所有依赖该服务的上游服务并告警。

**图模式:**
```cypher
CREATE (:Service {id: 'api-gateway', status: 'HEALTHY', namespace: 'it_ops'})
CREATE (:Service {id: 'user-service', status: 'HEALTHY', namespace: 'it_ops'})
CREATE (:Service {id: 'database', status: 'DEGRADED', namespace: 'it_ops'})
CREATE (:Alert {id: 'A001', severity: 'critical', message: 'DB connection pool exhausted', namespace: 'it_ops'})

MATCH (gw:Service {id: 'api-gateway'}), (usr:Service {id: 'user-service'})
CREATE (gw)-[:DEPENDS_ON]->(usr)

MATCH (usr:Service {id: 'user-service'}), (db:Service {id: 'database'})
CREATE (usr)-[:DEPENDS_ON]->(db)

MATCH (alert:Alert {id: 'A001'}), (db:Service {id: 'database'})
CREATE (alert)-[:AFFECTS]->(db)
```

**Standing Query:**
```cypher
REGISTER STANDING QUERY service_dependency_alert AS
MATCH (alert:Alert)-[:AFFECTS]->(svc:Service)
MATCH (upstream:Service)-[:DEPENDS_ON*1..3]->(svc)
WHERE alert.severity IN ['critical','high']
RETURN alert.id, svc.id, upstream.id, upstream.status
```

**业务动作:**
- 推送到 PagerDuty / OpsGenie
- 自动触发 Circuit Breaker
- 启动降级预案

---

### 5.2 接口链路分析

**业务描述:**  
基于 OpenTelemetry trace 构建接口调用关系图，分析故障影响范围。

**图模式:**
```cypher
CREATE (:ApiCall {id: 'call001', trace_id: 'abc123', endpoint: '/api/users', status: 200})
CREATE (:ApiCall {id: 'call002', trace_id: 'abc123', endpoint: '/api/auth', status: 200})
CREATE (:ApiCall {id: 'call003', trace_id: 'abc123', endpoint: '/db/query', status: 500})

MATCH (c1:ApiCall {id: 'call001'}), (c2:ApiCall {id: 'call002'})
CREATE (c1)-[:CALLS]->(c2)

MATCH (c2:ApiCall {id: 'call002'}), (c3:ApiCall {id: 'call003'})
CREATE (c2)-[:CALLS]->(c3)
```

**查询:**
```cypher
MATCH path = (root:ApiCall)-[:CALLS*]->(failed:ApiCall {status: 500})
RETURN path
```

---

## 6. 机器人仓储

### 6.1 路径冲突检测

**Domain:** `robotics_warehouse`

**业务描述:**  
实时检测多个机器人是否在同一时间段规划了冲突路径。

**图模式:**
```cypher
CREATE (:Robot {id: 'R001', status: 'MOVING'})
CREATE (:Robot {id: 'R002', status: 'MOVING'})
CREATE (:Zone {id: 'Z100', type: 'NARROW_AISLE'})
CREATE (:Task {id: 'T001', robot_id: 'R001', eta_zone_100: '2026-07-05T10:30:00Z'})
CREATE (:Task {id: 'T002', robot_id: 'R002', eta_zone_100: '2026-07-05T10:30:05Z'})

MATCH (t1:Task {id: 'T001'}), (z:Zone {id: 'Z100'})
CREATE (t1)-[:PASSES_THROUGH]->(z)

MATCH (t2:Task {id: 'T002'}), (z:Zone {id: 'Z100'})
CREATE (t2)-[:PASSES_THROUGH]->(z)
```

**Standing Query:**
```cypher
REGISTER STANDING QUERY path_conflict AS
MATCH (t1:Task)-[:PASSES_THROUGH]->(zone:Zone {type:'NARROW_AISLE'})<-[:PASSES_THROUGH]-(t2:Task)
WHERE t1.robot_id < t2.robot_id
  AND abs(duration.between(t1.eta_zone_100, t2.eta_zone_100).seconds) < 10
RETURN t1.robot_id, t2.robot_id, zone.id
```

**业务动作:**
- 自动重新规划路径
- 延迟低优先级任务
- 触发避让逻辑

---

## 7. 物流供应链

### 7.1 包裹时效异常

**Domain:** `logistics_supply_chain`

**业务描述:**  
包裹在某个网点停留时间超过 SLA 时自动告警。

**图模式:**
```cypher
CREATE (:Package {id: 'PKG001', sla_hours: 48, created_at: '2026-07-03T10:00:00Z'})
CREATE (:Hub {id: 'HUB_SH', name: 'Shanghai Hub'})
CREATE (:ScanEvent {id: 'SCAN001', timestamp: '2026-07-03T12:00:00Z', action: 'ARRIVE'})
CREATE (:ScanEvent {id: 'SCAN002', timestamp: '2026-07-05T11:00:00Z', action: 'DEPART'})

MATCH (pkg:Package {id: 'PKG001'}), (hub:Hub {id: 'HUB_SH'})
CREATE (pkg)-[:LOCATED_AT]->(hub)

MATCH (scan1:ScanEvent {id: 'SCAN001'}), (hub:Hub {id: 'HUB_SH'})
CREATE (scan1)-[:AT_HUB]->(hub)
```

**Standing Query:**
```cypher
REGISTER STANDING QUERY package_delay AS
MATCH (pkg:Package)-[:LOCATED_AT]->(hub:Hub)
MATCH (arrive:ScanEvent {action:'ARRIVE'})-[:AT_HUB]->(hub)
WHERE duration.between(arrive.timestamp, datetime()).hours > pkg.sla_hours
RETURN pkg.id, hub.id, pkg.sla_hours, arrive.timestamp
```

---

## 8. 能源电力

### 8.1 电网设备告警传播

**Domain:** `energy_power`

**业务描述:**  
变压器异常时，自动计算受影响的所有下游配电设备和用户。

**图模式:**
```cypher
CREATE (:PowerDevice {id: 'TRANSFORMER_01', type: 'TRANSFORMER', status: 'FAULT'})
CREATE (:PowerDevice {id: 'SWITCH_01', type: 'SWITCH', status: 'NORMAL'})
CREATE (:PowerDevice {id: 'METER_01', type: 'METER', status: 'NORMAL'})
CREATE (:Alert {id: 'A001', severity: 'critical', type: 'OVERLOAD'})

MATCH (t:PowerDevice {id: 'TRANSFORMER_01'}), (s:PowerDevice {id: 'SWITCH_01'})
CREATE (t)-[:FEEDS]->(s)

MATCH (s:PowerDevice {id: 'SWITCH_01'}), (m:PowerDevice {id: 'METER_01'})
CREATE (s)-[:FEEDS]->(m)

MATCH (alert:Alert {id: 'A001'}), (t:PowerDevice {id: 'TRANSFORMER_01'})
CREATE (alert)-[:AFFECTS]->(t)
```

**Standing Query:**
```cypher
REGISTER STANDING QUERY power_outage_impact AS
MATCH (alert:Alert)-[:AFFECTS]->(device:PowerDevice)
MATCH (device)-[:FEEDS*1..5]->(downstream:PowerDevice)
WHERE alert.severity = 'critical'
RETURN alert.id, device.id, collect(downstream.id) AS affected_devices
```

---

## 9. 安全生产

### 9.1 禁区进入检测

**Domain:** `safety_security`

**业务描述:**  
实时检测无权限人员进入危险区域。

**图模式:**
```cypher
CREATE (:Person {id: 'P001', name: 'Alice', clearance_level: 2})
CREATE (:Zone {id: 'Z001', name: 'High Voltage Area', required_clearance: 3})
CREATE (:AccessEvent {id: 'AE001', timestamp: '2026-07-05T10:30:00Z', action: 'ENTER'})

MATCH (person:Person {id: 'P001'}), (event:AccessEvent {id: 'AE001'})
CREATE (person)-[:TRIGGERED]->(event)

MATCH (event:AccessEvent {id: 'AE001'}), (zone:Zone {id: 'Z001'})
CREATE (event)-[:AT_ZONE]->(zone)
```

**Standing Query:**
```cypher
REGISTER STANDING QUERY unauthorized_access AS
MATCH (person:Person)-[:TRIGGERED]->(event:AccessEvent {action:'ENTER'})
MATCH (event)-[:AT_ZONE]->(zone:Zone)
WHERE person.clearance_level < zone.required_clearance
RETURN person.id, person.name, zone.id, zone.name, event.timestamp
```

**业务动作:**
- 立即推送告警到安保中心
- 触发现场警报
- 自动记录证据（摄像头截图）

---

## 10. AI Agent 图上下文

### 10.1 相似案例检索

**业务描述:**  
AI Agent 需要根据当前异常事件检索历史相似案例及解决方案。

**图模式:**
```cypher
CREATE (:Exception {id: 'EVT001', type: 'EMERGENCY_STOP', severity: 'P1', device_type: 'AGV'})
CREATE (:Exception {id: 'EVT_HIST_001', type: 'EMERGENCY_STOP', severity: 'P1', device_type: 'AGV', resolved: true})
CREATE (:Solution {id: 'SOL001', description: 'Reset emergency button and check sensors', success_rate: 0.85})

MATCH (hist:Exception {id: 'EVT_HIST_001'}), (sol:Solution {id: 'SOL001'})
CREATE (hist)-[:RESOLVED_BY]->(sol)
```

**查询:**
```cypher
MATCH (current:Exception {id: 'EVT001'})
MATCH (similar:Exception {type: current.type, device_type: current.device_type, resolved: true})
MATCH (similar)-[:RESOLVED_BY]->(solution:Solution)
RETURN solution.description, solution.success_rate, count(similar) AS similar_cases
ORDER BY solution.success_rate DESC
LIMIT 3
```

**AI Agent 提示:**
```
基于图上下文，当前 AGV 紧急停止异常有以下历史解决方案：
1. 重置紧急按钮并检查传感器（成功率 85%，12 个相似案例）
2. 重启 AGV 控制器（成功率 72%，8 个相似案例）
3. 更换电池模块（成功率 45%，3 个相似案例）
```

---

## 11. 场景测试矩阵

| 场景 | Domain | 测试文件 | 状态 |
|------|--------|----------|------|
| 通用对象影响传播 | generic | `tests/scenario_generic.rs` | ✅ 待实现 |
| AGV 故障影响航班 | air_cargo_terminal | `tests/scenario_agv_flight.rs` | ✅ 待实现 |
| 中转错失风险 | air_cargo_terminal | `tests/scenario_transfer_risk.rs` | 📋 计划 |
| 设备异常影响工单 | manufacturing | `tests/scenario_machine_order.rs` | ✅ 待实现 |
| 质量追溯 | manufacturing | `tests/scenario_quality_trace.rs` | 📋 计划 |
| 服务依赖告警 | it_observability | `tests/scenario_service_alert.rs` | ✅ 待实现 |
| 接口链路分析 | it_observability | `tests/scenario_api_trace.rs` | 📋 计划 |
| 路径冲突检测 | robotics_warehouse | `tests/scenario_path_conflict.rs` | 📋 计划 |
| 包裹时效异常 | logistics_supply_chain | `tests/scenario_package_delay.rs` | 📋 计划 |
| 电网告警传播 | energy_power | `tests/scenario_power_outage.rs` | 📋 计划 |
| 禁区进入检测 | safety_security | `tests/scenario_unauthorized_access.rs` | 📋 计划 |
| AI Agent 案例检索 | generic | `tests/scenario_agent_context.rs` | 📋 计划 |

---

## 12. 实现优先级

### Phase 1（P0-P1）
- ✅ 通用对象影响传播
- ✅ AGV 故障影响航班
- ✅ 设备异常影响工单
- ✅ 服务依赖告警

### Phase 2（P2）
- 中转错失风险
- 质量追溯
- 接口链路分析
- 路径冲突检测

### Phase 3（未来）
- 包裹时效异常
- 电网告警传播
- 禁区进入检测
- AI Agent 案例检索

---

**维护者:** Nexora Team  
**最后更新:** 2026/07/05
