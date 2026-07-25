# Nexora 事件摄取系统

**版本:** 1.0  
**日期:** 2026/07/05  
**状态:** Kafka/Zenoh 基础完整，标准化待增强（P3）

---

## 1. 概述

Nexora 支持从多种事件源实时摄取数据并更新图状态。

---

## 2. 支持的事件源

| 事件源 | 状态 | 用途 |
|--------|------|------|
| **Kafka** | ✅ 完整 | 企业事件总线 |
| **Zenoh** | ✅ 完整 | 分布式实时通信 |
| **HTTP** | ✅ 完整 | Webhook / Batch 导入 |
| **MQTT** | 📋 计划 | IoT 设备 |
| **CDC** | 📋 计划 | 数据库变更捕获 |

---

## 3. 事件映射配置

### 3.1 通用对象状态事件

```yaml
domain: generic
source: object_status
event_type: OBJECT_STATUS_CHANGED
node:
  label: $.object_type
  id: $.object_id
properties:
  status: $.status
  location: $.location
  updated_at: $.timestamp
edges:
  - type: DEPENDS_ON
    target_label: $.dependency_type
    target_id: $.dependency_id
```

### 3.2 航空货站 AGV 状态

```yaml
domain: air_cargo_terminal
source: agv_status
event_type: AGV_STATUS_CHANGED
node:
  label: Device
  id: $.device_id
properties:
  type: AGV
  status: $.status
  battery: $.battery
  location: $.location
edges:
  - type: EXECUTING
    target_label: Task
    target_id: $.task_id
```

### 3.3 智能制造设备状态

```yaml
domain: manufacturing
source: machine_status
event_type: MACHINE_STATUS_CHANGED
node:
  label: Machine
  id: $.machine_id
properties:
  status: $.status
  temperature: $.temperature
  speed: $.rpm
edges:
  - type: PART_OF
    target_label: ProductionLine
    target_id: $.line_id
```

---

## 4. 核心能力

### 4.1 幂等性

```rust
pub struct EventRequest {
    pub request_id: String,        // 幂等 ID
    pub correlation_id: String,    // 业务关联 ID
    pub source_system: String,
    pub event_time: DateTime<Utc>,
    pub payload: serde_json::Value,
}
```

### 4.2 Dead Letter Queue

失败事件自动进入 DLQ，可重放或告警。

### 4.3 Schema 验证

```rust
pub struct EventSchema {
    pub version: String,
    pub fields: Vec<FieldDef>,
}

pub struct FieldDef {
    pub name: String,
    pub field_type: String,  // string | integer | datetime
    pub required: bool,
}
```

---

## 5. 示例 Kafka 配置

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topics:
    - name: "device-status"
      group_id: "nexora-consumer"
      mapping: "configs/mappings/device_status.yaml"
    - name: "exceptions"
      group_id: "nexora-consumer"
      mapping: "configs/mappings/exceptions.yaml"
```

---

## 6. 性能指标

```prometheus
# 摄取速率
nexora_ingest_events_total{source="kafka",topic="device-status"}

# 摄取延迟
nexora_ingest_lag_seconds{source="kafka"}

# DLQ 数量
nexora_ingest_dlq_total{source="kafka",reason="parse_error"}
```

---

**维护者:** Nexora Team  
**最后更新:** 2026/07/05
