# Nexora 可观测性

**版本:** 1.0  
**日期:** 2026/07/05  
**状态:** 基础完整，增强中（P5）

---

## 1. 三大支柱

### 1.1 Metrics（指标）

- **收集器:** Prometheus
- **暴露端点:** `GET /metrics`
- **格式:** Prometheus exposition format

### 1.2 Logs（日志）

- **框架:** tracing + tracing-subscriber
- **格式:** JSON structured logs
- **聚合:** Loki / OpenSearch

### 1.3 Traces（追踪）

- **标准:** OpenTelemetry
- **后端:** Jaeger / Tempo
- **采样率:** 可配置（生产建议 1%-10%）

---

## 2. Metrics 指标

### 2.1 图规模指标

```prometheus
# 节点总数
nexora_nodes_total{namespace="airport_cargo",tenant="customer_a"} 125000

# 边总数
nexora_edges_total{edge_type="DEPENDS_ON"} 450000

# 标签分布
nexora_nodes_by_label{label="Device"} 5000
nexora_nodes_by_label{label="Task"} 3000
```

### 2.2 性能指标

```prometheus
# 查询延迟
nexora_query_duration_seconds{operation="cypher_read",percentile="p50"} 0.045
nexora_query_duration_seconds{operation="cypher_read",percentile="p95"} 0.230
nexora_query_duration_seconds{operation="cypher_read",percentile="p99"} 0.580

# Mutation 吞吐量
nexora_mutation_throughput_ops{operation="create_node"} 1250

# WAL 延迟
nexora_wal_append_duration_seconds{percentile="p99"} 0.002
```

### 2.3 Standing Query 指标

```prometheus
# SQ 评估次数
nexora_standing_query_evaluations_total{query_id="agv_flight_impact"} 45230

# SQ 命中次数
nexora_standing_query_matches_total{query_id="agv_flight_impact"} 127

# SQ 评估延迟
nexora_sq_evaluation_duration_seconds{query_id="impact_propagation",percentile="p95"} 0.085
```

### 2.4 资源指标

```prometheus
# Actor 数量
nexora_actor_count{shard="0"} 85000

# 内存使用
nexora_memory_usage_bytes{component="graph_service"} 2147483648

# RocksDB 指标
nexora_rocksdb_read_bytes_total 5368709120
nexora_rocksdb_write_bytes_total 1073741824
```

---

## 3. Structured Logging

### 3.1 日志格式

```json
{
  "timestamp": "2026-07-05T10:30:15.123456Z",
  "level": "INFO",
  "target": "nexora_core::graph",
  "message": "Node created",
  "fields": {
    "node_id": "550e8400-e29b-41d4-a716-446655440000",
    "labels": ["Device", "AGV"],
    "namespace": "airport_cargo",
    "tenant_id": "customer_a",
    "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
    "correlation_id": "evt-12345"
  }
}
```

### 3.2 日志级别

- `ERROR` — 错误（需要告警）
- `WARN` — 警告（需要关注）
- `INFO` — 关键操作（节点创建、SQ 命中）
- `DEBUG` — 调试信息（查询计划）
- `TRACE` — 详细追踪（性能分析）

### 3.3 关键日志事件

```json
// Standing Query 命中
{
  "level": "INFO",
  "event": "standing_query_match",
  "query_id": "agv_flight_impact",
  "matched_at": "2026-07-05T10:30:15.120Z",
  "bindings": {"e.id": "EVT001", "flight.id": "LH729"},
  "is_recovery": false
}

// WAL Replay 进度
{
  "level": "INFO",
  "event": "wal_replay_progress",
  "replayed_entries": 125000,
  "total_entries": 500000,
  "progress_percent": 25.0
}

// 崩溃恢复完成
{
  "level": "INFO",
  "event": "recovery_completed",
  "duration_ms": 12500,
  "replayed_entries": 500000
}
```

---

## 4. Distributed Tracing

### 4.1 Trace Context 传播

```rust
pub struct RequestContext {
    pub trace_id: String,          // OpenTelemetry Trace ID
    pub span_id: String,            // 当前 Span ID
    pub correlation_id: String,     // 业务关联 ID
    pub mutation_id: Option<String>, // Mutation 幂等 ID
}
```

### 4.2 Span 示例

```
Trace: 4bf92f3577b34da6a3ce929d0e0e4736
├─ Span: POST /api/v1/graph/nodes [100ms]
│  ├─ Span: WAL.append [2ms]
│  ├─ Span: NodeTask.apply [5ms]
│  └─ Span: StandingQueryManager.evaluate [80ms]
│     ├─ Span: evaluate_query:agv_flight_impact [40ms]
│     └─ Span: push_match:kafka [35ms]
```

### 4.3 集成示例

```rust
use tracing::{info_span, instrument};

#[instrument(skip(self), fields(node_id = %id))]
pub async fn create_node(&self, id: NexoraId, labels: Vec<Symbol>) -> Result<()> {
    let span = info_span!("create_node", node_id = %id);
    let _enter = span.enter();
    
    // 业务逻辑
    self.wal.append(GraphMutation::NodeCreated { id, labels }).await?;
    
    Ok(())
}
```

---

## 5. Health Checks

### 5.1 健康检查端点

```bash
# 基础健康检查
GET /health
→ 200 OK {"status": "healthy"}

# 就绪检查（K8s readinessProbe）
GET /ready
→ 200 OK {"ready": true, "wal_replay_completed": true}

# 存活检查（K8s livenessProbe）
GET /live
→ 200 OK {"alive": true, "actor_system_responsive": true}
```

### 5.2 详细状态

```bash
GET /debug/status
```

```json
{
  "version": "0.1.0",
  "uptime_seconds": 86400,
  "nodes_count": 125000,
  "edges_count": 450000,
  "standing_queries_count": 12,
  "actor_count": 85000,
  "wal": {
    "current_offset": 5000000,
    "disk_usage_mb": 2048
  },
  "rocksdb": {
    "size_mb": 8192,
    "compaction_pending": false
  }
}
```

---

## 6. 告警规则（Prometheus）

### 6.1 高延迟告警

```yaml
groups:
  - name: nexora_performance
    rules:
      - alert: HighQueryLatency
        expr: nexora_query_duration_seconds{percentile="p95"} > 1.0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "查询延迟过高"
          description: "P95 延迟 {{ $value }}s 超过 1s"
```

### 6.2 资源告警

```yaml
- alert: HighMemoryUsage
  expr: nexora_memory_usage_bytes > 8589934592  # 8GB
  for: 10m
  labels:
    severity: critical
  annotations:
    summary: "内存使用过高"
```

### 6.3 Standing Query 告警

```yaml
- alert: StandingQueryEvaluationFailed
  expr: rate(nexora_standing_query_errors_total[5m]) > 0.1
  labels:
    severity: critical
  annotations:
    summary: "Standing Query 评估失败率过高"
```

---

## 7. Grafana Dashboard

### 7.1 概览仪表板

```
┌─────────────────────────────────────────────────────────────┐
│  Nexora Overview Dashboard                                  │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ Nodes    │  │ Edges    │  │ Queries/s│  │ Actors   │   │
│  │ 125k     │  │ 450k     │  │ 85       │  │ 85k      │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
├─────────────────────────────────────────────────────────────┤
│  Query Latency (P95)         │  Mutation Throughput         │
│  [Line Chart]                │  [Line Chart]                │
├─────────────────────────────────────────────────────────────┤
│  Standing Query Matches      │  WAL Append Rate             │
│  [Bar Chart]                 │  [Line Chart]                │
└─────────────────────────────────────────────────────────────┘
```

### 7.2 示例 Panel 配置

```json
{
  "title": "Query Latency P95",
  "targets": [
    {
      "expr": "nexora_query_duration_seconds{operation='cypher_read',percentile='p95'}",
      "legendFormat": "P95 Latency"
    }
  ],
  "yaxis": {
    "label": "Latency (seconds)",
    "format": "s"
  }
}
```

---

## 8. 配置示例

### 8.1 nexora.yaml

```yaml
observability:
  metrics:
    enabled: true
    endpoint: /metrics
    
  tracing:
    enabled: true
    exporter: otlp
    endpoint: http://jaeger:4317
    sampling_rate: 0.1
  
  logging:
    level: INFO
    format: json
    outputs:
      - type: stdout
      - type: file
        path: /var/log/nexora/app.log
      - type: loki
        endpoint: http://loki:3100
```

---

## 9. 运维脚本

### 9.1 指标采集脚本

```bash
#!/bin/bash
# scripts/collect_metrics.sh

curl -s http://localhost:8080/metrics | \
  grep -E '^nexora_' | \
  promtool check metrics
```

### 9.2 日志查询示例

```bash
# 查询最近 5 分钟的错误日志
curl -G http://loki:3100/loki/api/v1/query_range \
  --data-urlencode 'query={job="nexora"} |= "ERROR"' \
  --data-urlencode 'start='$(date -u -d '5 minutes ago' +%s)000000000 \
  --data-urlencode 'end='$(date -u +%s)000000000
```

---

**维护者:** Nexora Team  
**最后更新:** 2026/07/05
