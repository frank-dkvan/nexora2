# Nexora 证据引用系统

**版本:** 1.0  
**日期:** 2026/07/05  
**状态:** 设计完成（P2.2）

---

## 1. 设计原则

**Nexora 不保存大对象原文**（图片/视频/日志/报文/时序数据），只保存 **EvidenceRef（证据引用）**。

### 1.1 为什么不保存原文？

- ❌ 图数据库不适合存储大对象（膨胀快）
- ❌ 混合存储导致备份/恢复复杂
- ✅ 专业系统更擅长存储特定类型数据
- ✅ 解耦存储和查询

---

## 2. 数据结构

```rust
pub struct EvidenceRef {
    /// 证据唯一 ID
    pub evidence_id: String,
    
    /// 存储类型
    pub store_type: EvidenceStoreType,
    
    /// 存储桶/数据库
    pub bucket: String,
    
    /// 条目/路径
    pub entry: String,
    
    /// 时间范围（可选，用于时序数据）
    pub timestamp_start: Option<DateTime<Utc>>,
    pub timestamp_end: Option<DateTime<Utc>>,
    
    /// 标签（用于过滤）
    pub labels: HashMap<String, String>,
    
    /// 访问 URI
    pub uri: Option<String>,
    
    /// 校验和（可选）
    pub checksum: Option<String>,
    
    /// 保留策略（可选）
    pub retention_policy: Option<String>,
    
    /// 来源系统
    pub source_system: Option<String>,
    
    /// 关联 ID
    pub correlation_id: Option<String>,
    
    /// 领域（可选）
    pub domain: Option<String>,
    
    /// 关联对象 ID
    pub object_id: Option<String>,
    
    /// 关联事件 ID
    pub event_id: Option<String>,
}

pub enum EvidenceStoreType {
    ReductStore,   // 时序 Blob 存储
    S3,            // 对象存储
    MinIO,         // 兼容 S3
    OpenSearch,    // 日志/全文搜索
    Loki,          // 日志聚合
    ExternalUrl,   // 外部 URL
    Custom(String),
}
```

---

## 3. 典型场景

### 3.1 航空货站 AGV 异常 + 摄像头证据

```cypher
// 创建异常事件
CREATE (e:Exception {
  id: 'EVT001',
  severity: 'P1',
  type: 'EMERGENCY_STOP',
  timestamp: '2026-07-05T10:30:00Z'
})

// 创建证据引用（ReductStore）
CREATE (ref:EvidenceRef {
  evidence_id: 'REDUCT_001',
  store_type: 'ReductStore',
  bucket: 'iot-equipment',
  entry: 'agv/agv001/camera/front',
  timestamp_start: '2026-07-05T10:29:50Z',
  timestamp_end: '2026-07-05T10:30:10Z',
  labels: {device_id: 'AGV001', sensor: 'camera_front'},
  uri: 'http://reductstore:8383/iot-equipment/agv/agv001/camera/front?start=1720174190&end=1720174210'
})

// 关联
MATCH (e:Exception {id: 'EVT001'}), (ref:EvidenceRef {evidence_id: 'REDUCT_001'})
CREATE (e)-[:HAS_EVIDENCE]->(ref)
```

### 3.2 IT 运维告警 + OpenSearch 日志

```cypher
CREATE (alert:Alert {
  id: 'A001',
  severity: 'critical',
  message: 'DB connection pool exhausted'
})

CREATE (ref:EvidenceRef {
  evidence_id: 'OS_LOG_001',
  store_type: 'OpenSearch',
  bucket: 'application-logs',
  entry: 'user-service',
  timestamp_start: '2026-07-05T10:25:00Z',
  timestamp_end: '2026-07-05T10:30:00Z',
  labels: {service: 'user-service', severity: 'ERROR'},
  uri: 'https://opensearch.example.com/_search?q=service:user-service AND severity:ERROR'
})

MATCH (alert:Alert {id: 'A001'}), (ref:EvidenceRef {evidence_id: 'OS_LOG_001'})
CREATE (alert)-[:HAS_EVIDENCE]->(ref)
```

### 3.3 智能制造设备异常 + S3 图片

```cypher
CREATE (e:Exception {
  id: 'MANU_E001',
  severity: 'P2',
  type: 'DEFECT_DETECTED'
})

CREATE (ref:EvidenceRef {
  evidence_id: 'S3_IMG_001',
  store_type: 'S3',
  bucket: 'quality-inspection',
  entry: 'images/2026-07-05/line-a/defect-12345.jpg',
  labels: {line: 'LINE_A', defect_type: 'scratch'},
  uri: 's3://quality-inspection/images/2026-07-05/line-a/defect-12345.jpg',
  checksum: 'sha256:abcdef123456...'
})

MATCH (e:Exception {id: 'MANU_E001'}), (ref:EvidenceRef {evidence_id: 'S3_IMG_001'})
CREATE (e)-[:HAS_EVIDENCE]->(ref)
```

---

## 4. 查询证据

### 4.1 按事件查询证据

```cypher
MATCH (e:Exception {id: 'EVT001'})-[:HAS_EVIDENCE]->(ref:EvidenceRef)
RETURN ref.store_type, ref.uri, ref.timestamp_start, ref.timestamp_end
```

### 4.2 按时间范围查询证据

```cypher
MATCH (ref:EvidenceRef)
WHERE ref.timestamp_start >= datetime('2026-07-05T10:00:00Z')
  AND ref.timestamp_end <= datetime('2026-07-05T11:00:00Z')
  AND ref.labels.device_id = 'AGV001'
RETURN ref
```

### 4.3 按对象查询证据

```cypher
MATCH (obj:Object {id: 'AGV001'})-[:RELATED_TO]->(e:Exception)-[:HAS_EVIDENCE]->(ref)
WHERE ref.store_type = 'ReductStore'
RETURN ref.uri
```

---

## 5. 证据系统集成

### 5.1 ReductStore 配置

```yaml
evidence_stores:
  - name: reductstore
    type: ReductStore
    endpoint: http://reductstore:8383
    buckets:
      - iot-equipment
      - quality-inspection
    retention: 90d
```

### 5.2 S3 / MinIO 配置

```yaml
evidence_stores:
  - name: s3
    type: S3
    endpoint: https://s3.amazonaws.com
    bucket: nexora-evidence
    credentials:
      access_key_id: ${S3_ACCESS_KEY}
      secret_access_key: ${S3_SECRET_KEY}
```

### 5.3 OpenSearch 配置

```yaml
evidence_stores:
  - name: opensearch
    type: OpenSearch
    endpoint: https://opensearch.example.com:9200
    index_pattern: logs-*
    credentials:
      username: ${OS_USER}
      password: ${OS_PASS}
```

---

## 6. API 示例

### 6.1 HTTP API

```bash
# 创建证据引用
POST /api/v1/evidence-refs
{
  "evidence_id": "REDUCT_001",
  "store_type": "ReductStore",
  "bucket": "iot-equipment",
  "entry": "agv/agv001/camera/front",
  "timestamp_start": "2026-07-05T10:29:50Z",
  "timestamp_end": "2026-07-05T10:30:10Z",
  "object_id": "AGV001",
  "event_id": "EVT001"
}

# 查询证据引用
GET /api/v1/evidence-refs?event_id=EVT001

# 获取证据访问 URL
GET /api/v1/evidence-refs/REDUCT_001/url
→ http://reductstore:8383/iot-equipment/agv/agv001/camera/front?start=...
```

---

## 7. 保留策略

### 7.1 自动清理

```rust
pub struct RetentionPolicy {
    pub max_age_days: u32,
    pub max_count: Option<u32>,
    pub archive_to: Option<String>,  // 冷存储路径
}
```

### 7.2 示例策略

```yaml
retention_policies:
  - name: hot_evidence
    max_age_days: 30
    archive_to: s3://nexora-archive/evidence/
  
  - name: compliance_evidence
    max_age_days: 2555  # 7年
    archive_to: glacier://nexora-compliance/
```

---

## 8. 性能考虑

- **索引** — 按 `event_id`, `object_id`, `timestamp_start` 索引
- **分片** — 按时间范围分片
- **缓存** — URI 生成结果缓存
- **懒加载** — 不自动拉取原文，只返回引用

---

## 9. 未来增强

- 证据版本管理
- 证据链（Chain of Custody）
- 自动证据采集（触发器）
- 证据完整性校验
- 加密证据支持

---

**维护者:** Nexora Team  
**最后更新:** 2026/07/05
