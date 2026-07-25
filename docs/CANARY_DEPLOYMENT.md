# Nexora 灰度发布方案 (3节点集群)

## 目标

在生产环境部署 Nexora 分布式图数据库，通过灰度发布验证系统稳定性，为全量上线积累经验。

## 硬件配置

### 集群规模
- **节点数**: 3 节点 (满足 Raft 法定人数要求)
- **部署模式**: 单数据中心，同机房跨机架部署

### 单节点配置
| 组件 | 规格 | 说明 |
|------|------|------|
| CPU | 8 核 (Intel Xeon or AMD EPYC) | 推荐: 关闭超线程,获得可预测延迟 |
| 内存 | 32 GB | 20GB 分配给 Nexora,12GB 系统保留 |
| 存储 | 500 GB NVMe SSD | RocksDB 数据盘,IOPS > 10k |
| 网络 | 10 Gbps | 节点间 RTT < 1ms |
| 操作系统 | Ubuntu 22.04 LTS | Kernel 5.15+ |

### 网络拓扑
```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Node-A    │────▶│   Node-B    │────▶│   Node-C    │
│  (Leader)   │◀────│ (Follower)  │◀────│ (Follower)  │
└─────────────┘     └─────────────┘     └─────────────┘
     │                    │                    │
     └────────────────────┴────────────────────┘
              内网负载均衡器 (Keepalived VIP)
                       │
                  PG-wire 客户端
```

## 流量接入策略

### 阶段 1: 内部验证 (Week 1)
- **流量来源**: 开发团队内部工具
- **QPS**: 10-50 (读写比 8:2)
- **数据规模**: 10万节点,50万边
- **目标**: 验证基本功能、监控告警

### 阶段 2: 小流量灰度 (Week 2)
- **流量来源**: 单个非关键业务 (如用户关系推荐)
- **QPS**: 100-200
- **数据规模**: 100万节点,500万边
- **流量切分**: 通过 HAProxy 路由 5% 流量到 Nexora,95% 到旧系统
- **双写验证**: 同时写入 Nexora 和旧系统,对比结果一致性

### 阶段 3: 中流量观察 (Week 3-4)
- **流量来源**: 2-3 个业务线
- **QPS**: 500-1000
- **数据规模**: 500万节点,2500万边
- **流量切分**: 20% 到 Nexora
- **性能对比**: P99 延迟、吞吐量 vs 旧系统

### 阶段 4: 全量迁移决策 (Week 5+)
- 根据 2 周观察结果决定是否继续扩大或回滚

## 监控告警规则

### Prometheus 告警 (AlertManager)

```yaml
groups:
- name: nexora_critical
  interval: 30s
  rules:
  # 复制健康率过低
  - alert: ReplicationHealthLow
    expr: nexora_replication_health_ratio < 0.9
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "Nexora 复制健康率低于 90%"
      description: "{{ $labels.instance }} 复制失败率过高,可能影响数据可用性"

  # Quorum 写入失败
  - alert: QuorumWriteFailed
    expr: rate(nexora_replication_quorum_failed_total[5m]) > 1
    for: 3m
    labels:
      severity: critical
    annotations:
      summary: "Nexora Quorum 写入持续失败"

  # 节点不可达
  - alert: NodeDown
    expr: up{job="nexora"} == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "Nexora 节点宕机"
      description: "节点 {{ $labels.instance }} 无响应"

  # 查询延迟过高
  - alert: QueryLatencyHigh
    expr: histogram_quantile(0.99, rate(nexora_query_duration_seconds_bucket[5m])) > 0.5
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Nexora P99 查询延迟超过 500ms"

  # 内存使用过高
  - alert: MemoryUsageHigh
    expr: process_resident_memory_bytes{job="nexora"} / 1024 / 1024 / 1024 > 25
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Nexora 内存使用超过 25GB"
```

### 日志监控 (ELK Stack)

**关键错误模式**:
```
ERROR.*split.brain
ERROR.*data.corruption
ERROR.*fencing.rejected.*stale.epoch
FATAL.*panic
ERROR.*out.of.memory
```

## 2周观察指标

### 可用性指标
- [ ] **SLA**: 99.9% (月故障时间 < 43 分钟)
- [ ] **故障切换时间**: < 10 秒 (Raft 选举 + 路由更新)
- [ ] **数据零丢失**: 所有 quorum 提交的写入在故障后可恢复

### 性能指标
| 指标 | 目标值 | 实际值 | 对比旧系统 |
|------|-------|-------|-----------|
| 写入 P99 延迟 | < 50ms | ___ | ___ |
| 读取 P99 延迟 | < 30ms | ___ | ___ |
| 吞吐量 (QPS) | 1000+ | ___ | ___ |
| CPU 使用率 | < 70% | ___ | ___ |
| 内存使用率 | < 80% | ___ | ___ |
| 磁盘 IOPS | < 5000 | ___ | ___ |

### 正确性指标
- [ ] **双写一致性**: Nexora vs 旧系统结果一致率 > 99.99%
- [ ] **事务完整性**: 所有 2PC 事务要么全部提交,要么全部回滚
- [ ] **线性一致性**: Quorum 读看到最新 quorum 写
- [ ] **脑裂防护**: 无 fencing rejection 导致的数据污染

### 运维指标
- [ ] **误报告警率**: < 5%
- [ ] **平均故障恢复时间 (MTTR)**: < 30 分钟
- [ ] **慢查询比例**: < 0.1%

## 回滚触发条件

**立即回滚**:
- 数据不一致错误 (双写对比失败率 > 0.01%)
- 节点频繁宕机 (24h 内 > 3 次)
- P99 延迟 > 1s 持续 10 分钟
- 复制健康率 < 0.7 持续 5 分钟

**计划回滚** (24h 内):
- SLA 低于 99.5%
- 慢查询比例 > 1%
- 运维团队投票决定

## 回滚流程

1. **停止新流量**: HAProxy 切换 100% 流量到旧系统
2. **数据补偿**: 如果 Nexora 有旧系统缺失的写入,通过双写日志补齐
3. **保留集群**: 保持 Nexora 集群运行 7 天用于调查,然后下线
4. **事后复盘**: 48h 内完成 RCA (Root Cause Analysis)

## 成功标准

满足以下条件视为灰度成功,可进入全量迁移:

- [ ] 2 周内无 P0 故障
- [ ] 所有可用性指标达标
- [ ] 所有性能指标达标或优于旧系统
- [ ] 无数据不一致事件
- [ ] 运维团队认可可维护性

## 应急联系

| 角色 | 联系方式 | 响应时间 |
|------|---------|---------|
| 值班 SRE | on-call@company.com | 5 分钟 |
| 数据库 DBA | dba@company.com | 15 分钟 |
| 开发负责人 | dev-lead@company.com | 30 分钟 |
| 架构师 | architect@company.com | 1 小时 |

## 检查清单

### 上线前 (D-1)
- [ ] 硬件资源就位并压测通过
- [ ] 监控告警规则配置完成
- [ ] 双写逻辑上线并验证
- [ ] HAProxy 流量切分规则测试
- [ ] 备份恢复流程演练
- [ ] 滚动升级 Runbook 就绪
- [ ] 应急响应团队待命

### 上线后 (D+1)
- [ ] 检查所有节点健康
- [ ] 验证监控数据上报
- [ ] 执行冒烟测试 (CRUD 操作)
- [ ] 确认告警通道畅通

### 每日检查 (D+1 ~ D+14)
- [ ] 查看 Grafana 面板
- [ ] 检查错误日志
- [ ] 对比双写一致性
- [ ] 记录性能数据
- [ ] 更新灰度日报

---

**文档版本**: v1.0  
**最后更新**: 2026-07-15  
**负责人**: SRE Team
