# 阶段3+B线+阶段5 推进总结

**日期**: 2026-07-18（续 17个核心任务之后）
**分支**: feat/metadata-durability-a0-a1-pgwire-dbeaver

## 按用户指定顺序完成的工作

### 阶段3 Stateful Streaming（F线）
| 任务 | 内容 | 状态 |
|------|------|------|
| F1.0 | Fragment Time Travel - MVCC覆盖语义 | ✅ |

- 补齐time_travel.rs，新增3-fragment MVCC测试
- T1→T2→T3版本覆盖，查询任意时刻看到正确版本
- nexora-fragment: 25→29测试

**说明**: F1.1-F1.4（Global Checkpoint的Barrier注入/Shard处理/元数据/恢复）
是4-6周大工程，B2已覆盖offset-aligned checkpoint的核心。F1.0作为
Time Travel基础先行完成。F2/F3/F4/F5为后续。

### Track B TileDB借鉴（B8-B10）
| 任务 | 内容 | 状态 |
|------|------|------|
| B8 | Fragment Consolidation策略+后台触发 | ✅ |
| B9 | VFS抽象（验证StorageBackend已覆盖）| ✅ |
| B10 | Filter Pipeline可插拔压缩/加密链 | ✅ |

- B8: ConsolidationConfig(min_frags/max_frags/size_ratio) + FragmentConsolidator
  - select_fragments选择策略 + consolidate_once + spawn_background
- B9: 确认StorageBackend trait(Local/Memory/S3)已满足VFS需求，无需新建
- B10: Filter trait + FilterPipeline + ZstdFilter/Aes256GcmFilter/ByteShuffleFilter
  - nexora-storage: 19→28测试

### 阶段5运维成熟度（E线）
| 任务 | 内容 | 状态 |
|------|------|------|
| E3 | Failover告警钩子 | ✅ |
| E6 | 更新EXPERIMENTAL横幅 | ✅ |
| E2 | 长稳soak测试框架（可配置时长）| ✅ |

- E3: FailoverEvent + FailoverAlertSink trait + LogAlertSink + emit_alert
  - auto_failover在检测/成功/失败3个转换点emit
  - webhook_payload构造(sink在app层，避免core引入reqwest)
  - nexora-zenoh: 270→275测试
- E6: 横幅从"NO FAULT TOLERANCE"更新为诚实的soak-pending caveat
  - 反映RF>1复制+failover已实现+chaos验证的实际能力
- E2: soak.rs框架，持续负载+故障注入+泄漏检测+read-your-writes不变量
  - 默认3s(CI友好)，NEXORA_SOAK_SECS=259200可跑72h
  - 短时验证: 3s内10万写读0违规

## 测试覆盖
- 全工作区库测试: 1060 passed（累计）
- 全部集成测试编译通过
- 本轮新增测试:
  - F1.0: 1个（MVCC 3-fragment）
  - B8: 4个（consolidator策略）
  - B10: 9个（filter pipeline）
  - E3: 5个（alert钩子）
  - E2: 2个（soak短/长）

## 提交记录（本轮5个commit）
```
8071e87e test(soak): 长稳soak测试框架 - 可配置时长 (E2骨架)
a9572b76 feat(ops): failover告警钩子(E3) + 更新EXPERIMENTAL横幅(E6)
22d2456b feat(fragment): 实现Fragment Consolidation策略+后台触发 (B8)
19ba41ea feat(storage): 实现Filter Pipeline可插拔压缩/加密链 (B10) + 验证VFS (B9)
6d06a3c5 feat(fragment): 完成F1.0 Fragment Time Travel - MVCC覆盖语义
```

## 剩余未做（后续）
- F1.1-F1.4: Global Checkpoint完整实现（Barrier注入/Shard处理/恢复）
- F2: Exactly-Once端到端chaos验证
- F3.1-F3.3: WAL + ReductStore异步复制（PropertyValue::BlobRef）
- F4: Watermark + 事件时间窗口
- E4: 滚动升级
- E5: 运维runbook
- E7: 安全加固（审计/密钥轮转/多租户）
- C2-C6: 集群扩展（反脑裂验证/扩缩容/背压/容量基线）
- 真实72h soak运行（框架已就绪，需实际执行）
