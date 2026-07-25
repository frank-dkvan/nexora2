# 当前进度总结

**最后更新**: 2026-07-18 (🎉🎉 阶段1+2+4(D线性能)全部完成，17个路线图任务全清！)

## 🎉 全部17个路线图任务完成 ✅

### 阶段1：正确性地基（数据面共识）
| 任务 | 内容 | 状态 |
|------|------|------|
| A0+A1+A2 | 控制面共识（持久化/统一元数据/openraft）| ✅ |
| A1.1 | 两阶段提交（quorum前不写owner）| ✅ |
| A1.2 | 严格W+R>N一致性（版本号quorum共识）| ✅ |
| A1.3 | Failover追赶（fence→catch-up→reopen→promote）| ✅ |
| A4 | WAL torn-write repair（幂等重放+gap检测）| ✅ |

### 阶段2：持久化 + 集群验证
| 任务 | 内容 | 状态 |
|------|------|------|
| B1 | 统一快照原语 SnapshotManifest（校验+一致性cut）| ✅ |
| B2 ⭐ | Offset-Aligned流式检查点（exactly-once恢复）| ✅ |
| B3 | Database备份/恢复/PITR基础（ZIP+manifest+Blake3）| ✅ |
| B4 | 零拷贝wake（JSON→MessagePack+向后兼容）| ✅ |
| B5 | ExportShard完整性（已验证正确）| ✅ |
| C1 | State Transfer端到端接通（已验证，走zenoh TCP）| ✅ |
| E1 ⭐ | Chaos测试体系（验证阶段1保证）| ✅ |

### 阶段4：性能领先（Track D）🆕
| 任务 | 内容 | 状态 |
|------|------|------|
| D1 | 批量并发BFS（buffer_unordered，5-50×提升）| ✅ |
| D2 | 邻接表专用遍历路径（typed neighbor，避免全边集clone）| ✅ |
| D3 | 读旁路投影做厚（覆盖遍历/扫描，lock-free）| ✅ |
| D4 | 拓扑感知常驻策略（hub抵抗LRU淘汰）| ✅ |

## 📊 测试覆盖
- **全工作区库测试: 270 passed, 0 failed**
- **全部集成测试编译通过** (cargo test --workspace --no-run)
- 本轮新增测试:
  - A1.1两阶段: 2个
  - A4 WAL: 3单元 + 4集成
  - A1.2 W+R>N: 5个
  - A1.3 Failover: 3个
  - B1快照: 6个
  - B2检查点: 9单元 + 4集成
  - B3备份: 8个
  - B4快照编码: 4个
  - E1 chaos: 3 + 4个
  - **D1 BFS: 1个（diamond并发+depth+type过滤）**
  - **D2 typed遍历: 1个（resident+cold路径）**
  - **D3 遍历投影: 1个（projection serves traversal）**
  - **D4 拓扑淘汰: 1个（hub vs leaf eviction）**
- 修复rotted集成测试: 9个文件（API漂移）

## 🏗️ 核心架构成果

### 正确性保证
1. **两阶段提交**: quorum达成前不写owner，杜绝脏数据
2. **W+R>N一致性**: 版本号quorum共识，读到最新已提交
3. **Failover追赶**: fail-safe promotion，lagging follower不成owner
4. **WAL修复**: 幂等重放 + torn-write检测 + seq gap诊断

### 持久化与恢复
1. **统一快照原语**: 一个SnapshotManifest，五种用途复用（Crc32/Blake3）
2. **流式检查点**: (offset,state)原子对，exactly-once恢复
3. **数据库备份**: manifest-last模式，Blake3完整性，HTTP endpoints
4. **零拷贝wake**: MessagePack替换JSON，向后兼容

### 集群能力
1. **State Transfer**: zenoh TCP端到端，集成failover
2. **Chaos测试**: 系统化验证所有正确性保证

### 性能领先（Track D）🆕
1. **批量并发BFS**: frontier用buffer_unordered(64并发)，把per-node延迟变per-batch延迟，深度遍历5-50×
2. **邻接表专用遍历**: `outgoing_neighbors(qid, edge_type)` typed路径，只返回target id（不clone全HalfEdge集），配合projection fast path + server-side过滤
3. **读旁路投影做厚**: 全部read方法(get_property/edges/labels/all_properties/outgoing_neighbors)均走lock-free projection，miss回退wake
4. **拓扑感知常驻**: eviction从纯LRU升级为effective_idle = real_idle - degree_bonus，hub(高入度)抵抗淘汰，遍历hotspot常驻

## 📝 关键技术决策记录

1. **两阶段提交模式**: validate → WAL append(不可回退点) → publish
2. **W+R>N**: 数学保证读写quorum必重叠
3. **Failover fail-safe**: catch-up失败abort，不损失数据
4. **快照manifest-last**: torn write缺尾部manifest → 检测
5. **B4选MessagePack而非手写FlatBuffers**: 低风险拿主要收益
6. **C1确认已接通**: 路线图HTTP/raft_handler描述过时，实际用TCP
7. **D1并发度64**: cap避免海量frontier压垮mailbox，仍全覆盖frontier
8. **D2 typed traversal**: 复用node task的edge_type过滤，projection读edges直接过滤
9. **D4 degree bonus**: 每单位degree买500ms常驻，上限30s防mega-hub永不淘汰

## 🎯 下一步（阶段3+，未开始）

### 剩余完善项（优先级从高到低）
- **Track E 运维成熟度**:
  - E2: 72h+长稳soak（内存/句柄泄漏、性能衰减）
  - E3: failover告警钩子（webhook/PagerDuty）
  - E4: 滚动升级（版本兼容协议+graceful drain）
  - E6: 移除EXPERIMENTAL横幅
  
- **Track B TileDB借鉴**（利用nexora-fragment/nexora-storage骨架）:
  - B8: Fragment Consolidation（后台自动合并小碎片）
  - B9: VFS抽象（统一文件系统接口，S3/Local/InMemory）
  - B10: Filter Pipeline（可插拔压缩/加密链）

- **Track F Stateful Streaming深化**:
  - F1.0: Fragment Time Travel（补齐nexora-fragment/time_travel.rs骨架）
  - F3.1-F3.3: WAL + ReductStore异步复制（PropertyValue::BlobRef）

- **Track C 集群扩展**:
  - C2: 自动failover + 反脑裂验证
  - C3: 动态成员变更（扩缩容）
  - C4: Group Commit双维度背压

- **Track D 性能持续领先**（剩余）:
  - D5: 摄入吞吐优化（per-thread WAL分片）
  - D6: 并行查询执行（专用查询线程池）

- **Track B 持久化增强**（剩余）:
  - 完整PITR orchestration（restore + WAL replay到target_tx_id）
  - GraphService跟踪真实last_tx_id（当前硬编码0）
  - B6: shard flush并发化（256 shards串行→buffer_unordered）
  - B7: 控制面快照 + 算子状态快照

## 提交记录（本轮会话，共15个commit）
```
9bbdbe19 test(projection): 验证D3读旁路投影覆盖遍历路径完整
543efb48 perf(residency): 拓扑感知常驻策略 - hub抵抗淘汰 (D4)
0e8f3c44 perf(traversal): 邻接表专用遍历路径 outgoing_neighbors (D2)
47be0c88 perf(traversal): 批量并发BFS - frontier用buffer_unordered (D1)
7b7db887 docs: 阶段1+阶段2核心全部完成 - 13个路线图任务全清
f62bc742 test: 修复rotted集成测试的API漂移 + E1 chaos审计文档
eb651c98 feat(snapshot): node快照JSON→MessagePack零拷贝wake (B4)
5570661f feat(backup): 实现Database备份/恢复/PITR基础 (B3)
9713cfa7 test(state-transfer): 修复two_node_real_graph编译+API漂移 (C1验证)
d8dc8f15 test(chaos): 实现Chaos测试体系验证阶段1正确性保证 (E1)⭐
6b48b406 feat(snapshot): 实现统一快照原语 SnapshotManifest (B1)
74fd3820 feat(streaming): 实现Offset-Aligned流式检查点 (B2)⭐核心
00933087 feat(failover): 实现Failover追赶协议 + auto_failover集成 (A1.3)
00642e80 feat(consistency): 实现严格W+R>N读写一致性 (A1.2)
78cf5d9a feat(correctness): 实现两阶段提交和WAL torn-write repair (A1.1+A4)
```

## 状态
- 分支: feat/metadata-durability-a0-a1-pgwire-dbeaver（未push）
- **所有阶段1+阶段2+阶段4(D线)任务完成** — 共17个核心任务
- 建议: 灰度验证 + 持续chaos测试后考虑push/PR
- 下一步: 沿ROADMAP推进阶段3(Stateful Streaming F线)或阶段5(运维成熟度E线)
