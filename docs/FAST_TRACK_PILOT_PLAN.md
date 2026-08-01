# Nexora 2.0 快速闭环试点计划

> **目标**: 3周内完成核心能力验证，进入应用试点测试阶段
> 
> **创建日期**: 2026-08-01  
> **状态**: 执行中

---

## 🎯 总体目标

完成以下核心能力的验证和集成：

1. ✅ **Event Streaming** - SQL流处理（基于RisingWave）
2. ✅ **Graph Streaming** - 事件到图的投影
3. ✅ **Iceberg历史数据库** - 事件存储和OLAP查询
4. ✅ **单节点版本** - 开发测试用
5. ✅ **多节点分布式版本** - 生产就绪
6. ✅ **应用试点** - 真实场景验证

---

## 📊 当前状态评估

### 已完成功能（可立即使用）

| 能力 | 实现状态 | 测试覆盖 | 生产就绪 |
|------|---------|---------|---------|
| **nexora-core** (图引擎) | ✅ 完整 | 高 | ✅ 是 |
| **nexora-eventlog** (Iceberg) | ✅ 完整 | 高 | ✅ 是 |
| **nexora-cypher** (查询) | ✅ 完整 | 高 | ✅ 是 |
| **nexora-stream** (连接器) | ✅ 完整 | 中 | ✅ 是 |
| **nexora-risingwave** (SQL流) | ✅ 完整 | 中 | 🟡 实验性 |
| **nexora-consensus** (Raft) | ✅ 完整 | 高 | 🟡 实验性 |
| **nexora-app** (API服务) | ✅ 完整 | 高 | ✅ 是 |

### 待验证部分

- 🔍 端到端完整链路验证
- 🔍 多节点集群长时间稳定性
- 🔍 真实负载性能测试
- 🔍 故障恢复和高可用验证

---

## 📅 3周详细计划

## Week 1: 端到端验证 + 单节点完善

### Day 1-2: 验证Event Streaming完整链路

**目标**: 确保Path A和Path B都能正常工作

#### 任务清单

- [ ] **Task 1.1**: 运行快速验证脚本
  ```bash
  ./scripts/quick-validation.sh
  ```
  - 验证编译通过
  - 验证核心测试通过
  - 验证HTTP API正常

- [ ] **Task 1.2**: 测试Path A（简单直连）
  ```bash
  # 启动单节点
  cargo run --release --features event-first
  
  # 测试Cypher写入
  curl -X POST http://localhost:8080/api/query/cypher \
    -d '{"query": "CREATE (n:User {name: \"test\"}) RETURN n"}'
  
  # 验证图查询
  curl -X POST http://localhost:8080/api/query/cypher \
    -d '{"query": "MATCH (n:User) RETURN n"}'
  ```

- [ ] **Task 1.3**: 测试Path B（RisingWave SQL）
  ```bash
  # 启动带RisingWave的单节点
  cargo run --release --features event-first,event-streaming
  
  # 创建RisingWave Source
  curl -X POST http://localhost:8080/api/risingwave/sql \
    -d '{"sql": "CREATE SOURCE events (data JSONB) WITH (connector = '\''kafka'\'', topic = '\''nexora-events'\'')"}'
  
  # 创建Materialized View
  curl -X POST http://localhost:8080/api/risingwave/sql \
    -d '{"sql": "CREATE MATERIALIZED VIEW user_counts AS SELECT data->>'\''user_id'\'' as user_id, COUNT(*) FROM events GROUP BY user_id"}'
  ```

- [ ] **Task 1.4**: 验证数据一致性
  - 同时向Path A和Path B写入相同数据
  - 验证两个路径的数据一致
  - 检查Iceberg中的事件记录

**产出**:
- ✅ 测试报告 (docs/reports/week1-path-validation.md)
- ✅ 问题清单 (docs/issues/week1-issues.md)
- ✅ 修复记录 (docs/fixes/week1-fixes.md)

---

### Day 3-4: 验证Iceberg查询能力

**目标**: 确保OLAP查询和时间旅行功能正常

#### 任务清单

- [ ] **Task 2.1**: 写入测试数据
  ```bash
  # 批量写入10万条事件
  for i in {1..100000}; do
    curl -X POST http://localhost:8080/api/ingest/event \
      -d "{\"type\": \"click\", \"user_id\": $((i % 1000)), \"ts\": $(date +%s)}"
  done
  ```

- [ ] **Task 2.2**: 测试OLAP聚合查询
  ```sql
  -- 用户行为统计
  SELECT user_id, COUNT(*) as event_count, 
         MIN(timestamp) as first_seen, MAX(timestamp) as last_seen
  FROM events
  GROUP BY user_id
  ORDER BY event_count DESC
  LIMIT 100
  ```

- [ ] **Task 2.3**: 测试时间旅行查询
  ```sql
  -- 查询1小时前的数据快照
  SELECT * FROM events
  FOR SYSTEM_TIME AS OF TIMESTAMP '2026-08-01 10:00:00'
  ```

- [ ] **Task 2.4**: 测试数据导出
  ```bash
  # 导出Parquet格式
  curl -X POST http://localhost:8080/api/eventlog/export \
    -d '{"format": "parquet", "path": "/tmp/export.parquet"}'
  
  # 导出CSV格式
  curl -X POST http://localhost:8080/api/eventlog/export \
    -d '{"format": "csv", "path": "/tmp/export.csv"}'
  ```

**产出**:
- ✅ OLAP查询性能报告
- ✅ 数据导出工具验证
- ✅ 查询优化建议

---

### Day 5: 创建集成测试套件

**目标**: 自动化端到端测试流程

#### 任务清单

- [ ] **Task 3.1**: 运行端到端测试
  ```bash
  ./scripts/test-e2e-full-pipeline.sh
  ```

- [ ] **Task 3.2**: 添加更多测试场景
  - 并发写入测试
  - 大数据量测试（>100万条记录）
  - 复杂Cypher查询测试
  - SQL JOIN测试

- [ ] **Task 3.3**: 集成到CI/CD
  - 更新.github/workflows/ci.yml
  - 添加端到端测试job
  - 设置性能基准阈值

**产出**:
- ✅ 自动化测试套件
- ✅ CI/CD配置更新
- ✅ 测试覆盖报告

**Week 1 里程碑**: ✅ 单节点所有核心能力验证完成

---

## Week 2: 多节点集群验证

### Day 1-2: 启动和验证3节点集群

**目标**: 3节点分布式集群正常运行

#### 任务清单

- [ ] **Task 4.1**: 启动3节点集群
  ```bash
  # 方案A: 使用脚本启动
  ./scripts/start-cluster-3nodes-risingwave.sh
  
  # 方案B: 手动启动（调试用）
  # Node 1
  cargo run --release --features event-first,event-streaming -- \
    --port 8080 --raft-addr 127.0.0.1:5001 --peers 127.0.0.1:5002,127.0.0.1:5003
  
  # Node 2
  cargo run --release --features event-first,event-streaming -- \
    --port 8081 --raft-addr 127.0.0.1:5002 --peers 127.0.0.1:5001,127.0.0.1:5003
  
  # Node 3
  cargo run --release --features event-first,event-streaming -- \
    --port 8082 --raft-addr 127.0.0.1:5003 --peers 127.0.0.1:5001,127.0.0.1:5002
  ```

- [ ] **Task 4.2**: 验证集群状态
  ```bash
  # 检查每个节点状态
  curl http://localhost:8080/api/cluster/status | jq
  curl http://localhost:8081/api/cluster/status | jq
  curl http://localhost:8082/api/cluster/status | jq
  
  # 验证Raft leader选举
  curl http://localhost:8080/api/raft/leader | jq
  
  # 验证节点成员列表
  curl http://localhost:8080/api/raft/members | jq
  ```

- [ ] **Task 4.3**: 测试数据复制
  ```bash
  # 向leader写入数据
  curl -X POST http://localhost:8080/api/query/cypher \
    -d '{"query": "CREATE (n:TestNode {id: 1}) RETURN n"}'
  
  # 从follower读取
  curl -X POST http://localhost:8081/api/query/cypher \
    -d '{"query": "MATCH (n:TestNode {id: 1}) RETURN n"}'
  
  curl -X POST http://localhost:8082/api/query/cypher \
    -d '{"query": "MATCH (n:TestNode {id: 1}) RETURN n"}'
  ```

- [ ] **Task 4.4**: 验证RisingWave Meta HA
  ```bash
  # 检查RisingWave Meta状态
  curl http://localhost:5690/api/v1/meta/status
  
  # 验证3个Meta节点都在运行
  ps aux | grep risingwave
  ```

**产出**:
- ✅ 集群启动文档
- ✅ 状态检查清单
- ✅ 数据复制验证报告

---

### Day 3-4: 故障恢复测试

**目标**: 验证高可用和故障恢复能力

#### 任务清单

- [ ] **Task 5.1**: Leader故障测试
  ```bash
  # 1. 识别当前leader
  LEADER=$(curl -s http://localhost:8080/api/raft/leader | jq -r '.id')
  
  # 2. Kill leader节点
  kill <leader-pid>
  
  # 3. 验证自动选举新leader（应在5秒内完成）
  sleep 5
  curl http://localhost:8081/api/raft/leader | jq
  
  # 4. 验证服务可用性
  curl -X POST http://localhost:8081/api/query/cypher \
    -d '{"query": "CREATE (n:FailoverTest {ts: timestamp()}) RETURN n"}'
  ```

- [ ] **Task 5.2**: Follower故障测试
  ```bash
  # 1. Kill一个follower
  kill <follower-pid>
  
  # 2. 验证剩余2节点继续工作
  curl http://localhost:8080/api/cluster/status
  
  # 3. 验证写入仍然成功
  curl -X POST http://localhost:8080/api/query/cypher \
    -d '{"query": "CREATE (n:Node2Test) RETURN n"}'
  ```

- [ ] **Task 5.3**: 节点恢复测试
  ```bash
  # 1. 重启被kill的节点
  cargo run --release --features event-first,event-streaming -- \
    --port <port> --raft-addr <addr> --peers <peers>
  
  # 2. 验证节点重新加入集群
  curl http://localhost:8080/api/raft/members | jq
  
  # 3. 验证数据同步
  curl -X POST http://<recovered-node>/api/query/cypher \
    -d '{"query": "MATCH (n) RETURN count(n)"}'
  ```

- [ ] **Task 5.4**: 网络分区测试（可选）
  ```bash
  # 使用iptables模拟网络分区
  # 观察split-brain预防机制
  ```

**产出**:
- ✅ 故障恢复测试报告
- ✅ HA能力评估文档
- ✅ 已知问题和限制清单

---

### Day 5: 性能压测

**目标**: 验证多节点性能指标

#### 任务清单

- [ ] **Task 6.1**: 使用nexora-bench压测
  ```bash
  # 写入压测（1小时）
  cargo run --release -p nexora-bench -- \
    --mode distributed \
    --nodes http://localhost:8080,http://localhost:8081,http://localhost:8082 \
    --operation write \
    --duration 3600 \
    --concurrency 100 \
    --batch-size 1000
  
  # 查询压测（30分钟）
  cargo run --release -p nexora-bench -- \
    --mode distributed \
    --nodes http://localhost:8080,http://localhost:8081,http://localhost:8082 \
    --operation query \
    --duration 1800 \
    --concurrency 50
  ```

- [ ] **Task 6.2**: 监控资源使用
  ```bash
  # CPU和内存监控
  while true; do
    ps aux | grep nexora | awk '{print $3, $4, $11}'
    sleep 5
  done > resource-monitor.log
  
  # 网络流量监控
  iftop -i lo
  ```

- [ ] **Task 6.3**: 分析性能瓶颈
  - 使用flamegraph分析CPU热点
  - 使用heaptrack分析内存分配
  - 识别慢查询和优化机会

**产出**:
- ✅ 多节点性能基准报告
- ✅ 资源使用分析
- ✅ 性能优化建议

**Week 2 里程碑**: ✅ 3节点集群验证完成，HA能力确认

---

## Week 3: 试点应用准备

### Day 1-2: 文档和工具完善

**目标**: 提供完整的部署和运维文档

#### 任务清单

- [ ] **Task 7.1**: 编写部署文档
  - [ ] `docs/deployment/SINGLE_NODE_DEPLOYMENT.md`
    - 硬件要求（4核8GB）
    - 安装步骤
    - 配置说明
    - 启动和停止
  
  - [ ] `docs/deployment/CLUSTER_DEPLOYMENT.md`
    - 硬件要求（3节点，每节点4核8GB）
    - 网络配置
    - 集群初始化
    - 节点扩容和缩容

- [ ] **Task 7.2**: 创建运维工具
  ```bash
  # 备份工具
  scripts/ops/backup.sh
    - 备份RocksDB数据
    - 备份Iceberg元数据
    - 备份配置文件
  
  # 恢复工具
  scripts/ops/restore.sh
    - 从备份恢复
    - 数据验证
  
  # 健康检查
  scripts/ops/health-check.sh
    - 节点状态检查
    - Raft集群检查
    - 数据一致性检查
  
  # 升级工具
  scripts/ops/upgrade.sh
    - 滚动升级
    - 版本兼容性检查
  ```

- [ ] **Task 7.3**: 编写开发者指南
  - [ ] `docs/guides/QUICK_START.md` - 5分钟快速开始
  - [ ] `docs/guides/API_REFERENCE.md` - HTTP API文档
  - [ ] `docs/guides/CYPHER_GUIDE.md` - Cypher查询教程
  - [ ] `docs/guides/SQL_STREAMING_GUIDE.md` - RisingWave SQL使用
  - [ ] `docs/guides/TROUBLESHOOTING.md` - 常见问题解决

**产出**:
- ✅ 完整部署文档
- ✅ 运维工具集
- ✅ 开发者指南

---

### Day 3-4: Docker镜像和部署包

**目标**: 一键部署能力

#### 任务清单

- [ ] **Task 8.1**: 创建Dockerfile
  ```dockerfile
  # Dockerfile
  FROM rust:1.88 as builder
  WORKDIR /build
  COPY . .
  RUN cargo build --release --features event-first,event-streaming
  
  FROM ubuntu:22.04
  COPY --from=builder /build/target/release/nexora-app /usr/local/bin/nexora
  ENTRYPOINT ["/usr/local/bin/nexora"]
  ```

- [ ] **Task 8.2**: 构建Docker镜像
  ```bash
  # 单节点镜像
  docker build -t nexora:2.0.0 .
  docker tag nexora:2.0.0 nexora:latest
  
  # 推送到registry（可选）
  docker push nexora:2.0.0
  ```

- [ ] **Task 8.3**: 创建docker-compose配置
  ```yaml
  # docker-compose.single.yml - 单节点
  version: '3.8'
  services:
    nexora:
      image: nexora:2.0.0
      ports:
        - "8080:8080"
      volumes:
        - ./data:/data
        - ./nexora.toml:/etc/nexora.toml
      command: --config /etc/nexora.toml
  
  # docker-compose.cluster.yml - 3节点集群
  version: '3.8'
  services:
    nexora-1:
      image: nexora:2.0.0
      ports:
        - "8080:8080"
        - "5001:5001"
      ...
    nexora-2:
      image: nexora:2.0.0
      ...
    nexora-3:
      image: nexora:2.0.0
      ...
  ```

- [ ] **Task 8.4**: Kubernetes配置（可选，如果需要）
  ```bash
  # 创建Helm chart
  helm create nexora-chart
  
  # 编写k8s manifests
  k8s/nexora-single-node.yaml
  k8s/nexora-cluster-3nodes.yaml
  k8s/nexora-statefulset.yaml
  ```

**产出**:
- ✅ Docker镜像
- ✅ docker-compose配置
- ✅ K8s部署配置（如需要）

---

### Day 5: 试点就绪检查

**目标**: 全面验证试点准备度

#### 任务清单

- [ ] **Task 9.1**: 功能完整性检查
  ```bash
  # 运行完整测试套件
  cargo test --workspace --all-features
  
  # 运行端到端测试
  ./scripts/test-e2e-full-pipeline.sh
  
  # 运行集群测试
  ./scripts/test-distributed-risingwave.sh
  ```

- [ ] **Task 9.2**: 性能指标验证
  - [ ] 单节点吞吐: ≥50k events/s ✅
  - [ ] 多节点吞吐: ≥100k events/s ✅
  - [ ] 查询延迟: P95 <100ms ✅
  - [ ] 故障恢复: <30s ✅

- [ ] **Task 9.3**: 文档完整性检查
  - [ ] 部署文档完整 ✅
  - [ ] API文档完整 ✅
  - [ ] 运维工具齐全 ✅
  - [ ] 故障排查指南完整 ✅

- [ ] **Task 9.4**: 试点支持准备
  - [ ] 技术支持渠道（Slack/钉钉/微信群）
  - [ ] 问题跟踪系统（GitHub Issues/Jira）
  - [ ] 紧急响应流程文档
  - [ ] 试点监控仪表板

**产出**:
- ✅ 试点就绪检查清单
- ✅ 已知问题和限制文档
- ✅ 试点支持流程文档

**Week 3 里程碑**: ✅ 试点准备完成，可以交付

---

## 📋 试点就绪检查清单

### 功能完整性

- [ ] Event Streaming (Path A + B) 正常工作
- [ ] Graph Streaming 正常工作
- [ ] Iceberg OLAP查询正常工作
- [ ] 单节点部署验证通过
- [ ] 3节点集群验证通过
- [ ] 故障恢复能力验证通过

### 性能指标

- [ ] 单节点吞吐: ≥50k events/s
- [ ] 多节点吞吐: ≥100k events/s
- [ ] 查询延迟: P95 <100ms
- [ ] 故障恢复时间: <30s
- [ ] 数据一致性: 100%

### 文档和工具

- [ ] 部署文档完整
- [ ] API文档完整
- [ ] 运维工具齐全（备份/恢复/健康检查/升级）
- [ ] 监控配置就绪
- [ ] 故障排查指南完整

### 试点支持

- [ ] 技术支持渠道建立
- [ ] 问题跟踪系统就绪
- [ ] 紧急响应流程定义
- [ ] 监控仪表板配置
- [ ] 日志收集和分析工具

---

## 🚀 立即开始

### 第一步：快速验证

```bash
# 1. 运行快速验证脚本
./scripts/quick-validation.sh

# 2. 运行端到端测试
./scripts/test-e2e-full-pipeline.sh

# 3. 查看测试结果
cat /tmp/nexora-e2e-test/nexora.log
```

### 第二步：启动3节点集群

```bash
# 1. 启动集群
./scripts/start-cluster-3nodes-risingwave.sh

# 2. 验证集群状态
curl http://localhost:8080/api/cluster/status | jq
curl http://localhost:8081/api/cluster/status | jq
curl http://localhost:8082/api/cluster/status | jq

# 3. 停止集群
./scripts/stop-cluster.sh
```

### 第三步：性能测试

```bash
# 运行基准测试
cargo run --release -p nexora-bench -- \
  --mode distributed \
  --duration 3600 \
  --concurrency 100
```

---

## 📊 进度跟踪

| Week | 目标 | 状态 | 完成日期 |
|------|------|------|----------|
| Week 1 | 端到端验证 + 单节点完善 | ⏳ 待开始 | - |
| Week 2 | 多节点集群验证 | ⏳ 待开始 | - |
| Week 3 | 试点应用准备 | ⏳ 待开始 | - |

---

## 📝 风险和缓解

### 识别的风险

1. **多节点集群稳定性未知**
   - 缓解: Week 2进行充分的故障测试
   - 备选: 先试点单节点版本

2. **RisingWave集成性能可能不达预期**
   - 缓解: 保留Path A作为fallback
   - 备选: 先使用Path A，后续优化Path B

3. **文档和工具可能不够完善**
   - 缓解: Week 3专门时间完善文档
   - 备选: 提供技术支持人员陪同试点

4. **真实业务场景可能有未知问题**
   - 缓解: 选择小规模试点，快速迭代
   - 备选: 准备快速回滚方案

---

## ✅ 交付物清单

### 软件交付

- [ ] Nexora 2.0二进制文件（单节点和多节点）
- [ ] Docker镜像
- [ ] docker-compose配置
- [ ] 配置文件模板

### 文档交付

- [ ] 部署文档（单节点和集群）
- [ ] API参考文档
- [ ] 开发者指南
- [ ] 运维手册
- [ ] 故障排查指南

### 工具交付

- [ ] 备份/恢复工具
- [ ] 健康检查工具
- [ ] 升级工具
- [ ] 监控配置
- [ ] 测试脚本集

### 测试报告

- [ ] 功能测试报告
- [ ] 性能测试报告
- [ ] 故障恢复测试报告
- [ ] 试点就绪评估报告

---

## 📞 联系和支持

**技术负责人**: [待填写]  
**试点支持**: [待填写]  
**问题反馈**: GitHub Issues  

---

**文档版本**: 1.0  
**最后更新**: 2026-08-01
