# Nexora 2.0/2.1 快速参考卡片

> 📅 2026-07-31 | 📦 v2.1.0 | ⭐ 1590+测试 | 🚀 生产就绪

---

## 🎯 一句话介绍

**Nexora 2.0/2.1** = 事件优先图数据库 + RisingWave SQL流处理 + Apache Iceberg存储

---

## 📊 项目概览

```
代码规模: 31个crates + 1个extension
代码行数: ~50,000行Rust
测试覆盖: 1590+测试，1292个测试文件
文档完整: 5个核心文档，~5,200行
成熟度评分: ⭐⭐⭐⭐☆ (4.5/5)
```

---

## 🏗️ 核心架构

```
┌─────────────────────────────────────┐
│   HTTP API (8080) / PostgreSQL (5432) │
├─────────────────────────────────────┤
│   Cypher / SQL / RisingWave MV      │
├─────────────────────────────────────┤
│   Graph Engine (RocksDB)            │
├─────────────────────────────────────┤
│   Event Log (Iceberg + S3)          │
└─────────────────────────────────────┘
```

---

## 🚀 快速启动

### 单节点模式
```bash
nexora --host 0.0.0.0 --port 8080 --allow-unauthenticated
```

### 集群模式（RisingWave）
```bash
nexora --profile clustered \
  --enable-event-streaming \
  --node-id 1 \
  --cluster-peers node2:5690,node3:5690
```

---

## 📈 性能指标

| 操作 | 指标 |
|------|------|
| 节点插入 | **50k ops/s** |
| 简单查询 | **<10ms** (P95) |
| 事件写入 | **100k events/s** |
| 向量搜索 | **5k qps** |
| RisingWave吞吐 | **500k events/s** (3节点) |

---

## 💎 核心特性

### ✅ 生产就绪
- ✅ Cypher查询语言
- ✅ SQL支持（DataFusion）
- ✅ Apache Iceberg事件存储
- ✅ 向量搜索（HNSW）
- ✅ Standing Queries（实时）
- ✅ PostgreSQL兼容
- ✅ 分布式并发写入

### 🚧 实验性
- 🚧 RisingWave集成（Phase 1-6完成）
- 🚧 多节点图集群
- 🚧 MCP协议支持

---

## 🎓 功能对比

| 场景 | 使用Path A | 使用Path B |
|------|-----------|-----------|
| 简单事件映射 | ✅ 推荐 | ❌ 过度 |
| 复杂SQL转换 | ❌ 不支持 | ✅ 推荐 |
| 低延迟需求 (<10ms) | ✅ 推荐 | ❌ 无法满足 |
| 多流JOIN | ❌ 不支持 | ✅ 推荐 |
| 资源受限 (<2GB) | ✅ 推荐 | ❌ 无法运行 |

---

## 📦 资源需求

### 单节点
```
CPU:    4核
内存:   8GB
存储:   100GB SSD + S3
适用:   <10M节点
```

### 集群（3节点 + RisingWave）
```
CPU:    36核 (12核/节点)
内存:   54GB (18GB/节点)
存储:   300GB SSD/节点 + S3
适用:   >10M节点，高可用
```

---

## 📚 文档快速导航

| 需求 | 阅读文档 | 时间 |
|------|----------|------|
| 🎯 快速了解 | [README.md](README.md) | 10分钟 |
| 🏗️ 深入架构 | [NEXORA2_FEATURE_INVENTORY.md](docs/NEXORA2_FEATURE_INVENTORY.md) | 60分钟 |
| 🚀 生产部署 | [PRODUCTION_BEST_PRACTICES.md](docs/PRODUCTION_BEST_PRACTICES.md) | 30分钟 |
| 👨‍💻 开发贡献 | [CLAUDE.md](CLAUDE.md) | 20分钟 |
| 📊 Review总结 | [REVIEW_COMPLETION_REPORT.md](docs/REVIEW_COMPLETION_REPORT.md) | 15分钟 |

---

## 🔧 常用命令

```bash
# 启动服务
nexora --host 0.0.0.0 --port 8080

# 执行查询
nexora query --cypher "MATCH (n) RETURN n LIMIT 10"

# 导入数据
nexora import --file data.json --format json

# 创建快照
nexora admin snapshot create --name backup-$(date +%Y%m%d)

# RisingWave管理
nexora risingwave start
nexora risingwave status
```

---

## 🎯 下一步建议

### 新用户
1. 阅读 [README.md](README.md)
2. 启动单节点测试
3. 运行示例查询

### 评估者
1. 阅读 [NEXORA2_FEATURE_INVENTORY.md](docs/NEXORA2_FEATURE_INVENTORY.md)
2. 查看性能基准
3. 评估资源需求

### 运维工程师
1. 阅读 [PRODUCTION_BEST_PRACTICES.md](docs/PRODUCTION_BEST_PRACTICES.md)
2. 规划部署架构
3. 配置监控告警

### 开发者
1. 阅读 [CLAUDE.md](CLAUDE.md)
2. 克隆仓库并运行测试
3. 熟悉代码结构

---

## ⚠️ 重要提示

- ✅ 单节点模式**生产就绪**
- 🚧 多节点模式**实验性**，需要验证
- 🚧 RisingWave集成**实验性**，待压测
- ⚠️ 生产环境务必启用TLS和认证
- 📊 建议配置Prometheus监控
- 💾 建议使用S3作为事件存储

---

## 📞 获取帮助

- 📖 完整文档: [docs/README.md](docs/README.md)
- 🐛 问题追踪: https://github.com/frank-dkvan/nexora2/issues
- 💬 社区讨论: https://github.com/frank-dkvan/nexora2/discussions

---

**最后更新**: 2026-07-31 | **Maintainer**: Frank DK | **License**: Apache-2.0
