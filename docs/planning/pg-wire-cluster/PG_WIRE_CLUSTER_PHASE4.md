# Phase 4: 生产就绪（预计2-3天）

## Task 4.1: 补充文档和示例

**目标**: 提供完整的用户和运维文档  
**工期**: 1天

### 用户文档清单

```markdown
# docs/user-guide/

## 1. QUICKSTART.md - 快速开始

### 单机模式
- 安装和启动
- 连接PostgreSQL客户端
- 创建Standing Query
- 创建Materialized View
- 配置Webhook Sink

### 集群模式
- 启动3节点集群
- 验证数据分布
- 测试故障转移

## 2. PG_WIRE_REFERENCE.md - PG-Wire协议参考

### 支持的SQL语法
- INSERT: 单行、批量、UPSERT
- UPDATE: 单行、批量、复杂WHERE
- DELETE: 单行、批量、级联删除
- SELECT: 投影、过滤、排序、分页、聚合

### 限制和注意事项
- SQL表名映射到Cypher节点标签
- 边操作需要使用edge_*表名
- 聚合查询在集群模式下性能开销

## 3. STANDING_QUERY_GUIDE.md - Standing Query指南

### 模式语法
- 属性过滤: GreaterThan, LessThan, Equals, Contains
- 标签过滤: hasLabel, hasAnyLabel
- 复合条件: AND, OR, NOT
- 边遍历: 单跳、多跳、路径模式

### 性能优化
- 使用属性索引加速匹配
- 避免全图扫描模式
- 限制边遍历深度

### 常见场景
- 实时告警: 库存低于阈值
- 欺诈检测: 异常交易模式
- 推荐系统: 用户兴趣图匹配

## 4. MATERIALIZED_VIEW_GUIDE.md - 物化视图指南

### 创建和管理
- 定义schema和Cypher查询
- 选择刷新模式: Incremental vs Manual
- 监控MV大小和性能

### 查询优化
- 为MV添加索引
- 使用WHERE提前过滤
- LIMIT避免大结果集

### 最佳实践
- MV命名规范: {entity}_{aggregation}_summary
- 定期清理过期数据
- 监控增量更新延迟

## 5. SINK_INTEGRATION_GUIDE.md - Sink集成指南

### Webhook配置
```yaml
sink:
  type: webhook
  url: https://api.example.com/events
  headers:
    Authorization: Bearer ${SECRET_TOKEN}
  timeout_ms: 5000
  max_retries: 3
  retry_delay_ms: 1000
```

### Kafka配置
```yaml
sink:
  type: kafka
  brokers:
    - kafka1.example.com:9092
    - kafka2.example.com:9092
  topic: nexora-standing-query-results
  acks: all
  compression: gzip
```

### 死信队列
- DLQ配置和监控
- 重放失败事件
- 告警集成

## 6. CLUSTER_OPERATIONS.md - 集群运维

### 集群拓扑
- Shard分配策略
- Epoch机制和故障隔离
- 节点发现和心跳

### 扩容缩容
- 添加新节点
- Shard rebalancing
- 数据迁移策略

### 故障处理
- 节点故障检测
- Owner不可用错误处理
- Epoch不匹配排查

### 监控和告警
- 关键指标: 写入QPS、查询延迟、SQ匹配率
- Grafana仪表盘示例
- Prometheus告警规则

## 7. TROUBLESHOOTING.md - 故障排查

### 常见问题
1. "Owner unavailable" 错误
   - 原因: Shard owner节点宕机
   - 解决: 等待epoch变更，或手动reassign shard

2. "Epoch mismatch" 错误
   - 原因: 写入使用了过期的epoch
   - 解决: 客户端重试，获取最新epoch

3. Standing Query不触发
   - 检查pattern定义是否正确
   - 验证数据确实匹配pattern
   - 查看SQ manager日志

4. Webhook投递失败
   - 检查目标URL可达性
   - 验证认证token
   - 查看DLQ是否有堆积

5. MV数据不一致
   - 检查SQ → MV订阅是否正常
   - 验证MV增量更新逻辑
   - 手动触发全量刷新

### 日志分析
```bash
# 查看Standing Query评估日志
grep "SQ evaluation" /var/log/nexora/server.log

# 查看Webhook投递日志
grep "Webhook delivery" /var/log/nexora/server.log | grep -i error

# 查看Shard路由日志
grep "Route to shard" /var/log/nexora/server.log
```

### 性能调优
- 调整GraphService shard数量
- 优化Standing Query pattern
- 增加MV刷新并发度
- 配置Webhook批量投递
```

### 代码示例

```rust
// examples/pg_wire_complete_pipeline.rs

/// Complete example: PG-Wire → SQ → MV → Webhook
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 启动Nexora服务器
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig::default(),
        Arc::new(InMemoryPersistor::new()),
    ));
    
    let sq_manager = Arc::new(StandingQueryManager::new(256));
    sq_manager.set_graph(graph.clone()).await;
    
    let mv_manager = Arc::new(MaterializedViewManager::new());
    let sink_registry = Arc::new(SinkRegistry::new());
    
    // 2. 启动PG-Wire服务器
    let pg_config = PgConfig {
        port: 5433,
        bind_addr: "0.0.0.0".to_string(),
        trust: true,
        ..Default::default()
    };
    
    let server = spawn_pg_server(
        graph.clone(),
        mv_manager.clone(),
        Some(sq_manager.clone()),
        pg_config,
    ).await?;
    
    println!("PG-Wire server listening on port 5433");
    
    // 3. 注册Webhook Sink
    let webhook_sink_id = sink_registry.register_sink(SinkConfig::Webhook {
        url: "http://localhost:8080/webhook".to_string(),
        headers: vec![
            ("Authorization".to_string(), "Bearer SECRET".to_string()),
        ],
        timeout_ms: 5000,
        max_retries: 3,
        retry_delay_ms: 1000,
    }).await?;
    
    println!("Registered webhook sink: {}", webhook_sink_id);
    
    // 4. 创建Standing Query
    let pattern = StandingQueryPattern::property(
        "amount",
        FilterCondition::GreaterThan(1000.0),
    );
    let sq_id = sq_manager.register("high_value_orders", pattern).await;
    
    println!("Registered Standing Query: {}", sq_id);
    
    // 5. 订阅Webhook
    sink_registry.subscribe_to_sq(webhook_sink_id, sq_id).await?;
    println!("Subscribed webhook to Standing Query");
    
    // 6. 创建Materialized View
    let mv_id = mv_manager.create_view(
        "high_value_order_summary".to_string(),
        "MATCH (o:Order) WHERE o.amount > 1000 RETURN o.id, o.amount".to_string(),
        vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::String,
            },
            ColumnDef {
                name: "amount".to_string(),
                data_type: DataType::Integer,
            },
        ],
        RefreshMode::Incremental,
    ).await?;
    
    println!("Created Materialized View: {}", mv_id);
    
    // 7. 连接SQ → MV
    let mut sq_results = sq_manager.subscribe();
    let mv_clone = mv_manager.clone();
    let mv_id_clone = mv_id.clone();
    
    tokio::spawn(async move {
        while let Ok(result) = sq_results.recv().await {
            if result.sq_id == sq_id {
                match result.result_type {
                    ResultType::Matched => {
                        let row = MaterializedRow {
                            key: result.qid.to_string(),
                            values: result.matched_properties,
                            version: 1,
                            updated_at: Utc::now(),
                        };
                        let _ = mv_clone.upsert_row(&mv_id_clone, row).await;
                    }
                    ResultType::Unmatched => {
                        let _ = mv_clone.delete_row(&mv_id_clone, &result.qid.to_string()).await;
                    }
                }
            }
        }
    });
    
    println!("Connected Standing Query to Materialized View");
    
    // 8. 启动Webhook runner
    let webhook_runner = WebhookSinkRunner::new(
        sink_registry.clone(),
        sq_manager.subscribe(),
    );
    
    tokio::spawn(async move {
        webhook_runner.run().await;
    });
    
    println!("Started Webhook runner");
    
    println!("\n✅ Complete pipeline ready!");
    println!("Connect with: psql -h localhost -p 5433 -U admin -d nexora");
    println!("Example query: SELECT * FROM high_value_order_summary;");
    
    // 保持服务运行
    tokio::signal::ctrl_c().await?;
    println!("\nShutting down...");
    
    Ok(())
}
```

### 验收标准

- ✅ 所有7份用户文档完整
- ✅ 至少2个完整代码示例
- ✅ 故障排查指南包含10+常见问题
- ✅ 文档经过技术审校

---

## Task 4.2: 监控和可观测性

**目标**: 集成Prometheus metrics和结构化日志  
**工期**: 1天

### Prometheus Metrics

```rust
// crates/nexora-observability/src/metrics.rs

use prometheus::{Counter, Histogram, IntGauge, Registry};

pub struct NexoraMetrics {
    // PG-Wire metrics
    pub pg_queries_total: Counter,
    pub pg_query_duration: Histogram,
    pub pg_active_connections: IntGauge,
    
    // Standing Query metrics
    pub sq_evaluations_total: Counter,
    pub sq_matches_total: Counter,
    pub sq_evaluation_duration: Histogram,
    
    // Materialized View metrics
    pub mv_upserts_total: Counter,
    pub mv_deletes_total: Counter,
    pub mv_row_count: IntGauge,
    
    // Webhook Sink metrics
    pub webhook_deliveries_total: Counter,
    pub webhook_failures_total: Counter,
    pub webhook_delivery_duration: Histogram,
    pub webhook_retry_count: Counter,
    pub webhook_dlq_size: IntGauge,
    
    // Distributed routing metrics
    pub distributed_queries_total: Counter,
    pub cross_shard_queries_total: Counter,
    pub shard_routing_duration: Histogram,
    pub epoch_mismatch_errors: Counter,
    pub owner_unavailable_errors: Counter,
}

impl NexoraMetrics {
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let pg_queries_total = Counter::new(
            "nexora_pg_queries_total",
            "Total number of PG-Wire queries"
        )?;
        registry.register(Box::new(pg_queries_total.clone()))?;
        
        let pg_query_duration = Histogram::with_opts(
            prometheus::HistogramOpts::new(
                "nexora_pg_query_duration_seconds",
                "PG-Wire query duration"
            ).buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0])
        )?;
        registry.register(Box::new(pg_query_duration.clone()))?;
        
        // ... 注册其他指标
        
        Ok(Self {
            pg_queries_total,
            pg_query_duration,
            // ...
        })
    }
    
    /// Record a PG-Wire query execution
    pub fn record_pg_query(&self, duration: Duration, query_type: &str) {
        self.pg_queries_total.inc();
        self.pg_query_duration.observe(duration.as_secs_f64());
    }
    
    /// Record a Standing Query evaluation
    pub fn record_sq_evaluation(&self, duration: Duration, matched: bool) {
        self.sq_evaluations_total.inc();
        if matched {
            self.sq_matches_total.inc();
        }
        self.sq_evaluation_duration.observe(duration.as_secs_f64());
    }
    
    /// Record a Webhook delivery
    pub fn record_webhook_delivery(&self, duration: Duration, success: bool) {
        self.webhook_deliveries_total.inc();
        if !success {
            self.webhook_failures_total.inc();
        }
        self.webhook_delivery_duration.observe(duration.as_secs_f64());
    }
}
```

### 集成到服务

```rust
// crates/nexora-app/src/main.rs

use nexora_observability::NexoraMetrics;
use prometheus::{Encoder, TextEncoder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化metrics
    let registry = Registry::new();
    let metrics = Arc::new(NexoraMetrics::new(&registry)?);
    
    // 启动metrics HTTP server
    tokio::spawn(serve_metrics(registry.clone()));
    
    // 将metrics传递给各组件
    let graph = Arc::new(GraphService::new_with_metrics(
        config,
        persistor,
        metrics.clone(),
    ));
    
    let sq_manager = Arc::new(StandingQueryManager::new_with_metrics(
        256,
        metrics.clone(),
    ));
    
    // ...
}

async fn serve_metrics(registry: Registry) {
    use axum::{Router, routing::get};
    
    let app = Router::new()
        .route("/metrics", get(|| async move {
            let encoder = TextEncoder::new();
            let metric_families = registry.gather();
            let mut buffer = Vec::new();
            encoder.encode(&metric_families, &mut buffer).unwrap();
            String::from_utf8(buffer).unwrap()
        }));
    
    let listener = tokio::net::TcpListener::bind("0.0.0.0:9090")
        .await
        .unwrap();
    
    println!("Metrics server listening on :9090/metrics");
    axum::serve(listener, app).await.unwrap();
}
```

### Grafana Dashboard配置

```json
// dashboards/nexora_overview.json

{
  "dashboard": {
    "title": "Nexora Overview",
    "panels": [
      {
        "title": "PG-Wire Query Rate",
        "targets": [{
          "expr": "rate(nexora_pg_queries_total[1m])"
        }]
      },
      {
        "title": "Standing Query Match Rate",
        "targets": [{
          "expr": "rate(nexora_sq_matches_total[1m])"
        }]
      },
      {
        "title": "Webhook Delivery Success Rate",
        "targets": [{
          "expr": "rate(nexora_webhook_deliveries_total{result=\"success\"}[5m]) / rate(nexora_webhook_deliveries_total[5m])"
        }]
      },
      {
        "title": "Query Latency (P95)",
        "targets": [{
          "expr": "histogram_quantile(0.95, rate(nexora_pg_query_duration_seconds_bucket[5m]))"
        }]
      },
      {
        "title": "Dead Letter Queue Size",
        "targets": [{
          "expr": "nexora_webhook_dlq_size"
        }]
      }
    ]
  }
}
```

### 结构化日志

```rust
// 使用tracing库替换println!和eprintln!

use tracing::{info, warn, error, debug};

// 在关键路径添加span
#[tracing::instrument(skip(graph))]
async fn execute_sql(graph: &GraphService, query: &str) -> Result<SqlResult, SqlError> {
    let start = Instant::now();
    
    info!(query = %query, "Executing SQL");
    
    let result = translate_and_execute(graph, query).await;
    
    let duration = start.elapsed();
    match &result {
        Ok(_) => info!(duration_ms = duration.as_millis(), "SQL execution succeeded"),
        Err(e) => error!(error = %e, duration_ms = duration.as_millis(), "SQL execution failed"),
    }
    
    result
}

// Standing Query evaluation span
#[tracing::instrument(skip(self))]
async fn evaluate_pattern(&self, qid: NexoraId, pattern: &Pattern) -> bool {
    debug!(qid = %qid, pattern = ?pattern, "Evaluating pattern");
    
    let matched = self.check_pattern(qid, pattern).await;
    
    if matched {
        info!(qid = %qid, "Pattern matched");
    } else {
        debug!(qid = %qid, "Pattern not matched");
    }
    
    matched
}
```

### Prometheus告警规则

```yaml
# prometheus/alerts.yml

groups:
  - name: nexora
    rules:
      - alert: HighWebhookFailureRate
        expr: |
          rate(nexora_webhook_failures_total[5m]) / rate(nexora_webhook_deliveries_total[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High webhook failure rate (>10%)"
          
      - alert: DeadLetterQueueGrowing
        expr: |
          deriv(nexora_webhook_dlq_size[10m]) > 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Dead letter queue is growing"
          
      - alert: HighQueryLatency
        expr: |
          histogram_quantile(0.95, rate(nexora_pg_query_duration_seconds_bucket[5m])) > 1.0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "P95 query latency exceeds 1 second"
          
      - alert: FrequentEpochMismatches
        expr: |
          rate(nexora_epoch_mismatch_errors[5m]) > 1
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Frequent epoch mismatch errors detected"
```

### 验收标准

- ✅ Prometheus metrics endpoint暴露
- ✅ 至少15个关键指标
- ✅ Grafana dashboard可导入
- ✅ 结构化日志可解析
- ✅ 告警规则配置完整

---

## Task 4.3: CI/CD和发布流程

**目标**: 自动化测试和发布  
**工期**: 1天

### GitHub Actions CI配置

```yaml
# .github/workflows/ci.yml

name: Nexora CI

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

env:
  CARGO_TERM_COLOR: always
  RUST_BACKTRACE: 1

jobs:
  test:
    name: Test Suite
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Rust
        uses: dtolnay/rust-toolchain@stable
        
      - name: Cache cargo registry
        uses: actions/cache@v3
        with:
          path: ~/.cargo/registry
          key: ${{ runner.os }}-cargo-registry-${{ hashFiles('**/Cargo.lock') }}
          
      - name: Cache cargo build
        uses: actions/cache@v3
        with:
          path: target
          key: ${{ runner.os }}-cargo-build-${{ hashFiles('**/Cargo.lock') }}
      
      - name: Run tests
        run: cargo test --all --verbose
        
      - name: Run clippy
        run: cargo clippy --all-targets --all-features -- -D warnings
        
      - name: Check formatting
        run: cargo fmt -- --check

  integration-test:
    name: Integration Tests
    runs-on: ubuntu-latest
    needs: test
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Rust
        uses: dtolnay/rust-toolchain@stable
        
      - name: Run PG-Wire integration tests
        run: cargo test -p nexora-pgwire --test complete_data_pipeline_e2e -- --test-threads=1
        
      - name: Run distributed tests
        run: cargo test -p nexora-distributed --tests -- --test-threads=1

  benchmark:
    name: Performance Benchmarks
    runs-on: ubuntu-latest
    needs: test
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Rust
        uses: dtolnay/rust-toolchain@stable
        
      - name: Run benchmarks
        run: cargo bench --no-fail-fast
        
      - name: Upload benchmark results
        uses: actions/upload-artifact@v3
        with:
          name: benchmark-results
          path: target/criterion

  security-audit:
    name: Security Audit
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Install cargo-audit
        run: cargo install cargo-audit
        
      - name: Run security audit
        run: cargo audit
```

### 发布流程

```yaml
# .github/workflows/release.yml

name: Release

on:
  push:
    tags:
      - 'v*'

jobs:
  build-and-release:
    name: Build and Release
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
        
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Rust
        uses: dtolnay/rust-toolchain@stable
        
      - name: Build release binary
        run: cargo build --release
        
      - name: Package binary
        run: |
          tar -czf nexora-${{ runner.os }}-x86_64.tar.gz \
            -C target/release nexora-server
            
      - name: Create GitHub Release
        uses: softprops/action-gh-release@v1
        with:
          files: nexora-${{ runner.os }}-x86_64.tar.gz
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          
      - name: Publish to crates.io
        if: runner.os == 'Linux'
        run: cargo publish --token ${{ secrets.CRATES_IO_TOKEN }}
```

### Docker镜像构建

```dockerfile
# Dockerfile

FROM rust:1.75 as builder

WORKDIR /app
COPY . .

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/nexora-server /usr/local/bin/

EXPOSE 5433 9090

ENTRYPOINT ["nexora-server"]
```

```yaml
# docker-compose.yml

version: '3.8'

services:
  nexora-node1:
    image: nexora:latest
    ports:
      - "5433:5433"
      - "9090:9090"
    environment:
      - NODE_ID=node1
      - SHARD_RANGE=0-3
      - CLUSTER_MODE=true
    volumes:
      - node1-data:/data
      
  nexora-node2:
    image: nexora:latest
    ports:
      - "5434:5433"
      - "9091:9090"
    environment:
      - NODE_ID=node2
      - SHARD_RANGE=4-7
      - CLUSTER_MODE=true
    volumes:
      - node2-data:/data
      
  nexora-node3:
    image: nexora:latest
    ports:
      - "5435:5433"
      - "9092:9090"
    environment:
      - NODE_ID=node3
      - SHARD_RANGE=8-11
      - CLUSTER_MODE=true
    volumes:
      - node3-data:/data
      
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9093:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus-data:/prometheus
      
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - ./dashboards:/etc/grafana/provisioning/dashboards
      - grafana-data:/var/lib/grafana

volumes:
  node1-data:
  node2-data:
  node3-data:
  prometheus-data:
  grafana-data:
```

### 验收标准

- ✅ CI通过（测试、clippy、fmt）
- ✅ 集成测试自动化运行
- ✅ 安全审计无高危漏洞
- ✅ Docker镜像可构建
- ✅ docker-compose可一键启动3节点集群

---

## Phase 4 总验收

完成以下检查清单：

### 文档完整性
- ✅ 用户文档7份齐全
- ✅ API参考文档
- ✅ 故障排查手册
- ✅ 运维手册
- ✅ 至少2个完整代码示例

### 可观测性
- ✅ Prometheus metrics endpoint
- ✅ 15+关键指标
- ✅ Grafana dashboard可用
- ✅ 结构化日志
- ✅ 告警规则配置

### CI/CD
- ✅ GitHub Actions CI通过
- ✅ 自动化测试覆盖
- ✅ 安全审计
- ✅ Docker镜像
- ✅ docker-compose启动脚本

### 生产就绪检查
- ✅ 所有核心测试通过
- ✅ 性能基准达标
- ✅ 监控告警完整
- ✅ 文档齐全
- ✅ 示例可运行
- ✅ 部署方式清晰

**Phase 4完成标志**:

🎉 **Nexora PG-Wire集群支持达到生产级要求，可正式发布！**
