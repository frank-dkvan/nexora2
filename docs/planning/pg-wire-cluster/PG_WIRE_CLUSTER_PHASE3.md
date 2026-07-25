# Phase 3: 真实Sink和MV集成验证（预计3-4天）

## Task 3.1: 真实WebhookSink E2E测试

**目标**: 启动真实HTTP服务器，验证Webhook投递完整链路  
**工期**: 1-2天

### MockWebhookServer测试工具

```rust
// crates/nexora-standing-query/tests/common/mock_webhook_server.rs

use axum::{Router, Json, extract::State, response::IntoResponse};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MockWebhookServer {
    pub received_payloads: Arc<Mutex<Vec<ReceivedRequest>>>,
    pub listening_addr: SocketAddr,
    server_handle: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Clone)]
pub struct ReceivedRequest {
    pub payload: Value,
    pub headers: HashMap<String, String>,
    pub timestamp: SystemTime,
}

impl MockWebhookServer {
    /// Start a mock HTTP server on random port
    pub async fn start() -> Self {
        let received = Arc::new(Mutex::new(Vec::new()));
        let app_state = received.clone();
        
        let app = Router::new()
            .route("/webhook", axum::routing::post(webhook_handler))
            .with_state(app_state);
        
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        
        let server_handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        
        Self {
            received_payloads: received,
            listening_addr: addr,
            server_handle,
        }
    }
    
    /// Get all received payloads
    pub async fn get_received(&self) -> Vec<ReceivedRequest> {
        self.received_payloads.lock().await.clone()
    }
    
    /// Wait for N requests with timeout
    pub async fn wait_for_requests(&self, count: usize, timeout: Duration) -> bool {
        let start = Instant::now();
        loop {
            let received = self.received_payloads.lock().await.len();
            if received >= count {
                return true;
            }
            if start.elapsed() > timeout {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    
    pub async fn shutdown(self) {
        self.server_handle.abort();
    }
}

async fn webhook_handler(
    State(received): State<Arc<Mutex<Vec<ReceivedRequest>>>>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    let header_map: HashMap<String, String> = headers
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();
    
    received.lock().await.push(ReceivedRequest {
        payload,
        headers: header_map,
        timestamp: SystemTime::now(),
    });
    
    axum::http::StatusCode::OK
}
```

### 真实Webhook投递测试

```rust
// crates/nexora-standing-query/tests/webhook_delivery_e2e.rs

#[tokio::test]
async fn test_real_webhook_delivery() {
    // 1. 启动mock webhook服务器
    let webhook_server = MockWebhookServer::start().await;
    let webhook_url = format!("http://{}/webhook", webhook_server.listening_addr);
    
    // 2. 设置测试环境
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig::default(),
        Arc::new(InMemoryPersistor::new()),
    ));
    
    let sq_manager = Arc::new(StandingQueryManager::new(256));
    sq_manager.set_graph(graph.clone()).await;
    
    let sink_registry = Arc::new(SinkRegistry::new());
    
    // 3. 注册Webhook Sink
    let sink_id = sink_registry.register_sink(SinkConfig::Webhook {
        url: webhook_url.clone(),
        headers: vec![
            ("Authorization".to_string(), "Bearer test-token".to_string()),
            ("X-Custom-Header".to_string(), "test-value".to_string()),
        ],
        timeout_ms: 5000,
        max_retries: 3,
        retry_delay_ms: 100,
    }).await.unwrap();
    
    // 4. 创建Standing Query并订阅Sink
    let pattern = StandingQueryPattern::property("price", FilterCondition::GreaterThan(500.0));
    let sq_id = sq_manager.register("high_price_products", pattern).await;
    
    sink_registry.subscribe_to_sq(sink_id, sq_id).await.unwrap();
    
    // 5. 启动WebhookSinkRunner
    let webhook_runner = WebhookSinkRunner::new(
        sink_registry.clone(),
        sq_manager.subscribe(),
    );
    
    tokio::spawn(async move {
        webhook_runner.run().await;
    });
    
    // 6. 插入匹配SQ的数据
    graph.create_node(None).await.unwrap();
    let qid = NexoraId::from_u128(1);
    
    graph.set_property(qid, "id", PropertyValue::String("prod123".to_string())).await.unwrap();
    graph.set_property(qid, "name", PropertyValue::String("Widget".to_string())).await.unwrap();
    graph.set_property(qid, "price", PropertyValue::Integer(600)).await.unwrap();
    
    sq_manager.on_property_change(qid).await;
    
    // 7. 等待webhook投递
    let received = webhook_server.wait_for_requests(1, Duration::from_secs(3)).await;
    assert!(received, "Webhook was not called within timeout");
    
    // 8. 验证收到的payload
    let payloads = webhook_server.get_received().await;
    assert_eq!(payloads.len(), 1);
    
    let payload = &payloads[0].payload;
    assert_eq!(payload["sq_name"], "high_price_products");
    assert_eq!(payload["result_type"], "Matched");
    assert_eq!(payload["node_id"], qid.to_string());
    
    // 验证自定义headers
    assert_eq!(payloads[0].headers.get("authorization"), Some(&"Bearer test-token".to_string()));
    assert_eq!(payloads[0].headers.get("x-custom-header"), Some(&"test-value".to_string()));
    
    webhook_server.shutdown().await;
}

#[tokio::test]
async fn test_webhook_retry_on_failure() {
    // 1. 启动会失败的mock服务器
    let failing_server = MockWebhookServer::start_with_failures(3).await; // 前3次返回500
    let webhook_url = format!("http://{}/webhook", failing_server.listening_addr);
    
    // 2-5. 同上设置
    let graph = setup_graph().await;
    let sq_manager = setup_sq_manager(graph.clone()).await;
    let sink_registry = Arc::new(SinkRegistry::new());
    
    let sink_id = sink_registry.register_sink(SinkConfig::Webhook {
        url: webhook_url,
        headers: vec![],
        timeout_ms: 1000,
        max_retries: 5, // 允许重试5次
        retry_delay_ms: 100,
    }).await.unwrap();
    
    let sq_id = sq_manager.register("test_sq", default_pattern()).await;
    sink_registry.subscribe_to_sq(sink_id, sq_id).await.unwrap();
    
    let webhook_runner = WebhookSinkRunner::new(
        sink_registry.clone(),
        sq_manager.subscribe(),
    );
    tokio::spawn(webhook_runner.run());
    
    // 6. 触发SQ
    trigger_standing_query(&graph, &sq_manager).await;
    
    // 7. 验证重试成功（第4次成功）
    let received = failing_server.wait_for_requests(1, Duration::from_secs(5)).await;
    assert!(received, "Webhook should succeed after retries");
    
    let attempts = failing_server.get_attempt_count().await;
    assert_eq!(attempts, 4, "Should have 3 failures + 1 success");
}

#[tokio::test]
async fn test_webhook_dead_letter_queue() {
    // 1. 启动永远失败的mock服务器
    let always_fail_server = MockWebhookServer::start_always_fail().await;
    let webhook_url = format!("http://{}/webhook", always_fail_server.listening_addr);
    
    let graph = setup_graph().await;
    let sq_manager = setup_sq_manager(graph.clone()).await;
    let sink_registry = Arc::new(SinkRegistry::new());
    
    let sink_id = sink_registry.register_sink(SinkConfig::Webhook {
        url: webhook_url,
        headers: vec![],
        timeout_ms: 1000,
        max_retries: 3,
        retry_delay_ms: 50,
    }).await.unwrap();
    
    let sq_id = sq_manager.register("test_sq", default_pattern()).await;
    sink_registry.subscribe_to_sq(sink_id, sq_id).await.unwrap();
    
    // 创建DLQ
    let dlq = Arc::new(DeadLetterQueue::new());
    
    let webhook_runner = WebhookSinkRunner::new_with_dlq(
        sink_registry.clone(),
        sq_manager.subscribe(),
        dlq.clone(),
    );
    tokio::spawn(webhook_runner.run());
    
    // 触发SQ
    trigger_standing_query(&graph, &sq_manager).await;
    
    // 等待重试耗尽并进入DLQ
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // 验证DLQ
    let dlq_entries = dlq.get_all().await;
    assert_eq!(dlq_entries.len(), 1);
    assert_eq!(dlq_entries[0].retry_count, 3);
    assert_eq!(dlq_entries[0].sink_id, sink_id);
}

#[tokio::test]
async fn test_no_duplicate_delivery() {
    let webhook_server = MockWebhookServer::start().await;
    let webhook_url = format!("http://{}/webhook", webhook_server.listening_addr);
    
    let graph = setup_graph().await;
    let sq_manager = setup_sq_manager(graph.clone()).await;
    let sink_registry = Arc::new(SinkRegistry::new());
    
    // 注册同一个Webhook两次（测试去重）
    let sink_id_1 = sink_registry.register_sink(SinkConfig::Webhook {
        url: webhook_url.clone(),
        headers: vec![],
        timeout_ms: 5000,
        max_retries: 1,
        retry_delay_ms: 100,
    }).await.unwrap();
    
    let sink_id_2 = sink_registry.register_sink(SinkConfig::Webhook {
        url: webhook_url.clone(),
        headers: vec![],
        timeout_ms: 5000,
        max_retries: 1,
        retry_delay_ms: 100,
    }).await.unwrap();
    
    let sq_id = sq_manager.register("test_sq", default_pattern()).await;
    
    // 两个Sink都订阅同一个SQ
    sink_registry.subscribe_to_sq(sink_id_1, sq_id).await.unwrap();
    sink_registry.subscribe_to_sq(sink_id_2, sq_id).await.unwrap();
    
    let webhook_runner = WebhookSinkRunner::new(
        sink_registry.clone(),
        sq_manager.subscribe(),
    );
    tokio::spawn(webhook_runner.run());
    
    // 触发SQ
    trigger_standing_query(&graph, &sq_manager).await;
    
    // 等待投递
    tokio::time::sleep(Duration::from_secs(1)).await;
    
    // 验证收到2个请求（每个Sink一个）
    let payloads = webhook_server.get_received().await;
    assert_eq!(payloads.len(), 2, "Should receive exactly 2 webhook calls");
}
```

### 验收标准

- ✅ 真实HTTP POST请求成功投递
- ✅ 自定义headers正确传递
- ✅ 重试机制在失败时生效
- ✅ 重试耗尽后进入DLQ
- ✅ 同一SQ Match对每个Sink只投递一次
- ✅ 多个Sink订阅同一SQ都能收到

---

## Task 3.2: 真实MV查询E2E测试

**目标**: 使用真实PostgreSQL客户端查询物化视图  
**工期**: 1天

### 真实psql客户端测试

```rust
// crates/nexora-pgwire/tests/real_pg_client_mv_test.rs

use tokio_postgres::{Client, NoTls};

async fn create_real_pg_client(port: u16) -> Client {
    let config = format!("host=127.0.0.1 port={} user=admin dbname=nexora", port);
    let (client, connection) = tokio_postgres::connect(&config, NoTls)
        .await
        .unwrap();
    
    tokio::spawn(async move {
        let _ = connection.await;
    });
    
    client
}

#[tokio::test]
async fn test_real_pg_client_query_mv() {
    // 1. 启动PG-Wire服务器
    let setup = setup_test_environment().await;
    let port = setup._server.local_addr().port();
    
    // 2. 创建Standing Query
    let pattern = StandingQueryPattern::property("amount", FilterCondition::GreaterThan(1000.0));
    let sq_id = setup.sq_manager.register("high_value_orders", pattern).await;
    
    // 3. 创建Materialized View
    let mv_id = setup.mv_manager.create_view(
        "high_value_order_summary".to_string(),
        "MATCH (o:Order) WHERE o.amount > 1000 RETURN o.id, o.amount, o.customer".to_string(),
        vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::String },
            ColumnDef { name: "amount".to_string(), data_type: DataType::Integer },
            ColumnDef { name: "customer".to_string(), data_type: DataType::String },
        ],
        RefreshMode::Incremental,
    ).await.unwrap();
    
    // 4. 连接SQ → MV
    wire_sq_to_mv(&setup.sq_manager, &setup.mv_manager, sq_id, &mv_id).await;
    
    // 5. 连接真实PostgreSQL客户端
    let pg_client = create_real_pg_client(port).await;
    
    // 6. 通过PG-Wire插入数据
    pg_client.simple_query(
        "INSERT INTO Order (id, customer, amount) VALUES \
         ('o1', 'Alice', 1500), \
         ('o2', 'Bob', 500), \
         ('o3', 'Charlie', 2000), \
         ('o4', 'David', 1200)"
    ).await.unwrap();
    
    tokio::time::sleep(Duration::from_millis(300)).await;
    
    // 7. 通过真实psql查询MV
    let rows = pg_client.query(
        "SELECT * FROM high_value_order_summary ORDER BY amount DESC",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 3); // o1, o3, o4 匹配
    
    // 验证返回的数据
    assert_eq!(rows[0].get::<_, String>(0), "o3"); // Charlie, 2000
    assert_eq!(rows[0].get::<_, i32>(1), 2000);
    assert_eq!(rows[0].get::<_, String>(2), "Charlie");
    
    assert_eq!(rows[1].get::<_, String>(0), "o1"); // Alice, 1500
    assert_eq!(rows[1].get::<_, i32>(1), 1500);
    
    assert_eq!(rows[2].get::<_, String>(0), "o4"); // David, 1200
    assert_eq!(rows[2].get::<_, i32>(1), 1200);
}

#[tokio::test]
async fn test_real_pg_client_mv_with_where() {
    let setup = setup_test_environment().await;
    let port = setup._server.local_addr().port();
    
    // 设置SQ和MV（同上）
    let (sq_id, mv_id) = setup_sq_and_mv(&setup).await;
    wire_sq_to_mv(&setup.sq_manager, &setup.mv_manager, sq_id, &mv_id).await;
    
    let pg_client = create_real_pg_client(port).await;
    
    // 插入数据
    pg_client.simple_query(
        "INSERT INTO Product (id, name, stock) VALUES \
         ('p1', 'Widget', 5), \
         ('p2', 'Gadget', 8), \
         ('p3', 'Gizmo', 3), \
         ('p4', 'Tool', 12)"
    ).await.unwrap();
    
    tokio::time::sleep(Duration::from_millis(300)).await;
    
    // 查询MV并使用WHERE过滤
    let rows = pg_client.query(
        "SELECT * FROM low_stock_alert WHERE stock < 6 ORDER BY stock",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 2); // p3(3), p1(5)
    assert_eq!(rows[0].get::<_, String>(0), "p3");
    assert_eq!(rows[1].get::<_, String>(0), "p1");
}

#[tokio::test]
async fn test_real_pg_client_mv_with_limit() {
    let setup = setup_test_environment().await;
    let port = setup._server.local_addr().port();
    
    let (sq_id, mv_id) = setup_sq_and_mv(&setup).await;
    wire_sq_to_mv(&setup.sq_manager, &setup.mv_manager, sq_id, &mv_id).await;
    
    let pg_client = create_real_pg_client(port).await;
    
    // 插入20条数据
    for i in 0..20 {
        pg_client.simple_query(&format!(
            "INSERT INTO Item (id, priority) VALUES ('i{}', {})",
            i, i * 10
        )).await.unwrap();
    }
    
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // 查询MV并LIMIT 5
    let rows = pg_client.query(
        "SELECT * FROM item_summary ORDER BY priority DESC LIMIT 5",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0].get::<_, i32>(1), 190); // 最高优先级
}

#[tokio::test]
async fn test_real_pg_client_prepared_statement() {
    let setup = setup_test_environment().await;
    let port = setup._server.local_addr().port();
    
    let (sq_id, mv_id) = setup_sq_and_mv(&setup).await;
    wire_sq_to_mv(&setup.sq_manager, &setup.mv_manager, sq_id, &mv_id).await;
    
    let pg_client = create_real_pg_client(port).await;
    
    // 插入测试数据
    insert_test_data(&pg_client).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    
    // 使用Prepared Statement查询MV
    let stmt = pg_client.prepare(
        "SELECT * FROM order_summary WHERE amount > $1 ORDER BY amount"
    ).await.unwrap();
    
    let rows = pg_client.query(&stmt, &[&1500]).await.unwrap();
    
    assert!(rows.len() > 0);
    for row in &rows {
        let amount: i32 = row.get(1);
        assert!(amount > 1500);
    }
}
```

### 验收标准

- ✅ 真实tokio-postgres客户端成功连接
- ✅ SELECT查询MV返回正确数据
- ✅ WHERE条件过滤生效
- ✅ ORDER BY排序生效
- ✅ LIMIT分页生效
- ✅ Prepared Statement支持

---

## Task 3.3: 完整链路压力测试

**目标**: 验证在高负载下链路稳定性  
**工期**: 1天

### 压力测试场景

```rust
// crates/nexora-pgwire/tests/complete_pipeline_stress_test.rs

#[tokio::test]
async fn test_10k_inserts_with_sq_and_webhook() {
    let webhook_server = MockWebhookServer::start().await;
    let setup = setup_full_pipeline(webhook_server.listening_addr).await;
    
    let start = Instant::now();
    
    // 插入10,000条记录
    for i in 0..10_000 {
        setup.pg_client.execute(
            "INSERT INTO Event (id, type, value) VALUES ($1, $2, $3)",
            &[&format!("e{}", i), &"click", &(i as i32)],
        ).await.unwrap();
        
        if i % 1000 == 0 {
            println!("Inserted {} records", i);
        }
    }
    
    let insert_duration = start.elapsed();
    println!("10k inserts completed in {:?}", insert_duration);
    
    // 等待SQ处理和Webhook投递
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // 验证Webhook收到匹配的事件
    let received = webhook_server.get_received().await;
    println!("Received {} webhook calls", received.len());
    
    // 验证MV数据
    let mv_rows = setup.mv_manager.query_all(&setup.mv_id).await.unwrap();
    println!("MV has {} rows", mv_rows.len());
    
    // 性能断言：10k插入应在30秒内完成
    assert!(insert_duration < Duration::from_secs(30));
}

#[tokio::test]
async fn test_concurrent_writes_and_reads() {
    let setup = setup_full_pipeline_no_webhook().await;
    
    // 启动10个并发写入任务
    let write_handles: Vec<_> = (0..10).map(|worker_id| {
        let client = setup.create_pg_client();
        tokio::spawn(async move {
            for i in 0..1000 {
                let id = format!("w{}_i{}", worker_id, i);
                client.execute(
                    "INSERT INTO Record (id, data) VALUES ($1, $2)",
                    &[&id, &(i as i32)],
                ).await.unwrap();
            }
        })
    }).collect();
    
    // 同时启动10个并发读取任务
    let read_handles: Vec<_> = (0..10).map(|_| {
        let client = setup.create_pg_client();
        tokio::spawn(async move {
            for _ in 0..100 {
                let _ = client.query(
                    "SELECT COUNT(*) FROM Record",
                    &[],
                ).await;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    }).collect();
    
    // 等待所有任务完成
    for handle in write_handles {
        handle.await.unwrap();
    }
    for handle in read_handles {
        handle.await.unwrap();
    }
    
    // 验证最终数据一致性
    let rows = setup.pg_client.query(
        "SELECT COUNT(*) FROM Record",
        &[],
    ).await.unwrap();
    
    let count: i64 = rows[0].get(0);
    assert_eq!(count, 10_000);
}

#[tokio::test]
async fn test_latency_slo() {
    let setup = setup_full_pipeline_with_metrics().await;
    
    let mut latencies = Vec::new();
    
    // 执行1000次写入并记录延迟
    for i in 0..1000 {
        let start = Instant::now();
        
        setup.pg_client.execute(
            "INSERT INTO Measurement (id, value) VALUES ($1, $2)",
            &[&format!("m{}", i), &(i as i32)],
        ).await.unwrap();
        
        let latency = start.elapsed();
        latencies.push(latency);
    }
    
    // 等待SQ处理
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // 计算P50, P95, P99
    latencies.sort();
    let p50 = latencies[500];
    let p95 = latencies[950];
    let p99 = latencies[990];
    
    println!("Write latency - P50: {:?}, P95: {:?}, P99: {:?}", p50, p95, p99);
    
    // SLO断言
    assert!(p95 < Duration::from_millis(100), "P95 latency exceeds 100ms");
    
    // 验证SQ触发延迟
    let sq_latencies = setup.metrics.get_sq_processing_latencies().await;
    let sq_p95 = percentile(&sq_latencies, 0.95);
    println!("SQ processing P95: {:?}", sq_p95);
    assert!(sq_p95 < Duration::from_millis(50), "SQ P95 latency exceeds 50ms");
}
```

### 验收标准

- ✅ 10k插入在30秒内完成
- ✅ 并发写入+读取无死锁
- ✅ 写入→SQ P95 < 100ms
- ✅ SQ处理 P95 < 50ms
- ✅ Webhook投递 P95 < 500ms
- ✅ 无数据丢失或重复

---

## Phase 3 总验收

运行完整测试套件：

```bash
# 1. 真实Webhook投递测试
cargo test --test webhook_delivery_e2e

# 2. 真实PostgreSQL客户端测试
cargo test --test real_pg_client_mv_test

# 3. 压力测试
cargo test --test complete_pipeline_stress_test -- --test-threads=1

# 4. 完整E2E回归
cargo test -p nexora-pgwire --test complete_data_pipeline_e2e
```

**Phase 3完成标志**:

- ✅ 真实HTTP服务器收到Webhook请求
- ✅ tokio-postgres客户端成功查询MV
- ✅ 10k插入性能达标
- ✅ 并发场景稳定
- ✅ 延迟SLO达标
