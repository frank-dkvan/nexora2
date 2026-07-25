//! Real WebhookSink End-to-End Test
//!
//! This test validates WebhookSink with a real HTTP server:
//! 1. Start a test HTTP server to receive webhook POSTs
//! 2. INSERT data via PG-Wire
//! 3. Standing Query matches and sends webhook
//! 4. Verify HTTP server received the correct payload

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_pgwire::{spawn_pg_server, PgConfig};
use nexora_standing_query::{
    pattern::{FilterCondition, StandingQueryPattern},
    SinkConfig, SinkRegistry, StandingQueryManager, WebhookSinkRunner,
};
use tokio_postgres::{Client, NoTls};
use warp::Filter;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct WebhookPayload {
    sq_id: String,
    sq_name: String,
    node_id: String,
    result_type: String,
    matched_properties: serde_json::Value,
}

struct TestSetup {
    pg_client: Client,
    sq_manager: Arc<StandingQueryManager>,
    sink_registry: Arc<SinkRegistry>,
    #[allow(dead_code)] // Webhook runner kept alive for test duration
    webhook_runner: Arc<WebhookSinkRunner>,
    received_webhooks: Arc<Mutex<Vec<WebhookPayload>>>,
    _server: nexora_pgwire::PgServerHandle,
    _webhook_server: tokio::task::JoinHandle<()>,
}

async fn setup_test_environment() -> TestSetup {
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 1000,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ));

    let sq_manager = Arc::new(StandingQueryManager::new(256));
    sq_manager.set_graph(graph.clone()).await;

    let sink_registry = Arc::new(SinkRegistry::new());
    let webhook_runner = Arc::new(WebhookSinkRunner::new());

    // Subscribe to SQ results
    let sq_receiver = sq_manager.subscribe();

    // Start WebhookSinkRunner to listen for SQ results
    let webhook_runner_clone = webhook_runner.clone();
    let sink_registry_clone = sink_registry.clone();
    tokio::spawn(async move {
        webhook_runner_clone
            .run(sq_receiver, sink_registry_clone)
            .await;
    });

    // Start test HTTP server to receive webhooks
    let received_webhooks = Arc::new(Mutex::new(Vec::new()));
    let received_webhooks_clone = received_webhooks.clone();

    let webhook_route = warp::post()
        .and(warp::path("webhook"))
        .and(warp::body::json())
        .map(move |payload: WebhookPayload| {
            let received = received_webhooks_clone.clone();
            tokio::spawn(async move {
                received.lock().await.push(payload);
            });
            warp::reply::json(&serde_json::json!({"status": "ok"}))
        });

    let (webhook_addr, webhook_server_future) =
        warp::serve(webhook_route).bind_ephemeral(([127, 0, 0, 1], 0));

    let webhook_server = tokio::spawn(webhook_server_future);

    println!("Webhook server listening on http://{}", webhook_addr);

    let config = PgConfig {
        port: 0,
        bind_addr: "127.0.0.1".to_owned(),
        trust: true,
        max_connections: 10,
        idle_timeout_secs: 60,
        shutdown_grace_secs: 2,
        ..PgConfig::default()
    };

    let query_pool = std::sync::Arc::new(nexora_core::query_pool::QueryPool::new(4));

    let server = spawn_pg_server(
        graph.clone(),
        Arc::new(nexora_core::materialized_view::MaterializedViewManager::new()),
        Some(sq_manager.clone()),
        query_pool,
        config,
    )
    .await
    .expect("server starts");

    let address = server.local_addr();
    let mut pg_config = tokio_postgres::Config::new();
    pg_config
        .host(address.ip().to_string())
        .port(address.port())
        .user("admin")
        .dbname("nexora");

    let (client, connection) = pg_config.connect(NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Register webhook sink
    let webhook_url = format!("http://{}/webhook", webhook_addr);

    let mut headers = HashMap::new();
    headers.insert("X-Test-Header".to_string(), "test-value".to_string());

    let webhook_config = nexora_standing_query::WebhookSinkConfig {
        url: webhook_url.clone(),
        headers,
        timeout_secs: 5,
        max_retries: 3,
        retry_delay_ms: 100,
    };

    let sink_id = sink_registry
        .register(
            "test_webhook".to_string(),
            SinkConfig::Webhook(webhook_config.clone()),
        )
        .await;

    // Register with runner
    webhook_runner
        .register(sink_id, webhook_config)
        .await
        .unwrap();

    println!(
        "Registered webhook sink {} with URL: {}",
        sink_id, webhook_url
    );

    TestSetup {
        pg_client: client,
        sq_manager,
        sink_registry,
        webhook_runner,
        received_webhooks,
        _server: server,
        _webhook_server: webhook_server,
    }
}

#[tokio::test]
async fn test_real_webhook_sink_receives_sq_results() {
    let setup = setup_test_environment().await;

    // Create Standing Query: products with price > 500
    let pattern = StandingQueryPattern::property("price", FilterCondition::GreaterThan(500.0));
    let sq_id = setup
        .sq_manager
        .register("expensive_products", pattern)
        .await;

    // Subscribe the webhook sink to the SQ
    let sinks = setup.sink_registry.list_all().await;
    let sink_id = sinks[0].id;
    setup
        .sink_registry
        .subscribe(sq_id, sink_id)
        .await
        .expect("subscription succeeds");

    // INSERT products via PG-Wire
    setup
        .pg_client
        .simple_query(
            "INSERT INTO Product (id, name, price) VALUES \
             ('laptop', 'Laptop', 1200), \
             ('mouse', 'Mouse', 25), \
             ('monitor', 'Monitor', 800)",
        )
        .await
        .unwrap();

    // Wait for SQ to process and webhooks to be sent
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify: SQ matched 2 expensive products
    let match_count = setup.sq_manager.match_count(sq_id).await;
    assert_eq!(match_count, 2, "Should match 2 expensive products");

    // Verify: Webhook server received 2 webhooks
    let received = setup.received_webhooks.lock().await;
    println!("DEBUG: Received {} webhooks", received.len());
    for (i, webhook) in received.iter().enumerate() {
        println!("DEBUG: Webhook {}: {:?}", i, webhook);
    }
    assert_eq!(received.len(), 2, "Should receive 2 webhooks");

    // Verify webhook payload structure
    for webhook in received.iter() {
        assert_eq!(webhook.sq_name, "expensive_products");
        assert_eq!(webhook.result_type, "Matched");
        assert!(webhook.matched_properties.is_object());

        let props = webhook.matched_properties.as_object().unwrap();
        println!("Webhook properties: {:?}", props);

        // The properties are stored as nested objects: {"price": {"Integer": 1200}}
        let price_obj = props.get("price").expect("Should have price property");
        let price_value = price_obj
            .as_object()
            .and_then(|o| o.get("Integer"))
            .expect("Should have Integer field");
        let price = price_value.as_i64().unwrap();
        assert!(price > 500, "Price should be > 500, got {}", price);
    }

    println!("✅ Real WebhookSink E2E test PASSED");
    println!("   - HTTP server received {} webhooks", received.len());
    println!("   - All payloads validated successfully");
}

#[tokio::test]
async fn test_webhook_sink_handles_unmatch_events() {
    let setup = setup_test_environment().await;

    // Create Standing Query: stock < 10
    let pattern = StandingQueryPattern::property("stock", FilterCondition::LessThan(10.0));
    let sq_id = setup.sq_manager.register("low_stock_alert", pattern).await;

    // Subscribe webhook sink
    let sinks = setup.sink_registry.list_all().await;
    let sink_id = sinks[0].id;
    setup.sink_registry.subscribe(sq_id, sink_id).await.unwrap();

    // INSERT low-stock product
    setup
        .pg_client
        .simple_query("INSERT INTO Product (id, name, stock) VALUES ('widget', 'Widget', 5)")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    let received_before = setup.received_webhooks.lock().await.len();
    assert_eq!(received_before, 1, "Should receive 1 match webhook");

    // UPDATE to high stock (should trigger unmatch)
    setup
        .pg_client
        .simple_query("UPDATE Product SET stock = 50 WHERE id = 'widget'")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify: Webhook received unmatch event
    let received = setup.received_webhooks.lock().await;
    assert_eq!(
        received.len(),
        2,
        "Should receive 2 webhooks (match + unmatch)"
    );

    let last_webhook = &received[1];
    assert_eq!(last_webhook.result_type, "Unmatched");
    assert_eq!(last_webhook.sq_name, "low_stock_alert");

    println!("✅ WebhookSink unmatch handling test PASSED");
}
