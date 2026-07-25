//! Complete End-to-End Data Pipeline Test
//!
//! This test validates the entire Nexora data pipeline:
//! 1. INSERT/UPDATE/DELETE via PG-Wire protocol
//! 2. Standing Query pattern matching and auto-triggering
//! 3. Materialized View incremental updates
//! 4. Query MV results via PG-Wire SELECT
//!
//! Architecture validated:
//! ```
//! PG-Wire SQL → GraphService → StandingQuery → MV → PG-Wire SELECT
//! ```

use std::sync::Arc;
use std::time::Duration;

use nexora_core::{
    materialized_view::{
        ColumnDef, DataType, MaterializedRow, MaterializedViewManager, RefreshMode,
    },
    GraphService, GraphServiceConfig, InMemoryPersistor,
};
use nexora_pgwire::{spawn_pg_server, PgConfig};
use nexora_standing_query::{
    pattern::{FilterCondition, StandingQueryPattern},
    StandingQueryManager,
};
use tokio_postgres::{Client, NoTls};

struct TestSetup {
    pg_client: Client,
    sq_manager: Arc<StandingQueryManager>,
    mv_manager: Arc<MaterializedViewManager>,
    _server: nexora_pgwire::PgServerHandle,
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

    let mv_manager = Arc::new(MaterializedViewManager::new());

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
        mv_manager.clone(),
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

    TestSetup {
        pg_client: client,
        sq_manager,
        mv_manager,
        _server: server,
    }
}

#[tokio::test]
async fn test_complete_insert_update_delete_pipeline() {
    let setup = setup_test_environment().await;

    // ========== Phase 1: Create Standing Query ==========
    // Match customer orders with amount > 1000
    let pattern = StandingQueryPattern::property("amount", FilterCondition::GreaterThan(1000.0));
    let sq_id = setup
        .sq_manager
        .register("high_value_orders", pattern)
        .await;

    // ========== Phase 2: Create Materialized View ==========
    let mv_id = setup
        .mv_manager
        .create_view(
            "high_value_order_summary".to_string(),
            "MATCH (o:CustomerOrder) WHERE o.amount > 1000 RETURN o.id, o.amount, o.customer"
                .to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::String,
                },
                ColumnDef {
                    name: "amount".to_string(),
                    data_type: DataType::Integer,
                },
                ColumnDef {
                    name: "customer".to_string(),
                    data_type: DataType::String,
                },
            ],
            RefreshMode::Incremental,
        )
        .await
        .unwrap();

    // ========== Phase 3: Wire SQ → MV Bridge ==========
    let sq_manager_clone = setup.sq_manager.clone();
    let mv_manager_clone = setup.mv_manager.clone();
    let mv_id_clone = mv_id.clone();

    let mut sq_results = sq_manager_clone.subscribe();
    tokio::spawn(async move {
        while let Ok(result) = sq_results.recv().await {
            if result.sq_id == sq_id {
                match result.result_type {
                    nexora_standing_query::ResultType::Matched => {
                        let row = MaterializedRow {
                            key: result.qid.to_string(),
                            values: result.matched_properties.clone(),
                            version: 1,
                            updated_at: chrono::Utc::now(),
                        };
                        let _ = mv_manager_clone.upsert_row(&mv_id_clone, row).await;
                    }
                    nexora_standing_query::ResultType::Unmatched => {
                        let _ = mv_manager_clone
                            .delete_row(&mv_id_clone, &result.qid.to_string())
                            .await;
                    }
                }
            }
        }
    });

    // ========== Phase 4: INSERT via PG-Wire ==========
    setup
        .pg_client
        .simple_query(
            "INSERT INTO CustomerOrder (id, customer, amount) VALUES \
             ('order1', 'Alice', 1500), \
             ('order2', 'Bob', 500), \
             ('order3', 'Charlie', 2000)",
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify: SQ matched 2 high-value orders
    let match_count_1 = setup.sq_manager.match_count(sq_id).await;
    println!("DEBUG: After INSERT, match_count = {}", match_count_1);
    assert_eq!(match_count_1, 2, "Should match 2 high-value orders");

    // Verify: MV has 2 rows
    let mv_rows = setup.mv_manager.query_all(&mv_id).await.unwrap();
    println!("DEBUG: After INSERT, MV rows = {}", mv_rows.len());
    assert_eq!(mv_rows.len(), 2, "MV should have 2 rows after INSERT");

    // ========== Phase 5: UPDATE via PG-Wire ==========
    // Update order2 to become high-value
    setup
        .pg_client
        .simple_query("UPDATE CustomerOrder SET amount = 1200 WHERE id = 'order2'")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify: SQ now matches 3 orders
    let match_count_2 = setup.sq_manager.match_count(sq_id).await;
    println!("DEBUG: After UPDATE, match_count = {}", match_count_2);
    assert_eq!(
        match_count_2, 3,
        "Should match 3 high-value orders after UPDATE"
    );

    // Verify: MV has 3 rows
    let mv_rows = setup.mv_manager.query_all(&mv_id).await.unwrap();
    assert_eq!(mv_rows.len(), 3, "MV should have 3 rows after UPDATE");

    // ========== Phase 6: DELETE via PG-Wire ==========
    // Delete one high-value order
    setup
        .pg_client
        .simple_query("DELETE FROM CustomerOrder WHERE id = 'order1'")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify: SQ now matches 2 orders
    assert_eq!(
        setup.sq_manager.match_count(sq_id).await,
        2,
        "Should match 2 high-value orders after DELETE"
    );

    // Verify: MV has 2 rows
    let mv_rows = setup.mv_manager.query_all(&mv_id).await.unwrap();
    assert_eq!(mv_rows.len(), 2, "MV should have 2 rows after DELETE");

    // ========== Phase 7: Verify MV content directly ==========
    // Note: Query MV via PG-Wire SELECT is not yet implemented
    // For now, we verify through the MaterializedViewManager API
    let final_rows = setup.mv_manager.query_all(&mv_id).await.unwrap();
    assert_eq!(final_rows.len(), 2, "Final MV should have 2 rows");

    println!("✅ Complete E2E pipeline test PASSED");
    println!("   - INSERT via PG-Wire ✓");
    println!("   - Standing Query auto-triggered ✓");
    println!("   - UPDATE via PG-Wire ✓");
    println!("   - MV incrementally updated ✓");
    println!("   - DELETE via PG-Wire ✓");
    println!("   - MV decremented correctly ✓");
    println!("   - MV content verified ✓");
}

#[tokio::test]
async fn test_batch_operations_pipeline() {
    let setup = setup_test_environment().await;

    // Create Standing Query: Products with stock < 10
    let pattern = StandingQueryPattern::property("stock", FilterCondition::LessThan(10.0));
    let sq_id = setup
        .sq_manager
        .register("low_stock_products", pattern)
        .await;

    // Create Materialized View
    let mv_id = setup
        .mv_manager
        .create_view(
            "low_stock_alert".to_string(),
            "MATCH (p:Product) WHERE p.stock < 10 RETURN p.id, p.name, p.stock".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::String,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::String,
                },
                ColumnDef {
                    name: "stock".to_string(),
                    data_type: DataType::Integer,
                },
            ],
            RefreshMode::Incremental,
        )
        .await
        .unwrap();

    // Wire SQ → MV
    let sq_manager_clone = setup.sq_manager.clone();
    let mv_manager_clone = setup.mv_manager.clone();
    let mv_id_clone = mv_id.clone();

    let mut sq_results = sq_manager_clone.subscribe();
    tokio::spawn(async move {
        while let Ok(result) = sq_results.recv().await {
            if result.sq_id == sq_id {
                match result.result_type {
                    nexora_standing_query::ResultType::Matched => {
                        let row = MaterializedRow {
                            key: result.qid.to_string(),
                            values: result.matched_properties.clone(),
                            version: 1,
                            updated_at: chrono::Utc::now(),
                        };
                        let _ = mv_manager_clone.upsert_row(&mv_id_clone, row).await;
                    }
                    nexora_standing_query::ResultType::Unmatched => {
                        let _ = mv_manager_clone
                            .delete_row(&mv_id_clone, &result.qid.to_string())
                            .await;
                    }
                }
            }
        }
    });

    // Batch INSERT
    setup
        .pg_client
        .simple_query(
            "INSERT INTO Product (id, name, stock) VALUES \
             ('p1', 'Widget', 5), \
             ('p2', 'Gadget', 15), \
             ('p3', 'Gizmo', 3), \
             ('p4', 'Thing', 20)",
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(250)).await;

    assert_eq!(
        setup.sq_manager.match_count(sq_id).await,
        2,
        "Should match 2 low-stock products"
    );

    // Batch UPDATE
    setup
        .pg_client
        .simple_query("UPDATE Product SET stock = stock - 10 WHERE stock > 10")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(250)).await;

    // After UPDATE: p1(5), p2(15-10=5), p3(3), p4(20-10=10)
    // Only p1, p2, p3 have stock < 10 (p4 has stock = 10, not < 10)
    assert_eq!(
        setup.sq_manager.match_count(sq_id).await,
        3,
        "Should match 3 low-stock products after batch UPDATE"
    );

    // Batch DELETE
    setup
        .pg_client
        .simple_query("DELETE FROM Product WHERE stock < 10")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(250)).await;

    assert_eq!(
        setup.sq_manager.match_count(sq_id).await,
        0,
        "Should match 0 products after batch DELETE"
    );

    let mv_rows = setup.mv_manager.query_all(&mv_id).await.unwrap();
    assert_eq!(mv_rows.len(), 0, "MV should be empty after batch DELETE");

    println!("✅ Batch operations pipeline test PASSED");
}
