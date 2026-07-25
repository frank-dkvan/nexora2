//! Complete DELETE operation chain test
//!
//! This test validates the complete DELETE data pipeline:
//! 1. DELETE via PG-Wire protocol (single and batch)
//! 2. DELETE automatically triggers Standing Query evaluation
//! 3. SQ unmatches are sent to downstream via broadcast channel
//! 4. Materialized Views are decremented correctly
//! 5. PG-Wire SELECT queries verify the deletions

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
    #[allow(dead_code)] // Graph kept alive for test duration
    graph: Arc<GraphService>,
    sq_manager: Arc<StandingQueryManager>,
    mv_manager: Arc<MaterializedViewManager>,
    _server: nexora_pgwire::PgServerHandle,
}

async fn setup_test_environment() -> TestSetup {
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 100,
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
        idle_timeout_secs: 30,
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
        graph,
        sq_manager,
        mv_manager,
        _server: server,
    }
}

#[tokio::test]
async fn test_delete_single_record_full_chain() {
    let setup = setup_test_environment().await;

    // Create Standing Query: Products with price > 400
    // Note: Only filter by property, not label, since SQL INSERT may not auto-add labels
    let pattern = StandingQueryPattern::property("price", FilterCondition::GreaterThan(400.0));
    let sq_id = setup
        .sq_manager
        .register("high_price_products", pattern)
        .await;

    // Create Materialized View
    let mv_id = setup
        .mv_manager
        .create_view(
            "high_price_count".to_string(),
            "MATCH (p:Product) WHERE p.price > 400 RETURN p.id, p.price".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::String,
                },
                ColumnDef {
                    name: "price".to_string(),
                    data_type: DataType::Integer,
                },
            ],
            RefreshMode::Incremental,
        )
        .await
        .unwrap();

    // Wire SQ → MV bridge
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

    // INSERT a high-price product via PG-Wire
    setup
        .pg_client
        .simple_query("INSERT INTO Product (id, name, price) VALUES ('p100', 'Laptop', 500)")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(
        setup.sq_manager.match_count(sq_id).await,
        1,
        "Should have 1 match after INSERT"
    );
    assert_eq!(
        setup.mv_manager.query_all(&mv_id).await.unwrap().len(),
        1,
        "MV should have 1 row"
    );

    // DELETE the product via PG-Wire
    // This should AUTOMATICALLY trigger SQ evaluation and send unmatch event
    setup
        .pg_client
        .simple_query("DELETE FROM Product WHERE id = 'p100'")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;

    let match_count = setup.sq_manager.match_count(sq_id).await;
    let mv_rows = setup.mv_manager.query_all(&mv_id).await.unwrap();

    assert_eq!(
        match_count, 0,
        "SQ should have 0 matches after DELETE (auto-triggered)"
    );
    assert_eq!(
        mv_rows.len(),
        0,
        "MV should be empty after DELETE (auto-triggered)"
    );

    // Verify via PG-Wire SELECT
    let messages = setup
        .pg_client
        .simple_query("SELECT * FROM Product WHERE id = 'p100'")
        .await
        .unwrap();
    let row_count = messages
        .iter()
        .filter(|msg| matches!(msg, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        row_count, 0,
        "Deleted product should not be found via SELECT"
    );

    println!("✅ DELETE single record chain test PASSED");
}

#[tokio::test]
async fn test_delete_batch_records_full_chain() {
    let setup = setup_test_environment().await;

    // Create Standing Query: Products with price > 400
    // Note: Only filter by property, not label, since SQL INSERT may not auto-add labels
    let pattern = StandingQueryPattern::property("price", FilterCondition::GreaterThan(400.0));
    let sq_id = setup
        .sq_manager
        .register("high_price_products", pattern)
        .await;

    // Create Materialized View
    let mv_id = setup
        .mv_manager
        .create_view(
            "high_price_count".to_string(),
            "MATCH (p:Product) WHERE p.price > 400 RETURN p.id, p.price".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::String,
                },
                ColumnDef {
                    name: "price".to_string(),
                    data_type: DataType::Integer,
                },
            ],
            RefreshMode::Incremental,
        )
        .await
        .unwrap();

    // Wire SQ → MV bridge
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

    // INSERT multiple high-price products via PG-Wire
    setup
        .pg_client
        .simple_query(
            "INSERT INTO Product (id, name, price) VALUES \
             ('p201', 'Phone', 600), \
             ('p202', 'Tablet', 700), \
             ('p203', 'Watch', 800)",
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(250)).await;

    assert_eq!(
        setup.sq_manager.match_count(sq_id).await,
        3,
        "Should have 3 matches after INSERT"
    );
    assert_eq!(
        setup.mv_manager.query_all(&mv_id).await.unwrap().len(),
        3,
        "MV should have 3 rows"
    );

    // DELETE two products via PG-Wire (using two separate statements instead of IN clause)
    setup
        .pg_client
        .simple_query("DELETE FROM Product WHERE id = 'p201'")
        .await
        .unwrap();

    setup
        .pg_client
        .simple_query("DELETE FROM Product WHERE id = 'p202'")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(250)).await;

    let match_count = setup.sq_manager.match_count(sq_id).await;
    let mv_rows = setup.mv_manager.query_all(&mv_id).await.unwrap();

    assert_eq!(match_count, 1, "SQ should have 1 match after batch DELETE");
    assert_eq!(mv_rows.len(), 1, "MV should have 1 row after batch DELETE");

    println!("✅ DELETE batch records chain test PASSED");
}

#[tokio::test]
async fn test_delete_no_match_does_not_affect_mv() {
    let setup = setup_test_environment().await;

    // Create Standing Query: Products with price > 500
    let pattern = StandingQueryPattern::property("price", FilterCondition::GreaterThan(500.0));
    let sq_id = setup
        .sq_manager
        .register("expensive_products", pattern)
        .await;

    // Create Materialized View
    let mv_id = setup
        .mv_manager
        .create_view(
            "expensive_count".to_string(),
            "MATCH (p:Product) WHERE p.price > 500 RETURN p.id, p.price".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::String,
                },
                ColumnDef {
                    name: "price".to_string(),
                    data_type: DataType::Integer,
                },
            ],
            RefreshMode::Incremental,
        )
        .await
        .unwrap();

    // Wire SQ → MV bridge
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

    // INSERT a high-price product
    setup
        .pg_client
        .simple_query("INSERT INTO Product (id, name, price) VALUES ('p400', 'Premium', 600)")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(setup.sq_manager.match_count(sq_id).await, 1);
    assert_eq!(setup.mv_manager.query_all(&mv_id).await.unwrap().len(), 1);

    // INSERT and DELETE a low-price product (does not match the SQ)
    setup
        .pg_client
        .simple_query("INSERT INTO Product (id, name, price) VALUES ('p401', 'Cheap', 100)")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    setup
        .pg_client
        .simple_query("DELETE FROM Product WHERE id = 'p401'")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;

    // MV should still have 1 row (the expensive product is not affected)
    assert_eq!(
        setup.sq_manager.match_count(sq_id).await,
        1,
        "SQ should still have 1 match"
    );
    assert_eq!(
        setup.mv_manager.query_all(&mv_id).await.unwrap().len(),
        1,
        "MV should still have 1 row"
    );

    println!("✅ DELETE non-matching record test PASSED");
}
