//! Test: UPDATE and DELETE automatically trigger Standing Query evaluation
//!
//! This test verifies that:
//! 1. UPDATE statements extract node IDs from WHERE clause and auto-trigger SQ
//! 2. DELETE statements extract node IDs from WHERE clause and auto-trigger SQ
//! 3. No manual `on_property_change` calls are needed

use std::sync::Arc;
use std::time::Duration;

use nexora_core::materialized_view::{ColumnDef, DataType, MaterializedViewManager, RefreshMode};
use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::PropertyValue;
use nexora_pgwire::{spawn_pg_server, PgConfig};
use nexora_standing_query::pattern::{FilterCondition, StandingQueryPattern};
use nexora_standing_query::StandingQueryManager;
use tokio_postgres::{Client, NoTls};

struct TestSetup {
    #[allow(dead_code)] // Graph kept alive for test duration
    graph: Arc<GraphService>,
    sq_manager: Arc<StandingQueryManager>,
    mv_manager: Arc<MaterializedViewManager>,
    pg_client: Client,
    #[allow(dead_code)]
    server_handle: nexora_pgwire::PgServerHandle,
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

    let pg_config = PgConfig {
        port: 0,
        bind_addr: "127.0.0.1".to_owned(),
        trust: true,
        max_connections: 10,
        idle_timeout_secs: 300,
        shutdown_grace_secs: 5,
        ..PgConfig::default()
    };

    let query_pool = std::sync::Arc::new(nexora_core::query_pool::QueryPool::new(4));

    let server_handle = spawn_pg_server(
        graph.clone(),
        mv_manager.clone(),
        Some(sq_manager.clone()),
        query_pool,
        pg_config,
    )
    .await
    .expect("PG server should start");

    let mut pg_client_config = tokio_postgres::Config::new();
    pg_client_config
        .host(server_handle.local_addr().ip().to_string())
        .port(server_handle.local_addr().port())
        .user("admin")
        .dbname("nexora");

    let (pg_client, connection) = pg_client_config.connect(NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {}", e);
        }
    });

    TestSetup {
        graph,
        sq_manager,
        mv_manager,
        pg_client,
        server_handle,
    }
}

#[tokio::test]
async fn test_update_auto_triggers_sq_without_manual_call() {
    let setup = setup_test_environment().await;

    // Create Standing Query: match nodes with speed > 100
    let sq_id = setup
        .sq_manager
        .register(
            "fast_vehicles",
            StandingQueryPattern::property("speed", FilterCondition::GreaterThan(100.0)),
        )
        .await;

    // Create MV connected to SQ
    let mv_id = setup
        .mv_manager
        .create_view(
            "fast_vehicle_count".to_string(),
            "MATCH (v:Vehicle) WHERE v.speed > 100 RETURN v.id AS vehicle_id, v.speed AS speed"
                .to_string(),
            vec![
                ColumnDef {
                    name: "vehicle_id".to_string(),
                    data_type: DataType::String,
                },
                ColumnDef {
                    name: "speed".to_string(),
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
                        let row = nexora_core::materialized_view::MaterializedRow {
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

    // Insert a fast vehicle via PG-Wire
    setup
        .pg_client
        .simple_query("INSERT INTO Vehicle (id, speed, model) VALUES ('v100', 150, 'FastCar')")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify initial match
    assert_eq!(
        setup.sq_manager.match_count(sq_id).await,
        1,
        "Should have 1 initial match"
    );
    assert_eq!(
        setup.mv_manager.query_all(&mv_id).await.unwrap().len(),
        1,
        "MV should have 1 row"
    );

    // UPDATE speed to below threshold via PG-Wire
    // This should AUTOMATICALLY trigger SQ evaluation without manual on_property_change
    setup
        .pg_client
        .simple_query("UPDATE Vehicle SET speed = 80 WHERE id = 'v100'")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Verify unmatch happened automatically
    let match_count = setup.sq_manager.match_count(sq_id).await;
    let mv_rows = setup.mv_manager.query_all(&mv_id).await.unwrap();

    assert_eq!(
        match_count, 0,
        "SQ should have 0 matches after UPDATE (auto-triggered)"
    );
    assert_eq!(
        mv_rows.len(),
        0,
        "MV should be empty after unmatch (auto-triggered)"
    );

    println!("✅ UPDATE automatically triggered SQ without manual on_property_change call");
}

#[tokio::test]
async fn test_delete_auto_triggers_sq_without_manual_call() {
    let setup = setup_test_environment().await;

    // Create Standing Query
    let sq_id = setup
        .sq_manager
        .register(
            "active_products",
            StandingQueryPattern::property(
                "status",
                FilterCondition::Equals(PropertyValue::String("active".into())),
            ),
        )
        .await;

    // Create MV
    let mv_id = setup
        .mv_manager
        .create_view(
            "active_product_list".to_string(),
            "MATCH (p:Product) WHERE p.status = 'active' RETURN p.id AS product_id".to_string(),
            vec![ColumnDef {
                name: "product_id".to_string(),
                data_type: DataType::String,
            }],
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
                        let row = nexora_core::materialized_view::MaterializedRow {
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

    // Insert an active product
    setup
        .pg_client
        .simple_query("INSERT INTO Product (id, status, name) VALUES ('p500', 'active', 'Widget')")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        setup.sq_manager.match_count(sq_id).await,
        1,
        "Should have 1 initial match"
    );
    assert_eq!(
        setup.mv_manager.query_all(&mv_id).await.unwrap().len(),
        1,
        "MV should have 1 row"
    );

    // DELETE the product via PG-Wire
    // This should AUTOMATICALLY trigger SQ evaluation
    setup
        .pg_client
        .simple_query("DELETE FROM Product WHERE id = 'p500'")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Verify unmatch happened automatically
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

    println!("✅ DELETE automatically triggered SQ without manual on_property_change call");
}

#[tokio::test]
async fn test_update_with_in_clause_extracts_multiple_ids() {
    let setup = setup_test_environment().await;

    let sq_id = setup
        .sq_manager
        .register(
            "hot_sensors",
            StandingQueryPattern::property("temperature", FilterCondition::GreaterThan(50.0)),
        )
        .await;

    // Insert multiple sensors
    setup
        .pg_client
        .simple_query(
            "INSERT INTO Sensor (id, temperature) VALUES ('s1', 70), ('s2', 80), ('s3', 90)",
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;
    let initial_matches = setup.sq_manager.match_count(sq_id).await;
    println!("Initial matches: {}", initial_matches);
    assert_eq!(initial_matches, 3, "Should have 3 initial matches");

    // UPDATE multiple sensors using IN clause
    println!("Running UPDATE with IN clause...");
    setup
        .pg_client
        .simple_query("UPDATE Sensor SET temperature = 30 WHERE id IN ('s1', 's2')")
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    let match_count = setup.sq_manager.match_count(sq_id).await;
    println!("Final matches: {}", match_count);

    // After update, only s3 should match (temperature=90 > 50); s1 and s2
    // unmatch (temperature=30 < 50). This previously failed because the write
    // executor's WHERE evaluator rejected `id IN [...]` (the list right side hit
    // the scalar path and errored "Unsupported WHERE expression: List(...)").
    // Fixed by adding a dedicated IN arm to eval_predicate.
    assert_eq!(
        match_count, 1,
        "Should have 1 match remaining (s3 with temp=90)"
    );

    println!("✅ UPDATE with IN clause correctly extracts multiple node IDs and triggers SQ");
}
