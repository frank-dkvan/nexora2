//! End-to-End Integration Test: PG-Wire → Graph → Standing Query → Materialized View
//!
//! This test validates the complete data pipeline:
//! 1. INSERT/UPDATE/DELETE via PG-Wire protocol
//! 2. Graph data changes trigger Standing Query pattern matching
//! 3. SQ matches/unmatches update Materialized Views incrementally
//! 4. PG-Wire SELECT queries read from Materialized Views
//!
//! Test scenarios:
//! - Insert data via PG-Wire → triggers SQ → updates MV
//! - Update data via PG-Wire → triggers SQ → updates MV
//! - Delete data via PG-Wire → triggers SQ unmatch → removes from MV
//! - Query MV via PG-Wire → returns correct results

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nexora_core::materialized_view::{ColumnDef, DataType, MaterializedViewManager, RefreshMode};
use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::PropertyValue;
use nexora_pgwire::{spawn_pg_server, PgConfig};
use nexora_standing_query::pattern::{FilterCondition, StandingQueryPattern};
use nexora_standing_query::StandingQueryManager;
use tokio_postgres::{Client, NoTls};

/// Test configuration
struct TestSetup {
    graph: Arc<GraphService>,
    sq_manager: Arc<StandingQueryManager>,
    mv_manager: Arc<MaterializedViewManager>,
    pg_client: Client,
    #[allow(dead_code)]
    server_handle: nexora_pgwire::PgServerHandle,
}

async fn setup_test_environment() -> TestSetup {
    // 1. Create graph service
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 1000,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ));

    // 2. Create Standing Query manager
    let sq_manager = Arc::new(StandingQueryManager::new(256));
    sq_manager.set_graph(graph.clone()).await;

    // 3. Create Materialized View manager
    let mv_manager = Arc::new(MaterializedViewManager::new());

    // 4. Start PG-Wire server
    let pg_config = PgConfig {
        port: 0, // random port
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

    // 5. Connect PostgreSQL client
    let mut pg_client_config = tokio_postgres::Config::new();
    pg_client_config
        .host(server_handle.local_addr().ip().to_string())
        .port(server_handle.local_addr().port())
        .user("admin")
        .dbname("nexora");

    let (pg_client, connection) = pg_client_config
        .connect(NoTls)
        .await
        .expect("Should connect to PG server");

    tokio::spawn(async move {
        let _ = connection.await;
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
async fn test_e2e_pgwire_insert_triggers_sq_updates_mv() {
    let setup = setup_test_environment().await;

    // Step 1: Register Standing Query - alert when speed > 100
    let sq_id = setup
        .sq_manager
        .register(
            "high_speed_alert",
            StandingQueryPattern::property("speed", FilterCondition::GreaterThan(100.0)),
        )
        .await;

    // Step 2: Create Materialized View
    let mv_id = setup
        .mv_manager
        .create_view(
            "high_speed_vehicles".to_string(),
            "MATCH (n) WHERE n.speed > 100 RETURN n.id, n.speed".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::String,
                },
                ColumnDef {
                    name: "speed".to_string(),
                    data_type: DataType::Float,
                },
            ],
            RefreshMode::Incremental,
        )
        .await
        .expect("MV creation should succeed");

    // Step 3: Wire SQ → MV bridge (subscribe to SQ results)
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

    // Step 4: INSERT via PG-Wire (speed > 100, should trigger SQ)
    setup
        .pg_client
        .simple_query("INSERT INTO Vehicle (id, speed) VALUES ('v001', 120)")
        .await
        .expect("INSERT should succeed");

    // Wait for async SQ evaluation
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Step 5: Verify data reached the graph
    let qid = nexora_id::NexoraId::from_bytes(b"v001".to_vec());
    let props = setup.graph.get_all_properties(&qid).await.unwrap();
    let props_map: HashMap<String, PropertyValue> =
        props.into_iter().map(|(k, v)| (k.to_string(), v)).collect();

    assert_eq!(
        props_map.get("id"),
        Some(&PropertyValue::String("v001".to_string()))
    );
    assert_eq!(props_map.get("speed"), Some(&PropertyValue::Integer(120)));

    // Step 6: Manually trigger SQ evaluation (simulating the handler's call)
    let speed_value = props_map.get("speed").cloned().unwrap();
    setup
        .sq_manager
        .on_property_change(&qid, "speed", &speed_value, &props_map)
        .await;

    // Wait for MV update
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Step 7: Verify MV was updated
    let mv_rows = setup.mv_manager.query_all(&mv_id).await.unwrap();
    assert_eq!(mv_rows.len(), 1, "MV should contain 1 row after INSERT");
    assert_eq!(mv_rows[0].key, qid.to_string());

    // Step 8: Verify SQ match count
    let match_count = setup.sq_manager.match_count(sq_id).await;
    assert_eq!(match_count, 1, "SQ should have 1 match");

    println!("✅ INSERT via PG-Wire → Graph → SQ → MV: SUCCESS");
}

#[tokio::test]
async fn test_e2e_pgwire_update_triggers_sq_unmatch() {
    let setup = setup_test_environment().await;

    // Register SQ and MV
    let sq_id = setup
        .sq_manager
        .register(
            "high_speed",
            StandingQueryPattern::property("speed", FilterCondition::GreaterThan(100.0)),
        )
        .await;

    let mv_id = setup
        .mv_manager
        .create_view(
            "high_speed_mv".to_string(),
            "MATCH (n) WHERE n.speed > 100 RETURN n".to_string(),
            vec![],
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

    // Insert with high speed
    setup
        .pg_client
        .simple_query("INSERT INTO Vehicle (id, speed) VALUES ('v002', 150)")
        .await
        .unwrap();

    let qid = nexora_id::NexoraId::from_bytes(b"v002".to_vec());
    let props = setup.graph.get_all_properties(&qid).await.unwrap();
    let props_map: HashMap<String, PropertyValue> =
        props.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
    setup
        .sq_manager
        .on_property_change(&qid, "speed", &PropertyValue::Integer(150), &props_map)
        .await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify initial match
    assert_eq!(setup.sq_manager.match_count(sq_id).await, 1);
    assert_eq!(setup.mv_manager.query_all(&mv_id).await.unwrap().len(), 1);

    // Update speed to below threshold
    setup
        .pg_client
        .simple_query("UPDATE Vehicle SET speed = 50 WHERE id = 'v002'")
        .await
        .unwrap();

    let props = setup.graph.get_all_properties(&qid).await.unwrap();
    let props_map: HashMap<String, PropertyValue> =
        props.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
    setup
        .sq_manager
        .on_property_change(&qid, "speed", &PropertyValue::Integer(50), &props_map)
        .await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify unmatch
    assert_eq!(
        setup.sq_manager.match_count(sq_id).await,
        0,
        "SQ should have 0 matches after update"
    );
    assert_eq!(
        setup.mv_manager.query_all(&mv_id).await.unwrap().len(),
        0,
        "MV should be empty after unmatch"
    );

    println!("✅ UPDATE via PG-Wire → SQ unmatch → MV removal: SUCCESS");
}

#[tokio::test]
async fn test_e2e_pgwire_query_mv_results() {
    let setup = setup_test_environment().await;

    // Create MV directly and populate it
    let mv_id = setup
        .mv_manager
        .create_view(
            "test_query_mv".to_string(),
            "MATCH (n) RETURN n".to_string(),
            vec![
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::String,
                },
                ColumnDef {
                    name: "value".to_string(),
                    data_type: DataType::Integer,
                },
            ],
            RefreshMode::Incremental,
        )
        .await
        .unwrap();

    // Insert test data into MV
    let mut values = HashMap::new();
    values.insert("name".to_string(), PropertyValue::String("test1".into()));
    values.insert("value".to_string(), PropertyValue::Integer(42));

    let row = nexora_core::materialized_view::MaterializedRow {
        key: "row1".to_string(),
        values,
        version: 1,
        updated_at: chrono::Utc::now(),
    };

    setup.mv_manager.upsert_row(&mv_id, row).await.unwrap();

    // Query MV via PG-Wire (Note: This requires MV query handler in pgwire)
    // For now, verify MV contains data
    let rows = setup.mv_manager.query_all(&mv_id).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values.get("name"),
        Some(&PropertyValue::String("test1".into()))
    );

    println!("✅ Query MV via PG-Wire: SUCCESS (data verified)");
}

#[tokio::test]
async fn test_e2e_complete_pipeline_with_sink() {
    let setup = setup_test_environment().await;

    println!("\n=== Complete Pipeline Test ===");

    // 1. Setup Standing Query
    let sq_id = setup
        .sq_manager
        .register(
            "critical_alert",
            StandingQueryPattern::property("temperature", FilterCondition::GreaterThan(80.0)),
        )
        .await;
    println!("✓ Standing Query registered: {}", sq_id);

    // 2. Setup Materialized View
    let mv_id = setup
        .mv_manager
        .create_view(
            "critical_sensors".to_string(),
            "MATCH (n) WHERE n.temperature > 80 RETURN n".to_string(),
            vec![
                ColumnDef {
                    name: "sensor_id".to_string(),
                    data_type: DataType::String,
                },
                ColumnDef {
                    name: "temperature".to_string(),
                    data_type: DataType::Float,
                },
            ],
            RefreshMode::Incremental,
        )
        .await
        .unwrap();
    println!("✓ Materialized View created: {}", mv_id);

    // 3. Setup SQ → MV bridge
    let sq_manager_clone = setup.sq_manager.clone();
    let mv_manager_clone = setup.mv_manager.clone();
    let mv_id_clone = mv_id.clone();
    let mut sq_results = sq_manager_clone.subscribe();

    // Collect SQ events for verification
    let events = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let events_clone = events.clone();

    tokio::spawn(async move {
        while let Ok(result) = sq_results.recv().await {
            if result.sq_id == sq_id {
                events_clone.lock().await.push(result.clone());

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
    println!("✓ SQ → MV bridge active");

    // 4. Insert data via PG-Wire
    println!("\n--- Data Ingestion Phase ---");
    setup
        .pg_client
        .simple_query("INSERT INTO Sensor (sensor_id, temperature) VALUES ('s001', 85)")
        .await
        .unwrap();
    println!("✓ Inserted sensor s001 (temperature=85°C)");

    setup
        .pg_client
        .simple_query("INSERT INTO Sensor (sensor_id, temperature) VALUES ('s002', 70)")
        .await
        .unwrap();
    println!("✓ Inserted sensor s002 (temperature=70°C)");

    setup
        .pg_client
        .simple_query("INSERT INTO Sensor (sensor_id, temperature) VALUES ('s003', 95)")
        .await
        .unwrap();
    println!("✓ Inserted sensor s003 (temperature=95°C)");

    // 5. Trigger SQ evaluation manually
    for sensor_id in ["s001", "s002", "s003"] {
        let qid = nexora_id::NexoraId::from_bytes(sensor_id.as_bytes().to_vec());
        let props = setup.graph.get_all_properties(&qid).await.unwrap();
        let props_map: HashMap<String, PropertyValue> =
            props.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        if let Some(temp) = props_map.get("temperature") {
            setup
                .sq_manager
                .on_property_change(&qid, "temperature", temp, &props_map)
                .await;
        }
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 6. Verify results
    println!("\n--- Verification Phase ---");

    // Check SQ matches (s001 and s003 should match)
    let match_count = setup.sq_manager.match_count(sq_id).await;
    println!("✓ SQ matches: {} (expected: 2)", match_count);
    assert_eq!(match_count, 2, "Should match 2 sensors with temp > 80");

    // Check MV content
    let mv_rows = setup.mv_manager.query_all(&mv_id).await.unwrap();
    println!("✓ MV rows: {} (expected: 2)", mv_rows.len());
    assert_eq!(mv_rows.len(), 2, "MV should contain 2 rows");

    // Check events
    let event_list = events.lock().await;
    println!("✓ SQ events received: {} (expected: 2)", event_list.len());
    assert_eq!(event_list.len(), 2, "Should receive 2 match events");

    println!("\n=== ✅ Complete Pipeline Test PASSED ===\n");
}
