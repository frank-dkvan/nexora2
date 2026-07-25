//! Example 3: Standing Query registration and matching.
//!
//! Run with: `cargo run --example standing_query -p nexora-app`
//!
//! This example demonstrates:
//! - Registering a Standing Query with a PropertyFilter
//! - Registering a Standing Query with a LabelFilter
//! - Triggering matches by setting properties
//! - Checking match counts

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::PropertyValue;
use nexora_standing_query::pattern::{FilterCondition, StandingQueryPattern};
use nexora_standing_query::StandingQueryManager;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 10_000,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ));

    let sq_manager = StandingQueryManager::new(16);
    sq_manager.set_graph(graph.clone()).await;

    println!("=== Nexora-RS Standing Query Example ===\n");

    // Register a Standing Query: alert when speed > 100
    let sq_id = sq_manager
        .register(
            "high-speed-alert",
            StandingQueryPattern::property("speed", FilterCondition::GreaterThan(100.0)),
        )
        .await;
    println!("Registered Standing Query: 'high-speed-alert' (speed > 100)");
    println!("  SQ ID: {}", sq_id);

    // Register a Label filter: match all Person nodes
    let label_sq_id = sq_manager
        .register("all-persons", StandingQueryPattern::label("Person"))
        .await;
    println!("Registered Standing Query: 'all-persons' (label=Person)");
    println!("  SQ ID: {}", label_sq_id);

    // Create a node and set properties
    let forklift = nexora_id::NexoraId::from_hex("666f726b01")?;
    let person = nexora_id::NexoraId::from_hex("7065727301")?;

    // Set forklift speed (below threshold)
    graph
        .set_property(&forklift, "speed", PropertyValue::Float(45.0))
        .await?;
    println!("\nSet forklift speed=45.0 (below threshold)");
    let props: HashMap<String, PropertyValue> = graph
        .get_all_properties(&forklift)
        .await?
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    let matched = sq_manager
        .on_property_change(&forklift, "speed", &PropertyValue::Float(45.0), &props)
        .await;
    println!("  Matches: {}", matched);

    // Increase speed above threshold
    graph
        .set_property(&forklift, "speed", PropertyValue::Float(120.0))
        .await?;
    println!("\nSet forklift speed=120.0 (above threshold!)");
    let props: HashMap<String, PropertyValue> = graph
        .get_all_properties(&forklift)
        .await?
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    let matched = sq_manager
        .on_property_change(&forklift, "speed", &PropertyValue::Float(120.0), &props)
        .await;
    println!("  Matches: {} <- ALERT TRIGGERED!", matched);

    // Create a Person node
    graph
        .set_property(&person, "name", PropertyValue::String("Alice".into()))
        .await?;
    let labels_val = PropertyValue::List(vec![PropertyValue::String("Person".into())]);
    graph
        .set_property(&person, "labels", labels_val.clone())
        .await?;
    println!("\nCreated Person node 'Alice'");
    let props: HashMap<String, PropertyValue> = graph
        .get_all_properties(&person)
        .await?
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    let matched = sq_manager
        .on_property_change(&person, "labels", &labels_val, &props)
        .await;
    println!("  Label matches: {}", matched);

    // Check final match counts
    println!("\n--- Final Match Counts ---");
    let all_sqs = sq_manager.list().await;
    for sq in &all_sqs {
        let count = sq_manager.match_count(sq.id).await;
        println!("  {}: {} matches", sq.name, count);
    }

    // List all registered Standing Queries
    println!("\n--- All Standing Queries ---");
    for sq in &all_sqs {
        println!("  {} (id={}, created={})", sq.name, sq.id, sq.created_at);
    }

    println!("\n=== Standing Query Example Complete ===");
    Ok(())
}
