//! Example 1: Basic CRUD operations with GraphService.
//!
//! Run with: `cargo run --example basic_crud -p nexora-app`
//!
//! This example demonstrates:
//! - Creating graph nodes by setting properties
//! - Reading properties back
//! - Adding edges between nodes
//! - Querying edges
//! - Deleting properties

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::{EdgeDirection, HalfEdge, Symbol};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create an in-memory graph with 4 shards
    let graph = GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 10_000,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    );

    println!("=== Nexora-RS Basic CRUD Example ===\n");

    // Create two nodes by setting properties
    let alice = NexoraId::from_hex("616c696365")?; // "alice" in hex
    let bob = NexoraId::from_hex("626f6200")?; // "bob\0" in hex

    // Set properties for Alice
    graph
        .set_property(&alice, "name", PropertyValue::String("Alice".into()))
        .await?;
    graph
        .set_property(&alice, "age", PropertyValue::Integer(30))
        .await?;
    graph
        .set_property(
            &alice,
            "labels",
            PropertyValue::List(vec![PropertyValue::String("Person".into())]),
        )
        .await?;
    println!("Created node: Alice (age=30)");

    // Set properties for Bob
    graph
        .set_property(&bob, "name", PropertyValue::String("Bob".into()))
        .await?;
    graph
        .set_property(&bob, "age", PropertyValue::Integer(25))
        .await?;
    graph
        .set_property(
            &bob,
            "labels",
            PropertyValue::List(vec![PropertyValue::String("Person".into())]),
        )
        .await?;
    println!("Created node: Bob (age=25)");

    // Read properties back
    let alice_name = graph.get_property(&alice, "name").await?;
    println!("\nRead Alice's name: {:?}", alice_name);

    let bob_age = graph.get_property(&bob, "age").await?;
    println!("Read Bob's age: {:?}", bob_age);

    // Add an edge: Alice KNOWS Bob
    let edge = HalfEdge::new(Symbol::new("KNOWS"), EdgeDirection::Out, bob);
    graph.add_edge(&alice, edge).await?;
    println!("\nAdded edge: Alice -[KNOWS]-> Bob");

    // Query edges
    let edges = graph.get_edges(&alice).await?;
    println!("Alice's edges: {} found", edges.len());
    for e in &edges {
        println!("  -[{}]-> {}", e.edge_type, e.other.to_hex());
    }

    // Check active nodes
    let count = graph.active_node_count().await;
    println!("\nActive nodes: {}", count);

    // Get all properties
    let all_props = graph.get_all_properties(&alice).await?;
    println!("\nAll properties for Alice:");
    for (key, value) in &all_props {
        println!("  {}: {:?}", key, value);
    }

    println!("\n=== CRUD Example Complete ===");
    Ok(())
}
