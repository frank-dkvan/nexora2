//! Example 5: File ingestion (JSONL format).
//!
//! Run with: `cargo run --example file_ingest -p nexora-app`
//!
//! This example demonstrates:
//! - Creating a FileSource (poll+commit) for JSONL data
//! - Draining it through GraphIngestHandler (write_batch sink)
//! - Querying the ingested data

use nexora_core::{BatchDurability, GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_stream::{
    FileSource, FileSourceConfig, GraphIngestHandler, IngestHandler, IngestionSource,
};
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

    println!("=== Nexora-RS File Ingest Example ===\n");

    // Create a temporary JSONL file for demonstration
    let temp_dir = tempfile::tempdir()?;
    let data_file = temp_dir.path().join("sample.jsonl");
    std::fs::write(
        &data_file,
        r#"{"id": "user-001", "name": "Alice", "age": 30, "role": "admin"}
{"id": "user-002", "name": "Bob", "age": 25, "role": "operator"}
{"id": "user-003", "name": "Charlie", "age": 35, "role": "viewer"}
{"id": "user-004", "name": "Diana", "age": 28, "role": "operator"}
{"id": "user-005", "name": "Eve", "age": 32, "role": "admin"}
"#,
    )?;

    println!(
        "Created sample JSONL file: {} (5 records)",
        data_file.display()
    );

    // Create the poll+commit file source (id_field "id" maps to the node id).
    let source = FileSource::new(FileSourceConfig {
        path: data_file.clone(),
        topic: "example-ingest".into(),
        id_field: "id".into(),
        label_field: None,
        event_time_field: None,
        event_time_unit: Default::default(),
        max_batch: 256,
    });
    source.connect().await?;
    println!("Ingest source connected");

    // The shared graph-write sink: batches → write_batch.
    let handler = GraphIngestHandler::new(graph.clone(), BatchDurability::WaitDurable);

    // Drain the file to EOF.
    println!("Running ingest pipeline...\n");
    let mut records = 0usize;
    while let Some(batch) = source.poll().await? {
        records += handler
            .handle_batch(&batch)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
    }
    println!("--- Ingest Statistics ---");
    println!("  Records ingested: {records}");

    // Verify: query the graph to see ingested data
    let active = graph.active_node_count().await;
    println!("\nActive nodes after ingest: {active}");

    // Query one of the ingested nodes
    let result = nexora_cypher::execute_cypher(
        &graph,
        "MATCH (n) RETURN n.name, n.age, n.role ORDER BY n.name",
    )
    .await?;

    if let nexora_cypher::CypherResult::Rows { columns, rows } = result {
        println!("\n--- Query Results ---");
        println!("Columns: {columns:?}");
        for row in &rows {
            println!("  {row:?}");
        }
    }

    println!("\n=== File Ingest Example Complete ===");
    Ok(())
}
