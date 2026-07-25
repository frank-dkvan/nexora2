//! Example 4: Vector similarity search with HNSW.
//!
//! Run with: `cargo run --example vector_search -p nexora-app`
//!
//! This example demonstrates:
//! - Creating an HNSW index
//! - Inserting vectors for graph nodes
//! - Searching for k-nearest neighbors
//! - Removing vectors from the index

use nexora_hnsw::{HnswConfig, HnswIndex};
use nexora_id::NexoraId;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Nexora-RS Vector Search Example ===\n");

    // Create an HNSW index with default config
    let mut index = HnswIndex::new(HnswConfig::default());
    println!("Created HNSW index with default config");
    println!("  Index size: {}", index.len());

    // Insert vectors for several nodes
    // Simulating embeddings for different items
    let vectors: Vec<(NexoraId, Vec<f32>)> = vec![
        (NexoraId::from_hex("6e6f646531")?, vec![1.0, 0.0, 0.0, 0.0]),
        (NexoraId::from_hex("6e6f646532")?, vec![0.9, 0.1, 0.0, 0.0]),
        (NexoraId::from_hex("6e6f646533")?, vec![0.0, 1.0, 0.0, 0.0]),
        (NexoraId::from_hex("6e6f646534")?, vec![0.0, 0.9, 0.1, 0.0]),
        (NexoraId::from_hex("6e6f646535")?, vec![0.0, 0.0, 1.0, 0.0]),
        (quake_id_fix(), vec![0.1, 0.0, 0.0, 0.9]),
    ];

    println!("\nInserting {} vectors:", vectors.len());
    for (qid, vec) in &vectors {
        index.insert(qid.clone(), vec.clone());
        println!("  Node {}: {:?}", qid.to_hex(), vec);
    }
    println!("\nIndex size: {}", index.len());

    // Search for k=3 nearest neighbors of [1.0, 0.0, 0.0, 0.0]
    let query = vec![1.0, 0.05, 0.0, 0.05];
    let k = 3;
    println!("\n--- k-NN Search (k={}) ---", k);
    println!("Query vector: {:?}", query);

    let results = index.search_knn(&query, k);
    println!("\nResults:");
    for (i, (qid, dist)) in results.iter().enumerate() {
        println!("  #{}: node={}, distance={:.4}", i + 1, qid.to_hex(), dist);
    }

    // Get vector for a specific node
    println!("\n--- Get Vector by Node ID ---");
    let target = NexoraId::from_hex("6e6f646533")?;
    match index.get(&target) {
        Some(vec) => println!("  Node {} vector: {:?}", target.to_hex(), vec),
        None => println!("  Node {} not found", target.to_hex()),
    }

    // Remove a vector
    println!("\n--- Remove Vector ---");
    let to_remove = NexoraId::from_hex("6e6f646535")?;
    index.remove(&to_remove);
    println!("Removed node: {}", to_remove.to_hex());
    println!("Index size: {}", index.len());

    // Search again after removal
    let results = index.search_knn(&query, k);
    println!("\nSearch after removal (k={}):", k);
    for (i, (qid, dist)) in results.iter().enumerate() {
        println!("  #{}: node={}, distance={:.4}", i + 1, qid.to_hex(), dist);
    }

    println!("\n=== Vector Search Example Complete ===");
    Ok(())
}

fn quake_id_fix() -> NexoraId {
    NexoraId::from_hex("6e6f646536").unwrap()
}
