//! Integration example: PropertyIndex + LabelIndex + QueryOptimizer
//!
//! Demonstrates how the three components work together for optimal query performance.

use nexora_core::{
    FilterPredicate, IndexConfig, IndexStatistics, LabelIndex, PropertyIndex, QueryOptimizer,
};
use nexora_id::{NexoraId, PropertyValue};

#[tokio::main]
async fn main() {
    println!("=== Nexora-RS Index & Optimizer Integration Demo ===\n");

    // 1. Create indexes
    println!("1. Creating indexes...");
    let property_index = PropertyIndex::new_with_config(IndexConfig::default());
    let label_index = LabelIndex::new();

    // 2. Populate test data (10K nodes)
    println!("2. Populating 10,000 nodes...");
    for i in 0..10_000 {
        let node = NexoraId::from_bytes(format!("user-{:05}", i).into_bytes());

        // Labels
        label_index.add_label("Person", node.clone()).await;
        if i % 10 == 0 {
            label_index.add_label("Employee", node.clone()).await;
        }

        // Properties
        property_index
            .insert("age", PropertyValue::Integer(18 + (i % 50)), node.clone())
            .await
            .unwrap();

        let city = match i % 3 {
            0 => "Beijing",
            1 => "Shanghai",
            _ => "Guangzhou",
        };
        property_index
            .insert("city", PropertyValue::String(city.into()), node.clone())
            .await
            .unwrap();
    }

    println!("   - 10,000 Person nodes");
    println!("   - 1,000 Employee nodes");
    println!("   - Age range: 18-67");
    println!("   - 3 cities: Beijing (33%), Shanghai (33%), Guangzhou (33%)");

    // 3. Create statistics
    println!("\n3. Collecting index statistics...");
    let mut stats = IndexStatistics::new();
    stats.total_nodes = 10_000;
    stats.label_cardinality.insert("Person".to_string(), 10_000);
    stats
        .label_cardinality
        .insert("Employee".to_string(), 1_000);
    stats.property_cardinality.insert("age".to_string(), 50);
    stats.property_cardinality.insert("city".to_string(), 3);

    println!("   Statistics:");
    println!(
        "   - Person selectivity: {:.1}%",
        stats.estimate_label_selectivity("Person") * 100.0
    );
    println!(
        "   - Employee selectivity: {:.1}%",
        stats.estimate_label_selectivity("Employee") * 100.0
    );
    println!(
        "   - City selectivity: {:.1}%",
        stats.estimate_property_selectivity("city") * 100.0
    );

    // 4. Create optimizer
    println!("\n4. Creating query optimizer...");
    let optimizer = QueryOptimizer::with_indexes(stats, property_index, label_index);

    // 5. Query 1: Find young employees in Beijing
    println!("\n5. Query 1: Find young employees in Beijing");
    println!("   MATCH (n:Person:Employee) WHERE n.age < 25 AND n.city = 'Beijing' RETURN n");

    let predicates1 = vec![
        FilterPredicate::HasLabel("Person".to_string()),
        FilterPredicate::HasLabel("Employee".to_string()),
        FilterPredicate::PropertyRange(
            "age".to_string(),
            PropertyValue::Integer(0),
            PropertyValue::Integer(25),
        ),
        FilterPredicate::PropertyEquals(
            "city".to_string(),
            PropertyValue::String("Beijing".into()),
        ),
    ];

    let plan1 = optimizer.optimize(predicates1);
    println!("\n   Execution Plan:");
    println!("   - Start with: {:?}", plan1.start_with);
    println!("   - Then filter: {} predicates", plan1.then_filter.len());
    println!(
        "   - Estimated cost: {:.0} nodes examined",
        plan1.estimated_cost
    );

    let cost1 = plan1.estimated_cost;
    let results1 = optimizer.execute(plan1).await.unwrap();
    println!("   ✅ Results: {} nodes found", results1.len());

    // 6. Query 2: Find all employees (simple)
    println!("\n6. Query 2: Find all employees");
    println!("   MATCH (n:Employee) RETURN n");

    let predicates2 = vec![FilterPredicate::HasLabel("Employee".to_string())];

    let plan2 = optimizer.optimize(predicates2);
    println!("\n   Execution Plan:");
    println!("   - Start with: {:?}", plan2.start_with);
    println!(
        "   - Estimated cost: {:.0} nodes examined",
        plan2.estimated_cost
    );

    let cost2 = plan2.estimated_cost;
    let results2 = optimizer.execute(plan2).await.unwrap();
    println!("   ✅ Results: {} nodes found", results2.len());

    // 7. Query 3: Complex multi-filter query
    println!("\n7. Query 3: Persons in Shanghai, age 30-40");
    println!(
        "   MATCH (n:Person) WHERE n.city = 'Shanghai' AND n.age >= 30 AND n.age <= 40 RETURN n"
    );

    let predicates3 = vec![
        FilterPredicate::HasLabel("Person".to_string()),
        FilterPredicate::PropertyEquals(
            "city".to_string(),
            PropertyValue::String("Shanghai".into()),
        ),
        FilterPredicate::PropertyRange(
            "age".to_string(),
            PropertyValue::Integer(30),
            PropertyValue::Integer(40),
        ),
    ];

    let plan3 = optimizer.optimize(predicates3);
    println!("\n   Execution Plan:");
    println!("   - Start with: {:?}", plan3.start_with);
    println!("   - Then filter: {} predicates", plan3.then_filter.len());
    println!(
        "   - Estimated cost: {:.0} nodes examined",
        plan3.estimated_cost
    );

    let cost3 = plan3.estimated_cost;
    let results3 = optimizer.execute(plan3).await.unwrap();
    println!("   ✅ Results: {} nodes found", results3.len());

    // 8. Performance comparison
    println!("\n8. Performance Summary:");
    println!("   Without optimizer (full scan): 10,000 nodes examined");
    println!("   With optimizer:");
    println!(
        "     - Query 1: {:.0} nodes examined ({:.1}x faster)",
        cost1,
        10_000.0 / cost1
    );
    println!(
        "     - Query 2: {:.0} nodes examined ({:.1}x faster)",
        cost2,
        10_000.0 / cost2
    );
    println!(
        "     - Query 3: {:.0} nodes examined ({:.1}x faster)",
        cost3,
        10_000.0 / cost3
    );

    println!("\n=== Demo Complete ===");
}
