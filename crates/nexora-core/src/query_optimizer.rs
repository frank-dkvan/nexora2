//! Query optimizer for Cypher queries.
//!
//! Automatically selects the best execution strategy based on:
//! - Available indexes (property index, label index)
//! - Index statistics (cardinality, selectivity)
//! - Query pattern (filters, labels, properties)
//!
//! Example:
//! ```cypher
//! MATCH (n:Person) WHERE n.age > 18 AND n.city = 'Beijing' RETURN n
//! ```
//!
//! Optimizer decisions:
//! - Option 1: Scan label index Person (10K results) → filter age → filter city
//! - Option 2: Scan property index age>18 (50K results) → filter Person → filter city
//! - Option 3: Scan property index city=Beijing (100 results) → filter Person → filter age ✅ Best
//!
//! The optimizer chooses Option 3 (lowest cardinality starting point).

use crate::{IndexError, LabelIndex, PropertyIndex};
use nexora_id::{NexoraId, PropertyValue};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Statistics for query optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatistics {
    /// Label → node count
    pub label_cardinality: HashMap<String, usize>,

    /// Property → distinct value count
    pub property_cardinality: HashMap<String, usize>,

    /// Total number of nodes in the graph
    pub total_nodes: usize,
}

impl IndexStatistics {
    pub fn new() -> Self {
        Self {
            label_cardinality: HashMap::new(),
            property_cardinality: HashMap::new(),
            total_nodes: 0,
        }
    }

    /// Estimate selectivity (fraction of nodes that match)
    pub fn estimate_label_selectivity(&self, label: &str) -> f64 {
        if self.total_nodes == 0 {
            return 1.0;
        }

        let count = self.label_cardinality.get(label).copied().unwrap_or(0);
        count as f64 / self.total_nodes as f64
    }

    /// Estimate property selectivity
    pub fn estimate_property_selectivity(&self, property: &str) -> f64 {
        if self.total_nodes == 0 {
            return 1.0;
        }

        // Assume uniform distribution
        let distinct_values = self
            .property_cardinality
            .get(property)
            .copied()
            .unwrap_or(1);
        1.0 / distinct_values as f64
    }
}

impl Default for IndexStatistics {
    fn default() -> Self {
        Self::new()
    }
}

/// Query filter predicate
#[derive(Debug, Clone)]
pub enum FilterPredicate {
    /// Label check: n:Person
    HasLabel(String),

    /// Property equality: n.city = 'Beijing'
    PropertyEquals(String, PropertyValue),

    /// Property range: n.age > 18
    PropertyRange(String, PropertyValue, PropertyValue),
}

impl FilterPredicate {
    /// Estimate cost of this predicate (lower is better)
    pub fn estimate_cost(&self, stats: &IndexStatistics) -> f64 {
        match self {
            FilterPredicate::HasLabel(label) => {
                let selectivity = stats.estimate_label_selectivity(label);
                selectivity * stats.total_nodes as f64
            }
            FilterPredicate::PropertyEquals(prop, _) => {
                let selectivity = stats.estimate_property_selectivity(prop);
                selectivity * stats.total_nodes as f64
            }
            FilterPredicate::PropertyRange(prop, _, _) => {
                // Range queries are more expensive (assume 10% selectivity)
                let selectivity = stats.estimate_property_selectivity(prop) * 10.0;
                selectivity.min(1.0) * stats.total_nodes as f64
            }
        }
    }
}

/// Execution plan for a query
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    /// Starting predicate (lowest cardinality)
    pub start_with: FilterPredicate,

    /// Remaining predicates to apply (ordered by cost)
    pub then_filter: Vec<FilterPredicate>,

    /// Estimated cost (number of nodes examined)
    pub estimated_cost: f64,
}

/// Query optimizer
pub struct QueryOptimizer {
    stats: IndexStatistics,
    property_index: Option<PropertyIndex>,
    label_index: Option<LabelIndex>,
}

impl QueryOptimizer {
    /// Create a new optimizer
    pub fn new(stats: IndexStatistics) -> Self {
        Self {
            stats,
            property_index: None,
            label_index: None,
        }
    }

    /// Create with indexes for execution
    pub fn with_indexes(
        stats: IndexStatistics,
        property_index: PropertyIndex,
        label_index: LabelIndex,
    ) -> Self {
        Self {
            stats,
            property_index: Some(property_index),
            label_index: Some(label_index),
        }
    }

    /// Optimize a query with multiple predicates
    ///
    /// Returns the best execution plan.
    pub fn optimize(&self, predicates: Vec<FilterPredicate>) -> ExecutionPlan {
        if predicates.is_empty() {
            return ExecutionPlan {
                start_with: FilterPredicate::HasLabel("*".to_string()),
                then_filter: Vec::new(),
                estimated_cost: self.stats.total_nodes as f64,
            };
        }

        // Calculate cost for each predicate
        let mut costs: Vec<_> = predicates
            .iter()
            .map(|p| (p.clone(), p.estimate_cost(&self.stats)))
            .collect();

        // Sort by cost (ascending)
        // Use total_cmp for f64 to handle NaN cases safely
        costs.sort_by(|a, b| a.1.total_cmp(&b.1));

        // Start with the cheapest predicate
        let (start_with, start_cost) = costs.remove(0);

        // Remaining predicates ordered by cost
        let then_filter: Vec<_> = costs.into_iter().map(|(p, _)| p).collect();

        // Estimate total cost (start + filters)
        let filter_cost: f64 = then_filter.iter().fold(start_cost, |acc, _p| acc * 0.5); // Assume each filter reduces by 50%

        ExecutionPlan {
            start_with,
            then_filter,
            estimated_cost: filter_cost,
        }
    }

    /// Execute an optimized plan
    pub async fn execute(&self, plan: ExecutionPlan) -> Result<Vec<NexoraId>, IndexError> {
        // Start with the base set
        let mut results = self.execute_predicate(&plan.start_with).await?;

        // Apply remaining filters
        for predicate in plan.then_filter {
            results = self.filter_results(results, &predicate).await?;
        }

        Ok(results)
    }

    /// Execute a single predicate using available indexes
    async fn execute_predicate(
        &self,
        predicate: &FilterPredicate,
    ) -> Result<Vec<NexoraId>, IndexError> {
        match predicate {
            FilterPredicate::HasLabel(label) => {
                if let Some(index) = &self.label_index {
                    Ok(index.query(label).await)
                } else {
                    Err(IndexError::NotFound("Label index not available".into()))
                }
            }
            FilterPredicate::PropertyEquals(prop, value) => {
                if let Some(index) = &self.property_index {
                    index.query(prop, value).await
                } else {
                    Err(IndexError::NotFound("Property index not available".into()))
                }
            }
            FilterPredicate::PropertyRange(prop, start, end) => {
                if let Some(index) = &self.property_index {
                    index.range_query(prop, start, end).await
                } else {
                    Err(IndexError::NotFound("Property index not available".into()))
                }
            }
        }
    }

    /// Filter results by a predicate
    async fn filter_results(
        &self,
        mut results: Vec<NexoraId>,
        predicate: &FilterPredicate,
    ) -> Result<Vec<NexoraId>, IndexError> {
        let matches = self.execute_predicate(predicate).await?;
        results.retain(|node| matches.contains(node));
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_selectivity() {
        let mut stats = IndexStatistics::new();
        stats.total_nodes = 10000;
        stats.label_cardinality.insert("Person".to_string(), 1000);
        stats.label_cardinality.insert("Product".to_string(), 100);

        assert_eq!(stats.estimate_label_selectivity("Person"), 0.1);
        assert_eq!(stats.estimate_label_selectivity("Product"), 0.01);
    }

    #[test]
    fn test_optimizer_chooses_cheapest_start() {
        let mut stats = IndexStatistics::new();
        stats.total_nodes = 10000;
        stats.label_cardinality.insert("Person".to_string(), 5000);
        stats.property_cardinality.insert("city".to_string(), 100);

        let optimizer = QueryOptimizer::new(stats);

        let predicates = vec![
            FilterPredicate::HasLabel("Person".to_string()),
            FilterPredicate::PropertyEquals(
                "city".to_string(),
                PropertyValue::String("Beijing".into()),
            ),
        ];

        let plan = optimizer.optimize(predicates);

        // Should start with city (100 results) not Person (5000 results)
        match plan.start_with {
            FilterPredicate::PropertyEquals(prop, _) => {
                assert_eq!(prop, "city");
            }
            _ => panic!("Expected PropertyEquals as starting predicate"),
        }
    }

    #[test]
    fn test_cost_estimation() {
        let mut stats = IndexStatistics::new();
        stats.total_nodes = 10000;
        stats.label_cardinality.insert("Person".to_string(), 1000);

        let pred = FilterPredicate::HasLabel("Person".to_string());
        let cost = pred.estimate_cost(&stats);

        assert_eq!(cost, 1000.0); // 10% of 10K nodes
    }

    #[tokio::test]
    async fn test_execute_with_indexes() {
        use crate::{LabelIndex, PropertyIndex};

        let mut stats = IndexStatistics::new();
        stats.total_nodes = 10;

        let label_index = LabelIndex::new();
        let property_index = PropertyIndex::new();

        // Add test data
        for i in 0..10 {
            let node = NexoraId::from_bytes(format!("node{}", i).into_bytes());
            label_index.add_label("Person", node.clone()).await;

            if i < 5 {
                property_index
                    .insert(
                        "city",
                        PropertyValue::String("Beijing".into()),
                        node.clone(),
                    )
                    .await
                    .unwrap();
            }
        }

        let optimizer = QueryOptimizer::with_indexes(stats, property_index, label_index);

        let predicates = vec![
            FilterPredicate::HasLabel("Person".to_string()),
            FilterPredicate::PropertyEquals(
                "city".to_string(),
                PropertyValue::String("Beijing".into()),
            ),
        ];

        let plan = optimizer.optimize(predicates);
        let results = optimizer.execute(plan).await.unwrap();

        // Should return 5 nodes (Person AND city=Beijing)
        assert_eq!(results.len(), 5);
    }
}
