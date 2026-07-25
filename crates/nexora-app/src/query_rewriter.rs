//! Query Rewriter — automatically use materialized views when available.
//!
//! Analyzes Cypher queries and rewrites them to use materialized views
//! when the view can satisfy the query.
#![allow(dead_code)]

use nexora_core::materialized_view::{MaterializedView, MaterializedViewManager};
use std::sync::Arc;

/// Query rewriter that detects when a materialized view can be used
pub struct QueryRewriter {
    mv_manager: Arc<MaterializedViewManager>,
}

impl QueryRewriter {
    pub fn new(mv_manager: Arc<MaterializedViewManager>) -> Self {
        Self { mv_manager }
    }

    /// Check if a query can be satisfied by a materialized view
    pub async fn find_matching_view(&self, query: &str) -> Option<String> {
        let views = self.mv_manager.list_views().await;

        for view in views {
            if self.query_matches_view(query, &view) {
                tracing::info!(
                    query = %query,
                    view_id = %view.id,
                    view_name = %view.name,
                    "Query can use materialized view"
                );
                return Some(view.id);
            }
        }

        None
    }

    /// Check if a query pattern matches a materialized view
    fn query_matches_view(&self, query: &str, view: &MaterializedView) -> bool {
        // Simplified matching logic
        // In production, this should use a proper Cypher AST comparison

        let query_normalized = normalize_query(query);
        let view_query_normalized = normalize_query(&view.source_query);

        // Exact match
        if query_normalized == view_query_normalized {
            return true;
        }

        // Subset match (query is more specific than view)
        // Example:
        // View: MATCH (n:Person) RETURN n
        // Query: MATCH (n:Person) WHERE n.age > 18 RETURN n
        // → Can use view + filter

        if query_normalized.contains(&view_query_normalized) {
            // Query is a refinement of the view
            return true;
        }

        false
    }

    /// Rewrite query to use materialized view
    pub async fn rewrite_query(&self, _query: &str, view_id: &str) -> Option<String> {
        let view = self.mv_manager.get_view(view_id).await?;

        // Simple rewrite: direct MV query
        // In production, this should handle filters, projections, etc.
        Some(format!(
            "// Rewritten to use materialized view: {}\n{}",
            view.name, view.source_query
        ))
    }
}

/// Normalize a Cypher query for comparison
fn normalize_query(query: &str) -> String {
    query
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

/// Query execution strategy
pub enum QueryStrategy {
    /// Execute original Cypher query
    Direct,
    /// Use materialized view
    MaterializedView {
        view_id: String,
        additional_filters: Vec<String>,
    },
}

impl QueryRewriter {
    /// Determine the best execution strategy for a query
    pub async fn plan_query(&self, query: &str) -> QueryStrategy {
        if let Some(view_id) = self.find_matching_view(query).await {
            // TODO: Extract additional filters not covered by the view
            QueryStrategy::MaterializedView {
                view_id,
                additional_filters: vec![],
            }
        } else {
            QueryStrategy::Direct
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexora_core::materialized_view::RefreshMode;

    #[tokio::test]
    async fn test_exact_match() {
        let mv_manager = Arc::new(MaterializedViewManager::new());
        let rewriter = QueryRewriter::new(mv_manager.clone());

        // Create a materialized view
        mv_manager
            .create_view(
                "test_view".to_string(),
                "MATCH (n:Person) RETURN n".to_string(),
                vec![],
                RefreshMode::Incremental,
            )
            .await
            .unwrap();

        // Test exact match
        let result = rewriter
            .find_matching_view("MATCH (n:Person) RETURN n")
            .await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_no_match() {
        let mv_manager = Arc::new(MaterializedViewManager::new());
        let rewriter = QueryRewriter::new(mv_manager.clone());

        mv_manager
            .create_view(
                "test_view".to_string(),
                "MATCH (n:Person) RETURN n".to_string(),
                vec![],
                RefreshMode::Incremental,
            )
            .await
            .unwrap();

        // Test no match
        let result = rewriter
            .find_matching_view("MATCH (n:Product) RETURN n")
            .await;
        assert!(result.is_none());
    }

    #[test]
    fn test_normalize_query() {
        assert_eq!(
            normalize_query("MATCH  (n:Person)   RETURN n"),
            "match (n:person) return n"
        );
    }
}
