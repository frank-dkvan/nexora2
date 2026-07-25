//! Integration tests for edge (relationship) SQL queries.

use nexora_sql::{classify_table, translate_sql_to_cypher, TableType};

#[test]
fn test_edge_table_classification() {
    // Node tables
    assert_eq!(
        classify_table("Person"),
        TableType::Node {
            label: "Person".into()
        }
    );

    // Generic edge tables
    assert_eq!(
        classify_table("edge_KNOWS"),
        TableType::GenericEdge {
            rel_type: "KNOWS".into()
        }
    );

    // Typed edge tables
    assert_eq!(
        classify_table("Person_KNOWS_Person"),
        TableType::TypedEdge {
            source_label: "Person".into(),
            rel_type: "KNOWS".into(),
            target_label: "Person".into(),
        }
    );

    // All edges
    assert_eq!(classify_table("_edges"), TableType::AllEdges);
}

#[cfg(test)]
mod sql_translation_tests {
    use super::*;

    // Helper function to translate SQL
    fn translate(sql: &str) -> String {
        match translate_sql_to_cypher(sql) {
            Ok((cypher, _)) => cypher,
            Err(e) => panic!("Translation failed: {}", e),
        }
    }

    // ========== SELECT Tests ==========

    #[test]
    fn test_select_from_generic_edge() {
        let sql = "SELECT * FROM edge_KNOWS";
        let cypher = translate(sql);

        assert!(cypher.contains("MATCH (a)-[r:KNOWS]->(b)"));
        assert!(cypher.contains("from_id"));
        assert!(cypher.contains("to_id"));
        assert!(cypher.contains("edge_type"));
    }

    #[test]
    fn test_select_from_typed_edge() {
        let sql = "SELECT * FROM Person_KNOWS_Person";
        let cypher = translate(sql);

        assert!(cypher.contains("MATCH (a:Person)-[r:KNOWS]->(b:Person)"));
    }

    #[test]
    fn test_select_specific_edge_columns() {
        let sql = "SELECT from_id, to_id, since FROM edge_KNOWS";
        let cypher = translate(sql);

        assert!(cypher.contains("id(a) AS from_id"));
        assert!(cypher.contains("id(b) AS to_id"));
        assert!(cypher.contains("r.since"));
    }

    #[test]
    fn test_select_all_edges() {
        let sql = "SELECT * FROM _edges";
        let cypher = translate(sql);

        assert!(cypher.contains("MATCH (a)-[r]->(b)"));
    }

    #[test]
    fn test_edge_query_with_where() {
        let sql = "SELECT * FROM edge_KNOWS WHERE since > 2020";
        let cypher = translate(sql);

        assert!(cypher.contains("MATCH (a)-[r:KNOWS]->(b)"));
        assert!(cypher.contains("WHERE"));
        assert!(cypher.contains("2020"));
    }

    #[test]
    fn test_edge_query_with_limit() {
        let sql = "SELECT * FROM edge_KNOWS LIMIT 10";
        let cypher = translate(sql);

        assert!(cypher.contains("LIMIT 10"));
    }

    // ========== INSERT Tests ==========

    #[test]
    fn test_insert_generic_edge() {
        let sql = "INSERT INTO edge_KNOWS (from_id, to_id, since) VALUES (1, 2, 2020)";
        let cypher = translate(sql);

        assert!(cypher.contains("MATCH (a) WHERE id(a) = 1"));
        assert!(cypher.contains("MATCH (b) WHERE id(b) = 2"));
        assert!(cypher.contains("CREATE (a)-[r:KNOWS"));
        assert!(cypher.contains("since: 2020"));
    }

    #[test]
    fn test_insert_typed_edge() {
        let sql = "INSERT INTO Person_KNOWS_Person (from_id, to_id, since) VALUES (1, 2, 2020)";
        let cypher = translate(sql);

        assert!(cypher.contains("MATCH (a:Person) WHERE id(a) = 1"));
        assert!(cypher.contains("MATCH (b:Person) WHERE id(b) = 2"));
        assert!(cypher.contains("CREATE (a)-[r:KNOWS"));
    }

    #[test]
    fn test_insert_edge_without_properties() {
        let sql = "INSERT INTO edge_FOLLOWS (from_id, to_id) VALUES (10, 20)";
        let cypher = translate(sql);

        assert!(cypher.contains("MATCH (a) WHERE id(a) = 10"));
        assert!(cypher.contains("MATCH (b) WHERE id(b) = 20"));
        assert!(cypher.contains("CREATE (a)-[r:FOLLOWS]->(b)"));
    }

    // ========== DELETE Tests ==========

    #[test]
    fn test_delete_generic_edge() {
        let sql = "DELETE FROM edge_KNOWS WHERE since < 2010";
        let cypher = translate(sql);

        assert!(cypher.contains("MATCH (a)-[r:KNOWS]->(b)"));
        assert!(cypher.contains("WHERE"));
        assert!(cypher.contains("2010"));
        assert!(cypher.contains("DELETE r"));
        assert!(!cypher.contains("DETACH")); // Should not use DETACH for edges
    }

    #[test]
    fn test_delete_typed_edge() {
        let sql = "DELETE FROM Person_KNOWS_Person WHERE since < 2010";
        let cypher = translate(sql);

        assert!(cypher.contains("MATCH (a:Person)-[r:KNOWS]->(b:Person)"));
        assert!(cypher.contains("DELETE r"));
    }

    #[test]
    fn test_delete_all_edges_with_condition() {
        let sql = "DELETE FROM _edges WHERE weight < 0.5";
        let cypher = translate(sql);

        assert!(cypher.contains("MATCH (a)-[r]->(b)"));
        assert!(cypher.contains("WHERE"));
        assert!(cypher.contains("DELETE r"));
    }

    // ========== Error Cases ==========

    #[test]
    #[should_panic(expected = "from_id")]
    fn test_insert_edge_missing_from_id() {
        let sql = "INSERT INTO edge_KNOWS (to_id, since) VALUES (2, 2020)";
        translate(sql);
    }

    #[test]
    #[should_panic(expected = "to_id")]
    fn test_insert_edge_missing_to_id() {
        let sql = "INSERT INTO edge_KNOWS (from_id, since) VALUES (1, 2020)";
        translate(sql);
    }
}
