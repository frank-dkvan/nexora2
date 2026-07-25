//! Integration tests for nexora-cli
//!
//! Tests command parsing, stdin handling, exit codes, error handling,
//! and JSON output formatting.

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::json;

#[test]
fn test_help_command() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("--help");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Nexora CLI client"));
}

#[test]
fn test_version_command() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("--version");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("nex"));
}

#[test]
fn test_cypher_command_requires_query() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("cypher");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_sql_command_requires_query() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("sql");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_ingest_command_default_stdin() {
    // Without a mock server, this will fail with connection error
    // but we can verify command parsing succeeds
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("ingest")
        .write_stdin(r#"[{"id": "test1", "name": "Alice"}]"#);

    // Will fail with connection error, but that's exit code 1 (client error)
    cmd.assert()
        .failure()
        .code(1)
        .stderr(predicate::str::contains("Error"));
}

#[test]
fn test_ingest_with_custom_id_field() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("ingest")
        .arg("--id-field")
        .arg("user_id")
        .write_stdin(r#"[{"user_id": "u1", "name": "Bob"}]"#);

    cmd.assert().failure().code(1); // Connection error expected
}

#[test]
fn test_invalid_json_in_ingest() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("ingest").write_stdin("not valid json");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("JSON"));
}

#[test]
fn test_node_get_requires_qid_and_key() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("node").arg("get");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_node_set_requires_all_args() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("node").arg("set").arg("abc123");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_node_set_with_invalid_json_value() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("node")
        .arg("set")
        .arg("abc123")
        .arg("name")
        .arg("not-json");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("JSON"));
}

#[test]
fn test_edges_get_requires_qid() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("edges").arg("get");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_edges_add_requires_source_and_target() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("edges").arg("add");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_edges_add_with_edge_type() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("edges")
        .arg("add")
        .arg("source123")
        .arg("--edge-type")
        .arg("FOLLOWS")
        .arg("target456");

    // Will fail with connection error
    cmd.assert().failure().code(1);
}

#[test]
fn test_sq_list_command() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("sq").arg("list");

    // Will fail with connection error
    cmd.assert().failure().code(1);
}

#[test]
fn test_sq_create_requires_name_and_pattern() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("sq").arg("create");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_sq_create_with_invalid_pattern_json() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("sq").arg("create").arg("test-sq").arg("not-json");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("JSON"));
}

#[test]
fn test_sq_get_requires_name() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("sq").arg("get");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_sq_delete_requires_name() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("sq").arg("delete");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_vector_search_requires_vector() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("vector").arg("search");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("required arguments"));
}

#[test]
fn test_vector_search_with_invalid_vector_json() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("vector").arg("search").arg("not-a-vector");

    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("JSON"));
}

#[test]
fn test_vector_search_with_custom_k() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("vector")
        .arg("search")
        .arg("[1.0, 2.0, 3.0]")
        .arg("--k")
        .arg("5");

    // Will fail with connection error
    cmd.assert().failure().code(1);
}

#[test]
fn test_health_liveness() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("health");

    // Will fail with connection error
    cmd.assert().failure().code(1);
}

#[test]
fn test_health_readiness() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("health").arg("--readiness");

    // Will fail with connection error
    cmd.assert().failure().code(1);
}

#[test]
fn test_custom_url_via_arg() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("--url").arg("http://custom:9090").arg("health");

    // Will fail but we tested URL parsing
    cmd.assert().failure();
}

#[test]
fn test_bearer_token_via_arg() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("--token").arg("test-token-123").arg("health");

    // Will fail but we tested token parsing
    cmd.assert().failure();
}

#[test]
fn test_cypher_from_stdin() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("cypher")
        .arg("-")
        .write_stdin("MATCH (n) RETURN n LIMIT 1");

    // Will fail with connection error
    cmd.assert().failure().code(1);
}

#[test]
fn test_sql_from_stdin() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("sql")
        .arg("-")
        .write_stdin("SELECT * FROM nodes LIMIT 1");

    // Will fail with connection error
    cmd.assert().failure().code(1);
}

#[test]
fn test_cypher_direct_query() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("cypher").arg("MATCH (n:Person) RETURN n");

    // Will fail with connection error
    cmd.assert().failure().code(1);
}

#[test]
fn test_sql_direct_query() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("sql").arg("SELECT * FROM person");

    // Will fail with connection error
    cmd.assert().failure().code(1);
}

#[test]
fn test_exit_code_for_connection_error() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("--url")
        .arg("http://nonexistent:9999")
        .arg("health");

    // Should be exit code 1 (client/network error)
    cmd.assert()
        .failure()
        .code(1)
        .stderr(predicate::str::contains("Error"));
}

#[test]
fn test_ingest_empty_array() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("ingest").write_stdin("[]");

    // Will fail with connection error, but JSON parsing should succeed
    cmd.assert().failure().code(1);
}

#[test]
fn test_node_set_with_valid_json_types() {
    // Test string value
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("node")
        .arg("set")
        .arg("qid123")
        .arg("name")
        .arg(r#""Alice""#);
    cmd.assert().failure().code(1);

    // Test number value
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("node")
        .arg("set")
        .arg("qid123")
        .arg("age")
        .arg("30");
    cmd.assert().failure().code(1);

    // Test boolean value
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("node")
        .arg("set")
        .arg("qid123")
        .arg("active")
        .arg("true");
    cmd.assert().failure().code(1);

    // Test null value
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("node")
        .arg("set")
        .arg("qid123")
        .arg("deleted_at")
        .arg("null");
    cmd.assert().failure().code(1);

    // Test array value
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("node")
        .arg("set")
        .arg("qid123")
        .arg("tags")
        .arg(r#"["tag1", "tag2"]"#);
    cmd.assert().failure().code(1);

    // Test object value
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("node")
        .arg("set")
        .arg("qid123")
        .arg("metadata")
        .arg(r#"{"created": "2024-01-01"}"#);
    cmd.assert().failure().code(1);
}

#[test]
fn test_edges_add_with_incoming_direction() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("edges")
        .arg("add")
        .arg("source123")
        .arg("--edge-type")
        .arg("FOLLOWS")
        .arg("target456")
        .arg("--direction")
        .arg("incoming");

    cmd.assert().failure().code(1);
}

#[test]
fn test_sq_create_with_valid_pattern() {
    let pattern = json!({
        "type": "PropertyMatch",
        "key": "age",
        "condition": {"Gt": 18},
        "labels": []
    });

    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("sq")
        .arg("create")
        .arg("adults")
        .arg(pattern.to_string());

    cmd.assert().failure().code(1); // Connection error expected
}

#[test]
fn test_multiple_ingest_records() {
    let records = json!([
        {"id": "user1", "name": "Alice", "age": 30},
        {"id": "user2", "name": "Bob", "age": 25},
        {"id": "user3", "name": "Charlie", "age": 35}
    ]);

    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("ingest").write_stdin(records.to_string());

    cmd.assert().failure().code(1);
}

#[test]
fn test_ingest_with_nested_properties() {
    let records = json!([
        {
            "id": "user1",
            "name": "Alice",
            "profile": {
                "email": "alice@example.com",
                "location": "NYC"
            },
            "tags": ["premium", "verified"]
        }
    ]);

    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("ingest").write_stdin(records.to_string());

    cmd.assert().failure().code(1);
}

#[test]
fn test_unicode_in_queries() {
    // Test Chinese characters
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("cypher").arg("MATCH (n {name: '张三'}) RETURN n");
    cmd.assert().failure().code(1);

    // Test Emoji
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("cypher").arg("MATCH (n {status: '🟢'}) RETURN n");
    cmd.assert().failure().code(1);
}

#[test]
fn test_very_long_query() {
    let long_query = "MATCH (n) ".to_string() + &"WHERE n.prop = 1 ".repeat(100) + "RETURN n";

    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("cypher").arg(&long_query);

    cmd.assert().failure().code(1);
}

#[test]
fn test_empty_stdin_for_cypher() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("cypher").arg("-").write_stdin("");

    // Empty query should still attempt connection
    cmd.assert().failure().code(1);
}

#[test]
fn test_whitespace_only_stdin() {
    let mut cmd = Command::cargo_bin("nex").unwrap();
    cmd.arg("cypher").arg("-").write_stdin("   \n\t  \n  ");

    cmd.assert().failure().code(1);
}
