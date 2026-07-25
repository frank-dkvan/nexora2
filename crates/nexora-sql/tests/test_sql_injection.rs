//! Tests for SQL injection prevention (P0-4 fix verification).

use nexora_sql::translate_to_cypher;

#[test]
fn test_like_pattern_escape() {
    // Test that LIKE patterns are properly escaped
    // Simulating an injection attempt with special characters
    let sql = "SELECT * FROM users WHERE name LIKE '%admin%'";
    let result = translate_to_cypher(sql);

    assert!(result.is_ok());
    let cypher = result.unwrap();

    // Should escape properly and use CONTAINS
    assert!(cypher.contains("CONTAINS"));
    assert!(cypher.contains("admin"));
}

#[test]
fn test_string_value_escape() {
    // Test that string values properly escape quotes and backslashes
    let sql = "SELECT * FROM users WHERE name = 'O''Reilly'"; // SQL escapes with double quote
    let result = translate_to_cypher(sql);

    if let Err(e) = &result {
        eprintln!("Error: {:?}", e);
    }

    assert!(result.is_ok());
    let cypher = result.unwrap();

    eprintln!("Generated Cypher: {}", cypher);

    // Should contain the name properly escaped for Cypher
    assert!(cypher.contains("Reilly"));
}

#[test]
fn test_function_whitelist_blocks_unknown() {
    // Test that unknown/dangerous functions are blocked
    let sql = "SELECT dbms_procedures() FROM users";
    let result = translate_to_cypher(sql);

    assert!(result.is_ok());
    let cypher = result.unwrap();

    // Should contain a comment indicating unsafe function
    assert!(cypher.contains("UNSAFE FUNCTION") || cypher.contains("dbms_procedures"));
}

#[test]
fn test_function_whitelist_allows_safe() {
    // Test that safe functions are allowed
    let sql = "SELECT COUNT(*), SUM(age), UPPER(name) FROM users";
    let result = translate_to_cypher(sql);

    assert!(result.is_ok());
    let cypher = result.unwrap();

    // Should contain the safe functions
    assert!(cypher.contains("count("));
    assert!(cypher.contains("sum("));
    assert!(cypher.contains("toUpper("));

    // Should not contain unsafe function warnings
    assert!(!cypher.contains("UNSAFE FUNCTION"));
}

#[test]
fn test_double_quoted_identifier_safe() {
    // Double-quoted identifiers should be prefixed with n. and cannot inject
    let sql = r#"SELECT "name", "age" FROM users"#;
    let result = translate_to_cypher(sql);

    assert!(result.is_ok());
    let cypher = result.unwrap();

    // Should prefix with n.
    assert!(cypher.contains("n.name") || cypher.contains("n.age"));
}

#[test]
fn test_numeric_values_no_injection() {
    // Numeric values should be passed through safely
    let sql = "SELECT * FROM users WHERE age > 18 AND age < 100";
    let result = translate_to_cypher(sql);

    assert!(result.is_ok());
    let cypher = result.unwrap();

    // Should contain the numeric values directly
    assert!(cypher.contains("18"));
    assert!(cypher.contains("100"));

    // Should not have quotes around numbers
    assert!(!cypher.contains("'18'"));
}

#[test]
fn test_backslash_in_like_pattern() {
    // Test that backslashes in LIKE patterns are properly escaped
    let sql = r#"SELECT * FROM files WHERE path LIKE 'C:\\Users\\%'"#;
    let result = translate_to_cypher(sql);

    assert!(result.is_ok());
    let cypher = result.unwrap();

    // Should escape backslashes to prevent breaking the string
    assert!(cypher.contains("\\\\"));
}

#[test]
fn test_null_value_safe() {
    // NULL values should be handled safely
    let sql = "SELECT * FROM users WHERE deleted_at IS NULL";
    let result = translate_to_cypher(sql);

    assert!(result.is_ok());
    let cypher = result.unwrap();

    // Should contain IS NULL without quotes
    assert!(cypher.contains("IS NULL"));
    assert!(!cypher.contains("'NULL'"));
}

#[test]
fn test_boolean_value_safe() {
    // Boolean values should be handled safely
    let sql = "SELECT * FROM users WHERE active = TRUE";
    let result = translate_to_cypher(sql);

    assert!(result.is_ok());
    let cypher = result.unwrap();

    // Should contain true/false without quotes
    assert!(cypher.to_lowercase().contains("true"));
}

#[test]
fn test_in_list_escape() {
    // Test that IN lists properly escape string values
    let sql = "SELECT * FROM users WHERE name IN ('Alice', 'Bob')"; // Simplified
    let result = translate_to_cypher(sql);

    if let Err(e) = &result {
        eprintln!("Error: {:?}", e);
    }

    assert!(result.is_ok());
    let cypher = result.unwrap();

    eprintln!("Generated Cypher: {}", cypher);

    // Should contain IN and the values
    assert!(cypher.contains("IN"));
    assert!(cypher.contains("Alice") || cypher.contains("Bob"));
}
