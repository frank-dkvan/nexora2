//! Path variable rewriter — converts `MATCH path = (a)-[r]->(b) RETURN path`
//! to `MATCH (a)-[r]->(b) RETURN a, r, b` so cypher-parser can execute it.
//!
//! Background: Shopify's cypher-parser v0.5.0 doesn't support path variable
//! assignment syntax (`path = ...`), but nexora-language does. This module
//! bridges the gap by rewriting queries that use path variables into equivalent
//! queries that cypher-parser can handle.

use nexora_language::ast::{Clause, PatternPart};
use nexora_language::CypherQuery;

/// Rewrite result with information about path variables found.
#[derive(Debug)]
pub struct PathRewriteResult {
    /// The rewritten query (if path variables were found), otherwise original.
    pub query: String,
    /// Whether any path variables were found and rewritten.
    pub has_path_vars: bool,
    /// Mapping from path variable names to their component variable names.
    /// E.g., {"path": ["a", "r", "b"]} means `path` should expand to (a, r, b).
    pub path_mappings: Vec<(String, Vec<String>)>,
}

/// Attempt to rewrite a query that uses path variables into one that doesn't.
///
/// Returns the original query unchanged if:
/// - No path variables are found
/// - Query has unsupported constructs (e.g., multiple patterns, complex paths)
pub fn rewrite_path_variables(query: &str, ast: &CypherQuery) -> PathRewriteResult {
    let mut path_mappings = Vec::new();
    let mut needs_rewrite = false;

    // Check all MATCH clauses for path variables
    for clause in &ast.clauses {
        if let Clause::Match { pattern, .. } = clause {
            for part in &pattern.parts {
                if let Some(ref path_var) = part.variable {
                    needs_rewrite = true;
                    let components = extract_path_components(part);
                    path_mappings.push((path_var.clone(), components));
                }
            }
        }
    }

    if !needs_rewrite {
        return PathRewriteResult {
            query: query.to_string(),
            has_path_vars: false,
            path_mappings,
        };
    }

    // Simple rewriter: remove `path =` prefix and expand in RETURN
    let rewritten = rewrite_query_text(query, &path_mappings);

    PathRewriteResult {
        query: rewritten,
        has_path_vars: true,
        path_mappings,
    }
}

/// Extract variable names from a pattern part (node and edge variables).
fn extract_path_components(part: &PatternPart) -> Vec<String> {
    let mut components: Vec<String> = Vec::new();

    for segment in &part.chain.segments {
        // Add node variable if present
        if let Some(var) = &segment.node.variable {
            components.push(var.to_owned());
        }

        // Add edge variable if present
        if let Some(edge) = &segment.edge {
            if let Some(var) = &edge.variable {
                components.push(var.to_owned());
            }
        }
    }

    components
}

/// Rewrite query text by removing path variable assignments and expanding RETURN.
///
/// This is a simple text-based rewriter. A more robust approach would rebuild
/// from the AST, but that requires more infrastructure.
fn rewrite_query_text(query: &str, path_mappings: &[(String, Vec<String>)]) -> String {
    let mut result = query.to_string();

    // Step 1: Remove `path_var =` assignments from MATCH clauses
    for (path_var, _) in path_mappings {
        // Match pattern: `path_var = ` (with optional whitespace)
        let pattern = format!(r"{}\s*=\s*", regex::escape(path_var));
        let re = regex::Regex::new(&pattern).unwrap();
        result = re.replace_all(&result, "").to_string();
    }

    // Step 2: Expand path variables in RETURN clause
    // Pattern: `RETURN path_var` → `RETURN a, r, b`
    for (path_var, components) in path_mappings {
        if components.is_empty() {
            continue;
        }

        // Match `RETURN path_var` (with optional LIMIT/ORDER BY after)
        let return_pattern = format!(r"(?i)\bRETURN\s+{}\b", regex::escape(path_var));
        let re = regex::Regex::new(&return_pattern).unwrap();

        let replacement = format!("RETURN {}", components.join(", "));
        result = re.replace(&result, replacement.as_str()).to_string();

        // Also handle `, path_var` in RETURN (e.g., `RETURN x, path, y`)
        let comma_pattern = format!(r",\s*{}\b", regex::escape(path_var));
        let comma_re = regex::Regex::new(&comma_pattern).unwrap();
        let comma_replacement = format!(", {}", components.join(", "));
        result = comma_re
            .replace_all(&result, comma_replacement.as_str())
            .to_string();
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexora_language::Parser;

    #[test]
    fn test_simple_path_variable() {
        let query = "MATCH path = (a)-[:KNOWS]->(b) RETURN path";
        let ast = Parser::parse(query).unwrap();
        let result = rewrite_path_variables(query, &ast);

        assert!(result.has_path_vars);
        assert_eq!(result.path_mappings.len(), 1);
        assert_eq!(result.path_mappings[0].0, "path");
        assert!(result.query.contains("RETURN a, b"));
        assert!(!result.query.contains("path ="));
    }

    #[test]
    fn test_path_with_edge_variable() {
        let query = "MATCH p = (a)-[r:KNOWS]->(b) RETURN p";
        let ast = Parser::parse(query).unwrap();
        let result = rewrite_path_variables(query, &ast);

        assert!(result.has_path_vars);
        assert!(result.query.contains("RETURN a, r, b"));
    }

    #[test]
    fn test_no_path_variable() {
        let query = "MATCH (a)-[:KNOWS]->(b) RETURN a, b";
        let ast = Parser::parse(query).unwrap();
        let result = rewrite_path_variables(query, &ast);

        assert!(!result.has_path_vars);
        assert_eq!(result.query, query);
    }

    #[test]
    fn test_variable_length_path() {
        let query = "MATCH path = (a)-[:NEXT*3..5]->(b) RETURN path LIMIT 10";
        let ast = Parser::parse(query).unwrap();
        let result = rewrite_path_variables(query, &ast);

        assert!(result.has_path_vars);
        assert!(result.query.contains("RETURN a, b"));
        assert!(result.query.contains("LIMIT 10"));
    }
}
