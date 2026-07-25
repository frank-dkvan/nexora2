//! Edge table mapping and classification.
//!
//! Maps SQL table names to graph edge patterns:
//! - `edge_KNOWS` → `()-[:KNOWS]->()`
//! - `Person_KNOWS_Person` → `(:Person)-[:KNOWS]->(:Person)`
//! - `_edges` → `()-[r]->()`

/// Classification of a SQL table name for edge vs node queries.
#[derive(Debug, Clone, PartialEq)]
pub enum TableType {
    /// Regular node table (e.g., Person, Company)
    Node { label: String },

    /// Generic edge table with prefix (e.g., edge_KNOWS)
    GenericEdge { rel_type: String },

    /// Fully qualified edge table (e.g., Person_KNOWS_Person)
    TypedEdge {
        source_label: String,
        rel_type: String,
        target_label: String,
    },

    /// Special table for all edges
    AllEdges,
}

/// Classify a SQL table name into node or edge table type.
pub fn classify_table(table_name: &str) -> TableType {
    // Normalize a possibly-qualified, possibly-quoted identifier from the SQL
    // parser. GUI clients (DBeaver, DataGrip, …) issue fully-formed SQL like
    // `SELECT f.* FROM public."Forklift" AS f`, so the raw relation string can
    // be `public."Forklift"`. Strip the schema qualifier (last segment wins)
    // and any surrounding double quotes so the label matches the graph.
    let normalized = normalize_table_ident(table_name);
    let table_name = normalized.as_str();

    // Special case: _edges for all edges
    if table_name == "_edges" {
        return TableType::AllEdges;
    }

    // Check for edge_ prefix (generic edge table)
    if let Some(rel_type) = table_name.strip_prefix("edge_") {
        return TableType::GenericEdge {
            rel_type: rel_type.to_string(),
        };
    }

    // Check for fully qualified edge table: Source_REL_Target
    if let Some((source, rel, target)) = parse_typed_edge_name(table_name) {
        return TableType::TypedEdge {
            source_label: source,
            rel_type: rel,
            target_label: target,
        };
    }

    // Default: node table
    TableType::Node {
        label: table_name.to_string(),
    }
}

/// Normalize a SQL table identifier as rendered by the parser into a bare graph
/// label. GUI clients (DBeaver, DataGrip, …) issue fully-formed SQL whose
/// relation string can carry a schema qualifier, double quotes, and a table
/// alias — e.g. `public."Forklift" AS f`. We must strip all three:
///
/// - trailing ` AS <alias>` (case-insensitive) or a bare trailing ` <alias>`;
/// - the schema qualifier (`schema.table` → `table`), respecting quotes;
/// - surrounding double quotes on the final segment.
///
/// Examples:
/// - `public."Forklift" AS f` → `Forklift`
/// - `public.active_forklifts` → `active_forklifts`
/// - `"Forklift"` → `Forklift`
/// - `Forklift f` (bare alias) → `Forklift`
pub fn normalize_table_ident(raw: &str) -> String {
    let mut s = raw.trim();

    // 1. Strip a trailing alias. `... AS alias` first (case-insensitive), then a
    //    bare `... alias` (a trailing space-separated token that is not part of a
    //    quoted identifier). We only strip a bare alias when the remainder is a
    //    well-formed table ref (ends with `"` or contains no space), to avoid
    //    eating part of an unquoted name.
    if let Some(pos) = find_ascii_case(s, " AS ") {
        s = s[..pos].trim_end();
    } else if let Some(sp) = s.rfind(' ') {
        // Bare alias: strip the last token if what precedes it looks complete
        // (quoted name or dotted/plain identifier with no interior spaces).
        let (head, _tail) = s.split_at(sp);
        let head_trim = head.trim_end();
        if head_trim.ends_with('"') || !head_trim.contains(' ') {
            s = head_trim;
        }
    }

    // 2. Take the final dot-separated segment (schema qualifier dropped),
    //    respecting a quoted final segment that could itself contain a dot.
    let last_segment = if let Some(without_close) = s.strip_suffix('"') {
        match without_close.rfind('"') {
            Some(open) => &s[open..], // includes both quotes
            None => s,
        }
    } else {
        s.rsplit('.').next().unwrap_or(s)
    };

    // 3. Unquote.
    last_segment.trim_matches('"').to_string()
}

/// Find `needle` in `haystack` case-insensitively, returning the byte offset of
/// the match. Both are ASCII in practice (SQL keywords), so a simple scan over
/// ASCII-lowercased windows is sufficient and allocation-light.
fn find_ascii_case(haystack: &str, needle: &str) -> Option<usize> {
    let hl = haystack.to_ascii_lowercase();
    let nl = needle.to_ascii_lowercase();
    hl.find(&nl)
}

/// Parse a fully qualified edge table name like "Person_KNOWS_Person".
///
/// Rules:
/// - Must have exactly 2 underscores
/// - Middle part is the relationship type (uppercase convention)
/// - First and last parts are node labels
fn parse_typed_edge_name(table_name: &str) -> Option<(String, String, String)> {
    let parts: Vec<&str> = table_name.split('_').collect();

    // Need at least 3 parts: Source_REL_Target
    if parts.len() < 3 {
        return None;
    }

    // Find the relationship type (uppercase part in the middle)
    // Heuristic: if a part is all uppercase, treat it as the relationship type
    let mut rel_idx = None;
    for (i, part) in parts.iter().enumerate().skip(1).take(parts.len() - 2) {
        if part.chars().all(|c| c.is_uppercase() || c == '_') && !part.is_empty() {
            rel_idx = Some(i);
            break;
        }
    }

    if let Some(idx) = rel_idx {
        let source = parts[..idx].join("_");
        let rel = parts[idx].to_string();
        let target = parts[idx + 1..].join("_");

        if !source.is_empty() && !rel.is_empty() && !target.is_empty() {
            return Some((source, rel, target));
        }
    }

    // Fallback: assume pattern is Source_REL_Target with single underscore separators
    // This handles cases like "Person_KNOWS_Company" where KNOWS is uppercase
    if parts.len() == 3 {
        return Some((
            parts[0].to_string(),
            parts[1].to_string(),
            parts[2].to_string(),
        ));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_table_ident_gui_forms() {
        // Schema-qualified + quoted + aliased (DBeaver's "read table data" SQL).
        assert_eq!(
            normalize_table_ident(r#"public."Forklift" AS f"#),
            "Forklift"
        );
        // Schema-qualified, quoted, no alias.
        assert_eq!(normalize_table_ident(r#"public."Forklift""#), "Forklift");
        // Schema-qualified, unquoted (matview).
        assert_eq!(
            normalize_table_ident("public.active_forklifts"),
            "active_forklifts"
        );
        // Quoted, unqualified.
        assert_eq!(normalize_table_ident(r#""Forklift""#), "Forklift");
        // Bare alias (no AS).
        assert_eq!(normalize_table_ident("Forklift f"), "Forklift");
        // lowercase `as`.
        assert_eq!(normalize_table_ident(r#"public."Zone" as z"#), "Zone");
        // Plain name unchanged.
        assert_eq!(normalize_table_ident("Person"), "Person");
    }

    #[test]
    fn test_classify_table_normalizes_gui_idents() {
        // The DBeaver form must classify as a plain node label, not a mangled one.
        assert_eq!(
            classify_table(r#"public."Forklift" AS f"#),
            TableType::Node {
                label: "Forklift".into()
            }
        );
    }

    #[test]
    fn test_classify_node_table() {
        assert_eq!(
            classify_table("Person"),
            TableType::Node {
                label: "Person".into()
            }
        );
        assert_eq!(
            classify_table("Company"),
            TableType::Node {
                label: "Company".into()
            }
        );
    }

    #[test]
    fn test_classify_generic_edge() {
        assert_eq!(
            classify_table("edge_KNOWS"),
            TableType::GenericEdge {
                rel_type: "KNOWS".into()
            }
        );
        assert_eq!(
            classify_table("edge_FOLLOWS"),
            TableType::GenericEdge {
                rel_type: "FOLLOWS".into()
            }
        );
    }

    #[test]
    fn test_classify_typed_edge() {
        assert_eq!(
            classify_table("Person_KNOWS_Person"),
            TableType::TypedEdge {
                source_label: "Person".into(),
                rel_type: "KNOWS".into(),
                target_label: "Person".into(),
            }
        );

        assert_eq!(
            classify_table("Company_EMPLOYS_Person"),
            TableType::TypedEdge {
                source_label: "Company".into(),
                rel_type: "EMPLOYS".into(),
                target_label: "Person".into(),
            }
        );
    }

    #[test]
    fn test_classify_all_edges() {
        assert_eq!(classify_table("_edges"), TableType::AllEdges);
    }

    #[test]
    fn test_parse_typed_edge_name() {
        assert_eq!(
            parse_typed_edge_name("Person_KNOWS_Person"),
            Some(("Person".into(), "KNOWS".into(), "Person".into()))
        );

        assert_eq!(
            parse_typed_edge_name("User_FOLLOWS_Post"),
            Some(("User".into(), "FOLLOWS".into(), "Post".into()))
        );

        // Multi-word labels with underscores
        assert_eq!(
            parse_typed_edge_name("Social_User_FOLLOWS_Blog_Post"),
            Some(("Social_User".into(), "FOLLOWS".into(), "Blog_Post".into()))
        );

        // Not an edge table
        assert_eq!(parse_typed_edge_name("Person"), None);
        assert_eq!(parse_typed_edge_name("edge_KNOWS"), None);
    }
}
