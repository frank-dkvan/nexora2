//! Non-ASCII column-alias bridge.
//!
//! cypher-parser 0.5 — and nexora-language — lex identifiers as ASCII-only
//! (`is_ascii_alphabetic`), so a query like
//! `MATCH (a:Airport) RETURN count(*) AS 航线数` fails to parse with
//! "unexpected character `航`". Neither parser supports backtick-escaped
//! identifiers either, so the standard Cypher `` AS `航线数` `` escape does not
//! help.
//!
//! This bridge rewrites each non-ASCII column alias to an ASCII placeholder
//! before parsing/execution, then renames the result columns back afterwards.
//! Only aliases introduced with `AS` are bridged, and replacement happens only
//! outside string literals so data values (e.g. `WHERE n.name = '北京'`) are
//! never touched. No-op when the query has no non-ASCII alias.

use crate::CypherResult;

/// A planned non-ASCII alias rewrite: the ASCII-safe query to execute plus the
/// placeholder→original column-name map used to restore result headers.
pub struct AliasRewrite {
    /// Query to execute (original when `mappings` is empty, else ASCII-rewritten).
    pub query: String,
    /// (placeholder, original) pairs. Empty = no rewrite (no-op).
    pub mappings: Vec<(String, String)>,
}

/// Analyze `query` for `AS <alias>` where `<alias>` contains a non-ASCII
/// character, and replace each such alias (everywhere outside string literals)
/// with an ASCII placeholder. Returns the rewritten query and the
/// placeholder→original map. No-op (original query, empty map) when there are no
/// non-ASCII aliases.
pub fn plan_alias_rewrite(query: &str) -> AliasRewrite {
    let chars: Vec<char> = query.chars().collect();

    // 1) Collect distinct non-ASCII aliases that follow an `AS` keyword.
    let mut originals: Vec<String> = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        // Skip string literals wholesale — an `AS` inside a string is data.
        if chars[i] == '\'' || chars[i] == '"' {
            i = skip_string(&chars, i);
            continue;
        }
        if is_as_keyword(&chars, i) {
            let mut j = i + 2;
            while j < chars.len() && chars[j].is_whitespace() {
                j += 1;
            }
            let start = j;
            while j < chars.len() && is_ident_char(chars[j]) {
                j += 1;
            }
            if j > start {
                let ident: String = chars[start..j].iter().collect();
                if !ident.is_ascii() && !originals.contains(&ident) {
                    originals.push(ident);
                }
            }
            i = j;
            continue;
        }
        i += 1;
    }

    if originals.is_empty() {
        return AliasRewrite {
            query: query.to_string(),
            mappings: Vec::new(),
        };
    }

    // 2) Assign ASCII placeholders. `__nx_alias{N}` mirrors the `__nx_`
    //    convention already used by the edge-property bridge.
    let mappings: Vec<(String, String)> = originals
        .iter()
        .enumerate()
        .map(|(n, orig)| (format!("__nx_alias{n}"), orig.clone()))
        .collect();

    // 3) Replace whole identifier tokens equal to an original with its
    //    placeholder, skipping string literals. Token-boundary matching means we
    //    only touch complete identifiers (the alias definition and any ORDER
    //    BY / WITH references), never substrings.
    let rewritten = replace_tokens_outside_strings(&chars, &mappings);

    AliasRewrite {
        query: rewritten,
        mappings,
    }
}

/// Rename placeholder result columns back to their original non-ASCII names.
/// Pass-through for non-row results (e.g. writes) and for the no-op case.
pub fn restore_aliases(result: CypherResult, rw: &AliasRewrite) -> CypherResult {
    if rw.mappings.is_empty() {
        return result;
    }
    match result {
        CypherResult::Rows { columns, rows } => {
            let columns = columns
                .into_iter()
                .map(|c| {
                    rw.mappings
                        .iter()
                        .find(|(ph, _)| ph == &c)
                        .map(|(_, orig)| orig.clone())
                        .unwrap_or(c)
                })
                .collect();
            CypherResult::Rows { columns, rows }
        }
        other => other,
    }
}

/// A character that may appear in an identifier: Unicode alphanumeric (covers
/// CJK) or `_`.
fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Is `AS` a standalone keyword at `chars[i]` (case-insensitive), i.e. preceded
/// by a non-identifier boundary and followed by whitespace?
fn is_as_keyword(chars: &[char], i: usize) -> bool {
    let (Some(&a), Some(&s)) = (chars.get(i), chars.get(i + 1)) else {
        return false;
    };
    if !matches!(a, 'a' | 'A') || !matches!(s, 's' | 'S') {
        return false;
    }
    let prev_ok = i == 0 || !is_ident_char(chars[i - 1]);
    let next_ok = chars.get(i + 2).is_some_and(|c| c.is_whitespace());
    prev_ok && next_ok
}

/// Return the index just past the string literal starting at `chars[start]`
/// (a `'` or `"`), honoring backslash escapes.
fn skip_string(chars: &[char], start: usize) -> usize {
    let quote = chars[start];
    let mut i = start + 1;
    while i < chars.len() {
        let ch = chars[i];
        if ch == '\\' {
            i += 2;
            continue;
        }
        if ch == quote {
            return i + 1;
        }
        i += 1;
    }
    i
}

/// Rebuild the query, replacing whole identifier tokens equal to a mapping's
/// original with its placeholder. String literals are copied verbatim.
fn replace_tokens_outside_strings(chars: &[char], mappings: &[(String, String)]) -> String {
    let mut out = String::with_capacity(chars.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        // String literal: copy verbatim (including escapes and the closing quote).
        if c == '\'' || c == '"' {
            let end = skip_string(chars, i);
            for &ch in &chars[i..end.min(chars.len())] {
                out.push(ch);
            }
            i = end;
            continue;
        }
        // Identifier run: may equal an alias to remap.
        if is_ident_char(c) {
            let start = i;
            while i < chars.len() && is_ident_char(chars[i]) {
                i += 1;
            }
            let run: String = chars[start..i].iter().collect();
            match mappings.iter().find(|(_, orig)| orig == &run) {
                Some((ph, _)) => out.push_str(ph),
                None => out.push_str(&run),
            }
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_alias_is_noop() {
        let q = "MATCH (a:Airport) RETURN a.code AS code";
        let rw = plan_alias_rewrite(q);
        assert!(rw.mappings.is_empty());
        assert_eq!(rw.query, q);
    }

    #[test]
    fn rewrites_single_non_ascii_alias() {
        let rw = plan_alias_rewrite("MATCH (a:Airport) RETURN count(*) AS 航线数");
        assert_eq!(rw.mappings.len(), 1);
        assert_eq!(rw.mappings[0].1, "航线数");
        assert!(rw.query.contains("AS __nx_alias0"));
        assert!(!rw.query.contains('航'));
    }

    #[test]
    fn rewrites_alias_and_its_order_by_reference() {
        let rw = plan_alias_rewrite("MATCH (a) RETURN count(*) AS 数量 ORDER BY 数量 DESC");
        assert_eq!(rw.mappings.len(), 1);
        // Both the definition and the ORDER BY reference must be rewritten so the
        // executed query is internally consistent.
        assert_eq!(rw.query.matches("__nx_alias0").count(), 2);
        assert!(!rw.query.contains('数'));
    }

    #[test]
    fn does_not_touch_non_ascii_in_string_literals() {
        // `北京` is a data value, not an alias — it must survive untouched.
        let rw =
            plan_alias_rewrite("MATCH (a:Airport) WHERE a.city = '北京' RETURN a.code AS 代码");
        assert_eq!(rw.mappings.len(), 1);
        assert_eq!(rw.mappings[0].1, "代码");
        assert!(
            rw.query.contains("'北京'"),
            "string literal must be preserved"
        );
        assert!(!rw.query.contains("AS 代码"));
    }

    #[test]
    fn multiple_distinct_aliases() {
        let rw = plan_alias_rewrite("MATCH (a:Airport) RETURN a.code AS 代码, a.city AS 城市");
        assert_eq!(rw.mappings.len(), 2);
        assert!(rw.query.contains("AS __nx_alias0"));
        assert!(rw.query.contains("AS __nx_alias1"));
    }

    #[test]
    fn restore_maps_columns_back() {
        let rw = plan_alias_rewrite("MATCH (a) RETURN count(*) AS 数量");
        let result = CypherResult::Rows {
            columns: vec!["__nx_alias0".to_string()],
            rows: vec![vec![serde_json::json!(5)]],
        };
        let restored = restore_aliases(result, &rw);
        let CypherResult::Rows { columns, .. } = restored else {
            panic!("expected rows");
        };
        assert_eq!(columns, vec!["数量".to_string()]);
    }

    #[test]
    fn restore_is_noop_without_mappings() {
        let rw = plan_alias_rewrite("MATCH (a) RETURN a.code");
        let result = CypherResult::Rows {
            columns: vec!["a.code".to_string()],
            rows: vec![],
        };
        let restored = restore_aliases(result, &rw);
        let CypherResult::Rows { columns, .. } = restored else {
            panic!("expected rows");
        };
        assert_eq!(columns, vec!["a.code".to_string()]);
    }
}
