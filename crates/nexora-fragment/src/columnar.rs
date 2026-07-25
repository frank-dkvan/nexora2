//! Columnar cold-layer format with predicate + projection pushdown (P3 OLAP).
//!
//! Warm/cold fragments are archived for analytics, not point lookups. A
//! row-oriented `nodes.jsonl` body forces every OLAP scan to parse every field
//! of every node even when the query touches one column and one value range.
//! This module stores a fragment's node records **column-by-column** with
//! per-column min/max statistics, so a scan can:
//!
//! - **project**: decode only the columns a query selects (skip the rest), and
//! - **push down predicates**: skip the whole fragment when a column's
//!   `[min,max]` can't satisfy the filter, and skip individual rows that don't
//!   match — without materializing unrelated columns.
//!
//! The format is self-contained (JSON envelope, same discipline as the rest of
//! the crate — no Arrow/Parquet dependency) but columnar in layout: one entry
//! per column, each holding that column's values across all rows (with a null
//! bitmap via `Option`). It round-trips losslessly with the row records the
//! sealer produces, so a fragment can be archived as columnar and still replayed
//! by [`crate::time_travel`] after a row reconstruction.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// A comparison operator for a pushdown predicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PredicateOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A single-column predicate: `column <op> value`. Rows where the column is
/// null never match (SQL NULL semantics).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Predicate {
    pub column: String,
    pub op: PredicateOp,
    pub value: Value,
}

impl Predicate {
    pub fn new(column: impl Into<String>, op: PredicateOp, value: Value) -> Self {
        Self {
            column: column.into(),
            op,
            value,
        }
    }

    /// Evaluate the predicate against a single (possibly null) cell value.
    fn matches(&self, cell: Option<&Value>) -> bool {
        let Some(cell) = cell else { return false };
        match cmp_json(cell, &self.value) {
            Some(ord) => match self.op {
                PredicateOp::Eq => ord == std::cmp::Ordering::Equal,
                PredicateOp::Ne => ord != std::cmp::Ordering::Equal,
                PredicateOp::Lt => ord == std::cmp::Ordering::Less,
                PredicateOp::Le => ord != std::cmp::Ordering::Greater,
                PredicateOp::Gt => ord == std::cmp::Ordering::Greater,
                PredicateOp::Ge => ord != std::cmp::Ordering::Less,
            },
            // Incomparable types (e.g. string vs number): only Ne is satisfiable.
            None => matches!(self.op, PredicateOp::Ne),
        }
    }
}

/// Per-column statistics for predicate pushdown (skip a fragment whose range
/// can't satisfy a filter). `min`/`max` are `None` for an all-null column.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ColumnStats {
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub null_count: usize,
    pub non_null_count: usize,
}

impl ColumnStats {
    /// Whether a fragment with these stats *could* contain a row matching `pred`
    /// on this column. A `false` return means the fragment can be skipped whole.
    /// Conservative: returns `true` when it can't prove exclusion.
    fn could_match(&self, pred: &Predicate) -> bool {
        // All-null column: only Ne against a non-null could (vacuously) not match;
        // but null never matches any op, so an all-null column matches nothing.
        let (Some(min), Some(max)) = (&self.min, &self.max) else {
            return false;
        };
        match pred.op {
            // Range predicates: compare against the surviving bound.
            PredicateOp::Lt => cmp_json(min, &pred.value)
                .map(|o| o == std::cmp::Ordering::Less)
                .unwrap_or(true),
            PredicateOp::Le => cmp_json(min, &pred.value)
                .map(|o| o != std::cmp::Ordering::Greater)
                .unwrap_or(true),
            PredicateOp::Gt => cmp_json(max, &pred.value)
                .map(|o| o == std::cmp::Ordering::Greater)
                .unwrap_or(true),
            PredicateOp::Ge => cmp_json(max, &pred.value)
                .map(|o| o != std::cmp::Ordering::Less)
                .unwrap_or(true),
            // Eq: value must lie within [min,max].
            PredicateOp::Eq => {
                let above_min = cmp_json(&pred.value, min)
                    .map(|o| o != std::cmp::Ordering::Less)
                    .unwrap_or(true);
                let below_max = cmp_json(&pred.value, max)
                    .map(|o| o != std::cmp::Ordering::Greater)
                    .unwrap_or(true);
                above_min && below_max
            }
            // Ne: only excludable if every value equals `value` (min==max==value).
            PredicateOp::Ne => {
                !(cmp_json(min, &pred.value) == Some(std::cmp::Ordering::Equal)
                    && cmp_json(max, &pred.value) == Some(std::cmp::Ordering::Equal))
            }
        }
    }

    fn observe(&mut self, v: Option<&Value>) {
        match v {
            None => self.null_count += 1,
            Some(v) => {
                self.non_null_count += 1;
                if self.min.as_ref().and_then(|m| cmp_json(v, m)) == Some(std::cmp::Ordering::Less)
                    || self.min.is_none()
                {
                    self.min = Some(v.clone());
                }
                if self.max.as_ref().and_then(|m| cmp_json(v, m))
                    == Some(std::cmp::Ordering::Greater)
                    || self.max.is_none()
                {
                    self.max = Some(v.clone());
                }
            }
        }
    }
}

/// A columnar-encoded fragment body: node ids + timestamp + one entry per
/// property column, each a per-row `Option<Value>` vector, plus column stats.
/// Edges are retained as an opaque per-row JSON blob (analytics queries target
/// properties; edges reconstruct on row rebuild without needing pushdown).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnarFragment {
    /// Node ids, one per row (row order is the canonical order).
    pub ids: Vec<String>,
    /// Timestamp per row.
    pub timestamps: Vec<u64>,
    /// Property columns: name → per-row cell (None = null for that row).
    pub columns: BTreeMap<String, Vec<Option<Value>>>,
    /// Per-column stats for predicate pushdown.
    pub stats: BTreeMap<String, ColumnStats>,
    /// Opaque edges per row (the fragment record's `edges` array, if any).
    pub edges: Vec<Option<Value>>,
}

impl ColumnarFragment {
    /// Number of rows (nodes) in the fragment.
    pub fn row_count(&self) -> usize {
        self.ids.len()
    }

    /// Build a columnar fragment from row records (the `nodes.jsonl` shape:
    /// `{id, timestamp, properties:{..}, edges:[..]}`). Columns are the union of
    /// all `properties` keys; a row missing a key gets a null cell there.
    pub fn from_rows(rows: &[Value]) -> Self {
        // Discover the full column set first (union of property keys).
        let mut col_names: Vec<String> = Vec::new();
        for row in rows {
            if let Some(props) = row.get("properties").and_then(|p| p.as_object()) {
                for k in props.keys() {
                    if !col_names.contains(k) {
                        col_names.push(k.clone());
                    }
                }
            }
        }

        let mut ids = Vec::with_capacity(rows.len());
        let mut timestamps = Vec::with_capacity(rows.len());
        let mut edges = Vec::with_capacity(rows.len());
        let mut columns: BTreeMap<String, Vec<Option<Value>>> = col_names
            .iter()
            .map(|n| (n.clone(), Vec::with_capacity(rows.len())))
            .collect();
        let mut stats: BTreeMap<String, ColumnStats> = col_names
            .iter()
            .map(|n| (n.clone(), ColumnStats::default()))
            .collect();

        for row in rows {
            ids.push(
                row.get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
            );
            timestamps.push(row.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0));
            edges.push(row.get("edges").cloned());

            let props = row.get("properties").and_then(|p| p.as_object());
            for name in &col_names {
                let cell = props.and_then(|p| p.get(name)).cloned();
                stats.get_mut(name).unwrap().observe(cell.as_ref());
                columns.get_mut(name).unwrap().push(cell);
            }
        }

        Self {
            ids,
            timestamps,
            columns,
            stats,
            edges,
        }
    }

    /// Serialize to bytes for the tiered store (JSON envelope).
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Deserialize from tiered-store bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }

    /// Can this fragment possibly contain a row matching *all* predicates?
    /// Returns `false` (skip the whole fragment) if any predicate's column
    /// stats prove no row can match. Predicates on unknown columns → no match
    /// (the column is absent, so it's null everywhere, which matches nothing).
    pub fn could_match(&self, predicates: &[Predicate]) -> bool {
        predicates.iter().all(|p| {
            self.stats
                .get(&p.column)
                .map(|s| s.could_match(p))
                .unwrap_or(false)
        })
    }

    /// Scan with projection + predicate pushdown. Returns the selected rows,
    /// each as `{id, timestamp, <projected columns>}`, for rows matching every
    /// predicate. `project` is the property columns to include (empty = all).
    /// Only the projected columns and the predicate columns are ever touched.
    pub fn scan(&self, predicates: &[Predicate], project: &[String]) -> Vec<Value> {
        // Fragment-level pushdown: skip entirely if stats rule it out.
        if !self.could_match(predicates) {
            return Vec::new();
        }

        let projected: Vec<String> = if project.is_empty() {
            self.columns.keys().cloned().collect()
        } else {
            project.to_vec()
        };

        let mut out = Vec::new();
        for row in 0..self.row_count() {
            // Row-level predicate check (only predicate columns are read).
            let keep = predicates.iter().all(|p| {
                let cell = self
                    .columns
                    .get(&p.column)
                    .and_then(|c| c.get(row))
                    .and_then(|c| c.as_ref());
                p.matches(cell)
            });
            if !keep {
                continue;
            }
            let mut obj = serde_json::Map::new();
            obj.insert("id".into(), Value::String(self.ids[row].clone()));
            obj.insert("timestamp".into(), Value::from(self.timestamps[row]));
            for name in &projected {
                if let Some(col) = self.columns.get(name) {
                    if let Some(Some(v)) = col.get(row) {
                        obj.insert(name.clone(), v.clone());
                    }
                }
            }
            out.push(Value::Object(obj));
        }
        out
    }

    /// Reconstruct the full row records (the `nodes.jsonl` shape) so an archived
    /// columnar fragment can be replayed by the time-travel engine.
    pub fn to_rows(&self) -> Vec<Value> {
        let mut out = Vec::with_capacity(self.row_count());
        for row in 0..self.row_count() {
            let mut obj = serde_json::Map::new();
            obj.insert("id".into(), Value::String(self.ids[row].clone()));
            obj.insert("timestamp".into(), Value::from(self.timestamps[row]));
            let mut props = serde_json::Map::new();
            for (name, col) in &self.columns {
                if let Some(Some(v)) = col.get(row) {
                    props.insert(name.clone(), v.clone());
                }
            }
            if !props.is_empty() {
                obj.insert("properties".into(), Value::Object(props));
            }
            if let Some(Some(edges)) = self.edges.get(row) {
                obj.insert("edges".into(), edges.clone());
            }
            out.push(Value::Object(obj));
        }
        out
    }
}

/// Compare two JSON values for ordering. Numbers compare numerically, strings
/// lexically, bools false<true. Returns `None` for incomparable types.
fn cmp_json(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64().partial_cmp(&y.as_f64()),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rows() -> Vec<Value> {
        vec![
            json!({"id":"n1","timestamp":1000,"properties":{"speed":50,"name":"Alice"}}),
            json!({"id":"n2","timestamp":1100,"properties":{"speed":80,"name":"Bob"}}),
            json!({"id":"n3","timestamp":1200,"properties":{"speed":30}}),
        ]
    }

    #[test]
    fn round_trip_rows() {
        let cf = ColumnarFragment::from_rows(&rows());
        assert_eq!(cf.row_count(), 3);
        let back = cf.to_rows();
        // n1 fully reconstructs.
        assert_eq!(back[0]["id"], json!("n1"));
        assert_eq!(back[0]["properties"]["speed"], json!(50));
        assert_eq!(back[0]["properties"]["name"], json!("Alice"));
        // n3 has no name → properties omit it.
        assert_eq!(back[2]["properties"].get("name"), None);
    }

    #[test]
    fn serialize_round_trip() {
        let cf = ColumnarFragment::from_rows(&rows());
        let bytes = cf.to_bytes();
        let cf2 = ColumnarFragment::from_bytes(&bytes).unwrap();
        assert_eq!(cf2.row_count(), 3);
        assert_eq!(cf2.to_rows(), cf.to_rows());
    }

    #[test]
    fn column_stats_capture_min_max() {
        let cf = ColumnarFragment::from_rows(&rows());
        let s = &cf.stats["speed"];
        assert_eq!(s.min, Some(json!(30)));
        assert_eq!(s.max, Some(json!(80)));
        assert_eq!(s.non_null_count, 3);
        assert_eq!(s.null_count, 0);
        // name is null on n3.
        assert_eq!(cf.stats["name"].null_count, 1);
    }

    #[test]
    fn predicate_pushdown_skips_fragment() {
        let cf = ColumnarFragment::from_rows(&rows());
        // speed > 100: max is 80 → whole fragment skippable.
        let pred = Predicate::new("speed", PredicateOp::Gt, json!(100));
        assert!(!cf.could_match(std::slice::from_ref(&pred)));
        assert!(cf.scan(&[pred], &[]).is_empty());

        // speed >= 50: max 80 qualifies → not skippable.
        let pred = Predicate::new("speed", PredicateOp::Ge, json!(50));
        assert!(cf.could_match(&[pred]));
    }

    #[test]
    fn scan_filters_rows_and_projects_columns() {
        let cf = ColumnarFragment::from_rows(&rows());
        // speed >= 50 → n1(50), n2(80); project only "name".
        let hits = cf.scan(
            &[Predicate::new("speed", PredicateOp::Ge, json!(50))],
            &["name".to_string()],
        );
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0]["id"], json!("n1"));
        assert_eq!(hits[0]["name"], json!("Alice"));
        // Projection excluded "speed" from the output.
        assert_eq!(hits[0].get("speed"), None);
        assert_eq!(hits[1]["id"], json!("n2"));
    }

    #[test]
    fn scan_unknown_column_predicate_matches_nothing() {
        let cf = ColumnarFragment::from_rows(&rows());
        let hits = cf.scan(&[Predicate::new("missing", PredicateOp::Eq, json!(1))], &[]);
        assert!(hits.is_empty());
    }

    #[test]
    fn eq_predicate_selects_exact_rows() {
        let cf = ColumnarFragment::from_rows(&rows());
        let hits = cf.scan(&[Predicate::new("speed", PredicateOp::Eq, json!(80))], &[]);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["id"], json!("n2"));
    }

    #[test]
    fn null_never_matches() {
        let cf = ColumnarFragment::from_rows(&rows());
        // n3 has no "name"; an Eq on name must not select it.
        let hits = cf.scan(
            &[Predicate::new("name", PredicateOp::Eq, json!("Zoe"))],
            &[],
        );
        assert!(hits.is_empty());
    }
}
