//! Merge per-owner partial results into the whole-graph answer.
//!
//! Each strategy in [`super::MergeKind`] has a matching combine here:
//! - `Concat` — union the rows.
//! - `GlobalAggregate` — combine partials into one row.
//! - `GroupedAggregate` — combine partials per group key.
//!
//! After the merge, the coordinator applies global DISTINCT → ORDER BY →
//! SKIP → LIMIT (in that Cypher-defined order).

use super::{DistributedPlan, JoinPost, JoinPostItem, MergeKind, ProjItem, RowFilter, WithStage};
use crate::RouterError;
use nexora_language::ast::{AggFunction, BinaryOp};
use serde_json::Value;
use std::collections::BTreeMap;

/// Merge per-owner `(columns, rows)` into the final result per `plan`.
pub fn merge_rows(
    plan: &DistributedPlan,
    per_owner: Vec<(Vec<String>, Vec<Vec<Value>>)>,
) -> Result<(Vec<String>, Vec<Vec<Value>>), RouterError> {
    let mut rows = match &plan.kind {
        MergeKind::Concat => concat(per_owner),
        MergeKind::GlobalAggregate { items } => global_aggregate(items, per_owner)?,
        MergeKind::GroupedAggregate { items } => grouped_aggregate(items, per_owner)?,
    };

    // Global DISTINCT (dedupe whole rows by their JSON encoding).
    if plan.distinct {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|r| seen.insert(serde_json::to_string(r).unwrap_or_default()));
    }

    // Global ORDER BY (stable, multi-key).
    if !plan.order_by.is_empty() {
        let order = plan.order_by.clone();
        rows.sort_by(|a, b| {
            for (idx, ascending) in &order {
                let (va, vb) = (a.get(*idx), b.get(*idx));
                let ord = cmp_json_opt(va, vb);
                if ord != std::cmp::Ordering::Equal {
                    return if *ascending { ord } else { ord.reverse() };
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    // Global SKIP then LIMIT.
    if let Some(skip) = plan.skip {
        if skip >= rows.len() {
            rows.clear();
        } else {
            rows.drain(0..skip);
        }
    }
    if let Some(limit) = plan.limit {
        rows.truncate(limit);
    }

    Ok((plan.columns.clone(), rows))
}

/// Apply local grouping/aggregation + order/skip/limit/distinct over a
/// relationship/path join's already-gathered raw rows. Unlike [`merge_rows`]
/// (which combines per-owner *partials*), the join rows are complete on the
/// coordinator, so this is a single-pass local aggregation.
pub fn apply_join_post(
    post: &JoinPost,
    raw_rows: Vec<Vec<Value>>,
) -> (Vec<String>, Vec<Vec<Value>>) {
    let has_aggregate = post
        .items
        .iter()
        .any(|it| matches!(it, JoinPostItem::Aggregate { .. }));
    let group_positions: Vec<usize> = (0..post.items.len())
        .filter(|i| matches!(post.items[*i], JoinPostItem::Grouping { .. }))
        .collect();

    let mut rows: Vec<Vec<Value>> = if !has_aggregate {
        // Pure projection over the join (no aggregates): map each raw row to the
        // grouping columns in item order.
        raw_rows
            .iter()
            .map(|raw| {
                post.items
                    .iter()
                    .map(|it| match it {
                        JoinPostItem::Grouping { raw_idx } => {
                            raw.get(*raw_idx).cloned().unwrap_or(Value::Null)
                        }
                        JoinPostItem::Aggregate { .. } => Value::Null,
                    })
                    .collect()
            })
            .collect()
    } else if group_positions.is_empty() {
        // Global aggregate (no grouping) → one output row.
        let mut combiners = init_join_combiners(&post.items);
        for raw in &raw_rows {
            feed_join_row(&mut combiners, &post.items, raw);
        }
        vec![assemble_join_row(&post.items, &[], &combiners)]
    } else {
        // Grouped aggregate: bucket rows by their grouping columns.
        let mut groups: BTreeMap<String, (Vec<Value>, Vec<AggCombiner>)> = BTreeMap::new();
        for raw in &raw_rows {
            let key_vals: Vec<Value> = post
                .items
                .iter()
                .filter_map(|it| match it {
                    JoinPostItem::Grouping { raw_idx } => {
                        Some(raw.get(*raw_idx).cloned().unwrap_or(Value::Null))
                    }
                    _ => None,
                })
                .collect();
            let key = serde_json::to_string(&key_vals).unwrap_or_default();
            let entry = groups
                .entry(key)
                .or_insert_with(|| (key_vals.clone(), init_join_combiners(&post.items)));
            feed_join_row(&mut entry.1, &post.items, raw);
        }
        groups
            .into_values()
            .map(|(gvals, combiners)| assemble_join_row(&post.items, &gvals, &combiners))
            .collect()
    };

    // Coordinator-side DISTINCT → ORDER BY → SKIP → LIMIT (Cypher order).
    if post.distinct {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|r| seen.insert(serde_json::to_string(r).unwrap_or_default()));
    }
    if !post.order_by.is_empty() {
        rows.sort_by(|a, b| {
            for (idx, ascending) in &post.order_by {
                let ord = cmp_json_opt(a.get(*idx), b.get(*idx));
                if ord != std::cmp::Ordering::Equal {
                    return if *ascending { ord } else { ord.reverse() };
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    if let Some(skip) = post.skip {
        if skip >= rows.len() {
            rows.clear();
        } else {
            rows.drain(0..skip);
        }
    }
    if let Some(limit) = post.limit {
        rows.truncate(limit);
    }

    (post.columns.clone(), rows)
}

/// Apply the second stage of a WITH pipeline to the materialized stage-1 rows:
/// WHERE filter (HAVING) → RETURN projection → DISTINCT → ORDER BY → SKIP →
/// LIMIT. `stage1_rows` are laid out per `stage.stage1_columns`.
pub fn apply_with_stage(
    stage: &WithStage,
    stage1_rows: Vec<Vec<Value>>,
) -> (Vec<String>, Vec<Vec<Value>>) {
    // WHERE filter over stage-1 columns (post-aggregation HAVING).
    let filtered: Vec<Vec<Value>> = match &stage.filter {
        None => stage1_rows,
        Some(f) => stage1_rows
            .into_iter()
            .filter(|row| row_passes_filter(f, row))
            .collect(),
    };

    // RETURN projection: pick the stage-1 columns the RETURN selects, in order.
    let mut rows: Vec<Vec<Value>> = filtered
        .iter()
        .map(|row| {
            stage
                .projection
                .iter()
                .map(|&idx| row.get(idx).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();

    // DISTINCT → ORDER BY → SKIP → LIMIT (Cypher order) over the output rows.
    if stage.distinct {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|r| seen.insert(serde_json::to_string(r).unwrap_or_default()));
    }
    if !stage.order_by.is_empty() {
        rows.sort_by(|a, b| {
            for (idx, ascending) in &stage.order_by {
                let ord = cmp_json_opt(a.get(*idx), b.get(*idx));
                if ord != std::cmp::Ordering::Equal {
                    return if *ascending { ord } else { ord.reverse() };
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    if let Some(skip) = stage.skip {
        if skip >= rows.len() {
            rows.clear();
        } else {
            rows.drain(0..skip);
        }
    }
    if let Some(limit) = stage.limit {
        rows.truncate(limit);
    }

    (stage.columns.clone(), rows)
}

/// Whether a stage-1 row passes a single-comparison WITH-WHERE filter.
fn row_passes_filter(f: &RowFilter, row: &[Value]) -> bool {
    let Some(cell) = row.get(f.col) else {
        return false;
    };
    let ord = cmp_json(cell, &f.literal);
    match f.op {
        BinaryOp::Eq => ord == std::cmp::Ordering::Equal,
        BinaryOp::Ne => ord != std::cmp::Ordering::Equal,
        BinaryOp::Lt => ord == std::cmp::Ordering::Less,
        BinaryOp::Le => ord != std::cmp::Ordering::Greater,
        BinaryOp::Gt => ord == std::cmp::Ordering::Greater,
        BinaryOp::Ge => ord != std::cmp::Ordering::Less,
        // Non-comparison ops are rejected in `build_row_filter`; unreachable.
        _ => false,
    }
}

/// Init a combiner per aggregate item (in item order).
fn init_join_combiners(items: &[JoinPostItem]) -> Vec<AggCombiner> {
    items
        .iter()
        .filter_map(|it| match it {
            JoinPostItem::Aggregate { func, .. } => Some(match func {
                AggFunction::Count => AggCombiner::Count(0),
                AggFunction::Sum => AggCombiner::Sum(0.0),
                AggFunction::Min => AggCombiner::Min(None),
                AggFunction::Max => AggCombiner::Max(None),
                AggFunction::Avg => AggCombiner::Avg { sum: 0.0, count: 0 },
                _ => AggCombiner::Count(0),
            }),
            _ => None,
        })
        .collect()
}

/// Feed one raw join row into the aggregate combiners.
fn feed_join_row(combiners: &mut [AggCombiner], items: &[JoinPostItem], raw: &[Value]) {
    let mut ci = 0;
    for it in items {
        if let JoinPostItem::Aggregate { raw_idx, .. } = it {
            let is_count_star = raw_idx.is_none();
            let val = raw_idx.and_then(|i| raw.get(i));
            combiners[ci].feed_raw(val, is_count_star);
            ci += 1;
        }
    }
}

/// Build one output row: grouping values (in `gvals`) + finalized aggregates.
fn assemble_join_row(
    items: &[JoinPostItem],
    gvals: &[Value],
    combiners: &[AggCombiner],
) -> Vec<Value> {
    let mut row = Vec::with_capacity(items.len());
    let mut gi = 0;
    let mut ci = 0;
    for it in items {
        match it {
            JoinPostItem::Grouping { .. } => {
                row.push(gvals.get(gi).cloned().unwrap_or(Value::Null));
                gi += 1;
            }
            JoinPostItem::Aggregate { .. } => {
                row.push(
                    combiners
                        .get(ci)
                        .map(|c| c.finalize())
                        .unwrap_or(Value::Null),
                );
                ci += 1;
            }
        }
    }
    row
}

/// Concatenate rows from every owner.
fn concat(per_owner: Vec<(Vec<String>, Vec<Vec<Value>>)>) -> Vec<Vec<Value>> {
    let mut out = Vec::new();
    for (_cols, mut rows) in per_owner {
        out.append(&mut rows);
    }
    out
}

/// Combine per-owner partials into a single global-aggregate row.
///
/// The owner query is rewritten (see [`build_partial_aggregate_return`]) so each
/// aggregate emits the partials the combine needs — notably `avg` emits
/// `sum,count`. The owner columns therefore line up positionally with the
/// *expanded* partial list, which we fold here back into the output columns.
fn global_aggregate(
    items: &[ProjItem],
    per_owner: Vec<(Vec<String>, Vec<Vec<Value>>)>,
) -> Result<Vec<Vec<Value>>, RouterError> {
    let mut combiners = init_combiners(items);
    for (_cols, rows) in &per_owner {
        // A global aggregate yields exactly one partial row per owner (or zero
        // if the owner has no matching nodes → treated as empty partials).
        if let Some(row) = rows.first() {
            feed_partial_row(&mut combiners, items, row)?;
        }
    }
    Ok(vec![finalize_combiners(&combiners)])
}

/// Combine per-owner partials per group key.
fn grouped_aggregate(
    items: &[ProjItem],
    per_owner: Vec<(Vec<String>, Vec<Vec<Value>>)>,
) -> Result<Vec<Vec<Value>>, RouterError> {
    // Group key = the JSON of the grouping columns (in output order).
    let group_col_positions: Vec<usize> = items
        .iter()
        .enumerate()
        .filter(|(_, it)| matches!(it, ProjItem::Grouping { .. }))
        .map(|(i, _)| i)
        .collect();

    // Ordered map so output is deterministic (coordinator may re-sort anyway).
    let mut groups: BTreeMap<String, (Vec<Value>, Vec<AggCombiner>)> = BTreeMap::new();
    for (_cols, rows) in &per_owner {
        for row in rows {
            // The partial row layout is the *expanded* partials; we need the
            // grouping key values, which occupy the leading positions matching
            // the grouping items (owners emit groupings first, then aggregate
            // partials — see build_partial_aggregate_return).
            let key_vals: Vec<Value> = group_col_positions
                .iter()
                .map(|_| Value::Null) // placeholder; filled from partial below
                .collect();
            let _ = key_vals;
            feed_partial_grouped(&mut groups, items, row)?;
        }
    }

    // Emit one output row per group, grouping values then finalized aggregates.
    let mut out = Vec::with_capacity(groups.len());
    for (_key, (group_vals, combiners)) in groups {
        let mut row = Vec::with_capacity(items.len());
        let mut gi = 0;
        let mut ci = 0;
        let finals = combiners.iter().map(|c| c.finalize()).collect::<Vec<_>>();
        for it in items {
            match it {
                ProjItem::Grouping { .. } => {
                    row.push(group_vals.get(gi).cloned().unwrap_or(Value::Null));
                    gi += 1;
                }
                ProjItem::Aggregate { .. } => {
                    row.push(finals.get(ci).cloned().unwrap_or(Value::Null));
                    ci += 1;
                }
            }
        }
        out.push(row);
    }
    Ok(out)
}

// ── Aggregate combiners ─────────────────────────────────────────────────────

/// Running combine state for one aggregate across owners.
#[derive(Clone, Debug)]
enum AggCombiner {
    /// count / sum accumulate a numeric total.
    Sum(f64),
    Count(i64),
    Min(Option<Value>),
    Max(Option<Value>),
    /// avg accumulates (sum, count) and divides at the end.
    Avg {
        sum: f64,
        count: i64,
    },
}

impl AggCombiner {
    fn finalize(&self) -> Value {
        match self {
            AggCombiner::Count(n) => Value::from(*n),
            AggCombiner::Sum(s) => number_value(*s),
            AggCombiner::Min(v) | AggCombiner::Max(v) => v.clone().unwrap_or(Value::Null),
            AggCombiner::Avg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    number_value(sum / (*count as f64))
                }
            }
        }
    }

    /// Feed one raw value directly into the combiner (local aggregation over
    /// join output, not per-owner partials). `val` is `None` for `count(*)`
    /// (counts the row) or when the aggregated cell is absent. Null values are
    /// ignored for sum/avg/min/max (SQL NULL semantics); count(*) still counts.
    fn feed_raw(&mut self, val: Option<&Value>, is_count_star: bool) {
        match self {
            AggCombiner::Count(n) => {
                // count(*) counts every row; count(expr) counts non-null values.
                if is_count_star || val.is_some_and(|v| !v.is_null()) {
                    *n += 1;
                }
            }
            AggCombiner::Sum(s) => {
                if let Some(f) = val.and_then(|v| v.as_f64()) {
                    *s += f;
                }
            }
            AggCombiner::Avg { sum, count } => {
                if let Some(f) = val.and_then(|v| v.as_f64()) {
                    *sum += f;
                    *count += 1;
                }
            }
            AggCombiner::Min(acc) => {
                if let Some(v) = val {
                    if !v.is_null()
                        && (acc.is_none()
                            || cmp_json(v, acc.as_ref().unwrap()) == std::cmp::Ordering::Less)
                    {
                        *acc = Some(v.clone());
                    }
                }
            }
            AggCombiner::Max(acc) => {
                if let Some(v) = val {
                    if !v.is_null()
                        && (acc.is_none()
                            || cmp_json(v, acc.as_ref().unwrap()) == std::cmp::Ordering::Greater)
                    {
                        *acc = Some(v.clone());
                    }
                }
            }
        }
    }
}

fn init_combiners(items: &[ProjItem]) -> Vec<AggCombiner> {
    items
        .iter()
        .filter_map(|it| match it {
            ProjItem::Aggregate { func, .. } => Some(match func {
                AggFunction::Count => AggCombiner::Count(0),
                AggFunction::Sum => AggCombiner::Sum(0.0),
                AggFunction::Min => AggCombiner::Min(None),
                AggFunction::Max => AggCombiner::Max(None),
                AggFunction::Avg => AggCombiner::Avg { sum: 0.0, count: 0 },
                // Unsupported aggregates are rejected in `plan`, so this is
                // unreachable; default to Count(0) defensively.
                _ => AggCombiner::Count(0),
            }),
            _ => None,
        })
        .collect()
}

fn finalize_combiners(combiners: &[AggCombiner]) -> Vec<Value> {
    combiners.iter().map(|c| c.finalize()).collect()
}

/// Feed one owner's partial row (global aggregate case — no grouping) into the
/// combiners. The partial row's layout is the expanded partials produced by
/// `build_partial_aggregate_return`.
fn feed_partial_row(
    combiners: &mut [AggCombiner],
    items: &[ProjItem],
    row: &[Value],
) -> Result<(), RouterError> {
    let mut col = 0; // position in the expanded partial row
    let mut ci = 0; // combiner index
    for it in items {
        if let ProjItem::Aggregate { func, .. } = it {
            combine_one(&mut combiners[ci], func, row, &mut col)?;
            ci += 1;
        }
    }
    Ok(())
}

/// Feed one owner's partial row (grouped case) into the per-group combiners.
fn feed_partial_grouped(
    groups: &mut BTreeMap<String, (Vec<Value>, Vec<AggCombiner>)>,
    items: &[ProjItem],
    row: &[Value],
) -> Result<(), RouterError> {
    // Owners emit grouping columns first (in item order), then expanded
    // aggregate partials. Split the row accordingly.
    let group_count = items
        .iter()
        .filter(|it| matches!(it, ProjItem::Grouping { .. }))
        .count();
    let group_vals: Vec<Value> = row.iter().take(group_count).cloned().collect();
    let key = serde_json::to_string(&group_vals).unwrap_or_default();

    let entry = groups
        .entry(key)
        .or_insert_with(|| (group_vals.clone(), init_combiners(items)));

    let mut col = group_count; // aggregate partials start after grouping cols
    let mut ci = 0;
    for it in items {
        if let ProjItem::Aggregate { func, .. } = it {
            combine_one(&mut entry.1[ci], func, row, &mut col)?;
            ci += 1;
        }
    }
    Ok(())
}

/// Combine one aggregate's partial(s) starting at `row[*col]`, advancing `col`
/// past the partials consumed (avg consumes two: sum then count).
fn combine_one(
    combiner: &mut AggCombiner,
    func: &AggFunction,
    row: &[Value],
    col: &mut usize,
) -> Result<(), RouterError> {
    let take = |c: &mut usize| -> Option<Value> {
        let v = row.get(*c).cloned();
        *c += 1;
        v
    };
    match (combiner, func) {
        (AggCombiner::Count(acc), AggFunction::Count) => {
            *acc += take(col).and_then(|v| v.as_i64()).unwrap_or(0);
        }
        (AggCombiner::Sum(acc), AggFunction::Sum) => {
            *acc += take(col).and_then(|v| v.as_f64()).unwrap_or(0.0);
        }
        (AggCombiner::Min(acc), AggFunction::Min) => {
            if let Some(v) = take(col) {
                if !v.is_null()
                    && (acc.is_none()
                        || cmp_json(&v, acc.as_ref().unwrap()) == std::cmp::Ordering::Less)
                {
                    *acc = Some(v);
                }
            }
        }
        (AggCombiner::Max(acc), AggFunction::Max) => {
            if let Some(v) = take(col) {
                if !v.is_null()
                    && (acc.is_none()
                        || cmp_json(&v, acc.as_ref().unwrap()) == std::cmp::Ordering::Greater)
                {
                    *acc = Some(v);
                }
            }
        }
        (AggCombiner::Avg { sum, count }, AggFunction::Avg) => {
            // avg partial is (sum, count).
            *sum += take(col).and_then(|v| v.as_f64()).unwrap_or(0.0);
            *count += take(col).and_then(|v| v.as_i64()).unwrap_or(0);
        }
        _ => {
            return Err(RouterError::Remote(
                "aggregate combiner/function mismatch".into(),
            ))
        }
    }
    Ok(())
}

/// Build the owner-side RETURN so partials merge. Groupings pass through; each
/// aggregate is expanded to the partials the coordinator combines:
///   count(x) → count(x);  sum(x) → sum(x);  min/max(x) → min/max(x);
///   avg(x)   → sum(x), count(x)   (coordinator divides).
/// Returns the full owner query string `MATCH ... RETURN <partials>`.
pub fn build_partial_aggregate_return(match_str: &str, items: &[ProjItem]) -> Option<String> {
    let mut parts = Vec::new();
    // Groupings first (so the grouped merge can split the row positionally).
    // Owners group by the raw expression (e.g. `n.city`), aliased to the output
    // column so the returned header matches what the coordinator expects.
    for it in items {
        if let ProjItem::Grouping { expr, column } = it {
            if expr == column {
                parts.push(expr.clone());
            } else {
                parts.push(format!("{expr} AS {column}"));
            }
        }
    }
    for it in items {
        if let ProjItem::Aggregate {
            func,
            column,
            distinct,
        } = it
        {
            let d = if *distinct { "DISTINCT " } else { "" };
            let arg = if column == "*" {
                "*".to_string()
            } else {
                column.clone()
            };
            match func {
                AggFunction::Count => parts.push(format!("count({d}{arg})")),
                AggFunction::Sum => parts.push(format!("sum({d}{arg})")),
                AggFunction::Min => parts.push(format!("min({arg})")),
                AggFunction::Max => parts.push(format!("max({arg})")),
                AggFunction::Avg => {
                    // Expand to sum + count so the coordinator can divide.
                    parts.push(format!("sum({d}{arg})"));
                    parts.push(format!("count({d}{arg})"));
                }
                _ => return None, // unsupported aggregate
            }
        }
    }
    Some(format!("{match_str} RETURN {}", parts.join(", ")))
}

// ── JSON comparison ─────────────────────────────────────────────────────────

fn number_value(f: f64) -> Value {
    // Emit an integer when the value is integral (matches single-node output).
    if f.fract() == 0.0 && f.abs() < 9e15 {
        Value::from(f as i64)
    } else {
        serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

fn cmp_json_opt(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(a), Some(b)) => cmp_json(a, b),
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
    }
}

/// Order JSON values: nulls first, then numbers, then strings, then bools.
fn cmp_json(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Number(x), Value::Number(y)) => x
            .as_f64()
            .partial_cmp(&y.as_f64())
            .unwrap_or(Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        // Fall back to stable string encoding for mixed/other types.
        _ => serde_json::to_string(a)
            .unwrap_or_default()
            .cmp(&serde_json::to_string(b).unwrap_or_default()),
    }
}
