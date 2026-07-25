//! Incremental Aggregation — efficient COUNT/SUM/AVG/MIN/MAX updates.
//!
//! Maintains aggregation state and updates incrementally when rows change.
//!
//! MIN/MAX are maintained over a value multiset (a count-keyed ordered map), so
//! removing a value correctly recomputes the extreme in O(log n) rather than
//! silently dropping it — the earlier implementation set min/max to `None` on
//! removal of the extremal value and never recovered.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// A total-order wrapper over `f64` so values can key a `BTreeMap` (f64 is only
/// `PartialOrd`). Uses `total_cmp`, which orders all finite values correctly;
/// NaN sorts to one end but never appears here (only finite numerics are fed in
/// via `extract_numeric_value`).
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrdF64(f64);

impl Eq for OrdF64 {}
impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Aggregation function type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum { column: String },
    Avg { column: String },
    Min { column: String },
    Max { column: String },
}

/// Aggregation state for a single group.
///
/// `count`/`sum` update in O(1). MIN/MAX are derived from `values`, a multiset
/// (value → occurrences) kept in sorted order, so a removal recomputes the
/// extreme correctly in O(log n) instead of being lost.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationState {
    /// Number of rows in this group
    pub count: u64,
    /// Sum of values (for SUM and AVG)
    pub sum: f64,
    /// Value multiset (value → occurrence count) for exact MIN/MAX on removal.
    /// Serialized as pairs since the key is a wrapper type.
    #[serde(with = "value_multiset_serde")]
    values: BTreeMap<OrdF64, u64>,
}

impl AggregationState {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            values: BTreeMap::new(),
        }
    }

    /// Update aggregation state when a row is added
    pub fn add_value(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        *self.values.entry(OrdF64(value)).or_insert(0) += 1;
    }

    /// Update aggregation state when a row is removed. Decrements the value's
    /// multiplicity and drops the key when it hits zero, so MIN/MAX stay exact.
    pub fn remove_value(&mut self, value: f64) {
        if self.count == 0 {
            return;
        }
        self.count -= 1;
        self.sum -= value;
        if let Some(occurrences) = self.values.get_mut(&OrdF64(value)) {
            *occurrences -= 1;
            if *occurrences == 0 {
                self.values.remove(&OrdF64(value));
            }
        }
    }

    /// Add a whole row: always increments `count` (so `COUNT(*)` counts every
    /// row, including those with a null metric), and folds the metric value into
    /// sum/multiset only when present. This is the row-level primitive used by
    /// [`AggregatedMaterializedView::process_update`], which maintains a group's
    /// state ONCE per row rather than once per aggregate (the previous per-agg
    /// loop double-counted when a view had both COUNT and SUM).
    pub fn add_row(&mut self, value: Option<f64>) {
        self.count += 1;
        if let Some(v) = value {
            self.sum += v;
            *self.values.entry(OrdF64(v)).or_insert(0) += 1;
        }
    }

    /// Remove a whole row: mirror of [`add_row`]. Decrements `count` and, when a
    /// metric value is present, subtracts it from sum and the multiset.
    pub fn remove_row(&mut self, value: Option<f64>) {
        if self.count == 0 {
            return;
        }
        self.count -= 1;
        if let Some(v) = value {
            self.sum -= v;
            if let Some(occurrences) = self.values.get_mut(&OrdF64(v)) {
                *occurrences -= 1;
                if *occurrences == 0 {
                    self.values.remove(&OrdF64(v));
                }
            }
        }
    }

    /// Adjust only the metric value (for an in-place UPDATE that keeps the row in
    /// the same group): swap `old` out of and `new` into sum/multiset without
    /// touching `count`.
    pub fn adjust_metric(&mut self, old: Option<f64>, new: Option<f64>) {
        if let Some(v) = old {
            self.sum -= v;
            if let Some(occurrences) = self.values.get_mut(&OrdF64(v)) {
                *occurrences -= 1;
                if *occurrences == 0 {
                    self.values.remove(&OrdF64(v));
                }
            }
        }
        if let Some(v) = new {
            self.sum += v;
            *self.values.entry(OrdF64(v)).or_insert(0) += 1;
        }
    }

    /// Current minimum (recomputed exactly from the multiset).
    pub fn min(&self) -> Option<f64> {
        self.values.keys().next().map(|v| v.0)
    }

    /// Current maximum (recomputed exactly from the multiset).
    pub fn max(&self) -> Option<f64> {
        self.values.keys().next_back().map(|v| v.0)
    }

    /// Get average value
    pub fn avg(&self) -> Option<f64> {
        if self.count > 0 {
            Some(self.sum / self.count as f64)
        } else {
            None
        }
    }
}

/// Serde for the value multiset: emit as a `Vec<(f64, u64)>` so the `OrdF64`
/// key wrapper doesn't need to be a serde map key.
mod value_multiset_serde {
    use super::{BTreeMap, OrdF64};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        values: &BTreeMap<OrdF64, u64>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        let pairs: Vec<(f64, u64)> = values.iter().map(|(k, v)| (k.0, *v)).collect();
        pairs.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<BTreeMap<OrdF64, u64>, D::Error> {
        let pairs = Vec::<(f64, u64)>::deserialize(d)?;
        Ok(pairs.into_iter().map(|(k, v)| (OrdF64(k), v)).collect())
    }
}

impl Default for AggregationState {
    fn default() -> Self {
        Self::new()
    }
}

/// Manager for aggregated materialized views
#[derive(Debug, Clone)]
pub struct AggregationManager {
    /// Group key → aggregation state
    groups: HashMap<String, AggregationState>,
}

impl AggregationManager {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
        }
    }

    /// Update aggregation when a row is inserted/updated
    pub fn upsert(&mut self, group_key: String, value: f64, old_value: Option<f64>) {
        let state = self.groups.entry(group_key).or_default();

        // Remove old value if exists
        if let Some(old) = old_value {
            state.remove_value(old);
        }

        // Add new value
        state.add_value(value);
    }

    /// Update aggregation when a row is deleted
    pub fn delete(&mut self, group_key: &str, value: f64) {
        if let Some(state) = self.groups.get_mut(group_key) {
            state.remove_value(value);

            // Remove empty groups
            if state.count == 0 {
                self.groups.remove(group_key);
            }
        }
    }

    /// Get aggregation result for a group
    pub fn get(&self, group_key: &str) -> Option<&AggregationState> {
        self.groups.get(group_key)
    }

    /// Get all groups
    pub fn all_groups(&self) -> Vec<(String, AggregationState)> {
        self.groups
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl Default for AggregationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Example: Aggregated materialized view definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedMaterializedView {
    pub id: String,
    pub name: String,
    /// Source query
    pub source_query: String,
    /// GROUP BY columns
    pub group_by: Vec<String>,
    /// Aggregation functions
    pub aggregates: Vec<AggregateFunction>,
    /// Aggregation state
    #[serde(skip)]
    pub manager: AggregationManager,
}

impl AggregatedMaterializedView {
    pub fn new(
        id: String,
        name: String,
        source_query: String,
        group_by: Vec<String>,
        aggregates: Vec<AggregateFunction>,
    ) -> Self {
        Self {
            id,
            name,
            source_query,
            group_by,
            aggregates,
            manager: AggregationManager::new(),
        }
    }

    /// Aggregation state for one group key, or `None` if the group is empty /
    /// absent. The key format matches [`compute_group_key`] (`{:?}`-rendered
    /// group-by values joined by `|`).
    pub fn group_state(&self, group_key: &str) -> Option<&AggregationState> {
        self.manager.get(group_key)
    }

    /// Total row count across all groups — the whole-view `COUNT(*)`.
    pub fn manager_all_count(&self) -> u64 {
        self.manager.all_groups().iter().map(|(_, s)| s.count).sum()
    }

    /// The single metric column this view aggregates over (SUM/AVG/MIN/MAX all
    /// share one column in this slice; COUNT needs none). `None` for a COUNT-only
    /// view. Returns the first metric column found — mixing different columns in
    /// one view isn't supported here (the state holds one sum/multiset).
    fn metric_column(&self) -> Option<&str> {
        self.aggregates.iter().find_map(|agg| match agg {
            AggregateFunction::Sum { column }
            | AggregateFunction::Avg { column }
            | AggregateFunction::Min { column }
            | AggregateFunction::Max { column } => Some(column.as_str()),
            AggregateFunction::Count => None,
        })
    }

    /// Process a row update (insert/update/delete).
    ///
    /// Maintains the group's [`AggregationState`] ONCE per row (not once per
    /// aggregate): `count` tracks rows, and the metric column — shared by
    /// SUM/AVG/MIN/MAX — folds into sum + the MIN/MAX multiset. A prior version
    /// looped per aggregate and bumped `count` inside both the Count and Sum
    /// arms, double-counting any view that had both; MIN/MAX were a silent no-op.
    pub fn process_update(
        &mut self,
        row: &HashMap<String, nexora_id::PropertyValue>,
        operation: RowOperation,
    ) {
        let group_key = self.compute_group_key(row);
        let metric_col = self.metric_column().map(|s| s.to_string());
        let value = metric_col
            .as_ref()
            .and_then(|col| extract_numeric_value(row.get(col)));

        match &operation {
            RowOperation::Insert => {
                let state = self.manager.groups.entry(group_key).or_default();
                state.add_row(value);
            }
            RowOperation::Delete => {
                if let Some(state) = self.manager.groups.get_mut(&group_key) {
                    state.remove_row(value);
                    if state.count == 0 {
                        self.manager.groups.remove(&group_key);
                    }
                }
            }
            RowOperation::Update { old_row } => {
                // The group key is computed from the NEW row. If the update moved
                // the row across group keys, treat it as delete-from-old +
                // insert-into-new; otherwise adjust the metric in place.
                let old_group_key = self.compute_group_key(old_row);
                let old_value = metric_col
                    .as_ref()
                    .and_then(|col| extract_numeric_value(old_row.get(col)));
                if old_group_key == group_key {
                    let state = self.manager.groups.entry(group_key).or_default();
                    state.adjust_metric(old_value, value);
                } else {
                    if let Some(state) = self.manager.groups.get_mut(&old_group_key) {
                        state.remove_row(old_value);
                        if state.count == 0 {
                            self.manager.groups.remove(&old_group_key);
                        }
                    }
                    let state = self.manager.groups.entry(group_key).or_default();
                    state.add_row(value);
                }
            }
        }
    }

    fn compute_group_key(&self, row: &HashMap<String, nexora_id::PropertyValue>) -> String {
        self.group_by
            .iter()
            .filter_map(|col| row.get(col).map(|v| format!("{:?}", v)))
            .collect::<Vec<_>>()
            .join("|")
    }
}

/// Row operation type
#[derive(Debug, Clone)]
pub enum RowOperation {
    Insert,
    Update {
        old_row: HashMap<String, nexora_id::PropertyValue>,
    },
    Delete,
}

/// Extract numeric value from PropertyValue
fn extract_numeric_value(val: Option<&nexora_id::PropertyValue>) -> Option<f64> {
    match val? {
        nexora_id::PropertyValue::Integer(i) => Some(*i as f64),
        nexora_id::PropertyValue::Float(f) => Some(*f),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nexora_id::PropertyValue;

    #[test]
    fn test_aggregation_state() {
        let mut state = AggregationState::new();

        state.add_value(10.0);
        state.add_value(20.0);
        state.add_value(30.0);

        assert_eq!(state.count, 3);
        assert_eq!(state.sum, 60.0);
        assert_eq!(state.avg(), Some(20.0));
        assert_eq!(state.min(), Some(10.0));
        assert_eq!(state.max(), Some(30.0));

        state.remove_value(20.0);
        assert_eq!(state.count, 2);
        assert_eq!(state.sum, 40.0);
        assert_eq!(state.avg(), Some(20.0));
    }

    #[test]
    fn test_aggregation_manager() {
        let mut mgr = AggregationManager::new();

        mgr.upsert("group1".to_string(), 10.0, None);
        mgr.upsert("group1".to_string(), 20.0, None);
        mgr.upsert("group2".to_string(), 30.0, None);

        let group1 = mgr.get("group1").unwrap();
        assert_eq!(group1.count, 2);
        assert_eq!(group1.sum, 30.0);

        let group2 = mgr.get("group2").unwrap();
        assert_eq!(group2.count, 1);
        assert_eq!(group2.sum, 30.0);
    }

    /// Regression: removing the current MIN/MAX must recompute the next extreme
    /// from the remaining values, not drop it to `None`. The prior implementation
    /// set min/max to `None` on removal of the extremal value and never recovered.
    #[test]
    fn min_max_recompute_correctly_after_removing_the_extreme() {
        let mut state = AggregationState::new();
        for v in [10.0, 20.0, 30.0] {
            state.add_value(v);
        }
        assert_eq!(state.min(), Some(10.0));
        assert_eq!(state.max(), Some(30.0));

        // Remove the current max (30) → max must fall back to 20, not vanish.
        state.remove_value(30.0);
        assert_eq!(
            state.max(),
            Some(20.0),
            "max must recompute to 20 after removing 30"
        );
        assert_eq!(state.min(), Some(10.0));

        // Remove the current min (10) → min must rise to 20.
        state.remove_value(10.0);
        assert_eq!(
            state.min(),
            Some(20.0),
            "min must recompute to 20 after removing 10"
        );
        assert_eq!(state.max(), Some(20.0));
    }

    /// Duplicate values must be tracked as a multiset: removing one occurrence of
    /// the extreme keeps it while another copy remains.
    #[test]
    fn min_max_handle_duplicate_extremes() {
        let mut state = AggregationState::new();
        for v in [5.0, 5.0, 9.0] {
            state.add_value(v);
        }
        assert_eq!(state.min(), Some(5.0));
        state.remove_value(5.0);
        assert_eq!(state.min(), Some(5.0), "one 5 remains, so min stays 5");
        state.remove_value(5.0);
        assert_eq!(state.min(), Some(9.0), "both 5s gone, min rises to 9");
    }

    fn row(pairs: &[(&str, PropertyValue)]) -> HashMap<String, PropertyValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    /// Regression: a view with BOTH COUNT and SUM must maintain `count` once per
    /// row, not once per aggregate. The prior per-aggregate loop bumped `count` in
    /// both the Count and Sum arms, double-counting.
    #[test]
    fn process_update_count_and_sum_do_not_double_count() {
        let mut view = AggregatedMaterializedView::new(
            "v1".into(),
            "by_city".into(),
            "…".into(),
            vec!["city".into()],
            vec![
                AggregateFunction::Count,
                AggregateFunction::Sum {
                    column: "age".into(),
                },
            ],
        );

        for (city, age) in [("NYC", 30), ("NYC", 40), ("LA", 25)] {
            view.process_update(
                &row(&[
                    ("city", PropertyValue::String(city.into())),
                    ("age", PropertyValue::Integer(age)),
                ]),
                RowOperation::Insert,
            );
        }

        let nyc = view
            .manager
            .get(&format!("{:?}", PropertyValue::String("NYC".into())))
            .unwrap();
        assert_eq!(
            nyc.count, 2,
            "COUNT must be 2 for NYC, not 4 (no double-count)"
        );
        assert_eq!(nyc.sum, 70.0, "SUM(age) for NYC is 30+40");
        let la = view
            .manager
            .get(&format!("{:?}", PropertyValue::String("LA".into())))
            .unwrap();
        assert_eq!(la.count, 1);
        assert_eq!(la.sum, 25.0);
    }

    /// A COUNT(*) view must count rows even when the metric column is null/absent,
    /// and MIN/MAX over a group must survive deletes.
    #[test]
    fn process_update_delete_and_min_max_over_group() {
        let mut view = AggregatedMaterializedView::new(
            "v2".into(),
            "temp_by_room".into(),
            "…".into(),
            vec!["room".into()],
            vec![
                AggregateFunction::Count,
                AggregateFunction::Min {
                    column: "temp".into(),
                },
                AggregateFunction::Max {
                    column: "temp".into(),
                },
            ],
        );

        let mk = |temp: i64| {
            row(&[
                ("room", PropertyValue::String("r1".into())),
                ("temp", PropertyValue::Integer(temp)),
            ])
        };
        for t in [18, 22, 25] {
            view.process_update(&mk(t), RowOperation::Insert);
        }
        let key = format!("{:?}", PropertyValue::String("r1".into()));
        let g = view.manager.get(&key).unwrap();
        assert_eq!(g.count, 3);
        assert_eq!(g.min(), Some(18.0));
        assert_eq!(g.max(), Some(25.0));

        // Delete the max (25) → max must recompute to 22.
        view.process_update(&mk(25), RowOperation::Delete);
        let g = view.manager.get(&key).unwrap();
        assert_eq!(g.count, 2);
        assert_eq!(
            g.max(),
            Some(22.0),
            "max recomputes to 22 after deleting 25"
        );
        assert_eq!(g.min(), Some(18.0));
    }
}
