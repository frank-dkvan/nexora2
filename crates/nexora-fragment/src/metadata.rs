//! Fragment metadata: pre-computed statistics for query optimization.
//!
//! Each fragment carries [`FragmentMetadata`] which includes counts, value
//! ranges, and per-property statistics. These statistics allow the query
//! planner to skip fragments that cannot match a given predicate (bloom-filter
//! style), avoiding expensive full scans.
//!
//! The key optimization method is [`FragmentMetadata::might_match_property`],
//! which checks whether a fragment _could_ contain data matching a
//! `PropertyCondition` given its min/max statistics.

use nexora_id::{NexoraId, PropertyValue};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Pre-computed metadata for a single fragment.
///
/// Includes node/edge counts, time range, per-property min/max/null
/// statistics, and a checksum for integrity verification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FragmentMetadata {
    /// The unique identifier for this fragment.
    pub id: super::FragmentId,
    /// Total number of nodes stored in this fragment.
    pub node_count: u64,
    /// Total number of edges stored in this fragment.
    pub edge_count: u64,
    /// Optional range of node IDs (minimum, maximum) for range filtering.
    pub id_range: Option<(Vec<u8>, Vec<u8>)>,
    /// The time window this fragment covers, as (start_us, end_us).
    pub time_range: (u64, u64),
    /// Per-property statistics keyed by property name.
    pub property_stats: BTreeMap<String, PropertyStats>,
    /// Logical namespace this fragment belongs to.
    pub namespace: String,
    /// Schema version for forward/backward compatibility.
    pub schema_version: u32,
    /// Whether this fragment has been consolidated (merged from smaller fragments).
    pub is_consolidated: bool,
    /// CRC32 checksum of the fragment data for integrity verification.
    pub checksum: u32,
}

/// Statistics for a single property across all nodes in a fragment.
///
/// Used by the query optimizer to determine whether a fragment can
/// satisfy a given filter condition without reading the actual data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PropertyStats {
    /// Number of non-null values for this property.
    pub non_null_count: u64,
    /// Number of null values for this property.
    pub null_count: u64,
    /// Minimum observed value (across all nodes), if any.
    pub min_value: Option<PropertyValue>,
    /// Maximum observed value (across all nodes), if any.
    pub max_value: Option<PropertyValue>,
    /// Sum of numeric values, for computing averages without full scans.
    pub sum_value: Option<f64>,
    /// Estimated number of distinct values (HyperLogLog-like cardinality estimate).
    pub distinct_estimate: Option<u64>,
}

impl FragmentMetadata {
    /// Create a new `FragmentMetadata` with zero counts and empty statistics.
    ///
    /// The time range is derived from the fragment ID's `start_us` and `end_us`.
    ///
    /// # Arguments
    ///
    /// * `id` - The fragment's unique identifier (encodes time window and UUID).
    /// * `namespace` - The logical namespace this fragment belongs to.
    pub fn new(id: super::FragmentId, namespace: String) -> Self {
        let start = id.start_us;
        let end = id.end_us;
        Self {
            id,
            node_count: 0,
            edge_count: 0,
            id_range: None,
            time_range: (start, end),
            property_stats: BTreeMap::new(),
            namespace,
            schema_version: 1,
            is_consolidated: false,
            checksum: 0,
        }
    }

    /// Check whether this fragment might contain a given node ID.
    ///
    /// This is a fast pre-filter: if it returns `false`, the fragment can be
    /// safely skipped; if `true`, a full scan is needed. Currently always
    /// returns `true` (conservative default).
    ///
    /// # Arguments
    ///
    /// * `_qid` - The node ID to check.
    pub fn might_contain(&self, _qid: &NexoraId) -> bool {
        true
    }

    /// Check whether this fragment might contain data matching a property condition.
    ///
    /// Uses the fragment's [`PropertyStats`] to determine if any node
    /// _could_ satisfy the given condition. Returns `false` only when the
    /// statistics prove the condition is impossible (e.g., `GreaterThan(100)`
    /// when `max_value` is 50).
    ///
    /// # Arguments
    ///
    /// * `key` - The property name to check.
    /// * `condition` - The filter condition to evaluate against statistics.
    ///
    /// # Returns
    ///
    /// `true` if the fragment might match, `false` if it can be safely skipped.
    pub fn might_match_property(&self, key: &str, condition: &PropertyCondition) -> bool {
        let stats = match self.property_stats.get(key) {
            Some(s) => s,
            None => return false,
        };
        match condition {
            PropertyCondition::GreaterThan(threshold) => stats
                .max_value
                .as_ref()
                .is_none_or(|max| max.as_f64().is_none_or(|v| v > *threshold)),
            PropertyCondition::LessThan(threshold) => stats
                .min_value
                .as_ref()
                .is_none_or(|min| min.as_f64().is_none_or(|v| v < *threshold)),
            PropertyCondition::Equals(_) => true,
            PropertyCondition::Exists => stats.non_null_count > 0,
            PropertyCondition::IsNull => stats.null_count > 0,
        }
    }
}

/// A filter condition on a property, used for fragment-level pre-filtering.
///
/// These conditions mirror common query predicates and are evaluated against
/// [`PropertyStats`] to prune fragments that cannot possibly match.
#[derive(Clone, Debug)]
pub enum PropertyCondition {
    /// Property value > threshold.
    GreaterThan(f64),
    /// Property value < threshold.
    LessThan(f64),
    /// Property value exactly equals the given value.
    Equals(PropertyValue),
    /// Property exists (is not null).
    Exists,
    /// Property is null.
    IsNull,
}
