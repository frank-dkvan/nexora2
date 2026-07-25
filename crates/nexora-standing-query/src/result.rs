//! Standing Query result types with P1.3 Explain support.

use crate::ResultType;
use nexora_id::{NexoraId, PropertyValue};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// P1.3: Detailed explain payload for a Standing Query hit.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StandingQueryExplain {
    /// Which rule/query version was hit.
    pub sq_id: Uuid,
    pub sq_name: String,
    pub sq_version: u64,

    /// Which objects participated.
    pub qid: NexoraId,

    /// Which properties satisfied the conditions.
    pub matching_properties: Vec<String>,

    /// Which labels contributed to the match.
    pub matching_labels: Vec<String>,

    /// Which edge types were traversed (for edge patterns).
    pub matching_edge_types: Vec<String>,

    /// Whether this is a new match or a recovery (unmatch).
    pub result_type: String, // "matched" | "unmatched" | "recovered"

    /// Unique hit ID for correlation/tracing.
    pub hit_id: String,

    /// When the hit was generated.
    pub hit_at: String, // RFC3339

    /// Source that triggered this evaluation.
    pub trigger_source: String, // "property_change" | "edge_added" | "label_added" | etc.

    /// Whether this result was suppressed (e.g., dedup window).
    pub suppressed: bool,
}

/// A Standing Query result — emitted when a match or unmatch occurs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StandingQueryResult {
    /// The Standing Query ID.
    pub sq_id: Uuid,
    /// The Standing Query name.
    pub sq_name: String,
    /// The node that matched/unmatched.
    pub qid: NexoraId,
    /// The matched properties (empty for Unmatch).
    pub matched_properties: HashMap<String, PropertyValue>,
    /// Whether this is a match or unmatch.
    pub result_type: ResultType,
    /// When the result was generated.
    pub timestamp: chrono::DateTime<chrono::Utc>,

    // ====== P1.3: Explain support ======
    /// Version of the rule that generated this result.
    pub sq_version: u64,
    /// Unique hit ID for tracing/correlation.
    pub hit_id: String,
    /// What triggered this evaluation.
    pub trigger_source: String,
    /// Which edge types were matched (for edge patterns).
    pub matching_edge_types: Vec<String>,
    /// Whether this result was suppressed.
    pub suppressed: bool,
}

impl StandingQueryResult {
    /// Create a minimal StandingQueryResult (old constructor, preserves backward compat).
    /// New fields get sensible defaults: version=1, auto-generated UUID for hit_id, etc.
    pub fn new(
        sq_id: Uuid,
        sq_name: impl Into<String>,
        qid: NexoraId,
        matched_properties: HashMap<String, PropertyValue>,
        result_type: ResultType,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            sq_id,
            sq_name: sq_name.into(),
            qid,
            matched_properties,
            result_type,
            timestamp,
            sq_version: 1,
            hit_id: Uuid::new_v4().to_string(),
            trigger_source: "unknown".to_string(),
            matching_edge_types: Vec::new(),
            suppressed: false,
        }
    }

    /// P1.3: Build an explain payload from this result.
    pub fn explain(&self) -> StandingQueryExplain {
        StandingQueryExplain {
            sq_id: self.sq_id,
            sq_name: self.sq_name.clone(),
            sq_version: self.sq_version,
            qid: self.qid.clone(),
            matching_properties: self.matched_properties.keys().cloned().collect(),
            matching_labels: Vec::new(), // populated by the SQ manager
            matching_edge_types: self.matching_edge_types.clone(),
            result_type: match self.result_type {
                ResultType::Matched => "matched".to_string(),
                ResultType::Unmatched => "unmatched".to_string(),
            },
            hit_id: self.hit_id.clone(),
            hit_at: self.timestamp.to_rfc3339(),
            trigger_source: self.trigger_source.clone(),
            suppressed: self.suppressed,
        }
    }

    /// Convert to JSON for API responses.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "sq_id": self.sq_id.to_string(),
            "sq_name": self.sq_name,
            "qid": self.qid.to_hex(),
            "result_type": match self.result_type {
                ResultType::Matched => "matched",
                ResultType::Unmatched => "unmatched",
            },
            "properties": self.matched_properties.iter().map(|(k, v)| {
                (k.clone(), pv_to_json(v))
            }).collect::<HashMap<String, serde_json::Value>>(),
            "timestamp": self.timestamp.to_rfc3339(),
            "sq_version": self.sq_version,
            "hit_id": self.hit_id,
            "trigger_source": self.trigger_source,
            "matching_edge_types": self.matching_edge_types,
            "suppressed": self.suppressed,
        })
    }
}

fn pv_to_json(v: &PropertyValue) -> serde_json::Value {
    match v {
        PropertyValue::Null => serde_json::Value::Null,
        PropertyValue::Boolean(b) => serde_json::Value::Bool(*b),
        PropertyValue::Integer(i) => serde_json::json!(i),
        PropertyValue::Float(f) => serde_json::json!(f),
        PropertyValue::String(s) => serde_json::Value::String(s.clone()),
        PropertyValue::Bytes(b) => serde_json::json!(b),
        PropertyValue::List(items) => {
            serde_json::Value::Array(items.iter().map(pv_to_json).collect())
        }
        PropertyValue::Map(m) => {
            let map: serde_json::Map<String, serde_json::Value> =
                m.iter().map(|(k, v)| (k.clone(), pv_to_json(v))).collect();
            serde_json::Value::Object(map)
        }
        other => serde_json::Value::String(format!("{other}")),
    }
}
