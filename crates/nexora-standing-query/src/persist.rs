//! Standing Query persistence — save and restore SQ definitions and match states.
//!
//! A1: SQ state persists through the unified [`ControlPlaneStore`] (a single
//! well-known key in [`Namespace::SqState`]) instead of the graph persistence
//! layer. All control-plane metadata (shard map, MV defs, SQ defs) thus shares
//! one durable backend, one fsync policy, and one restore path — and A2 gets a
//! single apply target for consensus.

use crate::StandingQuery;
use nexora_core::control_plane_store::{ControlPlaneStore, Namespace};
use nexora_id::NexoraId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Serialized SQ state for persistence.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistedSqState {
    /// Version for schema evolution.
    pub version: u32,
    /// All registered standing queries.
    pub queries: Vec<PersistedQuery>,
    /// Match states: (sq_id_hex, qid_hex) -> match_info.
    pub match_states: HashMap<String, PersistedMatchState>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistedQuery {
    pub id: String, // UUID hex
    pub name: String,
    pub pattern_json: String, // JSON serialized StandingQueryPattern
    pub created_at: String,
    /// Rule version (defaults to 1 for states persisted before this field existed).
    #[serde(default = "default_version")]
    pub version: u64,
    /// Rule metadata (domain, tags, author, …). Empty for pre-existing states.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

fn default_version() -> u64 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistedMatchState {
    pub is_matching: bool,
    pub matched_properties_json: String, // JSON
    pub last_checked: String,
}

const SQ_STATE_VERSION: u32 = 1;
/// Well-known key under which the whole SQ state blob is stored in
/// [`Namespace::SqState`]. There is a single SQ state per node, so one key.
const SQ_STATE_KEY: &str = "state";

/// Serialize the current SQ manager state to a `PersistedSqState`.
pub fn serialize_state(
    queries: &[StandingQuery],
    match_states: &HashMap<(Uuid, NexoraId), super::MatchState>,
) -> PersistedSqState {
    let queries = queries
        .iter()
        .map(|sq| PersistedQuery {
            id: sq.id.to_string(),
            name: sq.name.clone(),
            pattern_json: serde_json::to_string(&sq.pattern).unwrap_or_default(),
            created_at: sq.created_at.to_rfc3339(),
            version: sq.version,
            metadata: sq.metadata.clone(),
        })
        .collect();

    let match_states = match_states
        .iter()
        .map(|((sq_id, qid), ms)| {
            let key = format!("{}|{}", sq_id, qid.to_hex());
            let matched_props = serde_json::to_string(&ms.matched_properties).unwrap_or_default();
            (
                key,
                PersistedMatchState {
                    is_matching: ms.is_matching,
                    matched_properties_json: matched_props,
                    last_checked: ms.last_checked.to_rfc3339(),
                },
            )
        })
        .collect();

    PersistedSqState {
        version: SQ_STATE_VERSION,
        queries,
        match_states,
    }
}

/// Persist the SQ state through the unified control-plane store.
///
/// A1: SQ state was previously written as a `PropertySet` event under a
/// well-known NexoraId in the graph persistence layer. It now shares the same
/// [`ControlPlaneStore`] backend as the shard map and MV definitions — one
/// durable store, one fsync policy, one restore path. The whole state is a
/// single JSON blob under a fixed key in [`Namespace::SqState`].
pub fn persist_state(
    store: &dyn ControlPlaneStore,
    queries: &[StandingQuery],
    match_states: &HashMap<(Uuid, NexoraId), super::MatchState>,
) -> Result<(), String> {
    let state = serialize_state(queries, match_states);
    let json = serde_json::to_vec(&state).map_err(|e| format!("serialization error: {e}"))?;
    store
        .put(Namespace::SqState, SQ_STATE_KEY, &json)
        .map_err(|e| format!("persistence error: {e}"))
}

/// Restored SQ state: the persisted queries plus their per-`(query, node)`
/// match states. `None` at the call site means no state was persisted yet.
pub type RestoredSqState = (
    Vec<StandingQuery>,
    HashMap<(Uuid, NexoraId), super::MatchState>,
);

/// Restore SQ state from the control-plane store.
/// Returns (queries, match_states) if state was found, or None if no persisted state exists.
pub fn restore_state(store: &dyn ControlPlaneStore) -> Result<Option<RestoredSqState>, String> {
    let json_bytes = match store
        .get(Namespace::SqState, SQ_STATE_KEY)
        .map_err(|e| format!("persistence error: {e}"))?
    {
        Some(b) => b,
        None => return Ok(None),
    };

    let state: PersistedSqState =
        serde_json::from_slice(&json_bytes).map_err(|e| format!("deserialization error: {e}"))?;

    if state.version != SQ_STATE_VERSION {
        return Err(format!(
            "SQ state version mismatch: expected {}, got {}",
            SQ_STATE_VERSION, state.version
        ));
    }

    let mut queries = Vec::new();
    for pq in &state.queries {
        let id = Uuid::parse_str(&pq.id).map_err(|e| format!("invalid UUID: {e}"))?;
        let pattern: crate::pattern::StandingQueryPattern = serde_json::from_str(&pq.pattern_json)
            .map_err(|e| format!("pattern deserialization error: {e}"))?;
        let created_at = chrono::DateTime::parse_from_rfc3339(&pq.created_at)
            .map_err(|e| format!("invalid date: {e}"))?
            .with_timezone(&chrono::Utc);

        queries.push(StandingQuery {
            id,
            name: pq.name.clone(),
            pattern,
            created_at,
            version: pq.version,
            metadata: pq.metadata.clone(),
        });
    }

    let mut match_states = HashMap::new();
    for (key, pms) in &state.match_states {
        let parts: Vec<&str> = key.splitn(2, '|').collect();
        if parts.len() != 2 {
            continue;
        }
        let sq_id = Uuid::parse_str(parts[0]).map_err(|_| "invalid sq UUID".to_string())?;
        let qid = NexoraId::from_hex(parts[1]).map_err(|_| "invalid qid hex".to_string())?;

        let matched_properties: HashMap<String, nexora_id::PropertyValue> =
            serde_json::from_str(&pms.matched_properties_json).unwrap_or_default();

        let last_checked = chrono::DateTime::parse_from_rfc3339(&pms.last_checked)
            .map_err(|e| format!("invalid date: {e}"))?
            .with_timezone(&chrono::Utc);

        match_states.insert(
            (sq_id, qid),
            super::MatchState {
                is_matching: pms.is_matching,
                matched_properties,
                last_checked,
            },
        );
    }

    tracing::info!(
        "Restored {} SQs and {} match states from persistence",
        queries.len(),
        match_states.len()
    );

    Ok(Some((queries, match_states)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pattern::{FilterCondition, StandingQueryPattern};
    use nexora_core::control_plane_store::InMemoryControlPlaneStore;

    fn make_store() -> InMemoryControlPlaneStore {
        InMemoryControlPlaneStore::new()
    }

    fn make_sq(name: &str) -> StandingQuery {
        StandingQuery {
            id: Uuid::new_v4(),
            name: name.to_string(),
            pattern: StandingQueryPattern::property("speed", FilterCondition::GreaterThan(100.0)),
            created_at: chrono::Utc::now(),
            version: 1,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn test_persist_and_restore() {
        let store = make_store();

        let sq1 = make_sq("sq1");
        let sq2 = make_sq("sq2");
        let queries = vec![sq1.clone(), sq2.clone()];

        let mut match_states = HashMap::new();
        let qid = NexoraId::from_bytes(b"node-1".to_vec());
        match_states.insert(
            (sq1.id, qid.clone()),
            crate::MatchState {
                is_matching: true,
                matched_properties: {
                    let mut m = HashMap::new();
                    m.insert("speed".to_string(), nexora_id::PropertyValue::Integer(150));
                    m
                },
                last_checked: chrono::Utc::now(),
            },
        );

        // Persist
        persist_state(&store, &queries, &match_states).unwrap();

        // Restore
        let restored = restore_state(&store).unwrap();
        assert!(restored.is_some());
        let (restored_queries, restored_states) = restored.unwrap();

        assert_eq!(restored_queries.len(), 2);
        assert_eq!(restored_states.len(), 1);
        // queries come back keyed by id (unordered); assert by set membership.
        let names: std::collections::HashSet<_> =
            restored_queries.iter().map(|q| q.name.as_str()).collect();
        assert!(names.contains("sq1"));
        assert!(names.contains("sq2"));
    }

    #[test]
    fn test_restore_empty() {
        let store = make_store();
        let result = restore_state(&store).unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_serialize_roundtrip() {
        let sq = make_sq("test-sq");
        let queries = vec![sq.clone()];

        let mut match_states = HashMap::new();
        let qid = NexoraId::from_bytes(b"test-node".to_vec());
        match_states.insert(
            (sq.id, qid),
            crate::MatchState {
                is_matching: true,
                matched_properties: HashMap::new(),
                last_checked: chrono::Utc::now(),
            },
        );

        let state = serialize_state(&queries, &match_states);

        // Serialize to JSON and back
        let json = serde_json::to_string(&state).unwrap();
        let restored: PersistedSqState = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.version, 1);
        assert_eq!(restored.queries.len(), 1);
        assert_eq!(restored.match_states.len(), 1);
    }
}
