//! Node event types for the event sourcing model.
//!
//! In Nexora, all graph mutations are represented as timestamped events.
//! Events are:
//! 1. Written to the local WAL for crash recovery
//! 2. Persisted to the storage backend (RocksDB/S3/Iceberg)
//! 3. Published via Zenoh for cross-node propagation
//! 4. Used by Standing Queries for incremental pattern matching
//!
//! There are two categories:
//! - `NodeChangeEvent`: Affects local node state (properties, edges)
//! - `DomainIndexEvent`: Affects Standing Query subscription tracking

use crate::graph::node_task::TombstoneRecord;
use nexora_id::{EventTime, NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use serde::{Deserialize, Serialize};

/// A timestamped node event — the fundamental unit of change in the graph.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimedEvent<E> {
    pub event: E,
    pub time: EventTime,
}

impl<E> TimedEvent<E> {
    pub fn new(event: E, time: EventTime) -> Self {
        Self { event, time }
    }
}

/// Unified node event — either a local state change or a domain index change.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NodeEvent {
    Change(NodeChangeEvent),
    DomainIndex(DomainIndexEvent),
}

impl NodeEvent {
    pub fn as_change(&self) -> Option<&NodeChangeEvent> {
        match self {
            Self::Change(e) => Some(e),
            _ => None,
        }
    }

    pub fn as_domain_index(&self) -> Option<&DomainIndexEvent> {
        match self {
            Self::DomainIndex(e) => Some(e),
            _ => None,
        }
    }
}

/// Events that mutate a node's local state (properties and edges).
///
/// These correspond to the `NodeChangeEvent` sealed trait.
///
/// P0.1 Task 1.2: Extended with Label, EdgeProperty, and Tombstone events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NodeChangeEvent {
    // ====== Property operations ======
    /// Set a property on this node.
    PropertySet { key: Symbol, value: PropertyValue },
    /// Remove a property from this node.
    PropertyRemoved {
        key: Symbol,
        previous_value: PropertyValue,
    },

    // ====== P0.1 Step 1: Label operations ======
    /// Add a label to this node (first-class, not synthetic property).
    /// Triggers label index update and Standing Query re-evaluation.
    LabelAdded { label: Symbol },

    /// Remove a label from this node.
    /// Triggers label index update and Standing Query re-evaluation.
    LabelRemoved { label: Symbol },

    // ====== Edge operations ======
    /// Add a half-edge to this node's edge collection.
    EdgeAdded { edge: HalfEdge },
    /// Remove a half-edge from this node's edge collection.
    EdgeRemoved { edge: HalfEdge },

    // ====== P0.1 Step 2: Edge property operations ======
    /// Set a property on an edge (first-class, not synthetic property).
    /// The edge is identified by (src, edge_type, dst) tuple.
    EdgePropertySet {
        edge_type: Symbol,
        target: NexoraId,
        key: Symbol,
        value: PropertyValue,
    },

    /// Remove a property from an edge.
    EdgePropertyRemoved {
        edge_type: Symbol,
        target: NexoraId,
        key: Symbol,
    },

    // ====== P0.1 Step 3: Node lifecycle (soft delete) ======
    /// Node soft-deleted (tombstone created).
    /// The node is logically deleted but physically retained for audit/recovery.
    NodeDeleted { tombstone: TombstoneRecord },

    /// Node restored from soft-delete (tombstone removed).
    NodeRestored,
}

impl NodeChangeEvent {
    /// The property key affected, if this event touches a property.
    pub fn property_key(&self) -> Option<&Symbol> {
        match self {
            Self::PropertySet { key, .. } | Self::PropertyRemoved { key, .. } => Some(key),
            _ => None,
        }
    }

    /// Whether this event is a property mutation (set or remove).
    pub fn is_property_event(&self) -> bool {
        matches!(
            self,
            Self::PropertySet { .. } | Self::PropertyRemoved { .. }
        )
    }

    /// Whether this event is an edge mutation (add or remove).
    pub fn is_edge_event(&self) -> bool {
        matches!(self, Self::EdgeAdded { .. } | Self::EdgeRemoved { .. })
    }

    /// P0.1: Whether this event is a label mutation (add or remove).
    pub fn is_label_event(&self) -> bool {
        matches!(self, Self::LabelAdded { .. } | Self::LabelRemoved { .. })
    }

    /// P0.1: The label affected, if this event touches a label.
    pub fn label(&self) -> Option<&Symbol> {
        match self {
            Self::LabelAdded { label } | Self::LabelRemoved { label } => Some(label),
            _ => None,
        }
    }

    /// P0.1 Step 2: Whether this event is an edge property mutation.
    pub fn is_edge_property_event(&self) -> bool {
        matches!(
            self,
            Self::EdgePropertySet { .. } | Self::EdgePropertyRemoved { .. }
        )
    }

    /// P0.1 Step 3: Whether this event is a node lifecycle event (delete/restore).
    pub fn is_lifecycle_event(&self) -> bool {
        matches!(self, Self::NodeDeleted { .. } | Self::NodeRestored)
    }

    /// P0.1 Step 3: Whether this event marks a node as deleted.
    pub fn is_deleted(&self) -> bool {
        matches!(self, Self::NodeDeleted { .. })
    }

    /// P0.1 Step 3: Whether this event restores a deleted node.
    pub fn is_restored(&self) -> bool {
        matches!(self, Self::NodeRestored)
    }
}

/// Events that manage Standing Query subscription state on this node.
///
/// These correspond to the `DomainIndexEvent` sealed trait.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DomainIndexEvent {
    /// Subscribe a DomainGraphNode to this node's changes.
    CreateDomainNodeSubscription {
        subscriber_nexora_id: NexoraId,
        dgn_id: u64,
        query_id: uuid::Uuid,
    },
    /// Unsubscribe a DomainGraphNode from this node's changes.
    CancelDomainNodeSubscription {
        subscriber_nexora_id: NexoraId,
        dgn_id: u64,
    },
    /// A Standing Query subscription result notification.
    DomainNodeSubscriptionResult {
        subscriber_nexora_id: NexoraId,
        dgn_id: u64,
        result: bool,
    },
    /// Create a Standing Query subscription.
    CreateDomainStandingQuerySubscription {
        subscriber_nexora_id: NexoraId,
        dgn_id: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_property_set_event() {
        let event = NodeChangeEvent::PropertySet {
            key: Symbol::new("speed"),
            value: PropertyValue::Float(12.5),
        };
        assert!(event.is_property_event());
        assert!(!event.is_edge_event());
        assert_eq!(event.property_key().unwrap().as_str(), "speed");
    }

    #[test]
    fn test_edge_added_event() {
        let qid = NexoraId::new_random();
        let event = NodeChangeEvent::EdgeAdded {
            edge: HalfEdge::out(Symbol::new("KNOWS"), qid),
        };
        assert!(!event.is_property_event());
        assert!(event.is_edge_event());
        assert!(event.property_key().is_none());
    }

    #[test]
    fn test_timed_event() {
        let event = NodeChangeEvent::PropertySet {
            key: Symbol::new("name"),
            value: PropertyValue::String("Alice".into()),
        };
        let timed = TimedEvent::new(event, EventTime::now());
        assert!(timed.time.as_micros() > 0);
    }

    #[test]
    fn test_node_event_as_change() {
        let change = NodeChangeEvent::PropertyRemoved {
            key: Symbol::new("temp"),
            previous_value: PropertyValue::Float(36.5),
        };
        let event = NodeEvent::Change(change);
        assert!(event.as_change().is_some());
        assert!(event.as_domain_index().is_none());
    }

    #[test]
    fn test_serde_roundtrip() {
        let event = NodeEvent::Change(NodeChangeEvent::PropertySet {
            key: Symbol::new("status"),
            value: PropertyValue::String("active".into()),
        });
        let json = serde_json::to_string(&event).unwrap();
        let restored: NodeEvent = serde_json::from_str(&json).unwrap();
        match restored {
            NodeEvent::Change(NodeChangeEvent::PropertySet { key, value }) => {
                assert_eq!(key.as_str(), "status");
                assert_eq!(value.as_str(), Some("active"));
            }
            _ => panic!("Unexpected variant"),
        }
    }

    // ====== P0.1 Step 1: Label event tests ======
    #[test]
    fn test_label_added_event() {
        let event = NodeChangeEvent::LabelAdded {
            label: Symbol::new("Device"),
        };
        assert!(event.is_label_event());
        assert!(!event.is_property_event());
        assert!(!event.is_edge_event());
        assert_eq!(event.label().unwrap().as_str(), "Device");
    }

    #[test]
    fn test_label_removed_event() {
        let event = NodeChangeEvent::LabelRemoved {
            label: Symbol::new("Emergency"),
        };
        assert!(event.is_label_event());
        assert_eq!(event.label().unwrap().as_str(), "Emergency");
    }

    #[test]
    fn test_label_event_serde() {
        let event = NodeEvent::Change(NodeChangeEvent::LabelAdded {
            label: Symbol::new("Asset"),
        });
        let json = serde_json::to_string(&event).unwrap();
        let restored: NodeEvent = serde_json::from_str(&json).unwrap();
        match restored {
            NodeEvent::Change(NodeChangeEvent::LabelAdded { label }) => {
                assert_eq!(label.as_str(), "Asset");
            }
            _ => panic!("Unexpected variant"),
        }
    }

    // ====== P0.1 Step 2: Edge property event tests ======
    #[test]
    fn test_edge_property_set_event() {
        let target = NexoraId::new_random();
        let event = NodeChangeEvent::EdgePropertySet {
            edge_type: Symbol::new("EXECUTING"),
            target,
            key: Symbol::new("progress"),
            value: PropertyValue::Integer(50),
        };
        assert!(event.is_edge_property_event());
        assert!(!event.is_property_event());
        assert!(!event.is_edge_event());
        assert!(!event.is_label_event());
    }

    #[test]
    fn test_edge_property_removed_event() {
        let target = NexoraId::new_random();
        let event = NodeChangeEvent::EdgePropertyRemoved {
            edge_type: Symbol::new("EXECUTING"),
            target,
            key: Symbol::new("started_at"),
        };
        assert!(event.is_edge_property_event());
    }

    #[test]
    fn test_edge_property_event_serde() {
        let target = NexoraId::new_random();
        let event = NodeEvent::Change(NodeChangeEvent::EdgePropertySet {
            edge_type: Symbol::new("KNOWS"),
            target: target.clone(),
            key: Symbol::new("since"),
            value: PropertyValue::String("2020-01-01".into()),
        });
        let json = serde_json::to_string(&event).unwrap();
        let restored: NodeEvent = serde_json::from_str(&json).unwrap();
        match restored {
            NodeEvent::Change(NodeChangeEvent::EdgePropertySet {
                edge_type,
                target: restored_target,
                key,
                value,
            }) => {
                assert_eq!(edge_type.as_str(), "KNOWS");
                assert_eq!(restored_target, target);
                assert_eq!(key.as_str(), "since");
                assert_eq!(value.as_str(), Some("2020-01-01"));
            }
            _ => panic!("Unexpected variant"),
        }
    }

    // ====== P0.1 Step 3: Node lifecycle event tests ======
    #[test]
    fn test_node_deleted_event() {
        let tombstone = TombstoneRecord {
            deleted_at: EventTime::now(),
            deleted_by: Some("admin".into()),
            reason: Some("decommissioned".into()),
        };
        let event = NodeChangeEvent::NodeDeleted {
            tombstone: tombstone.clone(),
        };
        assert!(event.is_lifecycle_event());
        assert!(event.is_deleted());
        assert!(!event.is_restored());
        assert!(!event.is_property_event());
    }

    #[test]
    fn test_node_restored_event() {
        let event = NodeChangeEvent::NodeRestored;
        assert!(event.is_lifecycle_event());
        assert!(!event.is_deleted());
        assert!(event.is_restored());
    }

    #[test]
    fn test_node_deleted_event_serde() {
        let tombstone = TombstoneRecord {
            deleted_at: EventTime::now(),
            deleted_by: Some("user123".into()),
            reason: Some("test delete".into()),
        };
        let event = NodeEvent::Change(NodeChangeEvent::NodeDeleted {
            tombstone: tombstone.clone(),
        });
        let json = serde_json::to_string(&event).unwrap();
        let restored: NodeEvent = serde_json::from_str(&json).unwrap();
        match restored {
            NodeEvent::Change(NodeChangeEvent::NodeDeleted { tombstone: t }) => {
                assert_eq!(t.deleted_by, tombstone.deleted_by);
                assert_eq!(t.reason, tombstone.reason);
            }
            _ => panic!("Unexpected variant"),
        }
    }
}
