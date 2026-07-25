use crate::Symbol;
use nexora_id::NexoraId;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Direction of a graph edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EdgeDirection {
    /// Outgoing edge from this node (this → other).
    Out,
    /// Incoming edge to this node (other → this).
    In,
}

impl EdgeDirection {
    pub fn is_out(&self) -> bool {
        matches!(self, Self::Out)
    }
    pub fn is_in(&self) -> bool {
        matches!(self, Self::In)
    }
    pub fn reverse(&self) -> Self {
        match self {
            Self::Out => Self::In,
            Self::In => Self::Out,
        }
    }
}

impl fmt::Display for EdgeDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Out => write!(f, "OUT"),
            Self::In => write!(f, "IN"),
        }
    }
}

/// A "half-edge" — one side of a graph relationship as seen from one node.
///
/// In the Nexora model, each relationship is stored as two half-edges:
/// one on the source node (direction=Out) and one on the target node (direction=In).
/// This mirrors the `HalfEdge` case class:
///   `final case class HalfEdge(edgeType: Symbol, direction: EdgeDirection, other: NexoraId)`
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HalfEdge {
    /// The relationship type (e.g., "KNOWS", "LOCATED_AT").
    pub edge_type: Symbol,
    /// Direction relative to the owning node.
    pub direction: EdgeDirection,
    /// The NexoraId of the node at the other end of the edge.
    pub other: NexoraId,
}

impl HalfEdge {
    pub fn new(edge_type: Symbol, direction: EdgeDirection, other: NexoraId) -> Self {
        Self {
            edge_type,
            direction,
            other,
        }
    }

    /// Create an outgoing edge.
    pub fn out(edge_type: Symbol, target: NexoraId) -> Self {
        Self::new(edge_type, EdgeDirection::Out, target)
    }

    /// Create an incoming edge.
    pub fn incoming(edge_type: Symbol, source: NexoraId) -> Self {
        Self::new(edge_type, EdgeDirection::In, source)
    }

    /// The size of this half-edge in bytes (for memory accounting).
    pub fn memory_size(&self) -> usize {
        // Symbol (string header + content) + direction (1 byte) + NexoraId (vec header + content)
        24 + self.edge_type.as_str().len() + 1 + 24 + self.other.len()
    }
}

impl fmt::Debug for HalfEdge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "HalfEdge({} {} {})",
            self.edge_type, self.direction, self.other
        )
    }
}

impl fmt::Display for HalfEdge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "-[:{} {}]->({})",
            self.edge_type, self.direction, self.other
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_half_edge_outgoing() {
        let target = NexoraId::new_random();
        let he = HalfEdge::out(Symbol::new("KNOWS"), target.clone());
        assert_eq!(he.direction, EdgeDirection::Out);
        assert_eq!(he.edge_type.as_str(), "KNOWS");
        assert_eq!(he.other, target);
    }

    #[test]
    fn test_half_edge_incoming() {
        let source = NexoraId::new_random();
        let he = HalfEdge::incoming(Symbol::new("IN_ZONE"), source.clone());
        assert_eq!(he.direction, EdgeDirection::In);
        assert_eq!(he.edge_type.as_str(), "IN_ZONE");
        assert_eq!(he.other, source);
    }

    #[test]
    fn test_edge_direction_reverse() {
        assert_eq!(EdgeDirection::Out.reverse(), EdgeDirection::In);
        assert_eq!(EdgeDirection::In.reverse(), EdgeDirection::Out);
    }

    #[test]
    fn test_serde_roundtrip() {
        let he = HalfEdge::out(Symbol::new("FOLLOWS"), NexoraId::new_random());
        let json = serde_json::to_string(&he).unwrap();
        let restored: HalfEdge = serde_json::from_str(&json).unwrap();
        assert_eq!(he, restored);
    }

    #[test]
    fn test_memory_size_positive() {
        let he = HalfEdge::out(Symbol::new("TYPE"), NexoraId::new_random());
        assert!(he.memory_size() > 0);
    }
}
