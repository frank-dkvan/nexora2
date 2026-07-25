use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// The fundamental value type stored as node properties in the Nexora graph.
///
/// This enum mirrors the Cypher type system and the `NexoraValue` sealed trait.
/// It is the universal value representation for:
/// - Node property values
/// - Standing Query result fields
/// - Cypher expression evaluation results
/// - Ingest data transformations
///
/// # Design Notes
///
/// - `BTreeMap` (not `HashMap`) for deterministic iteration order, matching
///   the `ListMap` behavior used in Circe JSON serialization.
/// - `i64` for integers (matching `Long`).
/// - `f64` for floats (matching `Double`).
/// - `Vec<u8>` for bytes (matching `Array[Byte]`).
/// - Date/time types stored as `chrono` types for interop with the persistence layer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PropertyValue {
    /// Null / missing value.
    Null,
    /// Boolean.
    Boolean(bool),
    /// Signed 64-bit integer (Cypher `INTEGER`).
    Integer(i64),
    /// 64-bit floating point (Cypher `FLOAT`).
    Float(f64),
    /// UTF-8 string.
    String(String),
    /// Raw bytes.
    Bytes(Vec<u8>),
    /// Ordered list of values (Cypher `LIST`).
    List(Vec<PropertyValue>),
    /// Key-value map (Cypher `MAP`). Keys are always strings.
    Map(BTreeMap<String, PropertyValue>),
    /// A node reference (used in Cypher path expressions).
    Node(NodeValue),
    /// A relationship reference (used in Cypher path expressions).
    Relationship(RelationshipValue),
    /// A path (sequence of alternating nodes and relationships).
    Path(PathValue),
    /// Cypher `DATE` (year-month-day, no timezone).
    Date(NaiveDate),
    /// Cypher `LOCAL DATETIME` (date + time, no timezone).
    LocalDateTime(NaiveDateTime),
    /// Cypher `ZONED DATETIME` (date + time + timezone).
    ZonedDateTime(DateTime<Utc>),
    /// Duration (Cypher `DURATION`).
    Duration(DurationValue),
    /// Point (Cypher `POINT` — geographic or Cartesian).
    Point(PointValue),
    /// Reference to a binary blob stored in ReductStore (or S3/MinIO).
    BlobRef(crate::blob::BlobRef),
}

/// A graph node as seen by Cypher expressions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeValue {
    pub id: Vec<u8>,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, PropertyValue>,
}

/// A graph relationship as seen by Cypher expressions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RelationshipValue {
    pub id: Vec<u8>,
    pub rel_type: String,
    pub start_node_id: Vec<u8>,
    pub end_node_id: Vec<u8>,
    pub properties: BTreeMap<String, PropertyValue>,
}

/// A Cypher path: alternating node and relationship references.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PathValue {
    pub nodes: Vec<NodeValue>,
    pub relationships: Vec<RelationshipValue>,
}

/// Cypher `DURATION` — months, days, seconds, nanoseconds.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct DurationValue {
    pub months: i64,
    pub days: i64,
    pub seconds: i64,
    pub nanoseconds: i64,
}

/// Cypher `POINT` — geographic (WGS 84) or Cartesian.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct PointValue {
    pub x: f64,
    pub y: f64,
    pub z: Option<f64>,
    pub srid: i32,
}

// Re-export chrono types used in PropertyValue
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

impl PropertyValue {
    /// Convenience constructor for `PropertyValue::Null`.
    pub fn null() -> Self {
        Self::Null
    }

    /// Whether this value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Try to extract a boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to extract an integer.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to extract a float.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            Self::Integer(i) => Some(*i as f64), // implicit coercion like Cypher
            _ => None,
        }
    }

    /// Try to extract a string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to extract bytes reference.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Try to extract a list reference.
    pub fn as_list(&self) -> Option<&[PropertyValue]> {
        match self {
            Self::List(l) => Some(l),
            _ => None,
        }
    }

    /// Try to extract a map reference.
    pub fn as_map(&self) -> Option<&BTreeMap<String, PropertyValue>> {
        match self {
            Self::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Estimate the in-memory size of this value in bytes.
    ///
    /// Used by the node memory management system to decide when to sleep nodes.
    pub fn memory_size(&self) -> usize {
        match self {
            Self::Null | Self::Boolean(_) => 8,
            Self::Integer(_) | Self::Float(_) => 8,
            Self::String(s) => 24 + s.len(), // String header + content
            Self::Bytes(b) => 24 + b.len(),  // Vec header + content
            Self::List(l) => 24 + l.iter().map(|v| v.memory_size()).sum::<usize>(),
            Self::Map(m) => {
                24 + m
                    .iter()
                    .map(|(k, v)| 24 + k.len() + v.memory_size())
                    .sum::<usize>()
            }
            Self::Node(n) => 24 + n.id.len() + n.labels.iter().map(|l| 24 + l.len()).sum::<usize>(),
            Self::Relationship(r) => 24 + r.id.len() + r.rel_type.len() + 32,
            Self::Path(p) => {
                p.nodes.iter().map(|n| 24 + n.id.len()).sum::<usize>()
                    + p.relationships
                        .iter()
                        .map(|r| 24 + r.id.len())
                        .sum::<usize>()
            }
            Self::Date(_) => 16,
            Self::LocalDateTime(_) | Self::ZonedDateTime(_) => 16,
            Self::Duration(_) => 32,
            Self::Point(_) => 24,
            Self::BlobRef(b) => b.memory_size(),
        }
    }
}

// --- Conversions for ergonomic use ---

impl From<bool> for PropertyValue {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

impl From<i64> for PropertyValue {
    fn from(v: i64) -> Self {
        Self::Integer(v)
    }
}

impl From<i32> for PropertyValue {
    fn from(v: i32) -> Self {
        Self::Integer(v as i64)
    }
}

impl From<f64> for PropertyValue {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<String> for PropertyValue {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<&str> for PropertyValue {
    fn from(v: &str) -> Self {
        Self::String(v.to_owned())
    }
}

impl From<Vec<u8>> for PropertyValue {
    fn from(v: Vec<u8>) -> Self {
        Self::Bytes(v)
    }
}

impl From<Vec<PropertyValue>> for PropertyValue {
    fn from(v: Vec<PropertyValue>) -> Self {
        Self::List(v)
    }
}

impl From<BTreeMap<String, PropertyValue>> for PropertyValue {
    fn from(v: BTreeMap<String, PropertyValue>) -> Self {
        Self::Map(v)
    }
}

impl fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Boolean(b) => write!(f, "{b}"),
            Self::Integer(i) => write!(f, "{i}"),
            Self::Float(fl) => write!(f, "{fl}"),
            Self::String(s) => write!(f, "\"{s}\""),
            Self::Bytes(b) => write!(f, "<{} bytes>", b.len()),
            Self::List(l) => {
                write!(f, "[")?;
                for (i, v) in l.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
            Self::Map(m) => {
                write!(f, "{{")?;
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
            Self::Node(n) => write!(f, "(:{})", n.labels.join(":")),
            Self::Relationship(r) => write!(f, "[:{}]", r.rel_type),
            Self::Path(p) => write!(f, "<path len={}>", p.nodes.len()),
            Self::Date(d) => write!(f, "{d}"),
            Self::LocalDateTime(dt) => write!(f, "{dt}"),
            Self::ZonedDateTime(dt) => write!(f, "{dt}"),
            Self::Duration(d) => {
                write!(
                    f,
                    "P{}M{}DT{}.{}S",
                    d.months, d.days, d.seconds, d.nanoseconds
                )
            }
            Self::Point(p) => match p.z {
                Some(z) => write!(f, "point({}, {}, {})", p.x, p.y, z),
                None => write!(f, "point({}, {})", p.x, p.y),
            },
            Self::BlobRef(b) => write!(f, "{b}"),
        }
    }
}

impl From<crate::blob::BlobRef> for PropertyValue {
    fn from(v: crate::blob::BlobRef) -> Self {
        Self::BlobRef(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conversions() {
        let v: PropertyValue = 42i64.into();
        assert_eq!(v.as_i64(), Some(42));

        let v: PropertyValue = "hello".into();
        assert_eq!(v.as_str(), Some("hello"));

        let v: PropertyValue = true.into();
        assert_eq!(v.as_bool(), Some(true));

        let v: PropertyValue = 3.125f64.into();
        assert_eq!(v.as_f64(), Some(3.125));
    }

    #[test]
    fn test_null() {
        let v = PropertyValue::null();
        assert!(v.is_null());
        assert_eq!(v.as_i64(), None);
        assert_eq!(format!("{v}"), "null");
    }

    #[test]
    fn test_list_display() {
        let v = PropertyValue::List(vec![
            PropertyValue::Integer(1),
            PropertyValue::String("two".into()),
        ]);
        assert_eq!(format!("{v}"), "[1, \"two\"]");
    }

    #[test]
    fn test_map_display() {
        let mut m = BTreeMap::new();
        m.insert("name".to_string(), PropertyValue::String("Alice".into()));
        m.insert("age".to_string(), PropertyValue::Integer(30));
        let v = PropertyValue::Map(m);
        let s = format!("{v}");
        assert!(s.contains("name: \"Alice\""));
        assert!(s.contains("age: 30"));
    }

    #[test]
    fn test_memory_size() {
        let v = PropertyValue::Integer(42);
        assert_eq!(v.memory_size(), 8);

        let v = PropertyValue::String("hello".to_string());
        assert_eq!(v.memory_size(), 24 + 5);
    }

    #[test]
    fn test_serde_roundtrip() {
        let v = PropertyValue::List(vec![
            PropertyValue::Integer(1),
            PropertyValue::Boolean(true),
            PropertyValue::Null,
            PropertyValue::String("test".into()),
        ]);
        let json = serde_json::to_string(&v).unwrap();
        let restored: PropertyValue = serde_json::from_str(&json).unwrap();
        assert_eq!(v, restored);
    }

    #[test]
    fn test_float_coercion_from_integer() {
        let v = PropertyValue::Integer(42);
        assert_eq!(v.as_f64(), Some(42.0));
    }

    #[test]
    fn blob_ref_roundtrip_serde() {
        use crate::blob::BlobRef;
        let blob = BlobRef::new("videos", "cam-01", 1_700_000_000_000_000, 1024, "video/mp4");
        let pv = PropertyValue::BlobRef(blob.clone());
        let json = serde_json::to_string(&pv).expect("serialize BlobRef PropertyValue");
        let restored: PropertyValue =
            serde_json::from_str(&json).expect("deserialize BlobRef PropertyValue");
        assert_eq!(
            pv, restored,
            "BlobRef PropertyValue must round-trip through JSON"
        );
        // Also verify the From conversion works
        let pv2: PropertyValue = blob.into();
        assert!(matches!(pv2, PropertyValue::BlobRef(_)));
    }

    #[test]
    fn blob_ref_memory_size_non_zero() {
        use crate::blob::BlobRef;
        let blob = BlobRef::new("bucket", "entry", 0, 512, "application/octet-stream");
        let pv = PropertyValue::BlobRef(blob);
        assert!(
            pv.memory_size() >= 256,
            "BlobRef memory size should reflect BlobRef::memory_size()"
        );
    }
}
