use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::sync::Arc;

/// An interned string used for property keys and edge types.
///
/// In the reference implementation, `Symbol` is a JVM interned string.
/// In Rust, we use `Arc<str>` for cheap cloning and comparison.
/// This is the most frequently used key type in the graph:
/// every property read/write and every edge operation uses a Symbol.
///
/// # Design Choice
///
/// We use `Arc<str>` rather than `String` because Symbols are:
/// 1. Read-heavy (thousands of reads per write)
/// 2. Shared across many nodes (same property name, e.g., "speed", "location")
/// 3. Used as HashMap keys (Arc<str> compares by content, not pointer)
///
/// For high-performance scenarios, a global string interner could replace this,
/// but Arc<str> is a good starting point that avoids the complexity of interning.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Symbol(Arc<str>);

impl Serialize for Symbol {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(Self::from_string(s))
    }
}

impl Symbol {
    /// Create a new Symbol from a string slice.
    pub fn new(s: &str) -> Self {
        Self(Arc::from(s))
    }

    /// Create a Symbol from a String (takes ownership, avoids copy if possible).
    pub fn from_string(s: String) -> Self {
        Self(Arc::from(s))
    }

    /// Access the underlying string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The length of the symbol in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the symbol is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Symbol(\"{}\")", self.0)
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for Symbol {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Symbol {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for Symbol {
    fn from(s: String) -> Self {
        Self::from_string(s)
    }
}

impl PartialEq<str> for Symbol {
    fn eq(&self, other: &str) -> bool {
        self.0.as_ref() == other
    }
}

impl PartialEq<&str> for Symbol {
    fn eq(&self, other: &&str) -> bool {
        self.0.as_ref() == *other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_and_as_str() {
        let s = Symbol::new("speed");
        assert_eq!(s.as_str(), "speed");
        assert_eq!(s.len(), 5);
    }

    #[test]
    fn test_clone_is_cheap() {
        let s1 = Symbol::new("property_name");
        let s2 = s1.clone();
        // Both point to the same Arc allocation
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_ordering() {
        let s1 = Symbol::new("alpha");
        let s2 = Symbol::new("beta");
        assert!(s1 < s2);
    }

    #[test]
    fn test_from_string() {
        let owned = "test_key".to_string();
        let s = Symbol::from_string(owned);
        assert_eq!(s.as_str(), "test_key");
    }

    #[test]
    fn test_partial_eq_str() {
        let s = Symbol::new("hello");
        assert_eq!(s, "hello");
        assert_eq!(s, *"hello");
        assert_ne!(s, "world");
    }

    #[test]
    fn test_serde_roundtrip() {
        let s = Symbol::new("my_property");
        let json = serde_json::to_string(&s).unwrap();
        let restored: Symbol = serde_json::from_str(&json).unwrap();
        assert_eq!(s, restored);
    }
}
