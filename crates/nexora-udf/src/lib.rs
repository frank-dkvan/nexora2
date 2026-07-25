//! NOTE: This crate is planned for future integration.
//! UDF framework — user-defined functions for streaming graph processing.
//!
//! Supports:
//! - Native Rust UDFs (compiled inline, zero overhead)
//! - WASM UDFs (sandboxed, secure)
//! - Expression UDFs (declarative, SQL-like)
//!
//! Architecture:
//! ```text
//!   Graph Event → UDF Pipeline → Transformed Event → SQ / Output
//!                ├── Filter UDF
//!                ├── Map UDF
//!                └── Aggregate UDF
//! ```

pub mod manager;
pub mod native;
pub mod pipeline;
pub mod python_runtime;
pub mod types;
pub mod wasm_runtime;

use nexora_id::PropertyValue;
use std::collections::HashMap;

/// Trait for user-defined functions.
pub trait UserDefinedFunction: Send + Sync {
    /// Return the function name.
    fn name(&self) -> &str;

    /// Return the function type.
    fn kind(&self) -> UdfKind;

    /// Execute the function on a single event's properties.
    fn execute(
        &self,
        properties: &HashMap<String, PropertyValue>,
    ) -> Result<PropertyValue, UdfError>;
}

/// Types of UDFs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UdfKind {
    /// Filter: return Boolean, true = keep event
    Filter,
    /// Map: transform one property value to another
    Map,
    /// Aggregate: accumulate state across events
    Aggregate,
    /// Enrich: add new properties from external lookup
    Enrich,
}

#[derive(Debug, thiserror::Error)]
pub enum UdfError {
    #[error("UDF execution failed: {0}")]
    Execution(String),
    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
    #[error("function not found: {0}")]
    NotFound(String),
}

/// Error type for UDF runtime operations (Wasm and Python).
#[derive(Debug, thiserror::Error)]
pub enum UdfRuntimeError {
    #[error("Wasm runtime error: {0}")]
    Wasm(String),
    #[error("Python runtime error: {0}")]
    Python(String),
    #[error("UDF not found: {0}")]
    NotFound(String),
    #[error("Execution timeout after {0:?}")]
    Timeout(std::time::Duration),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("Security violation: {0}")]
    Security(String),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type UdfRuntimeResult<T> = Result<T, UdfRuntimeError>;

/// A pipeline of UDFs applied sequentially to events.
pub struct UdfPipeline {
    filters: Vec<Box<dyn UserDefinedFunction>>,
    maps: Vec<Box<dyn UserDefinedFunction>>,
    enrichers: Vec<Box<dyn UserDefinedFunction>>,
}

impl UdfPipeline {
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
            maps: Vec::new(),
            enrichers: Vec::new(),
        }
    }

    pub fn add_filter(&mut self, udf: Box<dyn UserDefinedFunction>) {
        self.filters.push(udf);
    }

    pub fn add_map(&mut self, udf: Box<dyn UserDefinedFunction>) {
        self.maps.push(udf);
    }

    pub fn add_enricher(&mut self, udf: Box<dyn UserDefinedFunction>) {
        self.enrichers.push(udf);
    }

    /// Run the full pipeline: filter → map → enrich.
    pub fn process(
        &self,
        mut properties: HashMap<String, PropertyValue>,
    ) -> Result<Option<HashMap<String, PropertyValue>>, UdfError> {
        // 1. Filters
        for filter in &self.filters {
            match filter.execute(&properties)? {
                PropertyValue::Boolean(true) => continue, // Passes filter
                PropertyValue::Boolean(false) => return Ok(None), // Filtered out
                other => {
                    return Err(UdfError::TypeMismatch {
                        expected: "Boolean".into(),
                        actual: format!("{:?}", other),
                    })
                }
            }
        }

        // 2. Maps
        for map in &self.maps {
            let result = map.execute(&properties)?;
            properties.insert(map.name().to_string(), result);
        }

        // 3. Enrichers
        for enricher in &self.enrichers {
            let result = enricher.execute(&properties)?;
            if let PropertyValue::Map(additions) = result {
                for (k, v) in additions {
                    properties.insert(k, v);
                }
            }
        }

        Ok(Some(properties))
    }

    /// Return the count of UDFs.
    pub fn len(&self) -> usize {
        self.filters.len() + self.maps.len() + self.enrichers.len()
    }

    /// Check if there are no UDFs registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for UdfPipeline {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A simple filter that checks if speed > threshold.
    struct SpeedFilter {
        threshold: f64,
    }
    impl UserDefinedFunction for SpeedFilter {
        fn name(&self) -> &str {
            "speed_filter"
        }
        fn kind(&self) -> UdfKind {
            UdfKind::Filter
        }
        fn execute(
            &self,
            props: &HashMap<String, PropertyValue>,
        ) -> Result<PropertyValue, UdfError> {
            let speed = props.get("speed").and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(PropertyValue::Boolean(speed > self.threshold))
        }
    }

    /// A map that computes a 'risk_score'.
    struct RiskScorer;
    impl UserDefinedFunction for RiskScorer {
        fn name(&self) -> &str {
            "risk_score"
        }
        fn kind(&self) -> UdfKind {
            UdfKind::Map
        }
        fn execute(
            &self,
            props: &HashMap<String, PropertyValue>,
        ) -> Result<PropertyValue, UdfError> {
            let speed = props.get("speed").and_then(|v| v.as_f64()).unwrap_or(0.0);
            Ok(PropertyValue::Float(speed * 1.5 + 10.0))
        }
    }

    #[test]
    fn test_filter_passes() {
        let filter = SpeedFilter { threshold: 50.0 };
        let mut props = HashMap::new();
        props.insert("speed".into(), PropertyValue::Float(80.0));
        let result = filter.execute(&props).unwrap();
        assert_eq!(result, PropertyValue::Boolean(true));
    }

    #[test]
    fn test_filter_rejects() {
        let filter = SpeedFilter { threshold: 50.0 };
        let mut props = HashMap::new();
        props.insert("speed".into(), PropertyValue::Float(30.0));
        let result = filter.execute(&props).unwrap();
        assert_eq!(result, PropertyValue::Boolean(false));
    }

    #[test]
    fn test_pipeline_filter_and_map() {
        let mut pipeline = UdfPipeline::new();
        pipeline.add_filter(Box::new(SpeedFilter { threshold: 50.0 }));
        pipeline.add_map(Box::new(RiskScorer));

        let mut props = HashMap::new();
        props.insert("speed".into(), PropertyValue::Float(80.0));

        let result = pipeline.process(props).unwrap();
        assert!(result.is_some());
        let out = result.unwrap();
        assert_eq!(out.get("risk_score"), Some(&PropertyValue::Float(130.0)));
    }

    #[test]
    fn test_pipeline_filters_out() {
        let mut pipeline = UdfPipeline::new();
        pipeline.add_filter(Box::new(SpeedFilter { threshold: 50.0 }));

        let mut props = HashMap::new();
        props.insert("speed".into(), PropertyValue::Float(30.0));

        let result = pipeline.process(props).unwrap();
        assert!(result.is_none(), "Should be filtered out");
    }
}
