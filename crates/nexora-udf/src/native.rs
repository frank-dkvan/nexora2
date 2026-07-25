//! Native Rust UDF registry — built-in functions and registration.

use crate::{UdfError, UdfKind, UserDefinedFunction};
use nexora_id::PropertyValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Registry of named UDFs, allowing dynamic lookup and execution.
pub struct UdfRegistry {
    functions: HashMap<String, Arc<dyn UserDefinedFunction>>,
}

impl UdfRegistry {
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
        }
    }

    /// Register a UDF by name.
    pub fn register(&mut self, udf: Arc<dyn UserDefinedFunction>) {
        let name = udf.name().to_string();
        self.functions.insert(name, udf);
    }

    /// Look up a UDF by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn UserDefinedFunction>> {
        self.functions.get(name).cloned()
    }

    /// Execute a named UDF on the given properties.
    pub fn execute(
        &self,
        name: &str,
        properties: &HashMap<String, PropertyValue>,
    ) -> Result<PropertyValue, UdfError> {
        self.functions
            .get(name)
            .ok_or_else(|| UdfError::NotFound(name.to_string()))
            .and_then(|udf| udf.execute(properties))
    }

    /// List all registered UDF names.
    pub fn list(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }

    /// Remove a registered UDF by name.
    pub fn remove(&mut self, name: &str) -> bool {
        self.functions.remove(name).is_some()
    }

    /// Register all built-in native UDFs.
    pub fn with_builtins() -> Self {
        let mut reg = Self::new();
        reg.register(Arc::new(SpeedConverter));
        reg.register(Arc::new(TemperatureConverter));
        reg.register(Arc::new(StringUpper));
        reg.register(Arc::new(StringLength));
        reg.register(Arc::new(BooleanNot));
        reg
    }
}

impl Default for UdfRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================
// Built-in Native UDFs
// ============================================================

/// Convert speed: km/h <-> mph.
/// Reads "speed" and "unit" fields; returns converted Float.
pub struct SpeedConverter;

impl UserDefinedFunction for SpeedConverter {
    fn name(&self) -> &str {
        "speed_convert"
    }
    fn kind(&self) -> UdfKind {
        UdfKind::Map
    }

    fn execute(&self, props: &HashMap<String, PropertyValue>) -> Result<PropertyValue, UdfError> {
        let speed = props.get("speed").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let unit = props.get("unit").and_then(|v| v.as_str()).unwrap_or("kmh");
        let result = match unit {
            "mph_to_kmh" => speed * 1.60934,
            "kmh_to_mph" => speed / 1.60934,
            _ => speed, // passthrough
        };
        Ok(PropertyValue::Float(result))
    }
}

/// Convert temperature: Celsius <-> Fahrenheit.
pub struct TemperatureConverter;

impl UserDefinedFunction for TemperatureConverter {
    fn name(&self) -> &str {
        "temp_convert"
    }
    fn kind(&self) -> UdfKind {
        UdfKind::Map
    }

    fn execute(&self, props: &HashMap<String, PropertyValue>) -> Result<PropertyValue, UdfError> {
        let temp = props.get("temp").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let to = props.get("to").and_then(|v| v.as_str()).unwrap_or("f");
        let result = match to {
            "f" => temp * 9.0 / 5.0 + 32.0,   // C -> F
            "c" => (temp - 32.0) * 5.0 / 9.0, // F -> C
            _ => temp,
        };
        Ok(PropertyValue::Float(result))
    }
}

/// Uppercase a string field.
pub struct StringUpper;

impl UserDefinedFunction for StringUpper {
    fn name(&self) -> &str {
        "to_upper"
    }
    fn kind(&self) -> UdfKind {
        UdfKind::Map
    }

    fn execute(&self, props: &HashMap<String, PropertyValue>) -> Result<PropertyValue, UdfError> {
        let text = props.get("text").and_then(|v| v.as_str()).unwrap_or("");
        Ok(PropertyValue::String(text.to_uppercase()))
    }
}

/// Return the length of a string or list.
pub struct StringLength;

impl UserDefinedFunction for StringLength {
    fn name(&self) -> &str {
        "length"
    }
    fn kind(&self) -> UdfKind {
        UdfKind::Map
    }

    fn execute(&self, props: &HashMap<String, PropertyValue>) -> Result<PropertyValue, UdfError> {
        if let Some(s) = props.get("text").and_then(|v| v.as_str()) {
            return Ok(PropertyValue::Integer(s.len() as i64));
        }
        if let Some(PropertyValue::List(l)) = props.get("list") {
            return Ok(PropertyValue::Integer(l.len() as i64));
        }
        Ok(PropertyValue::Integer(0))
    }
}

/// Logical NOT on a boolean field.
pub struct BooleanNot;

impl UserDefinedFunction for BooleanNot {
    fn name(&self) -> &str {
        "not"
    }
    fn kind(&self) -> UdfKind {
        UdfKind::Map
    }

    fn execute(&self, props: &HashMap<String, PropertyValue>) -> Result<PropertyValue, UdfError> {
        let b = props
            .get("value")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        Ok(PropertyValue::Boolean(!b))
    }
}

// ============================================================
// Macro for defining native UDFs inline
// ============================================================

/// Convenience macro to create a simple map UDF from a closure.
#[macro_export]
macro_rules! udf_map {
    ($name:expr, $field:expr, $body:expr) => {{
        struct AnonUdf;
        impl $crate::UserDefinedFunction for AnonUdf {
            fn name(&self) -> &str {
                $name
            }
            fn kind(&self) -> $crate::UdfKind {
                $crate::UdfKind::Map
            }
            fn execute(
                &self,
                props: &std::collections::HashMap<String, nexora_id::PropertyValue>,
            ) -> Result<nexora_id::PropertyValue, $crate::UdfError> {
                let __field_val = props.get($field);
                $body
            }
        }
        AnonUdf
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_registry_register_and_execute() {
        let mut reg = UdfRegistry::new();
        reg.register(Arc::new(SpeedConverter));

        let mut props = HashMap::new();
        props.insert("speed".into(), PropertyValue::Float(100.0));
        props.insert("unit".into(), PropertyValue::String("kmh_to_mph".into()));

        let result = reg.execute("speed_convert", &props).unwrap();
        assert!((result.as_f64().unwrap() - 62.137).abs() < 0.1);
    }

    #[test]
    fn test_registry_not_found() {
        let reg = UdfRegistry::new();
        let props = HashMap::new();
        let result = reg.execute("nonexistent", &props);
        assert!(matches!(result, Err(UdfError::NotFound(_))));
    }

    #[test]
    fn test_builtins() {
        let reg = UdfRegistry::with_builtins();
        assert!(reg.list().len() >= 5);

        let mut props = HashMap::new();
        props.insert("text".into(), PropertyValue::String("hello".into()));
        let result = reg.execute("to_upper", &props).unwrap();
        assert_eq!(result, PropertyValue::String("HELLO".into()));
    }

    #[test]
    fn test_temp_converter() {
        let reg = UdfRegistry::with_builtins();
        let mut props = HashMap::new();
        props.insert("temp".into(), PropertyValue::Float(0.0));
        props.insert("to".into(), PropertyValue::String("f".into()));
        let result = reg.execute("temp_convert", &props).unwrap();
        assert!((result.as_f64().unwrap() - 32.0).abs() < 0.01);
    }

    #[test]
    fn test_string_length() {
        let reg = UdfRegistry::with_builtins();
        let mut props = HashMap::new();
        props.insert("text".into(), PropertyValue::String("hello world".into()));
        let result = reg.execute("length", &props).unwrap();
        assert_eq!(result, PropertyValue::Integer(11));
    }

    #[test]
    fn test_boolean_not() {
        let reg = UdfRegistry::with_builtins();
        let mut props = HashMap::new();
        props.insert("value".into(), PropertyValue::Boolean(true));
        let result = reg.execute("not", &props).unwrap();
        assert_eq!(result, PropertyValue::Boolean(false));
    }
}
