use async_trait::async_trait;
use nexora_id::PropertyValue;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use thiserror::Error;

/// UDF 错误类型
#[derive(Debug, Error)]
pub enum UdfError {
    #[error("UDF '{0}' not found")]
    NotFound(String),
    #[error("Invalid argument count: expected {expected}, got {actual}")]
    InvalidArgCount { expected: usize, actual: usize },
    #[error("Invalid argument type: {0}")]
    InvalidArgType(String),
    #[error("Execution error: {0}")]
    ExecutionError(String),
}

/// 函数签名
#[derive(Debug, Clone)]
pub struct FunctionSignature {
    /// 参数类型
    pub params: Vec<ParamType>,
    /// 返回类型
    pub return_type: ReturnType,
    /// 函数描述
    pub description: String,
}

/// 参数类型
#[derive(Debug, Clone)]
pub enum ParamType {
    String,
    Integer,
    Float,
    Boolean,
    Any,
}

/// 返回类型
#[derive(Debug, Clone)]
pub enum ReturnType {
    String,
    Integer,
    Float,
    Boolean,
    Map,
    Array,
    Any,
}

/// 用户自定义函数 trait
#[async_trait]
pub trait UserDefinedFunction: Send + Sync {
    /// 函数名
    fn name(&self) -> &str;

    /// 执行函数
    async fn execute(&self, args: Vec<PropertyValue>) -> Result<PropertyValue, UdfError>;

    /// 函数签名
    fn signature(&self) -> FunctionSignature;
}

/// UDF 注册表
pub struct UdfRegistry {
    // `Arc` (not `Box`) so `call` can clone the handle out of the lock and drop
    // the guard *before* awaiting `execute` — holding this std RwLock guard
    // across an await could stall the runtime thread and risks deadlock if the
    // awaited UDF re-entered the registry.
    functions: RwLock<HashMap<String, Arc<dyn UserDefinedFunction>>>,
}

impl UdfRegistry {
    /// 创建新的注册表
    pub fn new() -> Self {
        Self {
            functions: RwLock::new(HashMap::new()),
        }
    }

    /// 注册内置 UDF
    pub fn register_builtin(&self) {
        self.register("parse_temp", Box::new(ParseTempUdf));
        self.register("parse_geo", Box::new(ParseGeoUdf));
        self.register("fahrenheit_to_celsius", Box::new(FahrenheitToCelsiusUdf));
        self.register("parse_json", Box::new(ParseJsonUdf));
        self.register("regex_extract", Box::new(RegexExtractUdf));
        self.register("base64_decode", Box::new(Base64DecodeUdf));
        self.register("timestamp_parse", Box::new(TimestampParseUdf));
        self.register("validate_email", Box::new(ValidateEmailUdf));
        self.register("to_uppercase", Box::new(ToUppercaseUdf));
        self.register("to_lowercase", Box::new(ToLowercaseUdf));
    }

    /// 注册 UDF
    pub fn register(&self, name: &str, func: Box<dyn UserDefinedFunction>) {
        let mut functions = self.functions.write().unwrap();
        functions.insert(name.to_string(), Arc::from(func));
    }

    /// 调用 UDF
    pub async fn call(
        &self,
        name: &str,
        args: Vec<PropertyValue>,
    ) -> Result<PropertyValue, UdfError> {
        // Clone the Arc handle out, then DROP the guard before awaiting — never
        // hold the std RwLock across `execute().await`.
        let func = {
            let functions = self.functions.read().unwrap();
            functions
                .get(name)
                .ok_or_else(|| UdfError::NotFound(name.to_string()))?
                .clone()
        };
        func.execute(args).await
    }

    /// 列出所有 UDF
    pub fn list(&self) -> Vec<(String, FunctionSignature)> {
        let functions = self.functions.read().unwrap();
        functions
            .iter()
            .map(|(name, func)| (name.clone(), func.signature()))
            .collect()
    }

    /// 获取 UDF 签名
    pub fn get_signature(&self, name: &str) -> Option<FunctionSignature> {
        let functions = self.functions.read().unwrap();
        functions.get(name).map(|f| f.signature())
    }

    /// 检查 UDF 是否存在
    pub fn exists(&self, name: &str) -> bool {
        let functions = self.functions.read().unwrap();
        functions.contains_key(name)
    }
}

impl Default for UdfRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============= 内置 UDF 实现 =============

/// 解析温度字符串（"25.3C" -> 25.3）
struct ParseTempUdf;

#[async_trait]
impl UserDefinedFunction for ParseTempUdf {
    fn name(&self) -> &str {
        "parse_temp"
    }

    async fn execute(&self, args: Vec<PropertyValue>) -> Result<PropertyValue, UdfError> {
        if args.len() != 1 {
            return Err(UdfError::InvalidArgCount {
                expected: 1,
                actual: args.len(),
            });
        }

        let input = match &args[0] {
            PropertyValue::String(s) => s,
            _ => return Err(UdfError::InvalidArgType("Expected string".to_string())),
        };

        // 移除单位后缀（C, F, K）
        let value_str = input.trim_end_matches(|c: char| c.is_alphabetic() || c.is_whitespace());

        match value_str.parse::<f64>() {
            Ok(v) => Ok(PropertyValue::Float(v)),
            Err(_) => Err(UdfError::ExecutionError(format!(
                "Failed to parse temperature: {}",
                input
            ))),
        }
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature {
            params: vec![ParamType::String],
            return_type: ReturnType::Float,
            description: "Parse temperature string (e.g., '25.3C' -> 25.3)".to_string(),
        }
    }
}

/// 解析地理位置字符串（"40.7128,-74.0060" -> {lat: 40.7128, lon: -74.0060}）
struct ParseGeoUdf;

#[async_trait]
impl UserDefinedFunction for ParseGeoUdf {
    fn name(&self) -> &str {
        "parse_geo"
    }

    async fn execute(&self, args: Vec<PropertyValue>) -> Result<PropertyValue, UdfError> {
        if args.len() != 1 {
            return Err(UdfError::InvalidArgCount {
                expected: 1,
                actual: args.len(),
            });
        }

        let input = match &args[0] {
            PropertyValue::String(s) => s,
            _ => return Err(UdfError::InvalidArgType("Expected string".to_string())),
        };

        let parts: Vec<&str> = input.split(',').collect();
        if parts.len() != 2 {
            return Err(UdfError::ExecutionError(
                "Invalid geo format, expected 'lat,lon'".to_string(),
            ));
        }

        let lat = parts[0]
            .trim()
            .parse::<f64>()
            .map_err(|_| UdfError::ExecutionError("Invalid latitude".to_string()))?;

        let lon = parts[1]
            .trim()
            .parse::<f64>()
            .map_err(|_| UdfError::ExecutionError("Invalid longitude".to_string()))?;

        let mut map = BTreeMap::new();
        map.insert("lat".to_string(), PropertyValue::Float(lat));
        map.insert("lon".to_string(), PropertyValue::Float(lon));

        Ok(PropertyValue::Map(map))
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature {
            params: vec![ParamType::String],
            return_type: ReturnType::Map,
            description: "Parse geo coordinates (e.g., '40.7128,-74.0060')".to_string(),
        }
    }
}

/// 华氏转摄氏
struct FahrenheitToCelsiusUdf;

#[async_trait]
impl UserDefinedFunction for FahrenheitToCelsiusUdf {
    fn name(&self) -> &str {
        "fahrenheit_to_celsius"
    }

    async fn execute(&self, args: Vec<PropertyValue>) -> Result<PropertyValue, UdfError> {
        if args.len() != 1 {
            return Err(UdfError::InvalidArgCount {
                expected: 1,
                actual: args.len(),
            });
        }

        let fahrenheit = match &args[0] {
            PropertyValue::Float(f) => *f,
            PropertyValue::Integer(i) => *i as f64,
            _ => return Err(UdfError::InvalidArgType("Expected number".to_string())),
        };

        let celsius = (fahrenheit - 32.0) * 5.0 / 9.0;
        Ok(PropertyValue::Float(celsius))
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature {
            params: vec![ParamType::Float],
            return_type: ReturnType::Float,
            description: "Convert Fahrenheit to Celsius".to_string(),
        }
    }
}

/// 解析 JSON 字符串
struct ParseJsonUdf;

#[async_trait]
impl UserDefinedFunction for ParseJsonUdf {
    fn name(&self) -> &str {
        "parse_json"
    }

    async fn execute(&self, args: Vec<PropertyValue>) -> Result<PropertyValue, UdfError> {
        if args.len() != 1 {
            return Err(UdfError::InvalidArgCount {
                expected: 1,
                actual: args.len(),
            });
        }

        let input = match &args[0] {
            PropertyValue::String(s) => s,
            _ => return Err(UdfError::InvalidArgType("Expected string".to_string())),
        };

        let json_value: serde_json::Value = serde_json::from_str(input)
            .map_err(|e| UdfError::ExecutionError(format!("Failed to parse JSON: {}", e)))?;

        // 转换 JSON value 到 PropertyValue
        json_to_property_value(&json_value)
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature {
            params: vec![ParamType::String],
            return_type: ReturnType::Map,
            description: "Parse JSON string to map".to_string(),
        }
    }
}

/// 正则提取
struct RegexExtractUdf;

#[async_trait]
impl UserDefinedFunction for RegexExtractUdf {
    fn name(&self) -> &str {
        "regex_extract"
    }

    async fn execute(&self, args: Vec<PropertyValue>) -> Result<PropertyValue, UdfError> {
        if args.len() != 2 {
            return Err(UdfError::InvalidArgCount {
                expected: 2,
                actual: args.len(),
            });
        }

        let input = match &args[0] {
            PropertyValue::String(s) => s,
            _ => return Err(UdfError::InvalidArgType("Expected string".to_string())),
        };

        let pattern = match &args[1] {
            PropertyValue::String(s) => s,
            _ => return Err(UdfError::InvalidArgType("Expected string".to_string())),
        };

        let re = regex::Regex::new(pattern)
            .map_err(|e| UdfError::ExecutionError(format!("Invalid regex pattern: {}", e)))?;

        match re.captures(input) {
            Some(caps) => {
                if let Some(m) = caps.get(1) {
                    Ok(PropertyValue::String(m.as_str().to_string()))
                } else if let Some(m) = caps.get(0) {
                    Ok(PropertyValue::String(m.as_str().to_string()))
                } else {
                    Ok(PropertyValue::Null)
                }
            }
            None => Ok(PropertyValue::Null),
        }
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature {
            params: vec![ParamType::String, ParamType::String],
            return_type: ReturnType::String,
            description: "Extract substring using regex pattern".to_string(),
        }
    }
}

/// Base64 解码
struct Base64DecodeUdf;

#[async_trait]
impl UserDefinedFunction for Base64DecodeUdf {
    fn name(&self) -> &str {
        "base64_decode"
    }

    async fn execute(&self, args: Vec<PropertyValue>) -> Result<PropertyValue, UdfError> {
        if args.len() != 1 {
            return Err(UdfError::InvalidArgCount {
                expected: 1,
                actual: args.len(),
            });
        }

        let input = match &args[0] {
            PropertyValue::String(s) => s,
            _ => return Err(UdfError::InvalidArgType("Expected string".to_string())),
        };

        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(input)
            .map_err(|e| UdfError::ExecutionError(format!("Base64 decode error: {}", e)))?;

        let decoded_str = String::from_utf8(decoded)
            .map_err(|e| UdfError::ExecutionError(format!("Invalid UTF-8: {}", e)))?;

        Ok(PropertyValue::String(decoded_str))
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature {
            params: vec![ParamType::String],
            return_type: ReturnType::String,
            description: "Decode base64 string".to_string(),
        }
    }
}

/// 时间戳解析
struct TimestampParseUdf;

#[async_trait]
impl UserDefinedFunction for TimestampParseUdf {
    fn name(&self) -> &str {
        "timestamp_parse"
    }

    async fn execute(&self, args: Vec<PropertyValue>) -> Result<PropertyValue, UdfError> {
        if args.is_empty() || args.len() > 2 {
            return Err(UdfError::InvalidArgCount {
                expected: 1,
                actual: args.len(),
            });
        }

        let input = match &args[0] {
            PropertyValue::String(s) => s,
            PropertyValue::Integer(i) => {
                // Unix timestamp (seconds)
                use chrono::{TimeZone, Utc};
                let dt = Utc.timestamp_opt(*i, 0).single().ok_or_else(|| {
                    UdfError::ExecutionError("Invalid unix timestamp".to_string())
                })?;
                return Ok(PropertyValue::String(dt.to_rfc3339()));
            }
            _ => {
                return Err(UdfError::InvalidArgType(
                    "Expected string or integer".to_string(),
                ))
            }
        };

        // 尝试解析 ISO 8601
        use chrono::DateTime;
        match DateTime::parse_from_rfc3339(input) {
            Ok(dt) => Ok(PropertyValue::String(dt.to_rfc3339())),
            Err(_) => Err(UdfError::ExecutionError(format!(
                "Failed to parse timestamp: {}",
                input
            ))),
        }
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature {
            params: vec![ParamType::Any],
            return_type: ReturnType::String,
            description: "Parse timestamp (ISO 8601 or Unix)".to_string(),
        }
    }
}

/// 邮箱验证
struct ValidateEmailUdf;

#[async_trait]
impl UserDefinedFunction for ValidateEmailUdf {
    fn name(&self) -> &str {
        "validate_email"
    }

    async fn execute(&self, args: Vec<PropertyValue>) -> Result<PropertyValue, UdfError> {
        if args.len() != 1 {
            return Err(UdfError::InvalidArgCount {
                expected: 1,
                actual: args.len(),
            });
        }

        let input = match &args[0] {
            PropertyValue::String(s) => s,
            _ => return Err(UdfError::InvalidArgType("Expected string".to_string())),
        };

        let email_regex =
            regex::Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap();

        Ok(PropertyValue::Boolean(email_regex.is_match(input)))
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature {
            params: vec![ParamType::String],
            return_type: ReturnType::Boolean,
            description: "Validate email address".to_string(),
        }
    }
}

/// 转大写
struct ToUppercaseUdf;

#[async_trait]
impl UserDefinedFunction for ToUppercaseUdf {
    fn name(&self) -> &str {
        "to_uppercase"
    }

    async fn execute(&self, args: Vec<PropertyValue>) -> Result<PropertyValue, UdfError> {
        if args.len() != 1 {
            return Err(UdfError::InvalidArgCount {
                expected: 1,
                actual: args.len(),
            });
        }

        let input = match &args[0] {
            PropertyValue::String(s) => s,
            _ => return Err(UdfError::InvalidArgType("Expected string".to_string())),
        };

        Ok(PropertyValue::String(input.to_uppercase()))
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature {
            params: vec![ParamType::String],
            return_type: ReturnType::String,
            description: "Convert string to uppercase".to_string(),
        }
    }
}

/// 转小写
struct ToLowercaseUdf;

#[async_trait]
impl UserDefinedFunction for ToLowercaseUdf {
    fn name(&self) -> &str {
        "to_lowercase"
    }

    async fn execute(&self, args: Vec<PropertyValue>) -> Result<PropertyValue, UdfError> {
        if args.len() != 1 {
            return Err(UdfError::InvalidArgCount {
                expected: 1,
                actual: args.len(),
            });
        }

        let input = match &args[0] {
            PropertyValue::String(s) => s,
            _ => return Err(UdfError::InvalidArgType("Expected string".to_string())),
        };

        Ok(PropertyValue::String(input.to_lowercase()))
    }

    fn signature(&self) -> FunctionSignature {
        FunctionSignature {
            params: vec![ParamType::String],
            return_type: ReturnType::String,
            description: "Convert string to lowercase".to_string(),
        }
    }
}

// 辅助函数：将 JSON value 转换为 PropertyValue
fn json_to_property_value(value: &serde_json::Value) -> Result<PropertyValue, UdfError> {
    match value {
        serde_json::Value::Null => Ok(PropertyValue::Null),
        serde_json::Value::Bool(b) => Ok(PropertyValue::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(PropertyValue::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(PropertyValue::Float(f))
            } else {
                Err(UdfError::ExecutionError("Invalid number".to_string()))
            }
        }
        serde_json::Value::String(s) => Ok(PropertyValue::String(s.clone())),
        serde_json::Value::Array(arr) => {
            let values: Result<Vec<_>, _> = arr.iter().map(json_to_property_value).collect();
            Ok(PropertyValue::List(values?))
        }
        serde_json::Value::Object(obj) => {
            let mut map = BTreeMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), json_to_property_value(v)?);
            }
            Ok(PropertyValue::Map(map))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_temp() {
        let udf = ParseTempUdf;
        let result = udf
            .execute(vec![PropertyValue::String("25.3C".to_string())])
            .await
            .unwrap();

        match result {
            PropertyValue::Float(f) => assert!((f - 25.3).abs() < 0.001),
            _ => panic!("Expected float"),
        }
    }

    #[tokio::test]
    async fn test_parse_geo() {
        let udf = ParseGeoUdf;
        let result = udf
            .execute(vec![PropertyValue::String("40.7128,-74.0060".to_string())])
            .await
            .unwrap();

        match result {
            PropertyValue::Map(map) => {
                assert!(map.contains_key("lat"));
                assert!(map.contains_key("lon"));
            }
            _ => panic!("Expected map"),
        }
    }

    #[tokio::test]
    async fn test_fahrenheit_to_celsius() {
        let udf = FahrenheitToCelsiusUdf;
        let result = udf.execute(vec![PropertyValue::Float(32.0)]).await.unwrap();

        match result {
            PropertyValue::Float(f) => assert!(f.abs() < 0.001), // 32F = 0C
            _ => panic!("Expected float"),
        }
    }

    #[tokio::test]
    async fn test_registry() {
        let registry = UdfRegistry::new();
        registry.register_builtin();

        assert!(registry.exists("parse_temp"));
        assert!(registry.exists("parse_geo"));
        assert!(registry.exists("fahrenheit_to_celsius"));

        let result = registry
            .call(
                "parse_temp",
                vec![PropertyValue::String("25.3C".to_string())],
            )
            .await
            .unwrap();

        assert!(matches!(result, PropertyValue::Float(_)));
    }

    #[tokio::test]
    async fn test_udf_not_found() {
        let registry = UdfRegistry::new();
        let result = registry.call("nonexistent", vec![]).await;

        assert!(matches!(result, Err(UdfError::NotFound(_))));
    }
}
