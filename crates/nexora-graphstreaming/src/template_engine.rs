//! Template engine for variable interpolation in projection rules
//!
//! Supports Handlebars-style templates: `{{variable}}`, `{{data.nested.field}}`

use crate::{GraphStreamingError, Result};
use handlebars::Handlebars;
use serde_json::Value;
use std::collections::HashMap;

/// Template engine for rendering projection rule templates
pub struct TemplateEngine {
    handlebars: Handlebars<'static>,
}

impl TemplateEngine {
    /// Create a new template engine
    pub fn new() -> Self {
        let mut handlebars = Handlebars::new();
        // Strict mode: error on missing variables
        handlebars.set_strict_mode(true);
        // Disable HTML escaping
        handlebars.register_escape_fn(handlebars::no_escape);

        Self { handlebars }
    }

    /// Render a template string with context
    ///
    /// # Examples
    ///
    /// ```
    /// use nexora_graphstreaming::TemplateEngine;
    /// use serde_json::json;
    ///
    /// let engine = TemplateEngine::new();
    /// let context = json!({"cargo_id": "CARGO-123", "status": "IN_TRANSIT"});
    /// let result = engine.render("{{cargo_id}}", &context).unwrap();
    /// assert_eq!(result, "CARGO-123");
    /// ```
    pub fn render(&self, template: &str, context: &Value) -> Result<String> {
        self.handlebars
            .render_template(template, context)
            .map_err(|e| GraphStreamingError::TemplateError(e.to_string()))
    }

    /// Render a map of templates
    pub fn render_map(
        &self,
        templates: &HashMap<String, String>,
        context: &Value,
    ) -> Result<HashMap<String, String>> {
        templates
            .iter()
            .map(|(key, template)| {
                let rendered = self.render(template, context)?;
                Ok((key.clone(), rendered))
            })
            .collect()
    }

    /// Extract variables from event payload into a context object
    ///
    /// Converts RawEvent payload (JSON string) into a Value for template rendering.
    pub fn extract_context(event_payload: &str) -> Result<Value> {
        serde_json::from_str(event_payload)
            .map_err(|e| GraphStreamingError::ParseError(format!("Invalid JSON payload: {}", e)))
    }
}

impl Default for TemplateEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_simple_variable() {
        let engine = TemplateEngine::new();
        let context = json!({"name": "Alice"});
        let result = engine.render("{{name}}", &context).unwrap();
        assert_eq!(result, "Alice");
    }

    #[test]
    fn test_nested_variable() {
        let engine = TemplateEngine::new();
        let context = json!({
            "user": {
                "profile": {
                    "name": "Bob"
                }
            }
        });
        let result = engine.render("{{user.profile.name}}", &context).unwrap();
        assert_eq!(result, "Bob");
    }

    #[test]
    fn test_missing_variable() {
        let engine = TemplateEngine::new();
        let context = json!({"name": "Alice"});
        let result = engine.render("{{missing}}", &context);
        assert!(result.is_err());
    }

    #[test]
    fn test_render_map() {
        let engine = TemplateEngine::new();
        let context = json!({"cargo_id": "CARGO-123", "status": "DELIVERED"});

        let mut templates = HashMap::new();
        templates.insert("id".to_string(), "{{cargo_id}}".to_string());
        templates.insert("state".to_string(), "{{status}}".to_string());

        let result = engine.render_map(&templates, &context).unwrap();

        assert_eq!(result.get("id").unwrap(), "CARGO-123");
        assert_eq!(result.get("state").unwrap(), "DELIVERED");
    }

    #[test]
    fn test_extract_context() {
        let payload = r#"{"cargo_id": "CARGO-999", "temperature": 28}"#;
        let context = TemplateEngine::extract_context(payload).unwrap();

        assert_eq!(context["cargo_id"], "CARGO-999");
        assert_eq!(context["temperature"], 28);
    }

    #[test]
    fn test_extract_context_invalid_json() {
        let payload = "not valid json";
        let result = TemplateEngine::extract_context(payload);
        assert!(result.is_err());
    }

    #[test]
    fn test_number_rendering() {
        let engine = TemplateEngine::new();
        let context = json!({"count": 42, "price": 19.99});

        assert_eq!(engine.render("{{count}}", &context).unwrap(), "42");
        assert_eq!(engine.render("{{price}}", &context).unwrap(), "19.99");
    }

    #[test]
    fn test_boolean_rendering() {
        let engine = TemplateEngine::new();
        let context = json!({"active": true, "deleted": false});

        assert_eq!(engine.render("{{active}}", &context).unwrap(), "true");
        assert_eq!(engine.render("{{deleted}}", &context).unwrap(), "false");
    }
}
