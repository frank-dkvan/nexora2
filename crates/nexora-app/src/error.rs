//! Unified error response system with structured error codes.
//!
//! All API errors follow the format:
//! ```json
//! {
//!   "error": "Human-readable message",
//!   "code": "ERROR_CODE",
//!   "details": { ... },
//!   "request_id": "uuid"
//! }
//! ```
#![allow(dead_code)]

use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

/// Structured error codes for programmatic error handling.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    // Client errors (4xx)
    InvalidQid,
    InvalidInput,
    NodeNotFound,
    NotFound,
    AlreadyExists,
    Conflict,
    AuthFailed,
    AuthMissing,
    Forbidden,
    RateLimited,
    PayloadTooLarge,
    UnsupportedMediaType,

    // Cypher/SQL errors
    CypherSyntaxError,
    CypherSemanticError,
    CypherExecutionError,
    SqlError,

    // Resource errors
    IngestError,
    StreamError,
    StorageError,
    UdfError,
    RecipeError,
    MaterializedViewError,
    FeatureNotEnabled,

    // Server errors (5xx)
    InternalError,
    ServiceUnavailable,
    Timeout,
}

impl ErrorCode {
    /// Get the HTTP status code for this error.
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidQid | Self::InvalidInput => StatusCode::BAD_REQUEST,
            Self::NodeNotFound | Self::NotFound => StatusCode::NOT_FOUND,
            Self::AlreadyExists | Self::Conflict => StatusCode::CONFLICT,
            Self::AuthFailed | Self::AuthMissing => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::Timeout => StatusCode::REQUEST_TIMEOUT,
            Self::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::CypherSyntaxError
            | Self::CypherSemanticError
            | Self::CypherExecutionError
            | Self::SqlError
            | Self::IngestError
            | Self::StreamError
            | Self::StorageError
            | Self::UdfError
            | Self::RecipeError
            | Self::MaterializedViewError
            | Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            Self::FeatureNotEnabled => StatusCode::NOT_IMPLEMENTED,
        }
    }
}

/// Unified API error response.
#[derive(Debug, Serialize)]
pub struct ApiError {
    pub error: String,
    pub code: ErrorCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl ApiError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code,
            details: None,
            request_id: None,
        }
    }

    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    pub fn with_request_id(mut self, id: String) -> Self {
        self.request_id = Some(id);
        self
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let status = self.code.status_code();
        (status, Json(json!(self))).into_response()
    }
}

/// Convenience constructors.
impl ApiError {
    pub fn invalid_qid(qid: &str) -> Self {
        Self::new(ErrorCode::InvalidQid, format!("Invalid node ID: {qid}"))
    }

    pub fn not_found(resource: &str, id: &str) -> Self {
        Self::new(ErrorCode::NotFound, format!("{resource} '{id}' not found"))
    }

    pub fn already_exists(resource: &str, id: &str) -> Self {
        Self::new(
            ErrorCode::AlreadyExists,
            format!("{resource} '{id}' already exists"),
        )
    }

    pub fn auth_missing() -> Self {
        Self::new(
            ErrorCode::AuthMissing,
            "Missing or invalid authentication token",
        )
    }

    pub fn forbidden() -> Self {
        Self::new(
            ErrorCode::Forbidden,
            "Insufficient permissions for this operation",
        )
    }

    pub fn rate_limited() -> Self {
        Self::new(
            ErrorCode::RateLimited,
            "Rate limit exceeded. Please slow down your requests.",
        )
    }

    pub fn internal() -> Self {
        Self::new(ErrorCode::InternalError, "Internal server error")
    }

    pub fn feature_not_enabled(feature: String) -> Self {
        Self::new(
            ErrorCode::FeatureNotEnabled,
            format!("Feature '{}' is not enabled. Compile with --features {} and enable at runtime.", feature, feature),
        )
    }

    #[allow(dead_code)]
    pub fn Internal(msg: String) -> Self {
        Self::new(ErrorCode::InternalError, msg)
    }

    #[allow(non_snake_case)]
    pub fn FeatureNotEnabled(feature: String) -> Self {
        Self::feature_not_enabled(feature)
    }
}

/// Generate a new request ID.
pub fn generate_request_id() -> String {
    Uuid::new_v4().to_string()
}

/// Sanitize an internal error message for client consumption.
/// Strips file paths, internal addresses, and implementation details.
pub fn sanitize_error_message(msg: &str) -> String {
    // Remove file system paths (Unix and Windows)
    let no_paths = regex::PathReplacer::replace_all(msg);
    // Remove IP addresses and ports

    regex::AddrReplacer::replace_all(&no_paths)
}

/// Helper to create a JSON error response tuple (for handlers that return tuples).
pub fn error_response(
    code: ErrorCode,
    message: impl Into<String>,
    request_id: Option<&str>,
) -> (StatusCode, Json<serde_json::Value>) {
    let err = ApiError::new(code, message);
    let err = if let Some(id) = request_id {
        err.with_request_id(id.to_string())
    } else {
        err
    };
    (err.code.status_code(), Json(json!(err)))
}

/// Helper to create a JSON error response with details.
pub fn error_response_with_details(
    code: ErrorCode,
    message: impl Into<String>,
    details: serde_json::Value,
    request_id: Option<&str>,
) -> (StatusCode, Json<serde_json::Value>) {
    let err = ApiError::new(code, message).with_details(details);
    let err = if let Some(id) = request_id {
        err.with_request_id(id.to_string())
    } else {
        err
    };
    (err.code.status_code(), Json(json!(err)))
}

/// Friendlify Cypher parser error messages.
/// Converts internal parser errors to user-friendly messages with suggestions.
pub fn friendlify_cypher_error(error: &str) -> (String, ErrorCode) {
    // Common parser errors → user-friendly messages
    let msg = error;

    // Unexpected token errors
    if msg.contains("Unexpected token") {
        let friendly = extract_token_error(msg);
        return (friendly, ErrorCode::CypherSyntaxError);
    }

    // Missing clause
    if msg.contains("expected") && msg.contains("RETURN") {
        return (
            "Query is missing a RETURN clause. Add RETURN at the end of your query.".to_string(),
            ErrorCode::CypherSyntaxError,
        );
    }

    // Node not found in graph
    if msg.contains("NodeNotFound") || msg.contains("node not found") {
        return (
            "Referenced node does not exist in the graph.".to_string(),
            ErrorCode::NodeNotFound,
        );
    }

    // Timeout
    if msg.contains("timeout") || msg.contains("Timeout") {
        return (
            "Query execution timed out. Try simplifying the query or adding limits.".to_string(),
            ErrorCode::Timeout,
        );
    }

    // Default: sanitize and return
    let sanitized = sanitize_error_message(msg);
    (sanitized, ErrorCode::CypherExecutionError)
}

/// Extract a user-friendly message from parser "Unexpected token" errors.
fn extract_token_error(msg: &str) -> String {
    // Try to extract the unexpected text
    if let Some(start) = msg.find("text: \"") {
        let rest = &msg[start + 7..];
        if let Some(end) = rest.find('"') {
            let token = &rest[..end];
            // Provide suggestions for common typos
            let suggestion = suggest_keyword(token);
            if let Some(sug) = suggestion {
                return format!("Syntax error near '{token}'. Did you mean '{sug}'?");
            }
            return format!("Syntax error near '{token}'. Check your Cypher syntax.");
        }
    }
    "Syntax error in Cypher query. Check your syntax.".to_string()
}

/// Suggest a keyword based on partial input.
fn suggest_keyword(token: &str) -> Option<&'static str> {
    let upper = token.to_uppercase();
    let keywords = [
        "MATCH", "WHERE", "RETURN", "CREATE", "SET", "DELETE", "MERGE", "REMOVE", "WITH", "UNWIND",
        "ORDER", "BY", "LIMIT", "SKIP", "DISTINCT", "AS", "AND", "OR", "NOT", "IN", "IS", "NULL",
        "UNION", "CALL", "LOAD", "CSV", "CASE", "WHEN", "THEN", "ELSE", "END", "EXISTS", "COUNT",
        "SUM", "AVG", "MIN", "MAX", "COLLECT", "DETACH", "ON", "ASC", "DESC",
    ];

    // Find closest keyword by edit distance
    let mut best: Option<(&'static str, usize)> = None;
    for kw in &keywords {
        let dist = levenshtein(&upper, kw);
        // Only suggest if distance is small relative to word length
        let threshold = (kw.len() / 3).max(1);
        if dist <= threshold && (best.is_none() || dist < best.unwrap().1) {
            best = Some((*kw, dist));
        }
    }
    best.map(|(kw, _)| kw)
}

/// Simple Levenshtein distance for keyword suggestions.
fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut curr: Vec<usize> = vec![0; b.len() + 1];

    for i in 1..=a.len() {
        curr[0] = i;
        for j in 1..=b.len() {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b.len()]
}

/// Regex helpers for sanitizing error messages.
mod regex {
    use std::sync::OnceLock;

    static PATH_RE: OnceLock<regex_lite::Regex> = OnceLock::new();
    static ADDR_RE: OnceLock<regex_lite::Regex> = OnceLock::new();

    pub struct PathReplacer;
    impl PathReplacer {
        pub fn replace_all(msg: &str) -> String {
            let re = PATH_RE.get_or_init(|| {
                regex_lite::Regex::new(r"(/[\w./-]+|[A-Za-z]:\\[\w\\.-]+)").unwrap()
            });
            re.replace_all(msg, "<path>").to_string()
        }
    }

    pub struct AddrReplacer;
    impl AddrReplacer {
        pub fn replace_all(msg: &str) -> String {
            let re = ADDR_RE.get_or_init(|| {
                regex_lite::Regex::new(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+").unwrap()
            });
            re.replace_all(msg, "<addr>").to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_status_mapping() {
        assert_eq!(ErrorCode::NotFound.status_code(), StatusCode::NOT_FOUND);
        assert_eq!(
            ErrorCode::AuthFailed.status_code(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            ErrorCode::RateLimited.status_code(),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            ErrorCode::InternalError.status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn test_suggest_keyword() {
        assert_eq!(suggest_keyword("MATC"), Some("MATCH"));
        assert_eq!(suggest_keyword("RETUR"), Some("RETURN"));
        assert_eq!(suggest_keyword("WHER"), Some("WHERE"));
        assert_eq!(suggest_keyword("CREAT"), Some("CREATE"));
    }

    #[test]
    fn test_friendlify_cypher_error() {
        let (msg, code) = friendlify_cypher_error("Unexpected token: Token { text: \"MATC\" }");
        assert_eq!(code, ErrorCode::CypherSyntaxError);
        assert!(msg.contains("MATC"));
        assert!(msg.contains("MATCH"));
    }

    #[test]
    fn test_sanitize_error_message() {
        let msg = "Failed to open /home/user/data/secret.db";
        let sanitized = sanitize_error_message(msg);
        assert!(!sanitized.contains("/home/user/data/secret.db"));
        assert!(sanitized.contains("<path>"));
    }
}
