//! Error mapping: Nexora errors → PostgreSQL ErrorResponse fields.

use pgwire::error::{ErrorInfo, PgWireError};

pub const SQLSTATE_SYNTAX_ERROR: &str = "42601";
pub const SQLSTATE_UNDEFINED_TABLE: &str = "42P01";
pub const SQLSTATE_DATATYPE_MISMATCH: &str = "42804";
pub const SQLSTATE_FEATURE_NOT_SUPPORTED: &str = "0A000";
pub const SQLSTATE_UNIQUE_VIOLATION: &str = "23505";
pub const SQLSTATE_TOO_MANY_CONNECTIONS: &str = "53300";
pub const SQLSTATE_SYSTEM_ERROR: &str = "58000";

pub fn classify_sql_error(error_msg: &str) -> &'static str {
    let lower = error_msg.to_lowercase();
    if lower.contains("parse error") || lower.contains("syntax") {
        SQLSTATE_SYNTAX_ERROR
    } else if lower.contains("not found") {
        SQLSTATE_UNDEFINED_TABLE
    } else if lower.contains("not supported") || lower.contains("unsupported") {
        SQLSTATE_FEATURE_NOT_SUPPORTED
    } else if lower.contains("duplicate") || lower.contains("unique") {
        SQLSTATE_UNIQUE_VIOLATION
    } else {
        SQLSTATE_SYSTEM_ERROR
    }
}

pub fn build_sql_error(error_msg: &str, _hint: Option<&str>) -> PgWireError {
    let sqlstate = classify_sql_error(error_msg);
    let info = ErrorInfo::new(
        "ERROR".to_string(),
        sqlstate.to_string(),
        error_msg.to_string(),
    );
    PgWireError::UserError(Box::new(info))
}

pub fn build_too_many_connections_error(max: usize) -> PgWireError {
    let info = ErrorInfo::new(
        "FATAL".to_string(),
        SQLSTATE_TOO_MANY_CONNECTIONS.to_string(),
        format!("too many connections — max is {}", max),
    );
    PgWireError::UserError(Box::new(info))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_classify_syntax_error() {
        assert_eq!(
            classify_sql_error("parse error: unexpected token"),
            SQLSTATE_SYNTAX_ERROR
        );
    }
    #[test]
    fn test_classify_feature_not_supported() {
        assert_eq!(
            classify_sql_error("unsupported: JOIN not supported"),
            SQLSTATE_FEATURE_NOT_SUPPORTED
        );
    }
    #[test]
    fn test_classify_system_error() {
        assert_eq!(
            classify_sql_error("something went wrong"),
            SQLSTATE_SYSTEM_ERROR
        );
    }
}
