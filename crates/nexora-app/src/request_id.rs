//! Request ID middleware — generates a unique ID for each request
//! and injects it into the response header and tracing span.
#![allow(dead_code)]

use axum::{extract::Request, http::HeaderValue, middleware::Next, response::Response};
use std::sync::OnceLock;
use uuid::Uuid;

static REQUEST_ID_HEADER: &str = "X-Request-Id";

// Thread-local storage for the current request ID.
// Used by handlers to include the request ID in error responses.
thread_local! {
    static CURRENT_REQUEST_ID: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

static HEADER_NAME: OnceLock<axum::http::HeaderName> = OnceLock::new();

/// Get the header name for the request ID.
pub fn header_name() -> &'static axum::http::HeaderName {
    HEADER_NAME.get_or_init(|| axum::http::HeaderName::from_static("x-request-id"))
}

/// Middleware: generate or extract a request ID for each request.
pub async fn request_id_middleware(mut req: Request, next: Next) -> Response {
    // Check if the client provided a request ID
    let request_id = req
        .headers()
        .get(header_name())
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    // Set the request ID in thread-local for handlers
    CURRENT_REQUEST_ID.with(|cell| {
        *cell.borrow_mut() = Some(request_id.clone());
    });

    // Inject into tracing span
    let span = tracing::info_span!("request", request_id = %request_id);

    // Add the request ID to the request headers for downstream handlers
    if let Ok(val) = HeaderValue::from_str(&request_id) {
        req.headers_mut().insert(header_name(), val);
    }

    let mut response = span.in_scope(|| next.run(req)).await;

    // Add the request ID to the response headers
    if let Ok(val) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(header_name(), val);
    }

    // Clear thread-local
    CURRENT_REQUEST_ID.with(|cell| {
        *cell.borrow_mut() = None;
    });

    response
}

/// Get the current request ID (from thread-local storage).
pub fn current_request_id() -> Option<String> {
    CURRENT_REQUEST_ID.with(|cell| cell.borrow().clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_request_id() {
        assert!(current_request_id().is_none());

        CURRENT_REQUEST_ID.with(|cell| {
            *cell.borrow_mut() = Some("test-123".to_string());
        });

        assert_eq!(current_request_id(), Some("test-123".to_string()));
    }
}
