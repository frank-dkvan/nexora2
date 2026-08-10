//! P1-4: Rate limiting middleware for Axum
//!
//! Integrates the token bucket rate limiter as Axum middleware to protect API endpoints.

use axum::{
    extract::{ConnectInfo, Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use nexora_common::{RateLimitError, RateLimiter};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

/// Axum middleware that enforces rate limiting per client IP
pub async fn rate_limit_middleware(
    State(limiter): State<Arc<RateLimiter>>,
    request: Request,
    next: Next,
) -> Response {
    // Extract client IP from ConnectInfo, fallback to 0.0.0.0 if not available
    let client_ip = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip())
        .unwrap_or_else(|| IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));

    match limiter.check_rate_limit(client_ip).await {
        Ok(()) => next.run(request).await,
        Err(e) => rate_limit_error_response(e),
    }
}

fn rate_limit_error_response(error: RateLimitError) -> Response {
    let (status, message) = match error {
        RateLimitError::GlobalLimitExceeded => (
            StatusCode::SERVICE_UNAVAILABLE,
            "Global rate limit exceeded. Please try again later.".to_string(),
        ),
        RateLimitError::ClientLimitExceeded(ip) => (
            StatusCode::TOO_MANY_REQUESTS,
            format!("Rate limit exceeded for {}. Please slow down.", ip),
        ),
    };

    (status, message).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, routing::get, Router};
    use nexora_common::RateLimiterConfig;
    use std::time::Duration;
    use tower::ServiceExt;

    async fn test_handler() -> &'static str {
        "OK"
    }

    #[tokio::test]
    async fn test_rate_limit_middleware_allows_under_limit() {
        let config = RateLimiterConfig {
            global_limit: 100_000,
            per_client_limit: 5,
            refill_interval: Duration::from_millis(100),
        };

        let limiter = Arc::new(RateLimiter::new(config));

        let app = Router::new().route("/test", get(test_handler)).layer(
            axum::middleware::from_fn_with_state(limiter.clone(), rate_limit_middleware),
        );

        // First request should succeed
        let response = app
            .clone()
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
