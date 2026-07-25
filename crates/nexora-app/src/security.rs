//! Production security hardening: rate limiting, audit logging, security headers.
//!
//! ## Rate Limiter
//! Token bucket per client IP. Configurable rate (req/s) and burst capacity.
//! Inactive entries are cleaned up periodically to bound memory.
//!
//! ## Audit Logger
//! Structured JSON audit log for all mutating operations (POST, PUT, DELETE).
//! Includes timestamp, client IP, user identity, HTTP method, path, and status.
//!
//! ## Security Headers
//! Standard HTTP security headers to harden the server against common web attacks.

use axum::{
    extract::{ConnectInfo, Request},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde_json::json;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};

// =========================================================================
// Rate Limiter — Token Bucket per IP
// =========================================================================

/// Configuration for the rate limiter.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Tokens replenished per second.
    pub rate: f64,
    /// Maximum burst capacity (tokens).
    pub burst: usize,
    /// How long to keep an entry after last access before cleanup.
    pub ttl: Duration,
    /// How often to run cleanup.
    pub cleanup_interval: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            rate: 100.0,
            burst: 200,
            ttl: Duration::from_secs(300),
            cleanup_interval: Duration::from_secs(60),
        }
    }
}

/// A token bucket for a single client.
#[derive(Debug, Clone)]
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
    capacity: f64,
    rate: f64,
}

impl TokenBucket {
    fn new(capacity: f64, rate: f64) -> Self {
        Self {
            tokens: capacity,
            last_refill: Instant::now(),
            capacity,
            rate,
        }
    }

    /// Attempt to consume one token. Returns true if allowed.
    fn try_consume(&mut self, now: Instant) -> bool {
        // Refill tokens based on elapsed time
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Thread-safe rate limiter state.
///
/// Buckets are keyed by client **IP** (`IpAddr`), not `SocketAddr`. Keying on
/// the full socket address (IP + ephemeral source port) let a single client
/// bypass the limit entirely by opening a fresh connection per request — each
/// got a new port, hence a new bucket — and let an attacker grow the map with
/// distinct ports. Per-IP keying makes the limit actually bind.
pub struct RateLimiter {
    buckets: RwLock<HashMap<std::net::IpAddr, (TokenBucket, Instant)>>,
    config: RateLimitConfig,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Arc<Self> {
        let limiter = Arc::new(Self {
            buckets: RwLock::new(HashMap::new()),
            config,
        });

        // Spawn periodic cleanup
        {
            let limiter = limiter.clone();
            let interval = limiter.config.cleanup_interval;
            let ttl = limiter.config.ttl;
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(interval);
                // Skip the first immediate tick
                tick.tick().await;
                loop {
                    tick.tick().await;
                    limiter.cleanup_expired(ttl).await;
                }
            });
        }

        limiter
    }

    /// Check if a request from `addr` should be allowed. Keyed by IP so a
    /// client cannot escape the limit by using a new source port per request.
    pub async fn check(&self, addr: SocketAddr) -> bool {
        let now = Instant::now();
        let mut buckets = self.buckets.write().await;

        let entry = buckets.entry(addr.ip()).or_insert_with(|| {
            (
                TokenBucket::new(self.config.burst as f64, self.config.rate),
                now,
            )
        });

        let allowed = entry.0.try_consume(now);
        entry.1 = now; // update last access time
        allowed
    }

    /// Remove entries that haven't been accessed for `ttl`.
    async fn cleanup_expired(&self, ttl: Duration) {
        let now = Instant::now();
        let mut buckets = self.buckets.write().await;
        buckets.retain(|_, (_, last_access)| now.duration_since(*last_access) < ttl);
    }
}

/// Axum middleware: rate limit by client IP.
pub async fn rate_limit(
    limiter: axum::extract::Extension<Arc<RateLimiter>>,
    req: Request,
    next: Next,
) -> Result<Response, Response> {
    // Skip rate limiting for public endpoints
    let path = req.uri().path();
    if crate::auth::PUBLIC_PATHS.contains(&path) {
        return Ok(next.run(req).await);
    }

    let addr = get_client_ip(&req);

    if limiter.check(addr).await {
        Ok(next.run(req).await)
    } else {
        tracing::warn!(
            target: "rate_limit",
            client = %addr,
            path = %path,
            "Rate limit exceeded"
        );
        Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({
                "error": "Rate limit exceeded. Please slow down your requests.",
                "code": "RATE_LIMITED"
            })),
        )
            .into_response())
    }
}

// =========================================================================
// Audit Logger — structured JSON audit trail
// =========================================================================

/// Global persistent audit sink. When set, audit entries are appended to a
/// file (as newline-delimited JSON) in addition to being emitted via `tracing`.
static AUDIT_SINK: OnceLock<AuditSink> = OnceLock::new();

/// A persistent, non-blocking audit sink.
///
/// Entries are handed to a background task over a bounded channel, so request
/// handling never blocks on disk I/O. If the channel is full (writer falling
/// behind), the entry is dropped from the file sink but still logged via
/// `tracing`, so no request is ever stalled by audit persistence.
#[derive(Clone)]
pub struct AuditSink {
    tx: mpsc::Sender<String>,
}

impl AuditSink {
    /// Create a sink that appends newline-delimited JSON to `path`, spawning a
    /// background writer task. The file is created if missing and opened in
    /// append mode so existing history is preserved across restarts.
    pub fn new(path: std::path::PathBuf) -> std::io::Result<Self> {
        use std::io::Write;

        // Open once up-front so configuration errors surface at startup.
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        let (tx, mut rx) = mpsc::channel::<String>(4096);

        tokio::task::spawn_blocking(move || {
            // Drain the channel synchronously on a blocking thread. Each line is
            // a complete JSON record terminated by a newline.
            while let Some(line) = rx.blocking_recv() {
                if let Err(e) = writeln!(file, "{line}") {
                    tracing::warn!(target: "audit", "audit sink write failed: {e}");
                    continue;
                }
                // Flush per record so entries are durable promptly. Audit volume
                // is low relative to request throughput, so this is acceptable.
                if let Err(e) = file.flush() {
                    tracing::warn!(target: "audit", "audit sink flush failed: {e}");
                }
            }
        });

        Ok(Self { tx })
    }

    /// Enqueue a serialized entry for persistence. Non-blocking; drops the entry
    /// (with a warning) if the writer has fallen too far behind.
    fn write(&self, line: String) {
        if let Err(e) = self.tx.try_send(line) {
            tracing::warn!(target: "audit", "audit sink backpressure, dropped entry: {e}");
        }
    }
}

/// Install the global persistent audit sink. Returns an error if a sink is
/// already installed or the file cannot be opened. Call once at startup.
pub fn init_audit_sink(path: std::path::PathBuf) -> std::io::Result<()> {
    let sink = AuditSink::new(path)?;
    AUDIT_SINK.set(sink).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "audit sink already initialized",
        )
    })
}

/// Audit log entry for a single request.
#[derive(Debug, serde::Serialize)]
struct AuditEntry {
    timestamp: String,
    client_ip: String,
    method: String,
    path: String,
    status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_agent: Option<String>,
}

/// Axum middleware: log all mutating operations to the audit trail.
pub async fn audit_log(req: Request, next: Next) -> Response {
    let path = req.uri().path().to_string();
    let method = req.method().clone();
    let is_mutating = method == Method::POST
        || method == Method::PUT
        || method == Method::DELETE
        || method == Method::PATCH;
    let client_ip = get_client_ip(&req).to_string();
    let user_agent = req
        .headers()
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Prefer the signature-verified identity that require_auth put in
    // extensions. Only if it is absent (auth disabled, or a public path that
    // skipped validation) fall back to the unverified header, tagging it so the
    // audit trail never conflates a forged subject with a verified one.
    let user = req
        .extensions()
        .get::<crate::auth::AuthenticatedUser>()
        .map(|u| u.0.clone())
        .or_else(|| extract_unverified_user_from_request(&req).map(|s| format!("unverified:{s}")));

    let response = next.run(req).await;
    let status = response.status().as_u16();

    if is_mutating {
        let entry = AuditEntry {
            timestamp: chrono::Utc::now().to_rfc3339(),
            client_ip,
            method: method.to_string(),
            path,
            status,
            user,
            user_agent,
        };

        // Write structured audit log
        if let Ok(json) = serde_json::to_string(&entry) {
            tracing::info!(target: "audit", "{json}");
            // Also persist to the durable file sink, if configured.
            if let Some(sink) = AUDIT_SINK.get() {
                sink.write(json);
            }
        }
    }

    response
}

/// Best-effort extraction of the `sub` from the Authorization header WITHOUT
/// verifying the signature. Only used as an audit fallback when no verified
/// identity is available, and its result is always prefixed `unverified:` so it
/// can never be mistaken for a signature-checked subject.
fn extract_unverified_user_from_request(req: &Request) -> Option<String> {
    let header = req.headers().get("authorization")?.to_str().ok()?;
    let token = header.strip_prefix("Bearer ")?;
    // Try to decode the base64 payload (before the dot) to get user info
    let payload_b64 = token.split('.').next()?;
    let payload_bytes = BASE64.decode(payload_b64).ok()?;
    let payload: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;
    payload
        .get("sub")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

// =========================================================================
// Security Headers — harden HTTP responses
// =========================================================================

/// Axum middleware: add standard security headers to every response.
pub async fn security_headers(req: Request, next: Next) -> Response {
    let mut response = next.run(req).await;

    let headers = response.headers_mut();
    insert_security_headers(headers);

    response
}

/// Insert recommended security headers into a HeaderMap.
fn insert_security_headers(headers: &mut HeaderMap) {
    // Prevent MIME type sniffing
    headers.insert(
        "X-Content-Type-Options",
        HeaderValue::from_static("nosniff"),
    );

    // Prevent clickjacking
    headers.insert("X-Frame-Options", HeaderValue::from_static("DENY"));

    // Enable browser XSS filter
    headers.insert(
        "X-XSS-Protection",
        HeaderValue::from_static("1; mode=block"),
    );

    // Restrict referrer information
    headers.insert(
        "Referrer-Policy",
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );

    // Disable browser features (camera, microphone, etc.)
    headers.insert(
        "Permissions-Policy",
        HeaderValue::from_static("camera=(), microphone=(), geolocation=(), interest-cohort=()"),
    );

    // Content-Security-Policy (relaxed for dashboard CDN resources)
    headers.insert(
        "Content-Security-Policy",
        HeaderValue::from_static(
            "default-src 'self'; \
             script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdnjs.cloudflare.com; \
             style-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; \
             img-src 'self' data: blob:; \
             connect-src 'self' ws: wss:; \
             font-src 'self' data:;",
        ),
    );

    // HSTS: enforce HTTPS for 1 year (only meaningful when TLS is enabled)
    headers.insert(
        "Strict-Transport-Security",
        HeaderValue::from_static("max-age=31536000; includeSubDomains"),
    );
}

// =========================================================================
// Helpers
// =========================================================================

/// Extract the client IP from the request, falling back to 0.0.0.0.
fn get_client_ip(req: &Request) -> SocketAddr {
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0)
        .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)))
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_allows_within_rate() {
        let mut bucket = TokenBucket::new(5.0, 10.0);
        let now = Instant::now();

        // Should allow 5 immediate requests (burst)
        for _ in 0..5 {
            assert!(
                bucket.try_consume(now),
                "should allow within burst capacity"
            );
        }

        // 6th request should be denied (bucket empty)
        assert!(!bucket.try_consume(now), "should deny when bucket empty");
    }

    #[test]
    fn test_token_bucket_refills_over_time() {
        let mut bucket = TokenBucket::new(10.0, 10.0);
        let start = Instant::now();

        // Consume all tokens
        for _ in 0..10 {
            assert!(bucket.try_consume(start));
        }
        assert!(!bucket.try_consume(start));

        // Advance time by 1 second — should refill 10 tokens
        let later = start + Duration::from_secs(1);
        for _ in 0..10 {
            assert!(bucket.try_consume(later), "should refill over time");
        }
    }

    #[test]
    fn test_token_bucket_caps_at_capacity() {
        let mut bucket = TokenBucket::new(5.0, 100.0);
        let start = Instant::now();

        // Advance far into the future — should cap at capacity
        let later = start + Duration::from_secs(100);
        assert!(bucket.try_consume(later)); // 1
        assert!(bucket.try_consume(later)); // 2
        assert!(bucket.try_consume(later)); // 3
        assert!(bucket.try_consume(later)); // 4
        assert!(bucket.try_consume(later)); // 5
        assert!(!bucket.try_consume(later)); // should be empty — capped at 5
    }

    #[test]
    fn test_default_rate_limit_config() {
        let config = RateLimitConfig::default();
        assert_eq!(config.rate, 100.0);
        assert_eq!(config.burst, 200);
        assert_eq!(config.ttl, Duration::from_secs(300));
    }

    #[test]
    fn test_security_headers_present() {
        let mut headers = HeaderMap::new();
        insert_security_headers(&mut headers);

        assert_eq!(headers.get("x-content-type-options").unwrap(), "nosniff");
        assert_eq!(headers.get("x-frame-options").unwrap(), "DENY");
        assert!(headers.get("content-security-policy").is_some());
        assert!(headers.get("referrer-policy").is_some());
    }

    #[tokio::test]
    async fn test_audit_sink_persists_entries() {
        let dir = std::env::temp_dir().join(format!("nexora-audit-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("audit.jsonl");
        let _ = std::fs::remove_file(&path);

        let sink = AuditSink::new(path.clone()).unwrap();
        sink.write(r#"{"event":"one"}"#.to_string());
        sink.write(r#"{"event":"two"}"#.to_string());

        // Give the background writer time to flush.
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if let Ok(contents) = std::fs::read_to_string(&path) {
                if contents.lines().count() >= 2 {
                    assert!(contents.contains(r#"{"event":"one"}"#));
                    assert!(contents.contains(r#"{"event":"two"}"#));
                    let _ = std::fs::remove_dir_all(&dir);
                    return;
                }
            }
        }
        panic!("audit entries were not persisted within timeout");
    }

    #[tokio::test]
    async fn test_audit_sink_appends_across_reopen() {
        let dir = std::env::temp_dir().join(format!("nexora-audit-reopen-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("audit.jsonl");
        let _ = std::fs::remove_file(&path);

        {
            let sink = AuditSink::new(path.clone()).unwrap();
            sink.write(r#"{"n":1}"#.to_string());
            drop(sink);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        {
            // Reopening the same path must not truncate existing history.
            let sink = AuditSink::new(path.clone()).unwrap();
            sink.write(r#"{"n":2}"#.to_string());
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(
            contents.contains(r#"{"n":1}"#),
            "first entry lost on reopen"
        );
        assert!(contents.contains(r#"{"n":2}"#), "second entry missing");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_rate_limiter_concurrent() {
        let limiter = RateLimiter::new(RateLimitConfig {
            rate: 10.0,
            burst: 3,
            ..Default::default()
        });

        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();

        // First 3 should pass
        assert!(limiter.check(addr).await);
        assert!(limiter.check(addr).await);
        assert!(limiter.check(addr).await);

        // 4th should fail
        assert!(!limiter.check(addr).await);
    }

    #[tokio::test]
    async fn test_rate_limiter_keys_by_ip_not_port() {
        let limiter = RateLimiter::new(RateLimitConfig {
            rate: 1.0,
            burst: 2,
            ..Default::default()
        });

        // Same IP, different source ports must share one bucket — otherwise a
        // client bypasses the limit by opening a new connection per request.
        assert!(limiter.check("10.0.0.1:1000".parse().unwrap()).await);
        assert!(limiter.check("10.0.0.1:2000".parse().unwrap()).await);
        assert!(
            !limiter.check("10.0.0.1:3000".parse().unwrap()).await,
            "third request from the same IP (new port) must be rate-limited"
        );

        // A different IP gets its own fresh bucket.
        assert!(
            limiter.check("10.0.0.2:1000".parse().unwrap()).await,
            "a distinct IP is limited independently"
        );
    }
}
