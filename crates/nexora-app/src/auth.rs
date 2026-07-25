//! Token-based authentication using HMAC-SHA256 with RBAC.
//!
//! Tokens are base64-encoded JSON payloads signed with HMAC-SHA256.
//! The secret is provided via CLI flag `--auth-secret` or the
//! `NEXORA_AUTH_SECRET` environment variable.
//!
//! # Role-Based Access Control (RBAC)
//!
//! | Role       | Permissions                                                     |
//! |------------|-----------------------------------------------------------------|
//! | `admin`    | Full access: all read/write/admin endpoints                     |
//! | `operator` | Read/write: ingest, properties, edges, standing queries, cypher |
//! | `readonly` | Read-only: query, health, metrics, dashboard                    |
//!
//! # Required roles per endpoint category:
//! - Admin endpoints (`/api/v2/admin/*`) → `admin`
//! - Write endpoints (POST/PUT/DELETE on graph, ingest, SQ) → `admin` or `operator`
//! - Query endpoints (GET, POST query) → any authenticated role
//! - Public endpoints (health, metrics, dashboard, auth/token) → no auth

use axum::{
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

// ---------------------------------------------------------------------------
// Role definitions
// ---------------------------------------------------------------------------

/// Access role for token-based authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Read-only access to query endpoints.
    ReadOnly,
    /// Can read, write, and manage data but not administrative functions.
    Operator,
    /// Full access to all endpoints.
    Admin,
}

impl Role {
    /// Parse from a string, case-insensitive.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "admin" => Some(Role::Admin),
            "operator" => Some(Role::Operator),
            "readonly" | "read_only" | "read-only" => Some(Role::ReadOnly),
            _ => None,
        }
    }

    /// Check if this role meets or exceeds the required role.
    pub fn satisfies(&self, required: Role) -> bool {
        *self >= required
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Admin => write!(f, "admin"),
            Role::Operator => write!(f, "operator"),
            Role::ReadOnly => write!(f, "readonly"),
        }
    }
}

// ---------------------------------------------------------------------------
// Claims & Auth
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,
    pub exp: u64,
    pub iat: u64,
    pub role: Option<String>,
}

/// The signature-verified subject of the current request, inserted into request
/// extensions by [`require_auth`] after `validate` succeeds. Downstream layers
/// (e.g. audit logging) read this instead of re-parsing the raw token, so the
/// recorded identity can never be forged by editing an unsigned payload.
#[derive(Debug, Clone)]
pub struct AuthenticatedUser(pub String);

impl Claims {
    /// Get the parsed Role, defaulting to ReadOnly if role is missing or invalid.
    pub fn role(&self) -> Role {
        self.role
            .as_deref()
            .and_then(Role::from_str)
            .unwrap_or(Role::ReadOnly)
    }
}

pub struct Auth {
    secret: Vec<u8>,
}

impl Auth {
    pub fn new(secret: &str) -> Self {
        Self {
            secret: secret.as_bytes().to_vec(),
        }
    }

    /// Default token expiry in seconds (24 hours).
    pub const DEFAULT_EXPIRES_IN: u64 = 86400;

    /// Maximum allowed token expiry (7 days).
    pub const MAX_EXPIRES_IN: u64 = 604800;

    /// Generate a signed token with a specific role.
    /// `expires_in` is the token lifetime in seconds; defaults to 86400 (24h).
    pub fn generate_token_with_role_expiring(
        &self,
        user_id: &str,
        role: Role,
        expires_in: Option<u64>,
    ) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let ttl = expires_in
            .unwrap_or(Self::DEFAULT_EXPIRES_IN)
            .min(Self::MAX_EXPIRES_IN);
        let claims = Claims {
            sub: user_id.into(),
            exp: now + ttl,
            iat: now,
            role: Some(role.to_string()),
        };
        let payload = serde_json::to_string(&claims).unwrap_or_default();
        let payload_b64 = BASE64.encode(payload.as_bytes());
        let sig = self.sign(payload_b64.as_bytes());
        format!("{payload_b64}.{sig}")
    }

    /// Generate a signed token with a specific role (default 24h expiry).
    pub fn generate_token_with_role(&self, user_id: &str, role: Role) -> String {
        self.generate_token_with_role_expiring(user_id, role, None)
    }

    /// Generate a signed token (defaults to Operator role).
    #[allow(dead_code)]
    pub fn generate_token(&self, user_id: &str) -> String {
        self.generate_token_with_role(user_id, Role::Operator)
    }

    /// Validate and decode a token.
    pub fn validate(&self, token: &str) -> Result<Claims, AuthError> {
        let dot = token.rfind('.').ok_or(AuthError::InvalidFormat)?;
        let (payload_b64, sig) = token.split_at(dot);
        let sig = &sig[1..]; // skip the dot

        // Verify signature with a constant-time comparison to prevent
        // timing side-channels on the HMAC digest. String equality (`!=`)
        // short-circuits on the first mismatched byte, leaking which
        // prefix is correct.
        let expected_sig = self.sign(payload_b64.as_bytes());
        if expected_sig.len() != sig.len() {
            // Length mismatch is a cheap early reject; the lengths are
            // deterministic (hex-encoded SHA256 ≡ 64 chars) so no useful
            // timing signal about the correct digest is excreted.
            return Err(AuthError::InvalidSignature);
        }
        let mut diff = 0u8;
        for (a, b) in expected_sig.as_bytes().iter().zip(sig.as_bytes()) {
            diff |= a ^ b;
        }
        if diff != 0 {
            return Err(AuthError::InvalidSignature);
        }

        // Decode payload
        let payload_bytes = BASE64
            .decode(payload_b64.as_bytes())
            .map_err(|_| AuthError::InvalidBase64)?;
        let payload_json = String::from_utf8(payload_bytes).map_err(|_| AuthError::InvalidUtf8)?;
        let claims: Claims =
            serde_json::from_str(&payload_json).map_err(|e| AuthError::JsonError(e.to_string()))?;

        // Check expiry
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if claims.exp < now {
            return Err(AuthError::Expired);
        }

        Ok(claims)
    }

    fn sign(&self, data: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("HMAC key");
        mac.update(data);
        hex::encode(mac.finalize().into_bytes())
    }
}

// ---------------------------------------------------------------------------
// Auth errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("invalid token format")]
    InvalidFormat,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("invalid base64 encoding")]
    InvalidBase64,
    #[error("invalid UTF-8 in payload")]
    InvalidUtf8,
    #[error("JSON decode error: {0}")]
    JsonError(String),
    #[error("token expired")]
    Expired,
}

// ---------------------------------------------------------------------------
// Axum middleware: require a valid Bearer token (any role)
// ---------------------------------------------------------------------------

/// Public endpoints that never require authentication.
pub const PUBLIC_PATHS: &[&str] = &[
    "/",
    "/dashboard",
    "/test",
    "/api/v2/health",
    "/api/v2/health/ready",
    "/api/v2/health/live",
    "/metrics",
    "/api/v2/metrics",
];

/// Axum middleware: require a valid Bearer token.
pub async fn require_auth(
    auth: axum::extract::Extension<Arc<Auth>>,
    req: Request,
    next: Next,
) -> Result<Response, Response> {
    let path = req.uri().path();

    if PUBLIC_PATHS.contains(&path) {
        return Ok(next.run(req).await);
    }

    // Also allow WebSocket upgrade paths to pass through (token validated in handler)
    if path.starts_with("/api/v2/ws/") {
        return Ok(next.run(req).await);
    }

    let token = extract_bearer_token(&req).ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": "Missing or invalid Authorization header. Expected: Bearer <token>",
                "code": "AUTH_MISSING"
            })),
        )
            .into_response()
    })?;

    match auth.validate(token) {
        Ok(claims) => {
            // Record the *validated* identity in request extensions so the audit
            // middleware attributes the action to a signature-verified subject,
            // not to whatever the caller wrote in an unverified token payload.
            let mut req = req;
            req.extensions_mut().insert(AuthenticatedUser(claims.sub));
            Ok(next.run(req).await)
        }
        Err(e) => Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": format!("Authentication failed: {e}"),
                "code": "AUTH_FAILED"
            })),
        )
            .into_response()),
    }
}

// ---------------------------------------------------------------------------
// Axum middleware: require a minimum role
// ---------------------------------------------------------------------------

/// Middleware factory: require the caller to have at least `required_role`.
///
/// Usage (in router):
/// ```ignore
/// .route_layer(middleware::from_fn_with_state(state, require_role(Role::Admin)))
/// ```
#[allow(clippy::type_complexity)]
pub fn require_role(
    required: Role,
) -> impl Fn(
    axum::extract::Extension<Arc<Auth>>,
    Request,
    Next,
)
    -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response, Response>> + Send>>
       + Clone {
    move |auth: axum::extract::Extension<Arc<Auth>>, req: Request, next: Next| {
        let required = required;
        Box::pin(async move {
            let path = req.uri().path();

            if PUBLIC_PATHS.contains(&path) {
                return Ok(next.run(req).await);
            }

            let token = extract_bearer_token(&req).ok_or_else(|| {
                (StatusCode::UNAUTHORIZED, Json(json!({
                    "error": "Missing or invalid Authorization header. Expected: Bearer <token>",
                    "code": "AUTH_MISSING"
                }))).into_response()
            })?;

            let claims = auth.validate(token).map_err(|e| {
                tracing::warn!(target: "audit", "Auth failed: {e}");
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({
                        "error": format!("Authentication failed: {e}"),
                        "code": "AUTH_FAILED"
                    })),
                )
                    .into_response()
            })?;

            let role = claims.role();
            if !role.satisfies(required) {
                tracing::warn!(
                    target: "audit",
                    subject = %claims.sub,
                    actual_role = %role,
                    required_role = %required,
                    path = %path,
                    "Insufficient role"
                );
                return Err((StatusCode::FORBIDDEN, Json(json!({
                    "error": format!("Insufficient permissions. Required role: {required}, your role: {role}"),
                    "code": "FORBIDDEN"
                }))).into_response());
            }

            Ok(next.run(req).await)
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the Bearer token from the Authorization header.
fn extract_bearer_token(req: &Request) -> Option<&str> {
    let header = req.headers().get(AUTHORIZATION)?.to_str().ok()?;
    header.strip_prefix("Bearer ")
}

/// Extract token from query parameter `?token=xxx` — used for WebSocket connections
/// where Bearer header cannot be set in browser environments.
pub fn extract_query_token(req: &Request) -> Option<String> {
    req.uri().query().and_then(|q| {
        q.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            if k == "token" {
                Some(v.to_string())
            } else {
                None
            }
        })
    })
}

/// Validate a WebSocket connection's token from query parameter.
/// Returns Ok(claims) if valid, Err(error_message) if not.
pub fn validate_ws_token(auth: &Auth, req: &Request) -> Result<Claims, String> {
    // Try query parameter first (browser WebSocket)
    if let Some(token) = extract_query_token(req) {
        return auth.validate(&token).map_err(|e| e.to_string());
    }
    // Fall back to Authorization header (non-browser clients)
    if let Some(token) = extract_bearer_token(req) {
        return auth.validate(token).map_err(|e| e.to_string());
    }
    Err("No authentication token provided. Use ?token=xxx query parameter or Bearer header.".into())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_roundtrip() {
        let auth = Auth::new("my-secret-key");
        let token = auth.generate_token("user-001");
        let claims = auth.validate(&token).unwrap();
        assert_eq!(claims.sub, "user-001");
    }

    #[test]
    fn test_wrong_secret_rejected() {
        let auth1 = Auth::new("secret-one");
        let auth2 = Auth::new("secret-two");
        let token = auth1.generate_token("user-001");
        assert!(auth2.validate(&token).is_err());
    }

    #[test]
    fn test_tampered_token_rejected() {
        let auth = Auth::new("my-secret-key");
        let token = auth.generate_token("user-001");
        let tampered = format!("{}.deadbeef", token.split('.').next().unwrap());
        assert!(auth.validate(&tampered).is_err());
    }

    #[test]
    fn test_role_parsing() {
        assert_eq!(Role::from_str("admin"), Some(Role::Admin));
        assert_eq!(Role::from_str("ADMIN"), Some(Role::Admin));
        assert_eq!(Role::from_str("operator"), Some(Role::Operator));
        assert_eq!(Role::from_str("readonly"), Some(Role::ReadOnly));
        assert_eq!(Role::from_str("read-only"), Some(Role::ReadOnly));
        assert_eq!(Role::from_str("read_only"), Some(Role::ReadOnly));
        assert_eq!(Role::from_str("unknown"), None);
    }

    #[test]
    fn test_role_satisfies() {
        assert!(Role::Admin.satisfies(Role::Admin));
        assert!(Role::Admin.satisfies(Role::Operator));
        assert!(Role::Admin.satisfies(Role::ReadOnly));

        assert!(!Role::Operator.satisfies(Role::Admin));
        assert!(Role::Operator.satisfies(Role::Operator));
        assert!(Role::Operator.satisfies(Role::ReadOnly));

        assert!(!Role::ReadOnly.satisfies(Role::Admin));
        assert!(!Role::ReadOnly.satisfies(Role::Operator));
        assert!(Role::ReadOnly.satisfies(Role::ReadOnly));
    }

    #[test]
    fn test_role_token() {
        let auth = Auth::new("my-secret-key");
        let token = auth.generate_token_with_role("admin-1", Role::Admin);
        let claims = auth.validate(&token).unwrap();
        assert_eq!(claims.sub, "admin-1");
        assert_eq!(claims.role(), Role::Admin);

        let token = auth.generate_token_with_role("reader-1", Role::ReadOnly);
        let claims = auth.validate(&token).unwrap();
        assert_eq!(claims.role(), Role::ReadOnly);
    }

    #[test]
    fn test_expired_token() {
        // Create a token that's already expired by manually constructing claims
        let auth = Auth::new("my-secret-key");
        let claims = Claims {
            sub: "expired-user".into(),
            exp: 0, // 1970
            iat: 0,
            role: Some("operator".into()),
        };
        let payload = serde_json::to_string(&claims).unwrap();
        let payload_b64 = BASE64.encode(payload.as_bytes());
        let sig = auth.sign(payload_b64.as_bytes());
        let token = format!("{payload_b64}.{sig}");

        assert!(matches!(auth.validate(&token), Err(AuthError::Expired)));
    }
}
