//! Configuration types for nexora-pgwire.
//!
//! These are used both for programmatic construction and for
//! CLI argument parsing through clap.

use std::collections::HashMap;
use std::net::IpAddr;

use clap::Parser;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PgConfigError {
    #[error("pg-max-connections must be greater than zero")]
    InvalidMaxConnections,
    #[error("pg-idle-timeout must be greater than zero")]
    InvalidIdleTimeout,
    #[error("pg-shutdown-grace must be greater than zero")]
    InvalidShutdownGrace,
    #[error("pg-server-version must not be empty")]
    EmptyServerVersion,
    #[error("pg-trust may only be used with a loopback pg-bind address")]
    UnsafeTrustBind,
    #[error("password authentication requires --pg-users")]
    MissingUsersFile,
    #[error("--pg-tls-cert and --pg-tls-key must be provided together")]
    IncompleteTlsConfiguration,
    #[error("TLS is required when password authentication listens on a non-loopback address")]
    TlsRequiredForRemotePasswordAuth,
    #[error("failed to read PG users file {path}: {source}")]
    ReadUsers {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid PG users file {path}: {source}")]
    ParseUsers {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("PG users file must contain at least one user")]
    EmptyUsers,
    #[error("PG username must not be empty")]
    EmptyUsername,
    #[error("password for PG user {0:?} must not be empty")]
    EmptyPassword(String),
    #[error("unsupported role {role:?} for PG user {username:?}; use operator or readonly")]
    InvalidRole { username: String, role: String },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum UserConfig {
    Password(String),
    Detailed {
        password: String,
        #[serde(default = "default_role")]
        role: String,
    },
}

fn default_role() -> String {
    "operator".to_owned()
}

/// PostgreSQL wire protocol server configuration.
#[derive(Debug, Clone, Parser)]
pub struct PgConfig {
    /// Port to listen on for PostgreSQL connections.
    #[arg(long = "pg-port", default_value = "5432")]
    pub port: u16,

    /// Bind address for the PG server.
    #[arg(long = "pg-bind", default_value = "127.0.0.1")]
    pub bind_addr: String,

    /// Use trust authentication (no password required).
    /// WARNING: Only use in development / localhost environments.
    #[arg(long = "pg-trust")]
    pub trust: bool,

    /// JSON file with credentials used by SCRAM-SHA-256 authentication.
    /// Supports {"username":"password"} and detailed password/role objects.
    #[arg(long = "pg-users")]
    pub users_file: Option<String>,

    /// Maximum number of concurrent PG connections.
    #[arg(long = "pg-max-connections", default_value = "100")]
    pub max_connections: usize,

    /// Connection idle timeout in seconds.
    #[arg(long = "pg-idle-timeout", default_value = "600")]
    pub idle_timeout_secs: u64,

    /// Server version string reported to PG clients.
    #[arg(long = "pg-server-version", default_value = "14.0")]
    pub server_version: String,

    /// TLS certificate in PEM format.
    #[arg(long = "pg-tls-cert", requires = "tls_key")]
    pub tls_cert: Option<String>,

    /// TLS private key in PEM format.
    #[arg(long = "pg-tls-key", requires = "tls_cert")]
    pub tls_key: Option<String>,

    /// Time allowed for active connections to stop during shutdown.
    #[arg(long = "pg-shutdown-grace", default_value = "10")]
    pub shutdown_grace_secs: u64,
}

impl Default for PgConfig {
    fn default() -> Self {
        Self {
            port: 5432,
            bind_addr: "127.0.0.1".to_string(),
            trust: false,
            users_file: None,
            max_connections: 100,
            idle_timeout_secs: 600,
            server_version: "14.0".to_string(),
            tls_cert: None,
            tls_key: None,
            shutdown_grace_secs: 10,
        }
    }
}

impl PgConfig {
    pub fn validate(&self) -> Result<(), PgConfigError> {
        if self.max_connections == 0 {
            return Err(PgConfigError::InvalidMaxConnections);
        }
        if self.idle_timeout_secs == 0 {
            return Err(PgConfigError::InvalidIdleTimeout);
        }
        if self.shutdown_grace_secs == 0 {
            return Err(PgConfigError::InvalidShutdownGrace);
        }
        if self.server_version.trim().is_empty() {
            return Err(PgConfigError::EmptyServerVersion);
        }
        if self.tls_cert.is_some() != self.tls_key.is_some() {
            return Err(PgConfigError::IncompleteTlsConfiguration);
        }

        let loopback = is_loopback_bind(&self.bind_addr);
        if self.trust {
            if !loopback {
                return Err(PgConfigError::UnsafeTrustBind);
            }
        } else {
            if self.users_file.is_none() {
                return Err(PgConfigError::MissingUsersFile);
            }
            if !loopback && self.tls_cert.is_none() {
                return Err(PgConfigError::TlsRequiredForRemotePasswordAuth);
            }
        }
        Ok(())
    }

    /// Load user credentials from the configured users file.
    /// Returns a map of username → (password, role).
    pub fn load_users(&self) -> Result<HashMap<String, (String, String)>, PgConfigError> {
        let mut users = HashMap::new();
        if let Some(ref path) = self.users_file {
            let content =
                std::fs::read_to_string(path).map_err(|source| PgConfigError::ReadUsers {
                    path: path.clone(),
                    source,
                })?;
            let parsed: HashMap<String, UserConfig> =
                serde_json::from_str(&content).map_err(|source| PgConfigError::ParseUsers {
                    path: path.clone(),
                    source,
                })?;
            for (username, entry) in parsed {
                if username.trim().is_empty() {
                    return Err(PgConfigError::EmptyUsername);
                }
                let (password, role) = match entry {
                    UserConfig::Password(password) => (password, default_role()),
                    UserConfig::Detailed { password, role } => (password, role),
                };
                if password.is_empty() {
                    return Err(PgConfigError::EmptyPassword(username));
                }
                let role = role.to_ascii_lowercase();
                if !matches!(role.as_str(), "operator" | "readonly") {
                    return Err(PgConfigError::InvalidRole { username, role });
                }
                users.insert(username, (password, role));
            }
        }
        if !self.trust && users.is_empty() {
            return Err(PgConfigError::EmptyUsers);
        }
        Ok(users)
    }
}

fn is_loopback_bind(bind_addr: &str) -> bool {
    bind_addr.eq_ignore_ascii_case("localhost")
        || bind_addr
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trust_requires_loopback() {
        let config = PgConfig {
            trust: true,
            bind_addr: "0.0.0.0".to_owned(),
            ..PgConfig::default()
        };
        assert!(matches!(
            config.validate(),
            Err(PgConfigError::UnsafeTrustBind)
        ));
    }

    #[test]
    fn password_auth_requires_users() {
        let config = PgConfig::default();
        assert!(matches!(
            config.validate(),
            Err(PgConfigError::MissingUsersFile)
        ));
    }
}
