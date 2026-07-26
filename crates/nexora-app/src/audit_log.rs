//! Structured audit logging for production compliance.
//!
//! Records critical operations (failover, rebalance, membership changes) as
//! JSON Lines to a dedicated audit log file, separate from debug/trace logs.
//!
//! ## Output Format
//!
//! Each line is a JSON object with:
//! - `timestamp`: ISO 8601 timestamp with timezone
//! - `event_type`: one of "failover", "rebalance", "membership_change", "auth", "admin"
//! - `severity`: "info", "warning", "error"
//! - `node_id`: identifier of the node that generated this event
//! - `user`: optional user identifier (for auth/admin events)
//! - `details`: event-specific structured data
//!
//! ## Example Output
//!
//! ```json
//! {"timestamp":"2026-07-25T10:30:45.123Z","event_type":"failover","severity":"info","node_id":"node-1","details":{"shard_id":3,"from":"node-2","to":"node-1","reason":"heartbeat_timeout","duration_ms":1250}}
//! {"timestamp":"2026-07-25T10:32:10.456Z","event_type":"membership_change","severity":"info","node_id":"node-1","details":{"action":"add","target_node":"node-4","cluster_size":4}}
//! ```

use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Audit event types.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventType {
    /// Shard ownership failover.
    Failover,
    /// Shard rebalancing operation.
    Rebalance,
    /// Cluster membership change (add/remove node).
    MembershipChange,
    /// Authentication/authorization event.
    Auth,
    /// Administrative operation (backup, restore, drain, key rotation).
    Admin,
}

/// Severity levels for audit events.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AuditSeverity {
    Info,
    Warning,
    Error,
}

/// A structured audit log entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditEntry {
    /// ISO 8601 timestamp with timezone.
    pub timestamp: String,
    /// Type of event being recorded.
    pub event_type: AuditEventType,
    /// Severity level.
    pub severity: AuditSeverity,
    /// Node ID that generated this event.
    pub node_id: String,
    /// Optional user identifier (for auth/admin events).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// Event-specific details as JSON object.
    pub details: serde_json::Value,
}

/// Audit logger — writes structured events to a JSON Lines file.
pub struct AuditLogger {
    writer: Arc<Mutex<BufWriter<File>>>,
    node_id: String,
    log_path: PathBuf,
}

impl AuditLogger {
    /// Create a new audit logger writing to the specified path.
    ///
    /// The file is opened in append mode. If it doesn't exist, it's created.
    pub fn new(log_path: impl AsRef<Path>, node_id: String) -> std::io::Result<Self> {
        let path = log_path.as_ref();

        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(Self {
            writer: Arc::new(Mutex::new(BufWriter::new(file))),
            node_id,
            log_path: path.to_path_buf(),
        })
    }

    /// Log a failover event.
    pub async fn log_failover(
        &self,
        shard_id: usize,
        from_node: &str,
        to_node: &str,
        reason: &str,
        duration_ms: u64,
        success: bool,
    ) {
        let details = serde_json::json!({
            "shard_id": shard_id,
            "from": from_node,
            "to": to_node,
            "reason": reason,
            "duration_ms": duration_ms,
            "success": success,
        });

        let severity = if success {
            AuditSeverity::Info
        } else {
            AuditSeverity::Error
        };

        self.write_entry(AuditEntry {
            timestamp: Utc::now().to_rfc3339(),
            event_type: AuditEventType::Failover,
            severity,
            node_id: self.node_id.clone(),
            user: None,
            details,
        })
        .await;
    }

    /// Log a rebalance operation.
    pub async fn log_rebalance(
        &self,
        shard_moves: usize,
        reason: &str,
        duration_ms: u64,
        success: bool,
    ) {
        let details = serde_json::json!({
            "shard_moves": shard_moves,
            "reason": reason,
            "duration_ms": duration_ms,
            "success": success,
        });

        let severity = if success {
            AuditSeverity::Info
        } else {
            AuditSeverity::Error
        };

        self.write_entry(AuditEntry {
            timestamp: Utc::now().to_rfc3339(),
            event_type: AuditEventType::Rebalance,
            severity,
            node_id: self.node_id.clone(),
            user: None,
            details,
        })
        .await;
    }

    /// Log a cluster membership change.
    pub async fn log_membership_change(
        &self,
        action: &str, // "add" or "remove"
        target_node: &str,
        cluster_size: usize,
    ) {
        let details = serde_json::json!({
            "action": action,
            "target_node": target_node,
            "cluster_size": cluster_size,
        });

        self.write_entry(AuditEntry {
            timestamp: Utc::now().to_rfc3339(),
            event_type: AuditEventType::MembershipChange,
            severity: AuditSeverity::Info,
            node_id: self.node_id.clone(),
            user: None,
            details,
        })
        .await;
    }

    /// Log an authentication/authorization event.
    pub async fn log_auth(
        &self,
        user: &str,
        action: &str,
        resource: &str,
        allowed: bool,
        reason: Option<&str>,
    ) {
        let mut details = serde_json::json!({
            "action": action,
            "resource": resource,
            "allowed": allowed,
        });

        if let Some(r) = reason {
            details["reason"] = serde_json::Value::String(r.to_string());
        }

        let severity = if allowed {
            AuditSeverity::Info
        } else {
            AuditSeverity::Warning
        };

        self.write_entry(AuditEntry {
            timestamp: Utc::now().to_rfc3339(),
            event_type: AuditEventType::Auth,
            severity,
            node_id: self.node_id.clone(),
            user: Some(user.to_string()),
            details,
        })
        .await;
    }

    /// Log an administrative operation.
    pub async fn log_admin(
        &self,
        user: Option<&str>,
        operation: &str,
        target: Option<&str>,
        success: bool,
        details_data: serde_json::Value,
    ) {
        let mut details = details_data;
        details["operation"] = serde_json::Value::String(operation.to_string());
        if let Some(t) = target {
            details["target"] = serde_json::Value::String(t.to_string());
        }
        details["success"] = serde_json::Value::Bool(success);

        let severity = if success {
            AuditSeverity::Info
        } else {
            AuditSeverity::Error
        };

        self.write_entry(AuditEntry {
            timestamp: Utc::now().to_rfc3339(),
            event_type: AuditEventType::Admin,
            severity,
            node_id: self.node_id.clone(),
            user: user.map(String::from),
            details,
        })
        .await;
    }

    /// Write a raw audit entry to the log.
    async fn write_entry(&self, entry: AuditEntry) {
        let mut writer = self.writer.lock().await;

        match serde_json::to_string(&entry) {
            Ok(json) => {
                if let Err(e) = writeln!(writer.get_mut(), "{}", json) {
                    eprintln!("Failed to write audit log: {}", e);
                }
                // Flush immediately for audit logs (durability over performance)
                if let Err(e) = writer.flush() {
                    eprintln!("Failed to flush audit log: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Failed to serialize audit entry: {}", e);
            }
        }
    }

    /// Get the path to the audit log file.
    pub fn log_path(&self) -> &Path {
        &self.log_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_failover_logging() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();

        let logger = AuditLogger::new(&path, "test-node".to_string()).unwrap();

        logger
            .log_failover(3, "node-2", "node-1", "heartbeat_timeout", 1250, true)
            .await;

        // Read the log file
        let mut file = File::open(&path).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        let entry: AuditEntry = serde_json::from_str(contents.trim()).unwrap();
        assert_eq!(entry.event_type, AuditEventType::Failover);
        assert_eq!(entry.severity, AuditSeverity::Info);
        assert_eq!(entry.node_id, "test-node");
        assert_eq!(entry.details["shard_id"], 3);
        assert_eq!(entry.details["success"], true);
    }

    #[tokio::test]
    async fn test_membership_change_logging() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();

        let logger = AuditLogger::new(&path, "node-1".to_string()).unwrap();

        logger
            .log_membership_change("add", "node-4", 4)
            .await;

        let mut file = File::open(&path).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        let entry: AuditEntry = serde_json::from_str(contents.trim()).unwrap();
        assert_eq!(entry.event_type, AuditEventType::MembershipChange);
        assert_eq!(entry.details["action"], "add");
        assert_eq!(entry.details["cluster_size"], 4);
    }

    #[tokio::test]
    async fn test_auth_logging() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();

        let logger = AuditLogger::new(&path, "node-1".to_string()).unwrap();

        logger
            .log_auth("alice", "write", "/api/admin/backup", false, Some("insufficient permissions"))
            .await;

        let mut file = File::open(&path).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        let entry: AuditEntry = serde_json::from_str(contents.trim()).unwrap();
        assert_eq!(entry.event_type, AuditEventType::Auth);
        assert_eq!(entry.severity, AuditSeverity::Warning);
        assert_eq!(entry.user, Some("alice".to_string()));
        assert_eq!(entry.details["allowed"], false);
    }

    #[tokio::test]
    async fn test_multiple_entries() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();

        let logger = AuditLogger::new(&path, "node-1".to_string()).unwrap();

        logger.log_failover(1, "n2", "n1", "test", 100, true).await;
        logger.log_rebalance(3, "scale-up", 5000, true).await;
        logger.log_membership_change("remove", "n3", 2).await;

        let mut file = File::open(&path).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();

        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 3);

        let e1: AuditEntry = serde_json::from_str(lines[0]).unwrap();
        let e2: AuditEntry = serde_json::from_str(lines[1]).unwrap();
        let e3: AuditEntry = serde_json::from_str(lines[2]).unwrap();

        assert_eq!(e1.event_type, AuditEventType::Failover);
        assert_eq!(e2.event_type, AuditEventType::Rebalance);
        assert_eq!(e3.event_type, AuditEventType::MembershipChange);
    }
}
