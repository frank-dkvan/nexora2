//! Evidence reference system — Nexora stores references, not blobs.
//!
//! EvidenceRef is a pointer to external evidence (images, videos, logs,
//! sensor data, etc.) stored in specialized systems like ReductStore,
//! S3, MinIO, OpenSearch, Loki, or external URLs.
//!
//! Every Exception, Alert, or Event node can link to EvidenceRef nodes
//! via a HAS_EVIDENCE relationship. The evidence itself is never stored
//! inside Nexora — only the reference is kept for query and audit.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Supported evidence store backends.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EvidenceStoreType {
    ReductStore,
    S3,
    MinIO,
    OpenSearch,
    Loki,
    ExternalUrl,
    Custom(String),
}

impl EvidenceStoreType {
    pub fn as_str(&self) -> &str {
        match self {
            EvidenceStoreType::ReductStore => "reductstore",
            EvidenceStoreType::S3 => "s3",
            EvidenceStoreType::MinIO => "minio",
            EvidenceStoreType::OpenSearch => "opensearch",
            EvidenceStoreType::Loki => "loki",
            EvidenceStoreType::ExternalUrl => "url",
            EvidenceStoreType::Custom(s) => s.as_str(),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "reductstore" => EvidenceStoreType::ReductStore,
            "s3" => EvidenceStoreType::S3,
            "minio" => EvidenceStoreType::MinIO,
            "opensearch" => EvidenceStoreType::OpenSearch,
            "loki" => EvidenceStoreType::Loki,
            "url" | "externalurl" | "external_url" => EvidenceStoreType::ExternalUrl,
            _ => EvidenceStoreType::Custom(s.to_string()),
        }
    }
}

/// A reference to external evidence (image, video, log, sensor data, etc.).
/// Nexora stores only this reference — the actual evidence lives in an
/// external store (ReductStore, S3, MinIO, OpenSearch, Loki, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceRef {
    pub evidence_id: String,
    pub store_type: EvidenceStoreType,
    pub bucket: String,
    pub entry: String,

    pub timestamp_start: Option<DateTime<Utc>>,
    pub timestamp_end: Option<DateTime<Utc>>,
    pub labels: HashMap<String, String>,

    pub uri: Option<String>,
    pub checksum: Option<String>,
    pub retention_policy: Option<String>,
    pub source_system: Option<String>,
    pub correlation_id: Option<String>,
    pub domain: Option<String>,
    pub object_id: Option<String>,
    pub event_id: Option<String>,
}

impl EvidenceRef {
    /// Create a new evidence reference.
    pub fn new(
        evidence_id: impl Into<String>,
        store_type: EvidenceStoreType,
        bucket: impl Into<String>,
        entry: impl Into<String>,
    ) -> Self {
        Self {
            evidence_id: evidence_id.into(),
            store_type,
            bucket: bucket.into(),
            entry: entry.into(),
            timestamp_start: None,
            timestamp_end: None,
            labels: HashMap::new(),
            uri: None,
            checksum: None,
            retention_policy: None,
            source_system: None,
            correlation_id: None,
            domain: None,
            object_id: None,
            event_id: None,
        }
    }

    /// Set the time range.
    pub fn with_time_range(mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        self.timestamp_start = Some(start);
        self.timestamp_end = Some(end);
        self
    }

    /// Add a label.
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Set the URI.
    pub fn with_uri(mut self, uri: impl Into<String>) -> Self {
        self.uri = Some(uri.into());
        self
    }

    /// Set the checksum.
    pub fn with_checksum(mut self, checksum: impl Into<String>) -> Self {
        self.checksum = Some(checksum.into());
        self
    }

    /// Set the source system.
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source_system = Some(source.into());
        self
    }

    /// Set the correlation ID.
    pub fn with_correlation(mut self, cid: impl Into<String>) -> Self {
        self.correlation_id = Some(cid.into());
        self
    }

    /// Set the domain.
    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = Some(domain.into());
        self
    }

    /// Set the object ID.
    pub fn with_object(mut self, oid: impl Into<String>) -> Self {
        self.object_id = Some(oid.into());
        self
    }

    /// Set the event ID.
    pub fn with_event(mut self, eid: impl Into<String>) -> Self {
        self.event_id = Some(eid.into());
        self
    }

    /// Convert to JSON for API responses.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "evidence_id": self.evidence_id,
            "store_type": self.store_type.as_str(),
            "bucket": self.bucket,
            "entry": self.entry,
            "uri": self.uri,
            "checksum": self.checksum,
            "retention_policy": self.retention_policy,
            "source_system": self.source_system,
            "correlation_id": self.correlation_id,
            "domain": self.domain,
            "object_id": self.object_id,
            "event_id": self.event_id,
            "timestamp_start": self.timestamp_start.map(|t| t.to_rfc3339()),
            "timestamp_end": self.timestamp_end.map(|t| t.to_rfc3339()),
            "labels": self.labels,
        })
    }

    /// Estimated memory size of this evidence ref.
    pub fn memory_size(&self) -> usize {
        let base = std::mem::size_of::<Self>();
        let strings = self.evidence_id.len()
            + self.bucket.len()
            + self.entry.len()
            + self.uri.as_ref().map_or(0, |s| s.len())
            + self.checksum.as_ref().map_or(0, |s| s.len())
            + self.retention_policy.as_ref().map_or(0, |s| s.len())
            + self.source_system.as_ref().map_or(0, |s| s.len())
            + self.correlation_id.as_ref().map_or(0, |s| s.len())
            + self.domain.as_ref().map_or(0, |s| s.len())
            + self.object_id.as_ref().map_or(0, |s| s.len())
            + self.event_id.as_ref().map_or(0, |s| s.len())
            + self
                .labels
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>();
        base + strings
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evidence_ref_creation() {
        let ref1 = EvidenceRef::new(
            "evt-001",
            EvidenceStoreType::S3,
            "camera-footage",
            "agv001/2026-07-05/front_camera.mp4",
        )
        .with_source("agv-telemetry")
        .with_domain("air_cargo_terminal")
        .with_label("device_id", "AGV001")
        .with_label("event_type", "EMERGENCY_STOP");

        assert_eq!(ref1.evidence_id, "evt-001");
        assert_eq!(ref1.store_type, EvidenceStoreType::S3);
        assert_eq!(ref1.bucket, "camera-footage");
        assert_eq!(ref1.source_system.unwrap(), "agv-telemetry");
        assert_eq!(ref1.labels.len(), 2);
    }

    #[test]
    fn test_evidence_ref_to_json() {
        let ref1 = EvidenceRef::new(
            "evt-001",
            EvidenceStoreType::ReductStore,
            "iot-equipment",
            "agv/agv001/camera/front",
        );

        let json = ref1.to_json();
        assert_eq!(json["evidence_id"], "evt-001");
        assert_eq!(json["store_type"], "reductstore");
    }

    #[test]
    fn test_evidence_store_type_roundtrip() {
        assert_eq!(EvidenceStoreType::from_str("s3"), EvidenceStoreType::S3);
        assert_eq!(
            EvidenceStoreType::from_str("ReductStore"),
            EvidenceStoreType::ReductStore
        );
        assert_eq!(EvidenceStoreType::from_str("loki"), EvidenceStoreType::Loki);
        assert_eq!(
            EvidenceStoreType::from_str("minio"),
            EvidenceStoreType::MinIO
        );
        assert_eq!(
            EvidenceStoreType::from_str("url"),
            EvidenceStoreType::ExternalUrl
        );
        assert_eq!(
            EvidenceStoreType::from_str("external_url"),
            EvidenceStoreType::ExternalUrl
        );
        assert_eq!(
            EvidenceStoreType::from_str("custom_type"),
            EvidenceStoreType::Custom("custom_type".to_string())
        );
    }

    #[test]
    fn test_evidence_ref_memory_size() {
        let ref1 =
            EvidenceRef::new("e1", EvidenceStoreType::S3, "bucket", "entry").with_source("cam-01");
        assert!(ref1.memory_size() > 0);
    }

    #[test]
    fn test_evidence_ref_with_all_fields() {
        let ref1 = EvidenceRef::new(
            "evt-002",
            EvidenceStoreType::OpenSearch,
            "logs",
            "2026/07/05/agv001",
        )
        .with_time_range(
            DateTime::parse_from_rfc3339("2026-07-05T10:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            DateTime::parse_from_rfc3339("2026-07-05T10:01:00Z")
                .unwrap()
                .with_timezone(&Utc),
        )
        .with_uri("https://opensearch:9200/logs/_doc/abc123")
        .with_checksum("sha256:abc123def456")
        .with_correlation("corr-001")
        .with_object("AGV001")
        .with_event("EVT001")
        .with_label("severity", "P1");

        assert!(ref1.uri.is_some());
        assert!(ref1.timestamp_start.is_some());
        assert!(ref1.timestamp_end.is_some());
        assert_eq!(ref1.labels.get("severity").unwrap(), "P1");
    }

    #[test]
    fn test_evidence_ref_serde_roundtrip() {
        let ref1 = EvidenceRef::new(
            "evt-003",
            EvidenceStoreType::MinIO,
            "evidence",
            "object/root/event/evt003",
        )
        .with_source("system-x")
        .with_label("type", "impact-analysis");

        let json = serde_json::to_string(&ref1).unwrap();
        let restored: EvidenceRef = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.evidence_id, "evt-003");
        assert_eq!(restored.store_type, EvidenceStoreType::MinIO);
        assert_eq!(restored.labels.get("type").unwrap(), "impact-analysis");
    }
}
