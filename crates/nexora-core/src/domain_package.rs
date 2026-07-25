//! Domain Package mechanism — schema-driven domain model definitions.
//!
//! Domain Packages are YAML/JSON configuration files that define labels,
//! edge types, properties, event mappings, Standing Queries, and
//! Materialized Views for a specific domain (air cargo, manufacturing, IT).
//!
//! The core engine is domain-agnostic — all domain-specific concepts
//! are loaded via Domain Packages at startup.
//!
//! Architecture:
//! ```text
//! DomainPackage (YAML)
//!   │
//!   ├── DomainSchema   — labels, edge_types, constraints, indexes
//!   ├── EventMapping   — event → graph mutation rules
//!   └── DomainAssets   — pre-registered SQs and MVs
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================
// Schema types
// ============================================================

/// A complete domain schema definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainSchema {
    pub domain: String,
    pub version: String,
    pub description: Option<String>,
    pub extends: Option<String>,
    #[serde(default)]
    pub labels: Vec<LabelDef>,
    #[serde(default)]
    pub edge_types: Vec<EdgeTypeDef>,
    #[serde(default)]
    pub constraints: Vec<ConstraintDef>,
    #[serde(default)]
    pub indexes: Vec<IndexDef>,
}

/// Label definition within a domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelDef {
    pub name: String,
    pub description: Option<String>,
    pub extends: Option<String>,
    pub properties: Vec<PropertyDef>,
}

/// Property definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyDef {
    pub name: String,
    #[serde(rename = "prop_type")]
    pub prop_type: String,
    pub required: Option<bool>,
    pub indexed: Option<bool>,
    pub description: Option<String>,
    #[serde(rename = "enum_values")]
    pub enum_values: Option<Vec<String>>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub default: Option<String>,
}

/// Edge type definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeTypeDef {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub properties: Vec<PropertyDef>,
    pub source_labels: Option<Vec<String>>,
    pub target_labels: Option<Vec<String>>,
}

/// Constraint definition (e.g., unique on properties).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintDef {
    pub constraint_type: String,
    pub label: String,
    pub properties: Vec<String>,
}

/// Index definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub label: String,
    pub properties: Vec<String>,
    pub index_type: Option<String>, // "hash" | "btree" | "composite"
}

// ============================================================
// Event mapping types
// ============================================================

/// A mapping from an external event to graph mutations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMapping {
    pub domain: String,
    pub source: String,
    pub event_type: String,
    pub node: NodeMapping,
    pub properties: HashMap<String, String>,
    pub edges: Vec<EdgeMapping>,
}

/// Node mapping from event to graph node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMapping {
    pub label: String,
    pub id: String, // JSON path expression
}

/// Edge mapping from event to graph edge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeMapping {
    pub edge_type: String,
    pub target_label: String,
    pub target_id: String, // JSON path expression
}

/// A Standing Query bundled in a Domain Package.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainQuery {
    pub name: String,
    pub description: Option<String>,
    pub query: String,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// A Materialized View bundled in a Domain Package.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainMV {
    pub name: String,
    pub query: String,
    #[serde(default)]
    pub refresh_mode: String,
    #[serde(default)]
    pub schema: Vec<DomainColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainColumnDef {
    pub name: String,
    pub data_type: String,
}

// ============================================================
// Domain Package — top-level container
// ============================================================

/// A complete Domain Package — loaded from a YAML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainPackage {
    pub schema: DomainSchema,
    #[serde(default)]
    pub mappings: Vec<EventMapping>,
    #[serde(default)]
    pub standing_queries: Vec<DomainQuery>,
    #[serde(default)]
    pub materialized_views: Vec<DomainMV>,
}

/// Loader for Domain Packages from filesystem paths.
pub struct DomainLoader {
    /// Loaded packages indexed by domain name.
    packages: HashMap<String, DomainPackage>,
}

impl DomainLoader {
    pub fn new() -> Self {
        Self {
            packages: HashMap::new(),
        }
    }

    /// Load a Domain Package from a YAML file.
    pub fn load_from_yaml(&mut self, yaml: &str) -> Result<String, String> {
        let pkg: DomainPackage =
            serde_yaml::from_str(yaml).map_err(|e| format!("YAML parse error: {e}"))?;
        self.load_from_package(pkg)
    }

    /// Register an already-parsed Domain Package (validates + inserts).
    pub fn load_from_package(&mut self, pkg: DomainPackage) -> Result<String, String> {
        let domain_name = pkg.schema.domain.clone();
        self.validate(&pkg)?;
        self.packages.insert(domain_name.clone(), pkg);
        Ok(domain_name)
    }

    /// Validate a Domain Package before registration.
    pub fn validate(&self, pkg: &DomainPackage) -> Result<(), String> {
        if pkg.schema.domain.is_empty() {
            return Err("domain name cannot be empty".to_string());
        }
        if pkg.schema.version.is_empty() {
            return Err("version cannot be empty".to_string());
        }
        for label in &pkg.schema.labels {
            if label.name.is_empty() {
                return Err("label name cannot be empty".to_string());
            }
        }
        Ok(())
    }

    /// Get a loaded package by domain name.
    pub fn get(&self, domain: &str) -> Option<&DomainPackage> {
        self.packages.get(domain)
    }

    /// List all loaded domains.
    pub fn list(&self) -> Vec<String> {
        self.packages.keys().cloned().collect()
    }

    /// Remove a domain package.
    pub fn remove(&mut self, domain: &str) -> bool {
        self.packages.remove(domain).is_some()
    }

    /// Total number of labels across all domains.
    pub fn total_labels(&self) -> usize {
        self.packages
            .values()
            .flat_map(|p| p.schema.labels.iter())
            .count()
    }

    /// Total number of edge types across all domains.
    pub fn total_edge_types(&self) -> usize {
        self.packages
            .values()
            .flat_map(|p| p.schema.edge_types.iter())
            .count()
    }
}

impl Default for DomainLoader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GENERIC_DOMAIN_YAML: &str = r#"
schema:
  domain: generic
  version: "1.0"
  description: "通用对象图模型"
  labels:
    - name: Object
      description: "通用对象实体"
      properties:
        - name: id
          prop_type: string
          required: true
          indexed: true
        - name: type
          prop_type: string
        - name: status
          prop_type: string
    - name: Exception
      description: "异常/告警事件"
      properties:
        - name: id
          prop_type: string
          required: true
        - name: severity
          prop_type: string
          enum_values: [P0, P1, P2, P3, P4]
        - name: type
          prop_type: string
          required: true
        - name: message
          prop_type: string
        - name: timestamp
          prop_type: datetime
          required: true
  edge_types:
    - name: DEPENDS_ON
      description: "依赖关系"
      properties:
        - name: weight
          prop_type: float
          default: "1.0"
    - name: AFFECTS
      description: "影响关系（异常影响对象）"
      source_labels: [Exception]
      target_labels: [Object]
  constraints:
    - constraint_type: unique
      label: Object
      properties: [id]
    - constraint_type: unique
      label: Exception
      properties: [id]
  indexes:
    - label: Object
      properties: [status]
    - label: Exception
      properties: [severity, timestamp]
mappings: []
standing_queries: []
materialized_views: []
"#;

    const AIR_CARGO_YAML: &str = r#"
schema:
  domain: air_cargo_terminal
  version: "1.0"
  description: "智慧航空货站领域模型"
  extends: generic
  labels:
    - name: Device
      description: "设备（AGV/ETV/Forklift）"
      extends: Object
      properties:
        - name: device_type
          prop_type: string
          required: true
          enum_values: [AGV, ETV, Forklift, Scanner, Robot]
        - name: battery
          prop_type: float
    - name: Task
      description: "搬运任务"
      properties:
        - name: id
          prop_type: string
          required: true
          indexed: true
        - name: status
          prop_type: string
          enum_values: [PENDING, IN_PROGRESS, COMPLETED, FAILED, CANCELLED]
    - name: Flight
      description: "航班"
      properties:
        - name: id
          prop_type: string
          required: true
        - name: cutoff_time
          prop_type: datetime
  edge_types:
    - name: EXECUTING
      description: "设备正在执行任务"
      source_labels: [Device]
      target_labels: [Task]
    - name: ASSIGNED_TO
      description: "ULD/货物分配到航班"
      source_labels: [ULD, Piece]
      target_labels: [Flight]
mappings:
  - domain: air_cargo_terminal
    source: agv_status
    event_type: AGV_STATUS_CHANGED
    node:
      label: Device
      id: "$.device_id"
    properties:
      type: "'AGV'"
      status: "$.status"
      battery: "$.battery"
    edges:
      - edge_type: EXECUTING
        target_label: Task
        target_id: "$.task_id"
standing_queries:
  - name: agv_flight_impact
    description: "AGV故障影响航班分析"
    query: "MATCH (e:Exception)-[:AFFECTS]->(agv:Device {device_type:'AGV'}) ..."
    metadata:
      domain: air_cargo_terminal
      severity: P1
materialized_views: []
"#;

    #[test]
    fn test_load_generic_domain() {
        let mut loader = DomainLoader::new();
        let name = loader.load_from_yaml(GENERIC_DOMAIN_YAML).unwrap();
        assert_eq!(name, "generic");
        assert_eq!(loader.list(), vec!["generic"]);
    }

    #[test]
    fn test_load_air_cargo_domain() {
        let mut loader = DomainLoader::new();
        loader.load_from_yaml(GENERIC_DOMAIN_YAML).unwrap();
        let name = loader.load_from_yaml(AIR_CARGO_YAML).unwrap();
        assert_eq!(name, "air_cargo_terminal");
    }

    #[test]
    fn test_domain_label_count() {
        let mut loader = DomainLoader::new();
        loader.load_from_yaml(GENERIC_DOMAIN_YAML).unwrap();
        let pkg = loader.get("generic").unwrap();
        assert_eq!(pkg.schema.labels.len(), 2); // Object, Exception
        assert_eq!(pkg.schema.edge_types.len(), 2); // DEPENDS_ON, AFFECTS
    }

    #[test]
    fn test_air_cargo_has_mappings() {
        let mut loader = DomainLoader::new();
        loader.load_from_yaml(AIR_CARGO_YAML).unwrap();
        let pkg = loader.get("air_cargo_terminal").unwrap();
        assert_eq!(pkg.mappings.len(), 1);
        assert_eq!(pkg.mappings[0].event_type, "AGV_STATUS_CHANGED");
        assert_eq!(pkg.mappings[0].node.label, "Device");
    }

    #[test]
    fn test_validate_invalid_domain() {
        let invalid_yaml = r#"
schema:
  domain: ""
  version: ""
  labels: []
  edge_types: []
mappings: []
standing_queries: []
materialized_views: []
"#;
        let mut loader = DomainLoader::new();
        let result = loader.load_from_yaml(invalid_yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_domain() {
        let mut loader = DomainLoader::new();
        loader.load_from_yaml(GENERIC_DOMAIN_YAML).unwrap();
        assert!(loader.remove("generic"));
        assert!(!loader.remove("nonexistent"));
        assert!(loader.list().is_empty());
    }

    #[test]
    fn test_list_domains() {
        let mut loader = DomainLoader::new();
        loader.load_from_yaml(GENERIC_DOMAIN_YAML).unwrap();
        loader.load_from_yaml(AIR_CARGO_YAML).unwrap();
        let domains = loader.list();
        assert_eq!(domains.len(), 2);
        assert!(domains.contains(&"generic".to_string()));
        assert!(domains.contains(&"air_cargo_terminal".to_string()));
    }

    #[test]
    fn test_serialize_roundtrip() {
        let mut loader = DomainLoader::new();
        loader.load_from_yaml(AIR_CARGO_YAML).unwrap();
        let pkg = loader.get("air_cargo_terminal").unwrap();

        let serialized = serde_yaml::to_string(pkg).unwrap();
        let deserialized: DomainPackage = serde_yaml::from_str(&serialized).unwrap();

        assert_eq!(deserialized.schema.domain, "air_cargo_terminal");
        assert_eq!(deserialized.mappings.len(), 1);
    }
}
