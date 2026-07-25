//! Configuration loading via figment (TOML + env vars + CLI args).
//!
//! Priority order: CLI args > environment variables (NEXORA_ prefix) > nexora.toml > defaults
#![allow(dead_code)]

use nexora_core::wal::WalSyncPolicy;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Production group-commit tuning, matching the hardcoded default in
/// `GraphService::new_with_wal`. Kept here so a config-driven `group` policy
/// reproduces the exact durability/throughput profile the engine ships with.
const GROUP_MAX_OPS: usize = 256;
const GROUP_MAX_DELAY: Duration = Duration::from_micros(500);

/// Map a WAL `sync_policy` string (+ `sync_interval` for the `every_n` case) to
/// the engine's [`WalSyncPolicy`]. Case-insensitive.
///
/// - `group` (default): batch one fsync across concurrent committers — best
///   throughput, "ack = durable" preserved. Reproduces the engine's built-in
///   default tuning.
/// - `always`: fsync every write — lowest single-write durability latency floor,
///   worst throughput under load.
/// - `every_n`: fsync every `sync_interval` records — a crash can lose up to the
///   last `sync_interval - 1` writes. Not durable-on-ack.
/// - `never`: never fsync explicitly — relies on the OS; may lose recent data.
pub fn parse_wal_sync_policy(policy: &str, sync_interval: u64) -> Result<WalSyncPolicy, String> {
    match policy.trim().to_ascii_lowercase().as_str() {
        "group" => Ok(WalSyncPolicy::Group {
            max_ops: GROUP_MAX_OPS,
            max_delay: GROUP_MAX_DELAY,
            max_bytes: None,
        }),
        "always" => Ok(WalSyncPolicy::Always),
        "never" => Ok(WalSyncPolicy::Never),
        "every_n" | "everyn" | "every-n" => Ok(WalSyncPolicy::EveryN(sync_interval.max(1))),
        other => Err(format!(
            "unknown WAL sync policy {other:?} (expected: group, always, every_n, never)"
        )),
    }
}

/// Complete application configuration, matching the reference nexora.toml format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppTomlConfig {
    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub graph: GraphTomlConfig,

    #[serde(default)]
    pub storage: Option<StorageConfig>,

    #[serde(default)]
    pub blob: Option<BlobConfig>,

    #[serde(default)]
    pub cluster: Option<ClusterTomlConfig>,

    #[serde(default)]
    pub ingest: Option<IngestConfig>,

    #[serde(default)]
    pub logging: LoggingConfig,

    #[serde(default)]
    pub metrics: MetricsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default)]
    pub request_body_limit_mb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphTomlConfig {
    #[serde(default = "default_shards")]
    pub num_shards: usize,

    #[serde(default = "default_max_nodes")]
    pub max_nodes_per_shard: usize,

    #[serde(default = "default_channel_size")]
    pub node_channel_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default)]
    pub rocksdb: Option<RocksDbTomlConfig>,

    #[serde(default)]
    pub wal: Option<WalTomlConfig>,

    #[serde(default)]
    pub s3: Option<S3TomlConfig>,

    #[serde(default)]
    pub iceberg: Option<IcebergTomlConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDbTomlConfig {
    #[serde(default = "default_rocksdb_path")]
    pub path: String,

    #[serde(default = "default_write_buffer")]
    pub write_buffer_size: String,

    #[serde(default = "default_max_wb")]
    pub max_write_buffers: i32,

    #[serde(default = "default_compression")]
    pub compression: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalTomlConfig {
    #[serde(default = "default_wal_dir")]
    pub dir: String,

    #[serde(default = "default_sync_policy")]
    pub sync_policy: String,

    #[serde(default = "default_sync_interval")]
    pub sync_interval: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3TomlConfig {
    pub bucket: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default = "default_prefix")]
    pub prefix: String,
    #[serde(default)]
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergTomlConfig {
    pub catalog_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobConfig {
    pub endpoint: String,
    #[serde(default = "default_blob_quota")]
    pub quota: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterTomlConfig {
    #[serde(default = "default_cluster_mode")]
    pub mode: String,

    #[serde(default)]
    pub listen: Vec<String>,

    #[serde(default)]
    pub seeds: Vec<String>,

    #[serde(default)]
    pub node_id: Option<String>,

    #[serde(default)]
    pub raft_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IngestConfig {
    pub sources: Vec<IngestSourceToml>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestSourceToml {
    #[serde(rename = "type")]
    pub source_type: String,

    #[serde(default)]
    pub path: Option<String>,

    #[serde(default)]
    pub format: Option<String>,

    #[serde(default)]
    pub brokers: Option<String>,

    #[serde(default)]
    pub topic: Option<String>,

    #[serde(default)]
    pub group_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,

    #[serde(default = "default_log_format")]
    pub format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

// ---- Default values ----

fn default_host() -> String {
    "0.0.0.0".into()
}
fn default_port() -> u16 {
    8080
}
fn default_shards() -> usize {
    256
}
fn default_max_nodes() -> usize {
    10_000
}
fn default_channel_size() -> usize {
    64
}
fn default_rocksdb_path() -> String {
    "./nexora-data".into()
}
fn default_write_buffer() -> String {
    "64MB".into()
}
fn default_max_wb() -> i32 {
    3
}
fn default_compression() -> String {
    "lz4".into()
}
fn default_wal_dir() -> String {
    "./nexora-data/wal".into()
}
fn default_sync_policy() -> String {
    // Matches the engine's built-in production default (group commit). Changing
    // this string in nexora.toml now actually takes effect (see main.rs wiring),
    // so the default must reproduce the shipped behaviour.
    "group".into()
}
fn default_sync_interval() -> u64 {
    1000
}
fn default_region() -> String {
    "us-east-1".into()
}
fn default_prefix() -> String {
    "nexora/".into()
}
fn default_blob_quota() -> String {
    "10GB".into()
}
fn default_cluster_mode() -> String {
    "peer".into()
}
fn default_log_level() -> String {
    "info".into()
}
fn default_log_format() -> String {
    "json".into()
}
fn default_true() -> bool {
    true
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            request_body_limit_mb: 16,
        }
    }
}

impl Default for GraphTomlConfig {
    fn default() -> Self {
        Self {
            num_shards: default_shards(),
            max_nodes_per_shard: default_max_nodes(),
            node_channel_size: default_channel_size(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

/// Application profile / run mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RunProfile {
    /// In-memory storage, zero dependencies, ephemeral data.
    LiteEphemeral,
    /// Local RocksDB + WAL, durable single-node.
    SingleDurable,
    /// Cluster mode with Zenoh discovery + Raft consensus.
    Clustered,
}

impl RunProfile {
    pub fn as_str(&self) -> &str {
        match self {
            RunProfile::LiteEphemeral => "lite-ephemeral",
            RunProfile::SingleDurable => "single-durable",
            RunProfile::Clustered => "clustered",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "lite-ephemeral" | "lite" => Some(RunProfile::LiteEphemeral),
            "single-durable" | "durable" | "single" => Some(RunProfile::SingleDurable),
            "clustered" | "cluster" => Some(RunProfile::Clustered),
            _ => None,
        }
    }
}

impl std::str::FromStr for RunProfile {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str(s).ok_or_else(|| {
            format!("unknown profile '{s}'. Valid: lite-ephemeral, single-durable, clustered")
        })
    }
}

/// Validate that CLI arguments don't conflict with the chosen profile.
pub fn validate_profile(
    cli_no_rocksdb: bool,
    cli_cluster: bool,
    profile: RunProfile,
) -> Result<(), String> {
    match profile {
        RunProfile::LiteEphemeral => {
            if !cli_no_rocksdb {
                return Err("Profile 'lite-ephemeral' requires --no-rocksdb. Remove the conflicting RocksDB config or use 'single-durable' profile.".into());
            }
            if cli_cluster {
                return Err("Profile 'lite-ephemeral' cannot be used with --cluster. Use 'clustered' profile instead.".into());
            }
        }
        RunProfile::SingleDurable => {
            if cli_cluster {
                return Err("Profile 'single-durable' cannot be used with --cluster. Use 'clustered' profile or remove --cluster.".into());
            }
        }
        RunProfile::Clustered => {
            if cli_no_rocksdb {
                return Err(
                    "Profile 'clustered' requires durable storage (cannot use --no-rocksdb)."
                        .into(),
                );
            }
            if !cli_cluster {
                return Err("Profile 'clustered' requires --cluster flag. Remove --profile clustered or add --cluster.".into());
            }
        }
    }
    Ok(())
}

/// Attempt to load configuration from a nexora.toml file.
/// Returns None if the file doesn't exist or if an explicit path wasn't given.
pub fn load_toml_config(path: Option<&std::path::Path>) -> Option<AppTomlConfig> {
    let path = path.unwrap_or_else(|| std::path::Path::new("nexora.toml"));
    if !path.exists() {
        return None;
    }
    match std::fs::read_to_string(path) {
        Ok(content) => match toml::from_str::<AppTomlConfig>(&content) {
            Ok(cfg) => {
                tracing::info!("Loaded configuration from {}", path.display());
                Some(cfg)
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to parse {}: {e}. Using CLI/defaults.",
                    path.display()
                );
                None
            }
        },
        Err(e) => {
            tracing::warn!("Cannot read {}: {e}. Using CLI/defaults.", path.display());
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_from_str() {
        assert_eq!(
            RunProfile::from_str("lite-ephemeral"),
            Some(RunProfile::LiteEphemeral)
        );
        assert_eq!(
            RunProfile::from_str("single-durable"),
            Some(RunProfile::SingleDurable)
        );
        assert_eq!(
            RunProfile::from_str("clustered"),
            Some(RunProfile::Clustered)
        );
        assert_eq!(RunProfile::from_str("unknown"), None);
    }

    #[test]
    fn test_validate_profile_lite() {
        assert!(validate_profile(true, false, RunProfile::LiteEphemeral).is_ok());
        assert!(validate_profile(false, false, RunProfile::LiteEphemeral).is_err());
        assert!(validate_profile(true, true, RunProfile::LiteEphemeral).is_err());
    }

    #[test]
    fn test_validate_profile_cluster() {
        assert!(validate_profile(false, true, RunProfile::Clustered).is_ok());
        assert!(validate_profile(true, true, RunProfile::Clustered).is_err());
    }

    #[test]
    fn test_parse_minimal_toml() {
        let toml = r#"
[server]
host = "127.0.0.1"
port = 9090

[graph]
num_shards = 128

[logging]
level = "debug"
"#;
        let cfg: AppTomlConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.server.host, "127.0.0.1");
        assert_eq!(cfg.server.port, 9090);
        assert_eq!(cfg.graph.num_shards, 128);
        assert_eq!(cfg.logging.level, "debug");
    }

    #[test]
    fn test_parse_full_toml() {
        let toml = r#"
[server]
host = "0.0.0.0"
port = 8080

[graph]
num_shards = 256
max_nodes_per_shard = 5000

[storage.rocksdb]
path = "/var/nexora/data"
write_buffer_size = "128MB"
compression = "zstd"

[storage.wal]
dir = "/var/nexora/wal"
sync_policy = "always"

[logging]
level = "info"
format = "json"
"#;
        let cfg: AppTomlConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.graph.max_nodes_per_shard, 5000);
        let storage = cfg.storage.unwrap();
        let rocksdb = storage.rocksdb.unwrap();
        assert_eq!(rocksdb.path, "/var/nexora/data");
        assert_eq!(rocksdb.compression, "zstd");
        let wal = storage.wal.unwrap();
        assert_eq!(wal.sync_policy, "always");
    }

    #[test]
    fn test_parse_wal_sync_policy_variants() {
        assert!(matches!(
            parse_wal_sync_policy("group", 1000),
            Ok(WalSyncPolicy::Group { max_ops: 256, .. })
        ));
        assert!(matches!(
            parse_wal_sync_policy("always", 1000),
            Ok(WalSyncPolicy::Always)
        ));
        assert!(matches!(
            parse_wal_sync_policy("never", 1000),
            Ok(WalSyncPolicy::Never)
        ));
        assert!(matches!(
            parse_wal_sync_policy("every_n", 500),
            Ok(WalSyncPolicy::EveryN(500))
        ));
    }

    #[test]
    fn test_parse_wal_sync_policy_case_insensitive_and_aliases() {
        assert!(matches!(
            parse_wal_sync_policy("  ALWAYS ", 1000),
            Ok(WalSyncPolicy::Always)
        ));
        assert!(matches!(
            parse_wal_sync_policy("Every-N", 42),
            Ok(WalSyncPolicy::EveryN(42))
        ));
    }

    #[test]
    fn test_parse_wal_sync_policy_every_n_never_zero() {
        // sync_interval of 0 would make EveryN(0) fsync-never; clamp to 1.
        assert!(matches!(
            parse_wal_sync_policy("every_n", 0),
            Ok(WalSyncPolicy::EveryN(1))
        ));
    }

    #[test]
    fn test_parse_wal_sync_policy_rejects_unknown() {
        let err = parse_wal_sync_policy("fsync-please", 1000).unwrap_err();
        assert!(err.contains("unknown WAL sync policy"));
    }

    #[test]
    fn test_default_sync_policy_matches_engine_default() {
        // The dead-config default must reproduce the engine's shipped group-commit
        // behaviour so that omitting the setting doesn't silently change durability.
        assert!(matches!(
            parse_wal_sync_policy(&default_sync_policy(), default_sync_interval()),
            Ok(WalSyncPolicy::Group { max_ops: 256, .. })
        ));
    }
}
