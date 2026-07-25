use nexora_zenoh::ClusterConfig;
use std::io::Write;
use std::time::Duration;
use tempfile::NamedTempFile;

#[test]
fn test_load_single_node_config() {
    let yaml = r#"
cluster:
  name: "test-cluster"
  total_shards: 64
  replication_factor: 1

node:
  id: "node-1"
  listen_addr: "127.0.0.1:7000"
  heartbeat_addr: "127.0.0.1:7001"

peers: []

health:
  heartbeat_interval_secs: 5
  failure_timeout_secs: 15

replication:
  log_dir: null
  write_timeout_secs: 10
"#;

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(yaml.as_bytes()).unwrap();
    file.flush().unwrap();

    let config = ClusterConfig::from_file(file.path()).unwrap();

    assert_eq!(config.node_id, "node-1");
    assert_eq!(config.listen_addr, "127.0.0.1:7000");
    assert_eq!(config.heartbeat_addr, "127.0.0.1:7001");
    assert_eq!(config.total_shards, 64);
    assert_eq!(config.replication_factor, 1);
    assert!(config.peers.is_empty());
    assert_eq!(config.heartbeat_interval, Duration::from_secs(5));
    assert_eq!(config.failure_timeout, Duration::from_secs(15));
    assert!(config.replication_log_dir.is_none());
}

#[test]
fn test_load_three_node_config() {
    let yaml = r#"
cluster:
  name: "test-cluster"
  total_shards: 256
  replication_factor: 3

node:
  id: "node-1"
  listen_addr: "127.0.0.1:7000"
  heartbeat_addr: "127.0.0.1:7001"

peers:
  - node_id: "node-2"
    graph_addr: "127.0.0.1:7010"
    heartbeat_addr: "127.0.0.1:7011"
  - node_id: "node-3"
    graph_addr: "127.0.0.1:7020"
    heartbeat_addr: "127.0.0.1:7021"

health:
  heartbeat_interval_secs: 3
  failure_timeout_secs: 10

replication:
  log_dir: "/var/lib/nexora/replog"
  write_timeout_secs: 5
"#;

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(yaml.as_bytes()).unwrap();
    file.flush().unwrap();

    let config = ClusterConfig::from_file(file.path()).unwrap();

    assert_eq!(config.node_id, "node-1");
    assert_eq!(config.total_shards, 256);
    assert_eq!(config.replication_factor, 3);
    assert_eq!(config.peers.len(), 2);
    assert_eq!(config.peers[0].node_id, "node-2");
    assert_eq!(config.peers[0].graph_addr, "127.0.0.1:7010");
    assert_eq!(config.peers[1].node_id, "node-3");
    assert_eq!(config.heartbeat_interval, Duration::from_secs(3));
    assert_eq!(config.failure_timeout, Duration::from_secs(10));
    assert_eq!(
        config.replication_log_dir,
        Some(std::path::PathBuf::from("/var/lib/nexora/replog"))
    );
}

#[test]
fn test_load_invalid_yaml() {
    let yaml = "this is not valid yaml: [[[";
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(yaml.as_bytes()).unwrap();
    file.flush().unwrap();

    let result = ClusterConfig::from_file(file.path());
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("parse YAML"));
}

#[test]
fn test_load_missing_fields() {
    let yaml = r#"
cluster:
  name: "incomplete"
"#;
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(yaml.as_bytes()).unwrap();
    file.flush().unwrap();

    let result = ClusterConfig::from_file(file.path());
    assert!(result.is_err());
}

#[test]
fn test_load_nonexistent_file() {
    let result = ClusterConfig::from_file("/nonexistent/path/config.yaml");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("failed to read"));
}
