//! Phase 5.1 Configuration Tests
//!
//! Validates TOML parsing for single-node and distributed Event Streaming modes.

#[cfg(feature = "event-streaming")]
#[test]
fn test_parse_event_streaming_single_mode() {
    let toml = r#"
[event_streaming]
enabled = true
mode = "single"
meta_addr = "127.0.0.1:5690"
frontend_addr = "127.0.0.1:4566"
"#;

    let cfg: nexora_app::config::EventStreamingConfig = toml::from_str(toml).unwrap();
    assert!(cfg.enabled);
    assert_eq!(cfg.mode, nexora_app::config::EventStreamingMode::Single);
    assert_eq!(cfg.meta_addr, "127.0.0.1:5690");
    assert_eq!(cfg.frontend_addr, "127.0.0.1:4566");
}

#[cfg(all(feature = "event-streaming", feature = "library"))]
#[test]
fn test_parse_event_streaming_distributed_mode() {
    let toml = r#"
[event_streaming]
enabled = true
mode = "distributed"

[event_streaming.distributed]
enabled = true
node_id = "meta-1"
raft_node_id = 1
data_dir = "/data/nexora/raft"

[event_streaming.distributed.meta]
listen_addr = "127.0.0.1:5690"

[[event_streaming.distributed.meta.peers]]
node_id = 2
addr = "127.0.0.1:5691"

[[event_streaming.distributed.meta.peers]]
node_id = 3
addr = "127.0.0.1:5692"

[event_streaming.distributed.consensus]
data_dir = "/data/nexora/raft"
heartbeat_interval_secs = 1
election_timeout_secs = 5
"#;

    let cfg: nexora_app::config::EventStreamingConfig = toml::from_str(toml).unwrap();
    assert!(cfg.enabled);
    assert_eq!(cfg.mode, nexora_app::config::EventStreamingMode::Distributed);

    let dist = cfg.distributed.expect("distributed config should exist");
    assert!(dist.enabled);
    assert_eq!(dist.node_id, "meta-1");
    assert_eq!(dist.raft_node_id, 1);
    assert_eq!(dist.data_dir, "/data/nexora/raft");

    assert_eq!(dist.meta.listen_addr, "127.0.0.1:5690");
    assert_eq!(dist.meta.peers.len(), 2);
    assert_eq!(dist.meta.peers[0].node_id, 2);
    assert_eq!(dist.meta.peers[0].addr, "127.0.0.1:5691");
    assert_eq!(dist.meta.peers[1].node_id, 3);
    assert_eq!(dist.meta.peers[1].addr, "127.0.0.1:5692");

    let consensus = dist.consensus.expect("consensus config should exist");
    assert_eq!(consensus.data_dir, "/data/nexora/raft");
    assert_eq!(consensus.heartbeat_interval_secs, 1);
    assert_eq!(consensus.election_timeout_secs, 5);
}

#[cfg(feature = "event-streaming")]
#[test]
fn test_event_streaming_mode_default() {
    let toml = r#"
[event_streaming]
enabled = true
"#;

    let cfg: nexora_app::config::EventStreamingConfig = toml::from_str(toml).unwrap();
    assert!(cfg.enabled);
    assert_eq!(cfg.mode, nexora_app::config::EventStreamingMode::Single);
}

#[cfg(feature = "event-streaming")]
#[test]
fn test_full_app_config_with_event_streaming() {
    let toml = r#"
[server]
host = "127.0.0.1"
port = 8080

[event_streaming]
enabled = true
mode = "single"
meta_addr = "127.0.0.1:5690"
frontend_addr = "127.0.0.1:4566"
"#;

    let cfg: nexora_app::config::AppTomlConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.server.host, "127.0.0.1");
    assert_eq!(cfg.server.port, 8080);

    let es = cfg.event_streaming.expect("event_streaming should exist");
    assert!(es.enabled);
    assert_eq!(es.mode, nexora_app::config::EventStreamingMode::Single);
}
