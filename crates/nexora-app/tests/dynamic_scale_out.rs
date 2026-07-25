//! Multi-process dynamic scale-out e2e.
//!
//! Combines the previously-segmented verification into one end-to-end path:
//!   1. launch a real 3-node `nexora` cluster from per-node YAML configs,
//!   2. INSERT rows through node-a's PG-wire,
//!   3. launch a 4th node process and add it to the cluster via the real
//!      `/api/v2/cluster/add-node` HTTP admin endpoint (drives `ClusterManager::add_node`
//!      → control-plane membership update → RF-aware rebalance → remote-client
//!      registration),
//!   4. assert the rebalance reassigned shards to the new node, and
//!   5. assert the data is still fully readable through node-a's PG-wire after
//!      the membership change (COUNT is unchanged).
//!
//! `#[ignore]` by default: four processes plus cross-node TCP bring-up is
//! timing-sensitive. Run on demand with:
//!   cargo test -p nexora-app --test dynamic_scale_out -- --ignored --nocapture

use std::fs;
use std::io::Write as _;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::time::sleep;

struct NodeSpec {
    id: String,
    listen_addr: String,
    heartbeat_addr: String,
    http_port: u16,
    pg_port: u16,
}

fn find_available_port() -> u16 {
    static NEXT_PORT: AtomicU16 = AtomicU16::new(21000);
    loop {
        let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
        if port < 1024 {
            continue;
        }
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

fn alloc_nodes(n: usize) -> Vec<NodeSpec> {
    (0..n)
        .map(|i| NodeSpec {
            id: format!("node-{}", (b'a' + i as u8) as char),
            listen_addr: format!("127.0.0.1:{}", find_available_port()),
            heartbeat_addr: format!("127.0.0.1:{}", find_available_port()),
            http_port: find_available_port(),
            pg_port: find_available_port(),
        })
        .collect()
}

/// Write a per-node cluster config listing the given `peers` (by index into
/// `nodes`). The initial cluster's nodes list each other; the late-joining node
/// starts standalone (empty peers) and is wired in via the add-node endpoint.
fn write_node_config(
    dir: &Path,
    nodes: &[NodeSpec],
    idx: usize,
    peers: &[usize],
    rf: usize,
) -> PathBuf {
    let me = &nodes[idx];
    let mut yaml = String::new();
    yaml.push_str("cluster:\n");
    yaml.push_str("  name: \"scaleout\"\n");
    yaml.push_str("  total_shards: 8\n");
    yaml.push_str(&format!("  replication_factor: {rf}\n"));
    yaml.push_str("node:\n");
    yaml.push_str(&format!("  id: \"{}\"\n", me.id));
    yaml.push_str(&format!("  listen_addr: \"{}\"\n", me.listen_addr));
    yaml.push_str(&format!("  heartbeat_addr: \"{}\"\n", me.heartbeat_addr));
    yaml.push_str("peers:\n");
    if peers.is_empty() {
        yaml.push_str("  []\n");
    } else {
        for &j in peers {
            let peer = &nodes[j];
            yaml.push_str(&format!("  - node_id: \"{}\"\n", peer.id));
            yaml.push_str(&format!("    graph_addr: \"{}\"\n", peer.listen_addr));
            yaml.push_str(&format!(
                "    heartbeat_addr: \"{}\"\n",
                peer.heartbeat_addr
            ));
        }
    }
    yaml.push_str("health:\n");
    yaml.push_str("  heartbeat_interval_secs: 1\n");
    yaml.push_str("  failure_timeout_secs: 5\n");
    yaml.push_str("replication:\n");
    yaml.push_str("  write_timeout_secs: 5\n");

    let path = dir.join(format!("cluster-{}.yaml", me.id));
    let mut f = fs::File::create(&path).expect("create node config");
    f.write_all(yaml.as_bytes()).expect("write node config");
    path
}

fn spawn_node(config_path: &Path, node: &NodeSpec) -> Child {
    Command::new(env!("CARGO_BIN_EXE_nexora"))
        .arg("--cluster")
        .arg("--cluster-config")
        .arg(config_path)
        .arg("--port")
        .arg(node.http_port.to_string())
        .arg("--pg-port")
        .arg(node.pg_port.to_string())
        .arg("--pg-trust")
        .arg("--allow-unauthenticated")
        .arg("--no-rocksdb")
        .arg("--no-wal")
        .arg("--num-shards")
        .arg("8")
        .spawn()
        .expect("failed to spawn nexora node")
}

struct Guard(Child);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn connect_pg_with_retry(pg_port: u16, timeout: Duration) -> tokio_postgres::Client {
    let conn_str = format!("host=127.0.0.1 port={pg_port} user=nexora dbname=nexora");
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_err = None;
    while tokio::time::Instant::now() < deadline {
        match tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                return client;
            }
            Err(e) => {
                last_err = Some(e);
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
    panic!("could not connect to PG-wire on :{pg_port} within {timeout:?}: {last_err:?}");
}

async fn count_emp(client: &tokio_postgres::Client) -> i64 {
    let rows = client
        .simple_query("SELECT COUNT(*) FROM Emp")
        .await
        .expect("count query");
    rows.iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                r.get(0).and_then(|v| v.parse::<i64>().ok())
            }
            _ => None,
        })
        .expect("a COUNT(*) row")
}

/// End-to-end dynamic scale-out: 3 nodes → add a 4th → data stays readable.
#[tokio::test]
#[ignore]
async fn dynamic_scale_out_preserves_data() {
    let dir = std::env::temp_dir().join(format!("nexora-scaleout-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&dir).unwrap();

    // 4 specs: a/b/c form the initial cluster; d joins later.
    let nodes = alloc_nodes(4);

    // Initial 3-node cluster: each lists the other two as peers.
    let mut guards = Vec::new();
    for i in 0..3 {
        let peers: Vec<usize> = (0..3).filter(|&j| j != i).collect();
        let cfg = write_node_config(&dir, &nodes, i, &peers, 1);
        guards.push(Guard(spawn_node(&cfg, &nodes[i])));
    }

    // Let the 3-node cluster bind listeners and exchange heartbeats.
    sleep(Duration::from_secs(3)).await;

    // Insert 6 rows through node-a's PG-wire.
    let client_a = connect_pg_with_retry(nodes[0].pg_port, Duration::from_secs(30)).await;
    for i in 0..6 {
        client_a
            .simple_query(&format!(
                "INSERT INTO Emp (id, salary) VALUES ('e{i}', {})",
                100 + i * 10
            ))
            .await
            .unwrap_or_else(|e| panic!("insert e{i} failed: {e}"));
    }
    let before = count_emp(&client_a).await;
    assert_eq!(before, 6, "all 6 rows must be present before scale-out");

    // Launch the 4th node standalone (empty peers) so its listeners are up and
    // reachable for migration before we add it to the cluster.
    let cfg_d = write_node_config(&dir, &nodes, 3, &[], 1);
    guards.push(Guard(spawn_node(&cfg_d, &nodes[3])));

    // The control plane gates add-node behind `quorum_healthy()`, which only
    // turns true once the heartbeat/health loop has marked a majority of voters
    // alive. That loop ticks on `failure_timeout_secs` (5s here), so the first
    // health sweep lands several seconds after boot. Poll the real add-node
    // endpoint until quorum is ready (it returns 500 "no quorum" until then)
    // rather than racing a single fixed sleep — this is convergence, not a bug.
    let http = reqwest::Client::new();
    let add_node_url = format!(
        "http://127.0.0.1:{}/api/v2/cluster/add-node",
        nodes[0].http_port
    );
    let add_body = serde_json::json!({
        "node_id": nodes[3].id,
        "graph_addr": nodes[3].listen_addr,
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let (status, body) = loop {
        let resp = http
            .post(&add_node_url)
            .json(&add_body)
            .send()
            .await
            .expect("add-node request failed");
        let status = resp.status();
        let body: serde_json::Value = resp.json().await.expect("add-node json body");
        // Retry only the transient "no quorum" convergence window; any other
        // failure is a real error and should fail the test immediately.
        let transient_no_quorum = status == reqwest::StatusCode::INTERNAL_SERVER_ERROR
            && body
                .get("error")
                .and_then(|e| e.as_str())
                .is_some_and(|e| e.contains("no quorum"));
        if !transient_no_quorum || std::time::Instant::now() >= deadline {
            break (status, body);
        }
        sleep(Duration::from_secs(2)).await;
    };
    assert!(
        status.is_success(),
        "add-node must return 2xx, got {status}; body={body}"
    );
    let reassignments = body
        .get("reassignments")
        .and_then(|v| v.as_u64())
        .expect("reassignments field");
    assert!(
        reassignments > 0,
        "adding a 4th node to an 8-shard cluster must move at least one shard, got {reassignments}"
    );

    // Give the rebalance/migration a moment to settle.
    sleep(Duration::from_secs(2)).await;

    // The data must still be fully readable through node-a after the membership
    // change — this is the whole point of the migration path.
    let after = count_emp(&client_a).await;
    assert_eq!(
        after, before,
        "COUNT must be unchanged after scale-out (data migrated, not lost)"
    );

    // Every process must still be alive (none crashed during rebalance).
    for g in &mut guards {
        assert!(g.0.try_wait().unwrap().is_none(), "a node exited early");
    }

    drop(guards);
    fs::remove_dir_all(&dir).ok();
}
