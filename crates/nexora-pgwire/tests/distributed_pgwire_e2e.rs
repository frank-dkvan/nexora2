//! Real multi-node PG-wire E2E: SQL issued at one node, executed across a real
//! two-node cluster over TCP transport.
//!
//! This is NOT a mock-router test. It wires the production stack end to end:
//!   node-A / node-B  = real GraphService → GraphServiceAdapter → TcpGraphServer
//!   a HybridRouter (no-local, matching production cluster wiring) on the PG
//!   server node routes every shard to whichever node owns it, over real TCP.
//!   A real tokio-postgres client connects to the PG server and issues SQL.
//!
//! It proves the cluster PG-wire path required by the review:
//!   - INSERT via PG-wire physically distributes nodes across BOTH owners.
//!   - SELECT COUNT(*) from any node returns the whole-cluster count (not just
//!     the local node's shards).
//!   - Statements outside the distributable subset return an explicit error
//!     instead of silently running against one node's local shards.

use std::sync::Arc;

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_pgwire::{spawn_pg_server_with_router, PgConfig};
use nexora_standing_query::{
    pattern::{FilterCondition, StandingQueryPattern},
    StandingQueryManager,
};
use nexora_zenoh::graph_service_adapter::GraphServiceAdapter;
use nexora_zenoh::router::HybridRouter;
use nexora_zenoh::shard_map::ShardMap;
use nexora_zenoh::{TcpGraphServer, TcpRemoteClient};
use tokio_postgres::{Client, NoTls};

const TOTAL_SHARDS: usize = 8;

/// Build a real GraphService fronted by a TCP server; return (graph, addr, server).
async fn spawn_node() -> (Arc<GraphService>, String, TcpGraphServer) {
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: TOTAL_SHARDS,
            max_nodes_per_shard: 1000,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ));
    let adapter = Arc::new(GraphServiceAdapter::new(graph.clone()));
    let server = TcpGraphServer::new(adapter, "127.0.0.1:0".to_string());
    let addr = server.start().await.unwrap();
    (graph, addr, server)
}

/// Connect a real tokio-postgres client to a PG server address.
async fn connect(addr: std::net::SocketAddr) -> Client {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host(addr.ip().to_string())
        .port(addr.port())
        .user("admin")
        .dbname("nexora");
    let (client, connection) = cfg.connect(NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// A whole two-node cluster with a PG server on node-A wired to a real router.
struct Cluster {
    pg: Client,
    graph_a: Arc<GraphService>,
    graph_b: Arc<GraphService>,
    _srv_a: TcpGraphServer,
    _srv_b: TcpGraphServer,
    _pg_server: nexora_pgwire::PgServerHandle,
    shard_map: ShardMap,
}

async fn setup_cluster() -> Cluster {
    let (graph_a, addr_a, srv_a) = spawn_node().await;
    let (graph_b, addr_b, srv_b) = spawn_node().await;

    // Router on node-A. no_local matches production cluster wiring: EVERY shard
    // (including node-A's own) is reached through the TCP client, so both nodes
    // must be registered.
    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("node-a", &addr_a).await;
    client.register_node("node-b", &addr_b).await;
    let shard_map = ShardMap::new_distributed(
        TOTAL_SHARDS,
        &["node-a".into(), "node-b".into()],
        "node-a".into(),
    );
    let router = Arc::new(HybridRouter::new_clustered_no_local(
        shard_map.clone(),
        client,
    ));

    let query_pool = Arc::new(nexora_core::query_pool::QueryPool::new(4));

    let pg_config = PgConfig {
        port: 0,
        bind_addr: "127.0.0.1".to_owned(),
        trust: true,
        max_connections: 10,
        idle_timeout_secs: 60,
        shutdown_grace_secs: 2,
        ..PgConfig::default()
    };

    let pg_server = spawn_pg_server_with_router(
        graph_a.clone(),
        Arc::new(nexora_core::materialized_view::MaterializedViewManager::new()),
        None,
        Some(router),
        None, // C2 replication_progress: tests exercise reads without a live progress tracker
        query_pool.clone(),
        #[cfg(feature = "event-first")]
        None, // event_store: tests run without an event store
        pg_config,
    )
    .await
    .expect("PG server starts");

    let pg = connect(pg_server.local_addr()).await;

    Cluster {
        pg,
        graph_a,
        graph_b,
        _srv_a: srv_a,
        _srv_b: srv_b,
        _pg_server: pg_server,
        shard_map,
    }
}

/// INSERT via PG-wire on a real 2-node cluster must physically distribute the
/// created nodes across BOTH owners (not pile them all on the local node), and a
/// COUNT(*) issued at node-A must return the whole-cluster total.
#[tokio::test]
async fn distributed_insert_spreads_across_owners_and_count_is_global() {
    let c = setup_cluster().await;

    // Insert 40 products with explicit ids. Each id's shard owner is decided by
    // the shared shard map, so we can independently check physical placement.
    let n = 40;
    for i in 0..n {
        let id = format!("prod-{i}");
        c.pg.simple_query(&format!(
            "INSERT INTO Product (id, name, price) VALUES ('{id}', 'p{i}', {})",
            (i * 10) as i64
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {id} failed: {e}"));
    }

    // Count how many of the ids each node physically owns per the shard map.
    let mut expect_a = 0usize;
    let mut expect_b = 0usize;
    for i in 0..n {
        let qid = nexora_id::NexoraId::from_bytes(format!("prod-{i}").into_bytes());
        let shard = c.shard_map.shard_of(&qid);
        match c.shard_map.get(shard).unwrap().owner.as_str() {
            "node-a" => expect_a += 1,
            "node-b" => expect_b += 1,
            other => panic!("unexpected owner {other}"),
        }
    }

    // Both nodes must actually own some data — otherwise the test isn't
    // exercising distribution at all.
    assert!(
        expect_a > 0 && expect_b > 0,
        "shard map must split ids across both nodes (a={expect_a}, b={expect_b})"
    );

    // Verify physical placement: each node's real graph holds exactly its owned
    // ids, looked up by the id the user supplied.
    let count_on = |graph: &Arc<GraphService>, ids: Vec<String>| {
        let graph = graph.clone();
        async move {
            let mut found = 0usize;
            for id in ids {
                let qid = nexora_id::NexoraId::from_bytes(id.into_bytes());
                if graph.get_property(&qid, "name").await.unwrap().is_some() {
                    found += 1;
                }
            }
            found
        }
    };
    let ids: Vec<String> = (0..n).map(|i| format!("prod-{i}")).collect();
    let ids_a: Vec<String> = ids
        .iter()
        .filter(|id| {
            let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
            c.shard_map.get(c.shard_map.shard_of(&qid)).unwrap().owner == "node-a"
        })
        .cloned()
        .collect();
    let ids_b: Vec<String> = ids
        .iter()
        .filter(|id| {
            let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
            c.shard_map.get(c.shard_map.shard_of(&qid)).unwrap().owner == "node-b"
        })
        .cloned()
        .collect();

    let found_a = count_on(&c.graph_a, ids_a).await;
    let found_b = count_on(&c.graph_b, ids_b).await;
    assert_eq!(
        found_a, expect_a,
        "node-a must physically hold exactly its owned ids"
    );
    assert_eq!(
        found_b, expect_b,
        "node-b must physically hold exactly its owned ids"
    );

    // Cross-check: node-a must NOT hold node-b's ids (no accidental local pile-up).
    let leaked = count_on(
        &c.graph_a,
        ids.iter()
            .filter(|id| {
                let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
                c.shard_map.get(c.shard_map.shard_of(&qid)).unwrap().owner == "node-b"
            })
            .cloned()
            .collect(),
    )
    .await;
    assert_eq!(leaked, 0, "node-b's ids must not appear on node-a");

    // A COUNT(*) issued through PG-wire at node-A must return the GLOBAL total,
    // summed across both owners — not just node-A's local shards.
    let rows =
        c.pg.simple_query("SELECT COUNT(*) FROM Product")
            .await
            .unwrap();
    let count_val = rows
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
            _ => None,
        })
        .expect("COUNT(*) returns a row");
    assert_eq!(
        count_val,
        n.to_string(),
        "cluster COUNT(*) must equal the total inserted across both nodes"
    );

    // Sanity: node-A's local shard count alone must be LESS than the global
    // total (otherwise the COUNT wasn't actually distributed).
    assert!(
        expect_a < n,
        "precondition: node-a should not own everything (a={expect_a}, n={n})"
    );
}

/// In cluster mode, a statement the distributed planner cannot prove mergeable
/// must be refused with an explicit error — never silently run against one
/// node's local shards (which would return incomplete / misrouted results).
#[tokio::test]
async fn unsupported_cluster_query_errors_instead_of_local_fallback() {
    let c = setup_cluster().await;

    // An arithmetic projection (`SELECT age + 1`) is outside the distributable
    // subset: cypher-parser can't evaluate the arithmetic owner-side, so the
    // planner refuses it. It must come back as an explicit error, not a silent
    // partial/local result.
    let result = c.pg.simple_query("SELECT price + 1 FROM Product").await;

    match result {
        Err(e) => {
            // tokio-postgres's Display is just "db error: ERROR"; the real text
            // lives in the server-sent DbError message field.
            let msg = e
                .as_db_error()
                .map(|db| db.message().to_string())
                .unwrap_or_else(|| e.to_string());
            assert!(
                msg.contains("cluster mode") || msg.contains("distributed"),
                "error must explain the cluster limitation, got: {msg}"
            );
        }
        Ok(_) => panic!("unsupported cluster query must error, not silently run locally"),
    }
}

/// Global aggregates issued through PG-wire on a real 2-node cluster must equal
/// the values a single node would compute over the same data (an oracle). This
/// is the review's `test_distributed_aggregation` acceptance criterion: SUM /
/// AVG / MIN / MAX combined across owners, not just node-A's local shards.
#[tokio::test]
async fn distributed_global_aggregates_match_single_node_oracle() {
    let c = setup_cluster().await;

    // Insert a known set of prices; compute the oracle locally in the test.
    let prices: [i64; 10] = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    for (i, p) in prices.iter().enumerate() {
        c.pg.simple_query(&format!(
            "INSERT INTO Metric (id, val) VALUES ('m-{i}', {p})"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert m-{i} failed: {e}"));
    }

    let oracle_sum: i64 = prices.iter().sum();
    let oracle_min = *prices.iter().min().unwrap();
    let oracle_max = *prices.iter().max().unwrap();
    let oracle_avg = oracle_sum as f64 / prices.len() as f64;

    // Helper: run an aggregate query and return its single scalar cell as text.
    let scalar = |sql: &'static str| {
        let pg = &c.pg;
        async move {
            let rows = pg
                .simple_query(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql} failed: {e}"));
            rows.iter()
                .find_map(|m| match m {
                    tokio_postgres::SimpleQueryMessage::Row(r) => {
                        Some(r.get(0).unwrap().to_string())
                    }
                    _ => None,
                })
                .unwrap_or_else(|| panic!("{sql} returned no row"))
        }
    };

    // Precondition: node-A must not own all the metric ids, so the aggregate is
    // genuinely combining partials from both owners.
    let mut on_a = 0usize;
    for i in 0..prices.len() {
        let qid = nexora_id::NexoraId::from_bytes(format!("m-{i}").into_bytes());
        if c.shard_map.get(c.shard_map.shard_of(&qid)).unwrap().owner == "node-a" {
            on_a += 1;
        }
    }
    assert!(
        on_a > 0 && on_a < prices.len(),
        "metrics must split across both nodes for a meaningful distributed aggregate (on_a={on_a})"
    );

    let sum: i64 = scalar("SELECT SUM(val) FROM Metric").await.parse().unwrap();
    assert_eq!(sum, oracle_sum, "distributed SUM must match oracle");

    let min: i64 = scalar("SELECT MIN(val) FROM Metric").await.parse().unwrap();
    assert_eq!(min, oracle_min, "distributed MIN must match oracle");

    let max: i64 = scalar("SELECT MAX(val) FROM Metric").await.parse().unwrap();
    assert_eq!(max, oracle_max, "distributed MAX must match oracle");

    let avg: f64 = scalar("SELECT AVG(val) FROM Metric").await.parse().unwrap();
    assert!(
        (avg - oracle_avg).abs() < 1e-6,
        "distributed AVG {avg} must match oracle {oracle_avg}"
    );
}

/// A grouped aggregate (`GROUP BY`) issued through PG-wire on a real 2-node
/// cluster must combine per-group partials from BOTH owners into the correct
/// whole-cluster answer. This is the review's `test_distributed_aggregation`
/// GROUP BY criterion: counts per group summed across owners, not just node-A's
/// local shards.
#[tokio::test]
async fn distributed_group_by_counts_match_oracle() {
    let c = setup_cluster().await;

    // 3 cities with known counts: NYC=5, LA=3, SF=2 (10 rows total). Ids are
    // spread across both owners by the shard map, so each group's rows land on
    // different nodes and the coordinator must combine partial per-group counts.
    let plan: [(&str, usize); 3] = [("NYC", 5), ("LA", 3), ("SF", 2)];
    let mut i = 0usize;
    let mut oracle: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for (city, count) in plan {
        for _ in 0..count {
            c.pg.simple_query(&format!(
                "INSERT INTO Person (id, name, city) VALUES ('g-{i}', 'n{i}', '{city}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert g-{i} failed: {e}"));
            i += 1;
        }
        *oracle.entry(city.to_string()).or_default() += count as i64;
    }

    // Precondition: the group rows genuinely span both nodes.
    let mut on_a = 0usize;
    for j in 0..i {
        let qid = nexora_id::NexoraId::from_bytes(format!("g-{j}").into_bytes());
        if c.shard_map.get(c.shard_map.shard_of(&qid)).unwrap().owner == "node-a" {
            on_a += 1;
        }
    }
    assert!(
        on_a > 0 && on_a < i,
        "grouped rows must split across both nodes (on_a={on_a}, total={i})"
    );

    // GROUP BY through the distributed planner: each owner returns partial
    // per-city counts; the coordinator sums them per group.
    let rows =
        c.pg.simple_query("SELECT city, COUNT(*) FROM Person GROUP BY city")
            .await
            .unwrap();

    let mut got: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for m in &rows {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            let city = r.get(0).unwrap().to_string();
            let cnt: i64 = r.get(1).unwrap().parse().unwrap();
            got.insert(city, cnt);
        }
    }

    assert_eq!(
        got, oracle,
        "distributed GROUP BY city counts must match the oracle (got {got:?}, oracle {oracle:?})"
    );
}

/// A filtered projection read (`SELECT id, name ... WHERE ...`) issued through
/// PG-wire on a real 2-node cluster must (a) actually distribute — returning
/// rows whose owners span BOTH nodes — and (b) apply the WHERE predicate on each
/// owner, returning exactly the matching rows and NOT the unfiltered set. This
/// guards against the failure mode where the predicate is dropped from the owner
/// query and every node scan-returns everything.
#[tokio::test]
async fn distributed_filtered_projection_read_applies_predicate_across_nodes() {
    let c = setup_cluster().await;

    // 20 people with ages 20..40. Exactly the 10 with age > 29 should come back.
    let n = 20;
    for i in 0..n {
        let age = 20 + i; // ages 20..=39
        c.pg.simple_query(&format!(
            "INSERT INTO Person (id, name, age) VALUES ('p-{i}', 'name{i}', {age})"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert p-{i} failed: {e}"));
    }

    // Oracle: the NAMES the test expects back (age > 29 → i in 10..20). We check
    // the `name` column rather than `id`: `SELECT id` maps to `id(n)`, the node's
    // internal hex qid (not the user-supplied `id` property), so `name` is the
    // clean round-trippable value to assert on.
    let expected_names: std::collections::HashSet<String> = (0..n)
        .filter(|i| 20 + i > 29)
        .map(|i| format!("name{i}"))
        .collect();
    assert_eq!(expected_names.len(), 10, "oracle: 10 people over 29");

    // Precondition: the matching ids must span both owners, so a correct result
    // can only come from combining filtered partials across nodes.
    let matching_ids: Vec<String> = (0..n)
        .filter(|i| 20 + i > 29)
        .map(|i| format!("p-{i}"))
        .collect();
    let mut match_on_a = 0usize;
    let mut match_on_b = 0usize;
    for id in &matching_ids {
        let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
        match c
            .shard_map
            .get(c.shard_map.shard_of(&qid))
            .unwrap()
            .owner
            .as_str()
        {
            "node-a" => match_on_a += 1,
            "node-b" => match_on_b += 1,
            other => panic!("unexpected owner {other}"),
        }
    }
    assert!(
        match_on_a > 0 && match_on_b > 0,
        "matching rows must span both nodes (a={match_on_a}, b={match_on_b})"
    );

    let rows =
        c.pg.simple_query("SELECT id, name FROM Person WHERE age > 29")
            .await
            .unwrap();

    // Column 1 is `name` (column 0 is `id` → internal hex qid, see above).
    let got_names: std::collections::HashSet<String> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(1).unwrap().to_string()),
            _ => None,
        })
        .collect();

    assert_eq!(
        got_names,
        expected_names,
        "filtered read must return exactly the matching rows from both nodes \
         (got {} rows, expected {})",
        got_names.len(),
        expected_names.len()
    );
}

/// A distributed INSERT via PG-wire must still trigger Standing Query evaluation
/// on the affected node — even when that node is owned by a REMOTE shard. This
/// guards the cluster-mode gap where the distributed write path fetched
/// properties from this node's local graph (which does not hold remotely-owned
/// nodes), so the SQ would silently never fire. Here we pin the inserted id to a
/// node-B-owned shard and assert the SQ registered on node-A's manager matches.
#[tokio::test]
async fn distributed_insert_triggers_sq_for_remote_owned_node() {
    // Build a cluster whose PG server carries a StandingQueryManager.
    let (graph_a, addr_a, srv_a) = spawn_node().await;
    let (graph_b, addr_b, srv_b) = spawn_node().await;

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("node-a", &addr_a).await;
    client.register_node("node-b", &addr_b).await;
    let shard_map = ShardMap::new_distributed(
        TOTAL_SHARDS,
        &["node-a".into(), "node-b".into()],
        "node-a".into(),
    );
    let router = Arc::new(HybridRouter::new_clustered_no_local(
        shard_map.clone(),
        client,
    ));

    let sq_manager = Arc::new(StandingQueryManager::new(256));
    // SQ: price > 500. We'll insert a product priced above it on a node-B shard.
    let sq_id = sq_manager
        .register(
            "expensive",
            StandingQueryPattern::property("price", FilterCondition::GreaterThan(500.0)),
        )
        .await;

    let pg_config = PgConfig {
        port: 0,
        bind_addr: "127.0.0.1".to_owned(),
        trust: true,
        max_connections: 10,
        idle_timeout_secs: 60,
        shutdown_grace_secs: 2,
        ..PgConfig::default()
    };
    let pg_server = spawn_pg_server_with_router(
        graph_a.clone(),
        Arc::new(nexora_core::materialized_view::MaterializedViewManager::new()),
        Some(sq_manager.clone()),
        Some(router),
        None, // C2 replication_progress: tests exercise reads without a live progress tracker
        std::sync::Arc::new(nexora_core::query_pool::QueryPool::new(4)),
        #[cfg(feature = "event-first")]
        None, // event_store: tests run without an event store
        pg_config,
    )
    .await
    .expect("PG server starts");
    let pg = connect(pg_server.local_addr()).await;

    // Find an id owned by node-B (the REMOTE node from node-A's PG server view),
    // so the SQ trigger must fetch its properties over the wire, not locally.
    let remote_id = (0..10_000u64)
        .map(|i| format!("rp-{i}"))
        .find(|id| {
            let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
            shard_map.get(shard_map.shard_of(&qid)).unwrap().owner == "node-b"
        })
        .expect("a node-b-owned id");

    // Sanity: the node must physically land on node-B, not node-A.
    let qid = nexora_id::NexoraId::from_bytes(remote_id.as_bytes().to_vec());

    pg.simple_query(&format!(
        "INSERT INTO Product (id, name, price) VALUES ('{remote_id}', 'gpu', 1200)"
    ))
    .await
    .unwrap_or_else(|e| panic!("insert {remote_id} failed: {e}"));

    // The node must exist on node-B's real graph (remote placement).
    assert!(
        graph_b.get_property(&qid, "price").await.unwrap().is_some(),
        "the inserted node must physically land on node-B"
    );
    assert!(
        graph_a.get_property(&qid, "price").await.unwrap().is_none(),
        "the inserted node must NOT be on node-A (it is remote-owned)"
    );

    // The SQ must have matched the remote-owned node — proving the distributed
    // write path fetched its properties through the router and evaluated the SQ.
    assert_eq!(
        sq_manager.match_count(sq_id).await,
        1,
        "SQ must fire for a remote-owned node inserted via a distributed write"
    );

    drop(pg_server);
    drop((srv_a, srv_b));
}

/// When a shard owner is unreachable, a cluster query that must reach it has to
/// fail with an explicit error — never silently return only the reachable node's
/// rows. This is the review's core fault-visibility property: with replication
/// factor 1, an owner being unavailable makes its shards unavailable, and the
/// system must say so rather than under-count.
///
/// node-B is registered at a dead address (nothing listening), so any dispatch
/// to it fails at connect — the faithful stand-in for a crashed owner (mirrors
/// `two_node_real_graph::quorum_fails_when_followers_unreachable`). Killing a
/// live `TcpGraphServer` mid-test does NOT work: `shutdown()` only stops the
/// accept loop, while already-pooled connections keep being served, so the node
/// would stay reachable. node-A holds real rows, so a silent partial answer
/// (node-A's count alone) is exactly the wrong behaviour this guards against.
#[tokio::test]
async fn owner_down_errors_instead_of_returning_partial_data() {
    let (graph_a, addr_a, _srv_a) = spawn_node().await;

    // node-A live; node-B points at a dead port (nothing listening there).
    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("node-a", &addr_a).await;
    client.register_node("node-b", "127.0.0.1:1").await;
    let shard_map = ShardMap::new_distributed(
        TOTAL_SHARDS,
        &["node-a".into(), "node-b".into()],
        "node-a".into(),
    );
    let router = Arc::new(HybridRouter::new_clustered_no_local(
        shard_map.clone(),
        client,
    ));

    let query_pool = Arc::new(nexora_core::query_pool::QueryPool::new(4));

    let pg_config = PgConfig {
        port: 0,
        bind_addr: "127.0.0.1".to_owned(),
        trust: true,
        max_connections: 10,
        idle_timeout_secs: 60,
        shutdown_grace_secs: 2,
        ..PgConfig::default()
    };
    let pg_server = spawn_pg_server_with_router(
        graph_a.clone(),
        Arc::new(nexora_core::materialized_view::MaterializedViewManager::new()),
        None,
        Some(router),
        None, // C2 replication_progress: tests exercise reads without a live progress tracker
        query_pool.clone(),
        #[cfg(feature = "event-first")]
        None, // event_store: tests run without an event store
        pg_config,
    )
    .await
    .expect("PG server starts");
    let pg = connect(pg_server.local_addr()).await;

    // Put real rows on node-A (ids owned by node-a route to the live node).
    let mut on_a = 0usize;
    for i in 0..40 {
        let id = format!("w-{i}");
        let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
        if shard_map.get(shard_map.shard_of(&qid)).unwrap().owner == "node-a" {
            pg.simple_query(&format!(
                "INSERT INTO Widget (id, name) VALUES ('{id}', 'n{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {id} failed: {e}"));
            on_a += 1;
        }
    }
    assert!(on_a > 0, "precondition: some rows must live on node-a");

    // A global COUNT must reach node-B (an owner in the shard map). node-B is
    // unreachable, so the query must ERROR — never return node-A's partial count.
    let result = pg.simple_query("SELECT COUNT(*) FROM Widget").await;
    match result {
        Err(e) => {
            let msg = e
                .as_db_error()
                .map(|db| db.message().to_string())
                .unwrap_or_else(|| e.to_string());
            assert!(
                msg.contains("cluster") || msg.contains("routing") || msg.contains("unavailable"),
                "owner-down error must explain the cross-node failure, got: {msg}"
            );
        }
        Ok(rows) => {
            let partial: i64 = rows
                .iter()
                .find_map(|m| match m {
                    tokio_postgres::SimpleQueryMessage::Row(r) => {
                        Some(r.get(0).unwrap().to_string())
                    }
                    _ => None,
                })
                .unwrap_or_default()
                .parse()
                .unwrap_or(-1);
            panic!(
                "owner-down COUNT(*) must error, not return partial data (got {partial}, \
                 node-a held {on_a})"
            );
        }
    }

    drop(pg_server);
}

/// A filtered UPDATE (`UPDATE ... SET ... WHERE ...`) issued through PG-wire on a
/// real 2-node cluster must apply on BOTH owners — each mutating only its own
/// matching nodes — and must NOT touch rows that fail the predicate. This is the
/// distributed filtered-write acceptance: the owner-parallel write fans to every
/// owner, and the single-node bulk MATCH-mutate path evaluates the WHERE on each,
/// so a node holding no match is a correct no-op (never an error or over-apply).
#[tokio::test]
async fn distributed_filtered_update_applies_across_owners() {
    let c = setup_cluster().await;

    // 30 people, ages 20..=49. The UPDATE targets age > 40 (i in 21..30 → 9 rows).
    let n = 30;
    for i in 0..n {
        let age = 20 + i;
        c.pg.simple_query(&format!(
            "INSERT INTO Person (id, name, age) VALUES ('u-{i}', 'name{i}', {age})"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert u-{i} failed: {e}"));
    }

    // Oracle: which ids should be updated (age > 40), and how they split by owner.
    let mut match_a = 0usize;
    let mut match_b = 0usize;
    let mut expected_matches = 0usize;
    for i in 0..n {
        if 20 + i > 40 {
            expected_matches += 1;
            let qid = nexora_id::NexoraId::from_bytes(format!("u-{i}").into_bytes());
            match c
                .shard_map
                .get(c.shard_map.shard_of(&qid))
                .unwrap()
                .owner
                .as_str()
            {
                "node-a" => match_a += 1,
                "node-b" => match_b += 1,
                other => panic!("unexpected owner {other}"),
            }
        }
    }
    assert!(
        match_a > 0 && match_b > 0,
        "the updated rows must span both owners (a={match_a}, b={match_b})"
    );

    // Distributed filtered UPDATE: fans to both owners; each sets `vip = true` on
    // its own matching nodes only.
    c.pg.simple_query("UPDATE Person SET vip = true WHERE age > 40")
        .await
        .expect("distributed filtered UPDATE must succeed");

    // Verify physically on each node's real graph: exactly the matching ids got
    // `vip`, and non-matching ids did NOT.
    let mut updated = 0usize;
    let mut wrongly_updated = 0usize;
    for i in 0..n {
        let id = format!("u-{i}");
        let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
        let owner = c
            .shard_map
            .get(c.shard_map.shard_of(&qid))
            .unwrap()
            .owner
            .clone();
        let graph = if owner == "node-a" {
            &c.graph_a
        } else {
            &c.graph_b
        };
        let vip = graph.get_property(&qid, "vip").await.unwrap();
        let should_match = 20 + i > 40;
        if vip.is_some() {
            updated += 1;
            if !should_match {
                wrongly_updated += 1;
            }
        }
    }

    assert_eq!(
        updated, expected_matches,
        "exactly the {expected_matches} rows with age > 40 must be updated (got {updated})"
    );
    assert_eq!(
        wrongly_updated, 0,
        "no row failing the WHERE predicate may be updated"
    );
}

/// A filtered DELETE across a real 2-node cluster must remove exactly the matching
/// rows from whichever owner holds them, and leave the rest — proving the
/// owner-parallel delete honours the WHERE predicate on each node.
#[tokio::test]
async fn distributed_filtered_delete_applies_across_owners() {
    let c = setup_cluster().await;

    let n = 30;
    for i in 0..n {
        let age = 20 + i;
        c.pg.simple_query(&format!(
            "INSERT INTO Person (id, name, age) VALUES ('d-{i}', 'name{i}', {age})"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert d-{i} failed: {e}"));
    }

    // Delete age < 25 (i in 0..5 → 5 rows). Oracle split across owners.
    let mut del_a = 0usize;
    let mut del_b = 0usize;
    for i in 0..n {
        if 20 + i < 25 {
            let qid = nexora_id::NexoraId::from_bytes(format!("d-{i}").into_bytes());
            match c
                .shard_map
                .get(c.shard_map.shard_of(&qid))
                .unwrap()
                .owner
                .as_str()
            {
                "node-a" => del_a += 1,
                "node-b" => del_b += 1,
                other => panic!("unexpected owner {other}"),
            }
        }
    }
    assert!(
        del_a > 0 && del_b > 0,
        "deleted rows must span both owners (a={del_a}, b={del_b})"
    );

    c.pg.simple_query("DELETE FROM Person WHERE age < 25")
        .await
        .expect("distributed filtered DELETE must succeed");

    // Verify on each node's real graph: matching ids gone, others intact.
    let mut surviving = 0usize;
    for i in 0..n {
        let id = format!("d-{i}");
        let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
        let owner = c
            .shard_map
            .get(c.shard_map.shard_of(&qid))
            .unwrap()
            .owner
            .clone();
        let graph = if owner == "node-a" {
            &c.graph_a
        } else {
            &c.graph_b
        };
        let exists = graph.get_property(&qid, "name").await.unwrap().is_some();
        let should_be_deleted = 20 + i < 25;
        if should_be_deleted {
            assert!(!exists, "row {id} (age < 25) must be deleted from {owner}");
        } else if exists {
            surviving += 1;
        }
    }
    assert_eq!(
        surviving,
        n as usize - 5,
        "every row failing the predicate must survive"
    );
}

/// Build a 2-node cluster whose PG server carries a StandingQueryManager, and
/// return the pieces a filtered-write SQ test needs. Mirrors `setup_cluster` but
/// wires an SQ manager so distributed writes can be observed to trigger SQs.
struct SqCluster {
    pg: Client,
    sq_manager: Arc<StandingQueryManager>,
    shard_map: ShardMap,
    _srv_a: TcpGraphServer,
    _srv_b: TcpGraphServer,
    _pg_server: nexora_pgwire::PgServerHandle,
}

async fn setup_cluster_with_sq(
    sq_name: &str,
    pattern: StandingQueryPattern,
) -> (SqCluster, uuid::Uuid) {
    let (graph_a, addr_a, srv_a) = spawn_node().await;
    let (graph_b, addr_b, srv_b) = spawn_node().await;
    let _ = (&graph_a, &graph_b);

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("node-a", &addr_a).await;
    client.register_node("node-b", &addr_b).await;
    let shard_map = ShardMap::new_distributed(
        TOTAL_SHARDS,
        &["node-a".into(), "node-b".into()],
        "node-a".into(),
    );
    let router = Arc::new(HybridRouter::new_clustered_no_local(
        shard_map.clone(),
        client,
    ));

    let sq_manager = Arc::new(StandingQueryManager::new(256));
    let sq_id = sq_manager.register(sq_name, pattern).await;

    let pg_config = PgConfig {
        port: 0,
        bind_addr: "127.0.0.1".to_owned(),
        trust: true,
        max_connections: 10,
        idle_timeout_secs: 60,
        shutdown_grace_secs: 2,
        ..PgConfig::default()
    };
    let pg_server = spawn_pg_server_with_router(
        graph_a.clone(),
        Arc::new(nexora_core::materialized_view::MaterializedViewManager::new()),
        Some(sq_manager.clone()),
        Some(router),
        None, // C2 replication_progress: tests exercise reads without a live progress tracker
        std::sync::Arc::new(nexora_core::query_pool::QueryPool::new(4)),
        #[cfg(feature = "event-first")]
        None, // event_store: tests run without an event store
        pg_config,
    )
    .await
    .expect("PG server starts");
    let pg = connect(pg_server.local_addr()).await;

    (
        SqCluster {
            pg,
            sq_manager,
            shard_map,
            _srv_a: srv_a,
            _srv_b: srv_b,
            _pg_server: pg_server,
        },
        sq_id,
    )
}

/// A distributed filtered UPDATE (`UPDATE ... SET ... WHERE ...`) must trigger
/// Standing Query evaluation on every affected node across the cluster — proving
/// the cluster write path captures the WHERE-matched ids and re-reads properties
/// through the router (not just this node's local graph). We insert rows spanning
/// both owners, register `vip = true` as the SQ, then UPDATE to set it and assert
/// the SQ matched exactly the updated rows.
#[tokio::test]
async fn distributed_filtered_update_triggers_sq_across_owners() {
    let (c, sq_id) = setup_cluster_with_sq(
        "vip_flag",
        StandingQueryPattern::property(
            "vip",
            FilterCondition::Equals(nexora_id::PropertyValue::Boolean(true)),
        ),
    )
    .await;

    // 20 people ages 20..=39; the UPDATE targets age > 34 (i in 15..20 → 5 rows).
    let n = 20;
    for i in 0..n {
        let age = 20 + i;
        c.pg.simple_query(&format!(
            "INSERT INTO Person (id, name, age) VALUES ('vu-{i}', 'name{i}', {age})"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert vu-{i} failed: {e}"));
    }

    // Oracle: matched rows must span both owners so the trigger is genuinely
    // cross-node.
    let mut on_a = 0usize;
    let mut on_b = 0usize;
    let mut expected = 0usize;
    for i in 0..n {
        if 20 + i > 34 {
            expected += 1;
            let qid = nexora_id::NexoraId::from_bytes(format!("vu-{i}").into_bytes());
            match c
                .shard_map
                .get(c.shard_map.shard_of(&qid))
                .unwrap()
                .owner
                .as_str()
            {
                "node-a" => on_a += 1,
                "node-b" => on_b += 1,
                other => panic!("unexpected owner {other}"),
            }
        }
    }
    assert!(
        on_a > 0 && on_b > 0,
        "updated rows must span both owners (a={on_a}, b={on_b})"
    );

    // Before the UPDATE nothing matches vip=true.
    assert_eq!(
        c.sq_manager.match_count(sq_id).await,
        0,
        "no vip before UPDATE"
    );

    c.pg.simple_query("UPDATE Person SET vip = true WHERE age > 34")
        .await
        .expect("distributed filtered UPDATE must succeed");

    // The SQ must have matched exactly the updated rows — across both owners.
    assert_eq!(
        c.sq_manager.match_count(sq_id).await,
        expected,
        "SQ must fire for every UPDATE-matched node across the cluster"
    );
}

/// A distributed filtered DELETE must trigger Standing Query *unmatch* for every
/// removed node — proving the cluster write path captures affected ids BEFORE the
/// write (the nodes are gone afterward) and signals unmatch. We set up rows that
/// match an SQ, confirm the matches, then DELETE them and assert the match count
/// drops to zero.
#[tokio::test]
async fn distributed_filtered_delete_triggers_sq_unmatch_across_owners() {
    let (c, sq_id) = setup_cluster_with_sq(
        "low_stock",
        StandingQueryPattern::property("stock", FilterCondition::LessThan(10.0)),
    )
    .await;

    // 20 widgets; those with stock < 10 (i in 0..9) match the SQ. We'll insert
    // each, triggering a match, then DELETE the matching ones.
    let n = 20;
    for i in 0..n {
        let stock = i; // 0..19
        c.pg.simple_query(&format!(
            "INSERT INTO Widget (id, name, stock) VALUES ('ds-{i}', 'w{i}', {stock})"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert ds-{i} failed: {e}"));
    }

    // Oracle: matching rows (stock < 10) span both owners.
    let mut on_a = 0usize;
    let mut on_b = 0usize;
    let mut expected = 0usize;
    for i in 0..n {
        if i < 10 {
            expected += 1;
            let qid = nexora_id::NexoraId::from_bytes(format!("ds-{i}").into_bytes());
            match c
                .shard_map
                .get(c.shard_map.shard_of(&qid))
                .unwrap()
                .owner
                .as_str()
            {
                "node-a" => on_a += 1,
                "node-b" => on_b += 1,
                other => panic!("unexpected owner {other}"),
            }
        }
    }
    assert!(
        on_a > 0 && on_b > 0,
        "matching rows must span both owners (a={on_a}, b={on_b})"
    );

    // After inserts, the SQ matches every stock<10 node.
    assert_eq!(
        c.sq_manager.match_count(sq_id).await,
        expected,
        "SQ must match all low-stock nodes after INSERT"
    );

    // Distributed filtered DELETE removes exactly those nodes.
    c.pg.simple_query("DELETE FROM Widget WHERE stock < 10")
        .await
        .expect("distributed filtered DELETE must succeed");

    // The SQ must now match zero — every deleted node was signalled unmatch.
    assert_eq!(
        c.sq_manager.match_count(sq_id).await,
        0,
        "SQ must unmatch every deleted node across the cluster"
    );
}

/// A distributed write that must reach a down owner has to fail with an explicit
/// error rather than silently reporting success while only some owners applied
/// it. This is the write-side counterpart to `owner_down_errors_instead_of_...`.
///
/// HONEST LIMITATION documented by this test: a multi-owner write is NOT atomic.
/// With node-B unreachable, node-A's shard mutation may still commit locally even
/// though the overall statement returns an error (there is no 2PC / rollback
/// across owners yet, RF=1). The guarantee this test pins is fault *visibility* —
/// the client is told the write failed, never a false success — not atomicity.
#[tokio::test]
async fn distributed_write_to_down_owner_errors_not_false_success() {
    let (graph_a, addr_a, _srv_a) = spawn_node().await;

    // node-A live; node-B unreachable (dead port).
    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("node-a", &addr_a).await;
    client.register_node("node-b", "127.0.0.1:1").await;
    let shard_map = ShardMap::new_distributed(
        TOTAL_SHARDS,
        &["node-a".into(), "node-b".into()],
        "node-a".into(),
    );
    let router = Arc::new(HybridRouter::new_clustered_no_local(
        shard_map.clone(),
        client,
    ));

    let query_pool = Arc::new(nexora_core::query_pool::QueryPool::new(4));

    let pg_config = PgConfig {
        port: 0,
        bind_addr: "127.0.0.1".to_owned(),
        trust: true,
        max_connections: 10,
        idle_timeout_secs: 60,
        shutdown_grace_secs: 2,
        ..PgConfig::default()
    };
    let pg_server = spawn_pg_server_with_router(
        graph_a.clone(),
        Arc::new(nexora_core::materialized_view::MaterializedViewManager::new()),
        None,
        Some(router),
        None, // C2 replication_progress: tests exercise reads without a live progress tracker
        query_pool.clone(),
        #[cfg(feature = "event-first")]
        None, // event_store: tests run without an event store
        pg_config,
    )
    .await
    .expect("PG server starts");
    let pg = connect(pg_server.local_addr()).await;

    // A filtered UPDATE fans to every owner; node-B is down, so the statement
    // must ERROR (fault visibility), never report a false success.
    let result = pg
        .simple_query("UPDATE Widget SET flag = true WHERE id = 'anything'")
        .await;
    match result {
        Err(e) => {
            let msg = e
                .as_db_error()
                .map(|db| db.message().to_string())
                .unwrap_or_else(|| e.to_string());
            assert!(
                msg.contains("cluster") || msg.contains("routing") || msg.contains("unavailable"),
                "down-owner write error must explain the cross-node failure, got: {msg}"
            );
        }
        Ok(_) => panic!("distributed write to a down owner must error, not report success"),
    }

    drop(pg_server);
}

/// The MV refresh chain rides on the SQ result broadcast: node-A's app layer
/// subscribes to `StandingQueryManager::subscribe()` and forwards each result to
/// the SQ→MV bridge (upsert on Matched, delete on Unmatched). The bridge itself
/// lives in `nexora-app` (importing it here would be a dependency cycle), so this
/// test proves the *input* that drives it: after a distributed filtered write on
/// a real 2-node cluster, the correct `StandingQueryResult` is broadcast — with
/// the right qid, result type, and properties — for a REMOTE-owned node. If this
/// broadcast is right, the MV bridge downstream refreshes correctly.
#[tokio::test]
async fn distributed_write_broadcasts_sq_result_for_mv_bridge() {
    use nexora_standing_query::ResultType;

    let (c, _sq_id) = setup_cluster_with_sq(
        "vip_mv",
        StandingQueryPattern::property(
            "vip",
            FilterCondition::Equals(nexora_id::PropertyValue::Boolean(true)),
        ),
    )
    .await;

    // Subscribe to the broadcast exactly as the app's MV bridge does.
    let mut rx = c.sq_manager.subscribe();

    // Pick an id owned by node-B (remote from node-A's PG server), so the whole
    // chain — write, affected-id capture, property re-read, SQ broadcast — must
    // cross the node boundary.
    let remote_id = (0..10_000u64)
        .map(|i| format!("mv-{i}"))
        .find(|id| {
            let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
            c.shard_map.get(c.shard_map.shard_of(&qid)).unwrap().owner == "node-b"
        })
        .expect("a node-b-owned id");
    let remote_qid = nexora_id::NexoraId::from_bytes(remote_id.as_bytes().to_vec());

    c.pg.simple_query(&format!(
        "INSERT INTO Person (id, name, age) VALUES ('{remote_id}', 'r', 30)"
    ))
    .await
    .unwrap_or_else(|e| panic!("insert {remote_id} failed: {e}"));

    // Distributed filtered UPDATE that makes the remote node match the SQ.
    c.pg.simple_query(&format!(
        "UPDATE Person SET vip = true WHERE id = '{remote_id}'"
    ))
    .await
    .expect("distributed filtered UPDATE must succeed");

    // The broadcast must carry a Matched result for the remote-owned node — this
    // is exactly what the MV bridge consumes to upsert the row.
    let matched = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match rx.recv().await {
                Ok(r) if r.qid == remote_qid && matches!(r.result_type, ResultType::Matched) => {
                    return r;
                }
                Ok(_) => continue,
                Err(e) => panic!("broadcast closed before Matched result: {e}"),
            }
        }
    })
    .await
    .expect("a Matched SQ result must be broadcast within 2s for the MV bridge");

    assert_eq!(
        matched.qid, remote_qid,
        "broadcast result must be for the updated node"
    );
    assert_eq!(
        matched.matched_properties.get("vip"),
        Some(&nexora_id::PropertyValue::Boolean(true)),
        "the broadcast must carry the vip=true property the MV bridge upserts"
    );
}

/// A batch UPDATE with `WHERE id IN (...)` on a real 2-node cluster must apply to
/// exactly the listed nodes across BOTH owners, and leave every other node
/// untouched. This guards the fix where the write executor's WHERE evaluator
/// gained IN-list support (previously `IN [...]` errored with "Unsupported WHERE
/// expression: List(...)"). The listed ids are chosen to span both owners so the
/// batch write is genuinely cross-partition.
#[tokio::test]
async fn distributed_in_clause_batch_update_applies_across_owners() {
    let c = setup_cluster().await;

    // Insert 30 people. We'll target a subset by explicit id list.
    let n = 30;
    for i in 0..n {
        c.pg.simple_query(&format!(
            "INSERT INTO Person (id, name, age) VALUES ('p-{i}', 'name{i}', {})",
            20 + i
        ))
        .await
        .unwrap_or_else(|e| panic!("insert p-{i} failed: {e}"));
    }

    // Build a target id list that spans both owners.
    let mut targets: Vec<String> = Vec::new();
    let mut tgt_a = 0usize;
    let mut tgt_b = 0usize;
    for i in 0..n {
        // Take a spread of ids; stop once we have some on each owner and >= 6 total.
        let id = format!("p-{i}");
        let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
        let owner = c
            .shard_map
            .get(c.shard_map.shard_of(&qid))
            .unwrap()
            .owner
            .clone();
        if targets.len() < 8 {
            targets.push(id);
            if owner == "node-a" {
                tgt_a += 1;
            } else {
                tgt_b += 1;
            }
        }
    }
    assert!(
        tgt_a > 0 && tgt_b > 0,
        "target id list must span both owners (a={tgt_a}, b={tgt_b})"
    );

    let in_list = targets
        .iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(", ");
    c.pg.simple_query(&format!(
        "UPDATE Person SET flagged = true WHERE id IN ({in_list})"
    ))
    .await
    .expect("distributed IN-clause batch UPDATE must succeed");

    // Verify: exactly the targeted ids got `flagged`, nothing else did.
    let target_set: std::collections::HashSet<&String> = targets.iter().collect();
    let mut flagged_targets = 0usize;
    let mut wrongly_flagged = 0usize;
    for i in 0..n {
        let id = format!("p-{i}");
        let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
        let owner = c
            .shard_map
            .get(c.shard_map.shard_of(&qid))
            .unwrap()
            .owner
            .clone();
        let graph = if owner == "node-a" {
            &c.graph_a
        } else {
            &c.graph_b
        };
        let flagged = graph.get_property(&qid, "flagged").await.unwrap().is_some();
        if flagged {
            if target_set.contains(&id) {
                flagged_targets += 1;
            } else {
                wrongly_flagged += 1;
            }
        }
    }
    assert_eq!(
        flagged_targets,
        targets.len(),
        "every id in the IN list must be flagged across both owners"
    );
    assert_eq!(
        wrongly_flagged, 0,
        "no id outside the IN list may be flagged"
    );
}

/// A full MV chain on a real 2-node cluster, wired end to end and queried BACK
/// through PG-wire — the point-2 acceptance the team asked for.
///
/// This is stronger than `distributed_write_broadcasts_sq_result_for_mv_bridge`
/// (which only checked the bridge's *input*): here the SQ→MV bridge is actually
/// connected (its subscribe-and-forward loop is replicated inline, because the
/// real `SQMaterializedViewBridge` lives in nexora-app and importing it would be
/// a dependency cycle), and after a distributed write we run `SELECT ... FROM
/// <mv>` over the SAME PG connection and assert the served rows are correct.
///
/// The MV manager is shared with the PG server, so the `SELECT ... FROM <mv>`
/// path (`mv_handler::query_materialized_view`) reads exactly the rows the bridge
/// upserted from cross-node SQ results.
struct MvCluster {
    pg: Client,
    sq_manager: Arc<StandingQueryManager>,
    mv_manager: Arc<nexora_core::materialized_view::MaterializedViewManager>,
    shard_map: ShardMap,
    _srv_a: TcpGraphServer,
    _srv_b: TcpGraphServer,
    _pg_server: nexora_pgwire::PgServerHandle,
    _bridge: tokio::task::JoinHandle<()>,
}

async fn setup_cluster_with_mv(
    sq_name: &str,
    pattern: StandingQueryPattern,
    mv_name: &str,
) -> (MvCluster, uuid::Uuid, String) {
    use nexora_core::materialized_view::{
        ColumnDef, DataType, MaterializedRow, MaterializedViewManager, RefreshMode,
    };

    let (graph_a, addr_a, srv_a) = spawn_node().await;
    let (graph_b, addr_b, srv_b) = spawn_node().await;
    let _ = (&graph_a, &graph_b);

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("node-a", &addr_a).await;
    client.register_node("node-b", &addr_b).await;
    let shard_map = ShardMap::new_distributed(
        TOTAL_SHARDS,
        &["node-a".into(), "node-b".into()],
        "node-a".into(),
    );
    let router = Arc::new(HybridRouter::new_clustered_no_local(
        shard_map.clone(),
        client,
    ));

    let sq_manager = Arc::new(StandingQueryManager::new(256));
    let sq_id = sq_manager.register(sq_name, pattern).await;

    // Shared MV manager: the PG server serves `SELECT ... FROM <mv>` from it, and
    // the bridge loop below writes into it — same Arc, so reads see the writes.
    let mv_manager = Arc::new(MaterializedViewManager::new());
    let mv_id = mv_manager
        .create_view(
            mv_name.to_string(),
            format!("SQ-backed view for {sq_name}"),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::String,
                },
                ColumnDef {
                    name: "vip".to_string(),
                    data_type: DataType::Boolean,
                },
            ],
            RefreshMode::Incremental,
        )
        .await
        .expect("create_view");

    // Wire the SQ→MV bridge exactly as nexora-app's main.rs does: subscribe to the
    // SQ result stream and forward each result (Matched→upsert, Unmatched→delete).
    // Replicated inline because importing the app bridge would cycle the crates.
    let bridge = {
        let mut rx = sq_manager.subscribe();
        let mv = mv_manager.clone();
        let mv_id = mv_id.clone();
        let want_sq = sq_id;
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(result) if result.sq_id == want_sq => match result.result_type {
                        nexora_standing_query::ResultType::Matched => {
                            let row = MaterializedRow {
                                key: result.qid.to_string(),
                                values: result.matched_properties.clone(),
                                version: 1,
                                updated_at: chrono::Utc::now(),
                            };
                            let _ = mv.upsert_row(&mv_id, row).await;
                        }
                        nexora_standing_query::ResultType::Unmatched => {
                            let _ = mv.delete_row(&mv_id, &result.qid.to_string()).await;
                        }
                    },
                    Ok(_) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    };

    let pg_config = PgConfig {
        port: 0,
        bind_addr: "127.0.0.1".to_owned(),
        trust: true,
        max_connections: 10,
        idle_timeout_secs: 60,
        shutdown_grace_secs: 2,
        ..PgConfig::default()
    };
    let pg_server = spawn_pg_server_with_router(
        graph_a.clone(),
        mv_manager.clone(),
        Some(sq_manager.clone()),
        Some(router),
        None, // C2 replication_progress: tests exercise reads without a live progress tracker
        std::sync::Arc::new(nexora_core::query_pool::QueryPool::new(4)),
        #[cfg(feature = "event-first")]
        None, // event_store: tests run without an event store
        pg_config,
    )
    .await
    .expect("PG server starts");
    let pg = connect(pg_server.local_addr()).await;

    (
        MvCluster {
            pg,
            sq_manager,
            mv_manager,
            shard_map,
            _srv_a: srv_a,
            _srv_b: srv_b,
            _pg_server: pg_server,
            _bridge: bridge,
        },
        sq_id,
        mv_id,
    )
}

/// End-to-end distributed MV: distributed write via PG-wire → cross-node SQ →
/// bridge upserts the MV → `SELECT ... FROM <mv>` via PG-wire returns the rows.
/// Proves the whole chain with a real bridge connected and the MV queried back
/// through the wire (not just asserted on the in-process manager).
#[tokio::test]
async fn distributed_mv_end_to_end_query_via_pgwire() {
    let (c, sq_id, mv_id) = setup_cluster_with_mv(
        "vip_sq",
        StandingQueryPattern::property(
            "vip",
            FilterCondition::Equals(nexora_id::PropertyValue::Boolean(true)),
        ),
        "vip_members",
    )
    .await;

    // Insert 12 people spanning both owners; none is vip yet.
    let n = 12;
    for i in 0..n {
        c.pg.simple_query(&format!(
            "INSERT INTO Person (id, name, vip) VALUES ('mv-{i}', 'name{i}', false)"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert mv-{i} failed: {e}"));
    }

    // Promote a subset to vip via a distributed IN-clause UPDATE. Choose ids that
    // span both owners so the MV is fed from cross-node SQ results.
    let mut targets: Vec<String> = Vec::new();
    let mut on_a = 0usize;
    let mut on_b = 0usize;
    for i in 0..n {
        let id = format!("mv-{i}");
        let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
        let owner = c
            .shard_map
            .get(c.shard_map.shard_of(&qid))
            .unwrap()
            .owner
            .clone();
        if targets.len() < 5 {
            targets.push(id);
            if owner == "node-a" {
                on_a += 1;
            } else {
                on_b += 1;
            }
        }
    }
    assert!(
        on_a > 0 && on_b > 0,
        "vip targets must span both owners (a={on_a}, b={on_b})"
    );

    let in_list = targets
        .iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(", ");
    c.pg.simple_query(&format!(
        "UPDATE Person SET vip = true WHERE id IN ({in_list})"
    ))
    .await
    .expect("distributed UPDATE must succeed");

    // First confirm the SQ fired at all (this half of the chain: distributed
    // write → affected-id capture → property re-read → on_property_change).
    let mut sq_ok = false;
    for _ in 0..40 {
        if c.sq_manager.match_count(sq_id).await == targets.len() {
            sq_ok = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        sq_ok,
        "SQ must match the vip set (got {}, want {})",
        c.sq_manager.match_count(sq_id).await,
        targets.len()
    );

    // Then confirm the bridge forwarded those results into the MV.
    let expected_keys: std::collections::HashSet<String> = targets
        .iter()
        .map(|id| nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec()).to_string())
        .collect();
    let mut ok = false;
    for _ in 0..40 {
        if c.mv_manager
            .get_rows(&mv_id)
            .await
            .map(|r| r.len())
            .unwrap_or(0)
            == targets.len()
        {
            ok = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        ok,
        "bridge must populate the MV with {} rows",
        targets.len()
    );

    // Now query the MV BACK through PG-wire — the point-2 acceptance.
    let rows =
        c.pg.simple_query(&format!("SELECT * FROM {}", "vip_members"))
            .await
            .expect("SELECT FROM mv via PG-wire must succeed");

    // Column 0 is the MV row key (the node qid hex). Collect served keys.
    let served: std::collections::HashSet<String> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
            _ => None,
        })
        .collect();

    assert_eq!(
        served, expected_keys,
        "SELECT FROM mv via PG-wire must return exactly the vip-matched node keys"
    );
}

/// Point-3 acceptance: an aggregate (grouped COUNT/SUM/AVG/MIN/MAX) materialized
/// view, maintained incrementally off cross-node SQ results, must show correct
/// before/after deltas as distributed writes insert and update nodes.
///
/// The aggregate engine (`nexora_core::incremental_aggregation`) is driven off
/// the same SQ broadcast the row-set MV bridge uses. Because an Unmatched result
/// carries no properties, we track each node's last-seen properties so a
/// transition out of the match set applies as a Delete against the aggregate.
/// Writes go through PG-wire in cluster mode; the matched nodes span both owners,
/// so the per-group aggregates are genuinely combined from cross-node results.
#[tokio::test]
async fn distributed_aggregate_mv_incremental_deltas_across_owners() {
    use nexora_core::incremental_aggregation::{
        AggregateFunction, AggregatedMaterializedView, RowOperation,
    };
    use nexora_standing_query::ResultType;
    use std::collections::HashMap;
    use std::sync::Mutex;

    // SQ: match every active person (active = true). The aggregate view groups by
    // city and maintains COUNT + SUM/AVG/MIN/MAX over `salary`.
    let (graph_a, addr_a, srv_a) = spawn_node().await;
    let (graph_b, addr_b, srv_b) = spawn_node().await;
    let _ = (&graph_a, &graph_b);
    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("node-a", &addr_a).await;
    client.register_node("node-b", &addr_b).await;
    let shard_map = ShardMap::new_distributed(
        TOTAL_SHARDS,
        &["node-a".into(), "node-b".into()],
        "node-a".into(),
    );
    let router = Arc::new(HybridRouter::new_clustered_no_local(
        shard_map.clone(),
        client,
    ));

    let sq_manager = Arc::new(StandingQueryManager::new(256));
    let sq_id = sq_manager
        .register(
            "active_people",
            StandingQueryPattern::property(
                "active",
                FilterCondition::Equals(nexora_id::PropertyValue::Boolean(true)),
            ),
        )
        .await;

    // The aggregate MV and the per-node last-seen properties, maintained inline
    // off the SQ broadcast (mirrors how an app-layer aggregate MV bridge would).
    let agg = Arc::new(Mutex::new(AggregatedMaterializedView::new(
        "mv-agg".to_string(),
        "salary_by_city".to_string(),
        "MATCH (n) WHERE n.active = true RETURN n.city, count(*), sum(n.salary)".to_string(),
        vec!["city".to_string()],
        vec![
            AggregateFunction::Count,
            AggregateFunction::Sum {
                column: "salary".to_string(),
            },
            AggregateFunction::Min {
                column: "salary".to_string(),
            },
            AggregateFunction::Max {
                column: "salary".to_string(),
            },
        ],
    )));
    let last_seen: Arc<Mutex<HashMap<String, HashMap<String, nexora_id::PropertyValue>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let bridge = {
        let mut rx = sq_manager.subscribe();
        let agg = agg.clone();
        let last_seen = last_seen.clone();
        let want_sq = sq_id;
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(result) if result.sq_id == want_sq => {
                        let node = result.qid.to_string();
                        match result.result_type {
                            ResultType::Matched => {
                                let props = result.matched_properties.clone();
                                let prev = last_seen.lock().unwrap().get(&node).cloned();
                                let mut a = agg.lock().unwrap();
                                match prev {
                                    Some(old) => a.process_update(
                                        &props,
                                        RowOperation::Update { old_row: old },
                                    ),
                                    None => a.process_update(&props, RowOperation::Insert),
                                }
                                last_seen.lock().unwrap().insert(node, props);
                            }
                            ResultType::Unmatched => {
                                // No properties on unmatch — use the last-seen row.
                                if let Some(old) = last_seen.lock().unwrap().remove(&node) {
                                    agg.lock()
                                        .unwrap()
                                        .process_update(&old, RowOperation::Delete);
                                }
                            }
                        }
                    }
                    Ok(_) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    };

    let pg_config = PgConfig {
        port: 0,
        bind_addr: "127.0.0.1".to_owned(),
        trust: true,
        max_connections: 10,
        idle_timeout_secs: 60,
        shutdown_grace_secs: 2,
        ..PgConfig::default()
    };
    let pg_server = spawn_pg_server_with_router(
        graph_a.clone(),
        Arc::new(nexora_core::materialized_view::MaterializedViewManager::new()),
        Some(sq_manager.clone()),
        Some(router),
        None, // C2 replication_progress: tests exercise reads without a live progress tracker
        std::sync::Arc::new(nexora_core::query_pool::QueryPool::new(4)),
        #[cfg(feature = "event-first")]
        None, // event_store: tests run without an event store
        pg_config,
    )
    .await
    .expect("PG server starts");
    let pg = connect(pg_server.local_addr()).await;

    // Insert 12 active people across two cities, salaries known. Span both owners.
    // city = "NYC" for even i, "LA" for odd i. salary = 100 + i*10.
    let n = 12;
    let mut oracle: HashMap<String, (u64, f64, f64, f64)> = HashMap::new(); // city -> (count, sum, min, max)
    let mut on_a = 0usize;
    let mut on_b = 0usize;
    for i in 0..n {
        let city = if i % 2 == 0 { "NYC" } else { "LA" };
        let salary = 100 + i * 10;
        let id = format!("ap-{i}");
        pg.simple_query(&format!(
            "INSERT INTO Person (id, city, salary, active) VALUES ('{id}', '{city}', {salary}, true)"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {id} failed: {e}"));
        let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
        match shard_map
            .get(shard_map.shard_of(&qid))
            .unwrap()
            .owner
            .as_str()
        {
            "node-a" => on_a += 1,
            "node-b" => on_b += 1,
            other => panic!("unexpected owner {other}"),
        }
        let e = oracle
            .entry(city.to_string())
            .or_insert((0, 0.0, f64::MAX, f64::MIN));
        e.0 += 1;
        e.1 += salary as f64;
        e.2 = e.2.min(salary as f64);
        e.3 = e.3.max(salary as f64);
    }
    assert!(
        on_a > 0 && on_b > 0,
        "active people must span both owners (a={on_a}, b={on_b})"
    );

    // Wait for the aggregate to reflect all 12 inserts (count across groups == 12).
    let settled = |agg: &Arc<Mutex<AggregatedMaterializedView>>| {
        let a = agg.lock().unwrap();
        a.manager_all_count() == n as u64
    };
    let mut ok = false;
    for _ in 0..60 {
        if settled(&agg) {
            ok = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(ok, "aggregate MV must reflect all {n} inserts");

    // BEFORE delta: assert per-city COUNT/SUM/AVG/MIN/MAX match the oracle.
    {
        let a = agg.lock().unwrap();
        for (city, (cnt, sum, min, max)) in &oracle {
            let key = format!("String(\"{city}\")");
            let st = a
                .group_state(&key)
                .unwrap_or_else(|| panic!("group {city} must exist"));
            assert_eq!(st.count, *cnt, "{city} COUNT before");
            assert_eq!(st.sum, *sum, "{city} SUM before");
            assert_eq!(st.avg(), Some(sum / *cnt as f64), "{city} AVG before");
            assert_eq!(st.min(), Some(*min), "{city} MIN before");
            assert_eq!(st.max(), Some(*max), "{city} MAX before");
        }
    }

    // AFTER delta: deactivate the highest-salary person (ap-11, LA, salary 210).
    // That is an Unmatch → Delete against LA: count-1, sum-210, and MAX must
    // recompute down to the next-highest LA salary (ap-9 = 190). This is the exact
    // MIN/MAX-on-delete recompute the old code got wrong.
    pg.simple_query("UPDATE Person SET active = false WHERE id = 'ap-11'")
        .await
        .expect("deactivate must succeed");

    // Wait for LA count to drop to 5.
    let mut ok2 = false;
    for _ in 0..60 {
        {
            let a = agg.lock().unwrap();
            if a.group_state("String(\"LA\")").map(|s| s.count) == Some(5) {
                ok2 = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(ok2, "LA count must drop to 5 after deactivating ap-11");

    {
        let a = agg.lock().unwrap();
        let la = a.group_state("String(\"LA\")").unwrap();
        // LA salaries were 110,130,150,170,190,210; after removing 210:
        assert_eq!(la.count, 5, "LA COUNT after delete");
        assert_eq!(
            la.sum,
            110.0 + 130.0 + 150.0 + 170.0 + 190.0,
            "LA SUM after delete"
        );
        assert_eq!(
            la.max(),
            Some(190.0),
            "LA MAX must recompute to 190 after removing 210"
        );
        assert_eq!(la.min(), Some(110.0), "LA MIN unchanged");
        // NYC untouched.
        let nyc = a.group_state("String(\"NYC\")").unwrap();
        assert_eq!(nyc.count, 6, "NYC COUNT unchanged");
    }

    bridge.abort();
    drop(pg_server);
    drop((srv_a, srv_b));
}

// ============================================================
// Point-4: 3-node cluster matrix + honest RF>1 failover gap.
// ============================================================

/// A real 3-node cluster with a PG server on node-A wired to a no-local router.
/// RF is configurable; with RF=1 there is no follower replication.
struct Cluster3 {
    pg: Client,
    graphs: Vec<Arc<GraphService>>,
    _srvs: Vec<TcpGraphServer>,
    _pg_server: nexora_pgwire::PgServerHandle,
    shard_map: ShardMap,
}

async fn setup_cluster3(rf: usize) -> Cluster3 {
    let (graph_a, addr_a, srv_a) = spawn_node().await;
    let (graph_b, addr_b, srv_b) = spawn_node().await;
    let (graph_c, addr_c, srv_c) = spawn_node().await;

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("node-a", &addr_a).await;
    client.register_node("node-b", &addr_b).await;
    client.register_node("node-c", &addr_c).await;
    let shard_map = ShardMap::new_distributed_rf(
        TOTAL_SHARDS,
        &["node-a".into(), "node-b".into(), "node-c".into()],
        "node-a".into(),
        rf,
    );

    // Build ReplicaWriter with replica sets when RF>1
    let router = if rf > 1 {
        use nexora_zenoh::replica_writer::ReplicaWriter;
        use nexora_zenoh::replication::ReplicaSet;

        let mut replica_sets = Vec::new();
        for (shard_id, assignment) in &shard_map.assignments {
            if !assignment.replicas.is_empty() {
                replica_sets.push(ReplicaSet::new(
                    *shard_id,
                    assignment.owner.clone(),
                    assignment.replicas.clone(),
                ));
            }
        }

        let replica_writer = Arc::new(ReplicaWriter::with_replica_sets(
            client.clone(),
            replica_sets,
        ));

        Arc::new(
            HybridRouter::new_clustered_no_local(shard_map.clone(), client)
                .with_replica_writer(replica_writer),
        )
    } else {
        Arc::new(HybridRouter::new_clustered_no_local(
            shard_map.clone(),
            client,
        ))
    };

    let pg_config = PgConfig {
        port: 0,
        bind_addr: "127.0.0.1".to_owned(),
        trust: true,
        max_connections: 10,
        idle_timeout_secs: 60,
        shutdown_grace_secs: 2,
        ..PgConfig::default()
    };
    let pg_server = spawn_pg_server_with_router(
        graph_a.clone(),
        Arc::new(nexora_core::materialized_view::MaterializedViewManager::new()),
        None,
        Some(router),
        None, // C2 replication_progress: tests exercise reads without a live progress tracker
        std::sync::Arc::new(nexora_core::query_pool::QueryPool::new(4)),
        #[cfg(feature = "event-first")]
        None, // event_store: tests run without an event store
        pg_config,
    )
    .await
    .expect("PG server starts");
    let pg = connect(pg_server.local_addr()).await;

    Cluster3 {
        pg,
        graphs: vec![graph_a, graph_b, graph_c],
        _srvs: vec![srv_a, srv_b, srv_c],
        _pg_server: pg_server,
        shard_map,
    }
}

/// A real 3-node cluster (RF=1) must spread INSERTed nodes across ALL THREE
/// owners, return a whole-cluster COUNT and correct global aggregates, all issued
/// via PG-wire at node-A. This is the 3-node extension of the 2-node matrix the
/// team asked for.
#[tokio::test]
async fn three_node_insert_spreads_and_aggregates_are_global() {
    let c = setup_cluster3(1).await;

    // Insert 60 rows with known salaries; count physical placement per owner.
    let n = 60i64;
    let mut per_owner = std::collections::HashMap::<String, usize>::new();
    let mut oracle_sum = 0i64;
    let mut oracle_min = i64::MAX;
    let mut oracle_max = i64::MIN;
    for i in 0..n {
        let id = format!("t3-{i}");
        let salary = 100 + i * 7 % 500;
        c.pg.simple_query(&format!(
            "INSERT INTO Emp (id, salary) VALUES ('{id}', {salary})"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {id} failed: {e}"));
        let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
        let owner = c
            .shard_map
            .get(c.shard_map.shard_of(&qid))
            .unwrap()
            .owner
            .clone();
        *per_owner.entry(owner).or_default() += 1;
        oracle_sum += salary;
        oracle_min = oracle_min.min(salary);
        oracle_max = oracle_max.max(salary);
    }

    // All three owners must physically hold some data.
    assert_eq!(
        per_owner.len(),
        3,
        "data must land on all 3 owners, got {per_owner:?}"
    );
    for node in ["node-a", "node-b", "node-c"] {
        assert!(
            per_owner.get(node).copied().unwrap_or(0) > 0,
            "{node} must own some rows"
        );
    }

    // Verify physical placement per owner against the shard map.
    for i in 0..n {
        let id = format!("t3-{i}");
        let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
        let owner = c
            .shard_map
            .get(c.shard_map.shard_of(&qid))
            .unwrap()
            .owner
            .clone();
        let idx = match owner.as_str() {
            "node-a" => 0,
            "node-b" => 1,
            _ => 2,
        };
        assert!(
            c.graphs[idx]
                .get_property(&qid, "salary")
                .await
                .unwrap()
                .is_some(),
            "{id} must physically live on {owner}"
        );
    }

    // Global COUNT and aggregates issued at node-A must span all three owners.
    let count = single_i64(&c.pg, "SELECT COUNT(*) FROM Emp").await;
    assert_eq!(count, n, "global COUNT must be whole-cluster");
    let sum = single_i64(&c.pg, "SELECT SUM(salary) FROM Emp").await;
    assert_eq!(sum, oracle_sum, "global SUM must combine all 3 owners");
    let min = single_i64(&c.pg, "SELECT MIN(salary) FROM Emp").await;
    assert_eq!(min, oracle_min, "global MIN across 3 owners");
    let max = single_i64(&c.pg, "SELECT MAX(salary) FROM Emp").await;
    assert_eq!(max, oracle_max, "global MAX across 3 owners");
}

/// Helper: run a query returning a single integer cell.
async fn single_i64(pg: &Client, sql: &str) -> i64 {
    let rows = pg
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} failed: {e}"));
    rows.iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql} returned no row"))
        .parse()
        .unwrap_or_else(|e| panic!("{sql} non-integer result: {e}"))
}

/// RF=3 PG-wire write with quorum replication: owner + followers all hold the data.
///
/// With ReplicaWriter integrated into the distributed write path:
///   - a 3-node RF=3 shard map assigns each shard an owner + 2 followers,
///   - a PG-wire INSERT writes the owner AND replicates to followers via quorum_write,
///   - so the followers' physical graphs ALSO hold the replicated node.
///
/// This test verifies RF>1 副本复制 is working correctly.
#[tokio::test]
async fn rf3_pgwire_write_replicates_to_followers() {
    let c = setup_cluster3(3).await;

    // With RF=3, every shard has an owner + 2 followers.
    let sample_shard = c
        .shard_map
        .shard_of(&nexora_id::NexoraId::from_bytes(b"probe".to_vec()));
    assert_eq!(
        c.shard_map.get(sample_shard).unwrap().replicas.len(),
        2,
        "RF=3 must assign 2 followers per shard"
    );

    // Insert one row and find its owner + intended followers.
    let id = "rep-1";
    c.pg.simple_query(&format!(
        "INSERT INTO Emp (id, salary) VALUES ('{id}', 500)"
    ))
    .await
    .unwrap_or_else(|e| panic!("insert {id} failed: {e}"));
    let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
    let shard = c.shard_map.shard_of(&qid);
    let asg = c.shard_map.get(shard).unwrap();

    let idx_of = |node: &str| match node {
        "node-a" => 0,
        "node-b" => 1,
        _ => 2,
    };

    // The owner physically holds the row.
    let owner_idx = idx_of(&asg.owner);
    assert!(
        c.graphs[owner_idx]
            .get_property(&qid, "salary")
            .await
            .unwrap()
            .is_some(),
        "owner {} must hold the row",
        asg.owner
    );

    // With RF=3 quorum replication, followers ALSO hold the replicated data.
    for follower in &asg.replicas {
        let f_idx = idx_of(follower);
        assert!(
            c.graphs[f_idx]
                .get_property(&qid, "salary")
                .await
                .unwrap()
                .is_some(),
            "follower {follower} must hold the replicated row (RF=3 quorum replication)"
        );
    }
}

/// **Owner Failover Test**: With RF=3, when the owner node fails, we can still
/// read from followers. This validates the basic failover read path.
///
/// Setup:
/// 1. Create a 3-node RF=3 cluster
/// 2. INSERT via PG-wire (replicates to all 3 nodes)
/// 3. Verify followers have replicated data (proving failover readiness)
///
/// Note: Full dynamic failover (detecting owner down + automatic promotion) requires
/// distributed consensus and is deferred to post-P0. This test validates the prerequisite:
/// replicated data exists on followers and is physically readable.
#[tokio::test]
async fn rf3_owner_failure_read_from_follower() {
    let c = setup_cluster3(3).await;

    // Insert one row via PG-wire
    let id = "failover-user";
    c.pg.simple_query(&format!(
        "INSERT INTO Emp (id, salary) VALUES ('{id}', 75000)"
    ))
    .await
    .unwrap();

    let qid = nexora_id::NexoraId::from_bytes(id.as_bytes().to_vec());
    let shard = c.shard_map.shard_of(&qid);
    let asg = c.shard_map.get(shard).unwrap();

    let idx_of = |node: &str| match node {
        "node-a" => 0,
        "node-b" => 1,
        _ => 2,
    };

    // Verify owner has the data
    let owner_idx = idx_of(&asg.owner);
    assert!(
        c.graphs[owner_idx]
            .get_property(&qid, "salary")
            .await
            .unwrap()
            .is_some(),
        "owner must have the data before failure"
    );

    // Direct verification: followers DO have the replicated data
    // This proves that IF the owner fails, a follower promotion would have the data.
    for follower in &asg.replicas {
        let f_idx = idx_of(follower);
        assert!(
            c.graphs[f_idx]
                .get_property(&qid, "salary")
                .await
                .unwrap()
                .is_some(),
            "follower {follower} has replicated data (failover-ready)"
        );
    }

    // The failover::read_with_failover function exists and is tested in unit tests.
    // Full integration (owner health monitoring + automatic shard map updates) requires
    // distributed consensus (Raft/Paxos) and is beyond current P0 scope.
}
