//! Real PostgreSQL wire-protocol integration tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_pgwire::{spawn_pg_server, PgConfig, PgServerHandle};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

fn create_test_graph() -> Arc<GraphService> {
    Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 100,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ))
}

fn create_test_mv_manager() -> Arc<nexora_core::materialized_view::MaterializedViewManager> {
    Arc::new(nexora_core::materialized_view::MaterializedViewManager::new())
}

fn trust_config() -> PgConfig {
    PgConfig {
        port: 0,
        bind_addr: "127.0.0.1".to_owned(),
        trust: true,
        max_connections: 10,
        idle_timeout_secs: 30,
        shutdown_grace_secs: 2,
        ..PgConfig::default()
    }
}

async fn start_trust_server() -> PgServerHandle {
    let query_pool = std::sync::Arc::new(nexora_core::query_pool::QueryPool::new(4));

    spawn_pg_server(
        create_test_graph(),
        create_test_mv_manager(),
        None,
        query_pool.clone(),
        trust_config(),
    )
    .await
    .expect("server starts")
}

async fn connect(
    address: SocketAddr,
    user: &str,
    password: Option<&str>,
) -> Result<Client, tokio_postgres::Error> {
    connect_to_database(address, user, password, "nexora").await
}

async fn connect_to_database(
    address: SocketAddr,
    user: &str,
    password: Option<&str>,
    database: &str,
) -> Result<Client, tokio_postgres::Error> {
    let mut config = tokio_postgres::Config::new();
    config
        .host(address.ip().to_string())
        .port(address.port())
        .user(user)
        .dbname(database);
    if let Some(password) = password {
        config.password(password);
    }
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

#[tokio::test]
async fn simple_query_returns_multiple_typed_rows() {
    let graph = create_test_graph();
    let query_pool = std::sync::Arc::new(nexora_core::query_pool::QueryPool::new(4));

    let server = spawn_pg_server(
        graph.clone(),
        create_test_mv_manager(),
        None,
        query_pool.clone(),
        trust_config(),
    )
    .await
    .unwrap();
    let client = connect(server.local_addr(), "admin", None).await.unwrap();

    let first_insert = client
        .simple_query("INSERT INTO Person (name, age) VALUES ('Alice', 30)")
        .await
        .unwrap();
    let second_insert = client
        .simple_query("INSERT INTO Person (name, age) VALUES ('Bob', 25)")
        .await
        .unwrap();
    assert!(
        first_insert
            .iter()
            .any(|message| matches!(message, SimpleQueryMessage::CommandComplete(1))),
        "first insert response: {first_insert:?}"
    );
    assert!(
        second_insert
            .iter()
            .any(|message| matches!(message, SimpleQueryMessage::CommandComplete(1))),
        "second insert response: {second_insert:?}"
    );
    let direct = nexora_sql::execute_sql(&graph, "SELECT name, age FROM Person")
        .await
        .unwrap();
    assert_eq!(direct.rows.len(), 2, "direct SQL result: {direct:?}");
    let messages = client
        .simple_query("SELECT name, age FROM Person")
        .await
        .unwrap();
    let rows = messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(rows.len(), 2, "wire responses: {messages:?}");
    let mut values = rows
        .iter()
        .map(|row| (row.get("name").unwrap(), row.get("age").unwrap()))
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, vec![("Alice", "30"), ("Bob", "25")]);
    drop(client);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn session_queries_and_version_are_supported() {
    let server = start_trust_server().await;
    let client = connect(server.local_addr(), "admin", None).await.unwrap();

    client
        .simple_query("SET application_name = 'integration-test'")
        .await
        .unwrap();
    let show = client.simple_query("SHOW application_name").await.unwrap();
    let value = show.iter().find_map(|message| match message {
        SimpleQueryMessage::Row(row) => row.get(0),
        _ => None,
    });
    assert_eq!(value, Some("integration-test"));

    let version = client.simple_query("SELECT version()").await.unwrap();
    let value = version.iter().find_map(|message| match message {
        SimpleQueryMessage::Row(row) => row.get(0),
        _ => None,
    });
    assert!(value.unwrap().contains("Nexora"));

    let error = client.simple_query("BEGIN").await.unwrap_err();
    assert_eq!(
        error.as_db_error().unwrap().code(),
        &tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED
    );
    let unknown_database = connect_to_database(server.local_addr(), "admin", None, "missing").await;
    assert_eq!(
        unknown_database.unwrap_err().as_db_error().unwrap().code(),
        &tokio_postgres::error::SqlState::INVALID_CATALOG_NAME
    );
    // pg_catalog introspection is now served by the GUI-client shim
    // (crates/nexora-pgwire/src/pg_catalog.rs), so pg_class returns a
    // RowDescription instead of the old FEATURE_NOT_SUPPORTED error.
    let catalog = client
        .simple_query("SELECT * FROM pg_catalog.pg_class")
        .await
        .expect("pg_catalog.pg_class is served by the introspection shim");
    assert!(
        catalog
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::CommandComplete(_))),
        "pg_class query completes without error"
    );

    drop(client);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn error_stops_remaining_statements() {
    let server = start_trust_server().await;
    let client = connect(server.local_addr(), "admin", None).await.unwrap();

    client
        .simple_query(
            "DELETE FROM Person;\
             INSERT INTO Person (name) VALUES ('must-not-exist')",
        )
        .await
        .expect_err("DELETE without WHERE is rejected");

    let messages = client
        .simple_query("SELECT * FROM Person WHERE name = 'must-not-exist'")
        .await
        .unwrap();
    assert!(!messages
        .iter()
        .any(|message| matches!(message, SimpleQueryMessage::Row(_))));

    drop(client);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn scram_auth_accepts_only_valid_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let users_path = directory.path().join("pg-users.json");
    std::fs::write(
        &users_path,
        r#"{
            "alice":{"password":"correct horse","role":"operator"},
            "reader":{"password":"read only","role":"readonly"}
        }"#,
    )
    .unwrap();
    let config = PgConfig {
        port: 0,
        bind_addr: "127.0.0.1".to_owned(),
        users_file: Some(users_path.display().to_string()),
        max_connections: 10,
        idle_timeout_secs: 30,
        shutdown_grace_secs: 2,
        ..PgConfig::default()
    };

    let server = spawn_pg_server(
        create_test_graph(),
        create_test_mv_manager(),
        None,
        std::sync::Arc::new(nexora_core::query_pool::QueryPool::new(4)),
        config,
    )
    .await
    .unwrap();

    let client = connect(server.local_addr(), "alice", Some("correct horse"))
        .await
        .expect("valid SCRAM credential");
    client.simple_query("SELECT version()").await.unwrap();

    let wrong = connect(server.local_addr(), "alice", Some("wrong")).await;
    assert!(wrong.is_err());
    let unknown = connect(server.local_addr(), "unknown", Some("correct horse")).await;
    assert!(unknown.is_err());

    let reader = connect(server.local_addr(), "reader", Some("read only"))
        .await
        .expect("readonly user authenticates");
    reader.simple_query("SELECT version()").await.unwrap();
    let denied = reader
        .simple_query("INSERT INTO Person (name) VALUES ('denied')")
        .await
        .expect_err("readonly user cannot write");
    assert_eq!(
        denied.as_db_error().unwrap().code(),
        &tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE
    );

    drop(client);
    drop(reader);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn connection_limit_returns_postgres_error() {
    let config = PgConfig {
        max_connections: 1,
        ..trust_config()
    };

    let server = spawn_pg_server(
        create_test_graph(),
        create_test_mv_manager(),
        None,
        std::sync::Arc::new(nexora_core::query_pool::QueryPool::new(4)),
        config,
    )
    .await
    .unwrap();
    let first = connect(server.local_addr(), "admin", None).await.unwrap();

    let second = connect(server.local_addr(), "admin", None)
        .await
        .expect_err("second connection must be rejected");
    assert_eq!(
        second.as_db_error().unwrap().code(),
        &tokio_postgres::error::SqlState::TOO_MANY_CONNECTIONS
    );

    drop(first);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn idle_connections_are_closed() {
    let config = PgConfig {
        idle_timeout_secs: 1,
        ..trust_config()
    };

    let server = spawn_pg_server(
        create_test_graph(),
        create_test_mv_manager(),
        None,
        std::sync::Arc::new(nexora_core::query_pool::QueryPool::new(4)),
        config,
    )
    .await
    .unwrap();
    let client = connect(server.local_addr(), "admin", None).await.unwrap();

    tokio::time::sleep(Duration::from_millis(1_300)).await;
    assert!(client.simple_query("SELECT version()").await.is_err());

    drop(client);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_closes_listener_and_connections() {
    let server = start_trust_server().await;
    let address = server.local_addr();
    let client = connect(address, "admin", None).await.unwrap();

    server.shutdown().await.unwrap();
    assert!(client.simple_query("SELECT version()").await.is_err());
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
}

#[tokio::test]
async fn tls_listener_accepts_postgres_ssl_request() {
    let directory = tempfile::tempdir().unwrap();
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert_path = directory.path().join("server.crt");
    let key_path = directory.path().join("server.key");
    std::fs::write(&cert_path, certified.cert.pem()).unwrap();
    std::fs::write(&key_path, certified.key_pair.serialize_pem()).unwrap();
    let config = PgConfig {
        tls_cert: Some(cert_path.display().to_string()),
        tls_key: Some(key_path.display().to_string()),
        ..trust_config()
    };

    let server = spawn_pg_server(
        create_test_graph(),
        create_test_mv_manager(),
        None,
        std::sync::Arc::new(nexora_core::query_pool::QueryPool::new(4)),
        config,
    )
    .await
    .unwrap();

    let mut stream = tokio::net::TcpStream::connect(server.local_addr())
        .await
        .unwrap();
    stream.write_all(&8_u32.to_be_bytes()).await.unwrap();
    stream
        .write_all(&80_877_103_u32.to_be_bytes())
        .await
        .unwrap();
    let response = stream.read_u8().await.unwrap();
    assert_eq!(response, b'S');

    drop(stream);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn extended_query_is_rejected_without_breaking_connection() {
    let server = start_trust_server().await;
    let client = connect(server.local_addr(), "admin", None).await.unwrap();

    let error = client
        .query("SELECT * FROM Person", &[])
        .await
        .expect_err("extended protocol is intentionally unsupported");
    assert_eq!(
        error.as_db_error().unwrap().code(),
        &tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED
    );
    client.simple_query("SELECT version()").await.unwrap();

    drop(client);
    server.shutdown().await.unwrap();
}
