//! 分布式 RisingWave 集群演示
//!
//! 演示如何启动和使用 3 节点 HA RisingWave 集群
//!
//! 运行方式：
//! ```bash
//! cargo run --example distributed_risingwave_demo --features embedded
//! ```

use nexora_risingwave::distributed::{
    DistributedEmbeddedRisingWave, DistributedConfig,
    MetaNodeConfig, FrontendNodeConfig, ComputeNodeConfig,
};
use tokio_postgres::NoTls;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    println!("========================================");
    println!("  分布式 RisingWave 集群演示");
    println!("========================================\n");

    // 1. 配置集群
    let config = DistributedConfig {
        binary_path: None, // 自动发现
        data_dir: "/tmp/nexora-risingwave-demo".into(),
        meta_nodes: vec![
            MetaNodeConfig {
                node_id: 1,
                listen_addr: "127.0.0.1:5690".to_string(),
                advertise_addr: "127.0.0.1:5690".to_string(),
                dashboard_addr: "127.0.0.1:5691".to_string(),
            },
            MetaNodeConfig {
                node_id: 2,
                listen_addr: "127.0.0.1:5692".to_string(),
                advertise_addr: "127.0.0.1:5692".to_string(),
                dashboard_addr: "127.0.0.1:5693".to_string(),
            },
            MetaNodeConfig {
                node_id: 3,
                listen_addr: "127.0.0.1:5694".to_string(),
                advertise_addr: "127.0.0.1:5694".to_string(),
                dashboard_addr: "127.0.0.1:5695".to_string(),
            },
        ],
        frontend: FrontendNodeConfig {
            listen_addr: "127.0.0.1:4566".to_string(),
        },
        compute_nodes: vec![
            ComputeNodeConfig {
                listen_addr: "127.0.0.1:5688".to_string(),
                parallelism: num_cpus::get(),
            },
        ],
        startup_timeout_secs: 60,
        shutdown_timeout_secs: 30,
    };

    println!("配置:");
    println!("  - Meta 节点: 3 个 (端口 5690, 5692, 5694)");
    println!("  - Frontend: 1 个 (端口 4566)");
    println!("  - Compute: 1 个 (端口 5688, {} 并发)", config.compute_nodes[0].parallelism);
    println!();

    // 2. 启动集群
    println!("启动集群...");
    let cluster = DistributedEmbeddedRisingWave::start(config.clone()).await?;
    println!("✓ 集群启动成功\n");

    // 3. 等待集群稳定
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 4. 连接 Frontend
    println!("连接 Frontend...");
    let (client, connection) = tokio_postgres::connect(
        "host=127.0.0.1 port=4566 user=root dbname=dev",
        NoTls,
    ).await?;

    // 连接处理器（后台任务）
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    println!("✓ 已连接到 Frontend\n");

    // 5. 执行测试查询
    println!("========================================");
    println!("  测试 1: 创建 Source");
    println!("========================================");

    let ddl = r#"
        CREATE SOURCE IF NOT EXISTS user_events (
            user_id VARCHAR,
            event_type VARCHAR,
            timestamp BIGINT,
            properties JSONB
        ) WITH (
            connector = 'datagen',
            fields.user_id.kind = 'sequence',
            fields.user_id.start = '1',
            fields.user_id.end = '1000',
            fields.event_type.kind = 'random',
            fields.event_type.seed = '1',
            fields.event_type.length = '10',
            datagen.rows.per.second = '10'
        ) FORMAT PLAIN ENCODE JSON;
    "#;

    client.execute(ddl, &[]).await?;
    println!("✓ Source 创建成功\n");

    // 6. 创建 Materialized View
    println!("========================================");
    println!("  测试 2: 创建 Materialized View");
    println!("========================================");

    let mv_ddl = r#"
        CREATE MATERIALIZED VIEW IF NOT EXISTS user_event_counts AS
        SELECT
            user_id,
            event_type,
            COUNT(*) AS event_count,
            MAX(timestamp) AS latest_timestamp
        FROM user_events
        GROUP BY user_id, event_type;
    "#;

    client.execute(mv_ddl, &[]).await?;
    println!("✓ Materialized View 创建成功\n");

    // 7. 等待数据生成
    println!("等待数据生成 (5 秒)...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 8. 查询 Materialized View
    println!("\n========================================");
    println!("  测试 3: 查询 Materialized View");
    println!("========================================");

    let rows = client.query(
        "SELECT user_id, event_type, event_count FROM user_event_counts LIMIT 10",
        &[],
    ).await?;

    println!("\nTop 10 用户事件统计:");
    println!("{:<10} {:<15} {:<12}", "User ID", "Event Type", "Count");
    println!("{}", "-".repeat(40));

    for row in rows {
        let user_id: String = row.get(0);
        let event_type: String = row.get(1);
        let count: i64 = row.get(2);
        println!("{:<10} {:<15} {:<12}", user_id, event_type, count);
    }

    // 9. 测试复杂查询
    println!("\n========================================");
    println!("  测试 4: 复杂聚合查询");
    println!("========================================");

    let agg_query = r#"
        SELECT
            event_type,
            COUNT(DISTINCT user_id) AS unique_users,
            COUNT(*) AS total_events,
            AVG(event_count) AS avg_events_per_user
        FROM user_event_counts
        GROUP BY event_type
        ORDER BY total_events DESC;
    "#;

    let agg_rows = client.query(agg_query, &[]).await?;

    println!("\n按事件类型统计:");
    println!("{:<15} {:<15} {:<15} {:<20}", "Event Type", "Unique Users", "Total Events", "Avg/User");
    println!("{}", "-".repeat(70));

    for row in agg_rows {
        let event_type: String = row.get(0);
        let unique_users: i64 = row.get(1);
        let total_events: i64 = row.get(2);
        let avg_per_user: Option<f64> = row.try_get(3).ok();
        println!(
            "{:<15} {:<15} {:<15} {:<20.2}",
            event_type,
            unique_users,
            total_events,
            avg_per_user.unwrap_or(0.0)
        );
    }

    // 10. 监控集群健康状态
    println!("\n========================================");
    println!("  测试 5: 集群健康状态");
    println!("========================================");

    let health = cluster.monitor_health().await?;
    println!("\nMeta 节点:");
    for node in &health.meta_nodes {
        let status = if node.is_running { "运行中" } else { "已停止" };
        let role = if node.is_leader { " [Leader]" } else { "" };
        println!("  - Node {} ({}): {}{}", node.node_id, node.address, status, role);
    }

    println!("\nFrontend:");
    let fe_status = if health.frontend.is_running { "运行中" } else { "已停止" };
    println!("  - {}: {}", health.frontend.address, fe_status);

    println!("\nCompute 节点:");
    for node in &health.compute_nodes {
        let status = if node.is_running { "运行中" } else { "已停止" };
        println!("  - Node {} ({}): {}", node.node_id, node.address, status);
    }

    if let Some(leader_id) = health.leader_node_id {
        println!("\n当前 Meta Leader: Node {}", leader_id);
    }

    // 11. 清理
    println!("\n========================================");
    println!("  清理");
    println!("========================================");

    println!("删除测试对象...");
    client.execute("DROP MATERIALIZED VIEW IF EXISTS user_event_counts", &[]).await?;
    client.execute("DROP SOURCE IF EXISTS user_events", &[]).await?;
    println!("✓ 清理完成\n");

    // 12. 关闭集群
    println!("关闭集群...");
    drop(client); // 先关闭客户端连接
    cluster.shutdown().await?;
    println!("✓ 集群已停止\n");

    println!("========================================");
    println!("  演示完成！");
    println!("========================================");
    println!("\n提示:");
    println!("  - 使用 scripts/test-distributed-risingwave.sh 进行更多测试");
    println!("  - 查看日志: RUST_LOG=debug cargo run --example distributed_risingwave_demo --features embedded");

    Ok(())
}
