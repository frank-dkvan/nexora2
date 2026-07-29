//! 航空货运站数据测试演示
//!
//! 模拟真实的航空货运场景，包括：
//! - 货物到达事件
//! - 仓库存储事件
//! - 航班装载事件
//! - 实时统计和监控

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

    println!("\n╔═══════════════════════════════════════════════════════════════╗");
    println!("║                                                               ║");
    println!("║          航空货运站实时数据处理系统测试                        ║");
    println!("║          Air Cargo Terminal Real-time System                  ║");
    println!("║                                                               ║");
    println!("╚═══════════════════════════════════════════════════════════════╝\n");

    // 1. 配置集群
    let config = DistributedConfig {
        binary_path: None,
        data_dir: "/tmp/nexora-cargo-demo".into(),
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

    println!("📦 系统配置:");
    println!("  - Meta 节点: 3 个 HA 集群");
    println!("  - Frontend: PostgreSQL 协议");
    println!("  - Compute: {} 核并行处理\n", config.compute_nodes[0].parallelism);

    // 2. 启动集群
    println!("🚀 启动分布式 RisingWave 集群...");
    let cluster = DistributedEmbeddedRisingWave::start(config.clone()).await?;
    println!("✓ 集群启动成功\n");

    tokio::time::sleep(Duration::from_secs(3)).await;

    // 3. 连接 Frontend
    println!("🔌 连接到数据库...");
    let (client, connection) = tokio_postgres::connect(
        "host=127.0.0.1 port=4566 user=root dbname=dev",
        NoTls,
    ).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("连接错误: {}", e);
        }
    });
    println!("✓ 数据库连接成功\n");

    // ========================================
    // 场景 1: 货物到达事件流
    // ========================================
    println!("═══════════════════════════════════════════════");
    println!("  场景 1: 货物到达实时流");
    println!("═══════════════════════════════════════════════\n");

    let cargo_arrival_ddl = r#"
        CREATE SOURCE cargo_arrivals (
            cargo_id VARCHAR,
            flight_no VARCHAR,
            origin VARCHAR,
            destination VARCHAR,
            weight_kg DECIMAL,
            cargo_type VARCHAR,
            priority VARCHAR,
            arrival_time BIGINT
        ) WITH (
            connector = 'datagen',
            fields.cargo_id.kind = 'sequence',
            fields.cargo_id.start = '1',
            fields.cargo_id.end = '10000',
            fields.flight_no.kind = 'random',
            fields.flight_no.seed = '1',
            fields.flight_no.length = '6',
            fields.origin.kind = 'random',
            fields.origin.seed = '2',
            fields.origin.length = '3',
            fields.destination.kind = 'random',
            fields.destination.seed = '3',
            fields.destination.length = '3',
            fields.weight_kg.kind = 'random',
            fields.weight_kg.min = '10',
            fields.weight_kg.max = '5000',
            fields.cargo_type.kind = 'random',
            fields.cargo_type.seed = '4',
            fields.cargo_type.length = '8',
            fields.priority.kind = 'random',
            fields.priority.seed = '5',
            fields.priority.length = '6',
            datagen.rows.per.second = '50'
        ) FORMAT PLAIN ENCODE JSON;
    "#;

    client.execute(cargo_arrival_ddl, &[]).await?;
    println!("✓ 货物到达事件流已创建 (50条/秒)\n");

    // ========================================
    // 场景 2: 仓库存储事件流
    // ========================================
    println!("═══════════════════════════════════════════════");
    println!("  场景 2: 仓库存储实时流");
    println!("═══════════════════════════════════════════════\n");

    let warehouse_ddl = r#"
        CREATE SOURCE warehouse_operations (
            operation_id VARCHAR,
            cargo_id VARCHAR,
            warehouse_zone VARCHAR,
            operation_type VARCHAR,
            operator_id VARCHAR,
            timestamp BIGINT
        ) WITH (
            connector = 'datagen',
            fields.operation_id.kind = 'sequence',
            fields.operation_id.start = '1',
            fields.operation_id.end = '20000',
            fields.cargo_id.kind = 'sequence',
            fields.cargo_id.start = '1',
            fields.cargo_id.end = '10000',
            fields.warehouse_zone.kind = 'random',
            fields.warehouse_zone.seed = '6',
            fields.warehouse_zone.length = '2',
            fields.operation_type.kind = 'random',
            fields.operation_type.seed = '7',
            fields.operation_type.length = '8',
            fields.operator_id.kind = 'random',
            fields.operator_id.seed = '8',
            fields.operator_id.length = '4',
            datagen.rows.per.second = '30'
        ) FORMAT PLAIN ENCODE JSON;
    "#;

    client.execute(warehouse_ddl, &[]).await?;
    println!("✓ 仓库操作事件流已创建 (30条/秒)\n");

    // ========================================
    // 场景 3: 航班装载事件流
    // ========================================
    println!("═══════════════════════════════════════════════");
    println!("  场景 3: 航班装载实时流");
    println!("═══════════════════════════════════════════════\n");

    let flight_loading_ddl = r#"
        CREATE SOURCE flight_loading (
            loading_id VARCHAR,
            flight_no VARCHAR,
            cargo_id VARCHAR,
            loading_time BIGINT,
            departure_time BIGINT,
            gate_no VARCHAR
        ) WITH (
            connector = 'datagen',
            fields.loading_id.kind = 'sequence',
            fields.loading_id.start = '1',
            fields.loading_id.end = '15000',
            fields.flight_no.kind = 'random',
            fields.flight_no.seed = '9',
            fields.flight_no.length = '6',
            fields.cargo_id.kind = 'sequence',
            fields.cargo_id.start = '1',
            fields.cargo_id.end = '10000',
            fields.gate_no.kind = 'random',
            fields.gate_no.seed = '10',
            fields.gate_no.length = '2',
            datagen.rows.per.second = '20'
        ) FORMAT PLAIN ENCODE JSON;
    "#;

    client.execute(flight_loading_ddl, &[]).await?;
    println!("✓ 航班装载事件流已创建 (20条/秒)\n");

    // ========================================
    // 创建实时统计 Materialized Views
    // ========================================
    println!("═══════════════════════════════════════════════");
    println!("  创建实时统计视图");
    println!("═══════════════════════════════════════════════\n");

    // MV1: 按航线统计货物量
    println!("📊 创建视图 1: 航线货物统计...");
    let mv1 = r#"
        CREATE MATERIALIZED VIEW route_cargo_stats AS
        SELECT
            origin,
            destination,
            COUNT(*) AS cargo_count,
            SUM(weight_kg) AS total_weight_kg,
            AVG(weight_kg) AS avg_weight_kg,
            MAX(arrival_time) AS latest_arrival
        FROM cargo_arrivals
        GROUP BY origin, destination;
    "#;
    client.execute(mv1, &[]).await?;
    println!("✓ 航线货物统计视图已创建\n");

    // MV2: 按货物类型统计
    println!("📊 创建视图 2: 货物类型统计...");
    let mv2 = r#"
        CREATE MATERIALIZED VIEW cargo_type_stats AS
        SELECT
            cargo_type,
            priority,
            COUNT(*) AS count,
            SUM(weight_kg) AS total_weight,
            COUNT(DISTINCT flight_no) AS flight_count
        FROM cargo_arrivals
        GROUP BY cargo_type, priority;
    "#;
    client.execute(mv2, &[]).await?;
    println!("✓ 货物类型统计视图已创建\n");

    // MV3: 仓库区域利用率
    println!("📊 创建视图 3: 仓库区域利用率...");
    let mv3 = r#"
        CREATE MATERIALIZED VIEW warehouse_utilization AS
        SELECT
            warehouse_zone,
            operation_type,
            COUNT(*) AS operation_count,
            COUNT(DISTINCT operator_id) AS active_operators,
            COUNT(DISTINCT cargo_id) AS unique_cargos
        FROM warehouse_operations
        GROUP BY warehouse_zone, operation_type;
    "#;
    client.execute(mv3, &[]).await?;
    println!("✓ 仓库利用率视图已创建\n");

    // MV4: 航班装载效率
    println!("📊 创建视图 4: 航班装载效率...");
    let mv4 = r#"
        CREATE MATERIALIZED VIEW flight_loading_efficiency AS
        SELECT
            flight_no,
            gate_no,
            COUNT(*) AS cargo_loaded,
            COUNT(DISTINCT cargo_id) AS unique_cargos,
            MAX(loading_time) AS last_loading_time
        FROM flight_loading
        GROUP BY flight_no, gate_no;
    "#;
    client.execute(mv4, &[]).await?;
    println!("✓ 航班装载效率视图已创建\n");

    // ========================================
    // 等待数据生成
    // ========================================
    println!("═══════════════════════════════════════════════");
    println!("  数据生成中...");
    println!("═══════════════════════════════════════════════\n");
    println!("⏳ 等待 10 秒钟生成测试数据...");
    println!("   - 货物到达: ~500 条");
    println!("   - 仓库操作: ~300 条");
    println!("   - 航班装载: ~200 条\n");

    tokio::time::sleep(Duration::from_secs(10)).await;

    // ========================================
    // 查询 1: 航线货物统计
    // ========================================
    println!("═══════════════════════════════════════════════");
    println!("  查询 1: Top 10 繁忙航线");
    println!("═══════════════════════════════════════════════\n");

    let query1 = r#"
        SELECT
            origin || ' → ' || destination AS route,
            cargo_count,
            ROUND(total_weight_kg::numeric, 2) AS total_weight_kg,
            ROUND(avg_weight_kg::numeric, 2) AS avg_weight_kg
        FROM route_cargo_stats
        ORDER BY cargo_count DESC
        LIMIT 10;
    "#;

    let rows = client.query(query1, &[]).await?;
    println!("{:<20} {:<15} {:<20} {:<15}", "航线", "货物数量", "总重量(kg)", "平均重量(kg)");
    println!("{}", "─".repeat(75));

    for row in rows {
        let route: String = row.get(0);
        let count: i64 = row.get(1);
        let total: f64 = row.get::<_, f64>(2);
        let avg: f64 = row.get::<_, f64>(3);
        println!("{:<20} {:<15} {:<20.2} {:<15.2}", route, count, total, avg);
    }
    println!();

    // ========================================
    // 查询 2: 货物类型分布
    // ========================================
    println!("═══════════════════════════════════════════════");
    println!("  查询 2: 货物类型和优先级分布");
    println!("═══════════════════════════════════════════════\n");

    let query2 = r#"
        SELECT
            cargo_type,
            priority,
            count,
            ROUND(total_weight::numeric, 2) AS total_weight_kg,
            flight_count
        FROM cargo_type_stats
        ORDER BY count DESC
        LIMIT 10;
    "#;

    let rows2 = client.query(query2, &[]).await?;
    println!("{:<12} {:<12} {:<10} {:<18} {:<12}", "货物类型", "优先级", "数量", "总重量(kg)", "航班数");
    println!("{}", "─".repeat(70));

    for row in rows2 {
        let cargo_type: String = row.get(0);
        let priority: String = row.get(1);
        let count: i64 = row.get(2);
        let weight: f64 = row.get::<_, f64>(3);
        let flights: i64 = row.get(4);
        println!("{:<12} {:<12} {:<10} {:<18.2} {:<12}",
            cargo_type, priority, count, weight, flights);
    }
    println!();

    // ========================================
    // 查询 3: 仓库利用率
    // ========================================
    println!("═══════════════════════════════════════════════");
    println!("  查询 3: 仓库区域实时利用率");
    println!("═══════════════════════════════════════════════\n");

    let query3 = r#"
        SELECT
            warehouse_zone,
            operation_type,
            operation_count,
            active_operators,
            unique_cargos
        FROM warehouse_utilization
        ORDER BY operation_count DESC
        LIMIT 10;
    "#;

    let rows3 = client.query(query3, &[]).await?;
    println!("{:<12} {:<15} {:<12} {:<12} {:<12}", "仓库区域", "操作类型", "操作次数", "操作员数", "货物数");
    println!("{}", "─".repeat(65));

    for row in rows3 {
        let zone: String = row.get(0);
        let op_type: String = row.get(1);
        let op_count: i64 = row.get(2);
        let operators: i64 = row.get(3);
        let cargos: i64 = row.get(4);
        println!("{:<12} {:<15} {:<12} {:<12} {:<12}",
            zone, op_type, op_count, operators, cargos);
    }
    println!();

    // ========================================
    // 查询 4: 航班装载效率
    // ========================================
    println!("═══════════════════════════════════════════════");
    println!("  查询 4: 航班装载效率 Top 10");
    println!("═══════════════════════════════════════════════\n");

    let query4 = r#"
        SELECT
            flight_no,
            gate_no,
            cargo_loaded,
            unique_cargos
        FROM flight_loading_efficiency
        ORDER BY cargo_loaded DESC
        LIMIT 10;
    "#;

    let rows4 = client.query(query4, &[]).await?;
    println!("{:<12} {:<10} {:<15} {:<15}", "航班号", "登机口", "已装载数量", "唯一货物数");
    println!("{}", "─".repeat(55));

    for row in rows4 {
        let flight: String = row.get(0);
        let gate: String = row.get(1);
        let loaded: i64 = row.get(2);
        let unique: i64 = row.get(3);
        println!("{:<12} {:<10} {:<15} {:<15}", flight, gate, loaded, unique);
    }
    println!();

    // ========================================
    // 复杂查询: 综合运营报表
    // ========================================
    println!("═══════════════════════════════════════════════");
    println!("  查询 5: 综合运营实时报表");
    println!("═══════════════════════════════════════════════\n");

    let query5 = r#"
        SELECT
            COUNT(*) AS total_cargos,
            COUNT(DISTINCT flight_no) AS total_flights,
            COUNT(DISTINCT origin) AS origin_airports,
            COUNT(DISTINCT destination) AS dest_airports,
            SUM(weight_kg) AS total_weight,
            AVG(weight_kg) AS avg_weight
        FROM cargo_arrivals;
    "#;

    let row = client.query_one(query5, &[]).await?;
    let total_cargos: i64 = row.get(0);
    let total_flights: i64 = row.get(1);
    let origins: i64 = row.get(2);
    let dests: i64 = row.get(3);
    let total_weight: Option<f64> = row.try_get::<_, f64>(4).ok();
    let avg_weight: Option<f64> = row.try_get::<_, f64>(5).ok();

    println!("📈 实时运营统计:");
    println!("   • 总货物数量: {} 件", total_cargos);
    println!("   • 涉及航班: {} 个", total_flights);
    println!("   • 始发机场: {} 个", origins);
    println!("   • 目的机场: {} 个", dests);
    println!("   • 总重量: {:.2} kg", total_weight.unwrap_or(0.0));
    println!("   • 平均重量: {:.2} kg", avg_weight.unwrap_or(0.0));
    println!();

    // ========================================
    // 集群健康状态
    // ========================================
    println!("═══════════════════════════════════════════════");
    println!("  系统健康状态");
    println!("═══════════════════════════════════════════════\n");

    let health = cluster.monitor_health().await?;

    println!("🖥️  Meta 节点:");
    for node in &health.meta_nodes {
        let status = if node.is_running { "✓ 运行中" } else { "✗ 已停止" };
        let role = if node.is_leader { " [Leader]" } else { "" };
        println!("   • Node {} ({}): {}{}", node.node_id, node.address, status, role);
    }

    println!("\n🌐 Frontend:");
    let fe_status = if health.frontend.is_running { "✓ 运行中" } else { "✗ 已停止" };
    println!("   • {}: {}", health.frontend.address, fe_status);

    println!("\n⚙️  Compute 节点:");
    for node in &health.compute_nodes {
        let status = if node.is_running { "✓ 运行中" } else { "✗ 已停止" };
        println!("   • Node {} ({}): {}", node.node_id, node.address, status);
    }

    if let Some(leader_id) = health.leader_node_id {
        println!("\n👑 当前 Meta Leader: Node {}", leader_id);
    }

    // ========================================
    // 清理
    // ========================================
    println!("\n═══════════════════════════════════════════════");
    println!("  清理测试数据");
    println!("═══════════════════════════════════════════════\n");

    println!("🧹 删除测试对象...");
    client.execute("DROP MATERIALIZED VIEW IF EXISTS flight_loading_efficiency", &[]).await?;
    client.execute("DROP MATERIALIZED VIEW IF EXISTS warehouse_utilization", &[]).await?;
    client.execute("DROP MATERIALIZED VIEW IF EXISTS cargo_type_stats", &[]).await?;
    client.execute("DROP MATERIALIZED VIEW IF EXISTS route_cargo_stats", &[]).await?;
    client.execute("DROP SOURCE IF EXISTS flight_loading", &[]).await?;
    client.execute("DROP SOURCE IF EXISTS warehouse_operations", &[]).await?;
    client.execute("DROP SOURCE IF EXISTS cargo_arrivals", &[]).await?;
    println!("✓ 清理完成\n");

    // ========================================
    // 关闭集群
    // ========================================
    println!("═══════════════════════════════════════════════");
    println!("  关闭系统");
    println!("═══════════════════════════════════════════════\n");

    println!("⏸️  关闭集群...");
    drop(client);
    cluster.shutdown().await?;
    println!("✓ 集群已安全关闭\n");

    // ========================================
    // 总结
    // ========================================
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║                                                               ║");
    println!("║                   测试完成！                                  ║");
    println!("║                                                               ║");
    println!("╚═══════════════════════════════════════════════════════════════╝\n");

    println!("✅ 测试总结:");
    println!("   • 3 个数据流 (货物到达、仓库操作、航班装载)");
    println!("   • 4 个实时统计视图");
    println!("   • 5 个复杂查询验证");
    println!("   • 集群健康监控");
    println!("   • 所有功能正常运行\n");

    println!("📚 更多信息:");
    println!("   • 测试脚本: ./scripts/test-distributed-risingwave.sh");
    println!("   • 详细文档: docs/DISTRIBUTED_RISINGWAVE_TESTING_GUIDE.md");
    println!("   • 手动连接: psql -h 127.0.0.1 -p 4566 -U root -d dev\n");

    Ok(())
}
