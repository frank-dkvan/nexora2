//! 应用场景集成测试 — 工厂叉车监控系统
//!
//! 模拟设计文档中描述的核心场景：
//! 1. 创建叉车节点和操作员节点
//! 2. 设置属性（速度、位置、状态）
//! 3. 建立关系（叉车-操作员、叉车-区域）
//! 4. 持久化 + 崩溃恢复
//! 5. 多节点并发操作
//! 6. 内存压力下的 LRU 淘汰

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use std::sync::Arc;

// ============================================================
// 场景 1：叉车车队基础 CRUD
// ============================================================

#[tokio::test]
async fn scenario_forklift_fleet_basic_operations() {
    let svc = make_service(4, 100);

    // 创建 3 台叉车
    let fl001 = NexoraId::from_bytes(b"forklift-001".to_vec());
    let fl002 = NexoraId::from_bytes(b"forklift-002".to_vec());
    let fl003 = NexoraId::from_bytes(b"forklift-003".to_vec());

    // 写入叉车属性
    for (qid, name, speed, zone) in [
        (&fl001, "FL-001", 12.5f64, "Zone-A"),
        (&fl002, "FL-002", 8.3, "Zone-B"),
        (&fl003, "FL-003", 15.0, "Zone-A"),
    ] {
        svc.set_property(qid, "name", PropertyValue::String(name.into()))
            .await
            .unwrap();
        svc.set_property(qid, "speed", PropertyValue::Float(speed))
            .await
            .unwrap();
        svc.set_property(qid, "zone", PropertyValue::String(zone.into()))
            .await
            .unwrap();
        svc.set_property(qid, "status", PropertyValue::String("active".into()))
            .await
            .unwrap();
    }

    // 验证属性
    assert_eq!(
        svc.get_property(&fl001, "speed").await.unwrap(),
        Some(PropertyValue::Float(12.5))
    );
    assert_eq!(
        svc.get_property(&fl002, "zone").await.unwrap(),
        Some(PropertyValue::String("Zone-B".into()))
    );

    // 创建操作员节点
    let op42 = NexoraId::from_bytes(b"operator-042".to_vec());
    svc.set_property(&op42, "name", PropertyValue::String("张三".into()))
        .await
        .unwrap();
    svc.set_property(&op42, "badge_level", PropertyValue::Integer(3))
        .await
        .unwrap();

    // 建立关系：操作员 → 叉车
    svc.add_edge(&op42, HalfEdge::out(Symbol::new("OPERATES"), fl001.clone()))
        .await
        .unwrap();

    // 建立关系：叉车 → 区域
    let zone_a = NexoraId::from_bytes(b"zone-a".to_vec());
    svc.add_edge(
        &fl001,
        HalfEdge::out(Symbol::new("IN_ZONE"), zone_a.clone()),
    )
    .await
    .unwrap();

    // 验证边
    let op_edges = svc.get_edges(&op42).await.unwrap();
    assert_eq!(op_edges.len(), 1);
    assert_eq!(op_edges[0].edge_type.as_str(), "OPERATES");
    assert_eq!(op_edges[0].other, fl001);

    let fl_edges = svc.get_edges(&fl001).await.unwrap();
    assert_eq!(fl_edges.len(), 1);
    assert_eq!(fl_edges[0].edge_type.as_str(), "IN_ZONE");

    // 验证总节点数（zone_a 从未被直接访问，不会自动创建）
    // 5 个 NexoraId 被创建：3 叉车 + 1 操作员
    // zone_a 只作为边的目标引用，不会触发 wake_node
    assert_eq!(svc.active_node_count().await, 4);

    println!("✅ 场景 1 通过：叉车车队基础 CRUD");
}

// ============================================================
// 场景 2：事件溯源 + 崩溃恢复
// ============================================================

#[tokio::test]
async fn scenario_crash_recovery_preserves_state() {
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 100,
        node_channel_size: 16,
    };

    let fl = NexoraId::from_bytes(b"forklift-crash-test".to_vec());

    // ====== Phase 1: 写入数据，模拟正常运行 ======
    {
        let svc = GraphService::new(config.clone(), persistor.clone());

        // 模拟传感器数据流
        let readings = vec![
            ("speed", 10.0f64),
            ("speed", 12.5),
            ("speed", 8.0),
            ("battery", 85.0),
            ("battery", 83.0),
        ];

        for (key, val) in &readings {
            svc.set_property(&fl, key, PropertyValue::Float(*val))
                .await
                .unwrap();
        }

        // 添加关系
        let zone = NexoraId::from_bytes(b"zone-b".to_vec());
        svc.add_edge(&fl, HalfEdge::out(Symbol::new("IN_ZONE"), zone))
            .await
            .unwrap();

        // 休眠节点（触发快照 + journal 持久化）
        svc.sleep_node(&fl).await.unwrap();
        assert_eq!(svc.active_node_count().await, 0);
    }
    // Service dropped — 模拟进程崩溃（但 persistor 存活，模拟磁盘数据）

    // ====== Phase 2: 恢复，验证数据完整性 ======
    {
        let svc = GraphService::new(config, persistor.clone());

        // 访问节点 — 自动从持久化层唤醒
        // 应该看到最后一次写入的值
        let speed = svc.get_property(&fl, "speed").await.unwrap();
        assert_eq!(
            speed,
            Some(PropertyValue::Float(8.0)),
            "应恢复最后一次写入的速度"
        );

        let battery = svc.get_property(&fl, "battery").await.unwrap();
        assert_eq!(
            battery,
            Some(PropertyValue::Float(83.0)),
            "应恢复最后一次写入的电量"
        );

        let zone_prop = svc.get_property(&fl, "zone").await.unwrap();
        // zone 是通过边建立的，不是属性，所以应该是 None
        assert_eq!(zone_prop, None);

        // 边应该被恢复
        let edges = svc.get_edges(&fl).await.unwrap();
        assert_eq!(edges.len(), 1, "应恢复边");
        assert_eq!(edges[0].edge_type.as_str(), "IN_ZONE");
    }

    println!("✅ 场景 2 通过：事件溯源 + 崩溃恢复");
}

// ============================================================
// 场景 3：内存压力下的 LRU 淘汰
// ============================================================

#[tokio::test]
async fn scenario_memory_pressure_lru_eviction() {
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = GraphServiceConfig {
        num_shards: 1,          // 1 个 shard，强制所有节点在同一个
        max_nodes_per_shard: 5, // 最多 5 个节点在内存
        node_channel_size: 16,
    };
    let svc = GraphService::new(config, persistor.clone());

    // 创建 10 个节点（超过内存限制）
    let qids: Vec<_> = (0..10)
        .map(|i| NexoraId::from_bytes(format!("node-{i:03}").into_bytes()))
        .collect();

    // 写入所有节点
    for (i, qid) in qids.iter().enumerate() {
        svc.set_property(qid, "index", PropertyValue::Integer(i as i64))
            .await
            .unwrap();
    }

    // 验证：由于 LRU 淘汰，活跃节点数不应超过限制
    let active = svc.active_node_count().await;
    assert!(
        active <= 5,
        "活跃节点数 {active} 应不超过 max_nodes_per_shard=5"
    );

    // 最近访问的节点应该还在内存中
    let last_val = svc.get_property(&qids[9], "index").await.unwrap();
    assert_eq!(last_val, Some(PropertyValue::Integer(9)));

    // 早期节点被唤醒后仍然可以访问（从持久化恢复）
    let first_val = svc.get_property(&qids[0], "index").await.unwrap();
    assert_eq!(first_val, Some(PropertyValue::Integer(0)));

    println!("✅ 场景 3 通过：LRU 淘汰 + 按需唤醒");
}

// ============================================================
// 场景 4：多节点并发操作（Actor 模型验证）
// ============================================================

#[tokio::test]
async fn scenario_concurrent_node_operations() {
    let svc = Arc::new(make_service(4, 1000));

    // 并发创建 100 个节点
    let mut handles = Vec::new();
    for i in 0..100 {
        let svc = svc.clone();
        let qid = NexoraId::from_bytes(format!("concurrent-{i:04}").into_bytes());
        handles.push(tokio::spawn(async move {
            svc.set_property(
                &qid,
                "created_by",
                PropertyValue::String(format!("task-{i}")),
            )
            .await
            .unwrap();
            svc.set_property(&qid, "index", PropertyValue::Integer(i))
                .await
                .unwrap();
            qid
        }));
    }

    // 等待所有任务完成
    let mut created_qids = Vec::new();
    for handle in handles {
        created_qids.push(handle.await.unwrap());
    }

    // 验证所有节点都正确创建
    for (i, qid) in created_qids.iter().enumerate() {
        let val = svc.get_property(qid, "index").await.unwrap();
        assert_eq!(val, Some(PropertyValue::Integer(i as i64)));
    }

    println!("✅ 场景 4 通过：100 节点并发操作");
}

// ============================================================
// 场景 5：属性覆盖 + 边操作序列
// ============================================================

#[tokio::test]
async fn scenario_property_overwrite_and_edge_lifecycle() {
    let svc = make_service(4, 100);
    let fl = NexoraId::from_bytes(b"forklift-overwrite".to_vec());

    // 模拟传感器数据流：属性反复更新
    for speed in [10.0, 12.5, 8.0, 15.0, 3.0] {
        svc.set_property(&fl, "speed", PropertyValue::Float(speed))
            .await
            .unwrap();
    }
    // 最终值应该是最后一次写入
    assert_eq!(
        svc.get_property(&fl, "speed").await.unwrap(),
        Some(PropertyValue::Float(3.0))
    );

    // 边的添加和查询
    let zone_a = NexoraId::from_bytes(b"zone-a".to_vec());
    let zone_b = NexoraId::from_bytes(b"zone-b".to_vec());

    svc.add_edge(&fl, HalfEdge::out(Symbol::new("IN_ZONE"), zone_a.clone()))
        .await
        .unwrap();
    svc.add_edge(&fl, HalfEdge::out(Symbol::new("IN_ZONE"), zone_b.clone()))
        .await
        .unwrap();

    let edges = svc.get_edges(&fl).await.unwrap();
    assert_eq!(edges.len(), 2, "应有 2 条边");

    println!("✅ 场景 5 通过：属性覆盖 + 多边操作");
}

// ============================================================
// 场景 6：WAL 崩溃恢复（模拟部分写入损坏）
// ============================================================

#[tokio::test]
async fn scenario_wal_corruption_recovery() {
    use nexora_core::event::{NodeChangeEvent, TimedEvent};
    use nexora_core::wal::WriteAheadLog;
    use nexora_id::EventTime;

    let dir = tempfile::tempdir().unwrap();
    let qid = NexoraId::from_bytes(b"wal-test-node".to_vec());

    // Phase 1: 写入几条正常记录
    {
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        for i in 1..=5 {
            wal.append(nexora_core::wal::WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("tick"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros(i as u64 * 1000),
                ),
            })
            .unwrap();
        }
        // WAL 自动 flush on drop
    }

    // Phase 2: 追加损坏数据（模拟 crash 中断写入）
    {
        use std::fs::OpenOptions;
        use std::io::Write;
        let wal_path = dir.path().join("current.wal");
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        // 写入有效的 magic 但截断的 payload
        file.write_all(&[0x51, 0x57]).unwrap(); // magic
        file.write_all(&[0, 0, 0, 100]).unwrap(); // length = 100
        file.write_all(&[0xDE, 0xAD]).unwrap(); // 只有 2 字节，不够 100
        file.flush().unwrap();
    }

    // Phase 3: 恢复 — 应该读取 5 条有效记录并截断损坏部分
    {
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        let result = wal.replay().unwrap();

        assert_eq!(result.records.len(), 5, "应恢复 5 条有效记录");

        // 验证记录内容
        for (i, record) in result.records.iter().enumerate() {
            match &record.operation {
                nexora_core::wal::WalOperation::NodeEvent { event, .. } => match &event.event {
                    NodeChangeEvent::PropertySet { key, value } => {
                        assert_eq!(key.as_str(), "tick");
                        assert_eq!(*value, PropertyValue::Integer(i as i64 + 1));
                    }
                    _ => panic!("意外的事件类型"),
                },
                _ => panic!("意外的操作类型"),
            }
        }
    }

    // Phase 4: 验证截断后可以继续写入
    {
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        let seq = wal
            .append(nexora_core::wal::WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("after_crash"),
                        value: PropertyValue::Boolean(true),
                    },
                    EventTime::from_micros(99999),
                ),
            })
            .unwrap();

        // 序列号应继续（不重置）
        assert!(seq > 5, "序列号应继续递增，实际为 {seq}");

        let result = wal.replay().unwrap();
        assert_eq!(result.records.len(), 6, "应有 5 条旧记录 + 1 条新记录");
    }

    println!("✅ 场景 7 通过：WAL 损坏恢复 + 截断 + 继续写入");
}

// ============================================================
// 辅助函数
// ============================================================

fn make_service(num_shards: usize, max_nodes: usize) -> GraphService {
    let config = GraphServiceConfig {
        num_shards,
        max_nodes_per_shard: max_nodes,
        node_channel_size: 16,
    };
    let persistor = Arc::new(InMemoryPersistor::new());
    GraphService::new(config, persistor)
}
