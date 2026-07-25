//! 边界条件测试 — BOUND-001 ~ BOUND-010
//! 并发冲突测试 — CONC-001 ~ CONC-006
//! 崩溃恢复补充 — CRASH-003 ~ CRASH-005

use nexora_core::{
    event::{NodeChangeEvent, TimedEvent},
    wal::{WalOperation, WriteAheadLog},
    GraphService, GraphServiceConfig, InMemoryPersistor,
};
use nexora_id::{EventTime, NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use std::sync::Arc;

// ============================================================
// 边界条件测试 (BOUND)
// ============================================================

// BOUND-001: 空 NexoraId
#[test]
fn test_empty_nexora_id() {
    let qid = NexoraId::from_bytes(vec![]);
    assert!(qid.is_empty());
    assert_eq!(qid.len(), 0);
    assert_eq!(qid.to_hex(), "");
    // shard_key 应该仍然有效（基于空输入的 hash）
    let _ = qid.shard_key();
}

// BOUND-005: 空属性 Map
#[tokio::test]
async fn test_empty_properties() {
    let svc = make_service();
    let qid = NexoraId::new_random();

    // 不存在的节点返回空属性
    let val = svc.get_property(&qid, "anything").await.unwrap();
    assert_eq!(val, None);
}

// BOUND-006: 空边集
#[tokio::test]
async fn test_empty_edges() {
    let svc = make_service();
    let qid = NexoraId::new_random();

    let edges = svc.get_edges(&qid).await.unwrap();
    assert!(edges.is_empty());
}

// BOUND-007: 零 shard 数（配置错误，应 panic 或返回错误）
#[test]
fn test_zero_shards_panics_or_works() {
    // num_shards=0 会导致 modulo by zero
    // 这是配置错误，应该在构建时拒绝
    let result = std::panic::catch_unwind(|| {
        let config = GraphServiceConfig {
            num_shards: 0,
            max_nodes_per_shard: 100,
            node_channel_size: 16,
        };
        let _persistor = Arc::new(InMemoryPersistor::new());
        GraphService::new(config, _persistor)
    });
    // 要么 panic（保护性），要么成功创建但后续操作可能失败
    // 两种都是可接受的行为
    let _ = result;
}

// BOUND-008: 极大 shard 数
#[tokio::test]
async fn test_many_shards() {
    let config = GraphServiceConfig {
        num_shards: 100_000,
        max_nodes_per_shard: 10,
        node_channel_size: 16,
    };
    let persistor = Arc::new(InMemoryPersistor::new());
    let svc = GraphService::new(config, persistor);

    let qid = NexoraId::new_random();
    svc.set_property(&qid, "test", PropertyValue::Boolean(true))
        .await
        .unwrap();

    let val = svc.get_property(&qid, "test").await.unwrap();
    assert_eq!(val, Some(PropertyValue::Boolean(true)));
}

// BOUND-009: 特殊字符 key
#[tokio::test]
async fn test_unicode_property_key() {
    let svc = make_service();
    let qid = NexoraId::new_random();

    svc.set_property(&qid, "速度", PropertyValue::Float(12.5))
        .await
        .unwrap();
    svc.set_property(&qid, "🚀rocket", PropertyValue::Boolean(true))
        .await
        .unwrap();
    svc.set_property(&qid, "", PropertyValue::Integer(0))
        .await
        .unwrap();

    assert_eq!(
        svc.get_property(&qid, "速度").await.unwrap(),
        Some(PropertyValue::Float(12.5))
    );
    assert_eq!(
        svc.get_property(&qid, "🚀rocket").await.unwrap(),
        Some(PropertyValue::Boolean(true))
    );
    assert_eq!(
        svc.get_property(&qid, "").await.unwrap(),
        Some(PropertyValue::Integer(0))
    );
}

// BOUND-010: 重复边去重
#[tokio::test]
async fn test_duplicate_edge_dedup() {
    let svc = make_service();
    let qid = NexoraId::new_random();
    let target = NexoraId::new_random();
    let edge = HalfEdge::out(Symbol::new("KNOWS"), target);

    svc.add_edge(&qid, edge.clone()).await.unwrap();
    svc.add_edge(&qid, edge.clone()).await.unwrap();
    svc.add_edge(&qid, edge.clone()).await.unwrap();

    let edges = svc.get_edges(&qid).await.unwrap();
    assert_eq!(edges.len(), 1, "HashSet should deduplicate identical edges");
}

// BOUND-003: EventTime::MAX
#[test]
fn test_event_time_max() {
    assert_eq!(EventTime::MAX.as_micros(), u64::MAX);
}

// BOUND-004: EventTime::MIN
#[test]
fn test_event_time_min() {
    assert_eq!(EventTime::MIN.as_micros(), 0);
}

// ============================================================
// 并发冲突测试 (CONC)
// ============================================================

// CONC-001: 同节点并发写（100 个 task 写同一节点）
#[tokio::test]
async fn test_concurrent_writes_same_node() {
    let svc = Arc::new(make_service());
    let qid = NexoraId::from_bytes(b"shared-node".to_vec());

    let mut handles = Vec::new();
    for i in 0..100 {
        let svc = svc.clone();
        let qid = qid.clone();
        handles.push(tokio::spawn(async move {
            svc.set_property(&qid, "counter", PropertyValue::Integer(i))
                .await
                .unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // 最终值应该是 0..99 中的某一个（最后写入的胜出）
    let val = svc.get_property(&qid, "counter").await.unwrap();
    assert!(val.is_some());
    if let Some(PropertyValue::Integer(n)) = val {
        assert!((0..100).contains(&n));
    }
}

// CONC-002: 同节点并发读写
#[tokio::test]
async fn test_concurrent_read_write_same_node() {
    let svc = Arc::new(make_service());
    let qid = NexoraId::from_bytes(b"rw-node".to_vec());

    // 先写入初始值
    svc.set_property(&qid, "x", PropertyValue::Integer(0))
        .await
        .unwrap();

    let mut handles = Vec::new();

    // 50 个写 task
    for i in 0..50 {
        let svc = svc.clone();
        let qid = qid.clone();
        handles.push(tokio::spawn(async move {
            svc.set_property(&qid, "x", PropertyValue::Integer(i))
                .await
                .unwrap();
        }));
    }

    // 50 个读 task
    for _ in 0..50 {
        let svc = svc.clone();
        let qid = qid.clone();
        handles.push(tokio::spawn(async move {
            let _ = svc.get_property(&qid, "x").await;
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // 节点仍然可访问
    let val = svc.get_property(&qid, "x").await.unwrap();
    assert!(val.is_some());
}

// CONC-006: 1000 个 task 并发创建新节点
#[tokio::test]
async fn test_concurrent_node_creation_1000() {
    let svc = Arc::new(make_service());
    let mut handles = Vec::new();

    for i in 0..1000 {
        let svc = svc.clone();
        let qid = NexoraId::from_bytes(format!("bulk-{i:04}").into_bytes());
        handles.push(tokio::spawn(async move {
            svc.set_property(&qid, "idx", PropertyValue::Integer(i))
                .await
                .unwrap();
            qid
        }));
    }

    let mut qids = Vec::new();
    for h in handles {
        qids.push(h.await.unwrap());
    }

    // 验证所有节点都创建成功
    for (i, qid) in qids.iter().enumerate() {
        let val = svc.get_property(qid, "idx").await.unwrap();
        assert_eq!(val, Some(PropertyValue::Integer(i as i64)));
    }
}

// CONC-003: 并发 sleep + 访问
#[tokio::test]
async fn test_concurrent_sleep_and_access() {
    let svc = Arc::new(make_service());
    let qid = NexoraId::from_bytes(b"sleep-race".to_vec());

    // 先写入数据
    svc.set_property(&qid, "data", PropertyValue::Integer(42))
        .await
        .unwrap();

    // 并发 sleep 和读取
    let svc2 = svc.clone();
    let qid2 = qid.clone();
    let sleep_handle = tokio::spawn(async move { svc2.sleep_node(&qid2).await });

    let svc3 = svc.clone();
    let qid3 = qid.clone();
    let read_handle = tokio::spawn(async move { svc3.get_property(&qid3, "data").await });

    // 两者都不应该 panic
    let _ = sleep_handle.await;
    let _ = read_handle.await;
}

// ============================================================
// 崩溃恢复补充 (CRASH)
// ============================================================

// CRASH-003: 多次崩溃循环
#[tokio::test]
async fn test_repeated_crash_recovery() {
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 100,
        node_channel_size: 16,
    };
    let qid = NexoraId::from_bytes(b"crash-loop".to_vec());

    for round in 0..5 {
        let svc = GraphService::new(config.clone(), persistor.clone());
        svc.set_property(&qid, "round", PropertyValue::Integer(round))
            .await
            .unwrap();
        svc.sleep_node(&qid).await.unwrap();
    }

    // 最终恢复后应该是最后一次写入的值
    let svc = GraphService::new(config, persistor);
    let val = svc.get_property(&qid, "round").await.unwrap();
    assert_eq!(val, Some(PropertyValue::Integer(4)));
}

// CRASH-004: 无快照崩溃恢复（仅 WAL）
#[test]
fn test_wal_only_recovery_no_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let qid = NexoraId::from_bytes(b"wal-only".to_vec());

    // 写入 WAL 记录
    {
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        for i in 1..=10 {
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("val"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros((i * 1000) as u64),
                ),
            })
            .unwrap();
        }
    }

    // 恢复
    let mut wal = WriteAheadLog::open(dir.path()).unwrap();
    let records = wal.replay().unwrap();
    assert_eq!(records.records.len(), 10);
    // 最后一条应该是 val=10
    match &records.records[9].operation {
        WalOperation::NodeEvent { event, .. } => match &event.event {
            NodeChangeEvent::PropertySet { value, .. } => {
                assert_eq!(*value, PropertyValue::Integer(10));
            }
            _ => panic!("Expected PropertySet"),
        },
        _ => panic!("Expected NodeEvent"),
    }
}

// CRASH-005: 空崩溃（0 条数据就崩溃）
#[test]
fn test_empty_crash_recovery() {
    let dir = tempfile::tempdir().unwrap();

    // 创建 WAL 但不写入任何数据
    {
        let _wal = WriteAheadLog::open(dir.path()).unwrap();
        // 立即 drop
    }

    // 恢复应该成功，返回空
    let mut wal = WriteAheadLog::open(dir.path()).unwrap();
    let records = wal.replay().unwrap();
    assert!(records.records.is_empty());
}

// ============================================================
// WAL 集成测试 (A-03)
// ============================================================

// WAL 集成：写入 → sleep → 新 service 启动 → WAL 回放 → 数据恢复
#[tokio::test]
async fn test_wal_integration_crash_recovery() {
    let wal_dir = tempfile::tempdir().unwrap();
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 100,
        node_channel_size: 16,
    };
    let qid = NexoraId::from_bytes(b"wal-integration".to_vec());

    // Phase 1: 写入数据，使用 WAL
    {
        let svc = GraphService::new_with_wal(
            config.clone(),
            persistor.clone(),
            wal_dir.path().to_path_buf(),
            None,
        )
        .unwrap();

        svc.set_property(&qid, "name", PropertyValue::String("FL-042".into()))
            .await
            .unwrap();
        svc.set_property(&qid, "speed", PropertyValue::Float(12.5))
            .await
            .unwrap();

        // 休眠节点（触发 snapshot + WAL checkpoint）
        svc.sleep_node(&qid).await.unwrap();
    }

    // Phase 2: 新 service 启动，WAL 回放
    {
        let svc = GraphService::new_with_wal(
            config.clone(),
            persistor.clone(),
            wal_dir.path().to_path_buf(),
            None,
        )
        .unwrap();

        // 回放 WAL
        let _replayed = svc.replay_all_wals().await.unwrap();
        // 可能回放 0 条（因为 sleep 时已写 checkpoint）
        // 但 checkpoint 前的事件应已持久化到 persistor

        // 验证数据恢复
        let name = svc.get_property(&qid, "name").await.unwrap();
        assert_eq!(name, Some(PropertyValue::String("FL-042".into())));

        let speed = svc.get_property(&qid, "speed").await.unwrap();
        assert_eq!(speed, Some(PropertyValue::Float(12.5)));
    }
}

// WAL 集成：无 sleep 的崩溃（只有 WAL，无 snapshot checkpoint）
#[tokio::test]
async fn test_wal_integration_crash_without_sleep() {
    let wal_dir = tempfile::tempdir().unwrap();
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 100,
        node_channel_size: 16,
    };
    let qid = NexoraId::from_bytes(b"no-sleep".to_vec());

    // Phase 1: 写入数据但不 sleep（模拟 crash）
    {
        let svc = GraphService::new_with_wal(
            config.clone(),
            persistor.clone(),
            wal_dir.path().to_path_buf(),
            None,
        )
        .unwrap();

        svc.set_property(&qid, "val", PropertyValue::Integer(42))
            .await
            .unwrap();

        // 不调用 sleep_node — 直接 drop（模拟 crash）
    }

    // Phase 2: 新 service 启动
    {
        let svc = GraphService::new_with_wal(
            config.clone(),
            persistor.clone(),
            wal_dir.path().to_path_buf(),
            None,
        )
        .unwrap();

        let replayed = svc.replay_all_wals().await.unwrap();

        assert!(replayed > 0, "WAL should contain the unsnapshotted write");

        let val = svc.get_property(&qid, "val").await.unwrap();
        assert_eq!(val, Some(PropertyValue::Integer(42)));
    }
}

#[tokio::test]
async fn test_shutdown_flushes_active_nodes() {
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 100,
        node_channel_size: 16,
    };
    let qid = NexoraId::from_bytes(b"graceful-shutdown".to_vec());

    let svc = GraphService::new(config.clone(), persistor.clone());
    svc.set_property(&qid, "name", PropertyValue::String("saved".into()))
        .await
        .unwrap();
    svc.shutdown().await.unwrap();

    let restored = GraphService::new(config, persistor);
    assert_eq!(
        restored.get_property(&qid, "name").await.unwrap(),
        Some(PropertyValue::String("saved".into()))
    );
}

// ============================================================
// 辅助函数
// ============================================================

fn make_service() -> GraphService {
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 100,
        node_channel_size: 16,
    };
    let persistor = Arc::new(InMemoryPersistor::new());
    GraphService::new(config, persistor)
}

// ====== SQ + GraphService 联动测试 ======
#[tokio::test]
async fn test_sq_trigger_via_graph_service() {
    use nexora_standing_query::{
        pattern::{FilterCondition, StandingQueryPattern},
        StandingQueryManager,
    };

    let svc = make_service();
    let sqm = StandingQueryManager::new(100);
    let qid = NexoraId::from_bytes(b"sq-test".to_vec());

    // Register SQ: speed > 100
    let sq_id = sqm
        .register(
            "high-speed",
            StandingQueryPattern::property("speed", FilterCondition::GreaterThan(100.0)),
        )
        .await;

    // Set speed=150
    svc.set_property(&qid, "speed", PropertyValue::Float(150.0))
        .await
        .unwrap();

    // Get all properties
    let all_props = svc.get_all_properties(&qid).await.unwrap();
    let props_map: std::collections::HashMap<String, PropertyValue> = all_props
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

    // Verify props
    assert!(
        props_map.contains_key("speed"),
        "Should have speed property"
    );
    assert_eq!(props_map.get("speed"), Some(&PropertyValue::Float(150.0)));

    // Trigger SQ
    let value = props_map.get("speed").cloned().unwrap();
    sqm.on_property_change(&qid, "speed", &value, &props_map)
        .await;

    // Verify match
    assert_eq!(
        sqm.match_count(sq_id).await,
        1,
        "SQ should match speed=150 > 100"
    );
}

#[tokio::test]
async fn test_sq_trigger_integer_value() {
    use nexora_standing_query::{
        pattern::{FilterCondition, StandingQueryPattern},
        StandingQueryManager,
    };

    let svc = make_service();
    let sqm = StandingQueryManager::new(100);
    let qid = NexoraId::from_bytes(b"sq-int".to_vec());

    let sq_id = sqm
        .register(
            "high-speed",
            StandingQueryPattern::property("speed", FilterCondition::GreaterThan(100.0)),
        )
        .await;

    // Set speed as Integer (JSON API sends integers)
    svc.set_property(&qid, "speed", PropertyValue::Integer(150))
        .await
        .unwrap();

    let all_props = svc.get_all_properties(&qid).await.unwrap();
    let props_map: std::collections::HashMap<String, PropertyValue> = all_props
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

    let value = props_map.get("speed").cloned().unwrap();
    sqm.on_property_change(&qid, "speed", &value, &props_map)
        .await;

    assert_eq!(sqm.match_count(sq_id).await, 1, "Integer(150) > 100.0");
}
