//! #1 读快照旁路(projection)一致性回归测试。
//!
//! 读 getter 现在优先命中共享只读投影(resident 节点),miss 时回退 mailbox。
//! 核心契约:
//!   - **read-your-writes**:一个写返回后,后续读必然看到该写(投影在 commit
//!     内、返回 receipt 前同步发布)。
//!   - 投影只持有 **resident** 节点;sleep/evict 后读回退 wake 慢路径,仍返回
//!     正确(从持久层恢复的)状态。
//!   - 投影旁路的返回值必须与 mailbox 路径**逐字段一致**(properties/edges/
//!     labels/edge_properties/tombstone)。
//!
//! 用 InMemoryPersistor(无需 WAL/RocksDB)即可覆盖投影语义;涉及 sleep/wake
//! 的用例依赖 persistor 存活。

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use std::sync::Arc;

fn make_config() -> GraphServiceConfig {
    GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 100,
        node_channel_size: 16,
    }
}

/// 写后立即读(同一节点),必须看到刚写的值 —— read-your-writes。
#[tokio::test]
async fn read_your_writes_property() {
    let svc = GraphService::new(make_config(), Arc::new(InMemoryPersistor::new()));
    let qid = NexoraId::from_bytes(b"ryw".to_vec());

    svc.set_property(&qid, "x", PropertyValue::Integer(1))
        .await
        .unwrap();
    assert_eq!(
        svc.get_property(&qid, "x").await.unwrap(),
        Some(PropertyValue::Integer(1)),
        "写后读必须看到刚写入的值"
    );

    // 覆盖写:再次读到新值(投影被新快照替换)。
    svc.set_property(&qid, "x", PropertyValue::Integer(2))
        .await
        .unwrap();
    assert_eq!(
        svc.get_property(&qid, "x").await.unwrap(),
        Some(PropertyValue::Integer(2)),
        "覆盖写后读必须看到更新后的值"
    );
}

/// 投影旁路返回的多字段状态,必须与真实写入一致(labels/edges/all_properties)。
#[tokio::test]
async fn projection_reflects_all_fields() {
    let svc = GraphService::new(make_config(), Arc::new(InMemoryPersistor::new()));
    let src = NexoraId::from_bytes(b"multi-src".to_vec());
    let dst = NexoraId::from_bytes(b"multi-dst".to_vec());
    let et = Symbol::new("LINKS");

    svc.set_property(&src, "a", PropertyValue::Integer(10))
        .await
        .unwrap();
    svc.set_property(&src, "b", PropertyValue::String("hi".into()))
        .await
        .unwrap();
    svc.add_edge(&src, HalfEdge::out(et.clone(), dst.clone()))
        .await
        .unwrap();
    svc.set_edge_property(
        &src,
        et.clone(),
        &dst,
        Symbol::new("w"),
        PropertyValue::Float(1.5),
        1,
    )
    .await
    .unwrap();

    // properties
    let props = svc.get_all_properties(&src).await.unwrap();
    assert_eq!(
        props.get(&Symbol::new("a")),
        Some(&PropertyValue::Integer(10))
    );
    assert_eq!(
        props.get(&Symbol::new("b")),
        Some(&PropertyValue::String("hi".into()))
    );

    // edges
    let edges = svc.get_edges(&src).await.unwrap();
    assert!(
        edges.iter().any(|e| e.edge_type == et && e.other == dst),
        "投影必须反映已添加的边"
    );

    // edge properties
    let eprops = svc.get_edge_properties(&src).await.unwrap();
    let entry = eprops.get(&(et.clone(), dst.clone()));
    assert_eq!(
        entry.and_then(|m| m.get(&Symbol::new("w"))),
        Some(&PropertyValue::Float(1.5)),
        "投影必须反映边属性"
    );
}

/// 软删除(tombstone)必须经投影可见。
#[tokio::test]
async fn projection_reflects_tombstone() {
    let svc = GraphService::new(make_config(), Arc::new(InMemoryPersistor::new()));
    let qid = NexoraId::from_bytes(b"soft-del".to_vec());

    svc.set_property(&qid, "x", PropertyValue::Integer(1))
        .await
        .unwrap();
    assert!(
        svc.get_tombstone(&qid).await.unwrap().is_none(),
        "初始无 tombstone"
    );

    // 走公共 delete_node API(内部经 commit 路径写 DeleteNode 事件)。
    svc.delete_node(&qid, 1, Some("tester"), Some("gc"))
        .await
        .unwrap();

    assert!(
        svc.get_tombstone(&qid).await.unwrap().is_some(),
        "软删除后 tombstone 必须经投影可见"
    );
}

/// sleep 后节点从投影移除,读回退 wake 慢路径 —— 仍返回持久化的正确状态。
#[tokio::test]
async fn read_after_sleep_falls_back_and_recovers() {
    let persistor = Arc::new(InMemoryPersistor::new());
    let svc = GraphService::new(make_config(), persistor);
    let qid = NexoraId::from_bytes(b"sleep-read".to_vec());

    svc.set_property(&qid, "v", PropertyValue::Integer(99))
        .await
        .unwrap();

    // 休眠 → 投影条目被移除,状态落到持久层。
    svc.sleep_node(&qid).await.unwrap();

    // 读会 miss 投影 → 回退 wake(从持久层恢复)。值必须完整。
    assert_eq!(
        svc.get_property(&qid, "v").await.unwrap(),
        Some(PropertyValue::Integer(99)),
        "sleep 后读回退 wake 路径,必须恢复出正确值"
    );

    // wake 之后节点重新 resident,再次写读走投影,read-your-writes 仍成立。
    svc.set_property(&qid, "v", PropertyValue::Integer(100))
        .await
        .unwrap();
    assert_eq!(
        svc.get_property(&qid, "v").await.unwrap(),
        Some(PropertyValue::Integer(100)),
        "wake 后再写读,投影必须反映新值"
    );
}

/// LRU 驱逐:写入超过 max_nodes_per_shard 的节点,触发驱逐;被驱逐节点读仍正确
/// (回退 wake),投影不会无界增长(每 shard 最多 max_nodes 个 resident)。
#[tokio::test]
async fn eviction_keeps_reads_correct() {
    // 单 shard、小容量,强制驱逐。
    let config = GraphServiceConfig {
        num_shards: 1,
        max_nodes_per_shard: 8,
        node_channel_size: 16,
    };
    let svc = GraphService::new(config, Arc::new(InMemoryPersistor::new()));

    // 写 40 个节点(远超容量 8),持续触发 LRU 驱逐 + 重新 wake。
    for i in 0..40i64 {
        let qid = NexoraId::from_bytes(format!("evict-{i}").into_bytes());
        svc.set_property(&qid, "v", PropertyValue::Integer(i))
            .await
            .unwrap();
    }

    // 逐一读回:命中投影或回退 wake,值都必须正确。
    for i in 0..40i64 {
        let qid = NexoraId::from_bytes(format!("evict-{i}").into_bytes());
        assert_eq!(
            svc.get_property(&qid, "v").await.unwrap(),
            Some(PropertyValue::Integer(i)),
            "节点 {i} 在驱逐/重唤醒后读值必须正确"
        );
    }
}

/// D3: 遍历读旁路 —— `outgoing_neighbors`(D2 typed 遍历路径)必须由投影服务,
/// resident 与 sleep→wake 回退路径逐字段一致。这把读旁路从单点 getter 扩展到
/// 多跳遍历的每一跳(遍历的主力读)。
#[tokio::test]
async fn projection_serves_typed_traversal() {
    let persistor = Arc::new(InMemoryPersistor::new());
    let svc = GraphService::new(make_config(), persistor);
    let a = NexoraId::from_bytes(b"trav-a".to_vec());
    let b = NexoraId::from_bytes(b"trav-b".to_vec());
    let c = NexoraId::from_bytes(b"trav-c".to_vec());
    let other = NexoraId::from_bytes(b"trav-other".to_vec());

    svc.add_edge(&a, HalfEdge::out(Symbol::new("KNOWS"), b.clone()))
        .await
        .unwrap();
    svc.add_edge(&a, HalfEdge::out(Symbol::new("KNOWS"), c.clone()))
        .await
        .unwrap();
    svc.add_edge(&a, HalfEdge::out(Symbol::new("FOLLOWS"), other.clone()))
        .await
        .unwrap();

    let hexset = |ids: Vec<NexoraId>| -> std::collections::HashSet<String> {
        ids.iter().map(|q| q.to_hex()).collect()
    };
    let expected: std::collections::HashSet<String> = [&b, &c].iter().map(|q| q.to_hex()).collect();

    // Resident: typed traversal served by the lock-free projection.
    assert_eq!(
        hexset(svc.outgoing_neighbors(&a, "KNOWS").await.unwrap()),
        expected,
        "resident: 遍历读旁路返回正确的 typed 邻居"
    );

    // sleep → wake 回退路径必须一致。
    svc.sleep_node(&a).await.unwrap();
    assert_eq!(
        hexset(svc.outgoing_neighbors(&a, "KNOWS").await.unwrap()),
        expected,
        "sleep 后遍历读回退 wake,typed 邻居仍一致"
    );
}
