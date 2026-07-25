//! GAP-1 回归测试 — 崩溃恢复正确性
//!
//! 覆盖此前恢复回放循环 no-op 导致的两类数据丢失:
//! 1. 边属性(EdgePropertySet/EdgePropertyRemoved)恢复后丢失。
//! 2. 节点软删除(NodeDeleted)恢复后 tombstone 丢失 → 已删除节点"复活"。
//!
//! 恢复的两条路径(sleep/wake 与崩溃+replay_wal)最终都汇入 wake_node
//! 的事件回放,因此这里用 sleep_node + drop-service + 新建 service 的方式
//! 模拟崩溃恢复,直接验证 wake_node 的重建逻辑。

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

/// 边属性在崩溃恢复后必须保留。
#[tokio::test]
async fn edge_properties_survive_crash_recovery() {
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = make_config();

    let src = NexoraId::from_bytes(b"node-src".to_vec());
    let dst = NexoraId::from_bytes(b"node-dst".to_vec());
    let edge_type = Symbol::new("CONNECTS");

    // ====== Phase 1: 写入边 + 边属性,然后休眠(持久化)======
    {
        let svc = GraphService::new(config.clone(), persistor.clone());

        // 先建边(边属性依附于边关系)
        svc.add_edge(&src, HalfEdge::out(edge_type.clone(), dst.clone()))
            .await
            .unwrap();

        // 设置两个边属性
        svc.set_edge_property(
            &src,
            edge_type.clone(),
            &dst,
            Symbol::new("weight"),
            PropertyValue::Float(3.5),
            1,
        )
        .await
        .unwrap();
        svc.set_edge_property(
            &src,
            edge_type.clone(),
            &dst,
            Symbol::new("label"),
            PropertyValue::String("primary".into()),
            2,
        )
        .await
        .unwrap();

        // 休眠 src 节点 → 持久化 journal
        svc.sleep_node(&src).await.unwrap();
    }
    // Service dropped — 模拟崩溃(persistor 存活 = 磁盘数据)

    // ====== Phase 2: 新建 service,访问节点触发 wake_node 恢复 ======
    {
        let svc = GraphService::new(config, persistor.clone());

        let edge_props = svc.get_edge_properties(&src).await.unwrap();
        let key = (edge_type.clone(), dst.clone());
        let props = edge_props
            .get(&key)
            .expect("边属性 map 应在恢复后包含 (CONNECTS, dst) 键");

        assert_eq!(
            props.get(&Symbol::new("weight")),
            Some(&PropertyValue::Float(3.5)),
            "边属性 weight 应在崩溃恢复后保留"
        );
        assert_eq!(
            props.get(&Symbol::new("label")),
            Some(&PropertyValue::String("primary".into())),
            "边属性 label 应在崩溃恢复后保留"
        );
    }
}

// NOTE: 边属性删除(EdgePropertyRemoved)的恢复路径已在 wake_node 中实现,
// 但目前没有 MutationOp::RemoveEdgeProperty / GraphService::remove_edge_property
// 公共 API 可以产生该事件,因此无法通过公共接口写一个端到端回归测试。
// 待删除 API 补齐后(见 GAP 清单)应补上对应的"设置→删除→崩溃→恢复"测试。

/// 软删除的节点在崩溃恢复后必须仍为已删除状态(不复活)。
#[tokio::test]
async fn deleted_node_stays_deleted_after_recovery() {
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = make_config();

    let qid = NexoraId::from_bytes(b"node-to-delete".to_vec());

    // ====== Phase 1: 写属性 → 软删除 → 休眠 ======
    {
        let svc = GraphService::new(config.clone(), persistor.clone());
        svc.set_property(&qid, "status", PropertyValue::String("active".into()))
            .await
            .unwrap();

        // 恢复前确认 tombstone 为空
        assert!(
            svc.get_tombstone(&qid).await.unwrap().is_none(),
            "删除前不应有 tombstone"
        );

        svc.delete_node(&qid, 1, Some("admin"), Some("test cleanup"))
            .await
            .unwrap();

        // 删除后确认 tombstone 已设置
        let ts = svc.get_tombstone(&qid).await.unwrap();
        assert!(ts.is_some(), "删除后应有 tombstone");

        svc.sleep_node(&qid).await.unwrap();
    }
    // 崩溃

    // ====== Phase 2: 恢复后 tombstone 必须仍在(节点不复活)======
    {
        let svc = GraphService::new(config, persistor.clone());
        let ts = svc.get_tombstone(&qid).await.unwrap();
        assert!(
            ts.is_some(),
            "已软删除的节点在崩溃恢复后应仍为删除状态(GAP-1:此前会因回放 no-op 而复活)"
        );
        let ts = ts.unwrap();
        assert_eq!(
            ts.deleted_by.as_deref(),
            Some("admin"),
            "tombstone 的 deleted_by 元数据应在恢复后保留"
        );
        assert_eq!(
            ts.reason.as_deref(),
            Some("test cleanup"),
            "tombstone 的 reason 元数据应在恢复后保留"
        );
    }
}

/// 对照组:未删除的节点恢复后 tombstone 应为空(确保回放不会误置 tombstone)。
#[tokio::test]
async fn live_node_has_no_tombstone_after_recovery() {
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = make_config();

    let qid = NexoraId::from_bytes(b"node-live".to_vec());
    {
        let svc = GraphService::new(config.clone(), persistor.clone());
        svc.set_property(&qid, "x", PropertyValue::Integer(1))
            .await
            .unwrap();
        svc.sleep_node(&qid).await.unwrap();
    }
    {
        let svc = GraphService::new(config, persistor.clone());
        assert!(
            svc.get_tombstone(&qid).await.unwrap().is_none(),
            "从未删除的节点恢复后不应有 tombstone"
        );
        // 属性仍应正常恢复
        assert_eq!(
            svc.get_property(&qid, "x").await.unwrap(),
            Some(PropertyValue::Integer(1))
        );
    }
}

/// 标签在崩溃恢复后必须保留。
///
/// GAP-1 的快照修复同时修好了 sleep/wake 路径上的标签恢复:此前 snapshot 只
/// 存 properties+edges,标签在 sleep/wake 后被静默丢弃(尽管注释声称"已恢复")。
#[tokio::test]
async fn labels_survive_crash_recovery() {
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = make_config();
    let qid = NexoraId::from_bytes(b"labeled-node".to_vec());

    {
        let svc = GraphService::new(config.clone(), persistor.clone());
        svc.add_label(&qid, Symbol::new("Person"), 1).await.unwrap();
        svc.add_label(&qid, Symbol::new("Employee"), 2)
            .await
            .unwrap();
        svc.sleep_node(&qid).await.unwrap();
    }
    {
        let svc = GraphService::new(config, persistor.clone());
        let labels = svc.get_labels(&qid).await.unwrap();
        assert!(
            labels.contains(&Symbol::new("Person")),
            "标签 Person 应保留"
        );
        assert!(
            labels.contains(&Symbol::new("Employee")),
            "标签 Employee 应保留"
        );
        assert_eq!(labels.len(), 2, "恢复后应恰好有 2 个标签");
    }
}

/// 验收:属性 / 边 / 边属性 / 标签 / 软删除 五类状态在一次崩溃恢复中全部正确往返。
#[tokio::test]
async fn all_node_state_survives_crash_recovery() {
    let persistor = Arc::new(InMemoryPersistor::new());
    let config = make_config();
    let src = NexoraId::from_bytes(b"combined-src".to_vec());
    let dst = NexoraId::from_bytes(b"combined-dst".to_vec());
    let edge_type = Symbol::new("OWNS");

    {
        let svc = GraphService::new(config.clone(), persistor.clone());
        // 属性
        svc.set_property(&src, "name", PropertyValue::String("Alice".into()))
            .await
            .unwrap();
        // 标签
        svc.add_label(&src, Symbol::new("Owner"), 1).await.unwrap();
        // 边 + 边属性
        svc.add_edge(&src, HalfEdge::out(edge_type.clone(), dst.clone()))
            .await
            .unwrap();
        svc.set_edge_property(
            &src,
            edge_type.clone(),
            &dst,
            Symbol::new("since"),
            PropertyValue::Integer(2020),
            2,
        )
        .await
        .unwrap();
        svc.sleep_node(&src).await.unwrap();
    }

    {
        let svc = GraphService::new(config, persistor.clone());
        // 属性
        assert_eq!(
            svc.get_property(&src, "name").await.unwrap(),
            Some(PropertyValue::String("Alice".into())),
            "属性应恢复"
        );
        // 标签
        assert!(
            svc.get_labels(&src)
                .await
                .unwrap()
                .contains(&Symbol::new("Owner")),
            "标签应恢复"
        );
        // 边
        let edges = svc.get_edges(&src).await.unwrap();
        assert_eq!(edges.len(), 1, "边应恢复");
        // 边属性
        let edge_props = svc.get_edge_properties(&src).await.unwrap();
        assert_eq!(
            edge_props
                .get(&(edge_type.clone(), dst.clone()))
                .and_then(|p| p.get(&Symbol::new("since"))),
            Some(&PropertyValue::Integer(2020)),
            "边属性应恢复"
        );
        // 未删除 → 无 tombstone
        assert!(
            svc.get_tombstone(&src).await.unwrap().is_none(),
            "未删除节点不应有 tombstone"
        );
    }
}
