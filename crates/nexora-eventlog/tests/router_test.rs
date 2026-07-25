#![cfg(feature = "olap")]
//! 集成测试: TopicRouter 路由逻辑

use nexora_eventlog::{Destination, TopicRouter};

#[test]
fn test_router_all_graph() {
    let router = TopicRouter::all_graph();
    assert_eq!(router.route("any_topic"), Destination::Graph);
    assert_eq!(router.route("sensor/temp"), Destination::Graph);
}

#[test]
fn test_router_all_both() {
    let router = TopicRouter::all_both();
    assert_eq!(router.route("any_topic"), Destination::Both);
    assert_eq!(router.route("sensor/temp"), Destination::Both);
}

#[test]
fn test_router_upsert_rule_runtime() {
    let router = TopicRouter::all_graph();
    // 默认所有 topic 走图
    assert_eq!(router.route("orders"), Destination::Graph);

    // 运行时插入规则:orders → Both
    router.upsert_rule("orders".to_string(), Destination::Both);
    assert_eq!(router.route("orders"), Destination::Both);
    // 其它 topic 仍走 default
    assert_eq!(router.route("other"), Destination::Graph);

    // 覆盖已有规则
    router.upsert_rule("orders".to_string(), Destination::EventTable);
    assert_eq!(router.route("orders"), Destination::EventTable);
}
