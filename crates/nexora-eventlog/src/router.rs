//! TopicRouter — topic → destination 路由
//!
//! 负责:
//! 1. 根据 DomainPackage 配置决定 topic 的路由目标
//! 2. 支持三种 destination:EventTable(只事件表)、Graph(只图)、Both(双写)
//! 3. 阶段 1:有 mapping 的 topic → Both,未 mapped → Graph(兼容)
//! 4. 阶段 6:运行时可变(arc-swap)——本体管理 API 创建本体后热更路由规则,
//!    运行中的摄入立即生效,无需重启。
//!
//! # 并发模型
//!
//! `rules` 用 [`ArcSwap`] 持有,读路径(`route`,每 batch 一次)无锁 load;
//! 写路径(`apply_domain_package`/`upsert_rule`)copy-on-write:clone 当前
//! map、改、原子 swap 回去。写很少(仅本体 CRUD)、读极频,这个权衡合适。

use arc_swap::ArcSwap;
use nexora_core::DomainPackage;
use std::collections::HashMap;
use std::sync::Arc;

/// 路由目标
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Destination {
    /// 只写事件表(Iceberg)
    EventTable,
    /// 只投图(兼容模式)
    Graph,
    /// 双写:先事件表,再投图(阶段 1 默认)
    Both,
}

/// TopicRouter 根据配置决定 topic 的路由目标。
///
/// 内部 `rules` 运行时可变(arc-swap);`default` 构造时固定。
pub struct TopicRouter {
    rules: ArcSwap<HashMap<String, Destination>>,
    default: Destination,
}

impl TopicRouter {
    /// 从 DomainPackage 构造路由规则
    ///
    /// - 有 EventMapping 的 topic → Both(双写)
    /// - 未 mapped 的 topic → Graph(兼容,保持现有行为)
    pub fn from_domain_package(pkg: &DomainPackage) -> Self {
        let mut rules = HashMap::new();
        for mapping in &pkg.mappings {
            let topic = mapping.source.clone();
            tracing::info!("Topic '{}' mapped to Destination::Both", topic);
            rules.insert(topic, Destination::Both);
        }

        Self {
            rules: ArcSwap::from_pointee(rules),
            default: Destination::Graph, // 未映射的 topic 保持图优先(兼容)
        }
    }

    /// 创建一个"全部双写"的路由器(测试用)
    pub fn all_both() -> Self {
        Self {
            rules: ArcSwap::from_pointee(HashMap::new()),
            default: Destination::Both,
        }
    }

    /// 创建一个"全部只写图"的路由器(兼容模式,event-first 启动默认)
    pub fn all_graph() -> Self {
        Self {
            rules: ArcSwap::from_pointee(HashMap::new()),
            default: Destination::Graph,
        }
    }

    /// 路由:根据 topic 名返回 destination(owned Clone)。
    ///
    /// 热路径:每摄入 batch 调一次。arc-swap `load` 近乎无锁。
    pub fn route(&self, topic: &str) -> Destination {
        self.rules
            .load()
            .get(topic)
            .cloned()
            .unwrap_or_else(|| self.default.clone())
    }

    /// 运行时并入一个 DomainPackage 的路由规则(copy-on-write)。
    ///
    /// 本体管理 API 创建/更新本体后调用:该 pkg 每个 EventMapping 的
    /// `source` topic → `Both`(双写事件表 + 图)。已存在的规则被覆盖。
    pub fn apply_domain_package(&self, pkg: &DomainPackage) {
        if pkg.mappings.is_empty() {
            return;
        }
        let mut next = (**self.rules.load()).clone();
        for mapping in &pkg.mappings {
            tracing::info!(
                "Topic '{}' mapped to Destination::Both (domain '{}')",
                mapping.source,
                pkg.schema.domain
            );
            next.insert(mapping.source.clone(), Destination::Both);
        }
        self.rules.store(Arc::new(next));
    }

    /// 运行时插入/更新单条路由规则(copy-on-write)。
    pub fn upsert_rule(&self, topic: String, dest: Destination) {
        let mut next = (**self.rules.load()).clone();
        next.insert(topic, dest);
        self.rules.store(Arc::new(next));
    }

    /// 运行时移除一个 DomainPackage 的路由规则(topic 退回 default)。
    ///
    /// 本体删除时调用。事件表本身不删(append-only 真相源)。
    pub fn remove_domain_package(&self, pkg: &DomainPackage) {
        if pkg.mappings.is_empty() {
            return;
        }
        let mut next = (**self.rules.load()).clone();
        for mapping in &pkg.mappings {
            next.remove(&mapping.source);
        }
        self.rules.store(Arc::new(next));
    }

    /// 验证 topic 名合法性(阶段 2 实现)
    ///
    /// 当前返回 Ok,阶段 2 再加:
    /// - Iceberg 标识符规则
    /// - 路径穿越检测
    /// - 长度/字符限制
    pub fn validate_topic(&self, _topic: &str) -> anyhow::Result<()> {
        // TODO(阶段 2): 实现 topic 校验
        Ok(())
    }
}

// 单元测试见 tests/router_test.rs (集成测试)
