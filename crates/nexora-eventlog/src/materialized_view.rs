//! 物化视图 (Materialized Views) - 预计算聚合结果
//!
//! 核心概念:
//! - 从事件表创建预计算的聚合视图
//! - 支持 Pull 模式 (定时刷新) 和 Push 模式 (实时增量)
//! - 存储为独立的 Iceberg 表

use serde::{Deserialize, Serialize};

/// 物化视图定义
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedView {
    /// 视图名称
    pub name: String,

    /// 源表名称
    pub source_table: String,

    /// 转换逻辑 (SQL-like)
    pub transform: ViewTransform,

    /// 刷新策略
    pub refresh_mode: RefreshMode,

    /// 目标表名称 (存储视图结果)
    pub target_table: String,
}

/// 转换逻辑
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ViewTransform {
    /// SQL 查询 (未来支持)
    #[allow(dead_code)]
    Sql(String),

    /// 聚合操作
    Aggregate {
        /// 分组字段
        group_by: Vec<String>,
        /// 聚合函数
        aggregations: Vec<Aggregation>,
        /// WHERE 条件 (可选)
        filter: Option<String>,
    },
}

/// 聚合函数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Aggregation {
    Count { field: String, alias: String },
    Sum { field: String, alias: String },
    Avg { field: String, alias: String },
    Max { field: String, alias: String },
    Min { field: String, alias: String },
}

/// 刷新策略
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RefreshMode {
    /// Pull 模式: 定时刷新 (每 N 秒)
    Pull { interval_secs: u64 },

    /// Push 模式: 实时增量更新
    Push,

    /// 混合模式: 实时增量 + 定时全量
    Hybrid {
        push_enabled: bool,
        pull_interval_secs: u64,
    },
}

impl MaterializedView {
    /// 创建聚合类型的物化视图
    pub fn aggregate(
        name: impl Into<String>,
        source_table: impl Into<String>,
        group_by: Vec<String>,
        aggregations: Vec<Aggregation>,
        refresh_mode: RefreshMode,
    ) -> Self {
        let name = name.into();
        let source_table = source_table.into();
        let target_table = format!("{}_mv", name);

        Self {
            name,
            source_table,
            transform: ViewTransform::Aggregate {
                group_by,
                aggregations,
                filter: None,
            },
            refresh_mode,
            target_table,
        }
    }

    /// 设置过滤条件
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        if let ViewTransform::Aggregate { filter: ref mut f, .. } = self.transform {
            *f = Some(filter.into());
        }
        self
    }

    /// 设置自定义目标表名
    pub fn with_target_table(mut self, target_table: impl Into<String>) -> Self {
        self.target_table = target_table.into();
        self
    }

    /// 创建 SQL 类型的物化视图。
    ///
    /// `source_table` 是查询读取的事件表名(用于从 Iceberg 加载 + 在
    /// DataFusion 里以真实表名注册)。`sql` 是完整查询,`FROM <source_table>`。
    pub fn sql(
        name: impl Into<String>,
        source_table: impl Into<String>,
        sql: impl Into<String>,
        refresh_mode: RefreshMode,
    ) -> Self {
        let name = name.into();
        let target_table = format!("{}_mv", name);
        Self {
            name,
            source_table: source_table.into(),
            transform: ViewTransform::Sql(sql.into()),
            refresh_mode,
            target_table,
        }
    }

    /// 从 DomainPackage 里的 [`DomainMV`](nexora_core::domain_package::DomainMV)
    /// 构造一个 SQL 物化视图。
    ///
    /// 契约(阶段6):`DomainMV.query` 是**单源表 SQL**,形如
    /// `SELECT ... FROM <event_table> [WHERE ...] [GROUP BY ...]`。源表名从
    /// `FROM` 子句解析(见 [`parse_source_table`])。`refresh_mode` 字符串:
    /// `"pull"` / `"pull:<secs>"` / `"scheduled:<secs>"` → Pull(默认 60s);
    /// `"push"` → Push;其它(含空串)→ Pull 默认。
    ///
    /// 返回 `Err` 当无法从 query 解析出源表(不满足单源表 SQL 契约)。
    pub fn from_domain_mv(mv: &nexora_core::domain_package::DomainMV) -> Result<Self, String> {
        let source_table = parse_source_table(&mv.query).ok_or_else(|| {
            format!(
                "DomainMV '{}': cannot parse a single source table from FROM clause \
                 (query must be single-table SQL: SELECT ... FROM <event_table> ...)",
                mv.name
            )
        })?;
        let refresh_mode = parse_refresh_mode(&mv.refresh_mode);
        Ok(Self::sql(
            mv.name.clone(),
            source_table,
            mv.query.clone(),
            refresh_mode,
        ))
    }
}

/// 解析 `refresh_mode` 字符串为 [`RefreshMode`]。
///
/// `"push"` → Push;`"pull"` / `"scheduled"` → Pull(60s);
/// `"pull:<secs>"` / `"scheduled:<secs>"` → Pull(自定义秒);其它 → Pull(60s)。
fn parse_refresh_mode(s: &str) -> RefreshMode {
    const DEFAULT_INTERVAL: u64 = 60;
    let s = s.trim().to_ascii_lowercase();
    if s == "push" {
        return RefreshMode::Push;
    }
    let (kind, rest) = match s.split_once(':') {
        Some((k, v)) => (k, Some(v)),
        None => (s.as_str(), None),
    };
    match kind {
        "pull" | "scheduled" | "" => {
            let interval = rest
                .and_then(|v| v.trim().parse::<u64>().ok())
                .filter(|n| *n > 0)
                .unwrap_or(DEFAULT_INTERVAL);
            RefreshMode::Pull { interval_secs: interval }
        }
        _ => RefreshMode::Pull { interval_secs: DEFAULT_INTERVAL },
    }
}

/// 从单源表 SQL 里解析出 `FROM` 后的表名。
///
/// 只支持简单的单表 `FROM <ident>` 形态(可选 schema 限定 `a.b` 取最后一段,
/// 可选反引号/双引号包裹)。多表 JOIN、子查询等复杂形态返回 `None`(契约外)。
fn parse_source_table(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    let from_pos = lower.find(" from ")?;
    let after = &sql[from_pos + 6..];
    // 取 FROM 之后第一个 token(到空白 / 逗号 / 分号 / 括号为止)。
    let token: String = after
        .trim_start()
        .chars()
        .take_while(|c| !c.is_whitespace() && !matches!(c, ',' | ';' | '(' | ')'))
        .collect();
    if token.is_empty() {
        return None;
    }
    // 去掉 schema 限定,取最后一段;去掉包裹的引号/反引号。
    let ident = token.rsplit('.').next().unwrap_or(&token);
    let ident = ident.trim_matches(|c| c == '`' || c == '"');
    if ident.is_empty() {
        None
    } else {
        Some(ident.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_aggregate_view() {
        let view = MaterializedView::aggregate(
            "sensor_hourly",
            "iot_sensors",
            vec!["device_id".into()],
            vec![
                Aggregation::Avg {
                    field: "temperature".into(),
                    alias: "avg_temp".into(),
                },
                Aggregation::Count {
                    field: "*".into(),
                    alias: "count".into(),
                },
            ],
            RefreshMode::Pull { interval_secs: 300 },
        );

        assert_eq!(view.name, "sensor_hourly");
        assert_eq!(view.source_table, "iot_sensors");
        assert_eq!(view.target_table, "sensor_hourly_mv");

        match view.transform {
            ViewTransform::Aggregate { group_by, aggregations, .. } => {
                assert_eq!(group_by.len(), 1);
                assert_eq!(aggregations.len(), 2);
            }
            _ => panic!("Expected Aggregate transform"),
        }
    }

    #[test]
    fn test_view_with_filter() {
        let view = MaterializedView::aggregate(
            "recent_sensors",
            "iot_sensors",
            vec!["device_id".into()],
            vec![],
            RefreshMode::Push,
        )
        .with_filter("_event_time > now() - interval '1 hour'");

        match view.transform {
            ViewTransform::Aggregate { filter, .. } => {
                assert!(filter.is_some());
            }
            _ => panic!("Expected Aggregate transform"),
        }
    }

    #[test]
    fn test_refresh_modes() {
        let pull = RefreshMode::Pull { interval_secs: 60 };
        let push = RefreshMode::Push;
        let hybrid = RefreshMode::Hybrid {
            push_enabled: true,
            pull_interval_secs: 300,
        };

        // 验证序列化
        let _ = serde_json::to_string(&pull).unwrap();
        let _ = serde_json::to_string(&push).unwrap();
        let _ = serde_json::to_string(&hybrid).unwrap();
    }
}
