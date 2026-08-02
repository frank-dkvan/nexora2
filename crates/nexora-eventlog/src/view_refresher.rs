//! ViewRefresher - 物化视图刷新执行器
//!
//! 负责:
//! 1. 执行视图转换逻辑 (聚合计算)
//! 2. 全量刷新 (Pull 模式)
//! 3. 增量刷新 (Push 模式)
//!
//! 实现策略:
//! - 使用 DataFusion 执行聚合计算
//! - 读取源表 → 聚合 → 写入目标表

use crate::event_log_store::{
    EventLogStore, MV_SRC_SNAPSHOT_COL, MV_UPDATED_AT_COL, MV_VERSION_COL,
};
use crate::materialized_view::{Aggregation, MaterializedView, ViewTransform};
use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use nexora_core::RawEvent;
use std::sync::Arc;

/// 物化视图目标表里,内部版本列的集合(读回时需从对外结果中剥离)。
const MV_INTERNAL_COLS: &[&str] = &[MV_VERSION_COL, MV_SRC_SNAPSHOT_COL, MV_UPDATED_AT_COL];

/// 目标表的「一代」结果:版本号 + 该代已并入的源表 snapshot 水位 + 数据(含内部列)。
struct Generation {
    version: i64,
    src_snapshot_id: Option<i64>,
    batches: Vec<RecordBatch>,
}

/// 一个「可合并度量」在目标表里的存储表示。
///
/// 关键设计:目标表存**可合并的部分聚合**而非最终值。AVG 拆成 `sum`+`count`
/// 两个伴生列,这样「旧态 ⊕ 新增量」= 两份部分聚合 `UNION ALL` 后按 combine
/// 算子再归约一次(结合律成立):COUNT→SUM、SUM→SUM、MAX→MAX、MIN→MIN、
/// AVG 的 sum→SUM / count→SUM。读回时再 `sum/count` 还原 AVG。
struct StoredMeasure {
    /// 存储列名
    col: String,
    /// 初次聚合表达式(over 原始事件 / delta)
    partial_expr: String,
    /// 合并表达式(over 旧态 UNION delta 的部分聚合)
    merge_expr: String,
}

pub struct ViewRefresher {
    event_log_store: Arc<EventLogStore>,
}

impl ViewRefresher {
    pub fn new(event_log_store: Arc<EventLogStore>) -> Self {
        Self { event_log_store }
    }

    /// 全量刷新视图
    pub async fn refresh(&self, view: &MaterializedView) -> Result<u64> {
        tracing::info!("Full refresh for view '{}'", view.name);

        match &view.transform {
            ViewTransform::Aggregate {
                group_by,
                aggregations,
                filter,
            } => {
                self.refresh_aggregate(view, group_by, aggregations, filter.as_deref())
                    .await
            }
            ViewTransform::Sql(sql) => self.refresh_sql(view, sql).await,
        }
    }

    /// 读取物化视图目标表的「最新一代」结果,剥掉内部 `_mv_*` 列。
    ///
    /// 目标表 append-only、每次刷新写完整的一代;这里取 `max(_mv_version)`
    /// 那一代作为当前结果(读时去重)。表不存在/为空时返回空 vec。
    pub async fn read_latest(&self, view: &MaterializedView) -> Result<Vec<RecordBatch>> {
        let gen = match self.latest_generation(&view.target_table).await? {
            Some(g) => g,
            None => return Ok(Vec::new()),
        };
        self.outward_project(view, gen.batches).await
    }

    /// 把存储形态的一代结果投影成对外形态:剥内部 `_mv_*` 列,并把 AVG 的
    /// `alias__sum`/`alias__count` 伴生列还原为 `alias = sum/count`。
    async fn outward_project(
        &self,
        view: &MaterializedView,
        gen_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        // 找出需要还原的 AVG 度量。
        let avg_aliases: Vec<(String, String)> = match &view.transform {
            ViewTransform::Aggregate { aggregations, .. } => aggregations
                .iter()
                .filter_map(|a| match a {
                    Aggregation::Avg { alias, .. } => {
                        Some((format!("{}__sum", alias), format!("{}__count", alias)))
                    }
                    _ => None,
                })
                .collect(),
            // SQL 视图没有 AVG 伴生列约定,直接剥内部列即可。
            ViewTransform::Sql(_) => Vec::new(),
        };

        if avg_aliases.is_empty() {
            return Self::strip_internal_cols(gen_batches);
        }

        // 用 DataFusion 做投影:非内部、非伴生列原样保留;每个 AVG 输出 sum/count。
        let ctx = SessionContext::new();
        let schema = gen_batches[0].schema();
        let mem = datafusion::datasource::MemTable::try_new(schema.clone(), vec![gen_batches])?;
        ctx.register_table("gen", Arc::new(mem))?;

        let companion_cols: std::collections::HashSet<String> = avg_aliases
            .iter()
            .flat_map(|(s, c)| [s.clone(), c.clone()])
            .collect();

        let mut select: Vec<String> = Vec::new();
        for f in schema.fields() {
            let name = f.name();
            if MV_INTERNAL_COLS.contains(&name.as_str()) || companion_cols.contains(name) {
                continue;
            }
            select.push(name.clone());
        }
        for (sum_col, count_col) in &avg_aliases {
            // alias = sum/count(count 为 0 时输出 NULL 而非除零错误)。
            let alias = sum_col.trim_end_matches("__sum");
            select.push(format!(
                "CAST({sum_col} AS DOUBLE) / NULLIF(CAST({count_col} AS DOUBLE), 0) AS {alias}"
            ));
        }

        let sql = format!("SELECT {} FROM gen", select.join(", "));
        let out = ctx
            .sql(&sql)
            .await
            .context("Failed to project outward MV result")?
            .collect()
            .await
            .context("Failed to collect outward MV result")?;
        Ok(out)
    }

    /// 读取目标表最新一代(保留内部 `_mv_*` 列),并解析出该代的版本号与源表
    /// snapshot 水位。供全量/增量刷新决定下一代版本号与增量水位。
    async fn latest_generation(&self, target_table: &str) -> Result<Option<Generation>> {
        use arrow::array::Int64Array;

        let batches = self
            .event_log_store
            .read_table_batches(target_table)
            .await?;
        if batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(None);
        }

        // 用 DataFusion 取 max(_mv_version) 的那一代。
        let ctx = SessionContext::new();
        let schema = batches[0].schema();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![batches])?;
        ctx.register_table("mv", Arc::new(mem))?;
        let sql = format!(
            "SELECT * FROM mv WHERE {v} = (SELECT MAX({v}) FROM mv)",
            v = MV_VERSION_COL
        );
        let gen_batches = ctx
            .sql(&sql)
            .await
            .context("Failed to query latest MV generation")?
            .collect()
            .await
            .context("Failed to collect latest MV generation")?;

        if gen_batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(None);
        }

        // 从首个非空 batch 的首行读版本号与源 snapshot 水位。
        let first = gen_batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .expect("non-empty generation");
        let version = first
            .column_by_name(MV_VERSION_COL)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .map(|a| a.value(0))
            .unwrap_or(0);
        let src_snapshot_id = first
            .column_by_name(MV_SRC_SNAPSHOT_COL)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .map(|a| a.value(0))
            .filter(|v| *v >= 0);

        Ok(Some(Generation {
            version,
            src_snapshot_id,
            batches: gen_batches,
        }))
    }

    /// 从结果 batch 里去掉内部 `_mv_*` 列(对外结果不含版本元数据)。
    fn strip_internal_cols(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let Some(first) = batches.first() else {
            return Ok(Vec::new());
        };
        let keep: Vec<usize> = first
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !MV_INTERNAL_COLS.contains(&f.name().as_str()))
            .map(|(i, _)| i)
            .collect();

        batches
            .iter()
            .map(|b| {
                b.project(&keep)
                    .context("Failed to project out MV internal columns")
            })
            .collect()
    }

    /// 聚合刷新 (使用 DataFusion)
    async fn refresh_aggregate(
        &self,
        view: &MaterializedView,
        group_by: &[String],
        aggregations: &[Aggregation],
        filter: Option<&str>,
    ) -> Result<u64> {
        // 1. 读取源表数据到 DataFusion
        let ctx = SessionContext::new();
        let source_batches = self.read_source_table(&view.source_table).await?;

        if source_batches.is_empty() {
            tracing::warn!("Source table '{}' is empty", view.source_table);
            return Ok(0);
        }

        // 2. 注册为 DataFusion 内存表
        let schema = source_batches[0].schema();
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![source_batches])?;
        ctx.register_table("source", Arc::new(mem_table))?;

        // 3. 构造部分聚合 SQL(存储形态:AVG 拆 sum+count,与增量一致)
        let sql = Self::build_partial_sql(group_by, aggregations, filter, "source");
        tracing::debug!("Aggregate SQL: {}", sql);

        // 4. 执行查询
        let df = ctx
            .sql(&sql)
            .await
            .context("Failed to execute aggregate SQL")?;
        let result_batches = df.collect().await.context("Failed to collect results")?;

        let row_count: u64 = result_batches.iter().map(|b| b.num_rows() as u64).sum();

        tracing::info!(
            "Aggregate computed {} rows for view '{}'",
            row_count,
            view.name
        );

        // 5. 写入目标表:版本化写(Phase 0)。全量刷新写「新的一代」,
        //    src_snapshot_id = 源表当前 snapshot(全量已并入源表全部数据)。
        if !result_batches.is_empty() {
            let schema = result_batches[0].schema();
            let combined = arrow::compute::concat_batches(&schema, &result_batches)
                .context("Failed to concat result batches")?;

            let next_version = self.next_version(&view.target_table).await?;
            let src_snapshot = self.source_snapshot_id(&view.source_table).await?;

            self.event_log_store
                .write_versioned_batch(&view.target_table, combined, next_version, src_snapshot)
                .await
                .with_context(|| {
                    format!("Failed to write to target table '{}'", view.target_table)
                })?;

            tracing::info!(
                "View '{}' results written to target table '{}' (version {})",
                view.name,
                view.target_table,
                next_version
            );
        }

        Ok(row_count)
    }

    /// 目标表下一代版本号 = 现有最新代 + 1(无则从 1 开始)。
    async fn next_version(&self, target_table: &str) -> Result<i64> {
        Ok(self
            .latest_generation(target_table)
            .await?
            .map(|g| g.version + 1)
            .unwrap_or(1))
    }

    /// 源表当前 Iceberg snapshot id(增量水位);源表尚无 snapshot 时为 None。
    async fn source_snapshot_id(&self, source_table: &str) -> Result<Option<i64>> {
        let table = self.event_log_store.load_table(source_table).await?;
        Ok(table.metadata().current_snapshot_id())
    }

    /// SQL 刷新:加载源表 → 以真实表名注册进 DataFusion → 执行 SQL → 写目标表。
    ///
    /// 与 `refresh_aggregate` 一样绕开受阻的 iceberg-datafusion catalog:手动
    /// 用 Iceberg scan 把源表读成 RecordBatch,注册成 MemTable(表名 =
    /// `view.source_table`,这样用户 SQL 里的 `FROM <source_table>` 直接命中)。
    async fn refresh_sql(&self, view: &MaterializedView, sql: &str) -> Result<u64> {
        let ctx = SessionContext::new();
        let source_batches = self.read_source_table(&view.source_table).await?;

        if source_batches.is_empty() {
            tracing::warn!(
                "Source table '{}' is empty for SQL view '{}'",
                view.source_table,
                view.name
            );
            return Ok(0);
        }

        let schema = source_batches[0].schema();
        let mem_table = datafusion::datasource::MemTable::try_new(schema, vec![source_batches])?;
        // 注册成源表的真实名字,让用户 SQL 的 FROM 子句原样命中。
        ctx.register_table(view.source_table.as_str(), Arc::new(mem_table))?;

        let df = ctx
            .sql(sql)
            .await
            .with_context(|| format!("Failed to execute SQL for view '{}'", view.name))?;
        let result_batches = df
            .collect()
            .await
            .context("Failed to collect SQL results")?;

        let row_count: u64 = result_batches.iter().map(|b| b.num_rows() as u64).sum();

        if !result_batches.is_empty() {
            let out_schema = result_batches[0].schema();
            let combined = arrow::compute::concat_batches(&out_schema, &result_batches)
                .context("Failed to concat SQL result batches")?;
            let next_version = self.next_version(&view.target_table).await?;
            let src_snapshot = self.source_snapshot_id(&view.source_table).await?;
            self.event_log_store
                .write_versioned_batch(&view.target_table, combined, next_version, src_snapshot)
                .await
                .with_context(|| {
                    format!(
                        "Failed to write SQL view to target table '{}'",
                        view.target_table
                    )
                })?;
        }

        tracing::info!(
            "SQL view '{}' computed {} rows → target table '{}'",
            view.name,
            row_count,
            view.target_table
        );
        Ok(row_count)
    }

    /// 增量刷新(Push 模式)。
    ///
    /// Aggregate 视图走真增量([`incremental_aggregate`]);SQL 视图无法通用增量化,
    /// 退回全量刷新。`new_events` 参数保留兼容旧签名,但增量以 Iceberg snapshot
    /// 边界为准(见 `incremental_aggregate`),不依赖调用方传入的事件内容——这样
    /// 迟到/乱序事件只要已 append 进源表就能被正确并入。
    pub async fn incremental_refresh(
        &self,
        view: &MaterializedView,
        new_events: &[RawEvent],
    ) -> Result<u64> {
        tracing::debug!(
            "Incremental refresh for view '{}' ({} events hint)",
            view.name,
            new_events.len()
        );

        match &view.transform {
            ViewTransform::Aggregate {
                group_by,
                aggregations,
                filter,
            } => {
                self.incremental_aggregate(view, group_by, aggregations, filter.as_deref())
                    .await
            }
            // SQL 视图:无通用增量,保持全量。
            ViewTransform::Sql(_) => self.refresh(view).await,
        }
    }

    /// 真增量聚合:只读源表自上次刷新以来新增的 snapshot 文件,算部分聚合,与
    /// 目标表最新一代合并,写出新一代。
    async fn incremental_aggregate(
        &self,
        view: &MaterializedView,
        group_by: &[String],
        aggregations: &[Aggregation],
        filter: Option<&str>,
    ) -> Result<u64> {
        // 1. 源表当前 snapshot + 目标表最新一代(拿到上次水位)。
        let current = match self.source_snapshot_id(&view.source_table).await? {
            Some(s) => s,
            None => {
                tracing::warn!("Source '{}' has no snapshot yet", view.source_table);
                return Ok(0);
            }
        };
        let last_gen = self.latest_generation(&view.target_table).await?;
        let last_snapshot = last_gen.as_ref().and_then(|g| g.src_snapshot_id);

        // 2. 水位相同 ⇒ 无新事件,真 no-op(不写新版本)。
        if last_snapshot == Some(current) {
            tracing::debug!(
                "View '{}' already up-to-date (snapshot {})",
                view.name,
                current
            );
            return Ok(0);
        }

        // 3. 读增量文件;首次(无旧态)或 snapshot 过期 ⇒ 回退全量。
        let delta_batches = match self
            .event_log_store
            .read_snapshot_delta(&view.source_table, last_snapshot, current)
            .await
        {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    "Delta read failed for view '{}' ({}); falling back to full refresh",
                    view.name,
                    e
                );
                return self.refresh(view).await;
            }
        };
        if last_gen.is_none() {
            // 无旧态可合并 ⇒ 等价全量(且能建目标表 schema)。
            return self.refresh(view).await;
        }
        if delta_batches.iter().all(|b| b.num_rows() == 0) {
            tracing::debug!("View '{}': empty delta, no-op", view.name);
            return Ok(0);
        }

        let ctx = SessionContext::new();

        // 4. delta 部分聚合(存储形态)。
        let delta_schema = delta_batches[0].schema();
        let delta_mem =
            datafusion::datasource::MemTable::try_new(delta_schema, vec![delta_batches])?;
        ctx.register_table("delta_src", Arc::new(delta_mem))?;
        let partial_sql = Self::build_partial_sql(group_by, aggregations, filter, "delta_src");
        let delta_partial = ctx
            .sql(&partial_sql)
            .await
            .context("Failed to compute delta partial aggregate")?
            .collect()
            .await
            .context("Failed to collect delta partial")?;

        // 5. 旧态(存储形态,含 _mv_* 列则先剥掉,只留 group_by + 度量列)。
        // last_gen was already checked at line 450, but use safe extraction for clarity
        let last_gen_data = last_gen
            .ok_or_else(|| anyhow::anyhow!("last_gen unexpectedly None after check"))?;
        let old_state = Self::strip_internal_cols(last_gen_data.batches)?;

        if old_state.is_empty() || old_state[0].num_rows() == 0 {
            // 理论上有旧态就非空;保险起见回退全量。
            return self.refresh(view).await;
        }

        // 6. 合并:old_state UNION ALL delta_partial → 按 combine 再归约。
        let old_schema = old_state[0].schema();
        let old_mem = datafusion::datasource::MemTable::try_new(old_schema, vec![old_state])?;
        ctx.register_table("old_state", Arc::new(old_mem))?;
        let delta_partial_schema = delta_partial[0].schema();
        let dp_mem =
            datafusion::datasource::MemTable::try_new(delta_partial_schema, vec![delta_partial])?;
        ctx.register_table("delta_partial", Arc::new(dp_mem))?;

        let merge_sql = Self::build_merge_sql(group_by, aggregations);
        tracing::debug!("Merge SQL: {}", merge_sql);
        let merged = ctx
            .sql(&merge_sql)
            .await
            .context("Failed to merge incremental aggregate")?
            .collect()
            .await
            .context("Failed to collect merged aggregate")?;

        let row_count: u64 = merged.iter().map(|b| b.num_rows() as u64).sum();

        // 7. 写新一代(水位推进到 current)。
        if !merged.is_empty() {
            let out_schema = merged[0].schema();
            let combined = arrow::compute::concat_batches(&out_schema, &merged)
                .context("Failed to concat merged batches")?;
            let next_version = self.next_version(&view.target_table).await?;
            self.event_log_store
                .write_versioned_batch(&view.target_table, combined, next_version, Some(current))
                .await
                .with_context(|| {
                    format!(
                        "Failed to write incremental result to '{}'",
                        view.target_table
                    )
                })?;
            tracing::info!(
                "View '{}' incrementally updated to snapshot {} ({} groups, version {})",
                view.name,
                current,
                row_count,
                next_version
            );
        }

        Ok(row_count)
    }

    /// 读取源表数据
    async fn read_source_table(
        &self,
        table_name: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        use futures::TryStreamExt;

        let table = self
            .event_log_store
            .load_table(table_name)
            .await
            .with_context(|| format!("Failed to load source table '{}'", table_name))?;

        // 使用 Iceberg scan API 读取数据
        let scan = table.scan().build().context("Failed to build table scan")?;

        let stream = scan
            .to_arrow()
            .await
            .context("Failed to create arrow stream")?;

        let batches: Vec<_> = stream
            .try_collect()
            .await
            .context("Failed to collect batches")?;

        Ok(batches)
    }

    /// 把用户声明的聚合展开成存储层的可合并度量(AVG → sum + count 两列)。
    fn stored_measures(aggregations: &[Aggregation]) -> Vec<StoredMeasure> {
        // 无聚合时退化为一个全局 count(与旧行为一致)。
        if aggregations.is_empty() {
            return vec![StoredMeasure {
                col: "count".into(),
                partial_expr: "COUNT(*)".into(),
                merge_expr: "SUM(count)".into(),
            }];
        }
        let mut out = Vec::new();
        for agg in aggregations {
            match agg {
                Aggregation::Count { field, alias } => {
                    let p = if field == "*" {
                        "COUNT(*)".to_string()
                    } else {
                        format!("COUNT({})", field)
                    };
                    out.push(StoredMeasure {
                        col: alias.clone(),
                        partial_expr: p,
                        merge_expr: format!("SUM({})", alias),
                    });
                }
                Aggregation::Sum { field, alias } => out.push(StoredMeasure {
                    col: alias.clone(),
                    partial_expr: format!("SUM(CAST({} AS DOUBLE))", field),
                    merge_expr: format!("SUM({})", alias),
                }),
                Aggregation::Max { field, alias } => out.push(StoredMeasure {
                    col: alias.clone(),
                    partial_expr: format!("MAX(CAST({} AS DOUBLE))", field),
                    merge_expr: format!("MAX({})", alias),
                }),
                Aggregation::Min { field, alias } => out.push(StoredMeasure {
                    col: alias.clone(),
                    partial_expr: format!("MIN(CAST({} AS DOUBLE))", field),
                    merge_expr: format!("MIN({})", alias),
                }),
                Aggregation::Avg { field, alias } => {
                    out.push(StoredMeasure {
                        col: format!("{}__sum", alias),
                        partial_expr: format!("SUM(CAST({} AS DOUBLE))", field),
                        merge_expr: format!("SUM({}__sum)", alias),
                    });
                    out.push(StoredMeasure {
                        col: format!("{}__count", alias),
                        partial_expr: format!("COUNT({})", field),
                        merge_expr: format!("SUM({}__count)", alias),
                    });
                }
            }
        }
        out
    }

    /// 部分聚合 SQL(存储形态):`over` 是被查询的表名(`source` 或 delta 表名)。
    fn build_partial_sql(
        group_by: &[String],
        aggregations: &[Aggregation],
        filter: Option<&str>,
        over: &str,
    ) -> String {
        let mut select_parts: Vec<String> = group_by.to_vec();
        for m in Self::stored_measures(aggregations) {
            select_parts.push(format!("{} AS {}", m.partial_expr, m.col));
        }
        let mut sql = format!("SELECT {} FROM {}", select_parts.join(", "), over);
        if let Some(filter) = filter {
            sql.push_str(&format!(" WHERE {}", filter));
        }
        if !group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", group_by.join(", ")));
        }
        sql
    }

    /// 合并 SQL:对「旧态 UNION ALL delta 部分聚合」按 combine 算子再归约一次。
    /// 旧态与 delta 都是同构的存储形态(同列),UNION 后再聚合即得新态。
    fn build_merge_sql(group_by: &[String], aggregations: &[Aggregation]) -> String {
        let mut select_parts: Vec<String> = group_by.to_vec();
        for m in Self::stored_measures(aggregations) {
            select_parts.push(format!("{} AS {}", m.merge_expr, m.col));
        }
        let mut sql = format!(
            "SELECT {} FROM (SELECT * FROM old_state UNION ALL SELECT * FROM delta_partial)",
            select_parts.join(", ")
        );
        if !group_by.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", group_by.join(", ")));
        }
        sql
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::materialized_view::Aggregation;

    #[test]
    fn test_build_partial_sql_avg_splits_sum_count() {
        // AVG 存储为 sum + count 伴生列(可合并形态)。
        let sql = ViewRefresher::build_partial_sql(
            &["device_id".to_string()],
            &[Aggregation::Avg {
                field: "temperature".into(),
                alias: "avg_temp".into(),
            }],
            None,
            "source",
        );

        assert_eq!(
            sql,
            "SELECT device_id, SUM(CAST(temperature AS DOUBLE)) AS avg_temp__sum, \
             COUNT(temperature) AS avg_temp__count FROM source GROUP BY device_id"
        );
    }

    #[test]
    fn test_build_partial_sql_with_filter() {
        let sql = ViewRefresher::build_partial_sql(
            &["device_id".to_string()],
            &[Aggregation::Count {
                field: "*".into(),
                alias: "count".into(),
            }],
            Some("temperature > 20"),
            "source",
        );

        assert_eq!(
            sql,
            "SELECT device_id, COUNT(*) AS count FROM source WHERE temperature > 20 GROUP BY device_id"
        );
    }

    #[test]
    fn test_build_partial_sql_multiple_aggs() {
        let sql = ViewRefresher::build_partial_sql(
            &["region".to_string()],
            &[
                Aggregation::Sum {
                    field: "sales".into(),
                    alias: "total_sales".into(),
                },
                Aggregation::Max {
                    field: "price".into(),
                    alias: "max_price".into(),
                },
            ],
            None,
            "source",
        );

        assert!(sql.contains("SUM(CAST(sales AS DOUBLE)) AS total_sales"));
        assert!(sql.contains("MAX(CAST(price AS DOUBLE)) AS max_price"));
        assert!(sql.contains("GROUP BY region"));
    }

    #[test]
    fn test_build_partial_sql_no_groupby() {
        let sql = ViewRefresher::build_partial_sql(
            &[],
            &[Aggregation::Count {
                field: "*".into(),
                alias: "total".into(),
            }],
            None,
            "source",
        );

        assert_eq!(sql, "SELECT COUNT(*) AS total FROM source");
        assert!(!sql.contains("GROUP BY"));
    }

    #[test]
    fn test_build_merge_sql_combine_ops() {
        // 合并阶段:COUNT→SUM、MAX→MAX、AVG 伴生列各自 SUM。
        let sql = ViewRefresher::build_merge_sql(
            &["region".to_string()],
            &[
                Aggregation::Count {
                    field: "*".into(),
                    alias: "c".into(),
                },
                Aggregation::Avg {
                    field: "v".into(),
                    alias: "av".into(),
                },
            ],
        );
        assert!(sql.contains("SUM(c) AS c"), "COUNT combine 应为 SUM: {sql}");
        assert!(sql.contains("SUM(av__sum) AS av__sum"), "{sql}");
        assert!(sql.contains("SUM(av__count) AS av__count"), "{sql}");
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("GROUP BY region"));
    }
}

#[cfg(test)]
mod mv_from_domain_tests {
    use crate::materialized_view::{MaterializedView, RefreshMode, ViewTransform};
    use nexora_core::domain_package::DomainMV;

    fn dmv(name: &str, query: &str, mode: &str) -> DomainMV {
        DomainMV {
            name: name.into(),
            query: query.into(),
            refresh_mode: mode.into(),
            schema: vec![],
        }
    }

    #[test]
    fn parses_source_table_from_from_clause() {
        let mv = MaterializedView::from_domain_mv(&dmv(
            "orders_by_region",
            "SELECT region, COUNT(*) AS c FROM orders GROUP BY region",
            "pull:30",
        ))
        .unwrap();
        assert_eq!(mv.source_table, "orders");
        assert_eq!(mv.target_table, "orders_by_region_mv");
        assert!(matches!(
            mv.refresh_mode,
            RefreshMode::Pull { interval_secs: 30 }
        ));
        assert!(matches!(mv.transform, ViewTransform::Sql(_)));
    }

    #[test]
    fn strips_schema_qualifier_and_quotes() {
        let mv = MaterializedView::from_domain_mv(&dmv(
            "v",
            "SELECT * FROM events.\"orders\" WHERE amount > 0",
            "",
        ))
        .unwrap();
        assert_eq!(mv.source_table, "orders");
        // 空 refresh_mode → Pull 默认 60s
        assert!(matches!(
            mv.refresh_mode,
            RefreshMode::Pull { interval_secs: 60 }
        ));
    }

    #[test]
    fn push_mode_recognized() {
        let mv = MaterializedView::from_domain_mv(&dmv("v", "SELECT x FROM t", "push")).unwrap();
        assert!(matches!(mv.refresh_mode, RefreshMode::Push));
    }

    #[test]
    fn missing_from_is_error() {
        let err = MaterializedView::from_domain_mv(&dmv("v", "SELECT 1", "pull"));
        assert!(err.is_err(), "query without FROM must be rejected");
    }
}
