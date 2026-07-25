//! Distributed Cypher read planner (work-line B).
//!
//! Multi-node Cypher would otherwise return 501 (see T1.6): the executor
//! snapshots one node's local shards, so a whole-graph query on a real cluster
//! would silently return partial results. This module analyzes a query's AST and
//! builds a [`DistributedPlan`] describing how per-owner partial results merge
//! into the correct whole-graph answer. Supported so far:
//!
//! - **Concatenable scans** — `MATCH (n[:Label]) RETURN <projections>` with no
//!   aggregation: each owner scans its shards, rows concatenate.
//! - **Global aggregates** — `count`/`sum`/`avg`/`min`/`max` with no grouping:
//!   each owner returns partial aggregates, the coordinator combines them.
//! - **Grouped aggregates** — `RETURN <keys>, <aggregates>` (GROUP BY the
//!   non-aggregate keys): partials keyed by group, combined per key.
//! - **Global ORDER BY / SKIP / LIMIT / DISTINCT** — applied by the coordinator
//!   over the merged rows (owners may still be sent the raw query).
//!
//! Anything the analysis can't prove mergeable (relationship traversal, writes,
//! UNION, WITH pipelines, subqueries, unsupported functions) yields `None` from
//! [`plan`], and the caller keeps the honest 501. Correctness over coverage:
//! when unsure, refuse rather than risk a wrong answer.

use crate::failover::ReadConcern;
use crate::replication_progress::{ReplicationProgress, SessionReadTracker};
use crate::router::HybridRouter;
use crate::{GraphOperation, GraphResult, RouterError};
use nexora_language::ast::{AggFunction, Clause, Expression, OrderBy};
use std::collections::BTreeSet;

mod join;
mod merge;
mod path;
#[cfg(test)]
mod tests;

pub use merge::merge_rows;

/// The 8 write-stat columns, in a fixed order, used to ship a `WriteResult` back
/// from an owner as a structured `CypherRows` (so the distributed write path can
/// SUM stats across owners). The adapter emits a single integer row in this
/// column order; [`WritePlan`] merges by summing column-wise.
pub const WRITE_STAT_COLUMNS: [&str; 8] = [
    "nodes_created",
    "nodes_deleted",
    "relationships_created",
    "relationships_deleted",
    "properties_set",
    "properties_removed",
    "labels_added",
    "labels_removed",
];

/// A single RETURN item, classified for distributed merge.
#[derive(Clone, Debug, PartialEq)]
pub enum ProjItem {
    /// A non-aggregate projection (grouping key when aggregates are present):
    /// `expr` is the raw expression text sent to owners (e.g. `n.city`); `column`
    /// is the output column name (the alias when `... AS x`, else the expr text).
    Grouping { expr: String, column: String },
    /// An aggregate over a column: `count(*)`, `sum(n.age)`, etc.
    Aggregate {
        func: AggFunction,
        column: String,
        distinct: bool,
    },
}

/// How the coordinator merges per-owner partial results.
#[derive(Clone, Debug, PartialEq)]
pub enum MergeKind {
    /// Concatenate rows (pure scan/projection, no aggregation).
    Concat,
    /// Global aggregation with no grouping — one output row combining partials.
    GlobalAggregate { items: Vec<ProjItem> },
    /// Grouped aggregation — partials combined per group key.
    GroupedAggregate { items: Vec<ProjItem> },
}

/// A single-hop directed relationship join: `MATCH (a:La)-[:REL]->(b:Lb)`.
///
/// The source `a` and its edge live on `a`'s owner; the target `b` may live on a
/// different owner. The coordinator scans sources per owner, fetches each
/// source's REL edges (from the source's owner), then fetches each target's
/// properties (from the target's owner) — a genuine cross-partition join that a
/// per-owner local run cannot do (the local executor drops cross-shard edge
/// targets). See [`join`].
#[derive(Clone, Debug)]
pub struct RelJoinSpec {
    pub source_var: String,
    pub source_labels: Vec<String>,
    pub edge_type: String,
    /// `true` = outgoing `-[:REL]->`, `false` = incoming `<-[:REL]-`.
    pub outgoing: bool,
    pub target_var: String,
    pub target_labels: Vec<String>,
    /// RETURN projections as `(binding_var, property_or_id, output_column)`.
    /// `property` is `None` for a bare node reference (projects its id).
    pub projections: Vec<RelProjection>,
}

/// One RETURN projection over a join binding.
#[derive(Clone, Debug, PartialEq)]
pub struct RelProjection {
    /// Which binding: the source or target variable.
    pub var: String,
    /// Property name, or `None` to project the node's id (bare `a` / `id(a)`).
    pub property: Option<String>,
    /// Output column name.
    pub column: String,
}

/// A multi-hop / variable-length cross-partition path join, e.g.
/// `MATCH (a)-[:R]->(b)-[:R]->(c)` or `MATCH (a)-[:R*1..3]->(b)`.
///
/// Generalizes [`RelJoinSpec`] to a chain of hops. The coordinator scans the
/// path's source nodes, then expands hop by hop across partitions (each hop
/// routed to the current node's owner for its edges), tracking bindings for the
/// named nodes; variable-length hops do a bounded BFS between endpoints. See
/// [`path`].
#[derive(Clone, Debug)]
pub struct PathJoinSpec {
    /// Named nodes along the path; `nodes[0]` is the source. `var` is `None` for
    /// an anonymous node (still traversed, just not projectable).
    pub nodes: Vec<PathNode>,
    /// One hop between each consecutive node pair (`hops.len() == nodes.len()-1`).
    pub hops: Vec<PathHop>,
    /// RETURN projections over the named nodes.
    pub projections: Vec<RelProjection>,
    /// Output column names in order.
    pub columns: Vec<String>,
}

/// A named (or anonymous) node position in a path.
#[derive(Clone, Debug)]
pub struct PathNode {
    pub var: Option<String>,
    pub labels: Vec<String>,
}

/// Direction of a hop's edges to follow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HopDir {
    /// `-[:R]->` — follow outgoing edges only.
    Outgoing,
    /// `<-[:R]-` — follow incoming edges only.
    Incoming,
    /// `-[:R]-` — undirected: follow edges in either direction.
    Either,
}

/// One hop in a path: an edge (optionally typed / directed / variable-length).
#[derive(Clone, Debug)]
pub struct PathHop {
    /// Edge type to follow, or `None` for any type (untyped `-[]->`).
    pub edge_type: Option<String>,
    /// Which directions to follow.
    pub dir: HopDir,
    /// Minimum hop count (1 for a plain edge; the `*min..max` lower bound).
    pub min_hops: u32,
    /// Maximum hop count (1 for a plain edge; the `*min..max` upper bound).
    pub max_hops: u32,
}

impl PathHop {
    /// A single fixed edge (not variable-length).
    fn is_fixed_single(&self) -> bool {
        self.min_hops == 1 && self.max_hops == 1
    }
}

/// A plan for distributed execution: what to run per owner, and how to merge.
#[derive(Clone, Debug)]
pub struct DistributedPlan {
    /// The merge strategy over per-owner partials.
    pub kind: MergeKind,
    /// Output column names, in order (drives the merged result header).
    pub columns: Vec<String>,
    /// The (possibly rewritten) query each owner runs against its local shards.
    pub owner_query: String,
    /// Global DISTINCT applied by the coordinator after the merge.
    pub distinct: bool,
    /// Global ORDER BY applied by the coordinator (column index + ascending).
    pub order_by: Vec<(usize, bool)>,
    /// Global SKIP applied by the coordinator after ordering.
    pub skip: Option<usize>,
    /// Global LIMIT applied by the coordinator after skip.
    pub limit: Option<usize>,
    /// When present, this is a single-hop cross-partition relationship join;
    /// [`execute`] dispatches to the join path and ignores `owner_query`.
    pub rel_join: Option<RelJoinSpec>,
    /// When present, this is a multi-hop / variable-length path join;
    /// [`execute`] dispatches to the path-join path and ignores `owner_query`.
    pub path_join: Option<PathJoinSpec>,
    /// When present, aggregate / group / order / limit is applied *locally* to
    /// the join's raw output rows (aggregation over a relationship join). The
    /// join's `projections` then hold the raw columns to extract, and this spec
    /// describes how to fold them into the final result. `None` = the join's
    /// projections are the output directly (no aggregation).
    pub join_post: Option<JoinPost>,
    /// When present, this plan produces the *first stage* of a WITH pipeline;
    /// after the stage-1 result is computed, [`execute`] applies this coordinator
    /// stage (WITH WHERE filter → projection → order/skip/limit/distinct) locally.
    /// This is the first coordinator stage; [`with_stage_chain`] holds any further
    /// stages (for multi-stage `WITH … WITH … RETURN` chains). `None` = no pipeline.
    pub with_stage: Option<WithStage>,
    /// Additional coordinator stages applied *after* [`with_stage`], in order, for
    /// multi-stage WITH chains (`MATCH → WITH → WITH → … → RETURN`). Each stage
    /// post-processes the previous stage's materialized rows the same way
    /// [`with_stage`] does. Empty for a two-stage pipeline (single WITH).
    pub with_stage_chain: Vec<WithStage>,
    /// When present, this is a UNION of sub-plans; [`execute`] runs each branch
    /// distributively and concatenates (`union_all`) or concatenates+dedups
    /// (`!union_all`). All other fields are unused for a union plan.
    pub union: Option<UnionPlan>,
    /// When present, this is a distributed write (MATCH-based SET/REMOVE/DELETE):
    /// [`execute_write`] fans the write out to every owner (each mutates only its
    /// own matched nodes) and sums the per-owner write stats.
    pub write: Option<WritePlan>,
    /// When present, this is a distributed CREATE: coordinator pre-generates qids,
    /// groups nodes by target owner (based on qid.shard_key()), and dispatches
    /// owner-specific CREATE sub-queries. Each owner creates only nodes that belong
    /// to its shards, ensuring correct data placement.
    pub create: Option<CreatePlan>,
    /// When present, this is a distributed MERGE: two-phase (MATCH across all owners
    /// to find existing nodes, then CREATE on the coordinator-chosen owner if not
    /// found). Ensures no duplicate creation across owners.
    pub merge: Option<MergePlan>,
}

/// A distributed write plan: an owner-parallel `MATCH (node) SET|REMOVE|DELETE`.
/// Each owner runs the same write against its local shards — because the write
/// touches only nodes the MATCH binds, and every node lives on exactly one
/// owner, the owners partition the work with no overlap and no cross-owner data
/// movement. The coordinator sums the per-owner [`WriteResult`] stats.
#[derive(Clone, Debug)]
pub struct WritePlan {
    /// The write query each owner runs against its local shards (verbatim).
    pub owner_query: String,
}

/// A distributed UNION: each branch is its own [`DistributedPlan`], executed
/// independently, then combined. `union_all` = keep duplicates (concat);
/// otherwise dedup whole rows. Column headers come from the first branch.
#[derive(Clone, Debug)]
pub struct UnionPlan {
    pub branches: Vec<DistributedPlan>,
    pub union_all: bool,
    /// Output column names (first branch's columns).
    pub columns: Vec<String>,
}

/// A distributed CREATE plan: coordinator pre-generates node qids, groups them
/// by target owner (based on `qid.shard_key() % total_shards`), and dispatches
/// owner-specific CREATE operations. Each owner creates only nodes that belong
/// to its shards, ensuring nodes land on the correct physical owner.
#[derive(Clone, Debug)]
pub struct CreatePlan {
    /// All nodes to create (ungrouped at planning time; grouped by owner at execution).
    pub nodes: Vec<CreateOp>,
}

/// A distributed MERGE plan: two-phase operation to avoid duplicate creation.
/// Phase 1: scatter-gather MATCH across all owners to find existing nodes.
/// Phase 2: if not found, CREATE on a coordinator-chosen owner (based on qid).
#[derive(Clone, Debug)]
pub struct MergePlan {
    /// Nodes to create if MATCH finds nothing (pre-generated qids).
    pub nodes: Vec<CreateOp>,
    /// ON CREATE SET clauses (applied after creation if node didn't exist).
    pub on_create: Vec<String>,
    /// ON MATCH SET clauses (applied to found nodes if they existed).
    pub on_match: Vec<String>,
}

/// A single node creation operation with pre-generated qid.
#[derive(Clone, Debug)]
pub struct CreateOp {
    /// Pre-generated qid (coordinator assigns based on shard routing).
    pub qid: nexora_id::NexoraId,
    /// Variable name to bind in the CREATE pattern (if named).
    pub variable: Option<String>,
    /// Labels to apply to the node.
    pub labels: Vec<String>,
    /// Properties to set on the node (key → JSON value).
    pub properties: std::collections::HashMap<String, serde_json::Value>,
}

/// The second stage of a `MATCH … WITH … RETURN` pipeline, applied locally by
/// the coordinator to the stage-1 result rows (whose columns are named by the
/// WITH items). This slice supports: an optional WHERE filter over the WITH
/// columns, a final RETURN projection selecting/reordering those columns, and
/// the RETURN's order/skip/limit/distinct.
#[derive(Clone, Debug)]
pub struct WithStage {
    /// Column names produced by stage 1 (the WITH items, by alias/expr text).
    pub stage1_columns: Vec<String>,
    /// Optional WHERE filter over the stage-1 columns (post-aggregation HAVING).
    pub filter: Option<RowFilter>,
    /// Final RETURN projection: for each output column, the stage-1 column index
    /// to copy. (This slice projects existing WITH columns; no new expressions.)
    pub projection: Vec<usize>,
    /// Final output column names, in RETURN order.
    pub columns: Vec<String>,
    /// RETURN-level order-by over the *output* columns (index + ascending).
    pub order_by: Vec<(usize, bool)>,
    pub skip: Option<usize>,
    pub limit: Option<usize>,
    pub distinct: bool,
}

/// A single-column comparison filter over a stage-1 row: `col <op> literal`.
/// (WITH-stage WHERE in this slice is one comparison against a literal; richer
/// predicates stay on the honest 501.)
#[derive(Clone, Debug)]
pub struct RowFilter {
    /// Stage-1 column index to test.
    pub col: usize,
    pub op: nexora_language::ast::BinaryOp,
    pub literal: serde_json::Value,
}

/// Local post-processing over a relationship/path join's raw rows: grouping,
/// aggregation, then coordinator-side order / skip / limit / distinct. The raw
/// rows are the join's extracted columns (one per distinct projected/grouped/
/// aggregated property); `items` reference them by raw column index.
#[derive(Clone, Debug)]
pub struct JoinPost {
    /// Output items in RETURN order (grouping keys + aggregates).
    pub items: Vec<JoinPostItem>,
    /// Output column names in order.
    pub columns: Vec<String>,
    pub order_by: Vec<(usize, bool)>,
    pub skip: Option<usize>,
    pub limit: Option<usize>,
    pub distinct: bool,
}

/// One RETURN item over join output: a grouping key or an aggregate, referencing
/// the join's raw extracted columns by index.
#[derive(Clone, Debug, PartialEq)]
pub enum JoinPostItem {
    /// A grouping key = the raw column at `raw_idx`.
    Grouping { raw_idx: usize },
    /// An aggregate; `raw_idx` is `None` for `count(*)`, else the raw column.
    Aggregate {
        func: AggFunction,
        raw_idx: Option<usize>,
        distinct: bool,
    },
}

/// Analyze `query` and, if it's provably mergeable, return a [`DistributedPlan`].
/// `None` means "not in the supported subset — caller must refuse (501)".
///
/// Conservative: any construct the analysis doesn't recognize (writes, UNION,
/// WITH pipelines, relationship patterns, subqueries, unsupported expressions in
/// grouping/order keys) yields `None`.
pub fn plan(query: &str) -> Option<DistributedPlan> {
    let parsed = nexora_language::Parser::parse(query).ok()?;
    // Try a distributed write plan first (MATCH-based SET/REMOVE/DELETE); if the
    // shape isn't a supported owner-parallel write, fall through to read planning.
    if let Some(w) = plan_write(&parsed.clauses, query) {
        return Some(w);
    }
    plan_clauses(&parsed.clauses)
}

/// Try to build a distributed write plan. Supported shapes:
/// 1. Single CREATE clause with node-only pattern (no relationships).
/// 2. Single MERGE clause with node-only pattern (two-phase match+create).
/// 3. MATCH (node-scan, no WHERE) followed by SET/REMOVE/DELETE clauses.
///
/// For CREATE: coordinator pre-generates qids, groups by owner, dispatches.
/// For MERGE: scatter-gather MATCH across owners, then CREATE if not found.
/// For MATCH-based writes: owner-parallel mutation (each owner mutates only its
/// own matched nodes), coordinator sums stats.
///
/// Returns `None` (→ read planning / honest 501) for unsupported shapes:
/// relationship-pattern CREATE/MERGE, relationship writes, cross-shard atomic
/// writes, WITH+write, etc.
fn plan_write(clauses: &[Clause], query: &str) -> Option<DistributedPlan> {
    // Single CREATE clause: coordinator-generated qids + routing.
    if clauses.len() == 1 {
        if let Clause::Create { pattern } = &clauses[0] {
            if pattern_is_node_only(pattern) {
                return plan_create(pattern);
            }
            // Relationship-pattern CREATE not in this slice (needs two-phase).
            return None;
        }
        // Single MERGE clause: two-phase match+create.
        if let Clause::Merge {
            pattern,
            on_create,
            on_match,
        } = &clauses[0]
        {
            if pattern_is_node_only(pattern) {
                return plan_merge(pattern, on_create, on_match);
            }
            // Relationship-pattern MERGE not in this slice.
            return None;
        }
    }

    // MATCH-based write: SET/REMOVE/DELETE on matched nodes, fanned to every owner
    // (each mutates only its own matched nodes). A WHERE predicate is allowed ONLY
    // for the strict single-node-variable shape: the single-node write executor's
    // bulk MATCH-mutate path (try_bulk_match_mutate) evaluates the predicate and
    // applies to every matching node, so an owner holding none is a correct no-op.
    // A multi-part / multi-variable pattern would fall through that path to the
    // generic loop (binds one node per variable → silent under-apply), so it's
    // refused when a predicate is present.
    let first = clauses.first()?;
    let (pattern, predicate_present) = match first {
        Clause::Match {
            optional: false,
            pattern,
            predicate,
        } => (pattern, predicate.is_some()),
        _ => return None,
    };
    if !pattern_is_node_only(pattern) {
        return None; // relationship-pattern writes not in this slice
    }
    if predicate_present && !pattern_is_single_node_with_var(pattern) {
        // A WHERE over a multi-node pattern isn't safely owner-parallel here.
        return None;
    }
    // Remaining clauses must all be write clauses of the owner-parallel kind.
    // (SET / REMOVE / DELETE mutate only matched nodes; CREATE/MERGE do not.)
    let rest = &clauses[1..];
    if rest.is_empty() {
        return None; // a bare MATCH is a read, not a write
    }
    let mut is_write = false;
    for c in rest {
        match c {
            Clause::Set { .. } | Clause::Remove { .. } | Clause::Delete { .. } => {
                is_write = true;
            }
            // CREATE/MERGE would assign new ids on the running owner → misplace nodes.
            // Not in this slice (CREATE handled above as a standalone clause).
            _ => return None,
        }
    }
    if !is_write {
        return None;
    }

    Some(DistributedPlan {
        kind: MergeKind::Concat,
        columns: Vec::new(),
        owner_query: String::new(),
        distinct: false,
        order_by: Vec::new(),
        skip: None,
        limit: None,
        rel_join: None,
        path_join: None,
        join_post: None,
        with_stage: None,
        with_stage_chain: Vec::new(),
        union: None,
        write: Some(WritePlan {
            owner_query: query.to_string(),
        }),
        create: None,
        merge: None,
    })
}

/// Plan a distributed CREATE: parse node-only pattern, pre-generate qids on the
/// coordinator, group nodes by their target owner (based on `qid.shard_key()`),
/// and return a [`CreatePlan`]. Each owner will create only nodes that belong to
/// its shards, ensuring correct physical placement.
///
/// Returns `None` for unsupported shapes (relationship-pattern CREATE, properties
/// with complex expressions, etc.).
fn plan_create(pattern: &nexora_language::ast::Pattern) -> Option<DistributedPlan> {
    use std::collections::HashMap;

    // Extract nodes from the pattern. A node-only pattern has one part with one
    // segment (the node itself, no trailing edge).
    let mut ops = Vec::new();
    for part in &pattern.parts {
        if part.chain.segments.len() != 1 {
            return None; // multi-segment = edges, not supported
        }
        let seg = &part.chain.segments[0];
        if seg.edge.is_some() {
            return None; // edge present, refuse
        }

        let node = &seg.node;

        // Extract properties: only literal values are supported (no expressions).
        // A `__qid` literal pins the node's id (and thus its owner) — the SQL
        // layer emits it for `INSERT INTO t (id, ...)`, and the single-node write
        // executor honours it the same way. Without this the coordinator would
        // place the node on a random owner under a random id, so a later lookup
        // by the caller's id would miss. Fall back to a random qid when absent.
        let mut properties = HashMap::new();
        let mut pinned_qid = None;
        for (key, expr) in &node.properties {
            let value = match expr {
                nexora_language::ast::Expression::Literal(pv) => property_value_to_json(pv),
                _ => return None, // complex expression not supported
            };
            if key == "__qid" {
                // Match the single-node executor: hex first, else raw bytes.
                if let Some(s) = value.as_str() {
                    pinned_qid = Some(nexora_id::NexoraId::from_hex(s).unwrap_or_else(|_| {
                        nexora_id::NexoraId::from_bytes(s.as_bytes().to_vec())
                    }));
                }
                // Don't store __qid as a property; build_create_subquery re-adds it.
                continue;
            }
            properties.insert(key.clone(), value);
        }
        let qid = pinned_qid.unwrap_or_else(nexora_id::NexoraId::new_random);

        ops.push(CreateOp {
            qid,
            variable: node.variable.clone(),
            labels: node.labels.clone(),
            properties,
        });
    }

    if ops.is_empty() {
        return None; // empty CREATE pattern
    }

    // Store all ops; grouping by owner happens at execution time when the
    // shard map is available (planning is pure, no runtime dependencies).
    Some(DistributedPlan {
        kind: MergeKind::Concat,
        columns: Vec::new(),
        owner_query: String::new(),
        distinct: false,
        order_by: Vec::new(),
        skip: None,
        limit: None,
        rel_join: None,
        path_join: None,
        join_post: None,
        with_stage: None,
        with_stage_chain: Vec::new(),
        union: None,
        write: None,
        create: Some(CreatePlan { nodes: ops }),
        merge: None,
    })
}

/// Plan a distributed MERGE: two-phase operation to avoid duplicate creation
/// across owners. Phase 1 scatter-gathers a MATCH across all owners; if any
/// owner finds a matching node, apply ON MATCH SET clauses to it. If no owner
/// finds a match, phase 2 creates the node on a coordinator-chosen owner (based
/// on pre-generated qid) and applies ON CREATE SET clauses.
///
/// Returns `None` for unsupported shapes (relationship-pattern MERGE, complex
/// property expressions, etc.).
fn plan_merge(
    pattern: &nexora_language::ast::Pattern,
    on_create: &[nexora_language::ast::SetItem],
    on_match: &[nexora_language::ast::SetItem],
) -> Option<DistributedPlan> {
    use std::collections::HashMap;

    // Extract nodes from the pattern (same logic as plan_create).
    let mut ops = Vec::new();
    for part in &pattern.parts {
        if part.chain.segments.len() != 1 {
            return None; // multi-segment = edges, not supported
        }
        let seg = &part.chain.segments[0];
        if seg.edge.is_some() {
            return None; // edge present, refuse
        }

        let node = &seg.node;
        // Pre-generate a random qid for this node (used if CREATE branch is taken).
        let qid = nexora_id::NexoraId::new_random();

        // Extract properties: only literal values are supported (no expressions).
        let mut properties = HashMap::new();
        for (key, expr) in &node.properties {
            let value = match expr {
                nexora_language::ast::Expression::Literal(pv) => property_value_to_json(pv),
                _ => return None, // complex expression not supported
            };
            properties.insert(key.clone(), value);
        }

        ops.push(CreateOp {
            qid,
            variable: node.variable.clone(),
            labels: node.labels.clone(),
            properties,
        });
    }

    if ops.is_empty() {
        return None; // empty MERGE pattern
    }

    // Convert ON CREATE / ON MATCH SetItems to query fragments (for now, store
    // them as strings; execution will apply them after MATCH/CREATE).
    let on_create_strs: Vec<String> = on_create.iter().map(|item| item.to_string()).collect();
    let on_match_strs: Vec<String> = on_match.iter().map(|item| item.to_string()).collect();

    Some(DistributedPlan {
        kind: MergeKind::Concat,
        columns: Vec::new(),
        owner_query: String::new(),
        distinct: false,
        order_by: Vec::new(),
        skip: None,
        limit: None,
        rel_join: None,
        path_join: None,
        join_post: None,
        with_stage: None,
        with_stage_chain: Vec::new(),
        union: None,
        write: None,
        create: None,
        merge: Some(MergePlan {
            nodes: ops,
            on_create: on_create_strs,
            on_match: on_match_strs,
        }),
    })
}

/// Plan a clause list (a whole query, or one UNION branch). Recurses for UNION.
fn plan_clauses(clauses: &[Clause]) -> Option<DistributedPlan> {
    // UNION: a single Union clause holding each branch as its own clause list.
    if clauses.len() == 1 {
        if let Clause::Union { all, queries } = &clauses[0] {
            return plan_union(*all, queries);
        }
    }

    // Shape gate: one MATCH (read, non-optional) then either one RETURN, or a
    // WITH pipeline (MATCH → WITH … WITH → RETURN). The pipeline supports any
    // number of intermediate WITH stages: stage 1 (MATCH + first WITH) runs
    // distributively, each further WITH plus the final RETURN is a coordinator-
    // side stage applied in order.
    // (UNWIND, CALL, LOAD CSV, writes → not this slice.)
    if clauses.len() >= 3 {
        // MATCH, one-or-more WITH, then RETURN.
        if matches!(clauses[0], Clause::Match { .. })
            && matches!(clauses.last(), Some(Clause::Return { .. }))
            && clauses[1..clauses.len() - 1]
                .iter()
                .all(|c| matches!(c, Clause::With { .. }))
        {
            return plan_with_pipeline(clauses);
        }
        return None;
    }
    if clauses.len() != 2 {
        return None;
    }
    let (pattern, predicate_present) = match &clauses[0] {
        Clause::Match {
            optional: false,
            pattern,
            predicate,
        } => (pattern, predicate.is_some()),
        _ => return None,
    };
    if !pattern_is_node_only(pattern) {
        // Relationship pattern: try to build a single-hop cross-partition join
        // plan; if the shape isn't the supported one-hop form, refuse.
        return plan_relationship_join(pattern, &clauses[1], predicate_present);
    }

    let (items, order_by, skip, limit, distinct) = match &clauses[1] {
        Clause::Return {
            items,
            order_by,
            skip,
            limit,
            distinct,
        } => (items, order_by, skip, limit, *distinct),
        _ => return None,
    };

    plan_node_scan_stage(
        &clauses[0],
        items,
        order_by.as_ref(),
        skip.as_ref(),
        limit.as_ref(),
        distinct,
    )
}

/// Plan a `UNION [ALL]` of sub-queries. Each branch is planned independently via
/// [`plan_clauses`]; if any branch isn't distributable, the whole union refuses
/// (→ honest 501). Branch column headers must all match. `all` = `UNION ALL`
/// (keep duplicates); otherwise the coordinator dedups whole rows after concat.
fn plan_union(all: bool, queries: &[Vec<Clause>]) -> Option<DistributedPlan> {
    if queries.len() < 2 {
        return None;
    }
    let mut branches = Vec::with_capacity(queries.len());
    for branch_clauses in queries {
        // A branch that is itself a UNION would nest; `plan_clauses` handles it.
        branches.push(plan_clauses(branch_clauses)?);
    }
    // Branches must be union-compatible: same column *arity*. Cypher takes the
    // output header from the first branch (column names needn't match across
    // branches — `RETURN n.name` UNION `RETURN m.name` is legal, output is
    // `n.name`). Mismatched arity → refuse.
    let columns = branches[0].columns.clone();
    for b in &branches[1..] {
        if b.columns.len() != columns.len() {
            return None;
        }
    }

    Some(DistributedPlan {
        kind: MergeKind::Concat,
        columns: columns.clone(),
        owner_query: String::new(),
        distinct: false,
        order_by: Vec::new(),
        skip: None,
        limit: None,
        rel_join: None,
        path_join: None,
        join_post: None,
        with_stage: None,
        with_stage_chain: Vec::new(),
        union: Some(UnionPlan {
            branches,
            union_all: all,
            columns,
        }),
        write: None,
        create: None,
        merge: None,
    })
}

/// Build a distributed plan for a node-scan stage: `MATCH (n[:L]) RETURN|WITH
/// <items> [ORDER BY .. SKIP .. LIMIT .. DISTINCT]`. Shared by the top-level
/// `RETURN` path and the first stage of a WITH pipeline (which passes the WITH
/// clause's items/clauses here). Classifies items into scan / global-aggregate /
/// grouped-aggregate and sets up owner-side rewrite + coordinator merge.
#[allow(clippy::too_many_arguments)]
fn plan_node_scan_stage(
    match_clause: &Clause,
    items: &[nexora_language::ast::ReturnItem],
    order_by: Option<&OrderBy>,
    skip: Option<&Expression>,
    limit: Option<&Expression>,
    distinct: bool,
) -> Option<DistributedPlan> {
    // A pure scan (no aggregate anywhere in the projection) is concatenable
    // regardless of what each item projects: every owner evaluates the RETURN
    // per-row against its own shards, and the coordinator just concatenates the
    // rows (then applies global ORDER/SKIP/LIMIT/DISTINCT). So arbitrary scalar
    // projections — `id(n)`, `n.name`, aliases, functions like `toLower(n.x)` —
    // are all safe here; only aggregates need the strict grouping/merge analysis
    // below (they combine partials across owners). This distinguishes a filtered
    // projection read (`SELECT id, name ... WHERE age > 30`) — now distributable
    // — from a grouped aggregate, which still needs classify_expr.
    let any_aggregate = items.iter().any(|it| contains_aggregate(&it.expression));

    let mut proj_items = Vec::with_capacity(items.len());
    let mut columns = Vec::with_capacity(items.len());
    let mut has_aggregate = false;
    let mut has_grouping = false;
    if any_aggregate {
        // Classify each item as a grouping projection or an aggregate.
        for item in items {
            let column = return_item_column(item);
            columns.push(column.clone());
            match classify_expr(&item.expression)? {
                ItemKind::Grouping => {
                    has_grouping = true;
                    // Owners group by the raw expression text (e.g. `n.city`), not
                    // the output alias — the alias isn't a bound variable on owners.
                    proj_items.push(ProjItem::Grouping {
                        expr: item.expression.to_string(),
                        column,
                    });
                }
                ItemKind::Aggregate {
                    func,
                    col,
                    distinct: agg_distinct,
                } => {
                    has_aggregate = true;
                    proj_items.push(ProjItem::Aggregate {
                        func,
                        column: col,
                        distinct: agg_distinct,
                    });
                }
            }
        }
    } else {
        // Pure scan: columns are the output names; no per-item merge metadata.
        // Every projection must be one the owners can actually evaluate (column
        // refs, property access, functions, literals) — arithmetic and other
        // operator expressions aren't parsed by the owner in a RETURN, so refuse
        // the whole plan rather than dispatch a query that errors on each owner.
        for item in items {
            if !is_distributable_scan_projection(&item.expression) {
                return None;
            }
            columns.push(return_item_column(item));
        }
    }

    let kind = if has_aggregate && has_grouping {
        MergeKind::GroupedAggregate {
            items: proj_items.clone(),
        }
    } else if has_aggregate {
        MergeKind::GlobalAggregate {
            items: proj_items.clone(),
        }
    } else {
        MergeKind::Concat
    };

    // Global order-by: resolve each sort expression to an output column index.
    // Only column references that appear in the items are orderable in this
    // slice (the coordinator sorts the merged rows by position).
    let order_by = resolve_order_by(order_by, &columns, items)?;

    let skip = eval_usize_literal(skip)?;
    let limit = eval_usize_literal(limit)?;

    // Owners run the stage query, minus the global-only clauses the coordinator
    // re-applies. For aggregates we rewrite so partials merge (build_owner_query);
    // for pure scans owners run it verbatim, minus ORDER/SKIP/LIMIT/DISTINCT.
    let owner_query = build_owner_query(match_clause, items, &kind)?;

    Some(DistributedPlan {
        kind,
        columns,
        owner_query,
        distinct,
        order_by,
        skip,
        limit,
        rel_join: None,
        path_join: None,
        join_post: None,
        with_stage: None,
        with_stage_chain: Vec::new(),
        union: None,
        write: None,
        create: None,
        merge: None,
    })
}

/// Try to build a `MATCH … WITH [WITH …] … RETURN` pipeline plan.
///
/// The MATCH (node scan or relationship pattern) plus the *first* WITH run
/// distributively as stage 1, producing intermediate rows materialized on the
/// coordinator. Every subsequent WITH and the final RETURN become coordinator
/// stages ([`WithStage`]), applied in order to the materialized rows: each stage
/// filters (the previous WITH's WHERE — a HAVING-style single comparison),
/// projects/reorders the columns, then applies its ORDER BY / SKIP / LIMIT /
/// DISTINCT. A two-stage `MATCH → WITH → RETURN` yields exactly one `WithStage`
/// (identical to before); a 3+ WITH chain yields one per additional clause.
///
/// Coordinator stages only *select* existing columns — no new expressions or
/// aggregates past the first WITH (those stay on the honest 501), since the
/// distributed aggregate merge happens only in stage 1.
fn plan_with_pipeline(clauses: &[Clause]) -> Option<DistributedPlan> {
    // clauses == [MATCH, WITH, (WITH…), RETURN] (at least 3).
    if clauses.len() < 3 {
        return None;
    }
    // Middle clauses (index 1..last) must all be WITH; the last must be RETURN.
    let last = clauses.len() - 1;
    if !matches!(clauses[last], Clause::Return { .. }) {
        return None;
    }
    for c in &clauses[1..last] {
        if !matches!(c, Clause::With { .. }) {
            return None;
        }
    }

    let (pattern, match_pred) = match &clauses[0] {
        Clause::Match {
            optional: false,
            pattern,
            predicate,
        } => (pattern, predicate),
        _ => return None,
    };

    let (with_items, with_where, with_order, with_skip, with_limit, with_distinct) =
        match &clauses[1] {
            Clause::With {
                items,
                where_clause,
                order_by,
                skip,
                limit,
                distinct,
            } => (items, where_clause, order_by, skip, limit, *distinct),
            _ => return None,
        };

    // Stage 1: MATCH + first WITH-as-RETURN, run distributively. A node-scan
    // MATCH goes through the scan/aggregate planner; a relationship pattern goes
    // through the cross-partition join/path planner (by synthesizing a RETURN
    // clause from the WITH items). Either way, stage-1 columns are the first
    // WITH's outputs, which the coordinator stages post-process.
    let mut stage1 = if pattern_is_node_only(pattern) {
        let _ = match_pred;
        plan_node_scan_stage(
            &clauses[0],
            with_items,
            with_order.as_ref(),
            with_skip.as_ref(),
            with_limit.as_ref(),
            with_distinct,
        )?
    } else {
        // Relationship-pattern stage 1: reuse the join planner, feeding it a
        // RETURN built from the WITH items (join planner only accepts a Return
        // clause). The WITH's own ORDER/SKIP/LIMIT/DISTINCT ride on the RETURN so
        // any aggregate/global clause folds into the join's JoinPost.
        let synth_return = Clause::Return {
            items: with_items.clone(),
            order_by: with_order.clone(),
            skip: with_skip.clone(),
            limit: with_limit.clone(),
            distinct: with_distinct,
        };
        plan_relationship_join(pattern, &synth_return, match_pred.is_some())?
    };

    // Walk the remaining clauses (WITH₂ … WITHₙ, RETURN), building one
    // coordinator [`WithStage`] each. A stage's *filter* is the WHERE of the
    // clause that produced its input columns (HAVING semantics: the WHERE runs
    // after that clause's projection); its projection/order/skip/limit/distinct
    // come from the current clause. So the filter for the stage built from
    // clause[i] is clause[i-1]'s WHERE.
    let mut prev_columns = stage1.columns.clone();
    let mut prev_where: Option<&Expression> = with_where.as_ref();
    let mut stages: Vec<WithStage> = Vec::with_capacity(clauses.len() - 2);
    for clause in &clauses[2..] {
        let (items, order, skip, limit, distinct, next_where) = match clause {
            Clause::With {
                items,
                where_clause,
                order_by,
                skip,
                limit,
                distinct,
            } => (
                items,
                order_by,
                skip,
                limit,
                *distinct,
                where_clause.as_ref(),
            ),
            Clause::Return {
                items,
                order_by,
                skip,
                limit,
                distinct,
            } => (items, order_by, skip, limit, *distinct, None),
            _ => return None,
        };
        let stage = build_with_stage(
            prev_where,
            &prev_columns,
            items,
            order.as_ref(),
            skip.as_ref(),
            limit.as_ref(),
            distinct,
        )?;
        prev_columns = stage.columns.clone();
        prev_where = next_where;
        stages.push(stage);
    }

    // The first coordinator stage lives in `with_stage` (backward-compatible with
    // the two-stage path); any further stages chain after it.
    let mut iter = stages.into_iter();
    stage1.with_stage = iter.next();
    stage1.with_stage_chain = iter.collect();
    Some(stage1)
}

/// Build one coordinator [`WithStage`] from a clause's projection over the
/// previous stage's `input_columns`, filtered by `filter_where` (the previous
/// WITH's WHERE — HAVING semantics). Items may only *select* existing input
/// columns (bare var / alias / property text matching an input column name); new
/// expressions and aggregates past stage 1 aren't mergeable here (→ `None`).
#[allow(clippy::too_many_arguments)]
fn build_with_stage(
    filter_where: Option<&Expression>,
    input_columns: &[String],
    items: &[nexora_language::ast::ReturnItem],
    order_by: Option<&OrderBy>,
    skip: Option<&Expression>,
    limit: Option<&Expression>,
    distinct: bool,
) -> Option<WithStage> {
    // Filter (over input columns) = the WHERE of the clause that produced them.
    let filter = match filter_where {
        None => None,
        Some(expr) => Some(build_row_filter(expr, input_columns)?),
    };

    // Projection: each item selects one existing input column. Aggregates and
    // arithmetic/function expressions aren't supported past stage 1.
    let mut projection = Vec::with_capacity(items.len());
    let mut out_columns = Vec::with_capacity(items.len());
    for item in items {
        if matches!(item.expression, Expression::Aggregation { .. }) {
            return None; // aggregate past stage 1 → not mergeable here
        }
        let key = item.expression.to_string();
        let col = return_item_column(item);
        let idx = input_columns
            .iter()
            .position(|c| c == &key)
            .or_else(|| input_columns.iter().position(|c| c == &col))?;
        projection.push(idx);
        out_columns.push(col);
    }

    let resolved_order = resolve_order_by(order_by, &out_columns, items)?;
    let skip_v = eval_usize_literal(skip)?;
    let limit_v = eval_usize_literal(limit)?;

    Some(WithStage {
        stage1_columns: input_columns.to_vec(),
        filter,
        projection,
        columns: out_columns,
        order_by: resolved_order,
        skip: skip_v,
        limit: limit_v,
        distinct,
    })
}

/// Build a single-comparison row filter from a WITH `WHERE col <op> literal`
/// expression, resolving `col` to a stage-1 column index. Returns `None` for
/// predicates outside this slice (compound boolean, non-literal RHS, etc.).
fn build_row_filter(expr: &Expression, columns: &[String]) -> Option<RowFilter> {
    use nexora_language::ast::BinaryOp;
    let Expression::BinOp { op, left, right } = expr else {
        return None;
    };
    // Only comparison operators (a HAVING-style filter).
    if !matches!(
        op,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
    ) {
        return None;
    }
    // LHS references a stage-1 column (by variable/alias or expression text).
    let key = left.to_string();
    let col = columns.iter().position(|c| c == &key)?;
    // RHS must be a literal.
    let Expression::Literal(v) = right.as_ref() else {
        return None;
    };
    let literal = property_value_to_json(v);
    Some(RowFilter {
        col,
        op: *op,
        literal,
    })
}

/// column projections the join must extract plus (when the RETURN aggregates or
/// carries global clauses) a [`JoinPost`] describing the local post-processing.
///
/// `bound` is the set of node variables the pattern binds. Returns
/// `(projections, columns, join_post)`:
/// - `projections` — the raw columns the join extracts (one per grouping key /
///   aggregate argument / bare projection);
/// - `columns` — output column names in RETURN order;
/// - `join_post` — `Some` when the RETURN has aggregates OR global clauses
///   (order/skip/limit/distinct), else `None` (raw projections are the output).
///
/// Returns `None` (→ 501) for expressions outside the supported set.
fn build_join_return(
    return_clause: &Clause,
    bound: &std::collections::HashSet<&String>,
) -> Option<(Vec<RelProjection>, Vec<String>, Option<JoinPost>)> {
    let (items, order_by, skip, limit, distinct) = match return_clause {
        Clause::Return {
            items,
            order_by,
            skip,
            limit,
            distinct,
        } => (items, order_by, skip, limit, *distinct),
        _ => return None,
    };

    // Classify each RETURN item as a bare/property projection or an aggregate.
    let has_aggregate = items
        .iter()
        .any(|it| matches!(it.expression, Expression::Aggregation { .. }));
    let has_global = order_by.is_some() || skip.is_some() || limit.is_some() || distinct;

    // Fast path: no aggregates, no global clauses → simple projections (existing
    // behavior; join output is the result directly).
    if !has_aggregate && !has_global {
        let mut projections = Vec::with_capacity(items.len());
        let mut columns = Vec::with_capacity(items.len());
        for item in items {
            let (proj, column) = simple_projection(item, bound)?;
            columns.push(column);
            projections.push(proj);
        }
        return Some((projections, columns, None));
    }

    // Aggregation / global path: build raw projections (deduped) + JoinPost.
    // Each grouping key or aggregate argument becomes a raw column; JoinPostItem
    // references it by index. `count(*)` needs no raw column.
    let mut projections: Vec<RelProjection> = Vec::new();
    let mut columns = Vec::with_capacity(items.len());
    let mut post_items = Vec::with_capacity(items.len());

    // Intern a raw projection (dedupe by var+property), returning its index.
    let mut intern = |var: String, property: Option<String>| -> usize {
        if let Some(i) = projections
            .iter()
            .position(|p| p.var == var && p.property == property)
        {
            return i;
        }
        let idx = projections.len();
        projections.push(RelProjection {
            var,
            property,
            column: format!("_raw{idx}"),
        });
        idx
    };

    for item in items {
        let column = return_item_column(item);
        columns.push(column);
        match &item.expression {
            Expression::Aggregation {
                function,
                expr,
                distinct: agg_distinct,
            } => {
                // count(*) → no raw column; otherwise the arg is var / var.prop.
                let raw_idx = match agg_arg_column(expr) {
                    Some(col) if col == "*" => None,
                    _ => {
                        let (var, property) = agg_var_property(expr, bound)?;
                        Some(intern(var, property))
                    }
                };
                post_items.push(JoinPostItem::Aggregate {
                    func: *function,
                    raw_idx,
                    distinct: *agg_distinct,
                });
            }
            Expression::Variable(v) if bound.contains(v) => {
                let raw_idx = intern(v.clone(), None);
                post_items.push(JoinPostItem::Grouping { raw_idx });
            }
            Expression::Property(base, prop) => {
                let Expression::Variable(v) = base.as_ref() else {
                    return None;
                };
                if !bound.contains(v) {
                    return None;
                }
                let raw_idx = intern(v.clone(), Some(prop.clone()));
                post_items.push(JoinPostItem::Grouping { raw_idx });
            }
            _ => return None,
        }
    }

    // Resolve ORDER BY against the output columns (by name / expression text).
    let ob = resolve_order_by(order_by.as_ref(), &columns, items)?;
    let skip_v = eval_usize_literal(skip.as_ref())?;
    let limit_v = eval_usize_literal(limit.as_ref())?;

    let post = JoinPost {
        items: post_items,
        columns: columns.clone(),
        order_by: ob,
        skip: skip_v,
        limit: limit_v,
        distinct,
    };
    Some((projections, columns, Some(post)))
}

/// A simple (non-aggregate) join projection: bare var → id, or var.prop.
fn simple_projection(
    item: &nexora_language::ast::ReturnItem,
    bound: &std::collections::HashSet<&String>,
) -> Option<(RelProjection, String)> {
    let column = return_item_column(item);
    let proj = match &item.expression {
        Expression::Variable(v) if bound.contains(v) => RelProjection {
            var: v.clone(),
            property: None,
            column: column.clone(),
        },
        Expression::Property(base, prop) => {
            let Expression::Variable(v) = base.as_ref() else {
                return None;
            };
            if !bound.contains(v) {
                return None;
            }
            RelProjection {
                var: v.clone(),
                property: Some(prop.clone()),
                column: column.clone(),
            }
        }
        _ => return None,
    };
    Some((proj, column))
}

/// Extract `(var, property)` from an aggregate's argument (`sum(n.age)` →
/// `("n", Some("age"))`, `count(n)` → `("n", None)`). `None` for complex args or
/// unbound vars.
fn agg_var_property(
    expr: &Expression,
    bound: &std::collections::HashSet<&String>,
) -> Option<(String, Option<String>)> {
    match expr {
        Expression::Variable(v) if bound.contains(v) => Some((v.clone(), None)),
        Expression::Property(base, prop) => {
            let Expression::Variable(v) = base.as_ref() else {
                return None;
            };
            if !bound.contains(v) {
                return None;
            }
            Some((v.clone(), Some(prop.clone())))
        }
        _ => None,
    }
}

/// `MATCH (a:La)-[:REL]->(b:Lb) RETURN a.x, b.y`. Returns `None` (→ 501) for any
/// shape outside this slice: multi-hop chains, variable-length paths, undirected
/// edges, missing edge type, WHERE, or projections referencing unbound variables.
/// Aggregates and global clauses over the join are handled via [`JoinPost`].
fn plan_relationship_join(
    pattern: &nexora_language::ast::Pattern,
    return_clause: &Clause,
    predicate_present: bool,
) -> Option<DistributedPlan> {
    if predicate_present {
        return None; // WHERE on a join not supported in this slice
    }
    // Exactly one path.
    if pattern.parts.len() != 1 {
        return None;
    }
    let chain = &pattern.parts[0].chain;
    // A single-hop chain is two segments: [ {node:a, edge:REL}, {node:b, edge:None} ].
    // Anything longer (multi-hop) or variable-length is handled by the path join.
    if chain.segments.len() != 2 {
        return plan_path_join(pattern, return_clause, predicate_present);
    }
    let seg0 = &chain.segments[0];
    let seg1 = &chain.segments[1];
    let edge = seg0.edge.as_ref()?;
    if seg1.edge.is_some() {
        return None; // more than one hop
    }
    // Directed, single-hop, typed edge → the dedicated single-hop join.
    // Undirected/untyped single hops delegate to the (more general) path join.
    use nexora_language::ast::EdgeDirection as Dir;
    let outgoing = match edge.direction {
        Dir::Outgoing => true,
        Dir::Incoming => false,
        Dir::Either => return plan_path_join(pattern, return_clause, predicate_present),
    };
    if edge.min_hops.is_some() || edge.max_hops.is_some() {
        // Variable-length single hop → path join handles it.
        return plan_path_join(pattern, return_clause, predicate_present);
    }
    let Some(edge_type) = edge.edge_type.clone() else {
        // Untyped single hop → path join (any edge type).
        return plan_path_join(pattern, return_clause, predicate_present);
    };
    let source_var = seg0.node.variable.clone()?;
    let target_var = seg1.node.variable.clone()?;
    if source_var == target_var {
        return None;
    }

    // RETURN over the two bound vars: simple projections, or aggregates/global
    // clauses folded into a JoinPost applied locally to the join output.
    let bound: std::collections::HashSet<&String> =
        [&source_var, &target_var].into_iter().collect();
    let (projections, columns, join_post) = build_join_return(return_clause, &bound)?;

    Some(DistributedPlan {
        kind: MergeKind::Concat,
        columns,
        owner_query: String::new(), // join path ignores owner_query
        distinct: false,
        order_by: Vec::new(),
        skip: None,
        limit: None,
        rel_join: Some(RelJoinSpec {
            source_var,
            source_labels: seg0.node.labels.clone(),
            edge_type,
            outgoing,
            target_var,
            target_labels: seg1.node.labels.clone(),
            projections,
        }),
        path_join: None,
        join_post,
        with_stage: None,
        with_stage_chain: Vec::new(),
        union: None,
        write: None,
        create: None,
        merge: None,
    })
}

/// Try to build a multi-hop / variable-length path-join plan for a pattern like
/// `MATCH (a:La)-[:R]->(b)-[:R]->(c) RETURN ...`, `MATCH (a)-[:R*1..3]->(b)`,
/// undirected `-[:R]-` or untyped `-[]->` hops. Returns `None` (→ 501) for
/// shapes outside this slice: multiple path parts, WHERE, aggregates / order /
/// skip / limit / distinct, or projections over unbound vars.
fn plan_path_join(
    pattern: &nexora_language::ast::Pattern,
    return_clause: &Clause,
    predicate_present: bool,
) -> Option<DistributedPlan> {
    use nexora_language::ast::EdgeDirection as Dir;

    if predicate_present || pattern.parts.len() != 1 {
        return None;
    }
    let chain = &pattern.parts[0].chain;
    // A chain of N nodes has segments [n0+edge, n1+edge, ..., n(N-1)+None].
    // Need at least one hop.
    if chain.segments.len() < 2 {
        return None;
    }

    let mut nodes = Vec::with_capacity(chain.segments.len());
    let mut hops = Vec::with_capacity(chain.segments.len() - 1);
    for (i, seg) in chain.segments.iter().enumerate() {
        nodes.push(PathNode {
            var: seg.node.variable.clone(),
            labels: seg.node.labels.clone(),
        });
        let is_last = i == chain.segments.len() - 1;
        match (&seg.edge, is_last) {
            (Some(edge), false) => {
                let dir = match edge.direction {
                    Dir::Outgoing => HopDir::Outgoing,
                    Dir::Incoming => HopDir::Incoming,
                    Dir::Either => HopDir::Either, // undirected supported
                };
                let edge_type = edge.edge_type.clone(); // None = any type (untyped)
                                                        // Variable-length bounds: default to a single hop when absent.
                let min_hops = edge.min_hops.unwrap_or(1).max(1);
                let max_hops = edge.max_hops.unwrap_or(min_hops).max(min_hops);
                // Bound the fan-out: cap variable-length expansion depth.
                if max_hops > 8 {
                    return None;
                }
                hops.push(PathHop {
                    edge_type,
                    dir,
                    min_hops,
                    max_hops,
                });
            }
            // Last node has no trailing edge; a non-last node must have one.
            (None, true) => {}
            _ => return None,
        }
    }
    if hops.is_empty() {
        return None;
    }

    // RETURN over named path nodes: simple projections, or aggregates/global
    // clauses folded into a JoinPost applied locally to the path output.
    let named: std::collections::HashSet<&String> =
        nodes.iter().filter_map(|n| n.var.as_ref()).collect();
    let (projections, columns, join_post) = build_join_return(return_clause, &named)?;

    Some(DistributedPlan {
        kind: MergeKind::Concat,
        columns: columns.clone(),
        owner_query: String::new(),
        distinct: false,
        order_by: Vec::new(),
        skip: None,
        limit: None,
        rel_join: None,
        path_join: Some(PathJoinSpec {
            nodes,
            hops,
            projections,
            columns,
        }),
        join_post,
        with_stage: None,
        with_stage_chain: Vec::new(),
        union: None,
        write: None,
        create: None,
        merge: None,
    })
}

/// A column-oriented query result: column names plus row values.
type QueryRows = (Vec<String>, Vec<Vec<serde_json::Value>>);
/// A boxed, `Send` future producing a [`QueryRows`] result — the return type of
/// the recursive [`execute`] entry point (boxed to break the recursion cycle).
type QueryFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<QueryRows, RouterError>> + Send + 'a>,
>;

/// Execute a plan: fan the owner query out to every distinct shard owner, then
/// merge per the plan. Any owner error is surfaced (never a silent partial).
pub fn execute<'a>(
    router: &'a HybridRouter,
    plan: &'a DistributedPlan,
    concern: Option<ReadConcern>,
    progress: Option<&'a ReplicationProgress>,
    session: Option<&'a SessionReadTracker>,
) -> QueryFuture<'a> {
    Box::pin(execute_inner(router, plan, concern, progress, session))
}

async fn execute_inner(
    router: &HybridRouter,
    plan: &DistributedPlan,
    concern: Option<ReadConcern>,
    progress: Option<&ReplicationProgress>,
    session: Option<&SessionReadTracker>,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>), RouterError> {
    // Distributed CREATE: group nodes by owner, dispatch CREATE sub-queries.
    if let Some(c) = &plan.create {
        return execute_create(router, c).await;
    }
    // Distributed MERGE: two-phase scatter-gather MATCH, then CREATE if not found.
    if let Some(m) = &plan.merge {
        return execute_merge(router, m).await;
    }
    // Distributed write: fan the write out to every owner (each mutates only
    // its own matched nodes), then sum the per-owner stats.
    if let Some(w) = &plan.write {
        return execute_write(router, w).await;
    }
    // UNION: run each branch distributively, then concat (+ dedup unless ALL).
    if let Some(u) = &plan.union {
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
        for branch in &u.branches {
            let (_cols, branch_rows) = execute(router, branch, concern, progress, session).await?;
            rows.extend(branch_rows);
        }
        if !u.union_all {
            let mut seen = std::collections::HashSet::new();
            rows.retain(|r| seen.insert(serde_json::to_string(r).unwrap_or_default()));
        }
        return Ok((u.columns.clone(), rows));
    }
    // Compute the stage-1 result via whichever source the plan selects:
    // relationship join, path join, or a fanned-out scan/aggregate. A WITH
    // pipeline (`with_stage`) then post-processes these rows uniformly below.
    let stage1 = if let Some(spec) = &plan.rel_join {
        let (raw_cols, raw_rows) = join::execute_rel_join(router, spec).await?;
        // Aggregation/order over the join? Post-process the raw rows locally.
        match &plan.join_post {
            Some(post) => merge::apply_join_post(post, raw_rows),
            None => (raw_cols, raw_rows),
        }
    } else if let Some(spec) = &plan.path_join {
        let (raw_cols, raw_rows) = path::execute_path_join(router, spec).await?;
        match &plan.join_post {
            Some(post) => merge::apply_join_post(post, raw_rows),
            None => (raw_cols, raw_rows),
        }
    } else {
        // C2: build a per-owner failover chain honoring the read concern. Each
        // logical owner scans the shards it owns; if it is unreachable, the read
        // may fail over to a replica — but only one that (for Majority) has
        // caught up to every one of that owner's shards per the C1/C2 guards.
        // Linearizable keeps an empty chain (owner only, no failover).
        let concern = concern.unwrap_or_default();
        let read_targets = build_read_targets(router, concern, progress, session).await;
        if read_targets.is_empty() {
            return Err(RouterError::NodeNotFound("no shard owners".into()));
        }

        // Use stream-based execution with bounded concurrency to prevent OOM
        // from unbounded parallel queries (P0 fix: backpressure mechanism)
        use futures::stream::{self, StreamExt};

        const MAX_CONCURRENT_QUERIES: usize = 32;

        let query = plan.owner_query.clone();
        let client = router.remote_client_arc();

        let mut per_owner: Vec<(Vec<String>, Vec<Vec<serde_json::Value>>)> = Vec::new();
        let mut stream = stream::iter(read_targets)
            .map(|target| {
                let op = GraphOperation::ExecuteCypher {
                    query: query.clone(),
                };
                let c = client.clone();
                async move {
                    let Some(client_ref) = c else {
                        return (
                            target.owner.clone(),
                            Err(RouterError::NodeNotFound(
                                "router has no remote client".into(),
                            )),
                        );
                    };
                    // Try the owner, then each qualifying failover candidate in turn.
                    let mut last_err = None;
                    for node in std::iter::once(&target.owner).chain(target.failover.iter()) {
                        match client_ref.execute(node, op.clone()).await {
                            Ok(r) => return (target.owner.clone(), Ok(r)),
                            Err(e) => last_err = Some(e),
                        }
                    }
                    (
                        target.owner.clone(),
                        Err(last_err.unwrap_or_else(|| {
                            RouterError::NodeNotFound("no reachable read target".into())
                        })),
                    )
                }
            })
            .buffer_unordered(MAX_CONCURRENT_QUERIES);

        while let Some((owner, res)) = stream.next().await {
            match res {
                Ok(GraphResult::CypherRows { columns, rows }) => per_owner.push((columns, rows)),
                Ok(other) => {
                    return Err(RouterError::Remote(format!(
                        "owner {owner} returned non-row result: {other:?}"
                    )))
                }
                Err(e) => {
                    return Err(RouterError::Remote(format!(
                        "owner {owner} failed: {e} (refusing partial result)"
                    )))
                }
            }
        }

        merge::merge_rows(plan, per_owner)?
    };

    // WITH pipeline: apply the coordinator stages (WHERE filter → projection →
    // order/skip/limit/distinct) in order to the materialized stage-1 rows. The
    // first stage lives in `with_stage`; any further stages (3+ WITH chains)
    // follow in `with_stage_chain`, each fed the previous stage's output.
    if let Some(with_stage) = &plan.with_stage {
        let (mut cols, mut rows) = merge::apply_with_stage(with_stage, stage1.1);
        for stage in &plan.with_stage_chain {
            let (c, r) = merge::apply_with_stage(stage, rows);
            cols = c;
            rows = r;
        }
        return Ok((cols, rows));
    }
    Ok(stage1)
}

/// Execute a distributed write: run the write on every owner (each mutates only
/// its own matched nodes) and sum the per-owner [`WriteResult`] stats. Owners
/// return the stats as a structured `CypherRows` (see `WRITE_STAT_COLUMNS`);
/// this sums them column-wise into a single stats row. Any owner error is
/// surfaced (never a silent partial write-count).
///
/// When a ReplicaWriter is configured on the router (RF>1), delegates to
/// `execute_write_with_replication` for quorum write support.
async fn execute_write(
    router: &HybridRouter,
    plan: &WritePlan,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>), RouterError> {
    // If RF>1 replication is configured, use the replication-aware write path
    if router.replica_writer().is_some() {
        return crate::distributed_query_with_replication::execute_write_with_replication(
            router, plan,
        )
        .await;
    }

    // RF=1 path: owner-only writes (original implementation)
    let owners = distinct_owners(router).await;
    if owners.is_empty() {
        return Err(RouterError::NodeNotFound("no shard owners".into()));
    }

    let mut handles = Vec::with_capacity(owners.len());
    for owner in owners {
        let op = GraphOperation::ExecuteCypher {
            query: plan.owner_query.clone(),
        };
        let client = router.remote_client_arc();
        handles.push(tokio::spawn(async move {
            match client {
                Some(c) => (owner.clone(), c.execute(&owner, op).await),
                None => (
                    owner.clone(),
                    Err(RouterError::NodeNotFound(
                        "router has no remote client".into(),
                    )),
                ),
            }
        }));
    }

    // Sum the 8 stat columns across owners.
    let mut totals = [0i64; WRITE_STAT_COLUMNS.len()];
    for h in handles {
        let (owner, res) = h.await.map_err(|e| RouterError::Remote(e.to_string()))?;
        match res {
            Ok(GraphResult::CypherRows { rows, .. }) => {
                if let Some(row) = rows.first() {
                    for (i, cell) in row.iter().enumerate().take(totals.len()) {
                        totals[i] += cell.as_i64().unwrap_or(0);
                    }
                }
            }
            Ok(other) => {
                return Err(RouterError::Remote(format!(
                    "owner {owner} returned non-stat write result: {other:?}"
                )))
            }
            Err(e) => {
                return Err(RouterError::Remote(format!(
                    "owner {owner} write failed: {e} (refusing partial write)"
                )))
            }
        }
    }

    let columns: Vec<String> = WRITE_STAT_COLUMNS.iter().map(|s| s.to_string()).collect();
    let row: Vec<serde_json::Value> = totals.iter().map(|n| serde_json::json!(n)).collect();
    Ok((columns, vec![row]))
}

/// Execute a distributed CREATE: group pre-generated nodes by their target owner
/// (based on `qid.shard_key() % total_shards`), build owner-specific CREATE
/// sub-queries, fan them out in parallel, and sum the per-owner write stats.
/// Each owner creates only nodes that belong to its shards, ensuring correct
/// physical placement.
///
/// When a ReplicaWriter is configured on the router (RF>1), delegates to
/// `execute_create_with_replication` for quorum write support.
async fn execute_create(
    router: &HybridRouter,
    plan: &CreatePlan,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>), RouterError> {
    // If RF>1 replication is configured, use the replication-aware CREATE path
    if router.replica_writer().is_some() {
        return crate::distributed_create_with_replication::execute_create_with_replication(
            router, plan,
        )
        .await;
    }

    // RF=1 path: owner-only creates (original implementation)
    use std::collections::HashMap;

    if plan.nodes.is_empty() {
        // Empty CREATE → return zero stats.
        let columns: Vec<String> = WRITE_STAT_COLUMNS.iter().map(|s| s.to_string()).collect();
        let row: Vec<serde_json::Value> = vec![serde_json::json!(0); WRITE_STAT_COLUMNS.len()];
        return Ok((columns, vec![row]));
    }

    // Group nodes by their target owner (determined by qid.shard_key()).
    let shard_map = router.shard_map_snapshot().await;
    let mut by_owner: HashMap<String, Vec<&CreateOp>> = HashMap::new();
    for op in &plan.nodes {
        let shard = shard_map.shard_of(&op.qid);
        let owner = match shard_map.get(shard) {
            Some(a) => &a.owner,
            None => {
                return Err(RouterError::NodeNotFound(format!(
                    "no owner for shard {shard} (qid {})",
                    op.qid.to_hex()
                )))
            }
        };
        by_owner.entry(owner.clone()).or_default().push(op);
    }

    // Build and dispatch a CREATE sub-query for each owner.
    let mut handles = Vec::with_capacity(by_owner.len());
    for (owner, ops) in by_owner {
        let query = build_create_subquery(&ops);
        let op = GraphOperation::ExecuteCypher { query };
        let client = router.remote_client_arc();
        handles.push(tokio::spawn(async move {
            match client {
                Some(c) => (owner.clone(), c.execute(&owner, op).await),
                None => (
                    owner.clone(),
                    Err(RouterError::NodeNotFound(
                        "router has no remote client".into(),
                    )),
                ),
            }
        }));
    }

    // Sum the write stats across owners (same as execute_write).
    let mut totals = [0i64; WRITE_STAT_COLUMNS.len()];
    for h in handles {
        let (owner, res) = h.await.map_err(|e| RouterError::Remote(e.to_string()))?;
        match res {
            Ok(GraphResult::CypherRows { rows, .. }) => {
                if let Some(row) = rows.first() {
                    for (i, cell) in row.iter().enumerate().take(totals.len()) {
                        totals[i] += cell.as_i64().unwrap_or(0);
                    }
                }
            }
            Ok(other) => {
                return Err(RouterError::Remote(format!(
                    "owner {owner} returned non-stat CREATE result: {other:?}"
                )))
            }
            Err(e) => {
                return Err(RouterError::Remote(format!(
                    "owner {owner} CREATE failed: {e} (refusing partial CREATE)"
                )))
            }
        }
    }

    let columns: Vec<String> = WRITE_STAT_COLUMNS.iter().map(|s| s.to_string()).collect();
    let row: Vec<serde_json::Value> = totals.iter().map(|n| serde_json::json!(n)).collect();
    Ok((columns, vec![row]))
}

/// Execute a distributed MERGE: two-phase operation to avoid duplicate creation.
/// Phase 1: scatter-gather MATCH across all owners to find existing nodes.
/// Phase 2: if no owner found a match, CREATE the node on a coordinator-chosen
/// owner (based on pre-generated qid) and apply ON CREATE SET clauses. If any
/// owner found a match, apply ON MATCH SET clauses to those nodes.
///
/// Returns write stats (nodes_created, properties_set, etc.) summed across owners.
async fn execute_merge(
    router: &HybridRouter,
    plan: &MergePlan,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>), RouterError> {
    use std::collections::HashMap;

    if plan.nodes.is_empty() {
        // Empty MERGE → return zero stats.
        let columns: Vec<String> = WRITE_STAT_COLUMNS.iter().map(|s| s.to_string()).collect();
        let row: Vec<serde_json::Value> = vec![serde_json::json!(0); WRITE_STAT_COLUMNS.len()];
        return Ok((columns, vec![row]));
    }

    // Phase 1: scatter-gather MATCH across all owners to find existing nodes.
    // Rebuild MATCH pattern from nodes: MATCH (var:Label {prop:val}), ...
    let match_parts: Vec<String> = plan
        .nodes
        .iter()
        .map(|op| {
            let var = op.variable.as_deref().unwrap_or("_");
            let labels = if op.labels.is_empty() {
                String::new()
            } else {
                format!(":{}", op.labels.join(":"))
            };
            let props = if op.properties.is_empty() {
                String::new()
            } else {
                let pairs: Vec<String> = op
                    .properties
                    .iter()
                    .map(|(k, v)| format!("{k}: {}", serde_json::to_string(v).unwrap_or_default()))
                    .collect();
                format!(" {{{}}}", pairs.join(", "))
            };
            format!("({var}{labels}{props})")
        })
        .collect();
    let match_pattern = match_parts.join(", ");
    let match_query = format!("MATCH {} RETURN count(*)", match_pattern);
    let owners = distinct_owners(router).await;
    if owners.is_empty() {
        return Err(RouterError::NodeNotFound("no shard owners".into()));
    }

    let mut handles = Vec::with_capacity(owners.len());
    for owner in &owners {
        let op = GraphOperation::ExecuteCypher {
            query: match_query.clone(),
        };
        let client = router.remote_client_arc();
        let owner = owner.clone();
        handles.push(tokio::spawn(async move {
            match client {
                Some(c) => (owner.clone(), c.execute(&owner, op).await),
                None => (
                    owner.clone(),
                    Err(RouterError::NodeNotFound(
                        "router has no remote client".into(),
                    )),
                ),
            }
        }));
    }

    // Collect MATCH results: sum counts across owners to see if any found a match.
    let mut total_matched = 0i64;
    for h in handles {
        let (owner, res) = h.await.map_err(|e| RouterError::Remote(e.to_string()))?;
        match res {
            Ok(GraphResult::CypherRows { rows, .. }) => {
                if let Some(row) = rows.first() {
                    if let Some(count) = row.first().and_then(|v| v.as_i64()) {
                        total_matched += count;
                    }
                }
            }
            Ok(other) => {
                return Err(RouterError::Remote(format!(
                    "owner {owner} returned non-row MATCH result: {other:?}"
                )))
            }
            Err(e) => {
                return Err(RouterError::Remote(format!(
                    "owner {owner} MATCH failed: {e} (refusing partial MERGE)"
                )))
            }
        }
    }

    // Phase 2: decide whether to CREATE or apply ON MATCH SET.
    let mut totals = [0i64; WRITE_STAT_COLUMNS.len()];

    if total_matched == 0 {
        // No match found → CREATE branch: group nodes by target owner and dispatch.
        let shard_map = router.shard_map_snapshot().await;
        let mut by_owner: HashMap<String, Vec<&CreateOp>> = HashMap::new();
        for op in &plan.nodes {
            let shard = shard_map.shard_of(&op.qid);
            let owner = match shard_map.get(shard) {
                Some(a) => &a.owner,
                None => {
                    return Err(RouterError::NodeNotFound(format!(
                        "no owner for shard {shard} (qid {})",
                        op.qid.to_hex()
                    )))
                }
            };
            by_owner.entry(owner.clone()).or_default().push(op);
        }

        let mut handles = Vec::with_capacity(by_owner.len());
        for (owner, ops) in by_owner {
            // Build CREATE query with ON CREATE SET clauses.
            let mut query = build_create_subquery(&ops);
            if !plan.on_create.is_empty() {
                query.push_str(" SET ");
                query.push_str(&plan.on_create.join(", "));
            }
            let op = GraphOperation::ExecuteCypher { query };
            let client = router.remote_client_arc();
            handles.push(tokio::spawn(async move {
                match client {
                    Some(c) => (owner.clone(), c.execute(&owner, op).await),
                    None => (
                        owner.clone(),
                        Err(RouterError::NodeNotFound(
                            "router has no remote client".into(),
                        )),
                    ),
                }
            }));
        }

        // Sum CREATE stats across owners.
        for h in handles {
            let (owner, res) = h.await.map_err(|e| RouterError::Remote(e.to_string()))?;
            match res {
                Ok(GraphResult::CypherRows { rows, .. }) => {
                    if let Some(row) = rows.first() {
                        for (i, cell) in row.iter().enumerate().take(totals.len()) {
                            totals[i] += cell.as_i64().unwrap_or(0);
                        }
                    }
                }
                Ok(other) => {
                    return Err(RouterError::Remote(format!(
                        "owner {owner} returned non-stat MERGE CREATE result: {other:?}"
                    )))
                }
                Err(e) => {
                    return Err(RouterError::Remote(format!(
                        "owner {owner} MERGE CREATE failed: {e}"
                    )))
                }
            }
        }
    } else {
        // Match found → ON MATCH branch: apply SET clauses to matched nodes.
        // Fan out the ON MATCH SET to all owners (each applies to its matched nodes).
        if !plan.on_match.is_empty() {
            // Rebuild MATCH pattern (same as phase 1, but now for the SET query).
            let match_parts: Vec<String> = plan
                .nodes
                .iter()
                .map(|op| {
                    let var = op.variable.as_deref().unwrap_or("_");
                    let labels = if op.labels.is_empty() {
                        String::new()
                    } else {
                        format!(":{}", op.labels.join(":"))
                    };
                    let props = if op.properties.is_empty() {
                        String::new()
                    } else {
                        let pairs: Vec<String> = op
                            .properties
                            .iter()
                            .map(|(k, v)| {
                                format!("{k}: {}", serde_json::to_string(v).unwrap_or_default())
                            })
                            .collect();
                        format!(" {{{}}}", pairs.join(", "))
                    };
                    format!("({var}{labels}{props})")
                })
                .collect();
            let match_pattern = match_parts.join(", ");
            let set_query = format!("MATCH {} SET {}", match_pattern, plan.on_match.join(", "));
            let mut handles = Vec::with_capacity(owners.len());
            for owner in &owners {
                let op = GraphOperation::ExecuteCypher {
                    query: set_query.clone(),
                };
                let client = router.remote_client_arc();
                let owner = owner.clone();
                handles.push(tokio::spawn(async move {
                    match client {
                        Some(c) => (owner.clone(), c.execute(&owner, op).await),
                        None => (
                            owner.clone(),
                            Err(RouterError::NodeNotFound(
                                "router has no remote client".into(),
                            )),
                        ),
                    }
                }));
            }

            // Sum ON MATCH SET stats across owners.
            for h in handles {
                let (owner, res) = h.await.map_err(|e| RouterError::Remote(e.to_string()))?;
                match res {
                    Ok(GraphResult::CypherRows { rows, .. }) => {
                        if let Some(row) = rows.first() {
                            for (i, cell) in row.iter().enumerate().take(totals.len()) {
                                totals[i] += cell.as_i64().unwrap_or(0);
                            }
                        }
                    }
                    Ok(other) => {
                        return Err(RouterError::Remote(format!(
                            "owner {owner} returned non-stat MERGE SET result: {other:?}"
                        )))
                    }
                    Err(e) => {
                        return Err(RouterError::Remote(format!(
                            "owner {owner} MERGE SET failed: {e}"
                        )))
                    }
                }
            }
        }
    }

    let columns: Vec<String> = WRITE_STAT_COLUMNS.iter().map(|s| s.to_string()).collect();
    let row: Vec<serde_json::Value> = totals.iter().map(|n| serde_json::json!(n)).collect();
    Ok((columns, vec![row]))
}

/// Build a CREATE sub-query for an owner: `CREATE (n:Label {prop:val}), (m:L2 ...), ...`
/// Uses the coordinator-assigned qids by encoding them as hex in a synthetic property
/// `__qid` that the write executor can recognize and use instead of generating new ones.
fn build_create_subquery(ops: &[&CreateOp]) -> String {
    let mut parts = Vec::with_capacity(ops.len());
    for op in ops {
        let var = op.variable.as_deref().unwrap_or("_");
        let labels = if op.labels.is_empty() {
            String::new()
        } else {
            format!(":{}", op.labels.join(":"))
        };
        // Inject __qid as a property so the executor uses this qid instead of generating.
        let mut props = op.properties.clone();
        props.insert("__qid".to_string(), serde_json::json!(op.qid.to_hex()));
        let props_str = if props.is_empty() {
            String::new()
        } else {
            let pairs: Vec<String> = props
                .iter()
                .map(|(k, v)| format!("{k}: {}", serde_json::to_string(v).unwrap_or_default()))
                .collect();
            format!(" {{{}}}", pairs.join(", "))
        };
        parts.push(format!("({var}{labels}{props_str})"));
    }
    format!("CREATE {}", parts.join(", "))
}

// ── AST analysis helpers ────────────────────────────────────────────────────

enum ItemKind {
    Grouping,
    Aggregate {
        func: AggFunction,
        col: String,
        distinct: bool,
    },
}

/// Classify a RETURN expression as a grouping key or an aggregate. Returns
/// `None` for expressions the distributed merge can't handle (nested
/// aggregates, aggregates over complex expressions, etc.).
fn classify_expr(expr: &Expression) -> Option<ItemKind> {
    match expr {
        Expression::Aggregation {
            function,
            expr,
            distinct,
        } => {
            // The aggregated expression must be a simple column ref or `*`
            // (represented as a variable or property access); complex arg
            // expressions aren't supported in this slice.
            let col = agg_arg_column(expr)?;
            Some(ItemKind::Aggregate {
                func: *function,
                col,
                distinct: *distinct,
            })
        }
        // Plain column references / property access are grouping keys.
        Expression::Variable(_) | Expression::Property(_, _) => Some(ItemKind::Grouping),
        // Anything else in a RETURN (arithmetic, functions, CASE) isn't
        // supported for distributed grouping/merge yet.
        _ => None,
    }
}

/// Whether an expression contains an aggregate anywhere in its tree. A pure
/// (aggregate-free) RETURN can be distributed as a plain scan: each owner
/// evaluates the projection per-row locally and the coordinator concatenates.
/// An aggregate anywhere means the merge must be aggregate-aware, so those still
/// go through [`classify_expr`]. Note that "no aggregate" is necessary but not
/// sufficient for a scan projection — the shape must also be one the owner's
/// executor can evaluate; see [`is_distributable_scan_projection`].
fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::Aggregation { .. } => true,
        Expression::Property(base, _) => contains_aggregate(base),
        Expression::List(items) => items.iter().any(contains_aggregate),
        Expression::Map(entries) => entries.iter().any(|(_, e)| contains_aggregate(e)),
        Expression::BinOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expression::UnaryOp { operand, .. } => contains_aggregate(operand),
        Expression::Function { args, .. } => args.iter().any(contains_aggregate),
        Expression::ListComprehension {
            list,
            predicate,
            projection,
            ..
        } => {
            contains_aggregate(list)
                || predicate.as_deref().is_some_and(contains_aggregate)
                || projection.as_deref().is_some_and(contains_aggregate)
        }
        Expression::Case {
            expr,
            whens,
            else_expr,
        } => {
            expr.as_deref().is_some_and(contains_aggregate)
                || whens
                    .iter()
                    .any(|(w, t)| contains_aggregate(w) || contains_aggregate(t))
                || else_expr.as_deref().is_some_and(contains_aggregate)
        }
        Expression::IsNull(e) | Expression::IsNotNull(e) | Expression::Parenthesized(e) => {
            contains_aggregate(e)
        }
        Expression::Variable(_) | Expression::Literal(_) | Expression::ExistsPattern(_) => false,
    }
}

/// Whether a (aggregate-free) RETURN item is one the owners can actually
/// evaluate, so the coordinator can concatenate per-owner rows. Owners run the
/// projection verbatim through cypher-parser, which parses column refs, property
/// access, function calls (`id(n)`, `toLower(n.name)`), and literals — but NOT
/// arithmetic operators (`n.age + 1` fails to parse). So we allow the former and
/// refuse operator expressions, keeping a filtered projection read
/// (`SELECT id, name … WHERE …` → `id(n) AS id, n.name AS name`) distributable
/// while an arithmetic projection stays on the honest 501.
fn is_distributable_scan_projection(expr: &Expression) -> bool {
    match expr {
        Expression::Variable(_) | Expression::Literal(_) => true,
        Expression::Property(base, _) => is_distributable_scan_projection(base),
        Expression::Parenthesized(e) => is_distributable_scan_projection(e),
        // Function args are themselves projections the owner must evaluate.
        Expression::Function { args, .. } => args.iter().all(is_distributable_scan_projection),
        // Arithmetic / comparison / boolean operators, CASE, comprehensions, etc.
        // are not accepted by the owner's parser in a RETURN in this slice.
        _ => false,
    }
}

/// Extract the column name an aggregate applies to: `count(*)` → "*",
/// `sum(n.age)` → "n.age", `count(n)` → "n". `None` for complex args.
fn agg_arg_column(expr: &Expression) -> Option<String> {
    match expr {
        // count(*) parses as a variable "*" or a star literal depending on the
        // grammar; accept a bare variable or property.
        Expression::Variable(v) => Some(v.clone()),
        Expression::Property(base, prop) => {
            if let Expression::Variable(v) = base.as_ref() {
                Some(format!("{v}.{prop}"))
            } else {
                None
            }
        }
        // A star literal for count(*).
        Expression::Literal(_) => Some("*".to_string()),
        _ => None,
    }
}

/// The output column name for a RETURN item (its alias, or the expression text).
fn return_item_column(item: &nexora_language::ast::ReturnItem) -> String {
    if let Some(alias) = &item.alias {
        alias.clone()
    } else {
        item.expression.to_string()
    }
}

/// Whether every part of a pattern is a bare node (no relationships / var-length
/// paths). Relationship traversal needs cross-partition joins (later slice).
fn pattern_is_node_only(pattern: &nexora_language::ast::Pattern) -> bool {
    pattern
        .parts
        .iter()
        .all(|part| part.chain.segments.len() == 1 && part.chain.segments[0].edge.is_none())
}

/// Whether a pattern is exactly one node-only segment binding one variable, i.e.
/// `MATCH (n[:L] [{props}])`. This is the precise shape the single-node write
/// executor's bulk MATCH-mutate fast path (`try_bulk_match_mutate`) engages on:
/// it filters every matching node by inline properties + WHERE and applies the
/// mutation to all of them. A multi-part or multi-variable pattern would fall
/// through that path to the generic clause loop, which binds only ONE node per
/// variable and silently under-applies a bulk write — so a distributed filtered
/// write must refuse anything but this shape. See [`plan_write`].
fn pattern_is_single_node_with_var(pattern: &nexora_language::ast::Pattern) -> bool {
    pattern.parts.len() == 1
        && pattern.parts[0].chain.segments.len() == 1
        && pattern.parts[0].chain.segments[0].edge.is_none()
        && pattern.parts[0].chain.segments[0].node.variable.is_some()
}

/// Resolve ORDER BY sort items to `(output_column_index, ascending)`. Each sort
/// expression must reference a column present in the RETURN (by alias, variable,
/// or matching expression text). Returns `None` if any can't be resolved.
fn resolve_order_by(
    order_by: Option<&OrderBy>,
    columns: &[String],
    items: &[nexora_language::ast::ReturnItem],
) -> Option<Vec<(usize, bool)>> {
    let Some(ob) = order_by else {
        return Some(Vec::new());
    };
    let mut resolved = Vec::with_capacity(ob.items.len());
    for sort in &ob.items {
        let key = sort.expression.to_string();
        // Match against output column name (alias/expr text) or the underlying
        // RETURN expression text.
        let idx = columns
            .iter()
            .position(|c| c == &key)
            .or_else(|| items.iter().position(|it| it.expression.to_string() == key))?;
        resolved.push((idx, sort.ascending));
    }
    Some(resolved)
}

/// Evaluate a SKIP/LIMIT expression to a usize. Only integer literals are
/// supported (parameters/expressions → `None` handling is caller-specific).
fn eval_usize_literal(expr: Option<&Expression>) -> Option<Option<usize>> {
    match expr {
        None => Some(None),
        Some(Expression::Literal(v)) => {
            // PropertyValue integer → usize.
            let n = property_value_as_i64(v)?;
            if n < 0 {
                return None;
            }
            Some(Some(n as usize))
        }
        // Non-literal skip/limit (parameter, expression) not supported here.
        Some(_) => None,
    }
}

fn property_value_as_i64(v: &nexora_id::PropertyValue) -> Option<i64> {
    match v {
        nexora_id::PropertyValue::Integer(i) => Some(*i),
        _ => None,
    }
}

/// Convert a literal `PropertyValue` (from a WHERE comparison RHS) to the plain
/// JSON the stage-1 rows carry, so a `RowFilter` can compare against them.
fn property_value_to_json(v: &nexora_id::PropertyValue) -> serde_json::Value {
    use nexora_id::PropertyValue as PV;
    match v {
        PV::Null => serde_json::Value::Null,
        PV::Boolean(b) => serde_json::Value::Bool(*b),
        PV::Integer(i) => serde_json::json!(i),
        PV::Float(f) => serde_json::json!(f),
        PV::String(s) => serde_json::Value::String(s.clone()),
        other => serde_json::Value::String(format!("{other:?}")),
    }
}

/// Build the query each owner runs. For a pure scan, owners run
/// `MATCH ... RETURN <items>` with the global ORDER BY/SKIP/LIMIT/DISTINCT
/// stripped (coordinator re-applies them). For aggregates, the RETURN is
/// rewritten so partials merge — e.g. `avg(x)` becomes `sum(x), count(x)` and
/// the coordinator divides. (That rewrite lives in task #17; task #16 keeps
/// count/scan working.)
fn build_owner_query(
    match_clause: &Clause,
    items: &[nexora_language::ast::ReturnItem],
    kind: &MergeKind,
) -> Option<String> {
    let match_str = match_clause.to_string();
    match kind {
        MergeKind::Concat => {
            // Owners return the projections verbatim; coordinator handles
            // DISTINCT/ORDER/SKIP/LIMIT globally.
            let ret = items
                .iter()
                .map(|it| it.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            Some(format!("{match_str} RETURN {ret}"))
        }
        MergeKind::GlobalAggregate { items: proj }
        | MergeKind::GroupedAggregate { items: proj } => {
            merge::build_partial_aggregate_return(&match_str, proj)
        }
    }
}

/// The distinct set of shard owners under the router's current map.
async fn distinct_owners(router: &HybridRouter) -> Vec<String> {
    let map = router.shard_map_snapshot().await;
    let owners: BTreeSet<String> = map.assignments.values().map(|a| a.owner.clone()).collect();
    owners.into_iter().collect()
}

/// A scatter-gather read unit: one logical owner plus an ordered list of
/// failover candidates to try if the owner is unreachable.
struct ReadTarget {
    owner: String,
    /// Replica node ids to fail over to, in priority order. Empty for
    /// `Linearizable` (owner only) or when no replica qualifies under the concern.
    failover: Vec<String>,
}

/// Build the per-owner read plan honoring `concern` (C2) and the session
/// read-after-write guard (C1).
///
/// For each distinct owner we collect the shards it owns. A replica qualifies as
/// a failover candidate for that owner only if it is a replica of **every** shard
/// the owner holds (so it can serve the owner's whole-graph sub-query without a
/// gap) AND it passes the concern's freshness gate for all those shards:
///
/// - `Linearizable`: no failover — owner only.
/// - `Local`: any common replica, no freshness gate (may be stale).
/// - `Majority`: only a replica caught up to each shard's quorum `commit_index`
///   and (when a `session` is given) the session's last-written seq — never a
///   stale read, never a read-your-own-writes violation.
///
/// With no `progress` (single-node / untracked) Majority falls back to Local
/// semantics for candidate selection, preserving prior behavior.
async fn build_read_targets(
    router: &HybridRouter,
    concern: ReadConcern,
    progress: Option<&ReplicationProgress>,
    session: Option<&SessionReadTracker>,
) -> Vec<ReadTarget> {
    let map = router.shard_map_snapshot().await;

    // owner -> the shards it owns.
    let mut owner_shards: std::collections::BTreeMap<String, Vec<crate::shard_map::ShardId>> =
        std::collections::BTreeMap::new();
    for (shard_id, asg) in &map.assignments {
        owner_shards
            .entry(asg.owner.clone())
            .or_default()
            .push(*shard_id);
    }

    let mut targets = Vec::with_capacity(owner_shards.len());
    for (owner, shards) in owner_shards {
        let failover = if concern == ReadConcern::Linearizable {
            // Owner only — a stale replica must never answer a linearizable read.
            Vec::new()
        } else {
            // Candidate replicas = nodes that are a replica of EVERY shard this
            // owner holds (can serve the whole sub-query), excluding the owner.
            let mut candidates: Option<BTreeSet<String>> = None;
            for shard_id in &shards {
                let reps: BTreeSet<String> = map
                    .assignments
                    .get(shard_id)
                    .map(|a| {
                        a.replicas
                            .iter()
                            .filter(|r| **r != owner)
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                candidates = Some(match candidates {
                    None => reps,
                    Some(acc) => acc.intersection(&reps).cloned().collect(),
                });
            }
            let candidates = candidates.unwrap_or_default();

            // Apply the concern's freshness gate (Majority only, and only when we
            // have progress to check against).
            let mut qualified = Vec::new();
            for cand in candidates {
                if concern == ReadConcern::Majority {
                    if let Some(prog) = progress {
                        if !replica_ok_for_shards(prog, &cand, &shards, session).await {
                            continue;
                        }
                    }
                }
                qualified.push(cand);
            }
            qualified
        };
        targets.push(ReadTarget { owner, failover });
    }
    targets
}

/// Whether `replica` is a safe Majority-read failover for all of `shards`: it
/// must have applied each shard's quorum `commit_index` and (if a session is
/// given) the session's last-written seq for that shard.
async fn replica_ok_for_shards(
    progress: &ReplicationProgress,
    replica: &str,
    shards: &[crate::shard_map::ShardId],
    session: Option<&SessionReadTracker>,
) -> bool {
    for shard_id in shards {
        // Session read-after-write guard (C1).
        let required_seq = session.map(|s| s.required_seq(*shard_id)).unwrap_or(0);
        if required_seq > 0
            && !progress
                .replica_caught_up(*shard_id, replica, required_seq)
                .await
        {
            return false;
        }
        // Quorum-committed-data guard (C2).
        if let Some(commit_index) = progress.get_commit_index(*shard_id).await {
            if commit_index > 0
                && !progress
                    .replica_caught_up(*shard_id, replica, commit_index)
                    .await
            {
                return false;
            }
        }
    }
    true
}
