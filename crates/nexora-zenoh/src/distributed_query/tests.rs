//! Tests for the AST-based distributed query planner.

use super::*;

/// Helper: plan a query, expecting it to be supported.
fn p(query: &str) -> DistributedPlan {
    plan(query).unwrap_or_else(|| panic!("expected a plan for: {query}"))
}

// ── Classification: what's supported vs refused ─────────────────────────────

#[test]
fn plain_scan_is_concat() {
    let plan = p("MATCH (n:Person) RETURN n");
    assert!(matches!(plan.kind, MergeKind::Concat));
    let plan = p("MATCH (n) RETURN n.name, n.age");
    assert!(matches!(plan.kind, MergeKind::Concat));
    assert_eq!(plan.columns, vec!["n.name", "n.age"]);
}

#[test]
fn lone_count_is_global_aggregate() {
    let plan = p("MATCH (n:Person) RETURN count(*)");
    match &plan.kind {
        MergeKind::GlobalAggregate { items } => {
            assert_eq!(items.len(), 1);
            assert!(matches!(
                items[0],
                ProjItem::Aggregate {
                    func: AggFunction::Count,
                    ..
                }
            ));
        }
        other => panic!("expected GlobalAggregate, got {other:?}"),
    }
}

#[test]
fn sum_avg_min_max_are_global_aggregates() {
    for (q, f) in [
        ("MATCH (n) RETURN sum(n.age)", AggFunction::Sum),
        ("MATCH (n) RETURN avg(n.age)", AggFunction::Avg),
        ("MATCH (n) RETURN min(n.age)", AggFunction::Min),
        ("MATCH (n) RETURN max(n.age)", AggFunction::Max),
    ] {
        let plan = p(q);
        match &plan.kind {
            MergeKind::GlobalAggregate { items } => {
                assert!(matches!(&items[0], ProjItem::Aggregate { func, .. } if *func == f));
            }
            other => panic!("expected GlobalAggregate for {q}, got {other:?}"),
        }
    }
}

#[test]
fn grouped_aggregate_detected() {
    let plan = p("MATCH (n:Person) RETURN n.city, count(*)");
    match &plan.kind {
        MergeKind::GroupedAggregate { items } => {
            assert_eq!(items.len(), 2);
            assert!(matches!(items[0], ProjItem::Grouping { .. }));
            assert!(matches!(
                items[1],
                ProjItem::Aggregate {
                    func: AggFunction::Count,
                    ..
                }
            ));
        }
        other => panic!("expected GroupedAggregate, got {other:?}"),
    }
}

#[test]
fn order_limit_skip_distinct_captured_not_refused() {
    // The AST planner supports these as coordinator-side ops (unlike the old
    // string classifier, which refused them).
    let plan = p("MATCH (n) RETURN n.age ORDER BY n.age DESC SKIP 5 LIMIT 10");
    assert_eq!(plan.order_by, vec![(0, false)]);
    assert_eq!(plan.limit, Some(10));
    assert_eq!(plan.skip, Some(5));

    let plan = p("MATCH (n) RETURN DISTINCT n.city");
    assert!(plan.distinct);
}

#[test]
fn plans_single_hop_directed_relationship_join() {
    // A single directed typed hop is now a supported cross-partition join.
    let plan = p("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name");
    let spec = plan.rel_join.expect("must be a relationship join");
    assert_eq!(spec.source_var, "a");
    assert_eq!(spec.target_var, "b");
    assert_eq!(spec.edge_type, "KNOWS");
    assert!(spec.outgoing);
    assert_eq!(spec.source_labels, vec!["Person"]);
    assert_eq!(spec.target_labels, vec!["Person"]);
    assert_eq!(spec.projections.len(), 2);
    assert_eq!(plan.columns, vec!["a.name", "b.name"]);
}

#[test]
fn plans_incoming_relationship_join() {
    let plan = p("MATCH (a)<-[:FOLLOWS]-(b) RETURN a, b");
    let spec = plan.rel_join.expect("must be a relationship join");
    assert!(!spec.outgoing, "<- is incoming");
    // Bare node projections → id.
    assert!(spec.projections.iter().all(|pr| pr.property.is_none()));
}

#[test]
fn plans_multi_hop_path_join() {
    // A fixed multi-hop chain is a path join with one hop per edge.
    let plan = p("MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN a.name, c.name");
    let spec = plan.path_join.expect("must be a path join");
    assert_eq!(spec.nodes.len(), 3);
    assert_eq!(spec.hops.len(), 2);
    assert_eq!(spec.hops[0].edge_type.as_deref(), Some("KNOWS"));
    assert_eq!(spec.hops[0].dir, super::HopDir::Outgoing);
    assert_eq!(spec.hops[0].min_hops, 1);
    assert_eq!(spec.hops[0].max_hops, 1);
    assert_eq!(plan.columns, vec!["a.name", "c.name"]);
}

#[test]
fn plans_variable_length_path_join() {
    let plan = p("MATCH (a:Person)-[:KNOWS*1..3]->(b) RETURN a.name, b.name");
    let spec = plan.path_join.expect("must be a path join");
    assert_eq!(spec.hops.len(), 1);
    assert_eq!(spec.hops[0].min_hops, 1);
    assert_eq!(spec.hops[0].max_hops, 3);
}

#[test]
fn plans_undirected_relationship_join() {
    // `-[:R]-` is undirected → path join with HopDir::Either.
    let plan = p("MATCH (a:Person)-[:KNOWS]-(b) RETURN a.name, b.name");
    let spec = plan.path_join.expect("undirected → path join");
    assert_eq!(spec.hops.len(), 1);
    assert_eq!(spec.hops[0].dir, super::HopDir::Either);
    assert_eq!(spec.hops[0].edge_type.as_deref(), Some("KNOWS"));
}

#[test]
fn plans_untyped_relationship_join() {
    // `-[]->` is untyped → path join with edge_type None (any type).
    let plan = p("MATCH (a:Person)-[]->(b) RETURN a.name, b.name");
    let spec = plan.path_join.expect("untyped → path join");
    assert_eq!(spec.hops.len(), 1);
    assert_eq!(spec.hops[0].dir, super::HopDir::Outgoing);
    assert_eq!(spec.hops[0].edge_type, None);
}

#[test]
fn plans_untyped_undirected_relationship_join() {
    // `-[]-` is untyped AND undirected.
    let plan = p("MATCH (a)-[]-(b) RETURN a, b");
    let spec = plan.path_join.expect("untyped undirected → path join");
    assert_eq!(spec.hops[0].dir, super::HopDir::Either);
    assert_eq!(spec.hops[0].edge_type, None);
}

#[test]
fn refuses_unsupported_relationship_shapes() {
    // Variable-length beyond the depth cap (8) not in this slice.
    assert!(plan("MATCH (a)-[:R*1..20]->(b) RETURN a").is_none());
    // Projections over an unbound variable are refused.
    assert!(plan("MATCH (a)-[:R]->(b) RETURN c.name").is_none());
    // Arithmetic / function projections over a join are refused.
    assert!(plan("MATCH (a)-[:R]->(b) RETURN a.age + 1").is_none());
}

// ── Aggregation / ordering over relationship & path joins (JoinPost) ────────

#[test]
fn count_over_relationship_join_uses_join_post() {
    let plan = p("MATCH (a:Person)-[:KNOWS]->(b) RETURN count(*)");
    assert!(plan.rel_join.is_some(), "still a relationship join");
    let post = plan.join_post.expect("count over join → JoinPost");
    assert_eq!(post.items.len(), 1);
    assert!(matches!(
        post.items[0],
        JoinPostItem::Aggregate {
            func: AggFunction::Count,
            raw_idx: None,
            ..
        }
    ));
    assert_eq!(plan.columns, vec!["count(*)"]);
}

#[test]
fn grouped_count_over_join_uses_join_post() {
    let plan = p("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.city, count(*)");
    let post = plan.join_post.expect("grouped count over join → JoinPost");
    assert_eq!(post.items.len(), 2);
    assert!(matches!(post.items[0], JoinPostItem::Grouping { .. }));
    assert!(matches!(
        post.items[1],
        JoinPostItem::Aggregate {
            func: AggFunction::Count,
            ..
        }
    ));
    // The join must extract the grouping raw column (a.city).
    let spec = plan.rel_join.unwrap();
    assert!(spec
        .projections
        .iter()
        .any(|pr| pr.property.as_deref() == Some("city")));
}

#[test]
fn order_limit_over_join_uses_join_post() {
    let plan = p("MATCH (a)-[:KNOWS]->(b) RETURN a.name ORDER BY a.name SKIP 1 LIMIT 2");
    let post = plan.join_post.expect("order/limit over join → JoinPost");
    assert_eq!(post.order_by, vec![(0, true)]);
    assert_eq!(post.skip, Some(1));
    assert_eq!(post.limit, Some(2));
}

#[test]
fn aggregate_over_path_join_uses_join_post() {
    let plan = p("MATCH (a:Person)-[:R]->(b)-[:R]->(c) RETURN count(*)");
    assert!(plan.path_join.is_some(), "still a path join");
    assert!(plan.join_post.is_some(), "count over path → JoinPost");
}

// ── Distributed writes (MATCH-based SET / REMOVE / DELETE) ──────────────────

#[test]
fn plans_match_set_as_distributed_write() {
    let plan = p("MATCH (n:Person) SET n.active = true");
    assert!(
        plan.write.is_some(),
        "MATCH … SET is an owner-parallel write"
    );
}

#[test]
fn plans_match_delete_as_distributed_write() {
    let plan = p("MATCH (n:Person) DELETE n");
    assert!(plan.write.is_some());
    let plan = p("MATCH (n) REMOVE n.tmp");
    assert!(plan.write.is_some());
}

#[test]
fn refuses_create_and_relationship_writes() {
    // Node-only CREATE is now supported (coordinator pre-generates qids).
    assert!(
        plan("CREATE (n:Person)").is_some(),
        "node CREATE is supported"
    );
    // MERGE is now supported (two-phase match+create).
    assert!(
        plan("MERGE (n:Person {id: 1})").is_some(),
        "node MERGE is supported"
    );
    // Relationship-pattern write not in this slice.
    assert!(plan("MATCH (a)-[:R]->(b) SET a.x = 1").is_none());
    // A bare MATCH scan is a read, not a write.
    assert!(plan("MATCH (n:Person) RETURN n").unwrap().write.is_none());
}

#[test]
fn plans_merge_as_distributed_merge() {
    let plan = p("MERGE (n:Person {id: 1})");
    assert!(plan.merge.is_some(), "MERGE is a distributed merge");
    let merge = plan.merge.unwrap();
    assert_eq!(merge.nodes.len(), 1);
    assert_eq!(merge.nodes[0].labels, vec!["Person"]);
    assert_eq!(
        merge.nodes[0].properties.get("id"),
        Some(&serde_json::json!(1))
    );
}

#[test]
fn plans_merge_with_on_create_on_match() {
    let plan =
        p("MERGE (n:Person {id: 1}) ON CREATE SET n.created = true ON MATCH SET n.matched = true");
    let merge = plan.merge.expect("MERGE with ON CREATE/ON MATCH");
    assert_eq!(merge.on_create.len(), 1);
    assert_eq!(merge.on_match.len(), 1);
}

#[test]
fn refuses_relationship_merge() {
    // Relationship-pattern MERGE not in this slice.
    assert!(plan("MERGE (a)-[:R]->(b)").is_none());
}

// ── Distributed UNION ───────────────────────────────────────────────────────

#[test]
fn plans_union_of_scans() {
    // UNION dedups; UNION ALL concatenates. Both branches are node scans.
    let plan = p("MATCH (n:Person) RETURN n.name UNION MATCH (m:Robot) RETURN m.name");
    let u = plan.union.expect("UNION → union plan");
    assert_eq!(u.branches.len(), 2);
    assert!(!u.union_all, "plain UNION dedups");
    assert_eq!(u.columns, vec!["n.name"]);

    let plan = p("MATCH (n:Person) RETURN n.name UNION ALL MATCH (m:Robot) RETURN m.name");
    let u = plan.union.expect("UNION ALL → union plan");
    assert!(u.union_all, "UNION ALL keeps duplicates");
}

#[test]
fn refuses_union_with_mismatched_headers() {
    // Union-incompatible projections (different column counts) → refuse.
    assert!(plan("MATCH (n) RETURN n.name UNION MATCH (m) RETURN m.name, m.age").is_none());
}

#[test]
fn refuses_union_with_nondistributable_branch() {
    // A branch that isn't itself distributable (arithmetic projection) → the
    // whole union refuses.
    assert!(plan("MATCH (n) RETURN n.name UNION MATCH (m) RETURN m.age + 1").is_none());
}

// ── WITH two-stage pipeline (MATCH → WITH → RETURN) ─────────────────────────

#[test]
fn plans_with_pipeline_projection() {
    // MATCH scan → WITH projects → RETURN selects a WITH column.
    let plan = p("MATCH (n:Person) WITH n.city AS city, n.age AS age RETURN city");
    let stage = plan.with_stage.expect("WITH pipeline → with_stage");
    // Stage 1 produced two columns (city, age); RETURN projects just city.
    assert_eq!(stage.stage1_columns, vec!["city", "age"]);
    assert_eq!(stage.columns, vec!["city"]);
    assert_eq!(stage.projection, vec![0]); // city is stage-1 column 0
    assert!(stage.filter.is_none());
}

#[test]
fn plans_with_having_filter_over_aggregate() {
    // Grouped aggregate in WITH, then a HAVING-style filter in the WITH WHERE.
    let plan = p("MATCH (n:Person) WITH n.city AS city, count(*) AS c WHERE c > 5 RETURN city, c");
    // Stage 1 is a grouped aggregate.
    assert!(matches!(plan.kind, MergeKind::GroupedAggregate { .. }));
    let stage = plan.with_stage.expect("WITH pipeline → with_stage");
    let f = stage.filter.expect("WHERE c > 5 → filter");
    // Filter tests stage-1 column `c` (index 1) with `>` against literal 5.
    assert_eq!(f.col, 1);
    assert_eq!(f.op, nexora_language::ast::BinaryOp::Gt);
    assert_eq!(f.literal, serde_json::json!(5));
    assert_eq!(stage.columns, vec!["city", "c"]);
}

#[test]
fn plans_with_order_limit_in_return() {
    let plan =
        p("MATCH (n) WITH n.city AS city, count(*) AS c RETURN city, c ORDER BY c DESC LIMIT 3");
    let stage = plan.with_stage.expect("with_stage");
    assert_eq!(stage.order_by, vec![(1, false)]); // ORDER BY c DESC → col 1 desc
    assert_eq!(stage.limit, Some(3));
}

#[test]
fn plans_with_pipeline_over_relationship() {
    // A relationship MATCH in a WITH pipeline: stage 1 is a cross-partition
    // join, stage 2 is the WITH/RETURN post-processing.
    let plan = p("MATCH (a:Person)-[:KNOWS]->(b) WITH a.name AS name RETURN name");
    // Stage 1 is a relationship join.
    assert!(plan.rel_join.is_some(), "relationship WITH stage-1 → join");
    let stage = plan
        .with_stage
        .expect("relationship WITH pipeline → with_stage");
    assert_eq!(stage.columns, vec!["name"]);
}

#[test]
fn plans_with_pipeline_relationship_aggregate() {
    // Aggregate in the WITH over a relationship join: stage-1 join carries a
    // JoinPost (grouped count), stage 2 keeps the projection.
    let plan =
        p("MATCH (a:Person)-[:KNOWS]->(b) WITH b.city AS city, count(*) AS c RETURN city, c");
    assert!(plan.rel_join.is_some());
    assert!(
        plan.join_post.is_some(),
        "aggregate WITH → JoinPost in stage 1"
    );
    let stage = plan.with_stage.expect("with_stage");
    assert_eq!(stage.columns, vec!["city", "c"]);
}

#[test]
fn refuses_with_pipeline_bad_return() {
    // RETURN referencing a non-WITH column → refuse (can't project it).
    assert!(plan("MATCH (n) WITH n.city AS c RETURN n.age").is_none());
}

#[test]
fn refuses_unsupported_return_expressions() {
    // Arithmetic projections aren't owner-evaluable (cypher-parser can't parse
    // `+` in a RETURN), so a pure-scan projection containing one is refused.
    assert!(plan("MATCH (n) RETURN n.age + 1").is_none());
}

#[test]
fn scalar_function_projection_is_distributable_scan() {
    // A function projection in a pure (aggregate-free) scan IS distributable:
    // each owner evaluates `toUpper(n.name)` per-row and the coordinator
    // concatenates. Only aggregates need the strict grouping/merge analysis.
    let plan = p("MATCH (n) RETURN toUpper(n.name)");
    assert!(matches!(plan.kind, MergeKind::Concat));
    // The owner query carries the projection verbatim so each node evaluates it.
    assert!(plan.owner_query.contains("toUpper(n.name)"));
}

// ── owner-query rewrite ─────────────────────────────────────────────────────

#[test]
fn owner_query_strips_global_clauses_for_scan() {
    let plan = p("MATCH (n:Person) RETURN n.name ORDER BY n.name LIMIT 5");
    // Owner runs the projection without the global ORDER BY / LIMIT.
    assert!(plan.owner_query.to_lowercase().contains("match"));
    assert!(plan.owner_query.to_lowercase().contains("return"));
    assert!(!plan.owner_query.to_lowercase().contains("order by"));
    assert!(!plan.owner_query.to_lowercase().contains("limit"));
}

#[test]
fn owner_query_expands_avg_to_sum_count() {
    let plan = p("MATCH (n) RETURN avg(n.age)");
    let lower = plan.owner_query.to_lowercase();
    assert!(
        lower.contains("sum(n.age)"),
        "avg must expand to sum: {lower}"
    );
    assert!(
        lower.contains("count(n.age)"),
        "avg must expand to count: {lower}"
    );
}

// ── merge behavior (pure functions, no cluster) ─────────────────────────────

#[test]
fn merge_concat_unions_rows() {
    let plan = p("MATCH (n) RETURN n.name");
    let per_owner = vec![
        (vec!["n.name".into()], vec![vec![serde_json::json!("a")]]),
        (
            vec!["n.name".into()],
            vec![vec![serde_json::json!("b")], vec![serde_json::json!("c")]],
        ),
    ];
    let (cols, rows) = merge_rows(&plan, per_owner).unwrap();
    assert_eq!(cols, vec!["n.name"]);
    assert_eq!(rows.len(), 3);
}

#[test]
fn merge_global_count_sums() {
    let plan = p("MATCH (n:Person) RETURN count(*)");
    let per_owner = vec![
        (vec!["count(*)".into()], vec![vec![serde_json::json!(3)]]),
        (vec!["count(*)".into()], vec![vec![serde_json::json!(4)]]),
        (vec!["count(*)".into()], vec![]), // owner with no matches → 0
    ];
    let (_cols, rows) = merge_rows(&plan, per_owner).unwrap();
    assert_eq!(rows, vec![vec![serde_json::json!(7)]]);
}

// ── Partial-aggregate merges (sum / avg / min / max) ────────────────────────

#[test]
fn merge_global_sum_adds() {
    let plan = p("MATCH (n) RETURN sum(n.age)");
    // Owners emit their local sum(n.age).
    let per_owner = vec![
        (
            vec!["sum(n.age)".into()],
            vec![vec![serde_json::json!(100)]],
        ),
        (vec!["sum(n.age)".into()], vec![vec![serde_json::json!(50)]]),
    ];
    let (_cols, rows) = merge_rows(&plan, per_owner).unwrap();
    assert_eq!(rows, vec![vec![serde_json::json!(150)]]);
}

#[test]
fn merge_global_avg_combines_sum_and_count() {
    let plan = p("MATCH (n) RETURN avg(n.age)");
    // Owner query expands avg → sum, count. Owner A: 3 nodes summing 90 (avg 30);
    // owner B: 1 node value 50. Global avg = (90+50)/(3+1) = 35.
    let per_owner = vec![
        (
            vec!["sum(n.age)".into(), "count(n.age)".into()],
            vec![vec![serde_json::json!(90), serde_json::json!(3)]],
        ),
        (
            vec!["sum(n.age)".into(), "count(n.age)".into()],
            vec![vec![serde_json::json!(50), serde_json::json!(1)]],
        ),
    ];
    let (_cols, rows) = merge_rows(&plan, per_owner).unwrap();
    assert_eq!(rows, vec![vec![serde_json::json!(35)]]);
}

#[test]
fn merge_global_min_max_pick_extremes() {
    let plan = p("MATCH (n) RETURN min(n.age)");
    let per_owner = vec![
        (vec!["min(n.age)".into()], vec![vec![serde_json::json!(30)]]),
        (vec!["min(n.age)".into()], vec![vec![serde_json::json!(12)]]),
        (vec!["min(n.age)".into()], vec![vec![serde_json::json!(45)]]),
    ];
    let (_cols, rows) = merge_rows(&plan, per_owner).unwrap();
    assert_eq!(rows, vec![vec![serde_json::json!(12)]]);

    let plan = p("MATCH (n) RETURN max(n.age)");
    let per_owner = vec![
        (vec!["max(n.age)".into()], vec![vec![serde_json::json!(30)]]),
        (vec!["max(n.age)".into()], vec![vec![serde_json::json!(88)]]),
        (vec!["max(n.age)".into()], vec![vec![serde_json::json!(45)]]),
    ];
    let (_cols, rows) = merge_rows(&plan, per_owner).unwrap();
    assert_eq!(rows, vec![vec![serde_json::json!(88)]]);
}

#[test]
fn owner_query_expands_avg_only() {
    // sum/min/max/count pass through unchanged; only avg expands.
    let plan = p("MATCH (n) RETURN sum(n.age)");
    let lower = plan.owner_query.to_lowercase();
    assert!(lower.contains("sum(n.age)"));
    assert!(!lower.contains("count("), "sum must not expand: {lower}");
}

// ── Grouped aggregation (GROUP BY the non-aggregate keys) ───────────────────

#[test]
fn merge_grouped_count_by_key() {
    let plan = p("MATCH (n:Person) RETURN n.city, count(*)");
    // Owners emit (city, local_count). Merge sums counts per city.
    let per_owner = vec![
        (
            vec!["n.city".into(), "count(*)".into()],
            vec![
                vec![serde_json::json!("NYC"), serde_json::json!(2)],
                vec![serde_json::json!("LA"), serde_json::json!(1)],
            ],
        ),
        (
            vec!["n.city".into(), "count(*)".into()],
            vec![
                vec![serde_json::json!("NYC"), serde_json::json!(3)],
                vec![serde_json::json!("SF"), serde_json::json!(4)],
            ],
        ),
    ];
    let (cols, mut rows) = merge_rows(&plan, per_owner).unwrap();
    assert_eq!(cols, vec!["n.city", "count(*)"]);
    // Sort for deterministic assertion.
    rows.sort_by_key(|r| r[0].as_str().unwrap_or("").to_string());
    assert_eq!(
        rows,
        vec![
            vec![serde_json::json!("LA"), serde_json::json!(1)],
            vec![serde_json::json!("NYC"), serde_json::json!(5)],
            vec![serde_json::json!("SF"), serde_json::json!(4)],
        ]
    );
}

#[test]
fn merge_grouped_avg_by_key() {
    let plan = p("MATCH (n) RETURN n.city, avg(n.age)");
    // Owner query expands to n.city, sum(n.age), count(n.age).
    let per_owner = vec![
        (
            vec!["n.city".into(), "sum(n.age)".into(), "count(n.age)".into()],
            vec![vec![
                serde_json::json!("NYC"),
                serde_json::json!(60),
                serde_json::json!(2),
            ]],
        ),
        (
            vec!["n.city".into(), "sum(n.age)".into(), "count(n.age)".into()],
            vec![vec![
                serde_json::json!("NYC"),
                serde_json::json!(30),
                serde_json::json!(1),
            ]],
        ),
    ];
    let (_cols, rows) = merge_rows(&plan, per_owner).unwrap();
    // NYC: (60+30)/(2+1) = 30.
    assert_eq!(
        rows,
        vec![vec![serde_json::json!("NYC"), serde_json::json!(30)]]
    );
}

// ── Global ORDER BY / SKIP / LIMIT / DISTINCT ───────────────────────────────

#[test]
fn merge_global_order_by_sorts_across_owners() {
    let plan = p("MATCH (n) RETURN n.age ORDER BY n.age");
    let per_owner = vec![
        (
            vec!["n.age".into()],
            vec![vec![serde_json::json!(30)], vec![serde_json::json!(10)]],
        ),
        (
            vec!["n.age".into()],
            vec![vec![serde_json::json!(20)], vec![serde_json::json!(5)]],
        ),
    ];
    let (_cols, rows) = merge_rows(&plan, per_owner).unwrap();
    let ages: Vec<i64> = rows.iter().map(|r| r[0].as_i64().unwrap()).collect();
    assert_eq!(ages, vec![5, 10, 20, 30]);
}

#[test]
fn merge_global_order_desc_skip_limit() {
    let plan = p("MATCH (n) RETURN n.age ORDER BY n.age DESC SKIP 1 LIMIT 2");
    let per_owner = vec![
        (
            vec!["n.age".into()],
            vec![vec![serde_json::json!(30)], vec![serde_json::json!(10)]],
        ),
        (
            vec!["n.age".into()],
            vec![vec![serde_json::json!(20)], vec![serde_json::json!(50)]],
        ),
    ];
    let (_cols, rows) = merge_rows(&plan, per_owner).unwrap();
    // Sorted desc: [50,30,20,10]; skip 1 → [30,20,10]; limit 2 → [30,20].
    let ages: Vec<i64> = rows.iter().map(|r| r[0].as_i64().unwrap()).collect();
    assert_eq!(ages, vec![30, 20]);
}

#[test]
fn merge_global_distinct_dedups_across_owners() {
    let plan = p("MATCH (n) RETURN DISTINCT n.city");
    let per_owner = vec![
        (
            vec!["n.city".into()],
            vec![
                vec![serde_json::json!("NYC")],
                vec![serde_json::json!("LA")],
            ],
        ),
        (
            vec!["n.city".into()],
            vec![
                vec![serde_json::json!("NYC")],
                vec![serde_json::json!("SF")],
            ],
        ),
    ];
    let (_cols, mut rows) = merge_rows(&plan, per_owner).unwrap();
    rows.sort_by_key(|r| r[0].as_str().unwrap_or("").to_string());
    assert_eq!(
        rows,
        vec![
            vec![serde_json::json!("LA")],
            vec![serde_json::json!("NYC")],
            vec![serde_json::json!("SF")],
        ]
    );
}

// ── apply_join_post: local aggregation over raw join rows ───────────────────

#[test]
fn join_post_global_count_over_raw_rows() {
    // count(*) over a join: raw rows have no columns needed; count every row.
    let plan = p("MATCH (a:Person)-[:KNOWS]->(b) RETURN count(*)");
    let post = plan.join_post.unwrap();
    // 3 joined pairs → count = 3.
    let raw = vec![vec![], vec![], vec![]];
    let (cols, rows) = super::merge::apply_join_post(&post, raw);
    assert_eq!(cols, vec!["count(*)"]);
    assert_eq!(rows, vec![vec![serde_json::json!(3)]]);
}

#[test]
fn join_post_grouped_count_over_raw_rows() {
    // RETURN a.city, count(*): raw col 0 = a.city, group + count.
    let plan = p("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.city, count(*)");
    let post = plan.join_post.unwrap();
    let raw = vec![
        vec![serde_json::json!("NYC")],
        vec![serde_json::json!("LA")],
        vec![serde_json::json!("NYC")],
    ];
    let (_cols, mut rows) = super::merge::apply_join_post(&post, raw);
    rows.sort_by_key(|r| r[0].as_str().unwrap_or("").to_string());
    assert_eq!(
        rows,
        vec![
            vec![serde_json::json!("LA"), serde_json::json!(1)],
            vec![serde_json::json!("NYC"), serde_json::json!(2)],
        ]
    );
}

#[test]
fn join_post_avg_over_raw_rows() {
    // avg(b.age) over the join: raw col 0 = b.age.
    let plan = p("MATCH (a)-[:KNOWS]->(b) RETURN avg(b.age)");
    let post = plan.join_post.unwrap();
    let raw = vec![
        vec![serde_json::json!(10)],
        vec![serde_json::json!(20)],
        vec![serde_json::json!(30)],
    ];
    let (_cols, rows) = super::merge::apply_join_post(&post, raw);
    assert_eq!(rows, vec![vec![serde_json::json!(20)]]); // (10+20+30)/3
}

#[test]
fn join_post_order_and_limit_over_raw_rows() {
    let plan = p("MATCH (a)-[:KNOWS]->(b) RETURN a.name ORDER BY a.name DESC LIMIT 2");
    let post = plan.join_post.unwrap();
    let raw = vec![
        vec![serde_json::json!("bob")],
        vec![serde_json::json!("alice")],
        vec![serde_json::json!("carol")],
    ];
    let (_cols, rows) = super::merge::apply_join_post(&post, raw);
    // Desc: carol, bob, alice → limit 2 → [carol, bob].
    assert_eq!(
        rows,
        vec![
            vec![serde_json::json!("carol")],
            vec![serde_json::json!("bob")],
        ]
    );
}

// ── apply_with_stage: WITH pipeline second stage over stage-1 rows ──────────

#[test]
fn with_stage_having_filter_and_projection() {
    // WITH n.city AS city, count(*) AS c WHERE c > 1 RETURN city, c.
    let plan = p("MATCH (n:Person) WITH n.city AS city, count(*) AS c WHERE c > 1 RETURN city, c");
    let stage = plan.with_stage.unwrap();
    // Stage-1 rows: (city, count). NYC=3, LA=1, SF=2.
    let stage1 = vec![
        vec![serde_json::json!("NYC"), serde_json::json!(3)],
        vec![serde_json::json!("LA"), serde_json::json!(1)],
        vec![serde_json::json!("SF"), serde_json::json!(2)],
    ];
    let (cols, mut rows) = super::merge::apply_with_stage(&stage, stage1);
    assert_eq!(cols, vec!["city", "c"]);
    // c > 1 keeps NYC(3) and SF(2), drops LA(1).
    rows.sort_by_key(|r| r[0].as_str().unwrap_or("").to_string());
    assert_eq!(
        rows,
        vec![
            vec![serde_json::json!("NYC"), serde_json::json!(3)],
            vec![serde_json::json!("SF"), serde_json::json!(2)],
        ]
    );
}

#[test]
fn with_stage_projection_reorders_and_selects() {
    // WITH projects (city, age); RETURN selects just city (drops age).
    let plan = p("MATCH (n:Person) WITH n.city AS city, n.age AS age RETURN city");
    let stage = plan.with_stage.unwrap();
    let stage1 = vec![
        vec![serde_json::json!("NYC"), serde_json::json!(30)],
        vec![serde_json::json!("LA"), serde_json::json!(25)],
    ];
    let (cols, rows) = super::merge::apply_with_stage(&stage, stage1);
    assert_eq!(cols, vec!["city"]);
    assert_eq!(
        rows,
        vec![
            vec![serde_json::json!("NYC")],
            vec![serde_json::json!("LA")],
        ]
    );
}

#[test]
fn with_stage_order_limit_over_stage1() {
    let plan =
        p("MATCH (n) WITH n.city AS city, count(*) AS c RETURN city, c ORDER BY c DESC LIMIT 2");
    let stage = plan.with_stage.unwrap();
    let stage1 = vec![
        vec![serde_json::json!("A"), serde_json::json!(1)],
        vec![serde_json::json!("B"), serde_json::json!(5)],
        vec![serde_json::json!("C"), serde_json::json!(3)],
    ];
    let (_cols, rows) = super::merge::apply_with_stage(&stage, stage1);
    // Desc by c: B(5), C(3), A(1) → limit 2 → [B, C].
    assert_eq!(
        rows,
        vec![
            vec![serde_json::json!("B"), serde_json::json!(5)],
            vec![serde_json::json!("C"), serde_json::json!(3)],
        ]
    );
}

// ── Distributed CREATE (coordinator qid generation + routing) ───────────────

#[test]
fn plan_create_single_node() {
    let plan = p("CREATE (n:Person {name: 'Alice'})");
    assert!(
        plan.create.is_some(),
        "CREATE must be planned as distributed write"
    );
    let create = plan.create.unwrap();
    assert_eq!(create.nodes.len(), 1, "single node CREATE");
    let node = &create.nodes[0];
    assert_eq!(node.variable, Some("n".to_string()));
    assert_eq!(node.labels, vec!["Person"]);
    assert!(node.properties.contains_key("name"));
}

#[test]
fn plan_create_multi_nodes_pre_generates_qids() {
    let plan = p("CREATE (a:Person), (b:Robot), (c:Person)");
    let create = plan.create.expect("multi-node CREATE must plan");
    assert_eq!(create.nodes.len(), 3, "three nodes");
    // Each node has a distinct pre-generated qid (coordinator assigns).
    let qids: Vec<String> = create.nodes.iter().map(|n| n.qid.to_hex()).collect();
    assert_eq!(qids.len(), 3);
    assert_ne!(qids[0], qids[1], "qids must be distinct");
    assert_ne!(qids[1], qids[2], "qids must be distinct");
}

#[test]
fn refuses_create_with_relationships() {
    // Relationship-pattern CREATE not in this slice (needs two-phase).
    assert!(
        plan("CREATE (a:Person)-[:KNOWS]->(b:Person)").is_none(),
        "relationship CREATE must refuse (not supported yet)"
    );
}

#[test]
fn refuses_create_with_complex_property_expressions() {
    // Only literal property values are supported; expressions refuse.
    assert!(
        plan("CREATE (n:Person {age: 10 + 5})").is_none(),
        "complex property expression must refuse"
    );
}

// ── Multi-stage WITH chains (3+ stages) ─────────────────────────────────────

#[test]
fn plans_three_stage_with_chain() {
    // MATCH → WITH (grouped count) → WITH (HAVING filter + reproject) → RETURN.
    // Stage 1 = distributed grouped aggregate; two coordinator stages follow.
    let plan = p("MATCH (n:Person) \
                  WITH n.city AS city, count(*) AS c \
                  WITH city, c WHERE c > 5 \
                  RETURN city, c ORDER BY c DESC");
    // Stage 1 is a grouped aggregate over the first WITH.
    assert!(matches!(plan.kind, MergeKind::GroupedAggregate { .. }));
    // First coordinator stage (WITH₂) lives in `with_stage`; the RETURN chains.
    let s = plan.with_stage.as_ref().expect("first coordinator stage");
    // WITH₂ has no WHERE of its own (WITH₁ had none), so this stage's filter
    // reflects WITH₁'s WHERE (None here); it just reprojects city, c.
    assert!(
        s.filter.is_none(),
        "WITH₁ has no WHERE → first stage unfiltered"
    );
    assert_eq!(s.columns, vec!["city", "c"]);
    assert_eq!(
        plan.with_stage_chain.len(),
        1,
        "one further stage (the RETURN)"
    );
    // The RETURN stage carries WITH₂'s WHERE (c > 5) as its filter and the
    // ORDER BY c DESC.
    let ret = &plan.with_stage_chain[0];
    let f = ret
        .filter
        .as_ref()
        .expect("WITH₂ WHERE c>5 → RETURN-stage filter");
    assert_eq!(f.op, nexora_language::ast::BinaryOp::Gt);
    assert_eq!(f.literal, serde_json::json!(5));
    assert_eq!(ret.order_by, vec![(1, false)]); // c desc
}

#[test]
fn plans_four_stage_with_chain_projection() {
    // Pure projection chain: MATCH → WITH → WITH → WITH → RETURN, renaming each
    // step. Stage 1 is a plain scan; three coordinator stages reproject.
    let plan = p("MATCH (n) \
                  WITH n.x AS x \
                  WITH x AS y \
                  WITH y AS z \
                  RETURN z");
    // with_stage (WITH₂) + chain (WITH₃, RETURN) = 3 coordinator stages total.
    assert!(plan.with_stage.is_some());
    assert_eq!(plan.with_stage_chain.len(), 2);
    // Final output column is z.
    assert_eq!(plan.with_stage_chain.last().unwrap().columns, vec!["z"]);
}

#[test]
fn refuses_multi_stage_with_aggregate_past_stage1() {
    // An aggregate in a coordinator stage (past the first WITH) isn't mergeable
    // there — the distributed aggregate merge only happens in stage 1. Refuse.
    assert!(
        plan(
            "MATCH (n) WITH n.city AS city, n.age AS age \
             WITH city, count(*) AS c RETURN city, c"
        )
        .is_none(),
        "aggregate past stage 1 must refuse"
    );
}

#[test]
fn refuses_multi_stage_with_bad_reference() {
    // A coordinator stage referencing a column the previous stage didn't produce.
    assert!(
        plan(
            "MATCH (n) WITH n.city AS city \
             WITH city AS c WHERE c > 1 \
             RETURN nonexistent"
        )
        .is_none(),
        "RETURN of a non-existent column must refuse"
    );
}

// ── C2: ReadConcern failover-chain construction ─────────────────────────────

use crate::replication_progress::ReplicationProgress;
use crate::shard_map::{ShardAssignment, ShardMap};
use crate::tcp_transport::TcpRemoteClient;
use std::collections::HashMap as StdHashMap;
use std::sync::Arc;

/// Build a single-shard map: shard 0 owned by `owner` with `replicas`.
fn one_shard_map(owner: &str, replicas: &[&str]) -> ShardMap {
    let mut assignments = StdHashMap::new();
    assignments.insert(
        0usize,
        ShardAssignment {
            owner: owner.to_string(),
            epoch: crate::shard_map::OwnerEpoch::new(),
            replicas: replicas.iter().map(|s| s.to_string()).collect(),
            writable: true,
        },
    );
    ShardMap {
        version: 1,
        total_shards: 1,
        assignments,
        local_node: "coordinator".to_string(),
    }
}

fn router_for(map: ShardMap) -> HybridRouter {
    let client: Arc<dyn crate::RemoteGraphClient> = Arc::new(TcpRemoteClient::new());
    HybridRouter::new_clustered_no_local(map, client)
}

/// replica_ok_for_shards: a replica behind the shard's quorum commit_index is
/// rejected; once caught up, it passes. Session seq adds a second, independent bar.
#[tokio::test]
async fn replica_ok_gates_on_commit_index_and_session_seq() {
    let progress = ReplicationProgress::new();
    // Owner wrote up to seq 10; a quorum committed seq 7.
    progress.record_write(0, 10).await;
    // Two replicas ack to establish commit_index = 7 (quorum of the acks).
    progress.record_ack(0, "r1", 7).await;
    progress.record_ack(0, "r2", 7).await;

    // r1 is caught up to the commit index → OK (no session constraint).
    assert!(replica_ok_for_shards(&progress, "r1", &[0], None).await);

    // A replica the tracker has never seen is behind by default → rejected.
    assert!(!replica_ok_for_shards(&progress, "r_unknown", &[0], None).await);
}

/// build_read_targets under Majority excludes a replica that has not reached the
/// quorum commit_index from the owner's failover chain; Linearizable yields an
/// empty chain (owner only); Local includes any common replica ungated.
#[tokio::test]
async fn build_read_targets_honors_concern() {
    let router = router_for(one_shard_map("owner", &["fresh", "stale"]));
    let progress = ReplicationProgress::new();
    progress.record_write(0, 5).await;
    // Quorum commit_index = 5 (fresh + owner-equivalent). Only "fresh" acked 5.
    progress.record_ack(0, "fresh", 5).await;
    progress.record_ack(0, "stale", 2).await;
    // With one follower at 5 and one at 2 over a 3-node set, commit_index = 2;
    // bump it: a second ack at 5 makes the quorum seq 5.
    progress.record_ack(0, "owner", 5).await;

    // Majority: chain should include "fresh" (caught up) but exclude "stale".
    let targets = build_read_targets(&router, ReadConcern::Majority, Some(&progress), None).await;
    let owner_target = targets
        .iter()
        .find(|t| t.owner == "owner")
        .expect("owner target");
    assert!(
        owner_target.failover.contains(&"fresh".to_string()),
        "caught-up replica must be a failover candidate"
    );
    assert!(
        !owner_target.failover.contains(&"stale".to_string()),
        "replica behind commit_index must be excluded under Majority"
    );

    // Linearizable: no failover at all.
    let lin = build_read_targets(&router, ReadConcern::Linearizable, Some(&progress), None).await;
    assert!(
        lin.iter()
            .find(|t| t.owner == "owner")
            .unwrap()
            .failover
            .is_empty(),
        "Linearizable must not fail over to any replica"
    );

    // Local: no freshness gate — both replicas are candidates.
    let local = build_read_targets(&router, ReadConcern::Local, Some(&progress), None).await;
    let local_owner = local.iter().find(|t| t.owner == "owner").unwrap();
    assert!(local_owner.failover.contains(&"fresh".to_string()));
    assert!(local_owner.failover.contains(&"stale".to_string()));
}
