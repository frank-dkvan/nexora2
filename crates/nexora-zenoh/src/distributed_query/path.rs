//! Cross-partition multi-hop and variable-length path join.
//!
//! Generalizes the single-hop [`super::join`] to a chain of hops
//! (`(a)-[:R]->(b)-[:R]->(c)`) and variable-length hops (`(a)-[:R*1..3]->(b)`).
//! Each hop crosses partitions the same way a single hop does: the current
//! node's edges live on its owner, and the next node may live elsewhere. So the
//! coordinator drives a bounded breadth-first expansion, hop by hop, carrying a
//! binding for every named node along the path.
//!
//! Model: a *partial path* is the sequence of node ids matched so far, keyed by
//! path position. We start from the source scan (position 0), then for each hop
//! expand every partial path by fetching the frontier node's edges (routed to
//! its owner). A fixed hop advances exactly one edge; a variable-length hop
//! `*min..max` does a per-hop BFS from the hop's start node, emitting a partial
//! path for each reachable endpoint within `[min,max]`. Label constraints on a
//! named node filter the frontier at that position.
//!
//! Cycle-safety: within one variable-length hop we track visited nodes so a
//! cyclic graph can't loop forever; `max_hops` (capped at 8 in the planner)
//! bounds depth regardless.

use super::{HopDir, PathHop, PathJoinSpec, RelProjection};
use crate::router::HybridRouter;
use crate::RouterError;
use nexora_id::NexoraId;
use std::collections::{HashMap, HashSet};

/// A partial path: the node id matched at each path position so far.
type Partial = Vec<NexoraId>;

/// Execute a multi-hop / variable-length path join across shard owners.
pub(super) async fn execute_path_join(
    router: &HybridRouter,
    spec: &PathJoinSpec,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>), RouterError> {
    // Frontier of partial paths. Start from the source node scan (position 0),
    // filtered by the source node's labels.
    let sources = super::join::scan_node_ids(router, &spec.nodes[0].labels).await?;
    let mut partials: Vec<Partial> = sources.into_iter().map(|s| vec![s]).collect();

    // Expand one hop at a time. After hop `h`, each partial has `h+2` node ids
    // (positions 0..=h+1). Node-label constraints at the arriving position are
    // applied as a per-position filter set.
    for (h, hop) in spec.hops.iter().enumerate() {
        let arrive_pos = h + 1;
        // Precompute the allowed id set for the arriving position's labels, if any.
        let allowed: Option<HashSet<String>> = if spec.nodes[arrive_pos].labels.is_empty() {
            None
        } else {
            Some(
                super::join::scan_node_ids(router, &spec.nodes[arrive_pos].labels)
                    .await?
                    .into_iter()
                    .map(|q| q.to_hex())
                    .collect(),
            )
        };

        let mut next: Vec<Partial> = Vec::new();
        // C5: parallelize frontier expansion across partials (analogous to D1 BFS
        // optimization). Each partial's hop expansion is independent and may route
        // to a different shard owner, so fanning them out concurrently reduces the
        // serial round-trip latency that kills multi-hop queries at scale. Cap
        // concurrency to bound in-flight requests and memory.
        use futures::stream::{FuturesUnordered, StreamExt};
        const MAX_CONCURRENT_HOPS: usize = 64;

        let mut futures = FuturesUnordered::new();
        for partial in partials.iter() {
            let start = partial.last().unwrap().clone();
            let partial_clone = partial.clone();
            let router_ref = router;
            let hop_ref = hop;
            let allowed_ref = &allowed;
            futures.push(async move {
                let endpoints = expand_hop(router_ref, &start, hop_ref).await?;
                let mut extensions = Vec::new();
                for end in endpoints {
                    if let Some(allow) = allowed_ref {
                        if !allow.contains(&end.to_hex()) {
                            continue;
                        }
                    }
                    let mut ext = partial_clone.clone();
                    ext.push(end);
                    extensions.push(ext);
                }
                Ok::<_, RouterError>(extensions)
            });
            // Limit concurrency: await some before spawning more
            while futures.len() >= MAX_CONCURRENT_HOPS {
                if let Some(Ok(exts)) = futures.next().await {
                    next.extend(exts);
                }
            }
        }
        // Drain remaining
        while let Some(result) = futures.next().await {
            match result {
                Ok(exts) => next.extend(exts),
                Err(e) => return Err(e),
            }
        }
        partials = next;
        if partials.is_empty() {
            break;
        }
    }

    // Fetch properties for the projected named nodes (by their path position).
    // Map each projection's var → position in the path.
    let var_pos: HashMap<&str, usize> = spec
        .nodes
        .iter()
        .enumerate()
        .filter_map(|(i, n)| n.var.as_deref().map(|v| (v, i)))
        .collect();

    let mut needed_positions: HashSet<usize> = HashSet::new();
    for proj in &spec.projections {
        if proj.property.is_some() {
            if let Some(&pos) = var_pos.get(proj.var.as_str()) {
                needed_positions.insert(pos);
            }
        }
    }
    let mut props: HashMap<String, serde_json::Map<String, serde_json::Value>> = HashMap::new();
    let mut to_fetch: HashSet<NexoraId> = HashSet::new();
    for partial in &partials {
        for pos in &needed_positions {
            if let Some(id) = partial.get(*pos) {
                to_fetch.insert(id.clone());
            }
        }
    }
    // C5: parallelize property fetch. Same reasoning as frontier expansion —
    // each fetch routes to a different shard owner, so concurrent fetches cut
    // the serial round-trip latency. Cap concurrency to bound requests.
    use futures::stream::{FuturesUnordered, StreamExt};
    const MAX_CONCURRENT_FETCHES: usize = 64;

    let mut fetch_futures = FuturesUnordered::new();
    for id in to_fetch.iter() {
        let id_clone = id.clone();
        let router_ref = router;
        fetch_futures.push(async move {
            let p = super::join::fetch_props(router_ref, &id_clone).await?;
            Ok::<_, RouterError>((id_clone.to_hex(), p))
        });
        // Bound concurrency
        while fetch_futures.len() >= MAX_CONCURRENT_FETCHES {
            if let Some(Ok((hex, p))) = fetch_futures.next().await {
                props.insert(hex, p);
            }
        }
    }
    // Drain remaining
    while let Some(result) = fetch_futures.next().await {
        match result {
            Ok((hex, p)) => {
                props.insert(hex, p);
            }
            Err(e) => return Err(e),
        }
    }

    // Assemble output rows.
    let mut rows = Vec::with_capacity(partials.len());
    for partial in &partials {
        let row = spec
            .projections
            .iter()
            .map(|proj| project_path(proj, &var_pos, partial, &props))
            .collect();
        rows.push(row);
    }

    Ok((spec.columns.clone(), rows))
}

/// Expand one hop from `start`, returning the set of reachable endpoint ids.
/// A fixed hop is one edge; a variable-length hop does a bounded BFS emitting
/// every node reachable in `[min_hops, max_hops]` edges.
async fn expand_hop(
    router: &HybridRouter,
    start: &NexoraId,
    hop: &PathHop,
) -> Result<Vec<NexoraId>, RouterError> {
    if hop.is_fixed_single() {
        return one_edge_targets(router, start, hop).await;
    }

    // Variable-length: BFS by depth, collecting endpoints within [min,max].
    let mut endpoints: Vec<NexoraId> = Vec::new();
    let mut seen_endpoints: HashSet<String> = HashSet::new();
    // Frontier at current depth; start at depth 0 = {start}.
    let mut frontier: Vec<NexoraId> = vec![start.clone()];
    let mut visited: HashSet<String> = HashSet::new();
    visited.insert(start.to_hex());

    for depth in 1..=hop.max_hops {
        let mut next: Vec<NexoraId> = Vec::new();
        for node in &frontier {
            let targets = one_edge_targets(router, node, hop).await?;
            for t in targets {
                // Emit as an endpoint if we're within the requested depth range.
                if depth >= hop.min_hops && seen_endpoints.insert(t.to_hex()) {
                    endpoints.push(t.clone());
                }
                // Continue BFS only to not-yet-visited nodes (cycle-safe).
                if visited.insert(t.to_hex()) {
                    next.push(t);
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    Ok(endpoints)
}

/// The direct targets of a single edge of `hop`'s type/direction from `node`.
/// Honors `HopDir` (outgoing / incoming / either) and an optional edge type
/// (`None` = any type).
async fn one_edge_targets(
    router: &HybridRouter,
    node: &NexoraId,
    hop: &PathHop,
) -> Result<Vec<NexoraId>, RouterError> {
    let edges = super::join::fetch_edges(router, node, hop.edge_type.as_deref()).await?;
    Ok(edges
        .into_iter()
        .filter(|(dir_out, _)| match hop.dir {
            HopDir::Outgoing => *dir_out,
            HopDir::Incoming => !*dir_out,
            HopDir::Either => true,
        })
        .map(|(_, t)| t)
        .collect())
}

/// Project one output cell for a path row.
fn project_path(
    proj: &RelProjection,
    var_pos: &HashMap<&str, usize>,
    partial: &Partial,
    props: &HashMap<String, serde_json::Map<String, serde_json::Value>>,
) -> serde_json::Value {
    let Some(&pos) = var_pos.get(proj.var.as_str()) else {
        return serde_json::Value::Null;
    };
    let Some(id) = partial.get(pos) else {
        return serde_json::Value::Null;
    };
    match &proj.property {
        None => serde_json::Value::String(id.to_hex()),
        Some(prop) => props
            .get(&id.to_hex())
            .and_then(|m| m.get(prop))
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    }
}
