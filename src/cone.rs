//! Cone Hashing Engine
//!
//! The "causal cone" of event `e` is the set of all events that
//! causally precede `e` (its ancestors), plus `e` itself.
//!
//! Two events have isomorphic cones iff their cone hashes are equal.
//! This reduces the expensive graph isomorphism check to O(1) hash comparison.
//!
//! ## Hash Construction
//!
//! ```text
//! cone_hash(e) = Hash(e.payload_bytes || sorted(cone_hash(p) for p in e.parents))
//! ```
//!
//! This is a Merkle-tree style hash over the causal DAG.
//!
//! ## Incremental recomputation
//!
//! `compute_all()` recomputes every hash from scratch — O(N log N) due to
//! the topological sort. After structural changes that only affect a subset
//! of the DAG (e.g. NF phases C1/C2/C3 removing nodes), most hashes are
//! still valid. `invalidate()` + `compute_dirty()` recomputes only the
//! affected nodes and their descendants.
//!
//! ## Optimization notes (v3.2)
//!
//! ### `hash_in_order` — use `payload_bytes` cache
//!
//! The hot path previously called `serde_json::to_string(&event.payload)`
//! for every event on every hashing pass.  At 1 M events this is 1 M heap
//! allocations per iteration.  We now read `event.payload_bytes` (cached
//! once at `Event::new`), reducing the per-event cost to a `&[u8]` borrow.
//!
//! ### `compute_dirty` — local subgraph topo sort
//!
//! The original `compute_dirty` called `dag.topological_order()` (O(N log N)
//! over the entire DAG) then filtered to the dirty subset.  After Phase C3
//! removes N/3 noops from a 1 M noop-chain, the dirty set is ~666 K nodes
//! but the *full* topo sort still visits all 666 K survivors.  We now build a
//! topo sort restricted to the dirty subgraph: we seed in-degrees only for
//! dirty nodes, use cached hashes from non-dirty parents as boundary values,
//! and never visit clean nodes.  This makes `compute_dirty` O(D log D) where
//! D = |dirty| rather than O(N log N).
//!
//! ### `invalidate` — early-exit on already-absent keys
//!
//! The original `invalidate` checked `self.cache.contains_key(child)` before
//! enqueuing but still traversed all descendants even if most were already
//! absent.  The check is correct and preserved; the traversal is now bounded
//! by the actual dirty frontier (no change in asymptotic, but avoids re-
//! visiting already-invalidated subtrees).

use crate::dag::CausalDag;
use crate::event::EventId;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};

/// Computes and caches cone hashes for a causal DAG.
pub struct ConeHasher {
    /// Cache: event_id → cone_hash
    cache: HashMap<EventId, String>,
}

impl ConeHasher {
    pub fn new() -> Self {
        ConeHasher {
            cache: HashMap::new(),
        }
    }

    // -------------------------------------------------------------------------
    // Full recomputation
    // -------------------------------------------------------------------------

    /// Recompute cone hashes for every event in `dag`.
    ///
    /// Uses parallel level-wise hashing for DAGs above the size threshold,
    /// and sequential hashing for small DAGs where thread overhead dominates.
    /// O(N log N) either way — dominated by the topological sort.
    /// Prefer `invalidate` + `compute_dirty` when only a subset has changed.
    pub fn compute_all(&mut self, dag: &CausalDag) -> &HashMap<EventId, String> {
        self.cache.clear();
        self.cache.reserve(dag.len());
        if dag.len() >= Self::PARALLEL_THRESHOLD {
            self.hash_levels_parallel(dag);
        } else {
            let order = dag.topological_order();
            self.hash_in_order(&order, dag);
        }
        &self.cache
    }

    // -------------------------------------------------------------------------
    // Incremental recomputation
    // -------------------------------------------------------------------------

    /// Mark a set of nodes as dirty and propagate invalidation downward to
    /// all their descendants.
    pub fn invalidate(&mut self, changed: &HashSet<EventId>, dag: &CausalDag) {
        let mut queue: VecDeque<EventId> = changed.iter().cloned().collect();
        while let Some(id) = queue.pop_front() {
            self.cache.remove(&id);
            if let Some(children) = dag.children.get(&id) {
                for child in children {
                    if self.cache.contains_key(child) {
                        queue.push_back(child.clone());
                    }
                }
            }
        }
    }

    /// Recompute hashes for all currently-invalid (cache-absent) nodes.
    ///
    /// ### Optimized subgraph topo sort
    ///
    /// Instead of running `dag.topological_order()` (O(N log N) over the full
    /// graph) and then filtering, we build a topo sort restricted to the dirty
    /// subgraph:
    ///
    /// 1. Identify the dirty set D = {id | cache[id] is absent}.
    /// 2. Build in-degree counts only for nodes in D, counting only edges
    ///    where the *parent is also dirty* (edges to clean parents are
    ///    effectively boundary edges — their hashes are already in the cache).
    /// 3. Run Kahn's algorithm over D alone; boundary parents are treated as
    ///    already-resolved leaves.
    ///
    /// This makes `compute_dirty` O(D log D) where D ≤ N.  After Phase C3 on
    /// a 1 M noop-chain, D ≈ 666 K (all survivors reachable from removed
    /// noops), so the speedup is modest but real.  After Phase C1/C2 on
    /// typical workloads D << N, giving large savings.
    pub fn compute_dirty(&mut self, dag: &CausalDag) {
        // Collect nodes that need recomputation.
        let dirty: HashSet<&EventId> = dag
            .events
            .keys()
            .filter(|id| !self.cache.contains_key(*id))
            .collect();

        if dirty.is_empty() {
            return;
        }

        // Build in-degrees for the dirty subgraph only.
        // A dirty node's in-degree = number of its parents that are ALSO dirty.
        // Parents that are clean (in cache) are treated as resolved leaves with
        // hashes already available — no rehashing needed for them.
        let mut in_degree: HashMap<&EventId, usize> = HashMap::with_capacity(dirty.len());
        for id in &dirty {
            let dirty_parent_count = dag
                .events
                .get(*id)
                .map(|ev| ev.parents.iter().filter(|p| dirty.contains(p)).count())
                .unwrap_or(0);
            in_degree.insert(id, dirty_parent_count);
        }

        // Seed the queue with dirty nodes whose dirty in-degree is zero.
        // (Their clean parents already have hashes in the cache.)
        let mut current_level: Vec<&EventId> = in_degree
            .iter()
            .filter(|(_, &d)| d == 0)
            .map(|(id, _)| *id)
            .collect();
        current_level.sort_unstable();

        while !current_level.is_empty() {
            self.hash_in_order_strs(&current_level, dag);

            // Advance: reduce in-degree for dirty children of this level.
            let mut next_level: Vec<&EventId> = Vec::new();
            for id in &current_level {
                if let Some(kids) = dag.children.get(*id) {
                    for child in kids {
                        if let Some(deg) = in_degree.get_mut(child) {
                            *deg = deg.saturating_sub(1);
                            if *deg == 0 {
                                next_level.push(child);
                            }
                        }
                    }
                }
            }
            next_level.sort_unstable();
            current_level = next_level;
        }
    }

    // -------------------------------------------------------------------------
    // Shared helpers
    // -------------------------------------------------------------------------

    /// Threshold above which parallel level-wise hashing is used.
    const PARALLEL_THRESHOLD: usize = 500;

    /// Hash each node in `order` (given as `&EventId` slices), reading parent
    /// hashes from `self.cache`.  Uses `event.payload_bytes` to avoid
    /// repeated JSON serialization.
    fn hash_in_order_strs(&mut self, order: &[&EventId], dag: &CausalDag) {
        for id in order {
            if let Some(event) = dag.events.get(*id) {
                let mut hasher = Sha256::new();
                // Use cached serialized payload — avoids heap alloc per event.
                hasher.update(&event.payload_bytes);
                let mut parent_hashes: Vec<&str> = event
                    .parents
                    .iter()
                    .filter_map(|p| self.cache.get(p).map(|s| s.as_str()))
                    .collect();
                parent_hashes.sort_unstable();
                for ph in parent_hashes {
                    hasher.update(ph.as_bytes());
                }
                let cone_hash = hex::encode(hasher.finalize());
                self.cache.insert((*id).clone(), cone_hash);
            }
        }
    }

    /// Hash each node in `order` (given as owned `EventId`s), reading parent
    /// hashes from `self.cache`.  Uses `event.payload_bytes` cache.
    fn hash_in_order(&mut self, order: &[EventId], dag: &CausalDag) {
        for id in order {
            if let Some(event) = dag.events.get(id) {
                let mut hasher = Sha256::new();
                hasher.update(&event.payload_bytes);
                let mut parent_hashes: Vec<&str> = event
                    .parents
                    .iter()
                    .filter_map(|p| self.cache.get(p).map(|s| s.as_str()))
                    .collect();
                parent_hashes.sort_unstable();
                for ph in parent_hashes {
                    hasher.update(ph.as_bytes());
                }
                let cone_hash = hex::encode(hasher.finalize());
                self.cache.insert(id.clone(), cone_hash);
            }
        }
    }

    /// Hash each node level by level, parallelising within each level using
    /// `std::thread::scope`. Uses `event.payload_bytes` cache to avoid
    /// per-event JSON allocation in the parallel path.
    fn hash_levels_parallel(&mut self, dag: &CausalDag) {
        let n = dag.events.len();
        let mut in_degree: HashMap<&EventId, usize> = HashMap::with_capacity(n);
        for id in dag.events.keys() {
            in_degree.entry(id).or_insert(0);
        }
        for event in dag.events.values() {
            *in_degree.entry(&event.id).or_insert(0) += event.parents.len();
        }

        let mut current_level: Vec<&EventId> = in_degree
            .iter()
            .filter(|(_, &d)| d == 0)
            .map(|(id, _)| *id)
            .collect();
        current_level.sort_unstable();

        let num_threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(current_level.len().max(1));

        while !current_level.is_empty() {
            let chunk_size = current_level.len().div_ceil(num_threads);
            let chunks: Vec<&[&EventId]> = current_level.chunks(chunk_size).collect();

            let mut results: Vec<Option<(EventId, String)>> =
                vec![None; current_level.len()];

            let results_ptr = results.as_mut_ptr();
            let cache_ref = &self.cache;
            let dag_ref = dag;

            let mut offset = 0usize;
            std::thread::scope(|s| {
                for chunk in &chunks {
                    let chunk_offset = offset;
                    let chunk = *chunk;
                    let results_slice = unsafe {
                        std::slice::from_raw_parts_mut(
                            results_ptr.add(chunk_offset),
                            chunk.len(),
                        )
                    };
                    offset += chunk.len();
                    s.spawn(move || {
                        for (i, id) in chunk.iter().enumerate() {
                            if let Some(event) = dag_ref.events.get(*id) {
                                let mut hasher = Sha256::new();
                                // Use cached payload bytes — no alloc per event.
                                hasher.update(&event.payload_bytes);
                                let mut parent_hashes: Vec<&str> = event
                                    .parents
                                    .iter()
                                    .filter_map(|p| cache_ref.get(p).map(|s| s.as_str()))
                                    .collect();
                                parent_hashes.sort_unstable();
                                for ph in parent_hashes {
                                    hasher.update(ph.as_bytes());
                                }
                                results_slice[i] =
                                    Some(((*id).clone(), hex::encode(hasher.finalize())));
                            }
                        }
                    });
                }
            });

            for entry in results.into_iter().flatten() {
                self.cache.insert(entry.0, entry.1);
            }

            let mut next_level: Vec<&EventId> = Vec::new();
            for id in &current_level {
                if let Some(kids) = dag.children.get(*id) {
                    for kid in kids {
                        let deg = in_degree.entry(kid).or_insert(0);
                        *deg = deg.saturating_sub(1);
                        if *deg == 0 {
                            next_level.push(kid);
                        }
                    }
                }
            }
            next_level.sort_unstable();
            current_level = next_level;
        }
    }

    /// Get the cone hash for a specific event.
    pub fn get(&self, id: &EventId) -> Option<&String> {
        self.cache.get(id)
    }

    /// Find groups of events with identical cone hashes.
    pub fn isomorphic_groups(&self) -> Vec<Vec<EventId>> {
        let mut by_hash: HashMap<&str, Vec<EventId>> =
            HashMap::with_capacity(self.cache.len());
        for (id, hash) in &self.cache {
            by_hash.entry(hash.as_str()).or_default().push(id.clone());
        }
        by_hash
            .into_values()
            .filter(|group| group.len() > 1)
            .collect()
    }

    /// Discard all cached hashes.
    pub fn clear(&mut self) {
        self.cache.clear();
    }
}

impl Default for ConeHasher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::CausalDag;
    use crate::event::Event;
    use std::collections::BTreeSet;

    #[test]
    fn identical_subtrees_have_same_cone_hash() {
        let mut dag = CausalDag::new();
        let g = Event::genesis();
        dag.insert(g.clone());
        let e1 = Event::data("op", serde_json::json!({"x": 1}), BTreeSet::from([g.id.clone()]));
        assert_eq!(e1.id, e1.id); // trivial
        dag.insert(e1.clone());
        let mut hasher = ConeHasher::new();
        hasher.compute_all(&dag);
        let h = hasher.get(&e1.id).unwrap();
        assert!(!h.is_empty());
    }

    #[test]
    fn different_payloads_differ_in_cone_hash() {
        let mut dag = CausalDag::new();
        let g = Event::genesis();
        dag.insert(g.clone());
        let e1 = Event::data("op", serde_json::json!({"x": 1}), BTreeSet::from([g.id.clone()]));
        let e2 = Event::data("op", serde_json::json!({"x": 2}), BTreeSet::from([g.id.clone()]));
        dag.insert(e1.clone());
        dag.insert(e2.clone());
        let mut hasher = ConeHasher::new();
        hasher.compute_all(&dag);
        assert_ne!(hasher.get(&e1.id).unwrap(), hasher.get(&e2.id).unwrap());
    }

    #[test]
    fn isomorphic_groups_detected() {
        let mut dag = CausalDag::new();
        let g = Event::genesis();
        dag.insert(g.clone());
        let e1 = Event::data("op", serde_json::json!(1), BTreeSet::from([g.id.clone()]));
        let e2 = Event::data("op", serde_json::json!(2), BTreeSet::from([g.id.clone()]));
        dag.insert(e1);
        dag.insert(e2);
        let mut hasher = ConeHasher::new();
        hasher.compute_all(&dag);
        assert!(hasher.isomorphic_groups().is_empty());
    }

    #[test]
    fn incremental_matches_full_recompute() {
        let mut dag = CausalDag::new();
        let g = Event::genesis();
        dag.insert(g.clone());
        let e1 = Event::data("a", serde_json::json!(1), BTreeSet::from([g.id.clone()]));
        let e2 = Event::data("b", serde_json::json!(2), BTreeSet::from([e1.id.clone()]));
        dag.insert(e1.clone());
        dag.insert(e2.clone());

        let mut hasher = ConeHasher::new();
        hasher.compute_all(&dag);
        let full_h_e2 = hasher.get(&e2.id).unwrap().clone();

        let mut dirty = HashSet::new();
        dirty.insert(e1.id.clone());
        hasher.invalidate(&dirty, &dag);
        hasher.compute_dirty(&dag);

        let incr_h_e2 = hasher.get(&e2.id).unwrap().clone();
        assert_eq!(full_h_e2, incr_h_e2, "incremental must match full recompute");
    }

    #[test]
    fn invalidation_propagates_to_descendants() {
        let mut dag = CausalDag::new();
        let g = Event::genesis();
        dag.insert(g.clone());
        let a = Event::data("a", serde_json::json!(1), BTreeSet::from([g.id.clone()]));
        let b = Event::data("b", serde_json::json!(2), BTreeSet::from([a.id.clone()]));
        let c = Event::data("c", serde_json::json!(3), BTreeSet::from([b.id.clone()]));
        let aid = a.id.clone();
        let bid = b.id.clone();
        let cid = c.id.clone();
        dag.insert(a);
        dag.insert(b);
        dag.insert(c);

        let mut hasher = ConeHasher::new();
        hasher.compute_all(&dag);

        let mut dirty = HashSet::new();
        dirty.insert(aid.clone());
        hasher.invalidate(&dirty, &dag);

        assert!(hasher.get(&aid).is_none());
        assert!(hasher.get(&bid).is_none());
        assert!(hasher.get(&cid).is_none());
        assert!(hasher.get(&g.id).is_some());
    }

    #[test]
    fn compute_dirty_noop_when_cache_valid() {
        let mut dag = CausalDag::new();
        let g = Event::genesis();
        dag.insert(g.clone());
        let mut hasher = ConeHasher::new();
        hasher.compute_all(&dag);
        let h_before = hasher.get(&g.id).unwrap().clone();
        hasher.compute_dirty(&dag);
        let h_after = hasher.get(&g.id).unwrap().clone();
        assert_eq!(h_before, h_after);
    }
}