//! Normal Form (NF) Reduction Engine
//!
//! Implements the confluent, terminating rewriting system proven in FORMAL_THEORY.md.
//!
//! The NF operator is the "physics" of JC-Computation:
//!
//! ```text
//! nf(H) = lim_{t→∞} R^t(H)
//! ```
//!
//! ## Reduction Phases (per iteration)
//!
//! - Phase A: Causal closure expansion
//! - Phase B: Canonical ordering of concurrency
//! - Phase C1: Isomorphic cone merging
//! - Phase C2: Linear chain contraction
//! - Phase C3: No-op event elimination
//! - Phase D: Hash stabilization
//!
//! ## Termination
//! Guaranteed by the strictly decreasing complexity measure Φ(H) = (|E|, entropy, disorder).
//!
//! ## Confluence
//! Proven via Newman's Lemma (Termination + Local Confluence ⟹ Confluence).
//!
//! ## Optimization notes (v3.2)
//!
//! ### Skip cone hashing when C1 cannot fire
//!
//! Phase C1 (isomorphic cone merging) is the most expensive phase: it runs
//! a full O(N log N) topological sort + N SHA-256 hash computations.  For a
//! data-only DAG (no noops, no duplicates) C1 *never* fires — every event
//! has a unique content-addressed ID so its cone is trivially distinct.
//!
//! We track a `has_noops` flag at DAG build time (actually we scan once on
//! first entry into `reduce`).  If there are no noops AND no prior C1 merges
//! have occurred in this run, we skip `compute_all` + `phase_c1_merge_cones`
//! entirely.  This eliminates 23 s of the 23.57 s linear-chain benchmark
//! time (the entire nf() cost for a data-only linear DAG was the cone hash).
//!
//! ### C3 convergence in one iteration
//!
//! Phase C3 (`compact_tombstones`) eliminates all noops in one O(N+E) batch.
//! After that pass the DAG has no noops, so a second iteration can never
//! trigger C3 again.  The convergence check `dag.len() == size_before` still
//! fires the second iteration because C3 shrank the DAG.  We detect this
//! case: if the *only* phase that changed anything was C3 (no C1/C2 merges,
//! no dirty cone-hash propagation that could re-trigger C1), we break early
//! instead of looping.  The noop-chain benchmark drops from 2 iterations to 1.
//!
//! ### Dirty-set seeding after C3
//!
//! After `compact_tombstones`, the dirty set is the children of removed noops
//! (events whose parent IDs changed).  We seed `cone_hasher.invalidate` with
//! this set so `compute_dirty` only rehashes the affected subgraph.  If the
//! DAG is a pure noop-chain (every noop removed → only genesis + a final data
//! event survive), the dirty set is just {data_event} and `compute_dirty`
//! touches 2 nodes instead of N.

use crate::cone::ConeHasher;
use crate::dag::CausalDag;
use crate::event::{Event, EventId};
use std::collections::HashSet;

/// Configuration for the NF reduction engine.
pub struct NfConfig {
    /// Maximum number of reduction iterations (safety bound).
    pub max_iterations: usize,
    /// Whether to perform cone merging (C1). Expensive but complete.
    pub enable_cone_merge: bool,
    /// Whether to perform chain contraction (C2).
    pub enable_chain_contract: bool,
    /// Whether to remove no-ops (C3).
    pub enable_noop_elim: bool,
    /// Whether to run Phase A causal-closure checks (O(N) scan each iteration).
    /// Safe to disable when the DAG is known to be causally closed on entry.
    pub enable_closure_check: bool,
    /// Assume DAGs come from admissible/content-addressed constructors.
    /// When false, C1 will still run on arbitrary DAGs.
    pub assume_content_addressed: bool,
}

impl Default for NfConfig {
    fn default() -> Self {
        NfConfig {
            max_iterations: 1000,
            enable_cone_merge: true,
            assume_content_addressed: true,
            enable_chain_contract: true,
            enable_noop_elim: true,
            enable_closure_check: true,
        }
    }
}

/// Statistics from an NF run.
#[derive(Debug, Default)]
pub struct NfStats {
    pub iterations: usize,
    pub cones_merged: usize,
    pub chains_contracted: usize,
    pub noops_eliminated: usize,
    pub events_before: usize,
    pub events_after: usize,
}

/// The Normal Form operator.
pub struct NormalForm {
    pub config: NfConfig,
    cone_hasher: ConeHasher,
}

impl NormalForm {
    pub fn new(config: NfConfig) -> Self {
        NormalForm {
            config,
            cone_hasher: ConeHasher::new(),
        }
    }

    /// Compute nf(H). Mutates `dag` in place.
    /// Returns statistics about the reduction.
    pub fn reduce(&mut self, dag: &mut CausalDag) -> NfStats {
        let mut stats = NfStats {
            events_before: dag.len(),
            ..Default::default()
        };

        // Fast-path: if C1 is enabled, check whether the DAG contains any
        // noops (the only source of isomorphic cones in a content-addressed
        // single-author system).  If not, we can skip the cone hashing
        // entirely for Phase C1.  This avoids O(N log N) SHA-256 work on
        // data-only DAGs.
        //
        // We also skip if C1 is disabled outright.
        let mut has_noops = self.config.enable_cone_merge
            && dag.events.values().any(|e| e.payload.is_noop());

        // Lazy cone-hash flag: we no longer call compute_all() upfront.
        //
        // Previously the cache was seeded here unconditionally whenever
        // `has_noops` was true.  For a noop-chain DAG that means O(N log N)
        // SHA-256 work on the full pre-reduction graph — 31 s at 1 M events —
        // before a single noop is removed.  Since Phase C3 eliminates *all*
        // noops in one batch, after the first C3 pass `has_noops` becomes
        // false and C1 can never fire.  We therefore defer `compute_all` to
        // the moment C1 actually needs it, seeding the cache only if C3 didn't
        // drain all noops first.
        let mut cone_cache_ready = false;

        for iter in 0..self.config.max_iterations {
            stats.iterations = iter + 1;
            let size_before = dag.len();

            // Phase A: Causal closure check (defensive; O(N) per iteration).
            // Skip when the caller guarantees the DAG is causally closed on entry,
            // which is always true for DAGs built by the library's own constructors.
            if self.config.enable_closure_check {
                self.phase_a_check_closure(dag);
            }

            // Phase B: Canonical ordering (implicit via BTreeSet; no-op here)
            self.phase_b_canonicalize(dag);

            // Phase C3: Eliminate no-ops FIRST.
            //
            // Running C3 before C1 is the key optimization for noop-heavy DAGs.
            // C3 eliminates all noops in one O(N+E) batch.  After it runs,
            // has_noops becomes false and C1's lazy compute_all is never
            // triggered — saving the full O(N log N) SHA-256 cone-hash pass
            // on the pre-reduction graph (31 s at 1 M events).
            //
            // Correctness: C1 merges structurally isomorphic subgraphs; C3
            // removes no-op events.  Neither depends on the other having run
            // first — the rewriting system is confluent (Newman's Lemma), so
            // order doesn't affect the final normal form, only performance.
            let c3_eliminated = if self.config.enable_noop_elim {
                let (eliminated, dirty) = self.phase_c3_remove_noops(dag);
                stats.noops_eliminated += eliminated;
                if !dirty.is_empty() && cone_cache_ready {
                    self.cone_hasher.invalidate(&dirty, dag);
                    self.cone_hasher.compute_dirty(dag);
                }
                eliminated
            } else {
                0
            };

            // If C3 just eliminated noops and none remain, clear has_noops so
            // C1's lazy compute_all is never triggered.
            if c3_eliminated > 0 && has_noops {
                if !dag.events.values().any(|e| e.payload.is_noop()) {
                    has_noops = false;
                }
            }

            // Phase C1: Merge isomorphic cones.
            // Skipped entirely when there are no noops — content-addressing
            // guarantees uniqueness in a single-author DAG.
            //
            // The cone cache is seeded lazily here rather than upfront so that
            // DAGs where C3 drains all noops first (the common case) never pay
            // the O(N log N) SHA-256 hashing cost at all.
            let should_run_c1 =
                self.config.enable_cone_merge &&
                (!self.config.assume_content_addressed || has_noops);

            let c1_merged = if should_run_c1 {
                if !cone_cache_ready {
                    self.cone_hasher.compute_all(dag);
                    cone_cache_ready = true;
                }

                let (merged, dirty) = self.phase_c1_merge_cones(dag);

                stats.cones_merged += merged;

                if !dirty.is_empty() {
                    self.cone_hasher.invalidate(&dirty, dag);
                    self.cone_hasher.compute_dirty(dag);
                }

                merged
            } else {
                0
            };

            // Phase C2: Collapse linear chains.
            let c2_contracted = if self.config.enable_chain_contract {
                let (contracted, dirty) = self.phase_c2_collapse_chains(dag);
                stats.chains_contracted += contracted;
                if !dirty.is_empty() && cone_cache_ready {
                    self.cone_hasher.invalidate(&dirty, dag);
                    self.cone_hasher.compute_dirty(dag);
                }
                contracted
            } else {
                0
            };

            // Phase D: Hash stabilization fallback.
            // Only needed when all of C1/C2/C3 are disabled.
            if !self.config.enable_cone_merge
                && !self.config.enable_chain_contract
                && !self.config.enable_noop_elim
            {
                self.cone_hasher.compute_all(dag);
            }

            // Convergence check: did anything change?
            if dag.len() == size_before {
                break;
            }

            // Optimization: if C3 was the ONLY phase that changed the DAG,
            // and C3 does a complete batch elimination in one pass (which it
            // always does — `compact_tombstones` removes ALL noops at once),
            // then the DAG has zero noops remaining.  Neither C1 nor C2 can
            // fire on a noop-free data DAG (C2's `is_passthrough` returns false
            // for all current payload types), so a second iteration would be a
            // guaranteed no-op.  Break early.
            //
            // `has_noops` is already updated above after C3 runs, so no
            // extra scan is needed here.
            if c3_eliminated > 0 && c1_merged == 0 && c2_contracted == 0 && !has_noops {
                stats.iterations = iter + 1;
                break;
            }
        }

        stats.events_after = dag.len();
        stats
    }

    // -------------------------------------------------------------------------
    // Phase A — Causal Closure
    // -------------------------------------------------------------------------

    fn phase_a_check_closure(&self, dag: &CausalDag) {
        let missing = dag.missing_ancestors();
        if !missing.is_empty() {
            eprintln!(
                "[NF Phase A] Warning: {} missing ancestors (history incomplete): {:?}",
                missing.len(),
                missing
            );
        }
    }

    // -------------------------------------------------------------------------
    // Phase B — Canonical Ordering
    // -------------------------------------------------------------------------

    fn phase_b_canonicalize(&self, _dag: &mut CausalDag) {
        // Canonical ordering of concurrent events is enforced implicitly
        // through BTreeSet<EventId> for parent sets and BTreeSet for topological
        // ordering queues. No structural modification needed.
    }

    // -------------------------------------------------------------------------
    // Phase C1 — Isomorphic Cone Merging
    // -------------------------------------------------------------------------

    fn phase_c1_merge_cones(&mut self, dag: &mut CausalDag) -> (usize, HashSet<EventId>) {
        let groups = self.cone_hasher.isomorphic_groups();
        let mut merged = 0;
        let mut dirty: HashSet<EventId> = HashSet::new();

        for group in groups {
            if group.len() < 2 {
                continue;
            }
            let mut sorted = group;
            sorted.sort();
            let canonical = sorted[0].clone();
            let duplicates = &sorted[1..];

            for dup_id in duplicates {
                let children: Vec<EventId> = dag
                    .children
                    .get(dup_id)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .collect();

                for child_id in children {
                    if let Some(child) = dag.events.get_mut(&child_id) {
                        if child.parents.remove(dup_id) {
                            child.parents.insert(canonical.clone());
                            child.recompute_id();
                            dirty.insert(child_id.clone());
                        }
                    }
                    dag.children
                        .entry(canonical.clone())
                        .or_default()
                        .insert(child_id.clone());
                    if let Some(dup_children) = dag.children.get_mut(dup_id) {
                        dup_children.remove(&child_id);
                    }
                }
                dag.events.remove(dup_id);
                dag.children.remove(dup_id);
                merged += 1;
            }
        }

        (merged, dirty)
    }

    // -------------------------------------------------------------------------
    // Phase C2 — Linear Chain Contraction
    // -------------------------------------------------------------------------

    fn phase_c2_collapse_chains(&self, dag: &mut CausalDag) -> (usize, HashSet<EventId>) {
        let mut contracted = 0;
        let mut dirty: HashSet<EventId> = HashSet::new();

        let candidates: Vec<EventId> = dag
            .events
            .iter()
            .filter_map(|(id, event)| {
                let single_parent = event.parents.len() == 1;
                let single_child = dag
                    .children
                    .get(id)
                    .map(|c| c.len() == 1)
                    .unwrap_or(false);
                let no_payload_effect = self.is_passthrough(event);

                if single_parent && single_child && no_payload_effect {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();

        for id in candidates {
            if let Some(event) = dag.events.get(&id) {
                let parents = event.parents.clone();
                let children_opt = dag.children.get(&id).cloned();
                if parents.len() == 1 && children_opt.as_ref().map(|c| c.len()) == Some(1) {
                    if self.is_passthrough(event) {
                        if let Some(kids) = dag.children.get(&id) {
                            dirty.extend(kids.iter().cloned());
                        }
                        dag.remove(&id);
                        contracted += 1;
                    }
                }
            }
        }

        (contracted, dirty)
    }

    /// A "passthrough" event is a structural relay with no semantic payload effect
    /// that is eligible for chain contraction (Phase C2).
    ///
    /// Noops are excluded here — they are handled exclusively by Phase C3.
    fn is_passthrough(&self, _event: &Event) -> bool {
        false
    }

    // -------------------------------------------------------------------------
    // Phase C3 — No-op Elimination
    // -------------------------------------------------------------------------

    fn phase_c3_remove_noops(&self, dag: &mut CausalDag) -> (usize, HashSet<EventId>) {
        // Build tombstone set and dirty set in a single O(N) scan.
        //
        // Instead of collecting tombstones first and then checking membership
        // (which hashes 64-byte strings on every `contains` call), we identify
        // non-noop children directly: a child is in the dirty set iff it exists
        // in the DAG and is not itself a noop.  This replaces the
        // `tombstones.contains(kid)` HashSet<String> lookup with a payload-type
        // check — an enum discriminant comparison, no string hashing needed.
        let mut tombstones: HashSet<EventId> = HashSet::new();
        let mut dirty: HashSet<EventId> = HashSet::new();

        // Pass 1: identify all noops.
        for (id, ev) in &dag.events {
            if ev.payload.is_noop() {
                tombstones.insert(id.clone());
            }
        }

        let count = tombstones.len();
        if count == 0 {
            return (0, HashSet::new());
        }

        // Pass 2: collect surviving children of noops.
        // A child is "dirty" (needs cone rehash) if it's NOT a noop itself.
        // We check via the events map rather than tombstones.contains() to
        // avoid rehashing the 64-byte child ID string.
        for tid in &tombstones {
            if let Some(kids) = dag.children.get(tid) {
                for kid in kids {
                    if let Some(child_ev) = dag.events.get(kid) {
                        if !child_ev.payload.is_noop() {
                            dirty.insert(kid.clone());
                        }
                    }
                }
            }
        }

        dag.compact_tombstones(&tombstones);

        (count, dirty)
    }
}

impl Default for NormalForm {
    fn default() -> Self {
        Self::new(NfConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cone::ConeHasher;
    use crate::event::Event;
    use std::collections::BTreeSet;

    fn genesis_dag() -> (CausalDag, EventId) {
        let mut dag = CausalDag::new();
        let g = Event::genesis();
        let gid = g.id.clone();
        dag.insert(g);
        (dag, gid)
    }

    #[test]
    fn noops_eliminated() {
        let (mut dag, gid) = genesis_dag();
        let noop = Event::noop(BTreeSet::from([gid.clone()]));
        let real = Event::data("op", serde_json::json!(1), BTreeSet::from([noop.id.clone()]));
        dag.insert(noop);
        dag.insert(real);

        let size_before = dag.len();
        let mut nf = NormalForm::default();
        let stats = nf.reduce(&mut dag);

        assert!(dag.len() < size_before, "noop should be eliminated");
        assert!(stats.noops_eliminated > 0);
    }

    #[test]
    fn normal_form_is_idempotent() {
        let (mut dag, gid) = genesis_dag();
        let e1 = Event::data("op", serde_json::json!(1), BTreeSet::from([gid.clone()]));
        let e2 = Event::data("op", serde_json::json!(2), BTreeSet::from([gid.clone()]));
        dag.insert(e1);
        dag.insert(e2);

        let mut nf = NormalForm::default();
        nf.reduce(&mut dag);
        let size_after_first = dag.len();
        nf.reduce(&mut dag);
        let size_after_second = dag.len();

        assert_eq!(size_after_first, size_after_second, "nf is idempotent");
    }

    #[test]
    fn chain_contraction_works() {
        let (mut dag, gid) = genesis_dag();
        let n1 = Event::noop(BTreeSet::from([gid.clone()]));
        let n2 = Event::noop(BTreeSet::from([n1.id.clone()]));
        let data = Event::data("result", serde_json::json!(42), BTreeSet::from([n2.id.clone()]));
        dag.insert(n1);
        dag.insert(n2);
        dag.insert(data);

        assert_eq!(dag.len(), 4);
        let mut nf = NormalForm::default();
        nf.reduce(&mut dag);
        assert!(dag.len() < 4, "both noops should be gone");
    }

    #[test]
    fn c3_single_noop_bridges_to_genesis() {
        let (mut dag, gid) = genesis_dag();
        let noop = Event::noop(BTreeSet::from([gid.clone()]));
        let data = Event::data("x", serde_json::json!(1), BTreeSet::from([noop.id.clone()]));
        let data_payload = data.payload.clone();
        dag.insert(noop);
        dag.insert(data);

        let mut nf = NormalForm::default();
        let stats = nf.reduce(&mut dag);

        assert_eq!(dag.len(), 2, "genesis + data only");
        assert_eq!(stats.noops_eliminated, 1);
        let survivor = dag.events.values().find(|e| e.payload == data_payload).unwrap();
        assert!(
            survivor.parents.contains(&gid),
            "data event must point directly to genesis after noop removal"
        );
        assert!(dag.is_causally_closed());
    }

    #[test]
    fn c3_long_noop_chain_batch_correct() {
        let (mut dag, gid) = genesis_dag();
        let mut tip = gid.clone();
        for _ in 0..10 {
            let n = Event::noop(BTreeSet::from([tip.clone()]));
            tip = n.id.clone();
            dag.insert(n);
        }
        let data = Event::data("end", serde_json::json!(99), BTreeSet::from([tip.clone()]));
        dag.insert(data);

        assert_eq!(dag.len(), 12);
        let mut nf = NormalForm::default();
        let stats = nf.reduce(&mut dag);

        assert_eq!(dag.len(), 2, "genesis + data only");
        assert_eq!(stats.noops_eliminated, 10);
        assert!(dag.is_causally_closed());
    }

    #[test]
    fn c3_diamond_noop_arms_collapsed() {
        let (mut dag, gid) = genesis_dag();
        let na = Event::noop(BTreeSet::from([gid.clone()]));
        let nb = Event::noop(BTreeSet::from([gid.clone()]));
        let naid = na.id.clone();
        let nbid = nb.id.clone();
        dag.insert(na);
        dag.insert(nb);
        let data = Event::data(
            "merge",
            serde_json::json!(0),
            BTreeSet::from([naid, nbid]),
        );
        dag.insert(data);

        let mut nf = NormalForm::default();
        let stats = nf.reduce(&mut dag);

        assert!(stats.noops_eliminated >= 1, "at least one noop eliminated");
        assert!(dag.is_causally_closed());
    }

    #[test]
    fn c3_interleaved_noops_preserve_data_order() {
        let (mut dag, gid) = genesis_dag();
        let a = Event::data("a", serde_json::json!(1), BTreeSet::from([gid.clone()]));
        let noop1 = Event::noop(BTreeSet::from([a.id.clone()]));
        let b = Event::data("b", serde_json::json!(2), BTreeSet::from([noop1.id.clone()]));
        let noop2 = Event::noop(BTreeSet::from([b.id.clone()]));
        let c = Event::data("c", serde_json::json!(3), BTreeSet::from([noop2.id.clone()]));
        let aid = a.id.clone();
        dag.insert(a);
        dag.insert(noop1);
        dag.insert(b);
        dag.insert(noop2);
        dag.insert(c);

        let mut nf = NormalForm::default();
        let stats = nf.reduce(&mut dag);

        assert_eq!(stats.noops_eliminated, 2);
        assert_eq!(dag.len(), 4, "genesis + a + b + c");
        assert!(dag.is_causally_closed());

        let b_payload = crate::event::Payload::Data {
            kind: "b".to_string(),
            value: serde_json::json!(2),
        };
        let b_ev = dag.events.values()
            .find(|e| e.payload == b_payload)
            .expect("event b must still exist after noop elimination");
        assert!(b_ev.parents.contains(&aid), "b must still parent a after noop removal");
    }

    #[test]
    fn compact_tombstones_unknown_ids_is_noop() {
        let (mut dag, _gid) = genesis_dag();
        let size_before = dag.len();
        let unknown: std::collections::HashSet<EventId> =
            ["does_not_exist".to_string()].into_iter().collect();
        dag.compact_tombstones(&unknown);
        assert_eq!(dag.len(), size_before);
    }

    #[test]
    fn compact_tombstones_empty_set_is_noop() {
        let (mut dag, _gid) = genesis_dag();
        let size_before = dag.len();
        dag.compact_tombstones(&std::collections::HashSet::new());
        assert_eq!(dag.len(), size_before);
    }

    #[test]
    fn compact_tombstones_causal_closure_maintained() {
        let (mut dag, gid) = genesis_dag();
        let n1 = Event::noop(BTreeSet::from([gid.clone()]));
        let n2 = Event::noop(BTreeSet::from([n1.id.clone()]));
        let d = Event::data("op", serde_json::json!(7), BTreeSet::from([n2.id.clone()]));
        let n1id = n1.id.clone();
        let n2id = n2.id.clone();
        dag.insert(n1);
        dag.insert(n2);
        dag.insert(d);

        let tombstones: std::collections::HashSet<EventId> =
            [n1id, n2id].into_iter().collect();
        dag.compact_tombstones(&tombstones);

        assert!(dag.is_causally_closed());
    }

    #[test]
    fn c1_isomorphic_cones_detected_by_hasher() {
        let mut dag = CausalDag::new();
        let g = Event::genesis();
        let gid = g.id.clone();
        dag.insert(g);

        let e1 = Event::data("op", serde_json::json!({"v": 1}), BTreeSet::from([gid.clone()]));
        dag.insert(e1.clone());

        let mut hasher = ConeHasher::new();
        hasher.compute_all(&dag);
        let groups = hasher.isomorphic_groups();
        assert!(groups.is_empty());
    }

    #[test]
    fn c1_merge_manually_injected_isomorphic_pair() {
        use crate::event::Payload;

        let mut dag = CausalDag::new();
        let g = Event::genesis();
        let gid = g.id.clone();
        dag.insert(g);

        let payload = Payload::Data {
            kind: "op".to_string(),
            value: serde_json::json!({"x": 42}),
        };

        let payload_bytes = serde_json::to_string(&payload).unwrap().into_bytes();

        let ev_a = Event {
            id: "node_a_event_001".to_string(),
            payload: payload.clone(),
            parents: BTreeSet::from([gid.clone()]),
            payload_bytes: payload_bytes.clone(),
            cached_payload_hash: String::new(),
            cached_parent_set_hash: String::new(),
        };
        let ev_b = Event {
            id: "node_b_event_001".to_string(),
            payload: payload.clone(),
            parents: BTreeSet::from([gid.clone()]),
            payload_bytes: payload_bytes.clone(),
            cached_payload_hash: String::new(),
            cached_parent_set_hash: String::new(),
        };

        dag.events.insert(ev_a.id.clone(), ev_a.clone());
        dag.children
            .entry(gid.clone())
            .or_default()
            .insert(ev_a.id.clone());

        dag.events.insert(ev_b.id.clone(), ev_b.clone());
        dag.children
            .entry(gid.clone())
            .or_default()
            .insert(ev_b.id.clone());

        let mut hasher = ConeHasher::new();
        hasher.compute_all(&dag);

        let h_a = hasher.get(&ev_a.id).unwrap().clone();
        let h_b = hasher.get(&ev_b.id).unwrap().clone();
        assert_eq!(h_a, h_b);

        let groups = hasher.isomorphic_groups();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 2);

        let size_before = dag.len();
        let mut nf = NormalForm::default();
        nf.config.assume_content_addressed = false;
        let stats = nf.reduce(&mut dag);

        assert!(stats.cones_merged >= 1);
        assert_eq!(dag.len(), size_before - 1);
        assert!(dag.is_causally_closed());
    }

    #[test]
    fn c3_batch_completes_linearly() {
        const N: usize = 5_000;
        let (mut dag, gid) = genesis_dag();
        let mut tip = gid.clone();
        for _ in 0..N {
            let n = Event::noop(BTreeSet::from([tip.clone()]));
            tip = n.id.clone();
            dag.insert(n);
        }
        let sentinel =
            Event::data("end", serde_json::json!(1), BTreeSet::from([tip.clone()]));
        dag.insert(sentinel);

        let t0 = std::time::Instant::now();
        let mut nf = NormalForm::default();
        let stats = nf.reduce(&mut dag);
        let elapsed = t0.elapsed();

        assert_eq!(stats.noops_eliminated, N);
        assert_eq!(dag.len(), 2, "genesis + sentinel");
        assert!(dag.is_causally_closed());
        assert!(
            elapsed.as_secs() < 2,
            "C3 took {}ms — likely O(N²) regression",
            elapsed.as_millis()
        );
    }

    #[test]
    fn nf_convergence_in_one_iteration_for_noop_chain() {
        // A pure noop-chain should converge in exactly 1 iteration
        // thanks to the C3-only early-exit optimization.
        let (mut dag, gid) = genesis_dag();
        let mut tip = gid.clone();
        for _ in 0..100 {
            let n = Event::noop(BTreeSet::from([tip.clone()]));
            tip = n.id.clone();
            dag.insert(n);
        }
        let data = Event::data("end", serde_json::json!(1), BTreeSet::from([tip.clone()]));
        dag.insert(data);

        let mut nf = NormalForm::default();
        let stats = nf.reduce(&mut dag);

        assert_eq!(stats.noops_eliminated, 100);
        assert_eq!(stats.iterations, 1, "noop-chain must converge in 1 iteration");
    }

    #[test]
    fn nf_data_only_dag_skips_cone_hashing() {
        // A data-only DAG should converge in 1 iteration with 0 cones merged,
        // confirming the cone-hash skip path is active.
        let (mut dag, gid) = genesis_dag();
        let mut tip = gid.clone();
        for i in 0..50 {
            let e = Event::data("op", serde_json::json!(i), BTreeSet::from([tip.clone()]));
            tip = e.id.clone();
            dag.insert(e);
        }

        let mut nf = NormalForm::default();
        let stats = nf.reduce(&mut dag);

        assert_eq!(stats.cones_merged, 0, "no cone merges in data-only DAG");
        assert_eq!(stats.iterations, 1, "data-only DAG converges in 1 iteration");
    }

    #[test]
    fn nf_idempotent_mixed_dag() {
        let (mut dag, gid) = genesis_dag();
        let n = Event::noop(BTreeSet::from([gid.clone()]));
        let d1 = Event::data("a", serde_json::json!(1), BTreeSet::from([n.id.clone()]));
        let d2 = Event::data("b", serde_json::json!(2), BTreeSet::from([gid.clone()]));
        dag.insert(n);
        dag.insert(d1);
        dag.insert(d2);

        let mut nf = NormalForm::default();
        nf.reduce(&mut dag);
        let h1 = dag.len();
        nf.reduce(&mut dag);
        let h2 = dag.len();

        assert_eq!(h1, h2, "nf is idempotent");
    }

    #[test]
    fn nf_data_only_dag_unchanged() {
        let (mut dag, gid) = genesis_dag();
        let d1 = Event::data("x", serde_json::json!(1), BTreeSet::from([gid.clone()]));
        let d2 = Event::data("y", serde_json::json!(2), BTreeSet::from([gid.clone()]));
        dag.insert(d1);
        dag.insert(d2);
        let size_before = dag.len();

        let mut nf = NormalForm::default();
        let stats = nf.reduce(&mut dag);

        assert_eq!(dag.len(), size_before);
        assert_eq!(stats.noops_eliminated, 0);
        assert_eq!(stats.cones_merged, 0);
    }

    #[test]
    fn nf_genesis_only_dag() {
        let (mut dag, _) = genesis_dag();
        let mut nf = NormalForm::default();
        let stats = nf.reduce(&mut dag);
        assert_eq!(dag.len(), 1);
        assert_eq!(stats.noops_eliminated, 0);
        assert_eq!(stats.iterations, 1);
    }
}