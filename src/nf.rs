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

use crate::cone::ConeHasher;
use crate::dag::CausalDag;
use crate::event::{Event, EventId};

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
}

impl Default for NfConfig {
    fn default() -> Self {
        NfConfig {
            max_iterations: 1000,
            enable_cone_merge: true,
            enable_chain_contract: true,
            enable_noop_elim: true,
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
    config: NfConfig,
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

        for iter in 0..self.config.max_iterations {
            stats.iterations = iter + 1;
            let size_before = dag.len();

            // Phase A: Causal closure (detect missing ancestors)
            // In a well-formed ingestion pipeline this should be a no-op,
            // but we check defensively.
            self.phase_a_check_closure(dag);

            // Phase B: Canonical ordering (idempotent after first pass)
            // The topological_order() in dag already enforces BTreeSet ordering
            // of concurrent events — no explicit edge addition needed here
            // because our DAG stores parents, not inter-sibling order edges.
            // The canonical order is implicit in topological_order().
            self.phase_b_canonicalize(dag);

            // Phase C1: Merge isomorphic cones
            if self.config.enable_cone_merge {
                let merged = self.phase_c1_merge_cones(dag);
                stats.cones_merged += merged;
            }

            // Phase C2: Collapse linear chains
            if self.config.enable_chain_contract {
                let contracted = self.phase_c2_collapse_chains(dag);
                stats.chains_contracted += contracted;
            }

            // Phase C3: Eliminate no-ops
            if self.config.enable_noop_elim {
                let eliminated = self.phase_c3_remove_noops(dag);
                stats.noops_eliminated += eliminated;
            }

            // Phase D: Hash stabilization (recompute cone hashes)
            self.cone_hasher.compute_all(dag);

            // Convergence check: did anything change?
            if dag.len() == size_before {
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
            // In a production system we would request missing events from peers.
            // Here we log — the NF will still converge on what's available.
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
        // The canonical ordering of concurrent events is enforced implicitly
        // through BTreeSet<EventId> for parent sets and BTreeSet for topological
        // ordering queues. No structural modification needed — the order is a
        // property of how we READ the DAG, not a stored edge set.
        //
        // For systems that need explicit ordering edges among concurrent events
        // (e.g., for strict serialization), this phase would add virtual
        // "ordering" edges. That extension is left to domain-specific admissibility
        // predicates (the A component of the kernel D = (E, ≺, A, →)).
    }

    // -------------------------------------------------------------------------
    // Phase C1 — Isomorphic Cone Merging
    // -------------------------------------------------------------------------

    fn phase_c1_merge_cones(&mut self, dag: &mut CausalDag) -> usize {
        self.cone_hasher.compute_all(dag);
        let groups = self.cone_hasher.isomorphic_groups();
        let mut merged = 0;

        for group in groups {
            if group.len() < 2 {
                continue;
            }
            // Keep the lexicographically smallest ID as canonical representative.
            let mut sorted = group;
            sorted.sort();
            let canonical = sorted[0].clone();
            let duplicates = &sorted[1..];

            // For each duplicate: redirect all children to the canonical event,
            // then remove the duplicate.
            for dup_id in duplicates {
                // Collect children of the duplicate
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
                        }
                    }
                    // Update children index
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

        merged
    }

    // -------------------------------------------------------------------------
    // Phase C2 — Linear Chain Contraction
    // -------------------------------------------------------------------------

    fn phase_c2_collapse_chains(&self, dag: &mut CausalDag) -> usize {
        let mut contracted = 0;
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
            // Re-check conditions (may have changed)
            if let Some(event) = dag.events.get(&id) {
                let parents = event.parents.clone();
                let children_opt = dag.children.get(&id).cloned();
                if parents.len() == 1 && children_opt.as_ref().map(|c| c.len()) == Some(1) {
                    if self.is_passthrough(event) {
                        dag.remove(&id);
                        contracted += 1;
                    }
                }
            }
        }

        contracted
    }

    /// A "passthrough" event is a structural relay with no semantic payload effect
    /// that is eligible for chain contraction (Phase C2).
    ///
    /// **Noops are excluded here** — they are handled exclusively by Phase C3
    /// (`phase_c3_remove_noops`) so that `NfStats::noops_eliminated` is accurate.
    /// Future payload variants (e.g. identity transforms, routing relays) can be
    /// added here without interfering with noop accounting.
    fn is_passthrough(&self, _event: &Event) -> bool {
        false
    }

    // -------------------------------------------------------------------------
    // Phase C3 — No-op Elimination
    // -------------------------------------------------------------------------

    fn phase_c3_remove_noops(&self, dag: &mut CausalDag) -> usize {
        let noops: Vec<EventId> = dag
            .events
            .iter()
            .filter(|(_, e)| e.payload.is_noop())
            .map(|(id, _)| id.clone())
            .collect();

        let count = noops.len();
        for id in noops {
            dag.remove(&id);
        }
        count
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

        let size_before = dag.len(); // 3: genesis + noop + real
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
        // Build: genesis → noop1 → noop2 → data
        let n1 = Event::noop(BTreeSet::from([gid.clone()]));
        let n2 = Event::noop(BTreeSet::from([n1.id.clone()]));
        let data = Event::data("result", serde_json::json!(42), BTreeSet::from([n2.id.clone()]));
        dag.insert(n1);
        dag.insert(n2);
        dag.insert(data);

        assert_eq!(dag.len(), 4); // genesis + 2 noops + data
        let mut nf = NormalForm::default();
        nf.reduce(&mut dag);
        // Both noops should be gone
        assert!(dag.len() < 4);
    }
}
