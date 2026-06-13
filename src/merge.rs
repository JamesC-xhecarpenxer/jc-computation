//! Distributed Merge Protocol
//!
//! The ONLY network primitive in JC-Computation:
//!
//! ```text
//! merge(A, B) = nf(A ∪ B)
//! ```
//!
//! Properties (from the confluence proof):
//! - Commutativity:  merge(A, B) = merge(B, A)
//! - Associativity:  merge(merge(A, B), C) = merge(A, merge(B, C))
//! - Idempotency:    merge(A, A) = A
//!
//! These are the CRDT laws — but derived from first principles,
//! not asserted as axioms.
//!
//! No conflict resolution is needed. Every "conflict" is simply
//! a non-normal-form representation, which NF resolves.

use crate::dag::CausalDag;
use crate::nf::{NfConfig, NormalForm};

/// Merge two histories and normalize the result.
///
/// ```text
/// merge(H_a, H_b) = nf(H_a ∪ H_b)
/// ```
///
/// This is the distributed sync primitive.
pub fn merge_histories(a: &CausalDag, b: &CausalDag) -> CausalDag {
    merge_histories_with_config(a, b, NfConfig::default())
}

/// Merge with custom NF configuration.
pub fn merge_histories_with_config(a: &CausalDag, b: &CausalDag, config: NfConfig) -> CausalDag {
    let mut result = a.clone();
    result.union_with(b);
    let mut nf = NormalForm::new(config);
    nf.reduce(&mut result);
    result
}

/// A simulated distributed node holding a local history.
pub struct DistributedNode {
    pub id: String,
    pub history: CausalDag,
    nf: NormalForm,
}

impl DistributedNode {
    pub fn new(id: impl Into<String>) -> Self {
        use crate::event::Event;
        let mut history = CausalDag::new();
        history.insert(Event::genesis());
        DistributedNode {
            id: id.into(),
            history,
            nf: NormalForm::default(),
        }
    }

    /// Sync this node with another node's history.
    ///
    /// ```text
    /// H_self := nf(H_self ∪ H_other)
    /// ```
    pub fn sync_with(&mut self, other: &DistributedNode) {
        self.history.union_with(&other.history);
        self.nf.reduce(&mut self.history);
    }

    /// Append a local event and normalize.
    pub fn append(&mut self, event: crate::event::Event) {
        self.history.insert(event);
        self.nf.reduce(&mut self.history);
    }

    /// Query state using a semantic functor.
    pub fn state<F: crate::kernel::SemanticFunctor>(&self, functor: &F) -> F::State {
        functor.interpret(&self.history)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use crate::kernel::{CounterFunctor, KvFunctor};
    use std::collections::BTreeSet;

    #[test]
    fn merge_is_commutative() {
        let g = Event::genesis();
        let gid = g.id.clone();

        let mut dag_a = CausalDag::new();
        dag_a.insert(g.clone());
        let e_a = Event::data("op", serde_json::json!(1), BTreeSet::from([gid.clone()]));
        dag_a.insert(e_a);

        let mut dag_b = CausalDag::new();
        dag_b.insert(g.clone());
        let e_b = Event::data("op", serde_json::json!(2), BTreeSet::from([gid.clone()]));
        dag_b.insert(e_b);

        let ab = merge_histories(&dag_a, &dag_b);
        let ba = merge_histories(&dag_b, &dag_a);

        assert_eq!(ab.len(), ba.len(), "merge is commutative (same size)");
        // Check same event IDs
        let mut ids_ab: Vec<_> = ab.events.keys().cloned().collect();
        let mut ids_ba: Vec<_> = ba.events.keys().cloned().collect();
        ids_ab.sort();
        ids_ba.sort();
        assert_eq!(ids_ab, ids_ba, "merge is commutative (same events)");
    }

    #[test]
    fn merge_is_idempotent() {
        let mut dag = CausalDag::new();
        dag.insert(Event::genesis());

        let merged = merge_histories(&dag, &dag);
        assert_eq!(dag.len(), merged.len(), "merge(A,A) = A");
    }

    #[test]
    fn distributed_convergence() {
        // Two nodes start from genesis, add different events,
        // then sync — should converge to the same state.
        let mut node_a = DistributedNode::new("A");
        let mut node_b = DistributedNode::new("B");

        // Node A: set x = 1
        let frontier_a = node_a.history.frontier();
        let ea = Event::data("set", serde_json::json!({"key": "x", "val": 1}), frontier_a);
        node_a.append(ea);

        // Node B: set y = 2 (offline)
        let frontier_b = node_b.history.frontier();
        let eb = Event::data("set", serde_json::json!({"key": "y", "val": 2}), frontier_b);
        node_b.append(eb);

        // Sync
        node_a.sync_with(&node_b);
        node_b.sync_with(&node_a);

        // Both should see x=1 and y=2
        let state_a = node_a.state(&KvFunctor);
        let state_b = node_b.state(&KvFunctor);

        assert_eq!(state_a.get("x"), state_b.get("x"), "convergent x");
        assert_eq!(state_a.get("y"), state_b.get("y"), "convergent y");
    }

    #[test]
    fn counter_convergence() {
        let mut node_a = DistributedNode::new("A");
        let mut node_b = DistributedNode::new("B");

        // Both increment independently
        let fa = node_a.history.frontier();
        node_a.append(Event::data("increment", serde_json::json!(10), fa));

        let fb = node_b.history.frontier();
        node_b.append(Event::data("increment", serde_json::json!(5), fb));

        node_a.sync_with(&node_b);
        node_b.sync_with(&node_a);

        let ca = node_a.state(&CounterFunctor);
        let cb = node_b.state(&CounterFunctor);
        assert_eq!(ca, cb, "counter converges");
        assert_eq!(ca, 15, "total = 15");
    }
}
