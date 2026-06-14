//! Property-based tests for JC-Computation.
//!
//! These tests verify the *mathematical laws* that the system is built on:
//!
//! - Content-addressing: same payload+parents → same ID (determinism)
//! - NF idempotence: nf(nf(H)) = nf(H)
//! - Merge commutativity: merge(A,B) = merge(B,A)
//! - Merge associativity: merge(merge(A,B),C) = merge(A,merge(B,C))
//! - Merge idempotence: merge(A,A) = A
//! - Causal closure: nf(H) is always causally closed
//! - Topological order: parents always precede children
//! - Semantic determinism: σ(nf(H)) is deterministic
//! - State convergence: two nodes reach the same state after sync
//!
//! Run with: `cargo test --test property_tests`

use jc_computation::{CausalDag, Event, EventId, NormalForm};
use jc_computation::merge::{merge_histories, DistributedNode};
use jc_computation::nf::NfConfig;
use jc_computation::kernel::{JcKernel, KvFunctor, CounterFunctor, LogFunctor};
use proptest::prelude::*;
use std::collections::BTreeSet;

// ────────────────────────────────────────────────────────────────────────────
// Generators
// ────────────────────────────────────────────────────────────────────────────

/// Generate a random JSON value (shallow — no nesting beyond depth 2).
fn arb_json() -> impl Strategy<Value = serde_json::Value> {
    prop_oneof![
        any::<i64>().prop_map(|n| serde_json::json!(n)),
        any::<bool>().prop_map(|b| serde_json::json!(b)),
        "[a-z]{1,8}".prop_map(|s| serde_json::json!(s)),
        (any::<i64>(), "[a-z]{1,6}").prop_map(|(n, k)| serde_json::json!({ k: n })),
    ]
}

/// Generate a random event kind tag.
fn arb_kind() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("set".to_string()),
        Just("increment".to_string()),
        Just("log".to_string()),
        Just("op".to_string()),
        "[a-z]{1,6}".prop_map(|s| s),
    ]
}

/// Generate a linear chain of `len` data events (no noops).
fn build_data_chain(len: usize, seed: u64) -> CausalDag {
    let mut dag = CausalDag::new();
    let g = Event::genesis();
    let mut tip = g.id.clone();
    dag.insert(g);
    for i in 0..len {
        let e = Event::data(
            "op",
            serde_json::json!({ "i": i as i64, "seed": seed as i64 }),
            BTreeSet::from([tip.clone()]),
        );
        tip = e.id.clone();
        dag.insert(e);
    }
    dag
}

/// Generate a chain with mixed data and noop events.
fn build_mixed_chain(len: usize, noop_every: usize) -> CausalDag {
    let mut dag = CausalDag::new();
    let g = Event::genesis();
    let mut tip = g.id.clone();
    dag.insert(g);
    for i in 0..len {
        let e = if noop_every > 0 && i % noop_every == 0 {
            Event::noop(BTreeSet::from([tip.clone()]))
        } else {
            Event::data("op", serde_json::json!(i as i64), BTreeSet::from([tip.clone()]))
        };
        tip = e.id.clone();
        dag.insert(e);
    }
    dag
}

/// Build a forked DAG: genesis → (branch A of `a_len` events) + (branch B of `b_len` events).
fn build_forked(a_len: usize, b_len: usize) -> (CausalDag, CausalDag) {
    let g = Event::genesis();
    let gid = g.id.clone();

    let mut dag_a = CausalDag::new();
    dag_a.insert(g.clone());
    let mut tip_a = gid.clone();
    for i in 0..a_len {
        let e = Event::data("a", serde_json::json!(i as i64), BTreeSet::from([tip_a.clone()]));
        tip_a = e.id.clone();
        dag_a.insert(e);
    }

    let mut dag_b = CausalDag::new();
    dag_b.insert(g);
    let mut tip_b = gid.clone();
    for i in 0..b_len {
        let e = Event::data("b", serde_json::json!(i as i64), BTreeSet::from([tip_b.clone()]));
        tip_b = e.id.clone();
        dag_b.insert(e);
    }

    (dag_a, dag_b)
}

fn sorted_event_ids(dag: &CausalDag) -> Vec<EventId> {
    let mut ids: Vec<_> = dag.events.keys().cloned().collect();
    ids.sort();
    ids
}

// ────────────────────────────────────────────────────────────────────────────
// Property 1: Content-addressing determinism
// ────────────────────────────────────────────────────────────────────────────

proptest! {
    #[test]
    fn prop_content_addressing_deterministic(
        kind in arb_kind(),
        value in arb_json(),
        parent_count in 0usize..4,
    ) {
        // Build a fake parent set (use genesis IDs)
        let mut parents = BTreeSet::new();
        for i in 0..parent_count {
            parents.insert(format!("parent_{}", i));
        }

        let e1 = Event::data(kind.clone(), value.clone(), parents.clone());
        let e2 = Event::data(kind.clone(), value.clone(), parents.clone());
        prop_assert_eq!(&e1.id, &e2.id, "same content must produce same ID");
        prop_assert_eq!(e1.payload_bytes(), e2.payload_bytes(), "payload_bytes must be stable");
    }

    #[test]
    fn prop_different_values_different_ids(
        kind in arb_kind(),
        v1 in any::<i64>(),
        v2 in any::<i64>(),
    ) {
        prop_assume!(v1 != v2);
        let g = Event::genesis();
        let parents = BTreeSet::from([g.id.clone()]);
        let e1 = Event::data(kind.clone(), serde_json::json!(v1), parents.clone());
        let e2 = Event::data(kind.clone(), serde_json::json!(v2), parents.clone());
        prop_assert_ne!(&e1.id, &e2.id, "different values must produce different IDs");
    }

    #[test]
    fn prop_genesis_always_same_id(_seed in any::<u64>()) {
        let g1 = Event::genesis();
        let g2 = Event::genesis();
        prop_assert_eq!(&g1.id, &g2.id, "genesis ID must be globally unique and deterministic");
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Property 2: NF idempotence — nf(nf(H)) = nf(H)
// ────────────────────────────────────────────────────────────────────────────

proptest! {
    #[test]
    fn prop_nf_idempotent_data_chain(len in 1usize..50) {
        let mut dag = build_data_chain(len, 0);
        let mut nf = NormalForm::default();

        nf.reduce(&mut dag);
        let ids_after_first = sorted_event_ids(&dag);
        let size_after_first = dag.len();

        nf.reduce(&mut dag);
        let ids_after_second = sorted_event_ids(&dag);
        let size_after_second = dag.len();

        prop_assert_eq!(size_after_first, size_after_second, "nf is idempotent (size)");
        prop_assert_eq!(ids_after_first, ids_after_second, "nf is idempotent (IDs)");
    }

    #[test]
    fn prop_nf_idempotent_mixed_chain(
        len in 1usize..40,
        noop_every in 1usize..5,
    ) {
        let mut dag = build_mixed_chain(len, noop_every);
        let mut nf = NormalForm::default();

        nf.reduce(&mut dag);
        let size1 = dag.len();

        nf.reduce(&mut dag);
        let size2 = dag.len();

        prop_assert_eq!(size1, size2, "nf idempotent on mixed chain: len={}, noop_every={}", len, noop_every);
    }

    #[test]
    fn prop_nf_idempotent_forked(a in 1usize..20, b in 1usize..20) {
        let (dag_a, dag_b) = build_forked(a, b);
        let mut merged = merge_histories(&dag_a, &dag_b);

        let mut nf = NormalForm::default();
        nf.reduce(&mut merged);
        let size1 = merged.len();

        nf.reduce(&mut merged);
        let size2 = merged.len();

        prop_assert_eq!(size1, size2, "nf idempotent after fork-merge");
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Property 3: Merge commutativity — merge(A,B) = merge(B,A)
// ────────────────────────────────────────────────────────────────────────────

proptest! {
    #[test]
    fn prop_merge_commutative(a in 1usize..20, b in 1usize..20) {
        let (dag_a, dag_b) = build_forked(a, b);
        let ab = merge_histories(&dag_a, &dag_b);
        let ba = merge_histories(&dag_b, &dag_a);

        prop_assert_eq!(ab.len(), ba.len(), "merge(A,B) and merge(B,A) must have same size");
        prop_assert_eq!(
            sorted_event_ids(&ab),
            sorted_event_ids(&ba),
            "merge(A,B) and merge(B,A) must contain same events"
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Property 4: Merge associativity — merge(merge(A,B),C) = merge(A,merge(B,C))
// ────────────────────────────────────────────────────────────────────────────

proptest! {
    #[test]
    fn prop_merge_associative(a in 1usize..15, b in 1usize..15, c in 1usize..15) {
        let g = Event::genesis();
        let gid = g.id.clone();

        let mut dag_a = CausalDag::new();
        dag_a.insert(g.clone());
        let mut tip_a = gid.clone();
        for i in 0..a {
            let e = Event::data("a", serde_json::json!(i as i64), BTreeSet::from([tip_a.clone()]));
            tip_a = e.id.clone();
            dag_a.insert(e);
        }

        let mut dag_b = CausalDag::new();
        dag_b.insert(g.clone());
        let mut tip_b = gid.clone();
        for i in 0..b {
            let e = Event::data("b", serde_json::json!(i as i64), BTreeSet::from([tip_b.clone()]));
            tip_b = e.id.clone();
            dag_b.insert(e);
        }

        let mut dag_c = CausalDag::new();
        dag_c.insert(g);
        let mut tip_c = gid.clone();
        for i in 0..c {
            let e = Event::data("c", serde_json::json!(i as i64), BTreeSet::from([tip_c.clone()]));
            tip_c = e.id.clone();
            dag_c.insert(e);
        }

        let abc = merge_histories(&merge_histories(&dag_a, &dag_b), &dag_c);
        let a_bc = merge_histories(&dag_a, &merge_histories(&dag_b, &dag_c));

        prop_assert_eq!(abc.len(), a_bc.len(), "merge is associative (size)");
        prop_assert_eq!(
            sorted_event_ids(&abc),
            sorted_event_ids(&a_bc),
            "merge is associative (IDs)"
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Property 5: Merge idempotence — merge(A,A) = A
// ────────────────────────────────────────────────────────────────────────────

proptest! {
    #[test]
    fn prop_merge_idempotent(len in 1usize..30) {
        let dag = build_data_chain(len, 42);
        let merged = merge_histories(&dag, &dag);

        prop_assert_eq!(
            sorted_event_ids(&dag),
            sorted_event_ids(&merged),
            "merge(A,A) = A: same events after idempotent merge"
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Property 6: Causal closure preserved through NF
// ────────────────────────────────────────────────────────────────────────────

proptest! {
    #[test]
    fn prop_nf_preserves_causal_closure_data(len in 1usize..40) {
        let mut dag = build_data_chain(len, 7);
        let mut nf = NormalForm::default();
        nf.reduce(&mut dag);
        prop_assert!(dag.is_causally_closed(), "data chain must be causally closed after nf");
    }

    #[test]
    fn prop_nf_preserves_causal_closure_mixed(
        len in 1usize..40,
        noop_every in 1usize..5,
    ) {
        let mut dag = build_mixed_chain(len, noop_every);
        let mut nf = NormalForm::default();
        nf.reduce(&mut dag);
        prop_assert!(
            dag.is_causally_closed(),
            "mixed chain must be causally closed after nf: len={}, noop_every={}",
            len, noop_every
        );
    }

    #[test]
    fn prop_merge_result_causally_closed(a in 1usize..20, b in 1usize..20) {
        let (dag_a, dag_b) = build_forked(a, b);
        let merged = merge_histories(&dag_a, &dag_b);
        prop_assert!(merged.is_causally_closed(), "merged DAG must be causally closed");
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Property 7: Topological order — parents always precede children
// ────────────────────────────────────────────────────────────────────────────

proptest! {
    #[test]
    fn prop_topo_order_correct(len in 1usize..30) {
        let dag = build_data_chain(len, 99);
        let order = dag.topological_order();
        let pos: std::collections::HashMap<&EventId, usize> =
            order.iter().enumerate().map(|(i, id)| (id, i)).collect();

        for (id, event) in &dag.events {
            let my_pos = *pos.get(id).expect("every event must appear in topo order");
            for parent in &event.parents {
                if let Some(&parent_pos) = pos.get(parent) {
                    prop_assert!(
                        parent_pos < my_pos,
                        "parent {} (pos {}) must precede child {} (pos {})",
                        parent, parent_pos, id, my_pos
                    );
                }
            }
        }
    }

    #[test]
    fn prop_topo_order_deterministic(len in 1usize..30, _seed in any::<u8>()) {
        let dag = build_data_chain(len, 42);
        let order1 = dag.topological_order();
        let order2 = dag.topological_order();
        prop_assert_eq!(order1, order2, "topological order must be deterministic");
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Property 8: Semantic determinism — σ(nf(H)) is always the same
// ────────────────────────────────────────────────────────────────────────────

proptest! {
    #[test]
    fn prop_kv_state_deterministic(
        keys in prop::collection::vec("[a-z]{1,4}", 1..8),
        values in prop::collection::vec(any::<i32>(), 1..8),
    ) {
        let pairs: Vec<_> = keys.into_iter().zip(values.into_iter()).collect();

        // Build the same kernel twice, apply same events, compare state
        let mut k1 = JcKernel::default();
        let mut k2 = JcKernel::default();

        for (key, val) in &pairs {
            let payload = serde_json::json!({ "key": key, "val": val });
            let e1 = k1.new_event("set", payload.clone());
            k1.append(e1);
            let e2 = k2.new_event("set", payload);
            k2.append(e2);
        }

        let state1 = k1.state(&KvFunctor);
        let state2 = k2.state(&KvFunctor);
        prop_assert_eq!(state1, state2, "kv state must be deterministic for same event sequence");
    }

    #[test]
    fn prop_counter_state_deterministic(increments in prop::collection::vec(any::<i32>(), 1..10)) {
        let mut k1 = JcKernel::default();
        let mut k2 = JcKernel::default();

        for &val in &increments {
            let e1 = k1.new_event("increment", serde_json::json!(val));
            k1.append(e1);
            let e2 = k2.new_event("increment", serde_json::json!(val));
            k2.append(e2);
        }

        let s1 = k1.state(&CounterFunctor);
        let s2 = k2.state(&CounterFunctor);
        prop_assert_eq!(s1, s2, "counter state must be deterministic");
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Property 9: Distributed convergence — two nodes always agree after sync
// ────────────────────────────────────────────────────────────────────────────

proptest! {
    #[test]
    fn prop_distributed_kv_convergence(
        a_keys in prop::collection::vec("[a-z]{1,4}", 0..5),
        a_vals in prop::collection::vec(any::<i32>(), 0..5),
        b_keys in prop::collection::vec("[a-z]{1,4}", 0..5),
        b_vals in prop::collection::vec(any::<i32>(), 0..5),
    ) {
        let mut node_a = DistributedNode::new("A");
        let mut node_b = DistributedNode::new("B");

        for (key, val) in a_keys.iter().zip(a_vals.iter()) {
            let payload = serde_json::json!({ "key": key, "val": val });
            let frontier = node_a.history.frontier();
            let e = Event::data("set", payload, frontier);
            node_a.append(e);
        }

        for (key, val) in b_keys.iter().zip(b_vals.iter()) {
            let payload = serde_json::json!({ "key": key, "val": val });
            let frontier = node_b.history.frontier();
            let e = Event::data("set", payload, frontier);
            node_b.append(e);
        }

        // Bi-directional sync
        node_a.sync_with(&node_b);
        node_b.sync_with(&node_a);

        let state_a = node_a.state(&KvFunctor);
        let state_b = node_b.state(&KvFunctor);

        prop_assert_eq!(
            state_a, state_b,
            "nodes must converge to the same KV state after sync"
        );
    }

    #[test]
    fn prop_distributed_counter_convergence(
        a_increments in prop::collection::vec(any::<i32>(), 0..6),
        b_increments in prop::collection::vec(any::<i32>(), 0..6),
    ) {
        let mut node_a = DistributedNode::new("A");
        let mut node_b = DistributedNode::new("B");

        for &val in &a_increments {
            let frontier = node_a.history.frontier();
            let e = Event::data("increment", serde_json::json!(val), frontier);
            node_a.append(e);
        }
        for &val in &b_increments {
            let frontier = node_b.history.frontier();
            let e = Event::data("increment", serde_json::json!(val), frontier);
            node_b.append(e);
        }

        node_a.sync_with(&node_b);
        node_b.sync_with(&node_a);

        let ca = node_a.state(&CounterFunctor);
        let cb = node_b.state(&CounterFunctor);
        prop_assert_eq!(ca, cb, "counter must converge after sync");

        let expected: i64 = a_increments.iter().map(|&x| x as i64).sum::<i64>()
            + b_increments.iter().map(|&x| x as i64).sum::<i64>();
        prop_assert_eq!(ca, expected, "counter total must equal sum of all increments");
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Property 10: Noop elimination — noops never affect semantic state
// ────────────────────────────────────────────────────────────────────────────

proptest! {
    #[test]
    fn prop_noop_injection_transparent(
        ops in prop::collection::vec(any::<i64>(), 1..8),
        noop_positions in prop::collection::vec(0usize..20, 0..5),
    ) {
        // Build kernel WITHOUT noops
        let mut k_clean = JcKernel::default();
        for &v in &ops {
            let e = k_clean.new_event("increment", serde_json::json!(v));
            k_clean.append(e);
        }
        let clean_state = k_clean.state(&CounterFunctor);

        // Build kernel WITH noops interleaved
        let mut k_noops = JcKernel::default();
        let mut op_iter = ops.iter();
        let noop_set: std::collections::HashSet<usize> = noop_positions.into_iter().collect();
        let total = ops.len() + noop_set.len();
        let mut op_count = 0;
        let mut all_ops = Vec::new();
        // Alternate ops and noops
        for i in 0..total {
            if noop_set.contains(&i) {
                all_ops.push(None);
            } else if let Some(&v) = op_iter.next() {
                all_ops.push(Some(v));
                op_count += 1;
            }
        }
        drop(op_iter);
        // Append remaining ops
        for &v in ops.iter().skip(op_count) {
            all_ops.push(Some(v));
        }

        for op in &all_ops {
            match op {
                Some(v) => {
                    let e = k_noops.new_event("increment", serde_json::json!(v));
                    k_noops.append(e);
                }
                None => {
                    let noop = k_noops.new_noop();
                    k_noops.append(noop);
                }
            }
        }

        let noop_state = k_noops.state(&CounterFunctor);
        prop_assert_eq!(
            clean_state, noop_state,
            "noops must not affect counter state"
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Property 11: Ancestry and causality relations
// ────────────────────────────────────────────────────────────────────────────

proptest! {
    #[test]
    fn prop_chain_causality(len in 2usize..20) {
        let mut dag = build_data_chain(len, 0);
        let order = dag.topological_order();

        // Every non-genesis event must causally follow the event before it
        for i in 1..order.len() {
            let later = &order[i];
            let earlier = &order[0]; // genesis precedes everything
            prop_assert!(
                dag.causally_precedes(earlier, later),
                "genesis must causally precede {} (pos {})",
                later, i
            );
        }
    }

    #[test]
    fn prop_concurrent_events_are_unordered(a in 1usize..10, b in 1usize..10) {
        let (dag_a, dag_b) = build_forked(a, b);
        let mut merged = merge_histories(&dag_a, &dag_b);

        // The tip of branch A and tip of branch B should be concurrent
        let a_frontier: Vec<EventId> = dag_a.frontier().into_iter().collect();
        let b_frontier: Vec<EventId> = dag_b.frontier().into_iter().collect();

        if !a_frontier.is_empty() && !b_frontier.is_empty() {
            let tip_a = &a_frontier[0];
            let tip_b = &b_frontier[0];

            if tip_a != tip_b {
                // They should be concurrent (neither precedes the other)
                prop_assert!(
                    merged.are_concurrent(tip_a, tip_b),
                    "tips of independent branches must be concurrent"
                );
            }
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Property 12: Kernel history growth is monotone (append-only)
// ────────────────────────────────────────────────────────────────────────────

proptest! {
    #[test]
    fn prop_kernel_history_monotone(ops in prop::collection::vec(any::<i64>(), 1..10)) {
        let mut k = JcKernel::default();
        let mut prev_size = k.history_size();

        for &v in &ops {
            let e = k.new_event("op", serde_json::json!(v));
            k.append(e);
            // Data events must not shrink history
            prop_assert!(
                k.history_size() >= prev_size,
                "history must be monotonically non-decreasing for data events"
            );
            prev_size = k.history_size();
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Fuzz-style: random event sequences don't panic
// ────────────────────────────────────────────────────────────────────────────

proptest! {
    #[test]
    fn prop_no_panic_random_sequence(
        event_count in 1usize..30,
        noop_prob in 0u8..10u8, // out of 10; 3 means 30% noop
        seed in any::<u64>(),
    ) {
        let mut k = JcKernel::default();
        // Deterministic pseudo-random using seed
        let mut rng = seed;
        let lcg = |x: u64| x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);

        for i in 0..event_count {
            rng = lcg(rng);
            if (rng % 10) < noop_prob as u64 {
                let noop = k.new_noop();
                k.append(noop);
            } else {
                let e = k.new_event("op", serde_json::json!(i as i64 ^ rng as i64));
                k.append(e);
            }
        }

        // Must not panic; must be causally closed
        prop_assert!(k.dag.is_causally_closed(), "kernel DAG must always be causally closed");
        prop_assert!(k.history_size() >= 1, "kernel must always have at least genesis");
    }
}
