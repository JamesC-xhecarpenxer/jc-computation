//! JC-Computation Demo
//!
//! Demonstrates the core equation:  State = σ(nf(History))

use jc_computation::{
    JcKernel,
    kernel::{CounterFunctor, KvFunctor, LogFunctor},
    merge::DistributedNode,
    event::Event,
};

fn main() {
    println!("═══════════════════════════════════════════════════");
    println!("  JC-Computation Kernel Demo");
    println!("  State = σ(nf(History))");
    println!("═══════════════════════════════════════════════════\n");

    demo_kv_store();
    demo_counter();
    demo_log();
    demo_distributed_convergence();
}

fn demo_kv_store() {
    println!("── Demo 1: Key-Value Store ──────────────────────────");
    let mut k = JcKernel::default();

    let e1 = k.new_event("set", serde_json::json!({"key": "name", "val": "Alice"}));
    k.append(e1);

    let e2 = k.new_event("set", serde_json::json!({"key": "role", "val": "admin"}));
    k.append(e2);

    let e3 = k.new_event("set", serde_json::json!({"key": "name", "val": "Bob"}));
    k.append(e3);

    let state = k.state(&KvFunctor);
    println!("  History size: {} events", k.history_size());
    println!("  Derived state (never stored — computed from history):");
    let mut keys: Vec<_> = state.keys().collect();
    keys.sort();
    for k_name in keys {
        println!("    {} = {}", k_name, state[k_name]);
    }
    println!();
}

fn demo_counter() {
    println!("── Demo 2: Distributed Counter ─────────────────────");
    let mut k = JcKernel::default();
    let deltas = [1i64, 5, 3, -2, 10];
    for d in deltas {
        let e = k.new_event("increment", serde_json::json!(d));
        k.append(e);
    }
    let total = k.state(&CounterFunctor);
    println!("  Increments: {:?}", deltas);
    println!("  Total (σ(nf(H))): {}", total);
    assert_eq!(total, 17);
    println!("  ✓ Correct\n");
}

fn demo_log() {
    println!("── Demo 3: Causal Event Log ─────────────────────────");
    let mut k = JcKernel::default();

    // Interleave some noops — they should be eliminated by NF
    let e1 = k.new_event("log", serde_json::json!("system started"));
    k.append(e1);

    let noop = k.new_noop();
    k.append(noop);

    let e2 = k.new_event("log", serde_json::json!("user logged in"));
    k.append(e2);

    let noop2 = k.new_noop();
    k.append(noop2);

    let e3 = k.new_event("log", serde_json::json!("action performed"));
    k.append(e3);

    let log = k.state(&LogFunctor);
    println!("  Log entries (noops invisible — eliminated by NF):");
    for entry in &log {
        println!("    → {}", entry);
    }
    assert_eq!(log.len(), 3, "only real events in log");
    println!("  ✓ {} entries (noops eliminated)\n", log.len());
}

fn demo_distributed_convergence() {
    println!("── Demo 4: Distributed Convergence ─────────────────");
    println!("  Two nodes partition, operate independently, then sync.");
    println!("  No consensus algorithm — convergence is NF convergence.\n");

    let mut node_a = DistributedNode::new("Node-A");
    let mut node_b = DistributedNode::new("Node-B");

    // Partition: both nodes advance independently
    let fa = node_a.history.frontier();
    node_a.append(Event::data("increment", serde_json::json!(100), fa));
    let fa2 = node_a.history.frontier();
    node_a.append(Event::data("increment", serde_json::json!(50), fa2));

    let fb = node_b.history.frontier();
    node_b.append(Event::data("increment", serde_json::json!(25), fb));
    let fb2 = node_b.history.frontier();
    node_b.append(Event::data("increment", serde_json::json!(75), fb2));

    println!("  Before sync:");
    println!("    Node-A counter: {}", node_a.state(&CounterFunctor));
    println!("    Node-B counter: {}", node_b.state(&CounterFunctor));

    // Heal partition — merge is the only primitive
    println!("\n  Syncing (merge = nf(H_a ∪ H_b))...");
    node_a.sync_with(&node_b);
    node_b.sync_with(&node_a);

    let ca = node_a.state(&CounterFunctor);
    let cb = node_b.state(&CounterFunctor);

    println!("\n  After sync:");
    println!("    Node-A counter: {}", ca);
    println!("    Node-B counter: {}", cb);
    println!("    Converged: {}", ca == cb);
    assert_eq!(ca, cb, "nodes must converge");
    assert_eq!(ca, 250, "total increments = 100+50+25+75 = 250");
    println!("  ✓ Convergent. Total = {}\n", ca);

    println!("═══════════════════════════════════════════════════");
    println!("  All demos passed.");
    println!("  State = σ(nf(H))  — no state was ever stored.");
    println!("═══════════════════════════════════════════════════");
}
