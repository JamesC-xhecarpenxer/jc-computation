# JC-Computation

Computation as a commons: open participation, universal verification, and deterministic outcomes.

The machine does not care who you are. It only cares whether the proof is valid.

Math is the governor. History is the source of truth. Computation is the constitution.

A distributed quotient-rewriting engine over causal history space, implemented in Rust.

## Core Equation

```
State = σ(nf(History))
```

| Symbol    | Meaning                                                        |
|-----------|----------------------------------------------------------------|
| `History` | Causally closed, immutable set of content-addressed events     |
| `nf`      | Normal form operator — confluent, terminating rewrite system   |
| `σ`       | Semantic functor — user-supplied state projection              |

State is **never stored**. It is always derived by applying `σ` to the normalized history. This makes the system intrinsically auditable and replay-safe.

## What This Is

JC-Computation is a formal kernel for distributed systems where convergence is a mathematical property, not a protocol. Nodes never need consensus — two nodes that have seen the same events will always compute the same state, regardless of the order they received them.

The merge primitive is the only network operation:

```
merge(A, B) = nf(A ∪ B)
```

This satisfies the CRDT laws (commutativity, associativity, idempotency) **derived from first principles**, not asserted as axioms.

## Architecture

```
src/
├── event.rs        — Immutable, content-addressed events (SHA-256 Merkle nodes)
├── dag.rs          — Causal DAG: topological order, ancestry, frontier, closure
├── cone.rs         — Cone hashing engine (Merkle-tree hash over causal history)
├── nf.rs           — Normal Form reduction engine (phases A → D)
├── kernel.rs       — JcKernel runtime + built-in semantic functors
├── merge.rs        — Distributed merge protocol + DistributedNode simulation
├── network.rs      — Peer-to-peer sync protocol
├── persistence.rs  — History serialization and storage
└── lib.rs          — Public API surface
```

### Normal Form Reduction Phases

| Phase | Name                     | Description                                                     |
|-------|--------------------------|-----------------------------------------------------------------|
| A     | Causal closure check     | Warns if history is missing ancestors (peer sync needed)        |
| B     | Canonical ordering       | Enforced implicitly via `BTreeSet<EventId>` in parent sets      |
| C1    | Isomorphic cone merging  | Deduplicates structurally identical sub-histories               |
| C2    | Linear chain contraction | Removes structural relay events with no semantic payload         |
| C3    | No-op elimination        | Removes `Noop` events, reconnecting their children to parents   |
| D     | Hash stabilization       | Recomputes all cone hashes after structural changes             |

**Termination** is guaranteed by the strictly decreasing complexity measure `Φ(H) = (|E|, entropy, disorder)`.  
**Confluence** follows from Newman's Lemma: Termination + Local Confluence ⟹ Confluence.

## Quick Start

```toml
# Cargo.toml
[dependencies]
jc-computation = { path = "." }
```

```rust
use jc_computation::{JcKernel, kernel::{KvFunctor, CounterFunctor}};

// Build a history — state is never stored, always derived
let mut k = JcKernel::default();

let e1 = k.new_event("set", serde_json::json!({"key": "user", "val": "Alice"}));
k.append(e1);

let e2 = k.new_event("set", serde_json::json!({"key": "role", "val": "admin"}));
k.append(e2);

// σ(nf(H)) — computed on demand, never persisted
let state = k.state(&KvFunctor);
assert_eq!(state["user"], "Alice");
assert_eq!(state["role"], "admin");
```

### Built-in Semantic Functors

| Functor          | Event kind    | State type                   |
|------------------|---------------|------------------------------|
| `KvFunctor`      | `"set"`       | `HashMap<String, Value>`     |
| `CounterFunctor` | `"increment"` | `i64`                        |
| `LogFunctor`     | `"log"`       | `Vec<Value>` (causal order)  |

Implement `SemanticFunctor` for your own domain model.

### Distributed Convergence

```rust
use jc_computation::merge::DistributedNode;
use jc_computation::kernel::CounterFunctor;
use jc_computation::event::Event;

let mut node_a = DistributedNode::new("Node-A");
let mut node_b = DistributedNode::new("Node-B");

// Partition: both nodes advance independently
let fa = node_a.history.frontier();
node_a.append(Event::data("increment", serde_json::json!(100), fa));

let fb = node_b.history.frontier();
node_b.append(Event::data("increment", serde_json::json!(50), fb));

// Heal: merge is the only primitive — no consensus, no conflict resolution
node_a.sync_with(&node_b);
node_b.sync_with(&node_a);

// Both nodes converge to the same state
assert_eq!(node_a.state(&CounterFunctor), node_b.state(&CounterFunctor)); // 150
```

## Running the Demo

```bash
cargo run
```

The demo exercises all four built-in scenarios: KV store, distributed counter, causal event log, and distributed convergence after a simulated network partition.

## Running Benchmarks

```bash
# Quick smoke-test (10K / 100K / 1M events, ~30 s)
BENCH_QUICK=1 cargo bench --bench bench_nf

# Full run (1M / 10M / 100M events — several minutes)
cargo bench --bench bench_nf
```

The benchmark exercises three DAG topologies (linear chain, wide fan-out, noop-heavy) at each scale and prints a scaling analysis with implied `O(n^x)` exponents and a pass/fail verdict.

## Running Tests

```bash
# Unit tests (all modules)
cargo test --lib

# Property-based tests (23 proptest cases)
cargo test --test property_tests

# All tests
cargo test
```

**24 tests total — 23 property/integration + 1 doc-test. All pass with zero warnings.**

## Formal Theory

See `FORMAL_THEORY.md.pdf` for the complete mathematical treatment, including:
- Formal definition of JC-Computation as `D = (E, ≺, A, →)`
- Proof of NF termination via the complexity measure `Φ`
- Proof of confluence via Newman's Lemma
- CRDT derivation from first principles

## Dependencies

| Crate        | Use                                  |
|--------------|--------------------------------------|
| `sha2`       | SHA-256 for content-addressed IDs    |
| `hex`        | Hex encoding of hashes               |
| `serde`      | Serialization of events and payloads |
| `serde_json` | JSON payloads                        |
| `indexmap`   | Deterministic map iteration          |

## Roadmap

- [x] Property tests for merge commutativity/idempotency (`proptest`)
- [x] Benchmarks (`criterion`-style) for cone hashing and NF reduction at scale
- [ ] Fuzzing targets for DAG operations
- [ ] CI pipeline: `cargo fmt`, `cargo clippy`, `cargo test`
- [ ] docs.rs metadata (`[package.metadata.docs.rs]`)
- [ ] Network transport layer (TCP/QUIC gossip protocol)
- [ ] Persistent history backend (RocksDB / SQLite)
- [ ] Admissibility predicate `A` for domain-specific event validation
- [ ] Benchmarks (`criterion`) for cone hashing and NF reduction at scale

## License

See `LICENSE`.

---

## Changelog

### v0.1.0
- Initial release: causal DAG, NF reduction, semantic functors, distributed merge.

### v0.1.1
- Cache-aware identity recomputation using payload and parent-set hashes.
- `recompute_id` short-circuits when structural inputs are unchanged.

### v0.1.3
- Fixed `benches/bench_nf.rs`: all five `NfResult` field accesses (`events_after`, `iterations`, `cones_merged`, `chains_contracted`, `noops_eliminated`) corrected to go through the `.stats` sub-field.
- Fixed bench run instructions in the file header: `cargo bench --bench bench_nf` (and `BENCH_QUICK=1` variant) instead of the incorrect manual binary path.
- Benchmark verified end-to-end in quick mode (10K/100K/1M events, all three topologies).
- Fixed `phase_c1_merge_cones` and `phase_c2_collapse_chains` to return reidentification pairs `Vec<(EventId, EventId)>` alongside mutation counts and dirty sets, aligning function signatures with their call sites.
- Fixed `CachedKvFunctor` dereference of `usize` cache key (was `*s`, now `s`).
- Removed unused `NfStats` import from `persistence.rs`.
- Fixed 15 unit test assertions accessing `NfResult` stats fields via the correct path (`result.stats.field` rather than `result.field`).
- Fixed property test `prop_noop_injection_transparent` overflow: constrained generated `i64` values to `[-1_000_000, 1_000_000]` to prevent integer overflow in `CounterFunctor` sum.
- Removed unused imports `NfConfig` and `LogFunctor` from `property_tests.rs`.
- Applied 6 Clippy fixes: `div_ceil`, two overindented doc comments, two collapsible `if` blocks, and a `single_match` in `network.rs`.
