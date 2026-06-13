# JC-Computation Project Index

## 📁 Project Structure

```
jc_computation/
├── INDEX.md                      ← You are here
├── README.md                     ← Start here for overview
├── QUICK_REFERENCE.md            ← Developer cheat sheet
├── PROJECT_ANALYSIS.md           ← Deep technical dive
├── IMPLEMENTATION_ROADMAP.md     ← Future extensions
├── FORMAL_THEORY.md.pdf          ← Mathematical proofs
├── LICENSE                       ← Dual license terms
├── Cargo.toml                    ← Rust dependencies
├── src/
│   ├── lib.rs                   ← Public API exports
│   ├── main.rs                  ← Demo application
│   ├── event.rs                 ← Immutable events
│   ├── dag.rs                   ← Causal DAG
│   ├── cone.rs                  ← Merkle cone hashing
│   ├── nf.rs                    ← Normal form reduction
│   ├── kernel.rs                ← Main runtime + semantic functors
│   └── merge.rs                 ← Distributed merge protocol
```

---

## 📖 Reading Guide

### For First-Time Users

**Start here:**
1. **README.md** (5 min) — Understand what JC-Computation does
2. **QUICK_REFERENCE.md** (15 min) — See code examples
3. **src/main.rs** (10 min) — Run the demo and explore the 4 examples

**Next:**
4. **PROJECT_ANALYSIS.md** (30 min) — Deep dive into design

### For Implementers

**Code walkthrough:**
1. **src/event.rs** — Events are immutable, content-addressed
2. **src/dag.rs** — DAG maintains causality
3. **src/nf.rs** — Normal form reduction (the magic)
4. **src/kernel.rs** — Runtime + semantic functors
5. **src/merge.rs** — Distributed consensus-less protocol

**Tests:**
- Each .rs file has `#[cfg(test)]` module at bottom
- Run: `cargo test --lib`

### For Formal Verification

1. **FORMAL_THEORY.md.pdf** — Complete mathematical treatment
2. **PROJECT_ANALYSIS.md** § "Formal Verification Strategy"
3. **IMPLEMENTATION_ROADMAP.md** § "Byzantine Fault Simulation"

---

## 🚀 Quick Start

### Install Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
```

### Clone & Run Demo

```bash
cd jc_computation
cargo run
```

Expected output:
```
═══════════════════════════════════════════════════
  JC-Computation Kernel Demo
  State = σ(nf(History))
═══════════════════════════════════════════════════

── Demo 1: Key-Value Store ──────────────────────────
  History size: 4 events
  Derived state (never stored — computed from history):
    name = "Bob"
    role = "admin"
  ...
```

### Run Tests

```bash
cargo test --lib
```

Expected: 21 tests pass ✅

---

## 📚 File Descriptions

### Core Documentation

| File | Purpose | Read Time | Audience |
|------|---------|-----------|----------|
| **README.md** | Project overview, quick start, examples | 10 min | Everyone |
| **QUICK_REFERENCE.md** | Developer cheat sheet, patterns, debugging | 15 min | Developers |
| **PROJECT_ANALYSIS.md** | Complete technical analysis, architecture | 45 min | Architects, researchers |
| **IMPLEMENTATION_ROADMAP.md** | Future extensions, effort estimates | 20 min | Project managers, engineers |
| **FORMAL_THEORY.md.pdf** | Mathematical proofs, formal definitions | 60+ min | Theorists, auditors |
| **LICENSE** | Dual license (personal free / commercial paid) | 5 min | Legal |

### Source Code

| File | Lines | Responsibility | Complexity |
|------|-------|-----------------|-----------|
| **event.rs** | 100 | Immutable events, SHA-256 content addressing | ⭐⭐ |
| **dag.rs** | 300 | Causal DAG, topological sort, ancestry | ⭐⭐⭐ |
| **cone.rs** | 200 | Merkle cone hashing for deduplication | ⭐⭐ |
| **nf.rs** | 400 | Normal form reduction (Phases A-D) | ⭐⭐⭐⭐ |
| **kernel.rs** | 250 | Runtime kernel, semantic functors | ⭐⭐ |
| **merge.rs** | 200 | Distributed merge, consensus-less sync | ⭐⭐⭐ |
| **lib.rs** | 35 | Public API surface | ⭐ |
| **main.rs** | 150 | Demo application (4 scenarios) | ⭐⭐ |

**Total**: ~1,600 lines of production code (including tests)

---

## 🏗️ Architecture Layers

### Layer 1: Data (Immutable)

**Files**: `event.rs`, `dag.rs`

```
EventId (SHA-256)
    ↓
Event { payload, parents }
    ↓
CausalDag { events, ancestors }
```

**Invariants**:
- Events are immutable
- DAG has no cycles
- All ancestors are present

---

### Layer 2: Reduction (Logic)

**Files**: `nf.rs`, `cone.rs`

```
CausalDag
    ↓ (Phase A: Closure check)
    ↓ (Phase B: Canonical ordering)
    ↓ (Phase C1: Cone merging)
    ↓ (Phase C2: Chain contraction)
    ↓ (Phase C3: Noop elimination)
    ↓ (Phase D: Hash stabilization)
NormalForm(CausalDag)
```

**Theorem**: Termination + Confluence (Newman's Lemma)

---

### Layer 3: Semantics (Interpretation)

**Files**: `kernel.rs`

```
NormalForm(CausalDag)
    ↓
SemanticFunctor::interpret() [σ]
    ↓
State
```

**Built-in Functors**:
- `KvFunctor` → HashMap<String, Value>
- `CounterFunctor` → i64
- `LogFunctor` → Vec<Value>
- Custom: implement trait

---

### Layer 4: Distribution (Networking)

**Files**: `merge.rs`

```
DistributedNode::sync_with()
    ↓
merge_histories(A, B) = nf(A ∪ B)
    ↓
Both nodes converge (no consensus needed)
```

---

## 🔍 How to Navigate the Code

### Understanding Event Creation

```
User calls: kernel.new_event("kind", json_value)
                    ↓
           Event::data(kind, value, frontier_parents)
                    ↓
           compute_hash(payload, parents)  [Phase D]
                    ↓
           event.id = EventId(hash)
                    ↓
           kernel.append(event)
```

**Files**: event.rs, kernel.rs

---

### Understanding State Derivation

```
User calls: kernel.state(&MyFunctor)
                    ↓
           kernel.dag.topological_order()  [All events in causal order]
                    ↓
           MyFunctor::interpret(&dag)  [User-supplied σ]
                    ↓
           For each event_id in topo_order:
               event = dag.events[event_id]
               state.update(event)  [Domain-specific logic]
                    ↓
           return state
```

**Files**: kernel.rs, nf.rs (topo sort)

---

### Understanding Distributed Merge

```
node_a.sync_with(&node_b)
        ↓
for event in peer.history.events.values():
    if !self.history.contains(event.id):
        kernel.append(event)  [Inserts event]
        nf.reduce()           [Automatically reduces]
        ↓
Both nodes now have:
    - Same history
    - Same normalized form
    - Same derived state (if functors match)
```

**Files**: merge.rs, kernel.rs, nf.rs

---

## 📊 Complexity Reference

### Time

| Operation | Complexity | Context |
|-----------|-----------|---------|
| Event append | O(E log E) | DAG insertion + topological sort |
| State derivation | O(E) | Single pass over events |
| Merge two histories | O(E₁ + E₂) | Union + reduction |
| Cone hash (single) | O(\|ancestors\|) | Transitive closure |

### Space

| Component | Complexity | Optimization |
|-----------|-----------|--------------|
| Event store | O(E) | Compression (future) |
| Ancestor cache | O(E²) worst | Lazy computation (future) |
| DAG index | O(E) | Already optimal |

---

## 🧪 Testing Strategy

### Run All Tests

```bash
cargo test --lib
```

### Run Specific Test Module

```bash
cargo test --lib event::tests
cargo test --lib dag::tests
cargo test --lib nf::tests
cargo test --lib kernel::tests
cargo test --lib merge::tests
```

### Run Single Test

```bash
cargo test --lib topological_order -- --nocapture
```

### View Test Output

```bash
cargo test --lib -- --nocapture --test-threads=1
```

### Check Test Coverage (requires tarpaulin)

```bash
cargo install cargo-tarpaulin
cargo tarpaulin --lib --out Html
```

---

## 🔨 Development Workflow

### Before Committing

```bash
# Format code
cargo fmt

# Check lints
cargo clippy --all-targets -- -D warnings

# Run tests
cargo test --lib

# Build release
cargo build --release

# Check documentation
cargo doc --no-deps --open
```

### Creating a New Semantic Functor

**Step 1**: Define state type
```rust
pub struct MyState {
    // ... fields
}
```

**Step 2**: Implement SemanticFunctor
```rust
impl SemanticFunctor for MyFunctor {
    type State = MyState;
    
    fn interpret(&self, dag: &CausalDag) -> MyState {
        let mut state = MyState::default();
        for event_id in dag.topological_order() {
            if let Some(event) = dag.events.get(&event_id) {
                // Process event
            }
        }
        state
    }
}
```

**Step 3**: Use it
```rust
let state = kernel.state(&MyFunctor);
```

### Extending the Demo

Edit `src/main.rs`:
- Add a new `demo_*()` function
- Call it from `main()`
- Run `cargo run` to test

---

## 🐛 Troubleshooting

### Compilation Errors

**"cannot find type in this scope"**
→ Check imports in lib.rs and source file

**"mismatched types"**
→ EventId is not String; use `event.id` not `event.id.to_string()`

**"borrowed value does not live long enough"**
→ Clone values when needed: `event.clone()`

### Test Failures

**"assertion failed: 3 == 4"**
→ Check test expectations; print actual values:
```rust
println!("Expected 3, got {}", actual);
```

**"thread 'main' panicked at 'assertion failed'"**
→ Run with backtrace:
```bash
RUST_BACKTRACE=1 cargo test --lib
```

### State Mismatch

**"node_a state != node_b state after sync"**
→ Verify both nodes have same events:
```rust
assert_eq!(node_a.history.len(), node_b.history.len());
```

---

## 📝 Common Tasks

### Task: Add a new event kind

**Step 1**: Create events
```rust
let e = kernel.new_event("my_kind", json!(value));
kernel.append(e);
```

**Step 2**: Create a functor
```rust
struct MyFunctor;
impl SemanticFunctor for MyFunctor {
    type State = MyState;
    fn interpret(&self, dag: &CausalDag) -> MyState { ... }
}
```

**Step 3**: Derive state
```rust
let state = kernel.state(&MyFunctor);
```

---

### Task: Simulate network partition

```rust
let mut node_a = DistributedNode::new("A");
let mut node_b = DistributedNode::new("B");

// Partition: both advance independently
for i in 0..5 {
    let parents = node_a.history.frontier();
    node_a.append(Event::data("increment", json!(i), parents));
}

for i in 0..3 {
    let parents = node_b.history.frontier();
    node_b.append(Event::data("increment", json!(i+10), parents));
}

// Check they diverge
assert_ne!(
    node_a.state(&CounterFunctor),
    node_b.state(&CounterFunctor)
);

// Heal: merge
node_a.sync_with(&node_b);
node_b.sync_with(&node_a);

// Check they converge
assert_eq!(
    node_a.state(&CounterFunctor),
    node_b.state(&CounterFunctor)
);
```

---

### Task: Debug normal form reduction

```rust
let mut nf = NormalForm::new(NfConfig::default());

// Before
println!("Before: {} events", kernel.dag.len());

// Reduce
let stats = nf.reduce(&mut kernel.dag);

// After
println!("After: {} events", kernel.dag.len());
println!("Cones merged: {}", stats.cones_merged);
println!("Chains contracted: {}", stats.chains_contracted);
println!("Noops eliminated: {}", stats.noops_eliminated);
```

---

## 🎯 Next Steps

### If you want to...

**Understand the math**
→ Read FORMAL_THEORY.md.pdf (60+ min)

**Use JC-Computation in your project**
→ See QUICK_REFERENCE.md examples (15 min)

**Extend JC-Computation**
→ Read IMPLEMENTATION_ROADMAP.md (20 min)

**Audit the code**
→ Read PROJECT_ANALYSIS.md + run tests (2 hours)

**Optimize performance**
→ Study IMPLEMENTATION_ROADMAP.md § "Benchmarks" (1 day)

**Deploy to production**
→ Implement storage.rs + transport.rs (3-5 days)

---

## 📞 Support

### Questions?

1. Check **QUICK_REFERENCE.md** § "Debugging Checklist"
2. Run `cargo test --lib` to verify everything works
3. Read the test code (src/**/*.rs) for examples
4. Review **PROJECT_ANALYSIS.md** for deep explanation

### Contributing

See **IMPLEMENTATION_ROADMAP.md** for areas that need work:
- Persistent storage (storage.rs)
- Network transport (transport.rs)
- Property-based testing (proptest)
- Benchmarks (criterion)
- Byzantine fault simulation

---

## 📜 License

JC-Computation is dual-licensed:
- **Personal use**: Free
- **Commercial use**: Paid license required

See LICENSE for details.

---

## 🎓 Educational Resources

### Papers Referenced

- **Lamport, L. (1978)**: "Time, Clocks, and the Ordering of Events in a Distributed System" — Causal ordering
- **Newman, M.H.A. (1942)**: "On theories with a combinatorial definition of 'equivalence'" — Newman's Lemma
- **Shapiro, M., et al. (2011)**: "Conflict-free replicated data types" — CRDT theory

### Related Systems

- **CRDT Libraries**: Automerge, Yjs, Operational Transformation
- **Event Sourcing**: EventStoreDB, Axon Framework
- **Consensus**: Raft, Paxos, PBFT
- **DAG-based**: Swirl, Hedera Hashgraph, IOTA

---

## 🚀 Project Status

| Aspect | Status | Notes |
|--------|--------|-------|
| Core implementation | ✅ Complete | All phases A-D |
| Unit tests | ✅ Complete | 21 tests, 100% pass |
| Documentation | ✅ Complete | 4 markdown files + 1 PDF |
| Demo | ✅ Complete | 4 scenarios |
| Production readiness | 🟡 Partial | Needs storage + transport |

**Next milestone**: Persistent storage (Phase 4)

---

**JC-Computation™ — Distributed systems without consensus. State derived, never stored.**
