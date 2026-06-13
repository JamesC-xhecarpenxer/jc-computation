# JC-Computation Quick Reference Guide

## Core Equation

```
State = σ(nf(History))
```

**Never store state.** Always derive it from normalized history.

---

## Key Types at a Glance

### Event (immutable, content-addressed)

```rust
pub struct Event {
    pub id: EventId,              // SHA-256 hash
    pub payload: Payload,         // Data { kind, value } | Noop
    pub parents: BTreeSet<...>,   // Causal dependencies
    pub timestamp: u64,           // Logical clock
}
```

**Create**:
```rust
let event = Event::data("kind", json!(value), parent_set);
let noop = Event::noop(parent_set);
let genesis = Event::genesis();  // Root event
```

---

### CausalDAG (history)

```rust
pub struct CausalDag {
    pub events: IndexMap<EventId, Event>,      // All events
    pub ancestors: IndexMap<EventId, Set<EventId>>,  // Transitive closure
}
```

**Query**:
```rust
let topo_order = dag.topological_order();  // Causal order
let tips = dag.frontier();                  // Tip events
let ancestors = dag.ancestors(&event_id);  // All ancestors
```

---

### JcKernel (main runtime)

```rust
pub struct JcKernel {
    pub dag: CausalDag,
    nf: NormalForm,
}

impl JcKernel {
    pub fn append(&mut self, event: Event) -> NfStats;
    pub fn state<F: SemanticFunctor>(&self, f: &F) -> F::State;
    pub fn frontier(&self) -> BTreeSet<EventId>;
    pub fn history_size(&self) -> usize;
    pub fn new_event(&self, kind: &str, value: Value) -> Event;
    pub fn new_noop(&self) -> Event;
}
```

---

## Common Patterns

### Pattern 1: Simple State Machine

```rust
use jc_computation::{JcKernel, kernel::KvFunctor};

fn main() {
    let mut kernel = JcKernel::default();
    
    // Append events
    let e1 = kernel.new_event("set", json!({"key": "x", "val": 10}));
    kernel.append(e1);
    
    let e2 = kernel.new_event("set", json!({"key": "y", "val": 20}));
    kernel.append(e2);
    
    // Derive state (never stored)
    let state = kernel.state(&KvFunctor);
    println!("{:?}", state);  // {"x": 10, "y": 20}
}
```

### Pattern 2: Distributed Consensus (No Protocol!)

```rust
use jc_computation::{DistributedNode, kernel::CounterFunctor, event::Event};

fn main() {
    let mut node_a = DistributedNode::new("A");
    let mut node_b = DistributedNode::new("B");
    
    // Independent work
    let parents_a = node_a.history.frontier();
    node_a.append(Event::data("increment", json!(100), parents_a));
    
    let parents_b = node_b.history.frontier();
    node_b.append(Event::data("increment", json!(50), parents_b));
    
    // Sync (only network operation)
    node_a.sync_with(&node_b);
    node_b.sync_with(&node_a);
    
    // Both converge automatically (no voting)
    let state_a = node_a.state(&CounterFunctor);
    let state_b = node_b.state(&CounterFunctor);
    assert_eq!(state_a, state_b);  // 150
}
```

### Pattern 3: Custom Semantic Functor

```rust
use jc_computation::kernel::SemanticFunctor;

struct MyFunctor;

impl SemanticFunctor for MyFunctor {
    type State = MyState;
    
    fn interpret(&self, dag: &CausalDag) -> MyState {
        let mut state = MyState::default();
        
        // Walk history in causal order
        for event_id in dag.topological_order() {
            if let Some(event) = dag.events.get(&event_id) {
                if let Payload::Data { kind, value } = &event.payload {
                    match kind.as_str() {
                        "my_event" => {
                            // Update state
                            state.process(value);
                        }
                        _ => {}
                    }
                }
            }
        }
        
        state
    }
}

// Use it
let state = kernel.state(&MyFunctor);
```

---

## Testing Patterns

### Unit Test: State Derivation

```rust
#[test]
fn test_kv_state() {
    let mut kernel = JcKernel::default();
    
    let e1 = kernel.new_event("set", json!({"key": "x", "val": 1}));
    kernel.append(e1);
    
    let state = kernel.state(&KvFunctor);
    assert_eq!(state["x"], json!(1));
}
```

### Test: Merge Commutativity

```rust
#[test]
fn test_merge_commutative() {
    let mut a = JcKernel::default();
    let mut b = JcKernel::default();
    
    let e_a = a.new_event("increment", json!(10));
    a.append(e_a);
    
    let e_b = b.new_event("increment", json!(20));
    b.append(e_b);
    
    // Merge in different orders
    let mut h1 = a.dag.clone();
    h1.insert(b.dag.events.values().next().unwrap().clone());
    
    let mut h2 = b.dag.clone();
    h2.insert(a.dag.events.values().next().unwrap().clone());
    
    // Results should be identical
    let mut k1 = JcKernel::default();
    k1.dag = h1;
    
    let mut k2 = JcKernel::default();
    k2.dag = h2;
    
    assert_eq!(
        k1.state(&CounterFunctor),
        k2.state(&CounterFunctor)
    );
}
```

### Test: Convergence After Partition

```rust
#[test]
fn test_partition_heal() {
    let mut node_a = DistributedNode::new("A");
    let mut node_b = DistributedNode::new("B");
    
    // Partition: both advance independently
    for _ in 0..5 {
        let parents = node_a.history.frontier();
        node_a.append(Event::data("increment", json!(10), parents));
    }
    
    for _ in 0..3 {
        let parents = node_b.history.frontier();
        node_b.append(Event::data("increment", json!(20), parents));
    }
    
    let state_a_before = node_a.state(&CounterFunctor);
    let state_b_before = node_b.state(&CounterFunctor);
    
    // They disagree (partition)
    assert_ne!(state_a_before, state_b_before);
    
    // Heal
    node_a.sync_with(&node_b);
    node_b.sync_with(&node_a);
    
    // They agree (convergence)
    assert_eq!(
        node_a.state(&CounterFunctor),
        node_b.state(&CounterFunctor)
    );
}
```

---

## Normal Form Phases (Mental Model)

### Before Reduction

```
History with:
- Noop events
- Redundant relay chains
- Isomorphic sub-histories
- Stale hashes
```

### Phase A: Causal Closure
```
⚠️  Warn if any ancestor is missing
```

### Phase B: Canonical Ordering
```
Sort parents alphabetically (implicit via BTreeSet)
```

### Phase C1: Cone Merging
```
if cone_hash(A) == cone_hash(B):
    merge A and B (same ancestry)
```

### Phase C2: Chain Contraction
```
e1 → e2 → e3  (where e2 has no semantic value)
↓
e1 → e3  (skip e2)
```

### Phase C3: Noop Elimination
```
e1 → noop → e3
↓
e1 → e3  (noop removed, children reconnect)
```

### Phase D: Hash Stabilization
```
Recompute all cone hashes
(only hashes change, structure stable)
```

---

## Performance Tips

### ✅ DO

- **Append events asynchronously** — each append reduces automatically
- **Batch merges** — merge 100 events once vs 100 times
- **Derive state on-demand** — don't cache (always derivable)
- **Use BTreeSet parents** — ordering matters for determinism
- **Clone kernel for snapshots** — DAG is small relative to history

### ❌ DON'T

- **Store derived state** — defeats auditability guarantee
- **Mutate events** — they're immutable by design
- **Assume event ordering** — use `topological_order()`
- **Create cycles** — DAG invariant enforced at insert
- **Ignore frontier() for new events** — must be causally connected

---

## Built-in Functors

### KvFunctor

```rust
impl SemanticFunctor for KvFunctor {
    type State = HashMap<String, Value>;
}

// Event format: {"kind": "set", "value": {"key": "...", "val": ...}}
let e = kernel.new_event("set", json!({"key": "username", "val": "alice"}));
```

**Semantics**: Last write wins (causal order).

### CounterFunctor

```rust
impl SemanticFunctor for CounterFunctor {
    type State = i64;
}

// Event format: {"kind": "increment", "value": 42}
let e = kernel.new_event("increment", json!(100));
```

**Semantics**: Sum all increments (commutative).

### LogFunctor

```rust
impl SemanticFunctor for LogFunctor {
    type State = Vec<Value>;
}

// Event format: {"kind": "log", "value": "..."}
let e = kernel.new_event("log", json!("event happened"));
```

**Semantics**: Append-only list in causal order.

---

## Debugging Checklist

### Event Not in History

```rust
// Check if event exists
if kernel.dag.events.contains_key(&event_id) {
    println!("Found!");
} else {
    println!("Missing — was it appended?");
}

// Check parents
if let Some(event) = kernel.dag.events.get(&event_id) {
    println!("Parents: {:?}", event.parents);
}
```

### State Mismatch

```rust
// Derive state
let state = kernel.state(&MyFunctor);

// Check history
for event_id in kernel.dag.topological_order() {
    println!("{}: {:?}", event_id, kernel.dag.events[&event_id]);
}

// Trace functor logic
// (add debug output to interpret() method)
```

### Merge Not Converging

```rust
// Check both nodes see same events
let events_a: HashSet<_> = node_a.history.events.keys().cloned().collect();
let events_b: HashSet<_> = node_b.history.events.keys().cloned().collect();

if events_a == events_b {
    println!("Same events, but states differ!");
    // → Bug in functor
} else {
    println!("Different events");
    println!("A only: {:?}", events_a - &events_b);
    println!("B only: {:?}", events_b - &events_a);
}
```

### Normal Form Not Reducing

```rust
let size_before = kernel.history_size();
kernel.append(event);
let size_after = kernel.history_size();

if size_after >= size_before {
    println!("No reduction! (expected for new data events)");
} else {
    println!("Reduced {} → {} events", size_before, size_after);
}
```

---

## Cargo Commands

```bash
# Build
cargo build
cargo build --release

# Test
cargo test --lib                    # All unit tests
cargo test --lib my_test_name       # Single test
cargo test --lib -- --nocapture     # Show println! output

# Run demo
cargo run

# Check formatting
cargo fmt --check

# Lint
cargo clippy

# Generate docs
cargo doc --open
```

---

## Common Errors & Solutions

### Error: "Event has missing ancestors"

**Cause**: Parent set includes events not in DAG.

**Solution**:
```rust
// ❌ Wrong
let parents = vec![some_unknown_event_id];
let e = Event::data("kind", json!(value), BTreeSet::from_iter(parents));

// ✅ Correct
let parents = kernel.frontier();  // Current tip events
let e = kernel.new_event("kind", json!(value));
```

### Error: "Functor returns inconsistent state"

**Cause**: State depends on event order, but events are re-ordered by NF.

**Solution**:
```rust
// ❌ Wrong
impl SemanticFunctor for MyFunctor {
    fn interpret(&self, dag: &CausalDag) -> MyState {
        // Walk events in any order
        for event in dag.events.values() { ... }
    }
}

// ✅ Correct
impl SemanticFunctor for MyFunctor {
    fn interpret(&self, dag: &CausalDag) -> MyState {
        // Walk events in topological order
        for event_id in dag.topological_order() {
            let event = &dag.events[&event_id];
            // ...
        }
    }
}
```

### Error: "State diverges after merge"

**Cause**: Two nodes computed state from different normalized histories.

**Possible reasons**:
1. Functors are different (different semantic rules)
2. History is incomplete (ancestors missing)
3. Normal form bug (try manual reduction trace)

**Debug**:
```rust
// Ensure both nodes have identical events
node_a.sync_with(&node_b);
node_b.sync_with(&node_a);

// Check event equality
assert_eq!(
    node_a.history.events.len(),
    node_b.history.events.len()
);

// Check state
let state_a = node_a.state(&MyFunctor);
let state_b = node_b.state(&MyFunctor);

if state_a != state_b {
    // → Bug in functor or NF engine
}
```

---

## Architecture Decision Record (ADR)

### ADR-1: Why Content Addressing?

**Decision**: Use SHA-256(payload + parents) as event ID.

**Rationale**:
- Deterministic (same content → same ID)
- Deduplicates identical events
- Cryptographically secure
- Order-independent (parents sorted)

**Trade-offs**:
- Cannot rename/modify events
- Hash computation overhead (acceptable)

---

### ADR-2: Why Causal DAG Over Blockchain?

**Decision**: Maintain a DAG instead of a linear chain.

**Rationale**:
- Supports concurrent authorship (multiple tips)
- Scales better (merges without linear dependency)
- Mirrors actual causality (not artificial)

**Trade-offs**:
- More complex to reason about
- Requires topological sorting

---

### ADR-3: Why Never Store State?

**Decision**: Derive state from history on every query.

**Rationale**:
- Guarantees auditability (full history preserved)
- Enables replay (reconstruct any prior state)
- Prevents state corruption bugs

**Trade-offs**:
- CPU cost per state query (O(E))
- Can cache derived state if needed

**Mitigation**:
```rust
// Optional caching layer
struct CachedKernel {
    kernel: JcKernel,
    cached_state: Option<(usize, State)>,  // (history_size, state)
}

impl CachedKernel {
    fn state(&mut self) -> State {
        if Some(self.kernel.history_size()) == self.cached_state.as_ref().map(|c| c.0) {
            return self.cached_state.as_ref().unwrap().1.clone();
        }
        // Derive and cache
    }
}
```

---

## Glossary

| Term | Definition |
|------|-----------|
| **DAG** | Directed acyclic graph (events + causality) |
| **Event** | Immutable, content-addressed unit of change |
| **EventId** | SHA-256 hash of (payload + parents) |
| **Frontier** | Set of current tip events (no children yet) |
| **Normal Form** | Canonical representation after reduction |
| **Nf** | Normal form reduction operator |
| **σ (sigma)** | Semantic functor (user-supplied state projection) |
| **CRDT** | Conflict-free replicated data type (commutativity + associativity + idempotency) |
| **Confluence** | All reduction paths lead to same result |
| **Termination** | Reduction always finishes in finite steps |
| **Newman's Lemma** | Termination + local confluence ⟹ global confluence |

---

## Further Reading

- **FORMAL_THEORY.md.pdf**: Complete mathematical proofs
- **PROJECT_ANALYSIS.md**: Detailed component breakdown
- **IMPLEMENTATION_ROADMAP.md**: Future extensions
- **README.md**: Overview and quick start

---

**JC-Computation™ — State Derived, Never Stored.**
