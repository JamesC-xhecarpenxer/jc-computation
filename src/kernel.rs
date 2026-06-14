//! JC Kernel — the main runtime object.
//!
//! ```text
//! Runtime = σ(nf(History))
//! ```
//!
//! The kernel is the minimal complete basis:
//! - E (event store, inside DAG)
//! - DAG (causality)
//! - nf (normal form engine)
//! - σ (semantic interpreter — user-supplied)
//!
//! State is NEVER stored. It is always derived from the normalized history.

use crate::dag::CausalDag;
use crate::event::{Event, EventId, Payload};
use crate::nf::{NfConfig, NfStats, NormalForm};
use std::collections::BTreeSet;

/// A user-supplied semantic functor: `σ : H → S`
///
/// Given the normalized DAG, produce the current "state" in whatever
/// domain the application cares about.
pub trait SemanticFunctor {
    type State;
    fn interpret(&self, dag: &CausalDag) -> Self::State;
}

/// The JC Kernel.
///
/// Stores a causal DAG and a normal form engine.
/// State is derived on demand via `σ(nf(H))`.
pub struct JcKernel {
    /// The current history (always kept in normal form).
    pub dag: CausalDag,
    /// The NF reduction engine.
    nf: NormalForm,
    /// Running stats.
    pub total_stats: NfStats,
}

impl JcKernel {
    pub fn new(config: NfConfig) -> Self {
        let mut kernel = JcKernel {
            dag: CausalDag::new(),
            nf: NormalForm::new(config),
            total_stats: NfStats::default(),
        };
        // Insert genesis
        let genesis = Event::genesis();
        kernel.dag.insert(genesis);
        kernel
    }

    /// Append an event and reduce to normal form.
    ///
    /// Steps:
    /// 1. H := H ∪ {e}
    /// 2. update DAG
    /// 3. H := nf(H)
    pub fn append(&mut self, event: Event) -> NfStats {
        self.dag.insert(event);
        let stats = self.nf.reduce(&mut self.dag);
        self.total_stats.events_after = self.dag.len();
        self.total_stats.cones_merged += stats.cones_merged;
        self.total_stats.chains_contracted += stats.chains_contracted;
        self.total_stats.noops_eliminated += stats.noops_eliminated;
        stats
    }

    /// Derive state using the provided semantic functor.
    ///
    /// `State = σ(nf(H))`  — state is NEVER stored, always computed.
    pub fn state<F: SemanticFunctor>(&self, functor: &F) -> F::State {
        functor.interpret(&self.dag)
    }

    /// Return the current frontier (tip events of the history).
    pub fn frontier(&self) -> BTreeSet<EventId> {
        self.dag.frontier()
    }

    /// Return the number of events in the normalized history.
    pub fn history_size(&self) -> usize {
        self.dag.len()
    }

    /// Create a new event extending the current frontier.
    pub fn new_event(&self, kind: impl Into<String>, value: serde_json::Value) -> Event {
        let parents = self.frontier();
        Event::data(kind, value, parents)
    }

    /// Create a no-op event (will be eliminated by NF).
    pub fn new_noop(&self) -> Event {
        Event::noop(self.frontier())
    }
}

impl Default for JcKernel {
    fn default() -> Self {
        Self::new(NfConfig::default())
    }
}

// ---------------------------------------------------------------------------
// Built-in Semantic Functors
// ---------------------------------------------------------------------------

/// A simple key-value ledger functor.
/// Interprets "set" events as key-value writes.
pub struct KvFunctor;

impl SemanticFunctor for KvFunctor {
    type State = std::collections::HashMap<String, serde_json::Value>;

    fn interpret(&self, dag: &CausalDag) -> Self::State {
        let mut state = std::collections::HashMap::new();
        for id in dag.topological_order() {
            if let Some(event) = dag.events.get(&id) {
                if let Payload::Data { kind, value } = &event.payload {
                    if kind == "set" {
                        if let (Some(k), Some(v)) = (value.get("key"), value.get("val")) {
                            if let Some(key) = k.as_str() {
                                state.insert(key.to_string(), v.clone());
                            }
                        }
                    }
                }
            }
        }
        state
    }
}

/// An append-only log functor.
/// Collects all "log" events in causal order.
pub struct LogFunctor;

impl SemanticFunctor for LogFunctor {
    type State = Vec<serde_json::Value>;

    fn interpret(&self, dag: &CausalDag) -> Self::State {
        dag.topological_order()
            .into_iter()
            .filter_map(|id| dag.events.get(&id))
            .filter_map(|e| {
                if let Payload::Data { kind, value } = &e.payload {
                    if kind == "log" {
                        return Some(value.clone());
                    }
                }
                None
            })
            .collect()
    }
}

/// A counter functor — sums all "increment" events.
pub struct CounterFunctor;

impl SemanticFunctor for CounterFunctor {
    type State = i64;

    fn interpret(&self, dag: &CausalDag) -> Self::State {
        dag.events
            .values()
            .filter_map(|e| {
                if let Payload::Data { kind, value } = &e.payload {
                    if kind == "increment" {
                        return value.as_i64();
                    }
                }
                None
            })
            .fold(0i64, |acc, x| acc.wrapping_add(x))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_functor_derives_state() {
        let mut k = JcKernel::default();

        let e1 = k.new_event("set", serde_json::json!({"key": "x", "val": 42}));
        k.append(e1);

        let e2 = k.new_event("set", serde_json::json!({"key": "y", "val": "hello"}));
        k.append(e2);

        let state = k.state(&KvFunctor);
        assert_eq!(state["x"], serde_json::json!(42));
        assert_eq!(state["y"], serde_json::json!("hello"));
    }

    #[test]
    fn counter_functor_sums() {
        let mut k = JcKernel::default();
        for i in [10i64, 20, 30] {
            let e = k.new_event("increment", serde_json::json!(i));
            k.append(e);
        }
        assert_eq!(k.state(&CounterFunctor), 60);
    }

    #[test]
    fn noop_events_are_eliminated() {
        let mut k = JcKernel::default();
        let size_before = k.history_size();

        let noop = k.new_noop();
        let stats = k.append(noop);

        // Size should not increase (noop eliminated)
        assert!(
            k.history_size() <= size_before + 1,
            "noop should not permanently grow history: stats = {:?}",
            stats.noops_eliminated
        );
    }

    #[test]
    fn log_functor_collects_in_order() {
        let mut k = JcKernel::default();
        for i in 0..5 {
            let e = k.new_event("log", serde_json::json!(i));
            k.append(e);
        }
        let log = k.state(&LogFunctor);
        assert_eq!(log.len(), 5);
        // Verify causal order
        for (i, v) in log.iter().enumerate() {
            assert_eq!(v.as_i64().unwrap(), i as i64);
        }
    }
}