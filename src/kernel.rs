//! JC Kernel — the main runtime object.
use crate::dag::CausalDag;
use crate::event::{Event, EventId, Payload};
use crate::nf::{NfConfig, NfStats, NfResult, NormalForm};
use std::collections::{BTreeSet, HashMap};

/// Semantic Functor
pub trait SemanticFunctor {
    type State;
    fn interpret(&self, dag: &CausalDag) -> Self::State;
}

pub trait FoldableFunctor: SemanticFunctor {
    fn empty(&self) -> Self::State;
    fn step(&self, state: &mut Self::State, event: &Event);
}

pub trait InvertibleFunctor: FoldableFunctor {
    fn unstep(&self, state: &mut Self::State, event: &Event);
}

pub trait DeltaFunctor {
    fn apply_delta(&mut self, delta: &crate::nf::NfDelta);
}

pub type DeltaSubscriber = Box<dyn Fn(&crate::nf::NfDelta) + Send + Sync>;

/// Kernel
pub struct JcKernel {
    pub dag: CausalDag,
    nf: NormalForm,
    pub total_stats: NfStats,
}

impl JcKernel {
    pub fn new(config: NfConfig) -> Self {
        let mut k = Self {
            dag: CausalDag::new(),
            nf: NormalForm::new(config),
            total_stats: NfStats::default(),
        };

        k.dag.insert(Event::genesis());
        k
    }

    /// Append event and normalize
    pub fn append(&mut self, event: Event) -> NfResult {
        self.dag.insert(event);

        let res = self.nf.reduce(&mut self.dag);

        // FIX: correct nested stats access
        self.total_stats.events_after = self.dag.len();
        self.total_stats.cones_merged += res.stats.cones_merged;
        self.total_stats.chains_contracted += res.stats.chains_contracted;
        self.total_stats.noops_eliminated += res.stats.noops_eliminated;

        res
    }

    pub fn state<F: SemanticFunctor>(&self, f: &F) -> F::State {
        f.interpret(&self.dag)
    }

    pub fn frontier(&self) -> BTreeSet<EventId> {
        self.dag.frontier()
    }

    pub fn history_size(&self) -> usize {
        self.dag.len()
    }

    pub fn new_event(&self, kind: impl Into<String>, value: serde_json::Value) -> Event {
        Event::data(kind, value, self.frontier())
    }

    pub fn new_noop(&self) -> Event {
        Event::noop(self.frontier())
    }
}

impl Default for JcKernel {
    fn default() -> Self {
        Self::new(NfConfig::default())
    }
}

// =====================================================
// Functors
// =====================================================

pub struct KvFunctor;

pub type KvState = HashMap<String, serde_json::Value>;

impl SemanticFunctor for KvFunctor {
    type State = KvState;

    fn interpret(&self, dag: &CausalDag) -> Self::State {
        let mut state = KvState::new();

        for id in dag.topological_order() {
            if let Some(e) = dag.events.get(&id) {
                if let Payload::Data { kind, value } = &e.payload {
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

/// Cached KV functor (FIXED)
#[derive(Default)]
pub struct CachedKvFunctor {
    cache: Option<(usize, KvState)>,
}

impl CachedKvFunctor {
    pub fn new() -> Self {
        Self { cache: None }
    }

    pub fn get(&mut self, kernel: &JcKernel) -> KvState {
        let size = kernel.history_size();

        if let Some((s, ref state)) = self.cache {
            if s == size {
                return state.clone();
            }
        }

        let state = KvFunctor.interpret(&kernel.dag);
        self.cache = Some((size, state.clone()));
        state
    }

    pub fn invalidate(&mut self) {
        self.cache = None;
    }
}

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
            .sum()
    }
}

// =====================================================
// Incremental kernel (fixed minimal)
// =====================================================

pub struct IncrementalStateKernel<F: FoldableFunctor> {
    pub inner: JcKernel,
    pub functor: F,
    pub cached_state: F::State,
    subscribers: Vec<DeltaSubscriber>,
}

impl<F: FoldableFunctor> IncrementalStateKernel<F> {
    pub fn new(inner: JcKernel, functor: F) -> Self {
        let state = functor.empty();
        Self {
            inner,
            functor,
            cached_state: state,
            subscribers: vec![],
        }
    }

    pub fn subscribe<T>(&mut self, f: T)
    where
        T: Fn(&crate::nf::NfDelta) + Send + Sync + 'static,
    {
        self.subscribers.push(Box::new(f));
    }
}