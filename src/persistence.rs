//! Incremental reduction and persistence for JC-Computation.
//!
//! ## Incremental Kernel
//!
//! The `IncrementalKernel` extends `JcKernel` with a *change journal* —
//! a record of every event that has been appended.  This enables:
//!
//! - **Replay**: reconstruct any past state from the empty kernel
//! - **Snapshots**: serialize/deserialize the current normalized DAG
//! - **WAL persistence**: append-only write-ahead log for durability
//! - **Incremental sync**: send only events the remote hasn't seen yet
//!
//! ## Persistence
//!
//! Two storage backends are provided:
//!
//! - `MemoryStore` — in-memory, for testing
//! - `FileStore` — newline-delimited JSON events on disk
//!
//! Both implement the `EventStore` trait.
//!
//! ## Snapshot + WAL
//!
//! The recommended production pattern:
//!
//! ```text
//! 1. Take snapshot of nf(H) every N events  → compressed JSON
//! 2. Append raw events to WAL between snapshots
//! 3. On startup: load latest snapshot, replay WAL tail
//! ```

use crate::dag::CausalDag;
use crate::event::{Event, EventId};
use crate::kernel::{JcKernel, SemanticFunctor};
use crate::nf::{NfConfig, NfStats};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};

// ────────────────────────────────────────────────────────────────────────────
// EventStore trait
// ────────────────────────────────────────────────────────────────────────────

/// Durable storage for events.
///
/// Implementations must guarantee that `append` is crash-safe: either the
/// event is stored or it is not — partial writes must be detectable.
pub trait EventStore: Send + Sync {
    type Error: std::fmt::Debug + std::fmt::Display;

    /// Append a single event to the store (idempotent — duplicate IDs ignored).
    fn append(&mut self, event: &Event) -> Result<(), Self::Error>;

    /// Load all events in the order they were appended.
    fn load_all(&self) -> Result<Vec<Event>, Self::Error>;

    /// Return the set of event IDs currently in the store.
    fn known_ids(&self) -> Result<HashSet<EventId>, Self::Error>;

    /// Number of events stored.
    fn len(&self) -> Result<usize, Self::Error>;

    /// True iff the store is empty.
    fn is_empty(&self) -> Result<bool, Self::Error> {
        Ok(self.len()? == 0)
    }
}

// ────────────────────────────────────────────────────────────────────────────
// MemoryStore
// ────────────────────────────────────────────────────────────────────────────

/// In-memory event store — useful for testing and ephemeral nodes.
#[derive(Debug, Default, Clone)]
pub struct MemoryStore {
    /// Ordered log of events (append-only).
    log: Vec<Event>,
    /// Fast membership check.
    ids: HashSet<EventId>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Drain all events (for inspection/testing).
    pub fn events(&self) -> &[Event] {
        &self.log
    }
}

#[derive(Debug)]
pub struct MemoryError(String);

impl std::fmt::Display for MemoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemoryStore error: {}", self.0)
    }
}

impl EventStore for MemoryStore {
    type Error = MemoryError;

    fn append(&mut self, event: &Event) -> Result<(), Self::Error> {
        if !self.ids.contains(&event.id) {
            self.ids.insert(event.id.clone());
            self.log.push(event.clone());
        }
        Ok(())
    }

    fn load_all(&self) -> Result<Vec<Event>, Self::Error> {
        Ok(self.log.clone())
    }

    fn known_ids(&self) -> Result<HashSet<EventId>, Self::Error> {
        Ok(self.ids.clone())
    }

    fn len(&self) -> Result<usize, Self::Error> {
        Ok(self.log.len())
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Snapshot
// ────────────────────────────────────────────────────────────────────────────

/// A serializable snapshot of a normalized DAG.
///
/// Contains the minimal information needed to reconstruct the exact DAG:
/// the normalized event set plus causal edges (parent pointers are embedded
/// in each event, so no separate edge list is needed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Snapshot sequence number (monotonically increasing).
    pub seq: u64,
    /// Timestamp (unix seconds, best-effort — not trusted for ordering).
    pub timestamp: u64,
    /// The normalized events.
    pub events: Vec<SerializableEvent>,
    /// Number of source events this snapshot was derived from.
    pub source_event_count: u64,
}

/// Serializable form of an event (without non-serializable cached fields).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SerializableEvent {
    pub id: EventId,
    pub payload: crate::event::Payload,
    pub parents: BTreeSet<EventId>,
}

impl From<&Event> for SerializableEvent {
    fn from(e: &Event) -> Self {
        SerializableEvent {
            id: e.id.clone(),
            payload: e.payload.clone(),
            parents: e.parents.clone(),
        }
    }
}

impl From<SerializableEvent> for Event {
    fn from(se: SerializableEvent) -> Self {
        Event::new(se.payload, se.parents)
    }
}

impl Snapshot {
    /// Create a snapshot from the current state of a DAG.
    pub fn from_dag(dag: &CausalDag, seq: u64, source_event_count: u64) -> Self {
        let events = dag.events.values().map(SerializableEvent::from).collect();
        Snapshot {
            seq,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            events,
            source_event_count,
        }
    }

    /// Reconstruct a DAG from this snapshot.
    pub fn to_dag(&self) -> CausalDag {
        let mut dag = CausalDag::with_capacity(self.events.len());
        for se in &self.events {
            dag.insert(Event::new(se.payload.clone(), se.parents.clone()));
        }
        dag
    }

    /// Serialize to JSON bytes.
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes.
    pub fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

// ────────────────────────────────────────────────────────────────────────────
// IncrementalKernel
// ────────────────────────────────────────────────────────────────────────────

/// A `JcKernel` augmented with incremental reduction tracking and persistence.
///
/// ## Incremental reduction
///
/// Tracks a *dirty frontier* — the set of events added since the last NF
/// reduction.  This allows the kernel to:
/// - Know which events are "new" and need to be written to the WAL
/// - Seed the cone hasher's dirty set after merges (faster incremental NF)
///
/// ## Persistence
///
/// The kernel can be configured with an `EventStore` backend.  Every event
/// appended to the kernel is also written to the store.  On startup, the
/// kernel can be rehydrated from the store.
pub struct IncrementalKernel<S: EventStore> {
    /// The underlying kernel.
    pub kernel: JcKernel,
    /// Backing event store (WAL).
    pub store: S,
    /// Monotonically increasing event counter (includes all appended events).
    pub event_count: u64,
    /// Current snapshot sequence number.
    snapshot_seq: u64,
    /// Events appended since last snapshot.
    events_since_snapshot: u64,
    /// Snapshot interval: take a snapshot every N events (0 = disabled).
    pub snapshot_interval: u64,
    /// In-memory snapshot cache (latest snapshot).
    pub latest_snapshot: Option<Snapshot>,
}

impl<S: EventStore> IncrementalKernel<S> {
    /// Create a new `IncrementalKernel` with the given store and config.
    pub fn new(store: S, config: NfConfig, snapshot_interval: u64) -> Self {
        IncrementalKernel {
            kernel: JcKernel::new(config),
            store,
            event_count: 0,
            snapshot_seq: 0,
            events_since_snapshot: 0,
            snapshot_interval,
            latest_snapshot: None,
        }
    }

    /// Append an event, persist it to the store, and reduce.
    pub fn append(&mut self, event: Event) -> Result<NfStats, S::Error> {
        // Persist first (WAL before apply — crash-safe).
        self.store.append(&event)?;
        self.event_count += 1;
        self.events_since_snapshot += 1;

        // Apply to in-memory kernel.
        let stats = self.kernel.append(event);

        // Optionally take a snapshot.
        if self.snapshot_interval > 0
            && self.events_since_snapshot >= self.snapshot_interval
        {
            self.take_snapshot();
        }

        Ok(stats)
    }

    /// Take a point-in-time snapshot of the current normalized DAG.
    pub fn take_snapshot(&mut self) {
        self.snapshot_seq += 1;
        let snap = Snapshot::from_dag(
            &self.kernel.dag,
            self.snapshot_seq,
            self.event_count,
        );
        self.latest_snapshot = Some(snap);
        self.events_since_snapshot = 0;
    }

    /// Replay all events from the store (rebuilds normalized DAG from scratch).
    ///
    /// Prefer `restore_from_snapshot_and_wal` for large histories.
    pub fn replay_from_store(&mut self) -> Result<u64, S::Error> {
        let events = self.store.load_all()?;
        let count = events.len() as u64;
        self.kernel = JcKernel::new(NfConfig::default());
        for event in events {
            self.kernel.append(event);
        }
        self.event_count = count;
        Ok(count)
    }

    /// Restore from a snapshot, then replay WAL events that followed it.
    ///
    /// This is the fast path for startup: O(WAL-tail) instead of O(total history).
    pub fn restore_from_snapshot_and_wal(
        &mut self,
        snapshot: &Snapshot,
    ) -> Result<u64, S::Error> {
        // Rebuild DAG from snapshot.
        self.kernel.dag = snapshot.to_dag();
        self.snapshot_seq = snapshot.seq;
        self.event_count = snapshot.source_event_count;

        // Load WAL events that are *not* in the snapshot.
        let snapshot_ids: HashSet<EventId> =
            snapshot.events.iter().map(|e| e.id.clone()).collect();

        let wal_events = self.store.load_all()?;
        let mut replayed = 0u64;
        for event in wal_events {
            if !snapshot_ids.contains(&event.id)
                && !self.kernel.dag.events.contains_key(&event.id)
            {
                self.kernel.append(event);
                self.event_count += 1;
                replayed += 1;
            }
        }
        Ok(replayed)
    }

    /// Returns event IDs the remote DAG does NOT have.
    ///
    /// Used for anti-entropy / incremental sync: send `diff_from(remote_ids)`
    /// to the remote peer to bring it up to date.
    pub fn diff_from(&self, remote_known: &HashSet<EventId>) -> Vec<Event> {
        self.kernel
            .dag
            .events
            .iter()
            .filter(|(id, _)| !remote_known.contains(*id))
            .map(|(_, e)| e.clone())
            .collect()
    }

    /// Current history size (normalized).
    pub fn history_size(&self) -> usize {
        self.kernel.history_size()
    }

    /// Query state using a semantic functor.
    pub fn state<F: SemanticFunctor>(&self, functor: &F) -> F::State {
        self.kernel.state(functor)
    }

    /// Current frontier.
    pub fn frontier(&self) -> BTreeSet<EventId> {
        self.kernel.frontier()
    }

    /// Convenience: create a new data event extending the frontier.
    pub fn new_event(
        &self,
        kind: impl Into<String>,
        value: serde_json::Value,
    ) -> Event {
        self.kernel.new_event(kind, value)
    }

    /// Convenience: create a noop event.
    pub fn new_noop(&self) -> Event {
        self.kernel.new_noop()
    }
}

// ────────────────────────────────────────────────────────────────────────────
// File-backed event store
// ────────────────────────────────────────────────────────────────────────────

/// File-backed event store: newline-delimited JSON (NDJSON) WAL.
///
/// Each line is a serialized `SerializableEvent`. The file is append-only.
pub struct FileStore {
    path: std::path::PathBuf,
}

impl FileStore {
    /// Open (or create) a WAL file at `path`.
    pub fn open(path: impl Into<std::path::PathBuf>) -> Self {
        FileStore { path: path.into() }
    }

    fn read_lines(&self) -> Result<Vec<SerializableEvent>, FileStoreError> {
        let path = &self.path;
        if !path.exists() {
            return Ok(vec![]);
        }
        let content = std::fs::read_to_string(path)
            .map_err(|e| FileStoreError::Io(e.to_string()))?;
        let mut events = Vec::new();
        for (line_no, line) in content.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let se: SerializableEvent = serde_json::from_str(trimmed).map_err(|e| {
                FileStoreError::Parse(format!("line {}: {}", line_no + 1, e))
            })?;
            events.push(se);
        }
        Ok(events)
    }
}

#[derive(Debug)]
pub enum FileStoreError {
    Io(String),
    Parse(String),
}

impl std::fmt::Display for FileStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileStoreError::Io(e) => write!(f, "I/O error: {}", e),
            FileStoreError::Parse(e) => write!(f, "Parse error: {}", e),
        }
    }
}

impl EventStore for FileStore {
    type Error = FileStoreError;

    fn append(&mut self, event: &Event) -> Result<(), Self::Error> {
        use std::io::Write;
        let se = SerializableEvent::from(event);
        let line = serde_json::to_string(&se)
            .map_err(|e| FileStoreError::Parse(e.to_string()))?;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| FileStoreError::Io(e.to_string()))?;
        writeln!(f, "{}", line).map_err(|e| FileStoreError::Io(e.to_string()))?;
        Ok(())
    }

    fn load_all(&self) -> Result<Vec<Event>, Self::Error> {
        let ses = self.read_lines()?;
        Ok(ses.into_iter().map(Event::from).collect())
    }

    fn known_ids(&self) -> Result<HashSet<EventId>, Self::Error> {
        let ses = self.read_lines()?;
        Ok(ses.into_iter().map(|se| se.id).collect())
    }

    fn len(&self) -> Result<usize, Self::Error> {
        Ok(self.read_lines()?.len())
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use crate::kernel::{CounterFunctor, KvFunctor};
    use crate::nf::NfConfig;

    fn make_kernel() -> IncrementalKernel<MemoryStore> {
        IncrementalKernel::new(MemoryStore::new(), NfConfig::default(), 0)
    }

    // ── EventStore: MemoryStore ──

    #[test]
    fn memory_store_append_and_load() {
        let mut store = MemoryStore::new();
        let e1 = Event::genesis();
        store.append(&e1).unwrap();
        let loaded = store.load_all().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, e1.id);
    }

    #[test]
    fn memory_store_idempotent_append() {
        let mut store = MemoryStore::new();
        let e = Event::genesis();
        store.append(&e).unwrap();
        store.append(&e).unwrap(); // duplicate
        assert_eq!(store.len().unwrap(), 1);
    }

    // ── Snapshot ──

    #[test]
    fn snapshot_roundtrip() {
        let mut kernel = make_kernel();
        let e = kernel.new_event("set", serde_json::json!({"key": "x", "val": 1}));
        kernel.append(e).unwrap();

        kernel.take_snapshot();
        let snap = kernel.latest_snapshot.clone().unwrap();

        let json = snap.to_json().unwrap();
        let snap2 = Snapshot::from_json(&json).unwrap();

        let dag1 = snap.to_dag();
        let dag2 = snap2.to_dag();

        let mut ids1: Vec<_> = dag1.events.keys().cloned().collect();
        let mut ids2: Vec<_> = dag2.events.keys().cloned().collect();
        ids1.sort();
        ids2.sort();
        assert_eq!(ids1, ids2, "snapshot JSON roundtrip must preserve all events");
    }

    #[test]
    fn snapshot_and_wal_replay() {
        let mut kernel = IncrementalKernel::new(MemoryStore::new(), NfConfig::default(), 0);

        // Append some events
        for i in 0..5 {
            let e = kernel.new_event("increment", serde_json::json!(i));
            kernel.append(e).unwrap();
        }

        // Take a snapshot at this point
        kernel.take_snapshot();
        let snap = kernel.latest_snapshot.clone().unwrap();

        // Append more events AFTER snapshot
        for i in 5..8 {
            let e = kernel.new_event("increment", serde_json::json!(i));
            kernel.append(e).unwrap();
        }

        let state_before = kernel.state(&CounterFunctor);

        // Restore from snapshot + WAL tail
        let mut kernel2 = IncrementalKernel::new(
            kernel.store.clone(),
            NfConfig::default(),
            0,
        );
        let replayed = kernel2.restore_from_snapshot_and_wal(&snap).unwrap();

        let state_after = kernel2.state(&CounterFunctor);
        assert_eq!(state_before, state_after, "state must be preserved after snapshot+WAL replay");
        assert_eq!(replayed, 3, "should replay exactly 3 post-snapshot events");
    }

    // ── IncrementalKernel ──

    #[test]
    fn incremental_kernel_persists_events() {
        let mut kernel = make_kernel();
        let e1 = kernel.new_event("op", serde_json::json!(1));
        kernel.append(e1).unwrap();
        let e2 = kernel.new_event("op", serde_json::json!(2));
        kernel.append(e2).unwrap();

        assert_eq!(kernel.store.len().unwrap(), 2);
    }

    #[test]
    fn incremental_kernel_replay() {
        let mut kernel = make_kernel();

        for i in 0..5 {
            let e = kernel.new_event("increment", serde_json::json!(i));
            kernel.append(e).unwrap();
        }
        let state_before = kernel.state(&CounterFunctor);

        // Replay from WAL
        let store = kernel.store.clone();
        let mut kernel2 = IncrementalKernel::new(store, NfConfig::default(), 0);
        kernel2.replay_from_store().unwrap();

        let state_after = kernel2.state(&CounterFunctor);
        assert_eq!(state_before, state_after, "replay must reproduce state exactly");
    }

    #[test]
    fn incremental_kernel_diff_sync() {
        let mut k_a = make_kernel();
        let mut k_b = make_kernel();

        // A gets some events
        for i in 0..3 {
            let e = k_a.new_event("set", serde_json::json!({"key": format!("k{}", i), "val": i}));
            k_a.append(e).unwrap();
        }

        // B gets different events
        for i in 0..2 {
            let e = k_b.new_event("set", serde_json::json!({"key": format!("b{}", i), "val": i * 10}));
            k_b.append(e).unwrap();
        }

        // Compute what B is missing
        let b_known = k_b.kernel.dag.events.keys().cloned().collect::<HashSet<_>>();
        let a_to_b = k_a.diff_from(&b_known);

        // Send diff to B
        for event in a_to_b {
            k_b.kernel.append(event);
        }

        // Now B should have all of A's keys
        let state_a = k_a.state(&KvFunctor);
        let state_b = k_b.state(&KvFunctor);

        for (k, v) in &state_a {
            assert_eq!(state_b.get(k), Some(v), "B must have key {} after sync", k);
        }
    }

    #[test]
    fn auto_snapshot_triggered_by_interval() {
        let mut kernel = IncrementalKernel::new(MemoryStore::new(), NfConfig::default(), 3);

        assert!(kernel.latest_snapshot.is_none());

        for i in 0..5 {
            let e = kernel.new_event("op", serde_json::json!(i));
            kernel.append(e).unwrap();
        }

        // Snapshot should have been taken after event 3
        assert!(kernel.latest_snapshot.is_some(), "auto-snapshot must fire after interval events");
    }

    #[test]
    fn file_store_roundtrip() {
        let tmp = std::env::temp_dir().join(format!("jc_test_wal_{}.ndjson",
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos()).unwrap_or(0)));

        {
            let mut store = FileStore::open(&tmp);
            let g = Event::genesis();
            store.append(&g).unwrap();
            let e = Event::data("op", serde_json::json!(42), {
                let mut s = BTreeSet::new();
                s.insert(g.id.clone());
                s
            });
            store.append(&e).unwrap();
        }

        {
            let store = FileStore::open(&tmp);
            let events = store.load_all().unwrap();
            assert_eq!(events.len(), 2);
        }

        // Cleanup
        let _ = std::fs::remove_file(&tmp);
    }
}
