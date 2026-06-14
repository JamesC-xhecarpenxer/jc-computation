//! Immutable event model.
//!
//! Events are the sole primitive of JC-Computation.
//! Everything else (state, messages, nodes) is derived.
//!
//! ## Optimization notes (v3.2)
//!
//! `payload_bytes` is a cached serialization of the payload, computed once
//! at construction and reused during cone hashing.  The hot path in
//! `ConeHasher::hash_in_order` / `hash_levels_parallel` called
//! `serde_json::to_string(&event.payload)` on *every* event *every*
//! hashing pass — at 1 M events that's 1 M allocations per iteration.
//! Caching it here reduces that to one allocation per event lifetime.
//!
//! The cache is invalidated and recomputed in `recompute_id` (called after
//! parent-set mutations in `compact_tombstones` and `phase_c1_merge_cones`).

use sha2::{Digest, Sha256};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Content-addressed event identifier (SHA-256 hex string).
pub type EventId = String;

/// Arbitrary payload — the semantic content of an event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Payload {
    /// A typed domain event with a kind tag and JSON value.
    Data { kind: String, value: serde_json::Value },
    /// A no-op event (can be eliminated during NF).
    Noop,
    /// Genesis / root event.
    Genesis,
}

impl Payload {
    pub fn is_noop(&self) -> bool {
        matches!(self, Payload::Noop)
    }
}

/// An immutable, content-addressed causal event.
///
/// The `id` is derived from `(parents, payload)` — it is
/// a Merkle node in the causal DAG.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Content-addressed identifier: hash(sorted_parents || payload)
    pub id: EventId,
    /// Payload carried by this event.
    pub payload: Payload,
    /// Direct causal predecessors (parent event IDs).
    pub parents: BTreeSet<EventId>,
    /// Cached JSON serialization of `payload` for cone-hashing hot path.
    /// Always in sync with `payload`; recomputed in `recompute_id`.
    #[serde(skip)]
    pub(crate) payload_bytes: Vec<u8>,
    #[serde(skip)]
    pub(crate) cached_payload_hash: String,
    #[serde(skip)]
    pub(crate) cached_parent_set_hash: String,
}

impl Event {
    /// Create a new event, computing its ID from content.
    pub fn new(payload: Payload, parents: BTreeSet<EventId>) -> Self {
        let payload_hash = Self::hash_payload(&payload);
        let parent_hash = Self::hash_parents(&parents);
        let id = Self::compute_id_from_hashes(&payload_hash,&parent_hash);
        let payload_bytes = serde_json::to_string(&payload)
            .unwrap_or_default()
            .into_bytes();
        Event { id, payload, parents, payload_bytes, cached_payload_hash: payload_hash, cached_parent_set_hash: parent_hash }
    }

    /// Create a genesis event (no parents).
    pub fn genesis() -> Self {
        Self::new(Payload::Genesis, BTreeSet::new())
    }

    /// Create a data event.
    pub fn data(kind: impl Into<String>, value: serde_json::Value, parents: BTreeSet<EventId>) -> Self {
        Self::new(Payload::Data { kind: kind.into(), value }, parents)
    }

    /// Create a no-op event (eligible for NF elimination).
    pub fn noop(parents: BTreeSet<EventId>) -> Self {
        Self::new(Payload::Noop, parents)
    }

    
    fn hash_payload(payload:&Payload)->String{ let mut h=Sha256::new(); h.update(serde_json::to_string(payload).unwrap_or_default().as_bytes()); hex::encode(h.finalize()) }
    fn hash_parents(parents:&BTreeSet<EventId>)->String{ let mut h=Sha256::new(); for p in parents { h.update(p.as_bytes()); } hex::encode(h.finalize()) }
    fn compute_id_from_hashes(payload_hash:&str,parent_hash:&str)->EventId{ let mut h=Sha256::new(); h.update(payload_hash.as_bytes()); h.update(parent_hash.as_bytes()); hex::encode(h.finalize()) }

    /// Compute the content-addressed ID for given payload and parents.
    pub fn compute_id(payload: &Payload, parents: &BTreeSet<EventId>) -> EventId { let ph=Self::hash_payload(payload); let pa=Self::hash_parents(parents); Self::compute_id_from_hashes(&ph,&pa) }

    /// Recompute and update this event's ID (used after structural changes).
    /// Also refreshes the `payload_bytes` cache.
    pub fn recompute_id(&mut self) {
        let new_payload_hash=Self::hash_payload(&self.payload);
        let new_parent_hash=Self::hash_parents(&self.parents);
        if new_payload_hash==self.cached_payload_hash && new_parent_hash==self.cached_parent_set_hash { return; }
        self.cached_payload_hash=new_payload_hash.clone();
        self.cached_parent_set_hash=new_parent_hash.clone();
        self.id=Self::compute_id_from_hashes(&new_payload_hash,&new_parent_hash);
        self.payload_bytes=serde_json::to_string(&self.payload).unwrap_or_default().into_bytes();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn genesis_is_deterministic() {
        let g1 = Event::genesis();
        let g2 = Event::genesis();
        assert_eq!(g1.id, g2.id);
    }

    #[test]
    fn content_addressing_works() {
        let g = Event::genesis();
        let e1 = Event::data(
            "deposit",
            serde_json::json!({"amount": 100}),
            BTreeSet::from([g.id.clone()]),
        );
        let e2 = Event::data(
            "deposit",
            serde_json::json!({"amount": 100}),
            BTreeSet::from([g.id.clone()]),
        );
        assert_eq!(e1.id, e2.id, "same content = same ID");
    }

    #[test]
    fn different_payloads_differ() {
        let g = Event::genesis();
        let parents = BTreeSet::from([g.id.clone()]);
        let e1 = Event::data("deposit", serde_json::json!({"amount": 100}), parents.clone());
        let e2 = Event::data("deposit", serde_json::json!({"amount": 200}), parents);
        assert_ne!(e1.id, e2.id);
    }

    #[test]
    fn noop_detected() {
        let e = Event::noop(BTreeSet::new());
        assert!(e.payload.is_noop());
    }

    #[test]
    fn payload_bytes_matches_serde() {
        let g = Event::genesis();
        let e = Event::data("op", serde_json::json!(42), BTreeSet::from([g.id.clone()]));
        let expected = serde_json::to_string(&e.payload).unwrap().into_bytes();
        assert_eq!(e.payload_bytes, expected, "payload_bytes cache must match serde output");
    }

    #[test]
    fn recompute_id_refreshes_payload_bytes() {
        let g = Event::genesis();
        let mut e = Event::data("op", serde_json::json!(1), BTreeSet::from([g.id.clone()]));
        // Manually mutate parents (as compact_tombstones does) and recompute
        e.parents.clear();
        e.recompute_id();
        let expected = serde_json::to_string(&e.payload).unwrap().into_bytes();
        assert_eq!(e.payload_bytes, expected);
    }
}