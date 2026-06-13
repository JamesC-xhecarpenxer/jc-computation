//! Immutable event model.
//!
//! Events are the sole primitive of JC-Computation.
//! Everything else (state, messages, nodes) is derived.

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
}

impl Event {
    /// Create a new event, computing its ID from content.
    pub fn new(payload: Payload, parents: BTreeSet<EventId>) -> Self {
        let id = Self::compute_id(&payload, &parents);
        Event { id, payload, parents }
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

    /// Compute the content-addressed ID for given payload and parents.
    pub fn compute_id(payload: &Payload, parents: &BTreeSet<EventId>) -> EventId {
        let mut hasher = Sha256::new();
        // Hash payload deterministically
        let payload_str = serde_json::to_string(payload).unwrap_or_default();
        hasher.update(payload_str.as_bytes());
        // Hash parents in sorted order (BTreeSet ensures determinism)
        for parent in parents {
            hasher.update(parent.as_bytes());
        }
        hex::encode(hasher.finalize())
    }

    /// Recompute and update this event's ID (used after structural changes).
    pub fn recompute_id(&mut self) {
        self.id = Self::compute_id(&self.payload, &self.parents);
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
}
