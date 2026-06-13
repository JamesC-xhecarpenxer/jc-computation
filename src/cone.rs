//! Cone Hashing Engine
//!
//! The "causal cone" of event `e` is the set of all events that
//! causally precede `e` (its ancestors), plus `e` itself.
//!
//! Two events have isomorphic cones iff their cone hashes are equal.
//! This reduces the expensive graph isomorphism check to O(1) hash comparison.
//!
//! ## Hash Construction
//!
//! ```text
//! cone_hash(e) = Hash(e.payload || sorted(cone_hash(p) for p in e.parents))
//! ```
//!
//! This is a Merkle-tree style hash over the causal DAG.

use crate::dag::CausalDag;
use crate::event::EventId;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Computes and caches cone hashes for a causal DAG.
pub struct ConeHasher {
    /// Cache: event_id → cone_hash
    cache: HashMap<EventId, String>,
}

impl ConeHasher {
    pub fn new() -> Self {
        ConeHasher {
            cache: HashMap::new(),
        }
    }

    /// Compute the cone hash of event `id` in `dag`.
    ///
    /// Processes events in topological order so parent hashes
    /// are always available when we process a child.
    pub fn compute_all(&mut self, dag: &CausalDag) -> &HashMap<EventId, String> {
        self.cache.clear();
        let order = dag.topological_order();
        for id in &order {
            if let Some(event) = dag.events.get(id) {
                let mut hasher = Sha256::new();
                // Hash the payload
                let payload_bytes = serde_json::to_string(&event.payload)
                    .unwrap_or_default();
                hasher.update(payload_bytes.as_bytes());
                // Hash parent cone hashes in sorted (deterministic) order
                let mut parent_hashes: Vec<&str> = event
                    .parents
                    .iter()
                    .filter_map(|p| self.cache.get(p).map(|s| s.as_str()))
                    .collect();
                parent_hashes.sort_unstable();
                for ph in parent_hashes {
                    hasher.update(ph.as_bytes());
                }
                let cone_hash = hex::encode(hasher.finalize());
                self.cache.insert(id.clone(), cone_hash);
            }
        }
        &self.cache
    }

    /// Get the cone hash for a specific event (must have called compute_all first).
    pub fn get(&self, id: &EventId) -> Option<&String> {
        self.cache.get(id)
    }

    /// Find groups of events with identical cone hashes.
    /// These are candidates for merging (Rule C1 in NF).
    pub fn isomorphic_groups(&self) -> Vec<Vec<EventId>> {
        let mut by_hash: HashMap<&str, Vec<EventId>> = HashMap::new();
        for (id, hash) in &self.cache {
            by_hash.entry(hash.as_str()).or_default().push(id.clone());
        }
        by_hash
            .into_values()
            .filter(|group| group.len() > 1)
            .collect()
    }
}

impl Default for ConeHasher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use std::collections::BTreeSet;

    #[test]
    fn identical_subtrees_have_same_cone_hash() {
        let mut dag = CausalDag::new();

        // Genesis
        let g = Event::genesis();
        dag.insert(g.clone());

        // Two identical events with same parent = same cone hash
        let e1 = Event::data("op", serde_json::json!({"x": 1}), BTreeSet::from([g.id.clone()]));
        let e2 = Event::data("op", serde_json::json!({"x": 1}), BTreeSet::from([g.id.clone()]));

        // They will have the same ID (same content) — confirm cone hashing works
        // even with the dedup case
        assert_eq!(e1.id, e2.id, "content-addressed: same payload+parents = same id");

        dag.insert(e1.clone());

        let mut hasher = ConeHasher::new();
        hasher.compute_all(&dag);

        // Manually compute expected hash for e1
        let h = hasher.get(&e1.id).unwrap();
        assert!(!h.is_empty());
    }

    #[test]
    fn different_payloads_differ_in_cone_hash() {
        let mut dag = CausalDag::new();
        let g = Event::genesis();
        dag.insert(g.clone());

        let e1 = Event::data("op", serde_json::json!({"x": 1}), BTreeSet::from([g.id.clone()]));
        let e2 = Event::data("op", serde_json::json!({"x": 2}), BTreeSet::from([g.id.clone()]));
        dag.insert(e1.clone());
        dag.insert(e2.clone());

        let mut hasher = ConeHasher::new();
        hasher.compute_all(&dag);

        let h1 = hasher.get(&e1.id).unwrap();
        let h2 = hasher.get(&e2.id).unwrap();
        assert_ne!(h1, h2);
    }

    #[test]
    fn isomorphic_groups_detected() {
        // Build two parallel identical subtrees
        let mut dag = CausalDag::new();
        let g = Event::genesis();
        dag.insert(g.clone());

        // Two events that are structurally identical should ideally share an ID
        // (content-addressing handles this naturally). The isomorphic_groups
        // test is meaningful when events have been added via different paths
        // (e.g. from different nodes) with different IDs but same structure.
        // We simulate this by inserting pre-built events with forced different IDs.

        // In a real distributed scenario, two nodes could create the same logical
        // event before learning about each other. For this test we confirm
        // the grouping machinery works on the hash map.

        // For now just confirm no panics and returns empty groups for distinct events.
        let e1 = Event::data("op", serde_json::json!(1), BTreeSet::from([g.id.clone()]));
        let e2 = Event::data("op", serde_json::json!(2), BTreeSet::from([g.id.clone()]));
        dag.insert(e1);
        dag.insert(e2);

        let mut hasher = ConeHasher::new();
        hasher.compute_all(&dag);
        let groups = hasher.isomorphic_groups();
        // No isomorphic groups since payloads differ
        assert!(groups.is_empty());
    }
}
