//! Causal DAG — the structural backbone of a JC history.
//!
//! Maintains:
//! - Forward index: event_id → Event
//! - Children index: event_id → Set<child_id>
//! - Ancestry cache: event_id → Set<ancestor_id> (memoized)

use crate::event::{Event, EventId};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

/// A causal DAG representing a history `H = (E, ≺, λ)`.
#[derive(Debug, Clone)]
pub struct CausalDag {
    /// Primary event store.
    pub events: HashMap<EventId, Event>,
    /// Reverse index: children of each event.
    pub children: HashMap<EventId, BTreeSet<EventId>>,
    /// Memoized ancestry sets (cleared on structural change).
    ancestry_cache: HashMap<EventId, HashSet<EventId>>,
}

impl CausalDag {
    pub fn new() -> Self {
        CausalDag {
            events: HashMap::new(),
            children: HashMap::new(),
            ancestry_cache: HashMap::new(),
        }
    }

    /// Insert an event. Returns false if already present.
    pub fn insert(&mut self, event: Event) -> bool {
        if self.events.contains_key(&event.id) {
            return false;
        }
        // Register this event as a child of each parent.
        for parent in &event.parents {
            self.children
                .entry(parent.clone())
                .or_default()
                .insert(event.id.clone());
        }
        self.events.insert(event.id.clone(), event);
        // Ancestry cache must be invalidated for descendants.
        self.ancestry_cache.clear();
        true
    }

    /// Remove an event by ID. Reconnects its parents to its children.
    pub fn remove(&mut self, id: &EventId) -> Option<Event> {
        let event = self.events.remove(id)?;
        // Remove from children indices
        for parent in &event.parents {
            if let Some(sibs) = self.children.get_mut(parent) {
                sibs.remove(id);
                // Reconnect: each child of `id` should now reference each parent of `id`
            }
        }
        // Redirect children to grandparents
        if let Some(my_children) = self.children.remove(id) {
            for child_id in &my_children {
                if let Some(child) = self.events.get_mut(child_id) {
                    child.parents.remove(id);
                    child.parents.extend(event.parents.clone());
                    child.recompute_id();
                }
                // Update grandparent → child links
                for parent in &event.parents {
                    self.children
                        .entry(parent.clone())
                        .or_default()
                        .insert(child_id.clone());
                }
            }
        }
        self.ancestry_cache.clear();
        Some(event)
    }

    /// Merge `other` into `self` (union of event sets).
    pub fn union_with(&mut self, other: &CausalDag) {
        for event in other.events.values() {
            self.insert(event.clone());
        }
    }

    /// Compute the set of all ancestors of `id` (memoized).
    pub fn ancestors(&mut self, id: &EventId) -> HashSet<EventId> {
        if let Some(cached) = self.ancestry_cache.get(id) {
            return cached.clone();
        }
        let mut result = HashSet::new();
        let mut queue = VecDeque::new();
        if let Some(event) = self.events.get(id) {
            queue.extend(event.parents.iter().cloned());
        }
        while let Some(ancestor_id) = queue.pop_front() {
            if result.insert(ancestor_id.clone()) {
                if let Some(ancestor) = self.events.get(&ancestor_id) {
                    queue.extend(ancestor.parents.iter().cloned());
                }
            }
        }
        self.ancestry_cache.insert(id.clone(), result.clone());
        result
    }

    /// Check whether `a ≺ b` (a is a causal ancestor of b).
    pub fn causally_precedes(&mut self, a: &EventId, b: &EventId) -> bool {
        self.ancestors(b).contains(a)
    }

    /// Check whether two events are causally independent (concurrent).
    pub fn are_concurrent(&mut self, a: &EventId, b: &EventId) -> bool {
        !self.causally_precedes(a, b) && !self.causally_precedes(b, a) && a != b
    }

    /// Topological sort of all events (Kahn's algorithm).
    /// Returns events in causal order (parents before children).
    pub fn topological_order(&self) -> Vec<EventId> {
        let mut in_degree: HashMap<EventId, usize> = HashMap::new();
        for id in self.events.keys() {
            in_degree.entry(id.clone()).or_insert(0);
        }
        for event in self.events.values() {
            for _ in &event.parents {
                *in_degree.entry(event.id.clone()).or_insert(0) += 1;
            }
        }
        // Use BTreeSet for deterministic ordering among independent events
        let mut queue: BTreeSet<EventId> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(id, _)| id.clone())
            .collect();
        let mut order = Vec::new();
        while let Some(id) = queue.iter().next().cloned() {
            queue.remove(&id);
            order.push(id.clone());
            if let Some(kids) = self.children.get(&id) {
                for child in kids {
                    let deg = in_degree.entry(child.clone()).or_insert(0);
                    *deg = deg.saturating_sub(1);
                    if *deg == 0 {
                        queue.insert(child.clone());
                    }
                }
            }
        }
        order
    }

    /// Ensure causal closure: all parents of every event are present.
    /// Returns IDs of missing events (if any).
    pub fn missing_ancestors(&self) -> HashSet<EventId> {
        let mut missing = HashSet::new();
        for event in self.events.values() {
            for parent in &event.parents {
                if !self.events.contains_key(parent) {
                    missing.insert(parent.clone());
                }
            }
        }
        missing
    }

    /// Returns true if the DAG is causally closed.
    pub fn is_causally_closed(&self) -> bool {
        self.missing_ancestors().is_empty()
    }

    /// Number of events.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Frontier: events with no children (tips of the DAG).
    pub fn frontier(&self) -> BTreeSet<EventId> {
        self.events
            .keys()
            .filter(|id| {
                self.children
                    .get(*id)
                    .map(|c| c.is_empty())
                    .unwrap_or(true)
            })
            .cloned()
            .collect()
    }
}

impl Default for CausalDag {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;

    fn make_chain(n: usize) -> CausalDag {
        let mut dag = CausalDag::new();
        let g = Event::genesis();
        let mut prev_id = g.id.clone();
        dag.insert(g);
        for i in 0..n {
            let e = Event::data(
                "step",
                serde_json::json!({"i": i}),
                BTreeSet::from([prev_id.clone()]),
            );
            prev_id = e.id.clone();
            dag.insert(e);
        }
        dag
    }

    #[test]
    fn topological_order_respects_causality() {
        let dag = make_chain(5);
        let order = dag.topological_order();
        // Every event should come after its parents
        for (i, id) in order.iter().enumerate() {
            let event = dag.events.get(id).unwrap();
            for parent in &event.parents {
                let parent_pos = order.iter().position(|x| x == parent).unwrap();
                assert!(parent_pos < i, "parent must come before child in topo order");
            }
        }
    }

    #[test]
    fn causal_closure_detection() {
        let mut dag = CausalDag::new();
        // Insert event with a dangling parent
        let e = Event {
            id: "abc".to_string(),
            payload: crate::event::Payload::Genesis,
            parents: BTreeSet::from(["missing_parent".to_string()]),
        };
        dag.events.insert(e.id.clone(), e);
        assert!(!dag.is_causally_closed());
    }

    #[test]
    fn frontier_is_tips() {
        let dag = make_chain(3);
        let frontier = dag.frontier();
        // Only the last event should be in the frontier
        assert_eq!(frontier.len(), 1);
    }
}
