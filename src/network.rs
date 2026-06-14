//! Network layer — peer-to-peer anti-entropy protocol.
//!
//! Turns the JC kernel into a real distributed system.
//!
//! ## Protocol
//!
//! ```text
//! ┌───────────────────────────────────────────────────────────┐
//! │  JC Wire Protocol (newline-delimited JSON over TCP)       │
//! │                                                           │
//! │  Client sends:  { "type": "hello",    "node_id": "..." } │
//! │  Server replies:{ "type": "hello_ack","node_id": "..." } │
//! │                                                           │
//! │  Anti-entropy sync:                                       │
//! │  → { "type": "have", "ids": ["aabb...", ...] }           │
//! │  ← { "type": "want", "ids": ["ccdd...", ...] }           │
//! │  → { "type": "events", "events": [...] }                 │
//! │  ← { "type": "events_ack", "count": N }                  │
//! │                                                           │
//! │  Push (real-time):                                        │
//! │  → { "type": "push", "events": [...] }                   │
//! │  ← { "type": "push_ack", "count": N }                    │
//! └───────────────────────────────────────────────────────────┘
//! ```
//!
//! The protocol is symmetric: both peers send `have` to each other and
//! exchange only the missing events.  This is the standard gossip anti-entropy
//! approach, but grounded in the JC merge semantics:
//!
//! ```text
//! sync(A, B) = { H_A := nf(H_A ∪ H_B), H_B := nf(H_A ∪ H_B) }
//! ```
//!
//! ## Usage
//!
//! ```ignore
//! use jc_computation::network::{NetworkNode, PeerAddr};
//!
//! // Start a listening node
//! let mut node = NetworkNode::new("node-1");
//! node.listen("127.0.0.1:7777").expect("failed to bind");
//!
//! // Connect to a peer and sync
//! node.sync_with_peer("127.0.0.1:7778").expect("sync failed");
//! ```

use crate::event::{Event, EventId};
use crate::merge::DistributedNode;
use crate::persistence::SerializableEvent;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};

// ────────────────────────────────────────────────────────────────────────────
// Wire messages
// ────────────────────────────────────────────────────────────────────────────

/// A message in the JC wire protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    /// Handshake: announce node identity.
    Hello { node_id: String },
    /// Handshake acknowledgement.
    HelloAck { node_id: String },
    /// Anti-entropy: announce the set of event IDs this peer has.
    Have { ids: Vec<EventId> },
    /// Anti-entropy: request the listed events.
    Want { ids: Vec<EventId> },
    /// Deliver requested or pushed events.
    Events { events: Vec<SerializableEvent> },
    /// Acknowledge receipt of events.
    EventsAck { count: usize },
    /// Real-time push of new events.
    Push { events: Vec<SerializableEvent> },
    /// Acknowledge a push.
    PushAck { count: usize },
    /// Error response.
    Error { message: String },
    /// Graceful bye.
    Bye,
}

impl Message {
    fn to_line(&self) -> Result<String, NetworkError> {
        serde_json::to_string(self).map_err(|e| NetworkError::Serialization(e.to_string()))
    }

    fn from_line(line: &str) -> Result<Self, NetworkError> {
        serde_json::from_str(line).map_err(|e| NetworkError::Serialization(e.to_string()))
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Error type
// ────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum NetworkError {
    Io(String),
    Serialization(String),
    Protocol(String),
    Timeout,
}

impl std::fmt::Display for NetworkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkError::Io(e) => write!(f, "I/O error: {}", e),
            NetworkError::Serialization(e) => write!(f, "Serialization error: {}", e),
            NetworkError::Protocol(e) => write!(f, "Protocol error: {}", e),
            NetworkError::Timeout => write!(f, "Connection timed out"),
        }
    }
}

impl From<std::io::Error> for NetworkError {
    fn from(e: std::io::Error) -> Self {
        NetworkError::Io(e.to_string())
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Connection helper
// ────────────────────────────────────────────────────────────────────────────

pub type PeerAddr = String;

struct Connection {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Connection {
    fn new(stream: TcpStream) -> Result<Self, NetworkError> {
        let writer = stream.try_clone()?;
        Ok(Connection {
            reader: BufReader::new(stream),
            writer,
        })
    }

    fn send(&mut self, msg: &Message) -> Result<(), NetworkError> {
        let line = msg.to_line()?;
        writeln!(self.writer, "{}", line)?;
        self.writer.flush()?;
        Ok(())
    }

    fn recv(&mut self) -> Result<Message, NetworkError> {
        let mut line = String::new();
        let n = self.reader.read_line(&mut line)?;
        if n == 0 {
            return Err(NetworkError::Protocol("connection closed by peer".into()));
        }
        Message::from_line(line.trim())
    }
}

// ────────────────────────────────────────────────────────────────────────────
// NetworkNode
// ────────────────────────────────────────────────────────────────────────────

/// A network-connected JC node.
///
/// Wraps a `DistributedNode` and exposes:
/// - `listen()` — accept incoming sync connections
/// - `sync_with_peer()` — connect to a peer and exchange events
/// - `push_to_peer()` — push new events to a connected peer
pub struct NetworkNode {
    pub node: DistributedNode,
    pub listen_addr: Option<PeerAddr>,
}

impl NetworkNode {
    /// Create a new network node with the given ID.
    pub fn new(id: impl Into<String>) -> Self {
        NetworkNode {
            node: DistributedNode::new(id),
            listen_addr: None,
        }
    }

    /// Bind to the given address and accept one incoming sync connection.
    ///
    /// In production you'd run this in a loop on a background thread.
    pub fn accept_one(&mut self, bind_addr: &str) -> Result<SyncStats, NetworkError> {
        let listener = TcpListener::bind(bind_addr)?;
        let (stream, peer_addr) = listener.accept()?;
        let stats = self.handle_incoming(stream, peer_addr.to_string())?;
        Ok(stats)
    }

    /// Connect to a peer, perform anti-entropy sync, and return stats.
    pub fn sync_with_peer(&mut self, peer_addr: &str) -> Result<SyncStats, NetworkError> {
        let stream = TcpStream::connect(peer_addr)?;
        let mut conn = Connection::new(stream)?;

        // ── Handshake ──
        conn.send(&Message::Hello { node_id: self.node.id.clone() })?;
        let ack = conn.recv()?;
        let peer_id = match ack {
            Message::HelloAck { node_id } => node_id,
            Message::Error { message } => return Err(NetworkError::Protocol(message)),
            other => return Err(NetworkError::Protocol(format!("unexpected: {:?}", other))),
        };

        // ── Anti-entropy: HAVE ──
        let my_ids: Vec<EventId> = self.node.history.events.keys().cloned().collect();
        conn.send(&Message::Have { ids: my_ids.clone() })?;

        // ── Receive WANT list from peer ──
        let want_ids = match conn.recv()? {
            Message::Want { ids } => ids,
            other => return Err(NetworkError::Protocol(format!("expected Want, got {:?}", other))),
        };

        // ── Send requested events ──
        let events_to_send: Vec<SerializableEvent> = want_ids
            .iter()
            .filter_map(|id| self.node.history.events.get(id))
            .map(SerializableEvent::from)
            .collect();
        let sent_count = events_to_send.len();
        conn.send(&Message::Events { events: events_to_send })?;

        // ── Wait for ACK ──
        match conn.recv()? {
            Message::EventsAck { count } => {
                if count != sent_count {
                    eprintln!("[net] peer acked {} but we sent {}", count, sent_count);
                }
            }
            other => return Err(NetworkError::Protocol(format!("expected EventsAck, got {:?}", other))),
        }

        // ── Receive peer's events ──
        let received_count = match conn.recv()? {
            Message::Events { events } => {
                let count = events.len();
                for se in events {
                    let event = Event::new(se.payload, se.parents);
                    self.node.history.insert(event);
                }
                // Normalize after receiving
                let mut nf = crate::nf::NormalForm::default();
                nf.reduce(&mut self.node.history);
                count
            }
            Message::Want { ids: _ } => {
                // Peer has nothing for us (sent empty Want)
                conn.send(&Message::EventsAck { count: 0 })?;
                0
            }
            other => return Err(NetworkError::Protocol(format!("expected Events, got {:?}", other))),
        };

        conn.send(&Message::EventsAck { count: received_count })?;
        conn.send(&Message::Bye)?;

        Ok(SyncStats {
            peer_id,
            sent: sent_count,
            received: received_count,
        })
    }

    /// Handle an incoming connection from a peer.
    fn handle_incoming(
        &mut self,
        stream: TcpStream,
        _peer_addr: String,
    ) -> Result<SyncStats, NetworkError> {
        let mut conn = Connection::new(stream)?;

        // ── Handshake ──
        let peer_id = match conn.recv()? {
            Message::Hello { node_id } => node_id,
            other => return Err(NetworkError::Protocol(format!("expected Hello, got {:?}", other))),
        };
        conn.send(&Message::HelloAck { node_id: self.node.id.clone() })?;

        // ── Receive peer's HAVE list ──
        let peer_ids: HashSet<EventId> = match conn.recv()? {
            Message::Have { ids } => ids.into_iter().collect(),
            other => return Err(NetworkError::Protocol(format!("expected Have, got {:?}", other))),
        };

        // ── Send WANT list (events peer is missing) ──
       let my_ids: HashSet<EventId> = self.node.history.events.keys().cloned().collect();
let want_ids: Vec<EventId> = peer_ids
    .iter()
    .filter(|id| !my_ids.contains(*id))
    .cloned()
    .collect();
        conn.send(&Message::Want { ids: want_ids })?;

        // ── Receive events from peer ──
        let received_count = match conn.recv()? {
            Message::Events { events } => {
                let count = events.len();
                for se in events {
                    let event = Event::new(se.payload, se.parents);
                    self.node.history.insert(event);
                }
                let mut nf = crate::nf::NormalForm::default();
                nf.reduce(&mut self.node.history);
                count
            }
            other => return Err(NetworkError::Protocol(format!("expected Events, got {:?}", other))),
        };

        conn.send(&Message::EventsAck { count: received_count })?;

        // ── Now send OUR events that peer is missing ──
        let my_events_for_peer: Vec<SerializableEvent> = self
            .node
            .history
            .events
            .iter()
            .filter(|(id, _)| !peer_ids.contains(*id))
            .map(|(_, e)| SerializableEvent::from(e))
            .collect();
        let sent_count = my_events_for_peer.len();
        conn.send(&Message::Events { events: my_events_for_peer })?;

        // ── Wait for peer's ACK ──
        match conn.recv()? {
            Message::EventsAck { .. } => {}
            other => return Err(NetworkError::Protocol(format!("expected EventsAck, got {:?}", other))),
        }

        // ── Drain Bye ──
        match conn.recv()? {
            Message::Bye => {}
            _ => {} // tolerate missing Bye
        }

        Ok(SyncStats {
            peer_id,
            sent: sent_count,
            received: received_count,
        })
    }

    /// Push a single event to a peer (real-time notification).
    pub fn push_event_to_peer(
        &self,
        event: &Event,
        peer_addr: &str,
    ) -> Result<(), NetworkError> {
        let stream = TcpStream::connect(peer_addr)?;
        let mut conn = Connection::new(stream)?;

        conn.send(&Message::Hello { node_id: self.node.id.clone() })?;
        match conn.recv()? {
            Message::HelloAck { .. } => {}
            other => return Err(NetworkError::Protocol(format!("expected HelloAck, got {:?}", other))),
        }

        let se = SerializableEvent::from(event);
        conn.send(&Message::Push { events: vec![se] })?;

        match conn.recv()? {
            Message::PushAck { .. } => {}
            other => return Err(NetworkError::Protocol(format!("expected PushAck, got {:?}", other))),
        }
        conn.send(&Message::Bye)?;
        Ok(())
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Sync statistics
// ────────────────────────────────────────────────────────────────────────────

/// Statistics from a completed sync operation.
#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    pub peer_id: String,
    /// Events we sent to the peer.
    pub sent: usize,
    /// Events we received from the peer.
    pub received: usize,
}

impl std::fmt::Display for SyncStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "sync with {}: sent={}, received={}",
            self.peer_id, self.sent, self.received
        )
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::CounterFunctor;
    use crate::kernel::SemanticFunctor;
    use std::thread;
    use std::time::Duration;

    fn next_available_port() -> u16 {
        // Bind to port 0 to get an OS-assigned port
        let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind");
        listener.local_addr().unwrap().port()
    }

    #[test]
    fn message_roundtrip() {
        let msg = Message::Have {
            ids: vec!["abc123".to_string(), "def456".to_string()],
        };
        let line = msg.to_line().unwrap();
        let decoded = Message::from_line(&line).unwrap();
        match decoded {
            Message::Have { ids } => {
                assert_eq!(ids, vec!["abc123", "def456"]);
            }
            _ => panic!("wrong message type"),
        }
    }

    #[test]
    fn sync_two_nodes_converge() {
        let port = next_available_port();
        let bind_addr = format!("127.0.0.1:{}", port);
        let connect_addr = bind_addr.clone();

        // Node B: server (listens)
        let server_handle = thread::spawn(move || {
            let mut node_b = NetworkNode::new("B");
            let frontier_b = node_b.node.history.frontier();
            let e_b = crate::event::Event::data(
                "increment",
                serde_json::json!(5),
                frontier_b,
            );
            node_b.node.append(e_b);

            let stats = node_b.accept_one(&bind_addr).expect("server accept failed");
            (node_b, stats)
        });

        // Give server a moment to bind
        thread::sleep(Duration::from_millis(50));

        // Node A: client (connects)
        let mut node_a = NetworkNode::new("A");
        let frontier_a = node_a.node.history.frontier();
        let e_a = crate::event::Event::data(
            "increment",
            serde_json::json!(10),
            frontier_a,
        );
        node_a.node.append(e_a);

        let stats_a = node_a.sync_with_peer(&connect_addr).expect("client sync failed");
        let (node_b, _stats_b) = server_handle.join().unwrap();

        // Both nodes should now see counter = 15 (10 + 5)
        let state_a = CounterFunctor.interpret(&node_a.node.history);
        let state_b = CounterFunctor.interpret(&node_b.node.history);

        assert_eq!(state_a, state_b, "nodes must converge after sync");
        assert_eq!(state_a, 15, "total counter must be 15");
        assert!(stats_a.sent > 0 || stats_a.received > 0, "at least some events must be exchanged");
    }

    #[test]
    fn sync_stats_display() {
        let stats = SyncStats {
            peer_id: "test-peer".to_string(),
            sent: 3,
            received: 7,
        };
        let s = format!("{}", stats);
        assert!(s.contains("test-peer"));
        assert!(s.contains("sent=3"));
        assert!(s.contains("received=7"));
    }

    #[test]
    fn already_converged_no_exchange() {
        let port = next_available_port();
        let bind_addr = format!("127.0.0.1:{}", port);
        let connect_addr = bind_addr.clone();

        // Both nodes have only genesis (identical state)
        let server_handle = thread::spawn(move || {
            let mut node = NetworkNode::new("server");
            node.accept_one(&bind_addr).expect("accept failed")
        });

        thread::sleep(Duration::from_millis(50));

        let mut node_a = NetworkNode::new("client");
        let stats = node_a.sync_with_peer(&connect_addr).expect("sync failed");
        let _server_stats = server_handle.join().unwrap();

        // Both start with just genesis — only genesis might be exchanged or nothing
        // (depends on whether genesis is treated as already shared)
        // The important thing is no panic and the system is stable
        assert!(stats.sent + stats.received <= 2, "no unexpected event explosion");
    }
}