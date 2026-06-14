//! `jc-node` — Run a JC-Computation network node.
//!
//! ## Usage
//!
//! ```text
//! # Start a node on port 7777
//! jc-node --id node-1 --bind 127.0.0.1:7777
//!
//! # Sync with a peer
//! jc-node --id node-2 --bind 127.0.0.1:7778 --peer 127.0.0.1:7777
//!
//! # Append an event and exit
//! jc-node --id node-1 --bind 127.0.0.1:7777 --append '{"key":"x","val":42}' --kind set
//! ```

use jc_computation::network::NetworkNode;
use jc_computation::kernel::{KvFunctor, CounterFunctor};
use jc_computation::kernel::SemanticFunctor;
use jc_computation::event::Event;
use std::collections::BTreeSet;

fn print_usage() {
    eprintln!("Usage: jc-node [OPTIONS]");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --id <ID>          Node identifier (default: node-1)");
    eprintln!("  --bind <ADDR>      Address to listen on (default: none — client only)");
    eprintln!("  --peer <ADDR>      Peer address to sync with");
    eprintln!("  --kind <KIND>      Event kind for --append (default: log)");
    eprintln!("  --append <JSON>    Append an event with this JSON payload");
    eprintln!("  --state kv|counter Print current state using the given functor");
    eprintln!("  --help             Print this help");
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_usage();
        return;
    }

    // Parse args
    let mut node_id = "node-1".to_string();
    let mut bind_addr: Option<String> = None;
    let mut peer_addr: Option<String> = None;
    let mut append_payload: Option<String> = None;
    let mut event_kind = "log".to_string();
    let mut state_functor: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--id" if i + 1 < args.len() => {
                node_id = args[i + 1].clone();
                i += 2;
            }
            "--bind" if i + 1 < args.len() => {
                bind_addr = Some(args[i + 1].clone());
                i += 2;
            }
            "--peer" if i + 1 < args.len() => {
                peer_addr = Some(args[i + 1].clone());
                i += 2;
            }
            "--append" if i + 1 < args.len() => {
                append_payload = Some(args[i + 1].clone());
                i += 2;
            }
            "--kind" if i + 1 < args.len() => {
                event_kind = args[i + 1].clone();
                i += 2;
            }
            "--state" if i + 1 < args.len() => {
                state_functor = Some(args[i + 1].clone());
                i += 2;
            }
            unknown => {
                eprintln!("Unknown argument: {}", unknown);
                print_usage();
                std::process::exit(1);
            }
        }
    }

    // Build node
    let mut net_node = NetworkNode::new(node_id.clone());
    println!("[jc-node] ID: {}", node_id);

    // Append an event if requested
    if let Some(payload_str) = &append_payload {
        let value: serde_json::Value = match serde_json::from_str(payload_str) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Invalid JSON payload: {}", e);
                std::process::exit(1);
            }
        };
        let frontier = net_node.node.history.frontier();
        let event = Event::data(event_kind.clone(), value, frontier);
        net_node.node.append(event);
        println!("[jc-node] Appended event (kind={})", event_kind);
        println!("[jc-node] History size: {}", net_node.node.history.len());
    }

    // Sync with peer if requested
    if let Some(ref peer) = peer_addr {
        println!("[jc-node] Syncing with peer: {}", peer);
        match net_node.sync_with_peer(peer) {
            Ok(stats) => println!("[jc-node] {}", stats),
            Err(e) => eprintln!("[jc-node] Sync failed: {}", e),
        }
    }

    // Print state if requested
    if let Some(ref functor) = state_functor {
        match functor.as_str() {
            "kv" => {
                let state = KvFunctor.interpret(&net_node.node.history);
                println!("[jc-node] KV state: {}", serde_json::to_string_pretty(&state).unwrap_or_default());
            }
            "counter" => {
                let count = CounterFunctor.interpret(&net_node.node.history);
                println!("[jc-node] Counter: {}", count);
            }
            unknown => {
                eprintln!("Unknown functor: {}. Use 'kv' or 'counter'.", unknown);
            }
        }
    }

    // Start server if bind addr given
    if let Some(ref addr) = bind_addr {
        if peer_addr.is_none() {
            println!("[jc-node] Listening on {} (accepting one connection then exiting)", addr);
            println!("[jc-node] (In production: run in a loop on a background thread)");
            match net_node.accept_one(addr) {
                Ok(stats) => println!("[jc-node] Completed sync: {}", stats),
                Err(e) => eprintln!("[jc-node] Accept error: {}", e),
            }
        }
    }

    println!("[jc-node] Final history size: {}", net_node.node.history.len());
    println!("[jc-node] Causally closed: {}", net_node.node.history.is_causally_closed());
}
