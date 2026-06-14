//! # JC-Computation Kernel (Rust)
//!
//! A distributed quotient-rewriting engine over causal history space.
//!
//! ## Core Equation
//!
//! ```text
//! State = σ(nf(History))
//! ```
//!
//! where:
//! - `History` = causally closed set of immutable events
//! - `nf`      = normal form operator (confluence-enforcing reduction)
//! - `σ`       = semantic functor (state projection)
//!
//! ## Modules
//!
//! | Module | Purpose |
//! |--------|---------|
//! | `event` | Immutable content-addressed events |
//! | `dag` | Causal DAG with O(V+E) operations |
//! | `cone` | Merkle cone hashing for isomorphism detection |
//! | `nf` | Confluent normal-form reduction engine |
//! | `kernel` | High-level runtime + semantic functors |
//! | `merge` | Distributed merge protocol + node simulation |
//! | `persistence` | Incremental reduction, snapshots, WAL |
//! | `network` | TCP peer-to-peer anti-entropy protocol |
//!
//! ## Author
//! James Chapman <xhecarpenxer@gmail.com>
//!
//! ## License
//! See LICENSE — dual license (personal free / commercial paid)

pub mod event;
pub mod dag;
pub mod cone;
pub mod nf;
pub mod kernel;
pub mod merge;
pub mod persistence;
pub mod network;

pub use event::{Event, EventId, Payload};
pub use dag::CausalDag;
pub use cone::ConeHasher;
pub use nf::NormalForm;
pub use kernel::JcKernel;
pub use merge::merge_histories;
pub use persistence::{
    EventStore, MemoryStore, Snapshot, SerializableEvent,
    IncrementalKernel, FileStore,
};
pub use network::{NetworkNode, Message, SyncStats};