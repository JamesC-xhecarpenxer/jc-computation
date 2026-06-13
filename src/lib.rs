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

pub use event::{Event, EventId, Payload};
pub use dag::CausalDag;
pub use cone::ConeHasher;
pub use nf::NormalForm;
pub use kernel::JcKernel;
pub use merge::merge_histories;
