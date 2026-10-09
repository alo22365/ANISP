//! On-demand local Git adapter. No network, persistence, or background polling.
//!
//! Public inputs and outputs contain only paths, primitives, and Git Context
//! Domain objects. Native Git handles remain private to this adapter.

mod collector;
mod diff;
mod error;

pub use collector::{LocalGitCollector, MAX_RECENT_COMMITS};
pub use error::CollectorError;
