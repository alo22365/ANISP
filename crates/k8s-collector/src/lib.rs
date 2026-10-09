//! One-shot, read-only Kubernetes API adapter. SDK types stay in this crate.
//! The client determines API connectivity; cluster_id is an explicit business
//! identity and is never inferred from kubeconfig. No watchers or pollers.

mod collector;
mod error;
mod mapping;
mod selector;

pub use collector::{DeploymentSnapshot, KubernetesSnapshotCollector};
pub use error::CollectorError;
