//! Provider- and storage-independent Kubernetes runtime facts.
//!
//! Validated models have private fields and read-only accessors. Observation
//! inputs never generate identity, derive health, fetch resources, or publish
//! events. Kubernetes UIDs are opaque source identifiers, not generated UUIDs.
//! Source timestamps and caller-supplied observation timestamps are preserved
//! independently, including precision and clock skew; absence remains absence.
//! Only workload targets, pod/container/deployment snapshots and Kubernetes
//! Events are modeled. Logs, resource metrics and provider types do not belong
//! in this crate.
//!
//! Pod instance identity cannot be changed to bypass validation:
//! ```compile_fail
//! use anisp_k8s_context::PodIdentity;
//! let mut pod = PodIdentity::new("cluster", "namespace", "pod", "source-uid").unwrap();
//! pod.pod_uid = String::new();
//! ```

mod container;
mod deployment;
mod error;
mod event;
mod observation;
mod pod;
mod target;

pub use container::{ContainerState, ContainerStatus};
pub use deployment::{DeploymentObservation, DeploymentObservationInput};
pub use error::K8sContextError;
pub use event::{KubernetesEvent, KubernetesEventInput, KubernetesEventType, KubernetesObjectRef};
pub use observation::{PodObservation, PodObservationInput};
pub use pod::{PodIdentity, PodPhase};
pub use target::{RuntimeTarget, WorkloadKind};
