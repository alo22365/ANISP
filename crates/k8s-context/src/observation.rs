use chrono::{DateTime, Utc};

use crate::{ContainerStatus, K8sContextError, PodIdentity, PodPhase, RuntimeTarget};

/// Caller-supplied snapshot; identity, target and containers are already
/// validated domain values. Empty containers are valid when none are reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodObservationInput {
    pub identity: PodIdentity,
    pub resource_version: Option<String>,
    pub target: RuntimeTarget,
    pub phase: PodPhase,
    pub ready: bool,
    pub node_name: Option<String>,
    pub pod_ip: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub containers: Vec<ContainerStatus>,
    pub observed_at: DateTime<Utc>,
}

/// Immutable pod facts. Running never implies Ready. Target ownership is
/// supplied by the caller; only matching cluster/namespace scope is enforced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodObservation {
    identity: PodIdentity,
    resource_version: Option<String>,
    target: RuntimeTarget,
    phase: PodPhase,
    ready: bool,
    node_name: Option<String>,
    pod_ip: Option<String>,
    started_at: Option<DateTime<Utc>>,
    containers: Vec<ContainerStatus>,
    observed_at: DateTime<Utc>,
}

impl PodObservation {
    pub fn new(input: PodObservationInput) -> Result<Self, K8sContextError> {
        if input.identity.cluster_id() != input.target.cluster_id()
            || input.identity.namespace() != input.target.namespace()
        {
            return Err(K8sContextError::PodTargetScopeMismatch);
        }
        Ok(Self {
            identity: input.identity,
            resource_version: input.resource_version,
            target: input.target,
            phase: input.phase,
            ready: input.ready,
            node_name: input.node_name,
            pod_ip: input.pod_ip,
            started_at: input.started_at,
            containers: input.containers,
            observed_at: input.observed_at,
        })
    }

    pub fn identity(&self) -> &PodIdentity {
        &self.identity
    }
    pub fn target(&self) -> &RuntimeTarget {
        &self.target
    }
    pub fn resource_version(&self) -> Option<&str> {
        self.resource_version.as_deref()
    }
    pub fn phase(&self) -> PodPhase {
        self.phase
    }
    pub fn ready(&self) -> bool {
        self.ready
    }
    pub fn node_name(&self) -> Option<&str> {
        self.node_name.as_deref()
    }
    pub fn pod_ip(&self) -> Option<&str> {
        self.pod_ip.as_deref()
    }
    /// Kubernetes source start time, not the collection timestamp.
    pub fn started_at(&self) -> Option<DateTime<Utc>> {
        self.started_at
    }
    pub fn containers(&self) -> &[ContainerStatus] {
        &self.containers
    }
    pub fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }
}
