use chrono::{DateTime, Utc};

use crate::{K8sContextError, RuntimeTarget, WorkloadKind, error::require_non_empty};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentObservationInput {
    pub target: RuntimeTarget,
    pub workload_uid: String,
    pub resource_version: Option<String>,
    pub generation: u64,
    /// None means status has not reported a generation/count, not zero.
    pub observed_generation: Option<u64>,
    pub desired_replicas: u32,
    pub ready_replicas: Option<u32>,
    pub available_replicas: Option<u32>,
    pub updated_replicas: Option<u32>,
    pub image_refs: Vec<String>,
    pub observed_at: DateTime<Utc>,
}

/// Raw replica snapshot, not a health evaluation. Status generations may lag
/// and rollout/surge counters may exceed desired replicas; no ordering or
/// cross-counter constraint is imposed. Empty image_refs remains empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentObservation {
    target: RuntimeTarget,
    workload_uid: String,
    resource_version: Option<String>,
    generation: u64,
    observed_generation: Option<u64>,
    desired_replicas: u32,
    ready_replicas: Option<u32>,
    available_replicas: Option<u32>,
    updated_replicas: Option<u32>,
    image_refs: Vec<String>,
    observed_at: DateTime<Utc>,
}

impl DeploymentObservation {
    pub fn new(input: DeploymentObservationInput) -> Result<Self, K8sContextError> {
        if input.target.workload_kind() != WorkloadKind::Deployment {
            return Err(K8sContextError::NotDeploymentTarget);
        }
        require_non_empty(&input.workload_uid, K8sContextError::EmptyWorkloadUid)?;
        for image in &input.image_refs {
            require_non_empty(image, K8sContextError::EmptyImage)?;
        }
        Ok(Self {
            target: input.target,
            workload_uid: input.workload_uid,
            resource_version: input.resource_version,
            generation: input.generation,
            observed_generation: input.observed_generation,
            desired_replicas: input.desired_replicas,
            ready_replicas: input.ready_replicas,
            available_replicas: input.available_replicas,
            updated_replicas: input.updated_replicas,
            image_refs: input.image_refs,
            observed_at: input.observed_at,
        })
    }

    pub fn target(&self) -> &RuntimeTarget {
        &self.target
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn workload_uid(&self) -> &str {
        &self.workload_uid
    }
    pub fn resource_version(&self) -> Option<&str> {
        self.resource_version.as_deref()
    }
    pub fn observed_generation(&self) -> Option<u64> {
        self.observed_generation
    }
    pub fn desired_replicas(&self) -> u32 {
        self.desired_replicas
    }
    pub fn ready_replicas(&self) -> Option<u32> {
        self.ready_replicas
    }
    pub fn available_replicas(&self) -> Option<u32> {
        self.available_replicas
    }
    pub fn updated_replicas(&self) -> Option<u32> {
        self.updated_replicas
    }
    pub fn image_refs(&self) -> &[String] {
        &self.image_refs
    }
    pub fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }
}
