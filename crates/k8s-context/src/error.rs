use std::{error::Error, fmt};

/// Fact validation errors without provider/client/storage details.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum K8sContextError {
    EmptyClusterId,
    EmptyNamespace,
    EmptyWorkloadName,
    EmptyPodName,
    EmptyPodUid,
    EmptyWorkloadUid,
    EmptyEventUid,
    EmptyContainerName,
    EmptyImage,
    EmptyObjectKind,
    EmptyObjectName,
    PodTargetScopeMismatch,
    NotDeploymentTarget,
}

impl fmt::Display for K8sContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyClusterId => "cluster ID must not be empty",
            Self::EmptyNamespace => "namespace must not be empty",
            Self::EmptyWorkloadName => "workload name must not be empty",
            Self::EmptyPodName => "pod name must not be empty",
            Self::EmptyPodUid => "pod UID must not be empty",
            Self::EmptyWorkloadUid => "workload UID must not be empty",
            Self::EmptyEventUid => "event UID must not be empty",
            Self::EmptyContainerName => "container name must not be empty",
            Self::EmptyImage => "image must not be empty",
            Self::EmptyObjectKind => "object kind must not be empty",
            Self::EmptyObjectName => "object name must not be empty",
            Self::PodTargetScopeMismatch => "pod and target cluster/namespace must match",
            Self::NotDeploymentTarget => "deployment observation requires a Deployment target",
        })
    }
}

impl Error for K8sContextError {}

pub(crate) fn require_non_empty(
    value: &str,
    error: K8sContextError,
) -> Result<(), K8sContextError> {
    if value.trim().is_empty() {
        Err(error)
    } else {
        Ok(())
    }
}
