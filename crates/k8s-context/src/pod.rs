use crate::{K8sContextError, error::require_non_empty};

/// Source identity of one pod instance. Reusing a name with a different UID
/// creates a different identity, even in the same cluster and namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PodIdentity {
    cluster_id: String,
    namespace: String,
    pod_name: String,
    pod_uid: String,
}

impl PodIdentity {
    pub fn new(
        cluster_id: impl Into<String>,
        namespace: impl Into<String>,
        pod_name: impl Into<String>,
        pod_uid: impl Into<String>,
    ) -> Result<Self, K8sContextError> {
        let cluster_id = cluster_id.into();
        let namespace = namespace.into();
        let pod_name = pod_name.into();
        let pod_uid = pod_uid.into();
        require_non_empty(&cluster_id, K8sContextError::EmptyClusterId)?;
        require_non_empty(&namespace, K8sContextError::EmptyNamespace)?;
        require_non_empty(&pod_name, K8sContextError::EmptyPodName)?;
        require_non_empty(&pod_uid, K8sContextError::EmptyPodUid)?;
        Ok(Self {
            cluster_id,
            namespace,
            pod_name,
            pod_uid,
        })
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn pod_name(&self) -> &str {
        &self.pod_name
    }
    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }
}

/// Lifecycle phase is separate from the pod Ready condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PodPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}
