use crate::{K8sContextError, error::require_non_empty};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkloadKind {
    Deployment,
    StatefulSet,
    DaemonSet,
}

/// Caller-supplied workload identity and optional application labels.
/// This records a mapping, not an owner-resolution or relation engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeTarget {
    cluster_id: String,
    namespace: String,
    workload_kind: WorkloadKind,
    workload_name: String,
    project_id: Option<String>,
    service: Option<String>,
    environment: Option<String>,
}

impl RuntimeTarget {
    pub fn new(
        cluster_id: impl Into<String>,
        namespace: impl Into<String>,
        workload_kind: WorkloadKind,
        workload_name: impl Into<String>,
        project_id: Option<String>,
        service: Option<String>,
        environment: Option<String>,
    ) -> Result<Self, K8sContextError> {
        let cluster_id = cluster_id.into();
        let namespace = namespace.into();
        let workload_name = workload_name.into();
        require_non_empty(&cluster_id, K8sContextError::EmptyClusterId)?;
        require_non_empty(&namespace, K8sContextError::EmptyNamespace)?;
        require_non_empty(&workload_name, K8sContextError::EmptyWorkloadName)?;
        Ok(Self {
            cluster_id,
            namespace,
            workload_kind,
            workload_name,
            project_id,
            service,
            environment,
        })
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn workload_kind(&self) -> WorkloadKind {
        self.workload_kind
    }
    pub fn workload_name(&self) -> &str {
        &self.workload_name
    }
    pub fn project_id(&self) -> Option<&str> {
        self.project_id.as_deref()
    }
    pub fn service(&self) -> Option<&str> {
        self.service.as_deref()
    }
    pub fn environment(&self) -> Option<&str> {
        self.environment.as_deref()
    }
}
