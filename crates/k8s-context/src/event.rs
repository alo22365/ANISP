use chrono::{DateTime, Utc};

use crate::{K8sContextError, error::require_non_empty};

/// Generic namespaced object reference; UID remains optional and opaque.
/// This does not resolve the reference or associate it with another domain.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KubernetesObjectRef {
    kind: String,
    namespace: String,
    name: String,
    uid: Option<String>,
}

impl KubernetesObjectRef {
    pub fn new(
        kind: impl Into<String>,
        namespace: impl Into<String>,
        name: impl Into<String>,
        uid: Option<String>,
    ) -> Result<Self, K8sContextError> {
        let kind = kind.into();
        let namespace = namespace.into();
        let name = name.into();
        require_non_empty(&kind, K8sContextError::EmptyObjectKind)?;
        require_non_empty(&namespace, K8sContextError::EmptyNamespace)?;
        require_non_empty(&name, K8sContextError::EmptyObjectName)?;
        Ok(Self {
            kind,
            namespace,
            name,
            uid,
        })
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn uid(&self) -> Option<&str> {
        self.uid.as_deref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KubernetesEventType {
    Normal,
    Warning,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KubernetesEventInput {
    pub event_uid: String,
    pub resource_version: Option<String>,
    pub cluster_id: String,
    pub namespace: String,
    pub involved_object: KubernetesObjectRef,
    pub event_type: KubernetesEventType,
    pub reason: String,
    pub message: String,
    pub count: Option<u32>,
    pub first_timestamp: Option<DateTime<Utc>>,
    pub last_timestamp: Option<DateTime<Utc>>,
    pub observed_at: DateTime<Utc>,
}

/// A Kubernetes source Event, NOT an ANISP ContextEvent. Reason/message/count
/// and optional source times are preserved without defaults or diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KubernetesEvent {
    event_uid: String,
    resource_version: Option<String>,
    cluster_id: String,
    namespace: String,
    involved_object: KubernetesObjectRef,
    event_type: KubernetesEventType,
    reason: String,
    message: String,
    count: Option<u32>,
    first_timestamp: Option<DateTime<Utc>>,
    last_timestamp: Option<DateTime<Utc>>,
    observed_at: DateTime<Utc>,
}

impl KubernetesEvent {
    pub fn new(input: KubernetesEventInput) -> Result<Self, K8sContextError> {
        require_non_empty(&input.cluster_id, K8sContextError::EmptyClusterId)?;
        require_non_empty(&input.namespace, K8sContextError::EmptyNamespace)?;
        require_non_empty(&input.event_uid, K8sContextError::EmptyEventUid)?;
        Ok(Self {
            event_uid: input.event_uid,
            resource_version: input.resource_version,
            cluster_id: input.cluster_id,
            namespace: input.namespace,
            involved_object: input.involved_object,
            event_type: input.event_type,
            reason: input.reason,
            message: input.message,
            count: input.count,
            first_timestamp: input.first_timestamp,
            last_timestamp: input.last_timestamp,
            observed_at: input.observed_at,
        })
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }
    pub fn event_uid(&self) -> &str {
        &self.event_uid
    }
    pub fn resource_version(&self) -> Option<&str> {
        self.resource_version.as_deref()
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn involved_object(&self) -> &KubernetesObjectRef {
        &self.involved_object
    }
    pub fn event_type(&self) -> KubernetesEventType {
        self.event_type
    }
    pub fn reason(&self) -> &str {
        &self.reason
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn count(&self) -> Option<u32> {
        self.count
    }
    pub fn first_timestamp(&self) -> Option<DateTime<Utc>> {
        self.first_timestamp
    }
    pub fn last_timestamp(&self) -> Option<DateTime<Utc>> {
        self.last_timestamp
    }
    pub fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }
}
