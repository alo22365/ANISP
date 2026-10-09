use anisp_k8s_context::{
    ContainerState, ContainerStatus, DeploymentObservation, DeploymentObservationInput,
    KubernetesEvent, KubernetesEventInput, KubernetesEventType, KubernetesObjectRef, PodIdentity,
    PodObservation, PodObservationInput, PodPhase, RuntimeTarget,
};
use chrono::{DateTime, Utc};
use k8s_openapi::{
    api::{
        apps::v1::Deployment,
        core::v1::{ContainerStatus as ApiContainerStatus, Event, Pod},
    },
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};

use crate::CollectorError;

fn required<'a>(value: Option<&'a str>, field: &'static str) -> Result<&'a str, CollectorError> {
    value.ok_or(CollectorError::Mapping(field))
}

fn count(value: i32, field: &'static str) -> Result<u32, CollectorError> {
    value.try_into().map_err(|_| CollectorError::Mapping(field))
}

fn generation(value: i64) -> Result<u64, CollectorError> {
    value
        .try_into()
        .map_err(|_| CollectorError::Mapping("generation"))
}

fn scoped_metadata(meta: &ObjectMeta, target: &RuntimeTarget) -> Result<(), CollectorError> {
    if meta.namespace.as_deref() != Some(target.namespace()) {
        return Err(CollectorError::Mapping("metadata.namespace"));
    }
    Ok(())
}

pub(crate) fn deployment(
    value: &Deployment,
    target: &RuntimeTarget,
    observed_at: DateTime<Utc>,
) -> Result<DeploymentObservation, CollectorError> {
    scoped_metadata(&value.metadata, target)?;
    if value.metadata.name.as_deref() != Some(target.workload_name()) {
        return Err(CollectorError::Mapping("deployment.metadata.name"));
    }
    let spec = value
        .spec
        .as_ref()
        .ok_or(CollectorError::Mapping("deployment.spec"))?;
    let status = value.status.as_ref();
    let images = spec
        .template
        .spec
        .as_ref()
        .ok_or(CollectorError::Mapping("deployment.template.spec"))?
        .containers
        .iter()
        .map(|c| {
            c.image
                .clone()
                .ok_or(CollectorError::Mapping("container.image"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DeploymentObservation::new(DeploymentObservationInput {
        target: target.clone(),
        workload_uid: required(value.metadata.uid.as_deref(), "deployment.metadata.uid")?.into(),
        resource_version: value.metadata.resource_version.clone(),
        generation: generation(
            value
                .metadata
                .generation
                .ok_or(CollectorError::Mapping("deployment.generation"))?,
        )?,
        observed_generation: status
            .and_then(|s| s.observed_generation)
            .map(generation)
            .transpose()?,
        // The Kubernetes DeploymentSpec contract defaults omitted replicas to 1.
        // Status counters below have NO default and preserve unknown versus zero.
        desired_replicas: count(spec.replicas.unwrap_or(1), "spec.replicas")?,
        ready_replicas: status
            .and_then(|s| s.ready_replicas)
            .map(|v| count(v, "ready_replicas"))
            .transpose()?,
        available_replicas: status
            .and_then(|s| s.available_replicas)
            .map(|v| count(v, "available_replicas"))
            .transpose()?,
        updated_replicas: status
            .and_then(|s| s.updated_replicas)
            .map(|v| count(v, "updated_replicas"))
            .transpose()?,
        image_refs: images,
        observed_at,
    })?)
}

pub(crate) fn pod(
    value: &Pod,
    target: &RuntimeTarget,
    observed_at: DateTime<Utc>,
) -> Result<PodObservation, CollectorError> {
    scoped_metadata(&value.metadata, target)?;
    let status = value.status.as_ref();
    let phase = match status.and_then(|s| s.phase.as_deref()) {
        Some("Pending") => PodPhase::Pending,
        Some("Running") => PodPhase::Running,
        Some("Succeeded") => PodPhase::Succeeded,
        Some("Failed") => PodPhase::Failed,
        _ => PodPhase::Unknown,
    };
    let ready = status
        .and_then(|s| s.conditions.as_deref())
        .unwrap_or_default()
        .iter()
        .any(|c| c.type_ == "Ready" && c.status == "True");
    let containers = status
        .and_then(|s| s.container_statuses.as_deref())
        .unwrap_or_default()
        .iter()
        .map(container)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PodObservation::new(PodObservationInput {
        identity: PodIdentity::new(
            target.cluster_id(),
            target.namespace(),
            required(value.metadata.name.as_deref(), "pod.metadata.name")?,
            required(value.metadata.uid.as_deref(), "pod.metadata.uid")?,
        )?,
        resource_version: value.metadata.resource_version.clone(),
        target: target.clone(),
        phase,
        ready,
        node_name: value.spec.as_ref().and_then(|s| s.node_name.clone()),
        pod_ip: status.and_then(|s| s.pod_ip.clone()),
        started_at: status.and_then(|s| s.start_time.as_ref()).map(|t| t.0),
        containers,
        observed_at,
    })?)
}

fn container(value: &ApiContainerStatus) -> Result<ContainerStatus, CollectorError> {
    let state = if let Some(state) = &value.state {
        // A malformed state must not silently discard contradictory facts.
        if usize::from(state.waiting.is_some())
            + usize::from(state.running.is_some())
            + usize::from(state.terminated.is_some())
            > 1
        {
            return Err(CollectorError::Mapping("container.state"));
        }
        if let Some(waiting) = &state.waiting {
            ContainerState::Waiting {
                reason: waiting.reason.clone(),
            }
        } else if let Some(running) = &state.running {
            ContainerState::Running {
                started_at: running.started_at.as_ref().map(|t| t.0),
            }
        } else if let Some(terminated) = &state.terminated {
            ContainerState::Terminated {
                exit_code: terminated.exit_code,
                reason: terminated.reason.clone(),
                finished_at: terminated.finished_at.as_ref().map(|t| t.0),
            }
        } else {
            ContainerState::Unknown
        }
    } else {
        ContainerState::Unknown
    };
    Ok(ContainerStatus::new(
        &value.name,
        &value.image,
        (!value.image_id.is_empty()).then(|| value.image_id.clone()),
        value.ready,
        count(value.restart_count, "container.restart_count")?,
        state,
    )?)
}

pub(crate) fn event(
    value: &Event,
    target: &RuntimeTarget,
    observed_at: DateTime<Utc>,
) -> Result<KubernetesEvent, CollectorError> {
    scoped_metadata(&value.metadata, target)?;
    let object = &value.involved_object;
    let event_type = match value.type_.as_deref() {
        Some("Normal") => KubernetesEventType::Normal,
        Some("Warning") => KubernetesEventType::Warning,
        _ => KubernetesEventType::Unknown,
    };
    Ok(KubernetesEvent::new(KubernetesEventInput {
        event_uid: required(value.metadata.uid.as_deref(), "event.metadata.uid")?.into(),
        resource_version: value.metadata.resource_version.clone(),
        cluster_id: target.cluster_id().into(),
        namespace: target.namespace().into(),
        involved_object: KubernetesObjectRef::new(
            required(object.kind.as_deref(), "involved_object.kind")?,
            required(object.namespace.as_deref(), "involved_object.namespace")?,
            required(object.name.as_deref(), "involved_object.name")?,
            object.uid.clone(),
        )?,
        event_type,
        // Domain text fields permit empty text; omitted optional provider text
        // is represented as empty. Counts and source timestamps stay optional.
        reason: value.reason.clone().unwrap_or_default(),
        message: value.message.clone().unwrap_or_default(),
        count: value.count.map(|v| count(v, "event.count")).transpose()?,
        first_timestamp: value
            .first_timestamp
            .as_ref()
            .map(|t| t.0)
            .or_else(|| value.event_time.as_ref().map(|t| t.0)),
        last_timestamp: value
            .series
            .as_ref()
            .and_then(|s| s.last_observed_time.as_ref())
            .map(|t| t.0)
            .or_else(|| value.last_timestamp.as_ref().map(|t| t.0))
            .or_else(|| value.event_time.as_ref().map(|t| t.0)),
        observed_at,
    })?)
}
