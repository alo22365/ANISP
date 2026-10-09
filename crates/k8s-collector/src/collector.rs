use std::collections::HashSet;

use anisp_k8s_context::{
    DeploymentObservation, K8sContextError, KubernetesEvent, PodObservation, RuntimeTarget,
    WorkloadKind,
};
use chrono::{DateTime, Utc};
use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{Event, Pod},
};
use kube::{Api, Client, api::ListParams};

use crate::{CollectorError, error::api_error, mapping, selector::pod_selector};

/// Observations share one collection timestamp, not a cross-resource atomic
/// Kubernetes snapshot. Each resource retains its own resourceVersion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentSnapshot {
    pub deployment: DeploymentObservation,
    pub pods: Vec<PodObservation>,
    pub events: Vec<KubernetesEvent>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct KubernetesSnapshotCollector {
    client: Client,
    cluster_id: String,
}

impl KubernetesSnapshotCollector {
    pub fn new(client: Client, cluster_id: impl Into<String>) -> Result<Self, CollectorError> {
        let cluster_id = cluster_id.into();
        if cluster_id.trim().is_empty() {
            return Err(K8sContextError::EmptyClusterId.into());
        }
        Ok(Self { client, cluster_id })
    }

    pub async fn collect_deployment_snapshot(
        &self,
        target: &RuntimeTarget,
    ) -> Result<DeploymentSnapshot, CollectorError> {
        if target.cluster_id() != self.cluster_id {
            return Err(CollectorError::TargetMismatch);
        }
        if target.workload_kind() != WorkloadKind::Deployment {
            return Err(CollectorError::UnsupportedWorkloadKind);
        }
        let observed_at = Utc::now();
        let deployments: Api<Deployment> = Api::namespaced(self.client.clone(), target.namespace());
        let raw_deployment = deployments
            .get(target.workload_name())
            .await
            .map_err(|e| api_error(e, true))?;
        let spec = raw_deployment
            .spec
            .as_ref()
            .ok_or(CollectorError::Mapping("deployment.spec"))?;
        let selector = pod_selector(&spec.selector)?;
        let deployment = mapping::deployment(&raw_deployment, target, observed_at)?;

        let pods_api: Api<Pod> = Api::namespaced(self.client.clone(), target.namespace());
        let raw_pods = pods_api
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(|e| api_error(e, false))?;
        // No list limit: never silently drop pods via a partial paginated list.
        if raw_pods
            .metadata
            .continue_
            .as_ref()
            .is_some_and(|v| !v.is_empty())
        {
            return Err(CollectorError::Mapping("incomplete pod list"));
        }
        let pods = raw_pods
            .items
            .iter()
            .map(|p| mapping::pod(p, target, observed_at))
            .collect::<Result<Vec<_>, _>>()?;
        let mut uids: HashSet<&str> = pods.iter().map(|p| p.identity().pod_uid()).collect();
        uids.insert(deployment.workload_uid());

        let events_api: Api<Event> = Api::namespaced(self.client.clone(), target.namespace());
        let raw_events = events_api
            .list(&ListParams::default())
            .await
            .map_err(|e| api_error(e, false))?;
        if raw_events
            .metadata
            .continue_
            .as_ref()
            .is_some_and(|v| !v.is_empty())
        {
            return Err(CollectorError::Mapping("incomplete event list"));
        }
        let events = raw_events
            .items
            .iter()
            .filter(|e| {
                e.involved_object
                    .uid
                    .as_deref()
                    .is_some_and(|uid| uids.contains(uid))
            })
            .map(|e| mapping::event(e, target, observed_at))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(DeploymentSnapshot {
            deployment,
            pods,
            events,
            observed_at,
        })
    }
}
