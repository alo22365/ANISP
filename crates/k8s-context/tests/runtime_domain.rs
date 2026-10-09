//! Pure domain tests: no runtime, provider, storage or network fixtures.
use anisp_k8s_context::{
    ContainerState, ContainerStatus, DeploymentObservation, DeploymentObservationInput,
    K8sContextError, KubernetesEvent, KubernetesEventInput, KubernetesEventType,
    KubernetesObjectRef, PodIdentity, PodObservation, PodObservationInput, PodPhase, RuntimeTarget,
    WorkloadKind,
};
use chrono::{DateTime, Duration, Utc};

fn fact_time() -> DateTime<Utc> {
    "2026-10-08T01:02:03.123456789Z".parse().unwrap()
}
fn target(kind: WorkloadKind) -> RuntimeTarget {
    RuntimeTarget::new(
        "cluster-xpa",
        "xpa",
        kind,
        "xpa-finance",
        Some("xpa".into()),
        Some("xpa-finance".into()),
        Some("prod".into()),
    )
    .unwrap()
}
fn identity() -> PodIdentity {
    PodIdentity::new(
        "cluster-xpa",
        "xpa",
        "xpa-finance-abc-123",
        "opaque-kubernetes-uid",
    )
    .unwrap()
}
fn container() -> ContainerStatus {
    ContainerStatus::new(
        "finance",
        "finance:v1",
        Some("image-digest".into()),
        true,
        5,
        ContainerState::Running {
            started_at: Some(fact_time()),
        },
    )
    .unwrap()
}
fn pod_input() -> PodObservationInput {
    PodObservationInput {
        identity: identity(),
        resource_version: Some("pod-version".into()),
        target: target(WorkloadKind::Deployment),
        phase: PodPhase::Running,
        ready: true,
        node_name: Some("node-a".into()),
        pod_ip: Some("2001:db8::1".into()),
        started_at: Some(fact_time()),
        containers: vec![container()],
        observed_at: fact_time() + Duration::seconds(10),
    }
}
fn deployment_input() -> DeploymentObservationInput {
    DeploymentObservationInput {
        target: target(WorkloadKind::Deployment),
        generation: 9,
        workload_uid: "deployment-uid".into(),
        resource_version: Some("deployment-version".into()),
        observed_generation: Some(8),
        desired_replicas: 3,
        ready_replicas: Some(5),
        available_replicas: Some(4),
        updated_replicas: Some(6),
        image_refs: vec!["finance:v1".into(), "finance:v2".into()],
        observed_at: fact_time(),
    }
}
fn event_input(event_type: KubernetesEventType) -> KubernetesEventInput {
    KubernetesEventInput {
        event_uid: "event-uid".into(),
        resource_version: Some("event-version".into()),
        cluster_id: "cluster-xpa".into(),
        namespace: "xpa".into(),
        involved_object: KubernetesObjectRef::new(
            "Pod",
            "xpa",
            "xpa-finance-abc-123",
            Some("opaque-kubernetes-uid".into()),
        )
        .unwrap(),
        event_type,
        reason: "Scheduled".into(),
        message: "Successfully assigned pod".into(),
        count: Some(3),
        first_timestamp: Some(fact_time()),
        last_timestamp: Some(fact_time() + Duration::seconds(2)),
        observed_at: fact_time() + Duration::seconds(10),
    }
}

#[test]
fn provider_identity_versions_are_preserved() {
    let deployment = DeploymentObservation::new(deployment_input()).unwrap();
    assert_eq!(deployment.workload_uid(), "deployment-uid");
    assert_eq!(deployment.resource_version(), Some("deployment-version"));
    let pod = PodObservation::new(pod_input()).unwrap();
    assert_eq!(pod.resource_version(), Some("pod-version"));
    let event = KubernetesEvent::new(event_input(KubernetesEventType::Normal)).unwrap();
    assert_eq!(event.event_uid(), "event-uid");
    assert_eq!(event.resource_version(), Some("event-version"));
}

#[test]
fn unknown_event_count_and_resource_versions_remain_none() {
    let mut input = event_input(KubernetesEventType::Normal);
    input.count = None;
    input.resource_version = None;
    let event = KubernetesEvent::new(input).unwrap();
    assert_eq!(event.count(), None);
    assert_eq!(event.resource_version(), None);
    let mut input = pod_input();
    input.resource_version = None;
    assert_eq!(PodObservation::new(input).unwrap().resource_version(), None);
    let mut input = deployment_input();
    input.resource_version = None;
    assert_eq!(
        DeploymentObservation::new(input)
            .unwrap()
            .resource_version(),
        None
    );
}

#[test]
fn workload_and_event_uids_must_not_be_empty() {
    let mut input = deployment_input();
    input.workload_uid = " ".into();
    assert_eq!(
        DeploymentObservation::new(input),
        Err(K8sContextError::EmptyWorkloadUid)
    );
    let mut input = event_input(KubernetesEventType::Normal);
    input.event_uid = " ".into();
    assert_eq!(
        KubernetesEvent::new(input),
        Err(K8sContextError::EmptyEventUid)
    );
}

#[test]
fn runtime_target_records_all_supported_workloads_and_optional_mapping() {
    for kind in [
        WorkloadKind::Deployment,
        WorkloadKind::StatefulSet,
        WorkloadKind::DaemonSet,
    ] {
        let value = target(kind);
        assert_eq!(value.cluster_id(), "cluster-xpa");
        assert_eq!(value.namespace(), "xpa");
        assert_eq!(value.workload_kind(), kind);
        assert_eq!(value.workload_name(), "xpa-finance");
        assert_eq!(value.project_id(), Some("xpa"));
        assert_eq!(value.service(), Some("xpa-finance"));
        assert_eq!(value.environment(), Some("prod"));
    }
}
#[test]
fn runtime_target_missing_mapping_is_not_replaced_with_defaults() {
    let value = RuntimeTarget::new(
        "cluster",
        "namespace",
        WorkloadKind::Deployment,
        "workload",
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(value.project_id(), None);
    assert_eq!(value.service(), None);
    assert_eq!(value.environment(), None);
}
#[test]
fn invalid_target_rejects_empty_and_whitespace_required_fields() {
    for empty in ["", " \t\n"] {
        for (cluster, namespace, name, error) in [
            (
                empty,
                "namespace",
                "workload",
                K8sContextError::EmptyClusterId,
            ),
            (
                "cluster",
                empty,
                "workload",
                K8sContextError::EmptyNamespace,
            ),
            (
                "cluster",
                "namespace",
                empty,
                K8sContextError::EmptyWorkloadName,
            ),
        ] {
            assert_eq!(
                RuntimeTarget::new(
                    cluster,
                    namespace,
                    WorkloadKind::Deployment,
                    name,
                    None,
                    None,
                    None
                )
                .unwrap_err(),
                error
            );
        }
    }
}
#[test]
fn pod_identity_preserves_opaque_source_uid() {
    let pod = identity();
    assert_eq!(pod.cluster_id(), "cluster-xpa");
    assert_eq!(pod.namespace(), "xpa");
    assert_eq!(pod.pod_name(), "xpa-finance-abc-123");
    assert_eq!(pod.pod_uid(), "opaque-kubernetes-uid");
}
#[test]
fn pod_name_reuse_with_different_uid_is_a_different_instance() {
    let old = PodIdentity::new("cluster", "ns", "same-name", "uid-old").unwrap();
    let new = PodIdentity::new("cluster", "ns", "same-name", "uid-new").unwrap();
    assert_ne!(old, new);
    assert_ne!(
        old,
        PodIdentity::new("other-cluster", "ns", "same-name", "uid-old").unwrap()
    );
    assert_ne!(
        old,
        PodIdentity::new("cluster", "other-ns", "same-name", "uid-old").unwrap()
    );
}
#[test]
fn invalid_pod_identity_rejects_each_missing_field() {
    for empty in ["", " \t\n"] {
        for (cluster, namespace, name, uid, error) in [
            (empty, "ns", "pod", "uid", K8sContextError::EmptyClusterId),
            (
                "cluster",
                empty,
                "pod",
                "uid",
                K8sContextError::EmptyNamespace,
            ),
            ("cluster", "ns", empty, "uid", K8sContextError::EmptyPodName),
            ("cluster", "ns", "pod", empty, K8sContextError::EmptyPodUid),
        ] {
            assert_eq!(
                PodIdentity::new(cluster, namespace, name, uid).unwrap_err(),
                error
            );
        }
    }
}
#[test]
fn running_ready_pod_preserves_snapshot() {
    let pod = PodObservation::new(pod_input()).unwrap();
    assert_eq!(pod.identity(), &identity());
    assert_eq!(pod.target(), &target(WorkloadKind::Deployment));
    assert_eq!(pod.phase(), PodPhase::Running);
    assert!(pod.ready());
    assert_eq!(pod.node_name(), Some("node-a"));
    assert_eq!(pod.pod_ip(), Some("2001:db8::1"));
    assert_eq!(pod.containers(), &[container()]);
}
#[test]
fn running_not_ready_pod_is_not_promoted_to_ready() {
    let mut input = pod_input();
    input.ready = false;
    let pod = PodObservation::new(input).unwrap();
    assert_eq!(pod.phase(), PodPhase::Running);
    assert!(!pod.ready());
    // No synthetic inference from the container's Ready flag either.
    assert!(pod.containers()[0].ready());
}
#[test]
fn failed_pod_can_carry_terminated_container_facts() {
    let mut input = pod_input();
    input.phase = PodPhase::Failed;
    input.ready = false;
    let state = ContainerState::Terminated {
        exit_code: 137,
        reason: Some("OOMKilled".into()),
        finished_at: Some(fact_time()),
    };
    input.containers =
        vec![ContainerStatus::new("finance", "finance:v1", None, false, 5, state.clone()).unwrap()];
    let pod = PodObservation::new(input).unwrap();
    assert_eq!(pod.phase(), PodPhase::Failed);
    assert!(!pod.ready());
    assert_eq!(pod.containers()[0].state(), &state);
}
#[test]
fn pending_succeeded_unknown_pods_and_unreported_details_remain_facts() {
    for phase in [PodPhase::Pending, PodPhase::Succeeded, PodPhase::Unknown] {
        let mut input = pod_input();
        input.phase = phase;
        input.ready = false;
        input.node_name = None;
        input.pod_ip = None;
        input.started_at = None;
        input.containers.clear();
        let pod = PodObservation::new(input).unwrap();
        assert_eq!(pod.phase(), phase);
        assert_eq!(pod.node_name(), None);
        assert_eq!(pod.pod_ip(), None);
        assert_eq!(pod.started_at(), None);
        assert!(pod.containers().is_empty());
    }
}
#[test]
fn pod_scope_must_match_target_without_resolving_ownership() {
    for (cluster, namespace) in [("other-cluster", "xpa"), ("cluster-xpa", "other-ns")] {
        let mut input = pod_input();
        input.identity = PodIdentity::new(cluster, namespace, "pod", "uid").unwrap();
        assert_eq!(
            PodObservation::new(input).unwrap_err(),
            K8sContextError::PodTargetScopeMismatch
        );
    }
    for kind in [WorkloadKind::StatefulSet, WorkloadKind::DaemonSet] {
        let mut input = pod_input();
        input.target = target(kind);
        assert_eq!(
            PodObservation::new(input).unwrap().target().workload_kind(),
            kind
        );
    }
}
#[test]
fn running_container_preserves_image_readiness_and_source_start_time() {
    let value = container();
    assert_eq!(value.name(), "finance");
    assert_eq!(value.image(), "finance:v1");
    assert_eq!(value.image_id(), Some("image-digest"));
    assert!(value.ready());
    assert_eq!(
        value.state(),
        &ContainerState::Running {
            started_at: Some(fact_time())
        }
    );
}
#[test]
fn running_container_may_be_not_ready_and_lack_source_time() {
    let value = ContainerStatus::new(
        "finance",
        "finance:v1",
        None,
        false,
        0,
        ContainerState::Running { started_at: None },
    )
    .unwrap();
    assert!(!value.ready());
    assert_eq!(value.image_id(), None);
    assert_eq!(value.state(), &ContainerState::Running { started_at: None });
}
#[test]
fn terminated_container_retains_exit_reason_and_optional_finish_time() {
    for (reason, finished_at) in [(Some("Error".into()), Some(fact_time())), (None, None)] {
        let state = ContainerState::Terminated {
            exit_code: 1,
            reason,
            finished_at,
        };
        let value =
            ContainerStatus::new("finance", "finance:v1", None, false, 3, state.clone()).unwrap();
        assert_eq!(value.state(), &state);
    }
}
#[test]
fn waiting_and_unknown_container_states_are_distinct() {
    for state in [
        ContainerState::Waiting {
            reason: Some("CrashLoopBackOff".into()),
        },
        ContainerState::Waiting { reason: None },
        ContainerState::Unknown,
    ] {
        let value =
            ContainerStatus::new("finance", "finance:v1", None, false, 0, state.clone()).unwrap();
        assert_eq!(value.state(), &state);
    }
}
#[test]
fn restart_count_is_preserved_without_inventing_a_health_score() {
    for restarts in [0, 5, u32::MAX] {
        let value = ContainerStatus::new(
            "finance",
            "finance:v1",
            None,
            false,
            restarts,
            ContainerState::Unknown,
        )
        .unwrap();
        assert_eq!(value.restart_count(), restarts);
    }
}
#[test]
fn invalid_container_rejects_empty_name_and_image() {
    for empty in ["", " \t\n"] {
        assert_eq!(
            ContainerStatus::new(empty, "image", None, false, 0, ContainerState::Unknown)
                .unwrap_err(),
            K8sContextError::EmptyContainerName
        );
        assert_eq!(
            ContainerStatus::new("name", empty, None, false, 0, ContainerState::Unknown)
                .unwrap_err(),
            K8sContextError::EmptyImage
        );
    }
}
#[test]
fn deployment_replica_snapshot_permits_rollout_surge_and_generation_lag() {
    let value = DeploymentObservation::new(deployment_input()).unwrap();
    assert_eq!(value.target(), &target(WorkloadKind::Deployment));
    assert_eq!(value.generation(), 9);
    assert_eq!(value.observed_generation(), Some(8));
    assert_eq!(value.desired_replicas(), 3);
    assert_eq!(value.ready_replicas(), Some(5));
    assert_eq!(value.available_replicas(), Some(4));
    assert_eq!(value.updated_replicas(), Some(6));
    assert_eq!(value.image_refs(), &["finance:v1", "finance:v2"]);
    assert_eq!(value.observed_at(), fact_time());
}
#[test]
fn unreported_deployment_status_is_not_replaced_by_zero() {
    let mut input = deployment_input();
    input.observed_generation = None;
    input.ready_replicas = None;
    input.available_replicas = None;
    input.updated_replicas = None;
    input.image_refs.clear();
    let value = DeploymentObservation::new(input).unwrap();
    assert_eq!(value.observed_generation(), None);
    assert_eq!(value.ready_replicas(), None);
    assert_eq!(value.available_replicas(), None);
    assert_eq!(value.updated_replicas(), None);
    assert!(value.image_refs().is_empty());
    assert_ne!(value.ready_replicas(), Some(0));
}
#[test]
fn deployment_can_record_zero_replicas_and_unordered_status_counters() {
    let mut input = deployment_input();
    input.desired_replicas = 0;
    input.ready_replicas = Some(0);
    input.available_replicas = Some(1);
    input.updated_replicas = Some(2);
    input.observed_generation = Some(10);
    let value = DeploymentObservation::new(input).unwrap();
    assert_eq!(value.desired_replicas(), 0);
    assert_eq!(value.ready_replicas(), Some(0));
    assert_eq!(value.observed_generation(), Some(10));
}
#[test]
fn deployment_rejects_non_deployment_target_and_empty_present_images() {
    for kind in [WorkloadKind::StatefulSet, WorkloadKind::DaemonSet] {
        let mut input = deployment_input();
        input.target = target(kind);
        assert_eq!(
            DeploymentObservation::new(input).unwrap_err(),
            K8sContextError::NotDeploymentTarget
        );
    }
    for image in ["", " \t\n"] {
        let mut input = deployment_input();
        input.image_refs.push(image.into());
        assert_eq!(
            DeploymentObservation::new(input).unwrap_err(),
            K8sContextError::EmptyImage
        );
    }
}
#[test]
fn normal_kubernetes_event_preserves_source_facts_and_object_uid() {
    let event = KubernetesEvent::new(event_input(KubernetesEventType::Normal)).unwrap();
    assert_eq!(event.cluster_id(), "cluster-xpa");
    assert_eq!(event.namespace(), "xpa");
    assert_eq!(event.involved_object().kind(), "Pod");
    assert_eq!(event.involved_object().namespace(), "xpa");
    assert_eq!(event.involved_object().name(), "xpa-finance-abc-123");
    assert_eq!(event.involved_object().uid(), Some("opaque-kubernetes-uid"));
    assert_eq!(event.event_type(), KubernetesEventType::Normal);
    assert_eq!(event.reason(), "Scheduled");
    assert_eq!(event.message(), "Successfully assigned pod");
    assert_eq!(event.count(), Some(3));
}
#[test]
fn warning_kubernetes_event_is_a_fact_not_a_diagnosis() {
    let mut input = event_input(KubernetesEventType::Warning);
    input.reason = "BackOff".into();
    input.message = "Back-off restarting container".into();
    let event = KubernetesEvent::new(input).unwrap();
    assert_eq!(event.event_type(), KubernetesEventType::Warning);
    assert_eq!(event.reason(), "BackOff");
    assert_eq!(event.message(), "Back-off restarting container");
}
#[test]
fn unknown_event_can_preserve_absent_source_times_empty_text_and_zero_count() {
    let mut input = event_input(KubernetesEventType::Unknown);
    input.first_timestamp = None;
    input.last_timestamp = None;
    input.reason.clear();
    input.message.clear();
    input.count = Some(0);
    input.involved_object =
        KubernetesObjectRef::new("Deployment", "xpa", "xpa-finance", None).unwrap();
    let event = KubernetesEvent::new(input).unwrap();
    assert_eq!(event.event_type(), KubernetesEventType::Unknown);
    assert_eq!(event.first_timestamp(), None);
    assert_eq!(event.last_timestamp(), None);
    assert_eq!(event.involved_object().uid(), None);
    assert_eq!(event.reason(), "");
    assert_eq!(event.message(), "");
    assert_eq!(event.count(), Some(0));
}
#[test]
fn invalid_object_ref_rejects_kind_namespace_and_name() {
    for empty in ["", " \t\n"] {
        for (kind, namespace, name, error) in [
            (empty, "ns", "name", K8sContextError::EmptyObjectKind),
            ("Pod", empty, "name", K8sContextError::EmptyNamespace),
            ("Pod", "ns", empty, K8sContextError::EmptyObjectName),
        ] {
            assert_eq!(
                KubernetesObjectRef::new(kind, namespace, name, None).unwrap_err(),
                error
            );
        }
    }
}
#[test]
fn invalid_kubernetes_event_rejects_empty_cluster_and_namespace() {
    for empty in ["", " \t\n"] {
        let mut input = event_input(KubernetesEventType::Normal);
        input.cluster_id = empty.into();
        assert_eq!(
            KubernetesEvent::new(input).unwrap_err(),
            K8sContextError::EmptyClusterId
        );
        let mut input = event_input(KubernetesEventType::Normal);
        input.namespace = empty.into();
        assert_eq!(
            KubernetesEvent::new(input).unwrap_err(),
            K8sContextError::EmptyNamespace
        );
    }
}
#[test]
fn pod_and_container_fact_times_are_independent_of_observation_with_full_precision() {
    let mut input = pod_input();
    // Independent provider/observer clocks may disagree; no fabricated time
    // or chronological ordering is imposed at this facts-only boundary.
    input.observed_at = fact_time() - Duration::seconds(5);
    let pod = PodObservation::new(input).unwrap();
    assert_eq!(pod.started_at(), Some(fact_time()));
    assert_eq!(
        pod.started_at().unwrap().timestamp_subsec_nanos(),
        123456789
    );
    assert_eq!(pod.observed_at(), fact_time() - Duration::seconds(5));
    assert_eq!(
        pod.containers()[0].state(),
        &ContainerState::Running {
            started_at: Some(fact_time())
        }
    );
}
#[test]
fn event_source_times_and_observation_time_remain_independent() {
    let event = KubernetesEvent::new(event_input(KubernetesEventType::Normal)).unwrap();
    assert_eq!(event.first_timestamp(), Some(fact_time()));
    assert_eq!(
        event.last_timestamp(),
        Some(fact_time() + Duration::seconds(2))
    );
    assert_eq!(event.observed_at(), fact_time() + Duration::seconds(10));
    assert_eq!(
        event.first_timestamp().unwrap().timestamp_subsec_nanos(),
        123456789
    );
    let mut input = event_input(KubernetesEventType::Warning);
    input.last_timestamp = Some(fact_time() - Duration::seconds(1));
    input.observed_at = fact_time() - Duration::seconds(5);
    let skewed = KubernetesEvent::new(input).unwrap();
    assert_eq!(skewed.first_timestamp(), Some(fact_time()));
    assert_eq!(
        skewed.last_timestamp(),
        Some(fact_time() - Duration::seconds(1))
    );
    assert_eq!(skewed.observed_at(), fact_time() - Duration::seconds(5));
}
#[test]
fn terminated_container_finish_time_is_not_overwritten_by_pod_observation() {
    let mut input = pod_input();
    let state = ContainerState::Terminated {
        exit_code: 0,
        reason: Some("Completed".into()),
        finished_at: Some(fact_time()),
    };
    input.containers =
        vec![ContainerStatus::new("finance", "finance:v1", None, false, 0, state.clone()).unwrap()];
    let pod = PodObservation::new(input).unwrap();
    assert_eq!(pod.containers()[0].state(), &state);
    assert_eq!(pod.observed_at(), fact_time() + Duration::seconds(10));
}
#[test]
fn domain_validation_errors_are_standard_errors_without_provider_details() {
    let error: &dyn std::error::Error = &K8sContextError::EmptyPodUid;
    assert_eq!(error.to_string(), "pod UID must not be empty");
    assert!(error.source().is_none());
}
