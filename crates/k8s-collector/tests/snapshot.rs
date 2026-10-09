//! Real kube::Client requests over a deterministic mock HTTP service. No
//! kubeconfig, cluster, network, clock sleeps, or provider type in Domain.
use std::{
    collections::VecDeque,
    convert::Infallible,
    error::Error,
    sync::{Arc, Mutex},
};

use anisp_k8s_collector::{CollectorError, DeploymentSnapshot, KubernetesSnapshotCollector};
use anisp_k8s_context::{
    ContainerState, K8sContextError, KubernetesEventType, PodPhase, RuntimeTarget, WorkloadKind,
};
use chrono::{DateTime, Utc};
use http::{Request, Response};
use kube::{Client, client::Body};
use serde_json::{Value, json};
use tower::service_fn;

const DEPLOYMENT_PATH: &str = "/apis/apps/v1/namespaces/xpa/deployments/finance";
const PODS_PATH: &str = "/api/v1/namespaces/xpa/pods";
const EVENTS_PATH: &str = "/api/v1/namespaces/xpa/events";
const FACT_TIME: &str = "2026-10-08T01:02:03.123456Z";
const LATER_TIME: &str = "2026-10-08T01:03:04.654321Z";

fn target(kind: WorkloadKind) -> RuntimeTarget {
    RuntimeTarget::new(
        "cluster-business",
        "xpa",
        kind,
        "finance",
        Some("xpa".into()),
        Some("xpa-finance".into()),
        Some("prod".into()),
    )
    .unwrap()
}

fn deployment() -> Value {
    json!({"apiVersion":"apps/v1", "kind":"Deployment",
        "metadata":{"name":"finance", "namespace":"xpa", "uid":"deployment-uid", "resourceVersion":"dep-rv", "generation":7},
        "spec":{"replicas":3, "selector":{"matchLabels":{"app":"finance"}},
            "template":{"spec":{"containers":[{"name":"finance", "image":"finance:v3"}]}}},
        "status":{"observedGeneration":6, "readyReplicas":4, "availableReplicas":2, "updatedReplicas":3}})
}

fn pod(name: &str, uid: &str, phase: &str, ready: &str) -> Value {
    json!({"apiVersion":"v1", "kind":"Pod",
        "metadata":{"name":name, "namespace":"xpa", "uid":uid, "resourceVersion":"pod-rv"},
        "spec":{"nodeName":"node-a", "containers":[{"name":"finance", "image":"finance:v3"}]},
        "status":{"phase":phase, "conditions":[{"type":"Ready", "status":ready}],
            "podIP":"10.0.0.8", "startTime":FACT_TIME,
            "containerStatuses":[{"name":"finance", "image":"finance:v3", "imageID":"sha256:digest",
                "ready":true, "restartCount":5, "state":{"running":{"startedAt":FACT_TIME}}}]}})
}

fn event(uid: &str, object_uid: Option<&str>, event_type: &str) -> Value {
    json!({"apiVersion":"v1", "kind":"Event",
        "metadata":{"name":uid, "namespace":"xpa", "uid":uid, "resourceVersion":"event-rv"},
        "involvedObject":{"kind":"Pod", "namespace":"xpa", "name":"finance-pod", "uid":object_uid},
        "type":event_type, "reason":"Restarted", "message":"runtime fact", "count":4,
        "firstTimestamp":FACT_TIME, "lastTimestamp":LATER_TIME})
}

fn list(kind: &str, items: Vec<Value>) -> Value {
    json!({"apiVersion":"v1", "kind":kind, "metadata":{}, "items":items})
}

type Calls = Arc<Mutex<Vec<(String, String)>>>;
type Replies = Arc<Mutex<VecDeque<(String, u16, Value)>>>;

fn mock(replies: Vec<(&str, u16, Value)>) -> (Client, Calls, Replies) {
    let replies: Replies = Arc::new(Mutex::new(
        replies
            .into_iter()
            .map(|(path, status, value)| (path.into(), status, value))
            .collect(),
    ));
    let calls: Calls = Arc::new(Mutex::new(vec![]));
    let responses = replies.clone();
    let requests = calls.clone();
    let service = service_fn(move |request: Request<Body>| {
        let (path, status, body) = responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected API request");
        assert_eq!(request.method(), "GET");
        assert_eq!(request.uri().path(), path);
        requests
            .lock()
            .unwrap()
            .push((path, request.uri().query().unwrap_or_default().into()));
        async move {
            Ok::<_, Infallible>(
                Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
        }
    });
    (
        Client::new(service, "unused-default-namespace"),
        calls,
        replies,
    )
}

async fn collect(
    dep: Value,
    pods: Vec<Value>,
    events: Vec<Value>,
) -> Result<DeploymentSnapshot, CollectorError> {
    let (client, _, _) = mock(vec![
        (DEPLOYMENT_PATH, 200, dep),
        (PODS_PATH, 200, list("PodList", pods)),
        (EVENTS_PATH, 200, list("EventList", events)),
    ]);
    KubernetesSnapshotCollector::new(client, "cluster-business")
        .unwrap()
        .collect_deployment_snapshot(&target(WorkloadKind::Deployment))
        .await
}

#[tokio::test]
async fn deployment_mapping_retains_uid_version_generations_replicas_and_images() {
    let snapshot = collect(deployment(), vec![], vec![]).await.unwrap();
    let d = snapshot.deployment;
    assert_eq!(d.target(), &target(WorkloadKind::Deployment));
    assert_eq!(d.workload_uid(), "deployment-uid");
    assert_eq!(d.resource_version(), Some("dep-rv"));
    assert_eq!(d.generation(), 7);
    assert_eq!(d.observed_generation(), Some(6));
    assert_eq!(d.desired_replicas(), 3);
    assert_eq!(d.ready_replicas(), Some(4)); // surge is a valid snapshot
    assert_eq!(d.available_replicas(), Some(2));
    assert_eq!(d.updated_replicas(), Some(3));
    assert_eq!(d.image_refs(), ["finance:v3"]);
}

#[tokio::test]
async fn missing_deployment_status_is_none_not_zero_and_spec_default_is_one() {
    let mut dep = deployment();
    dep.as_object_mut().unwrap().remove("status");
    dep["spec"].as_object_mut().unwrap().remove("replicas");
    let d = collect(dep, vec![], vec![]).await.unwrap().deployment;
    assert_eq!(d.observed_generation(), None);
    assert_eq!(d.ready_replicas(), None);
    assert_eq!(d.available_replicas(), None);
    assert_eq!(d.updated_replicas(), None);
    assert_eq!(d.desired_replicas(), 1);
    let mut dep = deployment();
    dep["spec"]["replicas"] = json!(0);
    dep["status"]["readyReplicas"] = json!(0);
    let d = collect(dep, vec![], vec![]).await.unwrap().deployment;
    assert_eq!(d.desired_replicas(), 0);
    assert_eq!(d.ready_replicas(), Some(0));
}

#[tokio::test]
async fn selector_is_applied_to_pod_list_and_events_are_listed_once() {
    let mut dep = deployment();
    dep["spec"]["selector"]["matchExpressions"] = json!([
        {"key":"env", "operator":"In", "values":["prod","staging"]},
        {"key":"tier", "operator":"NotIn", "values":["batch"]},
        {"key":"enabled", "operator":"Exists"},
        {"key":"disabled", "operator":"DoesNotExist"}]);
    let (client, calls, replies) = mock(vec![
        (DEPLOYMENT_PATH, 200, dep),
        (
            PODS_PATH,
            200,
            list(
                "PodList",
                vec![
                    pod("a", "pod-a", "Running", "True"),
                    pod("b", "pod-b", "Running", "False"),
                ],
            ),
        ),
        (EVENTS_PATH, 200, list("EventList", vec![])),
    ]);
    KubernetesSnapshotCollector::new(client, "cluster-business")
        .unwrap()
        .collect_deployment_snapshot(&target(WorkloadKind::Deployment))
        .await
        .unwrap();
    assert!(replies.lock().unwrap().is_empty());
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    let query: Vec<_> = url::form_urlencoded::parse(calls[1].1.as_bytes()).collect();
    let selector = query
        .iter()
        .find(|(key, _)| key == "labelSelector")
        .unwrap();
    assert_eq!(
        selector.1,
        "app=finance,env in (prod,staging),tier notin (batch),enabled,!disabled"
    );
    assert!(!calls[2].1.contains("fieldSelector"));
}

#[tokio::test]
async fn running_ready_running_not_ready_and_failed_are_independent() {
    let s = collect(
        deployment(),
        vec![
            pod("a", "pod-a", "Running", "True"),
            pod("b", "pod-b", "Running", "False"),
            pod("c", "pod-c", "Failed", "False"),
        ],
        vec![],
    )
    .await
    .unwrap();
    assert_eq!(s.pods[0].phase(), PodPhase::Running);
    assert!(s.pods[0].ready());
    assert_eq!(s.pods[1].phase(), PodPhase::Running);
    assert!(!s.pods[1].ready());
    assert_eq!(s.pods[2].phase(), PodPhase::Failed);
    assert!(!s.pods[2].ready());
    let p = &s.pods[0];
    assert_eq!(p.identity().pod_uid(), "pod-a");
    assert_eq!(p.resource_version(), Some("pod-rv"));
    assert_eq!(p.node_name(), Some("node-a"));
    assert_eq!(p.pod_ip(), Some("10.0.0.8"));
    assert_eq!(p.started_at(), Some(FACT_TIME.parse().unwrap()));
    let c = &p.containers()[0];
    assert_eq!(c.name(), "finance");
    assert_eq!(c.image(), "finance:v3");
    assert_eq!(c.image_id(), Some("sha256:digest"));
    assert_eq!(c.restart_count(), 5);
    assert!(c.ready());
    assert_eq!(
        c.state(),
        &ContainerState::Running {
            started_at: Some(FACT_TIME.parse().unwrap())
        }
    );
}

#[tokio::test]
async fn terminated_waiting_and_unknown_containers_preserve_source_facts() {
    for (state, expected) in [
        (
            json!({"terminated":{"exitCode":137,"reason":"OOMKilled","finishedAt":LATER_TIME}}),
            ContainerState::Terminated {
                exit_code: 137,
                reason: Some("OOMKilled".into()),
                finished_at: Some(LATER_TIME.parse().unwrap()),
            },
        ),
        (
            json!({"waiting":{"reason":"ImagePullBackOff"}}),
            ContainerState::Waiting {
                reason: Some("ImagePullBackOff".into()),
            },
        ),
        (json!({}), ContainerState::Unknown),
        (Value::Null, ContainerState::Unknown),
    ] {
        let mut p = pod("a", "pod-a", "Running", "False");
        p["status"]["containerStatuses"][0]["state"] = state;
        p["status"]["containerStatuses"][0]["imageID"] = json!("");
        let s = collect(deployment(), vec![p], vec![]).await.unwrap();
        assert_eq!(s.pods[0].containers()[0].state(), &expected);
        assert_eq!(s.pods[0].containers()[0].image_id(), None);
    }
}

#[tokio::test]
async fn absent_statuses_are_not_synthesized_and_init_ephemeral_are_not_merged() {
    let mut p = pod("a", "pod-a", "Running", "True");
    let status = p["status"].as_object_mut().unwrap();
    let containers = status.remove("containerStatuses").unwrap();
    status.insert("initContainerStatuses".into(), containers.clone());
    status.insert("ephemeralContainerStatuses".into(), containers);
    status.remove("conditions");
    let s = collect(deployment(), vec![p], vec![]).await.unwrap();
    assert!(s.pods[0].containers().is_empty());
    assert!(!s.pods[0].ready());
}

#[tokio::test]
async fn absent_pod_status_is_unknown_not_ready_and_has_no_containers() {
    let mut p = pod("a", "pod-a", "Running", "True");
    p.as_object_mut().unwrap().remove("status");
    let s = collect(deployment(), vec![p], vec![]).await.unwrap();
    assert_eq!(s.pods[0].phase(), PodPhase::Unknown);
    assert!(!s.pods[0].ready());
    assert!(s.pods[0].containers().is_empty());
    assert_eq!(s.pods[0].started_at(), None);
}

#[tokio::test]
async fn events_are_uid_filtered_not_name_filtered_and_share_observation_time() {
    let mut normal = event("normal", Some("deployment-uid"), "Normal");
    normal["involvedObject"]["kind"] = json!("Deployment");
    normal["involvedObject"]["name"] = json!("finance");
    let before = Utc::now();
    let s = collect(
        deployment(),
        vec![pod("a", "pod-a", "Running", "True")],
        vec![
            normal,
            event("warning", Some("pod-a"), "Warning"),
            event("old-instance-same-name", Some("old-pod-uid"), "Warning"),
            event("uid-missing", None, "Warning"),
        ],
    )
    .await
    .unwrap();
    assert!(s.observed_at >= before && s.observed_at <= Utc::now());
    assert_eq!(s.deployment.observed_at(), s.observed_at);
    assert!(s.pods.iter().all(|p| p.observed_at() == s.observed_at));
    assert!(s.events.iter().all(|e| e.observed_at() == s.observed_at));
    assert_eq!(s.events.len(), 2);
    assert_eq!(s.events[0].event_type(), KubernetesEventType::Normal);
    assert_eq!(s.events[1].event_type(), KubernetesEventType::Warning);
    assert_eq!(s.events[1].event_uid(), "warning");
    assert_eq!(s.events[1].resource_version(), Some("event-rv"));
    assert_eq!(s.events[1].count(), Some(4));
    assert_eq!(
        s.events[1].first_timestamp(),
        Some(FACT_TIME.parse().unwrap())
    );
    assert_eq!(
        s.events[1].last_timestamp(),
        Some(LATER_TIME.parse().unwrap())
    );
    assert_ne!(s.events[1].first_timestamp(), Some(s.observed_at));
}

#[tokio::test]
async fn event_time_precedence_and_unknown_count_are_preserved() {
    let mut e = event("e", Some("deployment-uid"), "FutureType");
    e.as_object_mut().unwrap().remove("count");
    e["eventTime"] = json!(FACT_TIME);
    e["series"] = json!({"count":9,"lastObservedTime":LATER_TIME});
    e["lastTimestamp"] = json!(FACT_TIME);
    let s = collect(deployment(), vec![], vec![e.clone()])
        .await
        .unwrap();
    assert_eq!(s.events[0].event_type(), KubernetesEventType::Unknown);
    assert_eq!(s.events[0].count(), None);
    assert_eq!(
        s.events[0].last_timestamp(),
        Some(LATER_TIME.parse().unwrap())
    );
    e.as_object_mut().unwrap().remove("series");
    e.as_object_mut().unwrap().remove("firstTimestamp");
    e.as_object_mut().unwrap().remove("lastTimestamp");
    let s = collect(deployment(), vec![], vec![e.clone()])
        .await
        .unwrap();
    assert_eq!(
        s.events[0].first_timestamp(),
        Some(FACT_TIME.parse().unwrap())
    );
    assert_eq!(
        s.events[0].last_timestamp(),
        Some(FACT_TIME.parse().unwrap())
    );
    e.as_object_mut().unwrap().remove("eventTime");
    let s = collect(deployment(), vec![], vec![e]).await.unwrap();
    assert_eq!(s.events[0].first_timestamp(), None);
    assert_eq!(s.events[0].last_timestamp(), None);
}

#[tokio::test]
async fn event_first_and_last_fallback_precedence_is_exact() {
    let mut e = event("e", Some("deployment-uid"), "Normal");
    e["eventTime"] = json!("2026-10-08T01:01:01.000001Z");
    e["series"] = json!({"count":8,"lastObservedTime":"2026-10-08T01:04:05.000001Z"});
    let s = collect(deployment(), vec![], vec![e.clone()])
        .await
        .unwrap();
    assert_eq!(
        s.events[0].first_timestamp(),
        Some(FACT_TIME.parse().unwrap())
    );
    assert_eq!(
        s.events[0].last_timestamp(),
        Some("2026-10-08T01:04:05.000001Z".parse().unwrap())
    );
    // A present series without its lastObservedTime must fall through.
    e["series"]
        .as_object_mut()
        .unwrap()
        .remove("lastObservedTime");
    let s = collect(deployment(), vec![], vec![e]).await.unwrap();
    assert_eq!(
        s.events[0].last_timestamp(),
        Some(LATER_TIME.parse().unwrap())
    );
}

#[tokio::test]
async fn missing_pod_event_uid_and_foreign_namespace_fail_mapping() {
    let mut p = pod("a", "pod-a", "Running", "True");
    p["metadata"].as_object_mut().unwrap().remove("uid");
    assert_eq!(
        collect(deployment(), vec![p], vec![]).await,
        Err(CollectorError::Mapping("pod.metadata.uid"))
    );
    let mut e = event("e", Some("deployment-uid"), "Normal");
    e["metadata"].as_object_mut().unwrap().remove("uid");
    assert_eq!(
        collect(deployment(), vec![], vec![e]).await,
        Err(CollectorError::Mapping("event.metadata.uid"))
    );
    let mut p = pod("a", "pod-a", "Running", "True");
    p["metadata"]["namespace"] = json!("other-namespace");
    assert_eq!(
        collect(deployment(), vec![p], vec![]).await,
        Err(CollectorError::Mapping("metadata.namespace"))
    );
}

#[tokio::test]
async fn mismatched_target_and_unsupported_workloads_do_not_call_api() {
    let (client, calls, _) = mock(vec![]);
    let collector = KubernetesSnapshotCollector::new(client, "other-business-cluster").unwrap();
    assert_eq!(
        collector
            .collect_deployment_snapshot(&target(WorkloadKind::Deployment))
            .await,
        Err(CollectorError::TargetMismatch)
    );
    assert!(calls.lock().unwrap().is_empty());
    let (client, calls, _) = mock(vec![]);
    let collector = KubernetesSnapshotCollector::new(client, "cluster-business").unwrap();
    for kind in [WorkloadKind::StatefulSet, WorkloadKind::DaemonSet] {
        assert_eq!(
            collector.collect_deployment_snapshot(&target(kind)).await,
            Err(CollectorError::UnsupportedWorkloadKind)
        );
    }
    assert!(calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn empty_cluster_id_is_rejected() {
    let (client, _, _) = mock(vec![]);
    assert!(matches!(
        KubernetesSnapshotCollector::new(client, " "),
        Err(CollectorError::Domain(K8sContextError::EmptyClusterId))
    ));
}

#[tokio::test]
async fn invalid_selector_stops_before_pods_or_events_are_requested() {
    for selector in [
        json!({}),
        json!({"matchLabels":{"app":"finance,other"}}),
        json!({"matchExpressions":[{"key":"app","operator":"Exists","values":["ignored"]}]}),
    ] {
        let mut dep = deployment();
        dep["spec"]["selector"] = selector;
        let (client, calls, _) = mock(vec![(DEPLOYMENT_PATH, 200, dep)]);
        let result = KubernetesSnapshotCollector::new(client, "cluster-business")
            .unwrap()
            .collect_deployment_snapshot(&target(WorkloadKind::Deployment))
            .await;
        assert_eq!(result, Err(CollectorError::InvalidSelector));
        assert_eq!(calls.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn provider_failures_have_stable_categories_and_no_raw_error_source() {
    for (code, expected) in [
        (404, CollectorError::WorkloadNotFound),
        (403, CollectorError::Forbidden),
        (401, CollectorError::Forbidden),
        (429, CollectorError::ApiUnavailable),
        (503, CollectorError::ApiUnavailable),
    ] {
        let body = json!({"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Failure",
            "message":"sensitive provider details","code":code});
        let (client, _, _) = mock(vec![(DEPLOYMENT_PATH, code, body)]);
        let error = KubernetesSnapshotCollector::new(client, "cluster-business")
            .unwrap()
            .collect_deployment_snapshot(&target(WorkloadKind::Deployment))
            .await
            .unwrap_err();
        assert_eq!(error, expected);
        assert!(!error.to_string().contains("sensitive"));
        assert!(error.source().is_none());
    }
}

#[tokio::test]
async fn network_failure_is_api_unavailable_without_raw_message() {
    let service = service_fn(|_: Request<Body>| async {
        Err::<Response<Body>, _>(std::io::Error::other("sensitive transport detail"))
    });
    let collector =
        KubernetesSnapshotCollector::new(Client::new(service, "default"), "cluster-business")
            .unwrap();
    assert_eq!(
        collector
            .collect_deployment_snapshot(&target(WorkloadKind::Deployment))
            .await,
        Err(CollectorError::ApiUnavailable)
    );
}

#[tokio::test]
async fn pod_or_event_api_failure_fails_the_whole_snapshot() {
    let failure = json!({"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Forbidden","message":"secret", "code":403});
    for replies in [
        vec![
            (DEPLOYMENT_PATH, 200, deployment()),
            (PODS_PATH, 403, failure.clone()),
        ],
        vec![
            (DEPLOYMENT_PATH, 200, deployment()),
            (PODS_PATH, 200, list("PodList", vec![])),
            (EVENTS_PATH, 403, failure),
        ],
    ] {
        let (client, _, _) = mock(replies);
        assert_eq!(
            KubernetesSnapshotCollector::new(client, "cluster-business")
                .unwrap()
                .collect_deployment_snapshot(&target(WorkloadKind::Deployment))
                .await,
            Err(CollectorError::Forbidden)
        );
    }
}

#[tokio::test]
async fn missing_uid_and_negative_counters_are_mapping_errors_not_defaults() {
    let mut dep = deployment();
    dep["metadata"].as_object_mut().unwrap().remove("uid");
    assert_eq!(
        collect(dep, vec![], vec![]).await,
        Err(CollectorError::Mapping("deployment.metadata.uid"))
    );
    let mut dep = deployment();
    dep["status"]["readyReplicas"] = json!(-1);
    assert_eq!(
        collect(dep, vec![], vec![]).await,
        Err(CollectorError::Mapping("ready_replicas"))
    );
    let mut p = pod("a", "pod-a", "Running", "True");
    p["status"]["containerStatuses"][0]["restartCount"] = json!(-1);
    assert_eq!(
        collect(deployment(), vec![p], vec![]).await,
        Err(CollectorError::Mapping("container.restart_count"))
    );
}

#[tokio::test]
async fn domain_invalid_image_and_malformed_container_state_are_not_silently_accepted() {
    let mut p = pod("a", "pod-a", "Running", "True");
    p["status"]["containerStatuses"][0]["image"] = json!(" ");
    assert_eq!(
        collect(deployment(), vec![p], vec![]).await,
        Err(CollectorError::Domain(K8sContextError::EmptyImage))
    );
    let mut p = pod("a", "pod-a", "Running", "True");
    p["status"]["containerStatuses"][0]["state"]["waiting"] = json!({"reason":"bad"});
    assert_eq!(
        collect(deployment(), vec![p], vec![]).await,
        Err(CollectorError::Mapping("container.state"))
    );
}

#[tokio::test]
async fn malformed_response_becomes_mapping_error() {
    let (client, _, _) = mock(vec![(
        DEPLOYMENT_PATH,
        200,
        json!({"metadata":{"generation":"not-a-number"}}),
    )]);
    assert_eq!(
        KubernetesSnapshotCollector::new(client, "cluster-business")
            .unwrap()
            .collect_deployment_snapshot(&target(WorkloadKind::Deployment))
            .await,
        Err(CollectorError::Mapping("API response"))
    );
}

#[tokio::test]
async fn incomplete_lists_are_rejected_not_returned_as_complete_snapshots() {
    let mut pods = list("PodList", vec![]);
    pods["metadata"]["continue"] = json!("another-page");
    let (client, _, _) = mock(vec![
        (DEPLOYMENT_PATH, 200, deployment()),
        (PODS_PATH, 200, pods),
    ]);
    assert_eq!(
        KubernetesSnapshotCollector::new(client, "cluster-business")
            .unwrap()
            .collect_deployment_snapshot(&target(WorkloadKind::Deployment))
            .await,
        Err(CollectorError::Mapping("incomplete pod list"))
    );
}

#[test]
fn fact_time_keeps_submillisecond_precision() {
    let value: DateTime<Utc> = FACT_TIME.parse().unwrap();
    assert_eq!(value.timestamp_subsec_micros(), 123456);
}
