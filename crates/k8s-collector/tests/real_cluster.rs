use std::env;

use anisp_k8s_collector::KubernetesSnapshotCollector;
use anisp_k8s_context::{RuntimeTarget, WorkloadKind};

/// Explicitly opt in to READ-ONLY access to the cluster in your kubeconfig.
/// No resources are created and no cluster identity is inferred from context.
#[tokio::test]
#[ignore = "requires kubeconfig and ANISP_K8S_TEST_CLUSTER_ID/NAMESPACE/DEPLOYMENT"]
async fn collect_real_deployment_snapshot() {
    let cluster_id = env::var("ANISP_K8S_TEST_CLUSTER_ID").expect("set ANISP_K8S_TEST_CLUSTER_ID");
    let namespace = env::var("ANISP_K8S_TEST_NAMESPACE").expect("set ANISP_K8S_TEST_NAMESPACE");
    let deployment = env::var("ANISP_K8S_TEST_DEPLOYMENT").expect("set ANISP_K8S_TEST_DEPLOYMENT");
    let target = RuntimeTarget::new(
        &cluster_id,
        namespace,
        WorkloadKind::Deployment,
        deployment,
        None,
        None,
        None,
    )
    .unwrap();
    let client = kube::Client::try_default()
        .await
        .expect("configure Kubernetes API access");
    let collector = KubernetesSnapshotCollector::new(client, cluster_id).unwrap();
    let snapshot = collector
        .collect_deployment_snapshot(&target)
        .await
        .expect("snapshot must succeed");
    assert_eq!(snapshot.deployment.target(), &target);
    assert!(!snapshot.deployment.workload_uid().is_empty());
    assert_eq!(snapshot.deployment.observed_at(), snapshot.observed_at);
    assert!(
        snapshot
            .pods
            .iter()
            .all(|p| p.observed_at() == snapshot.observed_at)
    );
    assert!(
        snapshot
            .events
            .iter()
            .all(|e| e.observed_at() == snapshot.observed_at)
    );
}
