# Kubernetes API Snapshot Collector V1

Read-only, on-demand adapter: Kubernetes API → kube-rs → `anisp-k8s-context`.
Only this infrastructure crate depends on kube/k8s-openapi. The pure Domain
has no provider, storage, HTTP, Event or Support dependency. The server does
not instantiate or expose this collector.

## Usage

```rust,ignore
use anisp_k8s_collector::KubernetesSnapshotCollector;
use anisp_k8s_context::{RuntimeTarget, WorkloadKind};

let client = kube::Client::try_default().await?;
let collector = KubernetesSnapshotCollector::new(client, "business-cluster")?;
let target = RuntimeTarget::new(
    "business-cluster", "xpa", WorkloadKind::Deployment, "xpa-finance",
    Some("xpa".into()), Some("xpa-finance".into()), Some("prod".into()),
)?;
let snapshot = collector.collect_deployment_snapshot(&target).await?;
```

Client connectivity/authentication comes from the caller. Cluster identity
comes only from the explicit business value. A mismatched target fails before
any request. StatefulSet/DaemonSet return `UnsupportedWorkloadKind`.

## Mapping and collection contract

1. GET apps/v1 Deployment by namespace/name; preserve UID, resourceVersion,
   generation, observedGeneration, desired and reported replica counts, and
   ordinary template container images. Missing spec.replicas uses Kubernetes'
   documented default of 1; missing **status** fields remain `None`.
2. LIST core/v1 Pods by the complete Deployment selector: matchLabels, In,
   NotIn, Exists, DoesNotExist combined with AND. Validate keys/values and
   operator cardinality before rendering/URL encoding through kube. Empty
   Deployment selectors and unsupported/malformed requirements fail closed;
   no partial selector or global Pod fallback is used. NotIn retains standard
   Kubernetes semantics, including objects without the key.
3. Map Pod UID/resourceVersion, source phase, Ready=True condition, node/IP,
   startTime, and **status.containerStatuses only**. Running and Ready remain
   independent. Missing status means Unknown/not-ready/no reported containers.
   Do not synthesize status from PodSpec or merge init/ephemeral containers.
   Waiting/Running/Terminated/Unknown preserve restartCount, readiness, image,
   optional imageID, exitCode/reason/finishedAt. Negative unsigned counts or
   contradictory container states fail mapping, rather than dropping facts.
4. LIST namespace core/v1 Events once (not once per Pod). Keep only Events
   whose involvedObject UID matches the current Deployment or a listed Pod.
   Names alone do not identify a Pod instance. Preserve Event UID/version,
   type, reason/message and optional core Event count. A missing count stays
   `None`, even if a separate series exists. Optional text absent from the
   provider is empty Domain text. Source times use:
   firstTimestamp OR eventTime; series.lastObservedTime OR lastTimestamp OR
   eventTime. No fallback to observation time is used.

One observation timestamp is captured at collection start and shared by all
observations, with chrono's source timestamp precision preserved. This is a
sequential best-effort snapshot, **not** a Kubernetes cross-resource atomic
read; different resources retain independent resource versions. Selector
membership is not additional ownerReference/ReplicaSet ownership resolution.
Every mapped Domain object goes through its existing validated constructor.
Missing required identity/spec data fails mapping. No list limit is imposed;
an unexpected nonempty continuation token is an error, never silently ignored.

## Errors

Stable errors: TargetMismatch, UnsupportedWorkloadKind, WorkloadNotFound,
InvalidSelector, Forbidden, ApiUnavailable, Mapping, Domain, Internal.
Deployment GET 404 means WorkloadNotFound; 401/403 means Forbidden. Transport,
throttling and server failures mean ApiUnavailable; malformed responses mean
Mapping. Error messages contain only fixed category/field names or validated
Domain error categories, never provider bodies, credentials or raw kube errors.
Any required API/mapping failure aborts the whole snapshot, not partial success.

## Tests and limits

```sh
cargo test -p anisp-k8s-context -p anisp-k8s-collector
ANISP_K8S_TEST_CLUSTER_ID=business-cluster \
ANISP_K8S_TEST_NAMESPACE=xpa \
ANISP_K8S_TEST_DEPLOYMENT=xpa-finance \
cargo test -p anisp-k8s-collector --test real_cluster -- --ignored
```

Normal tests exercise actual kube client serialization/deserialization and
requests against a mock service with no cluster/network. The ignored test
uses kubeconfig via `Client::try_default()` and needs namespace-scoped RBAC:
get deployments (apps), list pods and events (core). It is strictly read-only.

kube 2.0.x + k8s-openapi 0.26.x/v1_34 are used to retain workspace Rust 1.85
compatibility; v1_34 selects SDK schemas, not a collector requirement to create
a Kubernetes 1.34 cluster. No Watch, continuous polling, retries, custom
collector timeout/config, source logs, metrics, new resources, projection,
storage, HTTP routes or SupportCase association are included. Namespace Event
LIST can be large; source Events are best-effort/retained by Kubernetes and
cannot be interpreted as a complete historical timeline.
