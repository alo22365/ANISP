mod common;

use std::{
    future,
    sync::{Arc, Mutex},
};

use anisp_clickhouse_store::{ClickHouseReadiness, PingFuture};
use anisp_config::AppConfig;
use anisp_context::{SupportCaseContextService, SupportContextSyncReader};
use anisp_event::{
    ContextEvent, EventQuery, EventRepository, EventService, RepositoryError, StoredContextEvent,
};
use anisp_git_context::{
    ChangeType, GitCommitInput, GitCommitView, GitCommitViewInput, GitContextQueryService,
    GitContextSyncReader, GitFileChange, GitReaderError,
};
use anisp_postgres_store::{PostgresPingFuture, PostgresReadiness};
use anisp_server::{AppState, build_app};
use anisp_support::{
    CaseType, CreateSupportCaseCommand, Priority, SupportCase, SupportCaseLifecycleEvent,
    SupportCaseQuery, SupportCaseRepository, SupportOutboxOperationalReader,
    SupportOutboxOperationalStatus, SupportRepositoryError, SupportService,
};
use anisp_support_outbox::SupportOutboxStatusService;
use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use chrono::{Duration, TimeZone, Utc};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

struct StubClickHouse;
struct StubPostgres;
struct NoopEventRepository;

impl ClickHouseReadiness for StubClickHouse {
    fn ping(&self) -> PingFuture<'_> {
        Box::pin(future::ready(Ok(())))
    }
}

impl PostgresReadiness for StubPostgres {
    fn ping(&self) -> PostgresPingFuture<'_> {
        Box::pin(future::ready(Ok(())))
    }
}

#[async_trait]
impl EventRepository for NoopEventRepository {
    async fn insert(&self, _event: &ContextEvent) -> Result<(), RepositoryError> {
        Ok(())
    }

    async fn find_by_id(&self, _event_id: Uuid) -> Result<Option<ContextEvent>, RepositoryError> {
        Ok(None)
    }

    async fn search(&self, _query: &EventQuery) -> Result<Vec<ContextEvent>, RepositoryError> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Copy)]
enum Failure {
    Unavailable,
    Timeout,
    Decode,
    Internal,
}

struct FakeSupportRepository {
    cases: Mutex<Vec<SupportCase>>,
    pending: Mutex<u64>,
    failure: Option<Failure>,
    update_conflict: bool,
}

impl FakeSupportRepository {
    fn available() -> Self {
        Self {
            cases: Mutex::new(Vec::new()),
            pending: Mutex::new(0),
            failure: None,
            update_conflict: false,
        }
    }

    fn failing(failure: Failure) -> Self {
        Self {
            cases: Mutex::new(Vec::new()),
            pending: Mutex::new(0),
            failure: Some(failure),
            update_conflict: false,
        }
    }

    fn conflicting_update() -> Self {
        Self {
            cases: Mutex::new(Vec::new()),
            pending: Mutex::new(0),
            failure: None,
            update_conflict: true,
        }
    }

    fn failure(&self) -> Result<(), SupportRepositoryError> {
        match self.failure {
            Some(Failure::Unavailable) => Err(SupportRepositoryError::unavailable()),
            Some(Failure::Timeout) => Err(SupportRepositoryError::timeout()),
            Some(Failure::Decode) => Err(SupportRepositoryError::decode()),
            Some(Failure::Internal) => Err(SupportRepositoryError::internal()),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl SupportCaseRepository for FakeSupportRepository {
    async fn insert(
        &self,
        support_case: &SupportCase,
        _lifecycle_event: &SupportCaseLifecycleEvent,
    ) -> Result<(), SupportRepositoryError> {
        self.failure()?;
        self.cases.lock().unwrap().push(support_case.clone());
        Ok(())
    }

    async fn update(
        &self,
        support_case: &SupportCase,
        _lifecycle_event: &SupportCaseLifecycleEvent,
    ) -> Result<(), SupportRepositoryError> {
        if self.update_conflict {
            return Err(SupportRepositoryError::conflict());
        }
        self.failure()?;
        let mut cases = self.cases.lock().unwrap();
        let persisted = cases
            .iter_mut()
            .find(|persisted| persisted.case_id() == support_case.case_id())
            .ok_or_else(SupportRepositoryError::internal)?;
        *persisted = support_case.clone();
        Ok(())
    }

    async fn find_by_id(
        &self,
        case_id: Uuid,
    ) -> Result<Option<SupportCase>, SupportRepositoryError> {
        self.failure()?;
        Ok(self
            .cases
            .lock()
            .unwrap()
            .iter()
            .find(|support_case| support_case.case_id() == case_id)
            .cloned())
    }

    async fn search(
        &self,
        query: &SupportCaseQuery,
    ) -> Result<Vec<SupportCase>, SupportRepositoryError> {
        self.failure()?;
        let filters = query.filters();
        let mut cases: Vec<_> = self
            .cases
            .lock()
            .unwrap()
            .iter()
            .filter(|support_case| {
                filters
                    .case_type
                    .is_none_or(|value| support_case.case_type() == value)
                    && filters
                        .status
                        .is_none_or(|value| support_case.status() == value)
                    && filters
                        .priority
                        .is_none_or(|value| support_case.priority() == value)
                    && filters
                        .reporter_id
                        .as_deref()
                        .is_none_or(|value| support_case.reporter_id() == value)
                    && filters
                        .assignee_id
                        .as_deref()
                        .is_none_or(|value| support_case.assignee_id() == Some(value))
                    && filters
                        .project_id
                        .as_deref()
                        .is_none_or(|value| support_case.project_id() == Some(value))
                    && filters
                        .service
                        .as_deref()
                        .is_none_or(|value| support_case.service() == Some(value))
                    && filters
                        .environment
                        .as_deref()
                        .is_none_or(|value| support_case.environment() == Some(value))
                    && query
                        .created_from()
                        .is_none_or(|from| support_case.created_at() >= from)
                    && query
                        .created_to()
                        .is_none_or(|to| support_case.created_at() < to)
            })
            .cloned()
            .collect();
        cases.sort_by(|left, right| {
            right
                .created_at()
                .cmp(&left.created_at())
                .then_with(|| right.case_id().cmp(&left.case_id()))
        });
        cases.truncate(usize::from(query.limit()));
        Ok(cases)
    }
}

struct FakeContextEventRepository {
    events: Mutex<Vec<ContextEvent>>,
    unavailable: bool,
}

#[async_trait]
impl EventRepository for FakeContextEventRepository {
    async fn insert(&self, _event: &ContextEvent) -> Result<(), RepositoryError> {
        Ok(())
    }

    async fn find_by_id(&self, event_id: Uuid) -> Result<Option<ContextEvent>, RepositoryError> {
        if self.unavailable {
            return Err(RepositoryError::unavailable());
        }
        Ok(self
            .events
            .lock()
            .unwrap()
            .iter()
            .find(|event| event.event_id() == event_id)
            .cloned())
    }

    async fn search(&self, query: &EventQuery) -> Result<Vec<ContextEvent>, RepositoryError> {
        if self.unavailable {
            return Err(RepositoryError::unavailable());
        }
        let filters = query.filters();
        let mut events: Vec<_> = self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| {
                filters
                    .source
                    .as_deref()
                    .is_none_or(|value| event.source() == value)
                    && filters
                        .subject_type
                        .as_deref()
                        .is_none_or(|value| event.subject_type() == value)
                    && filters
                        .subject_id
                        .as_deref()
                        .is_none_or(|value| event.subject_id() == value)
            })
            .cloned()
            .collect();
        events.sort_by(|left, right| right.event_time().cmp(&left.event_time()));
        Ok(events)
    }
}

#[async_trait]
impl SupportContextSyncReader for FakeSupportRepository {
    async fn pending_count_for_case(&self, _case_id: Uuid) -> Result<u64, SupportRepositoryError> {
        self.failure()?;
        Ok(*self.pending.lock().unwrap())
    }
}

#[async_trait]
impl SupportOutboxOperationalReader for FakeSupportRepository {
    async fn operational_status(
        &self,
        _now: chrono::DateTime<Utc>,
    ) -> Result<SupportOutboxOperationalStatus, SupportRepositoryError> {
        self.failure()?;
        let pending = *self.pending.lock().unwrap();
        Ok(SupportOutboxOperationalStatus::new(
            pending,
            (pending > 0).then_some(5),
            0,
            None,
        ))
    }
}

fn app_with_repository(repository: Arc<FakeSupportRepository>) -> axum::Router {
    app_with_context_event_repository(repository, Arc::new(NoopEventRepository))
}

fn app_with_context_event_repository(
    repository: Arc<FakeSupportRepository>,
    event_repository: Arc<dyn EventRepository>,
) -> axum::Router {
    app_with_git_dependencies(
        repository,
        event_repository,
        common::empty_git_service(),
        common::empty_git_sync_reader(),
    )
}

fn app_with_git_dependencies(
    repository: Arc<FakeSupportRepository>,
    event_repository: Arc<dyn EventRepository>,
    git_service: GitContextQueryService,
    git_sync: Arc<dyn GitContextSyncReader>,
) -> axum::Router {
    let event_service = EventService::new(event_repository);
    let support_service = SupportService::new(repository.clone());
    let context_service = SupportCaseContextService::new(
        support_service.clone(),
        event_service.clone(),
        repository.clone(),
        git_service.clone(),
        git_sync,
        Default::default(),
    );
    let outbox_status_service = SupportOutboxStatusService::new(repository);
    let state = AppState::with_dependencies(
        AppConfig::default(),
        StubClickHouse,
        StubPostgres,
        event_service,
        support_service,
        context_service,
        outbox_status_service,
        git_service,
        common::empty_git_status_service(),
    );
    build_app(state)
}

fn app_with_failure(failure: Failure) -> axum::Router {
    app_with_repository(Arc::new(FakeSupportRepository::failing(failure)))
}

fn valid_request() -> Value {
    json!({
        "case_type": "incident",
        "title": "xpa-finance unavailable",
        "description": "Users cannot access xpa-finance production service.",
        "reporter_id": "user-001",
        "assignee_id": null,
        "project_id": "xpa",
        "service": "xpa-finance",
        "environment": "prod",
        "priority": "high"
    })
}

async fn context_json(app: axum::Router, case_id: Uuid, status: StatusCode) -> Value {
    let response = app
        .oneshot(
            Request::get(format!("/api/v1/support/cases/{case_id}/context"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), status);
    let id = request_id(&response);
    let value: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    if value.get("error").is_some() {
        assert_eq!(value["error"]["request_id"], id);
    }
    value
}

#[tokio::test]
async fn context_git_multiple_repositories_window_order_truncation_and_binary_null() {
    let repository = Arc::new(FakeSupportRepository::available());
    let case = SupportService::new(repository.clone())
        .create_case(valid_command())
        .await
        .unwrap();
    let t = case.created_at().timestamp();
    let binary = GitCommitView::new(GitCommitViewInput {
        event_id: Uuid::now_v7(),
        repository_name: "repo-b".into(),
        project_id: "xpa".into(),
        service: "xpa-finance".into(),
        branch: None,
        commit: GitCommitInput {
            repository_id: "repo-b".into(),
            commit_sha: "c".repeat(40),
            parent_shas: vec!["b".repeat(40)],
            author_name: "Author".into(),
            author_email: None,
            message: "Commit C".into(),
            committed_at: chrono::DateTime::from_timestamp(t - 1, 0).unwrap(),
            changes: vec![
                GitFileChange::new("binary.bin", None, ChangeType::Added, None, None).unwrap(),
            ],
        },
        observed_at: Utc::now(),
        changed_file_count: 1000,
        additions: None,
        deletions: None,
        changes_truncated: true,
    })
    .unwrap();
    let rows = vec![
        binary,
        common::view("repo-a", 'a', t - 3, "xpa", "xpa-finance", Some("main")),
        common::view("repo-b", 'b', t - 2, "xpa", "xpa-finance", Some("main")),
        common::view(
            "repo-a",
            'd',
            t - 172800,
            "xpa",
            "xpa-finance",
            Some("main"),
        ),
        common::view("repo-a", 'e', t + 10, "xpa", "xpa-finance", Some("main")),
        common::view("other", 'f', t - 4, "other", "xpa-finance", None),
        common::view("other", 'f', t - 4, "xpa", "other-service", None),
    ];
    let git = GitContextQueryService::new(Arc::new(common::FakeGitReader { rows, error: None }));
    let app = app_with_git_dependencies(
        repository,
        Arc::new(NoopEventRepository),
        git,
        common::empty_git_sync_reader(),
    );
    let response = context_json(app, case.case_id(), StatusCode::OK).await;
    assert_eq!(
        response["history_sync"],
        json!({"status":"synced","pending_events":0})
    );
    let g = &response["git_context"];
    assert_eq!(g["status"], "synced");
    assert_eq!(g["pending_events"], 0);
    assert_eq!(g["count"], 3);
    assert_eq!(g["window"]["to"], json!(case.created_at()));
    assert_eq!(
        g["window"]["from"],
        json!(case.created_at() - Duration::hours(24))
    );
    assert_eq!(
        g["commits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["commit_sha"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["a".repeat(40), "b".repeat(40), "c".repeat(40)]
    );
    let v = &g["commits"][2];
    assert_eq!(v["repository_id"], "repo-b");
    assert_eq!(v["changed_file_count"], 1000);
    assert_eq!(v["changes_truncated"], true);
    assert_eq!(v["changed_files"].as_array().unwrap().len(), 1);
    assert!(v["additions"].is_null() && v["deletions"].is_null() && v["author_email"].is_null());
    assert!(
        v["changed_files"][0]["additions"].is_null()
            && v["changed_files"][0]["deletions"].is_null()
    );
    for forbidden in [
        "relevance_score",
        "probable_cause",
        "risky_commit",
        "root_cause",
        "payload_json",
        "attempt_count",
        "claimed_by",
    ] {
        assert!(g.get(forbidden).is_none() && v.get(forbidden).is_none());
    }
}

#[tokio::test]
async fn context_git_synced_empty_pending_and_support_sync_are_independent() {
    let repository = Arc::new(FakeSupportRepository::available());
    let case = SupportService::new(repository.clone())
        .create_case(valid_command())
        .await
        .unwrap();
    *repository.pending.lock().unwrap() = 2;
    for (pending, status) in [(0, "synced"), (3, "pending")] {
        let app = app_with_git_dependencies(
            repository.clone(),
            Arc::new(NoopEventRepository),
            common::empty_git_service(),
            Arc::new(common::FakeGitSyncReader {
                pending,
                error: None,
            }),
        );
        let response = context_json(app, case.case_id(), StatusCode::OK).await;
        assert_eq!(
            response["history_sync"],
            json!({"status":"pending","pending_events":2})
        );
        assert_eq!(response["git_context"]["status"], status);
        assert_eq!(response["git_context"]["pending_events"], pending);
        assert_eq!(response["git_context"]["count"], 0);
        assert_eq!(response["git_context"]["commits"], json!([]));
        assert!(response["git_context"]["window"].is_object());
    }
}

#[tokio::test]
async fn context_git_unmapped_never_calls_failing_git_dependencies() {
    for (project, service) in [
        (None, Some("xpa-finance")),
        (Some("xpa"), None),
        (None, None),
    ] {
        let repository = Arc::new(FakeSupportRepository::available());
        let mut command = valid_command();
        command.project_id = project.map(str::to_owned);
        command.service = service.map(str::to_owned);
        let case = SupportService::new(repository.clone())
            .create_case(command)
            .await
            .unwrap();
        let git = GitContextQueryService::new(Arc::new(common::FakeGitReader {
            rows: vec![],
            error: Some(GitReaderError::Unavailable),
        }));
        let app = app_with_git_dependencies(
            repository,
            Arc::new(NoopEventRepository),
            git,
            Arc::new(common::FakeGitSyncReader {
                pending: 99,
                error: Some(GitReaderError::Unavailable),
            }),
        );
        assert_eq!(
            context_json(app, case.case_id(), StatusCode::OK).await["git_context"],
            json!({"status":"unmapped","window":null,"pending_events":0,"count":0,"commits":[]})
        );
    }
}

#[tokio::test]
async fn context_git_errors_are_opaque_and_do_not_gate_plain_case_get() {
    let repository = Arc::new(FakeSupportRepository::available());
    let case = SupportService::new(repository.clone())
        .create_case(valid_command())
        .await
        .unwrap();
    for (error, status, code) in [
        (
            GitReaderError::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
        ),
        (
            GitReaderError::Timeout,
            StatusCode::GATEWAY_TIMEOUT,
            "request_timeout",
        ),
        (
            GitReaderError::Decode,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
        ),
        (
            GitReaderError::Internal,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
        ),
    ] {
        for sync_fails in [false, true] {
            let git = GitContextQueryService::new(Arc::new(common::FakeGitReader {
                rows: vec![],
                error: (!sync_fails).then_some(error),
            }));
            let app = app_with_git_dependencies(
                repository.clone(),
                Arc::new(NoopEventRepository),
                git,
                Arc::new(common::FakeGitSyncReader {
                    pending: 0,
                    error: sync_fails.then_some(error),
                }),
            );
            assert_eq!(
                context_json(app.clone(), case.case_id(), status).await["error"]["code"],
                code
            );
            let plain = app
                .oneshot(
                    Request::get(format!("/api/v1/support/cases/{}", case.case_id()))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(plain.status(), StatusCode::OK);
        }
    }
}

fn valid_command() -> CreateSupportCaseCommand {
    CreateSupportCaseCommand {
        case_type: CaseType::Incident,
        title: "xpa-finance unavailable".to_owned(),
        description: "Users cannot access xpa-finance production service.".to_owned(),
        reporter_id: "user-001".to_owned(),
        assignee_id: None,
        project_id: Some("xpa".to_owned()),
        service: Some("xpa-finance".to_owned()),
        environment: Some("prod".to_owned()),
        priority: Priority::High,
    }
}

async fn post_case(app: axum::Router, payload: Value) -> axum::response::Response {
    app.oneshot(
        Request::post("/api/v1/support/cases")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn patch_status(app: axum::Router, case_id: Uuid, status: &str) -> axum::response::Response {
    app.oneshot(
        Request::patch(format!("/api/v1/support/cases/{case_id}/status"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(json!({ "status": status }).to_string()))
            .unwrap(),
    )
    .await
    .unwrap()
}

fn request_id(response: &axum::response::Response) -> String {
    let value = response
        .headers()
        .get("x-request-id")
        .expect("every response has X-Request-ID")
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(Uuid::parse_str(&value).unwrap().get_version_num(), 7);
    value
}

async fn error_payload(response: axum::response::Response) -> Value {
    request_id(&response);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn get_outbox_status(app: axum::Router) -> axum::response::Response {
    app.oneshot(
        Request::get("/api/v1/internal/support/outbox/status")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn outbox_status_returns_only_operational_statistics() {
    let repository = Arc::new(FakeSupportRepository::available());
    *repository.pending.lock().unwrap() = 2;
    let response = get_outbox_status(app_with_repository(repository)).await;

    assert_eq!(response.status(), StatusCode::OK);
    request_id(&response);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload,
        json!({
            "pending_events": 2,
            "oldest_pending_age_seconds": 5,
            "exhausted_events": 0,
            "last_publish_success_at": null
        })
    );
    assert!(payload.get("payload_json").is_none());
    assert!(payload.get("description").is_none());
}

#[tokio::test]
async fn outbox_status_maps_repository_unavailable() {
    let response = get_outbox_status(app_with_failure(Failure::Unavailable)).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload = error_payload(response).await;
    assert_eq!(payload["error"]["code"], "service_unavailable");
}

#[tokio::test]
async fn post_returns_full_created_case() {
    let response = post_case(
        app_with_repository(Arc::new(FakeSupportRepository::available())),
        valid_request(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::CREATED);
    request_id(&response);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        Uuid::parse_str(payload["case_id"].as_str().unwrap())
            .unwrap()
            .get_version_num(),
        7
    );
    assert_eq!(payload["case_type"], "incident");
    assert_eq!(payload["priority"], "high");
    assert_eq!(payload["status"], "open");
    assert!(payload["created_at"].is_string());
    assert_eq!(payload["created_at"], payload["updated_at"]);
    assert_eq!(payload["resolved_at"], Value::Null);
}

#[tokio::test]
async fn invalid_http_enums_return_400() {
    for (field, value) in [("case_type", "problem"), ("priority", "urgent")] {
        let mut payload = valid_request();
        payload[field] = json!(value);
        let response = post_case(
            app_with_repository(Arc::new(FakeSupportRepository::available())),
            payload,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload = error_payload(response).await;
        assert_eq!(payload["error"]["code"], "invalid_request");
    }
}

#[tokio::test]
async fn invalid_domain_fields_return_400_before_persistence() {
    let cases = [
        ("title", json!("")),
        ("title", json!("x".repeat(257))),
        ("description", json!("x".repeat(64 * 1024 + 1))),
        ("reporter_id", json!("x".repeat(129))),
        ("assignee_id", json!("x".repeat(129))),
        ("project_id", json!("x".repeat(129))),
        ("service", json!("x".repeat(129))),
        ("environment", json!("x".repeat(65))),
    ];

    for (field, value) in cases {
        let mut payload = valid_request();
        payload[field] = value;
        let response = post_case(
            app_with_repository(Arc::new(FakeSupportRepository::available())),
            payload,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "field: {field}");
        let payload = error_payload(response).await;
        assert_eq!(payload["error"]["code"], "invalid_support_case");
    }
}

#[tokio::test]
async fn oversized_body_returns_413() {
    let mut payload = valid_request();
    payload["description"] = json!("x".repeat(256 * 1024));
    let response = post_case(
        app_with_repository(Arc::new(FakeSupportRepository::available())),
        payload,
    )
    .await;

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let payload = error_payload(response).await;
    assert_eq!(payload["error"]["code"], "payload_too_large");
}

#[tokio::test]
async fn client_generated_fields_are_rejected() {
    let mut payload = valid_request();
    payload["status"] = json!("open");
    let response = post_case(
        app_with_repository(Arc::new(FakeSupportRepository::available())),
        payload,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload = error_payload(response).await;
    assert_eq!(payload["error"]["code"], "invalid_request");
}

#[tokio::test]
async fn get_existing_returns_200() {
    let repository = Arc::new(FakeSupportRepository::available());
    let service = SupportService::new(repository.clone());
    let support_case = service.create_case(valid_command()).await.unwrap();
    let response = app_with_repository(repository)
        .oneshot(
            Request::get(format!("/api/v1/support/cases/{}", support_case.case_id()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    request_id(&response);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["case_id"], support_case.case_id().to_string());
    assert_eq!(payload["description"], support_case.description());
}

#[tokio::test]
async fn patch_status_returns_updated_case() {
    let repository = Arc::new(FakeSupportRepository::available());
    let service = SupportService::new(repository.clone());
    let support_case = service.create_case(valid_command()).await.unwrap();

    let response = patch_status(
        app_with_repository(repository),
        support_case.case_id(),
        "investigating",
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    request_id(&response);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["case_id"], support_case.case_id().to_string());
    assert_eq!(payload["status"], "investigating");
    assert_eq!(payload["resolved_at"], Value::Null);
}

#[tokio::test]
async fn patch_invalid_transition_returns_409() {
    let repository = Arc::new(FakeSupportRepository::available());
    let service = SupportService::new(repository.clone());
    let support_case = service.create_case(valid_command()).await.unwrap();

    let response = patch_status(
        app_with_repository(repository),
        support_case.case_id(),
        "resolved",
    )
    .await;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload = error_payload(response).await;
    assert_eq!(payload["error"]["code"], "invalid_support_case_transition");
}

#[tokio::test]
async fn patch_missing_case_returns_404() {
    let response = patch_status(
        app_with_repository(Arc::new(FakeSupportRepository::available())),
        Uuid::now_v7(),
        "investigating",
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload = error_payload(response).await;
    assert_eq!(payload["error"]["code"], "support_case_not_found");
}

#[tokio::test]
async fn oversized_patch_body_returns_413() {
    let response = app_with_repository(Arc::new(FakeSupportRepository::available()))
        .oneshot(
            Request::patch(format!("/api/v1/support/cases/{}/status", Uuid::now_v7()))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("x".repeat(256 * 1024 + 1)))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let payload = error_payload(response).await;
    assert_eq!(payload["error"]["code"], "payload_too_large");
}

#[tokio::test]
async fn get_missing_returns_404() {
    let response = app_with_repository(Arc::new(FakeSupportRepository::available()))
        .oneshot(
            Request::get(format!("/api/v1/support/cases/{}", Uuid::now_v7()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload = error_payload(response).await;
    assert_eq!(payload["error"]["code"], "support_case_not_found");
}

#[tokio::test]
async fn invalid_uuid_returns_400() {
    let response = app_with_repository(Arc::new(FakeSupportRepository::available()))
        .oneshot(
            Request::get("/api/v1/support/cases/not-a-uuid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload = error_payload(response).await;
    assert_eq!(payload["error"]["code"], "invalid_request");
}

#[tokio::test]
async fn repository_errors_have_stable_http_mapping() {
    for (failure, status, code) in [
        (
            Failure::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
        ),
        (
            Failure::Timeout,
            StatusCode::GATEWAY_TIMEOUT,
            "request_timeout",
        ),
        (
            Failure::Decode,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
        ),
        (
            Failure::Internal,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
        ),
    ] {
        let get_response = app_with_failure(failure)
            .oneshot(
                Request::get(format!("/api/v1/support/cases/{}", Uuid::now_v7()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), status);
        let payload = error_payload(get_response).await;
        assert_eq!(payload["error"]["code"], code);

        if matches!(failure, Failure::Unavailable | Failure::Timeout) {
            let post_response = post_case(app_with_failure(failure), valid_request()).await;
            assert_eq!(post_response.status(), status);
            let payload = error_payload(post_response).await;
            assert_eq!(payload["error"]["code"], code);
        }
    }
}

#[tokio::test]
async fn get_case_list_filters_and_returns_count() {
    let repository = Arc::new(FakeSupportRepository::available());
    let service = SupportService::new(repository.clone());
    service.create_case(valid_command()).await.unwrap();
    let mut request = valid_command();
    request.case_type = CaseType::Request;
    request.priority = Priority::Low;
    request.reporter_id = "user-002".to_owned();
    request.project_id = Some("other".to_owned());
    service.create_case(request).await.unwrap();

    let response = app_with_repository(repository)
        .oneshot(
            Request::get(
                "/api/v1/support/cases?case_type=incident&status=open&priority=high&project_id=xpa&service=xpa-finance&environment=prod",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    request_id(&response);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["count"], 1);
    assert_eq!(payload["items"][0]["case_type"], "incident");
    assert_eq!(payload["items"][0]["project_id"], "xpa");
}

#[tokio::test]
async fn invalid_case_list_query_returns_400() {
    for query in [
        "case_type=problem",
        "status=unknown",
        "priority=urgent",
        "limit=501",
        "created_from=2026-10-06T11%3A00%3A00Z&created_to=2026-10-06T10%3A00%3A00Z",
    ] {
        let response = app_with_repository(Arc::new(FakeSupportRepository::available()))
            .oneshot(
                Request::get(format!("/api/v1/support/cases?{query}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "query: {query}");
        let payload = error_payload(response).await;
        assert_eq!(payload["error"]["code"], "invalid_support_case_query");
    }
}

#[tokio::test]
async fn context_returns_timeline_oldest_first_and_sync_state() {
    let repository = Arc::new(FakeSupportRepository::available());
    let service = SupportService::new(repository.clone());
    let support_case = service.create_case(valid_command()).await.unwrap();
    let case_id = support_case.case_id();
    let event_repository = Arc::new(FakeContextEventRepository {
        events: Mutex::new(vec![
            context_event(case_id, 3, "closed"),
            context_event(case_id, 0, "created"),
        ]),
        unavailable: false,
    });

    let response = app_with_context_event_repository(repository.clone(), event_repository)
        .oneshot(
            Request::get(format!("/api/v1/support/cases/{case_id}/context"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["case"]["case_id"], case_id.to_string());
    assert_eq!(
        payload["timeline"][0]["event_type"],
        "support.incident.created"
    );
    assert_eq!(
        payload["timeline"][1]["event_type"],
        "support.incident.closed"
    );
    assert_eq!(payload["history_sync"]["status"], "synced");
    assert_eq!(payload["history_sync"]["pending_events"], 0);

    *repository.pending.lock().unwrap() = 2;
    let pending_response = app_with_repository(repository)
        .oneshot(
            Request::get(format!("/api/v1/support/cases/{case_id}/context"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pending_response.status(), StatusCode::OK);
    let body = to_bytes(pending_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["history_sync"]["status"], "pending");
    assert_eq!(payload["history_sync"]["pending_events"], 2);
}

#[tokio::test]
async fn context_missing_and_dependency_failures_have_stable_statuses() {
    let missing_id = Uuid::now_v7();
    let missing = app_with_repository(Arc::new(FakeSupportRepository::available()))
        .oneshot(
            Request::get(format!("/api/v1/support/cases/{missing_id}/context"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    let postgres_down = app_with_repository(Arc::new(FakeSupportRepository::failing(
        Failure::Unavailable,
    )))
    .oneshot(
        Request::get(format!("/api/v1/support/cases/{missing_id}/context"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(postgres_down.status(), StatusCode::SERVICE_UNAVAILABLE);

    let repository = Arc::new(FakeSupportRepository::available());
    let support_case = SupportService::new(repository.clone())
        .create_case(valid_command())
        .await
        .unwrap();
    let clickhouse_down = app_with_context_event_repository(
        repository,
        Arc::new(FakeContextEventRepository {
            events: Mutex::new(Vec::new()),
            unavailable: true,
        }),
    )
    .oneshot(
        Request::get(format!(
            "/api/v1/support/cases/{}/context",
            support_case.case_id()
        ))
        .body(Body::empty())
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(clickhouse_down.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn optimistic_conflict_returns_409() {
    let repository = Arc::new(FakeSupportRepository::conflicting_update());
    let support_case = SupportService::new(repository.clone())
        .create_case(valid_command())
        .await
        .unwrap();
    let response = patch_status(
        app_with_repository(repository),
        support_case.case_id(),
        "investigating",
    )
    .await;

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let payload = error_payload(response).await;
    assert_eq!(payload["error"]["code"], "support_case_conflict");
}

fn context_event(case_id: Uuid, offset_seconds: i64, action: &str) -> ContextEvent {
    let time =
        Utc.with_ymd_and_hms(2026, 10, 6, 10, 0, 0).unwrap() + Duration::seconds(offset_seconds);
    ContextEvent::from_stored(StoredContextEvent {
        event_id: Uuid::now_v7(),
        event_time: time,
        ingest_time: time,
        source: "support".to_owned(),
        event_type: format!("support.incident.{action}"),
        project_id: "xpa".to_owned(),
        service: "xpa-finance".to_owned(),
        environment: "prod".to_owned(),
        subject_type: "support_case".to_owned(),
        subject_id: case_id.to_string(),
        title: "xpa unavailable".to_owned(),
        content: "lifecycle".to_owned(),
        trace_id: None,
        correlation_id: Some(case_id.to_string()),
        metadata: json!({}),
    })
    .unwrap()
}
