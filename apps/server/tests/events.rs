mod common;

use std::{
    future,
    sync::{Arc, Mutex},
};

use anisp_clickhouse_store::{ClickHouseReadiness, PingFuture};
use anisp_config::AppConfig;
use anisp_context::{SupportCaseContextService, SupportContextSyncReader};
use anisp_event::{
    ContextEvent, CreateEventRequest, EventQuery, EventRepository, EventService, RepositoryError,
};
use anisp_postgres_store::{PostgresPingFuture, PostgresReadiness};
use anisp_server::{AppState, build_app};
use anisp_support::{
    SupportCase, SupportCaseLifecycleEvent, SupportCaseQuery, SupportCaseRepository,
    SupportOutboxOperationalReader, SupportOutboxOperationalStatus, SupportRepositoryError,
    SupportService,
};
use anisp_support_outbox::SupportOutboxStatusService;
use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

struct StubClickHouse;

struct StubPostgres;

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

struct NoopSupportRepository;

#[async_trait]
impl SupportCaseRepository for NoopSupportRepository {
    async fn insert(
        &self,
        _support_case: &SupportCase,
        _lifecycle_event: &SupportCaseLifecycleEvent,
    ) -> Result<(), SupportRepositoryError> {
        Ok(())
    }

    async fn update(
        &self,
        _support_case: &SupportCase,
        _lifecycle_event: &SupportCaseLifecycleEvent,
    ) -> Result<(), SupportRepositoryError> {
        Ok(())
    }

    async fn find_by_id(
        &self,
        _case_id: Uuid,
    ) -> Result<Option<SupportCase>, SupportRepositoryError> {
        Ok(None)
    }

    async fn search(
        &self,
        _query: &SupportCaseQuery,
    ) -> Result<Vec<SupportCase>, SupportRepositoryError> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl SupportContextSyncReader for NoopSupportRepository {
    async fn pending_count_for_case(&self, _case_id: Uuid) -> Result<u64, SupportRepositoryError> {
        Ok(0)
    }
}

#[async_trait]
impl SupportOutboxOperationalReader for NoopSupportRepository {
    async fn operational_status(
        &self,
        _now: chrono::DateTime<chrono::Utc>,
    ) -> Result<SupportOutboxOperationalStatus, SupportRepositoryError> {
        Ok(SupportOutboxOperationalStatus::new(0, None, 0, None))
    }
}

struct FakeEventRepository {
    failure: Option<RepositoryFailure>,
    events: Mutex<Vec<ContextEvent>>,
}

#[derive(Clone, Copy)]
enum RepositoryFailure {
    Unavailable,
    Timeout,
    Decode,
    Internal,
}

impl RepositoryFailure {
    fn error(self) -> RepositoryError {
        match self {
            Self::Unavailable => RepositoryError::unavailable(),
            Self::Timeout => RepositoryError::timeout(),
            Self::Decode => RepositoryError::decode_with_source(std::io::Error::other("bad row")),
            Self::Internal => RepositoryError::internal(),
        }
    }
}

#[async_trait]
impl EventRepository for FakeEventRepository {
    async fn insert(&self, event: &ContextEvent) -> Result<(), RepositoryError> {
        if let Some(failure) = self.failure {
            Err(failure.error())
        } else {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    async fn find_by_id(&self, event_id: Uuid) -> Result<Option<ContextEvent>, RepositoryError> {
        if let Some(failure) = self.failure {
            return Err(failure.error());
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
        if let Some(failure) = self.failure {
            return Err(failure.error());
        }
        let mut events = self.events.lock().unwrap().clone();
        events.retain(|event| {
            query
                .filters()
                .project_id
                .as_deref()
                .is_none_or(|project| event.project_id() == project)
        });
        events.sort_by(|left, right| {
            right
                .event_time()
                .cmp(&left.event_time())
                .then_with(|| right.event_id().cmp(&left.event_id()))
        });
        events.truncate(usize::from(query.limit()));
        Ok(events)
    }
}

fn app(repository_fails: bool) -> axum::Router {
    app_with_failure(repository_fails.then_some(RepositoryFailure::Unavailable))
}

fn app_with_failure(failure: Option<RepositoryFailure>) -> axum::Router {
    app_with_repository(Arc::new(FakeEventRepository {
        failure,
        events: Mutex::default(),
    }))
}

fn app_with_repository(repository: Arc<FakeEventRepository>) -> axum::Router {
    let event_service = EventService::new(repository);
    let support_repository = Arc::new(NoopSupportRepository);
    let support_service = SupportService::new(support_repository.clone());
    let context_service = SupportCaseContextService::new(
        support_service.clone(),
        event_service.clone(),
        support_repository.clone(),
        common::empty_git_service(),
        common::empty_git_sync_reader(),
        Default::default(),
    );
    let outbox_status_service = SupportOutboxStatusService::new(support_repository);
    let state = AppState::with_dependencies(
        AppConfig::default(),
        StubClickHouse,
        StubPostgres,
        event_service,
        support_service,
        context_service,
        outbox_status_service,
        common::empty_git_service(),
        common::empty_git_status_service(),
    );
    build_app(state)
}

async fn seeded_app() -> (axum::Router, Uuid) {
    let repository = Arc::new(FakeEventRepository {
        failure: None,
        events: Mutex::default(),
    });
    let service = EventService::new(repository.clone());
    let request: CreateEventRequest = serde_json::from_value(valid_request()).unwrap();
    let event = service.create_event(request).await.unwrap();
    (app_with_repository(repository), event.event_id())
}

fn valid_request() -> Value {
    json!({
        "event_time": "2026-09-21T10:31:22.123Z",
        "source": "kubernetes",
        "event_type": "k8s.pod.restart",
        "project_id": "xpa",
        "service": "xpa-finance",
        "environment": "prod",
        "subject_type": "pod",
        "subject_id": "xpa-finance-675894c5fc-ddgzh",
        "title": "Container restarted",
        "content": "Container restart detected.",
        "metadata": {
            "namespace": "xpa",
            "restart_count": 5
        }
    })
}

async fn post_event(app: axum::Router, payload: Value) -> axum::response::Response {
    app.oneshot(
        Request::post("/api/v1/events")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
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

#[tokio::test]
async fn post_event_returns_created() {
    let response = post_event(app(false), valid_request()).await;

    assert_eq!(response.status(), StatusCode::CREATED);
    request_id(&response);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    let event_id = Uuid::parse_str(payload["event_id"].as_str().unwrap()).unwrap();

    assert_eq!(event_id.get_version_num(), 7);
    assert_eq!(payload["status"], "created");
}

#[tokio::test]
async fn invalid_post_returns_bad_request() {
    let mut payload = valid_request();
    payload["event_type"] = json!("restart");

    let response = post_event(app(false), payload).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let id = request_id(&response);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"]["code"], "invalid_event");
    assert_eq!(payload["error"]["request_id"], id);
}

#[tokio::test]
async fn unavailable_clickhouse_repository_returns_service_unavailable() {
    let response = post_event(app(true), valid_request()).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let id = request_id(&response);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"]["code"], "service_unavailable");
    assert_eq!(payload["error"]["request_id"], id);
    assert_eq!(
        payload["error"]["message"],
        "event repository is unavailable"
    );
}

#[tokio::test]
async fn get_detail_returns_full_event() {
    let (app, event_id) = seeded_app().await;
    let response = app
        .oneshot(
            Request::get(format!("/api/v1/events/{event_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    request_id(&response);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let event: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(event["event_id"], event_id.to_string());
    assert_eq!(event["event_type"], "k8s.pod.restart");
    assert_eq!(event["metadata"]["restart_count"], 5);
    assert!(event["ingest_time"].is_string());
    assert!(event.get("metadata_json").is_none());
}

#[tokio::test]
async fn get_missing_event_returns_404() {
    let response = app(false)
        .oneshot(
            Request::get(format!("/api/v1/events/{}", Uuid::now_v7()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let id = request_id(&response);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"]["code"], "event_not_found");
    assert_eq!(payload["error"]["request_id"], id);
}

#[tokio::test]
async fn invalid_uuid_returns_400() {
    let response = app(false)
        .oneshot(
            Request::get("/api/v1/events/not-a-uuid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    request_id(&response);
}

#[tokio::test]
async fn get_list_returns_items_and_count() {
    let (app, event_id) = seeded_app().await;
    let response = app
        .oneshot(
            Request::get("/api/v1/events?project_id=xpa&limit=1")
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
    assert_eq!(payload["items"][0]["event_id"], event_id.to_string());
}

#[tokio::test]
async fn invalid_list_query_returns_400() {
    for uri in [
        "/api/v1/events?limit=501",
        "/api/v1/events?from=2026-09-22T00%3A00%3A00Z&to=2026-09-21T00%3A00%3A00Z",
    ] {
        let response = app(false)
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let id = request_id(&response);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["error"]["code"], "invalid_event_query");
        assert_eq!(payload["error"]["request_id"], id);
    }
}

#[tokio::test]
async fn unavailable_repository_makes_get_return_503() {
    for uri in [
        format!("/api/v1/events/{}", Uuid::now_v7()),
        "/api/v1/events".to_owned(),
    ] {
        let response = app(true)
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        request_id(&response);
    }
}

#[tokio::test]
async fn oversized_body_returns_413() {
    let mut payload = valid_request();
    payload["content"] = json!("x".repeat(256 * 1024));
    let response = post_event(app(false), payload).await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let id = request_id(&response);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"]["code"], "payload_too_large");
    assert_eq!(payload["error"]["request_id"], id);
}

#[tokio::test]
async fn oversized_event_fields_return_400() {
    for (field, value) in [
        ("title", json!("x".repeat(257))),
        ("content", json!("x".repeat(64 * 1024 + 1))),
        ("metadata", json!({"blob": "x".repeat(128 * 1024)})),
    ] {
        let mut payload = valid_request();
        payload[field] = value;
        let response = post_event(app(false), payload).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "field: {field}");
        let id = request_id(&response);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["error"]["code"], "invalid_event");
        assert_eq!(payload["error"]["request_id"], id);
    }
}

#[tokio::test]
async fn repository_error_kinds_have_stable_http_mapping() {
    for (failure, status, code) in [
        (
            RepositoryFailure::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
        ),
        (
            RepositoryFailure::Timeout,
            StatusCode::GATEWAY_TIMEOUT,
            "request_timeout",
        ),
        (
            RepositoryFailure::Decode,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
        ),
        (
            RepositoryFailure::Internal,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
        ),
    ] {
        for request in [
            Request::get("/api/v1/events").body(Body::empty()).unwrap(),
            Request::get(format!("/api/v1/events/{}", Uuid::now_v7()))
                .body(Body::empty())
                .unwrap(),
        ] {
            let response = app_with_failure(Some(failure))
                .oneshot(request)
                .await
                .unwrap();
            assert_eq!(response.status(), status);
            let id = request_id(&response);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let payload: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(payload["error"]["code"], code);
            assert_eq!(payload["error"]["request_id"], id);
            assert!(
                !payload["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("ClickHouse")
            );
        }
    }
}
