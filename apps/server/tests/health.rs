mod common;

use std::{future, sync::Arc};

use anisp_clickhouse_store::{ClickHouseReadiness, PingFuture, StoreError as ClickHouseStoreError};
use anisp_config::AppConfig;
use anisp_context::{SupportCaseContextService, SupportContextSyncReader};
use anisp_event::{ContextEvent, EventQuery, EventRepository, EventService, RepositoryError};
use anisp_postgres_store::{
    PostgresPingFuture, PostgresReadiness, StoreError as PostgresStoreError,
};
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

#[derive(Clone, Copy)]
struct StubClickHouse {
    available: bool,
    timed_out: bool,
}

#[derive(Clone, Copy)]
struct StubPostgres {
    available: bool,
    timed_out: bool,
}

struct NoopEventRepository;
struct NoopSupportRepository;

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

impl ClickHouseReadiness for StubClickHouse {
    fn ping(&self) -> PingFuture<'_> {
        let result = if self.timed_out {
            Err(ClickHouseStoreError::timeout())
        } else if self.available {
            Ok(())
        } else {
            Err(ClickHouseStoreError::unavailable())
        };
        Box::pin(future::ready(result))
    }
}

impl PostgresReadiness for StubPostgres {
    fn ping(&self) -> PostgresPingFuture<'_> {
        let result = if self.timed_out {
            Err(PostgresStoreError::timeout())
        } else if self.available {
            Ok(())
        } else {
            Err(PostgresStoreError::unavailable())
        };
        Box::pin(future::ready(result))
    }
}

fn app(clickhouse_available: bool, postgres_available: bool) -> axum::Router {
    app_with_readiness(clickhouse_available, false, postgres_available, false)
}

fn app_with_readiness(
    clickhouse_available: bool,
    clickhouse_timed_out: bool,
    postgres_available: bool,
    postgres_timed_out: bool,
) -> axum::Router {
    let event_service = EventService::new(Arc::new(NoopEventRepository));
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
        StubClickHouse {
            available: clickhouse_available,
            timed_out: clickhouse_timed_out,
        },
        StubPostgres {
            available: postgres_available,
            timed_out: postgres_timed_out,
        },
        event_service,
        support_service,
        context_service,
        outbox_status_service,
        common::empty_git_service(),
        common::empty_git_status_service(),
    );
    build_app(state)
}

fn request_id(response: &axum::response::Response) -> String {
    let value = response
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(Uuid::parse_str(value).unwrap().get_version_num(), 7);
    value.to_owned()
}

async fn readiness_payload(app: axum::Router) -> (StatusCode, Value) {
    let response = app
        .oneshot(Request::get("/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    request_id(&response);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn health_returns_ok_even_when_both_dependencies_fail() {
    let response = app(false, false)
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    request_id(&response);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload, json!({ "status": "ok" }));
}

#[tokio::test]
async fn readiness_reports_both_dependencies() {
    for (clickhouse, postgres, expected_status) in [
        (true, true, StatusCode::OK),
        (false, true, StatusCode::SERVICE_UNAVAILABLE),
        (true, false, StatusCode::SERVICE_UNAVAILABLE),
        (false, false, StatusCode::SERVICE_UNAVAILABLE),
    ] {
        let (status, payload) = readiness_payload(app(clickhouse, postgres)).await;
        assert_eq!(status, expected_status);
        assert_eq!(
            payload,
            json!({
                "status": if clickhouse && postgres { "ready" } else { "not_ready" },
                "dependencies": {
                    "clickhouse": if clickhouse { "ok" } else { "unavailable" },
                    "postgres": if postgres { "ok" } else { "unavailable" }
                }
            })
        );
    }
}

#[tokio::test]
async fn readiness_timeout_is_503_and_identifies_dependency() {
    for (ch_timeout, pg_timeout, expected_ch, expected_pg) in [
        (true, false, "unavailable", "ok"),
        (false, true, "ok", "unavailable"),
    ] {
        let (status, payload) = readiness_payload(app_with_readiness(
            !ch_timeout,
            ch_timeout,
            !pg_timeout,
            pg_timeout,
        ))
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(payload["dependencies"]["clickhouse"], expected_ch);
        assert_eq!(payload["dependencies"]["postgres"], expected_pg);
    }
}

#[tokio::test]
async fn unknown_route_uses_json_error_response() {
    let response = app(true, true)
        .oneshot(Request::get("/missing").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let id = request_id(&response);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload,
        json!({
            "error": {
                "code": "not_found",
                "message": "resource not found",
                "request_id": id
            }
        })
    );
}

#[tokio::test]
async fn request_id_is_generated_per_request_and_ignores_client_header() {
    let app = app(true, true);
    let first = app
        .clone()
        .oneshot(
            Request::get("/health")
                .header("x-request-id", "client-controlled")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let second = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let first_id = request_id(&first);
    let second_id = request_id(&second);
    assert_ne!(first_id, "client-controlled");
    assert_ne!(first_id, second_id);
}
