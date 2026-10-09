mod common;

use anisp_git_ingest::{GitOutboxError, GitOutboxOperationalStatus, GitOutboxStatusService};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tower::ServiceExt;

fn app(status: GitOutboxOperationalStatus, error: Option<GitOutboxError>) -> axum::Router {
    common::app_with_git_operations(
        common::empty_git_service(),
        GitOutboxStatusService::new(
            Arc::new(common::FakeGitOperationalReader { status, error }),
            Duration::from_secs(30),
        )
        .unwrap(),
    )
}
async fn get(app: axum::Router, path: &str, expected: StatusCode) -> Value {
    let response = app
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), expected);
    let id = response
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    if body.get("error").is_some() {
        assert_eq!(body["error"]["request_id"], id);
    }
    body
}
#[tokio::test]
async fn git_status_exposes_only_aggregate_fields_and_backlog_does_not_gate_readiness() {
    let published = "2026-10-08T01:00:00Z".parse().unwrap();
    let app = app(
        GitOutboxOperationalStatus {
            pending_events: 5,
            exhausted_events: 2,
            active_claims: 1,
            oldest_pending_age_seconds: Some(120),
            last_publish_success_at: Some(published),
        },
        None,
    );
    assert_eq!(
        get(
            app.clone(),
            "/api/v1/internal/git/outbox/status",
            StatusCode::OK
        )
        .await,
        json!({"pending_events":5,"exhausted_events":2,"active_claims":1,"oldest_pending_age_seconds":120,"last_publish_success_at":published})
    );
    get(app.clone(), "/ready", StatusCode::OK).await;
    get(app, "/health", StatusCode::OK).await;
}
#[tokio::test]
async fn git_status_empty_age_and_last_success_are_null() {
    assert_eq!(
        get(
            app(Default::default(), None),
            "/api/v1/internal/git/outbox/status",
            StatusCode::OK
        )
        .await,
        json!({"pending_events":0,"exhausted_events":0,"active_claims":0,"oldest_pending_age_seconds":null,"last_publish_success_at":null})
    );
}
#[tokio::test]
async fn git_status_errors_are_opaque_and_do_not_become_readiness_dependencies() {
    for (error, status, code) in [
        (
            GitOutboxError::Unavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
        ),
        (
            GitOutboxError::Timeout,
            StatusCode::GATEWAY_TIMEOUT,
            "request_timeout",
        ),
        (
            GitOutboxError::Decode,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
        ),
        (
            GitOutboxError::Internal,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
        ),
        (
            GitOutboxError::Conflict,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
        ),
    ] {
        let app = app(Default::default(), Some(error));
        let body = get(app.clone(), "/api/v1/internal/git/outbox/status", status).await;
        assert_eq!(body["error"]["code"], code);
        assert_eq!(body["error"].as_object().unwrap().len(), 3);
        get(app.clone(), "/ready", StatusCode::OK).await;
        get(app, "/health", StatusCode::OK).await;
    }
}
