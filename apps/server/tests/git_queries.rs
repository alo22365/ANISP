mod common;
use anisp_git_context::{GitContextQueryService, GitReaderError};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use common::{FakeGitReader, view};
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

fn app(error: Option<GitReaderError>) -> axum::Router {
    common::app(GitContextQueryService::new(Arc::new(FakeGitReader {
        error,
        rows: vec![
            view("repo-a", 'a', 1, "xpa", "finance", Some("main")),
            view("repo-a", 'b', 2, "xpa", "finance", Some("feature")),
            view("repo-b", 'a', 3, "other", "other", None),
        ],
    })))
}
async fn get(app: axum::Router, uri: &str, expected: StatusCode) -> Value {
    let response = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), expected, "{uri}");
    let id = response
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(uuid::Uuid::parse_str(&id).unwrap().get_version_num(), 7);
    let value: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    if expected.is_client_error() || expected.is_server_error() {
        assert_eq!(value["error"]["request_id"], id);
    }
    value
}
#[tokio::test]
async fn empty_list() {
    assert_eq!(
        get(
            common::app(common::empty_git_service()),
            "/api/v1/git/commits",
            StatusCode::OK
        )
        .await,
        serde_json::json!({"items":[],"count":0})
    );
}
#[tokio::test]
async fn list_filters_order_time_range_and_limit() {
    let all = get(app(None), "/api/v1/git/commits", StatusCode::OK).await;
    assert_eq!(all["count"], 3);
    assert_eq!(all["items"][0]["repository_id"], "repo-b");
    assert_eq!(all["items"][1]["commit_sha"], "b".repeat(40));
    for (query, count) in [
        ("repository_id=repo-a", 2),
        ("project_id=xpa", 2),
        ("service=finance", 2),
        ("branch=main", 1),
        ("branch=absent", 0),
        ("from=1970-01-01T00:00:02Z&to=1970-01-01T00:00:03Z", 1),
        ("limit=1", 1),
        ("limit=500", 3),
        ("from=1970-01-01T00:00:02Z&to=1970-01-01T00:00:02Z", 0),
    ] {
        let response = get(
            app(None),
            &format!("/api/v1/git/commits?{query}"),
            StatusCode::OK,
        )
        .await;
        assert_eq!(response["count"], count, "{query}");
    }
}
#[tokio::test]
async fn invalid_queries_return_400() {
    for query in [
        "limit=0",
        "limit=501",
        "limit=-1",
        "limit=abc",
        "from=bad",
        "from=1970-01-01T00:00:03Z&to=1970-01-01T00:00:02Z",
        "repository_id=",
        "branch=",
        "author=not-supported",
    ] {
        let value = get(
            app(None),
            &format!("/api/v1/git/commits?{query}"),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_eq!(value["error"]["code"], "invalid_git_commit_query");
    }
}
#[tokio::test]
async fn detail_compound_identity_uppercase_and_metadata() {
    for repo in ["repo-a", "repo-b"] {
        let value = get(
            app(None),
            &format!("/api/v1/git/commits/{repo}/{}", "A".repeat(40)),
            StatusCode::OK,
        )
        .await;
        assert_eq!(value["repository_id"], repo);
        assert_eq!(value["commit_sha"], "a".repeat(40));
        assert_eq!(value["parent_shas"][0], "d".repeat(40));
        assert_eq!(value["author_name"], "Author");
        assert_eq!(value["author_email"], "author@example.com");
        assert!(value.get("metadata_json").is_none());
        assert!(value.get("metadata").is_none());
    }
    assert_eq!(
        get(
            app(None),
            &format!("/api/v1/git/commits/missing/{}", "a".repeat(40)),
            StatusCode::NOT_FOUND
        )
        .await["error"]["code"],
        "git_commit_not_found"
    );
    get(
        app(None),
        "/api/v1/git/commits/repo-a/not-a-sha",
        StatusCode::BAD_REQUEST,
    )
    .await;
}
#[tokio::test]
async fn opaque_reader_errors_for_list_and_detail() {
    for (e, status, code) in [
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
        for uri in [
            "/api/v1/git/commits".to_owned(),
            format!("/api/v1/git/commits/repo-a/{}", "a".repeat(40)),
        ] {
            assert_eq!(get(app(Some(e)), &uri, status).await["error"]["code"], code);
        }
    }
}
