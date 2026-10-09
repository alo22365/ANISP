use axum::{Router, extract::DefaultBodyLimit, routing::get};

use crate::{AppError, AppState, handler};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(handler::health))
        .route("/ready", get(handler::ready))
        .route(
            "/api/v1/events",
            axum::routing::post(handler::create_event)
                .layer(DefaultBodyLimit::max(256 * 1024))
                .get(handler::search_events),
        )
        .route("/api/v1/events/{event_id}", get(handler::get_event))
        .route("/api/v1/git/commits", get(handler::search_git_commits))
        .route(
            "/api/v1/git/commits/{repository_id}/{commit_sha}",
            get(handler::get_git_commit),
        )
        .route(
            "/api/v1/support/cases",
            axum::routing::post(handler::create_support_case)
                .layer(DefaultBodyLimit::max(256 * 1024))
                .get(handler::search_support_cases),
        )
        .route(
            "/api/v1/support/cases/{case_id}",
            get(handler::get_support_case),
        )
        .route(
            "/api/v1/support/cases/{case_id}/status",
            axum::routing::patch(handler::update_support_case_status)
                .layer(DefaultBodyLimit::max(256 * 1024)),
        )
        .route(
            "/api/v1/support/cases/{case_id}/context",
            get(handler::get_support_case_context),
        )
        .route(
            "/api/v1/internal/support/outbox/status",
            get(handler::get_support_outbox_status),
        )
        .route(
            "/api/v1/internal/git/outbox/status",
            get(handler::get_git_outbox_status),
        )
        .fallback(not_found)
}

async fn not_found() -> AppError {
    AppError::RouteNotFound
}
