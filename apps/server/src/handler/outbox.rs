use std::time::Instant;

use axum::{Json, extract::State};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tracing::info;

use crate::{AppError, AppResult, AppState};

#[derive(Debug, Serialize)]
pub(crate) struct SupportOutboxStatusResponse {
    pending_events: u64,
    oldest_pending_age_seconds: Option<u64>,
    exhausted_events: u64,
    last_publish_success_at: Option<DateTime<Utc>>,
}

pub(crate) async fn get_support_outbox_status(
    State(state): State<AppState>,
) -> AppResult<Json<SupportOutboxStatusResponse>> {
    let started = Instant::now();
    let status = state
        .support_outbox_status_service()
        .status()
        .await
        .map_err(AppError::from)?;
    let response = SupportOutboxStatusResponse {
        pending_events: status.pending_events(),
        oldest_pending_age_seconds: status.oldest_pending_age_seconds(),
        exhausted_events: status.exhausted_events(),
        last_publish_success_at: status.last_publish_success_at(),
    };

    info!(
        operation = "support.outbox.status",
        result = "success",
        pending_events = response.pending_events,
        exhausted_events = response.exhausted_events,
        duration_ms = started.elapsed().as_millis() as u64,
        "support outbox operational status fetched"
    );

    Ok(Json(response))
}

#[derive(Debug, Serialize)]
pub(crate) struct GitOutboxStatusResponse {
    pending_events: u64,
    exhausted_events: u64,
    active_claims: u64,
    oldest_pending_age_seconds: Option<u64>,
    last_publish_success_at: Option<DateTime<Utc>>,
}

pub(crate) async fn get_git_outbox_status(
    State(state): State<AppState>,
) -> AppResult<Json<GitOutboxStatusResponse>> {
    let started = Instant::now();
    let status = state
        .git_outbox_status_service()
        .status()
        .await
        .map_err(AppError::from)?;
    let response = GitOutboxStatusResponse {
        pending_events: status.pending_events,
        exhausted_events: status.exhausted_events,
        active_claims: status.active_claims,
        oldest_pending_age_seconds: status.oldest_pending_age_seconds,
        last_publish_success_at: status.last_publish_success_at,
    };
    info!(
        operation = "git.outbox.status",
        result = "success",
        pending_events = response.pending_events,
        exhausted_events = response.exhausted_events,
        active_claims = response.active_claims,
        duration_ms = started.elapsed().as_millis() as u64,
        "Git outbox operational status fetched"
    );
    Ok(Json(response))
}
