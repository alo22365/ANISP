use anisp_common::HealthResponse;
use axum::{Json, extract::State};

use crate::{AppResult, AppState};

pub(crate) async fn health(State(_state): State<AppState>) -> AppResult<Json<HealthResponse>> {
    Ok(Json(HealthResponse::ok()))
}
