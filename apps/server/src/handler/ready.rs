use anisp_common::ReadinessResponse;
use axum::{Json, extract::State, http::StatusCode};
use tracing::warn;

use crate::AppState;

pub(crate) async fn ready(State(state): State<AppState>) -> (StatusCode, Json<ReadinessResponse>) {
    let (clickhouse, postgres) = tokio::join!(state.clickhouse().ping(), state.postgres().ping());

    let clickhouse_ok = match clickhouse {
        Ok(()) => true,
        Err(error) => {
            warn!(error = %error, dependency = "clickhouse", "readiness check failed");
            false
        }
    };
    let postgres_ok = match postgres {
        Ok(()) => true,
        Err(error) => {
            warn!(error = %error, dependency = "postgres", "readiness check failed");
            false
        }
    };

    let status = if clickhouse_ok && postgres_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status,
        Json(ReadinessResponse::from_availability(
            clickhouse_ok,
            postgres_ok,
        )),
    )
}
