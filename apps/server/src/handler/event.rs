use anisp_event::{
    CreateEventRequest, CreateEventResponse, EventQuery, EventResponse, SearchEventsRequest,
    SearchEventsResponse,
};
use axum::{
    Json,
    extract::{
        Path, Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::StatusCode,
};
use tracing::info;
use uuid::Uuid;

use crate::{AppError, AppResult, AppState};

pub(crate) async fn create_event(
    State(state): State<AppState>,
    payload: Result<Json<CreateEventRequest>, JsonRejection>,
) -> AppResult<(StatusCode, Json<CreateEventResponse>)> {
    let Json(request) = payload.map_err(|error| {
        if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
            AppError::PayloadTooLarge
        } else {
            AppError::InvalidRequest
        }
    })?;
    let event = state
        .event_service()
        .create_event(request)
        .await
        .map_err(AppError::from)?;

    info!(
        operation = "create_event",
        result = "success",
        event_id = %event.event_id(),
        event_type = %event.event_type(),
        project_id = %event.project_id(),
        service = %event.service(),
        "event created"
    );

    Ok((
        StatusCode::CREATED,
        Json(CreateEventResponse::created(event.event_id())),
    ))
}

pub(crate) async fn get_event(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> AppResult<Json<EventResponse>> {
    let event_id = Uuid::parse_str(&event_id).map_err(|_| AppError::InvalidEventId)?;
    let event = state
        .event_service()
        .get_event(event_id)
        .await
        .map_err(AppError::from)?
        .ok_or(AppError::EventNotFound)?;

    info!(operation = "get_event", result = "success", event_id = %event.event_id(), "event fetched");

    Ok(Json(EventResponse::from(event)))
}

pub(crate) async fn search_events(
    State(state): State<AppState>,
    query: Result<Query<SearchEventsRequest>, QueryRejection>,
) -> AppResult<Json<SearchEventsResponse>> {
    let Query(request) = query.map_err(|_| AppError::InvalidEventQuery)?;
    let query = EventQuery::try_from(request).map_err(|_| AppError::InvalidEventQuery)?;
    let events = state
        .event_service()
        .search_events(&query)
        .await
        .map_err(AppError::from)?;

    info!(
        operation = "search_events",
        result = "success",
        count = events.len(),
        "events searched"
    );

    Ok(Json(SearchEventsResponse::from_events(events)))
}
