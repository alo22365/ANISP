use std::time::Instant;

use anisp_context::{GitContext, GitContextStatus, HistorySyncStatus};
use anisp_event::EventResponse;
use anisp_support::{
    CaseStatus, CaseType, CreateSupportCaseCommand, Priority, SupportCase, SupportCaseFilters,
    SupportCaseQuery,
};
use axum::{
    Json,
    extract::{
        Path, Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

use super::git::GitCommitResponse;
use crate::{AppError, AppResult, AppState};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateSupportCaseRequest {
    case_type: CaseTypeDto,
    title: String,
    description: String,
    reporter_id: String,
    assignee_id: Option<String>,
    project_id: Option<String>,
    service: Option<String>,
    environment: Option<String>,
    priority: PriorityDto,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CaseTypeDto {
    Ticket,
    Incident,
    Request,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum PriorityDto {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateSupportCaseStatusRequest {
    status: CaseStatusDto,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CaseStatusDto {
    Open,
    Investigating,
    Resolved,
    Closed,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct SearchSupportCasesRequest {
    case_type: Option<CaseTypeDto>,
    status: Option<CaseStatusDto>,
    priority: Option<PriorityDto>,
    reporter_id: Option<String>,
    assignee_id: Option<String>,
    project_id: Option<String>,
    service: Option<String>,
    environment: Option<String>,
    created_from: Option<DateTime<Utc>>,
    created_to: Option<DateTime<Utc>>,
    limit: Option<u32>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SupportCaseResponse {
    case_id: Uuid,
    case_type: &'static str,
    title: String,
    description: String,
    reporter_id: String,
    assignee_id: Option<String>,
    project_id: Option<String>,
    service: Option<String>,
    environment: Option<String>,
    priority: &'static str,
    status: &'static str,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    resolved_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SearchSupportCasesResponse {
    items: Vec<SupportCaseResponse>,
    count: usize,
}

#[derive(Serialize)]
pub(crate) struct SupportCaseContextResponse {
    case: SupportCaseResponse,
    timeline: Vec<EventResponse>,
    history_sync: HistorySyncResponse,
    git_context: GitContextResponse,
}

#[derive(Serialize)]
struct GitContextResponse {
    status: &'static str,
    window: Option<GitContextWindowResponse>,
    pending_events: u64,
    count: usize,
    commits: Vec<GitCommitResponse>,
}
#[derive(Serialize)]
struct GitContextWindowResponse {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}
impl From<GitContext> for GitContextResponse {
    fn from(context: GitContext) -> Self {
        Self {
            status: match context.status() {
                GitContextStatus::Synced => "synced",
                GitContextStatus::Pending => "pending",
                GitContextStatus::Unmapped => "unmapped",
            },
            window: context.window().map(|w| GitContextWindowResponse {
                from: w.from(),
                to: w.to(),
            }),
            pending_events: context.pending_events(),
            count: context.commits().len(),
            commits: context.into_commits().into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct HistorySyncResponse {
    status: &'static str,
    pending_events: u64,
}

pub(crate) async fn create_support_case(
    State(state): State<AppState>,
    payload: Result<Json<CreateSupportCaseRequest>, JsonRejection>,
) -> AppResult<(StatusCode, Json<SupportCaseResponse>)> {
    let Json(request) = payload.map_err(|error| {
        if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
            AppError::PayloadTooLarge
        } else {
            AppError::InvalidRequest
        }
    })?;
    let support_case = state
        .support_service()
        .create_case(request.into())
        .await
        .map_err(AppError::from)?;

    info!(
        operation = "create_support_case",
        result = "success",
        case_id = %support_case.case_id(),
        case_type = case_type_str(support_case.case_type()),
        project_id = support_case.project_id().unwrap_or_default(),
        service = support_case.service().unwrap_or_default(),
        status = status_str(support_case.status()),
        "support case created"
    );

    Ok((StatusCode::CREATED, Json(support_case.into())))
}

pub(crate) async fn get_support_case(
    State(state): State<AppState>,
    Path(case_id): Path<String>,
) -> AppResult<Json<SupportCaseResponse>> {
    let case_id = Uuid::parse_str(&case_id).map_err(|_| AppError::InvalidSupportCaseId)?;
    let support_case = state
        .support_service()
        .get_case(case_id)
        .await
        .map_err(AppError::from)?
        .ok_or(AppError::SupportCaseNotFound)?;

    info!(
        operation = "get_support_case",
        result = "success",
        case_id = %support_case.case_id(),
        case_type = case_type_str(support_case.case_type()),
        project_id = support_case.project_id().unwrap_or_default(),
        service = support_case.service().unwrap_or_default(),
        status = status_str(support_case.status()),
        "support case fetched"
    );

    Ok(Json(support_case.into()))
}

pub(crate) async fn search_support_cases(
    State(state): State<AppState>,
    query: Result<Query<SearchSupportCasesRequest>, QueryRejection>,
) -> AppResult<Json<SearchSupportCasesResponse>> {
    let started = Instant::now();
    let Query(request) = query.map_err(|_| AppError::InvalidSupportCaseQuery)?;
    let query =
        SupportCaseQuery::try_from(request).map_err(|_| AppError::InvalidSupportCaseQuery)?;
    let cases = state
        .support_service()
        .search_cases(&query)
        .await
        .map_err(AppError::from)?;
    let items: Vec<_> = cases.into_iter().map(SupportCaseResponse::from).collect();

    info!(
        operation = "support.case.search",
        result = "success",
        count = items.len(),
        duration_ms = started.elapsed().as_millis() as u64,
        "support cases searched"
    );

    Ok(Json(SearchSupportCasesResponse {
        count: items.len(),
        items,
    }))
}

pub(crate) async fn get_support_case_context(
    State(state): State<AppState>,
    Path(case_id): Path<String>,
) -> AppResult<Json<SupportCaseContextResponse>> {
    let started = Instant::now();
    let case_id = Uuid::parse_str(&case_id).map_err(|_| AppError::InvalidSupportCaseId)?;
    let context = state
        .support_context_service()
        .get_context(case_id)
        .await
        .map_err(AppError::from)?
        .ok_or(AppError::SupportCaseNotFound)?;
    let (support_case, events, history_sync, git_context) = context.into_parts();
    let git_context = GitContextResponse::from(git_context);
    let event_count = events.len();
    let pending_event_count = history_sync.pending_events();
    let history_sync_status = match history_sync.status() {
        HistorySyncStatus::Synced => "synced",
        HistorySyncStatus::Pending => "pending",
    };

    info!(
        operation = "support.case.context",
        result = "success",
        case_id = %case_id,
        event_count,
        pending_event_count,
        history_sync_status,
        git_commit_count = git_context.count,
        pending_git_event_count = git_context.pending_events,
        git_context_status = git_context.status,
        duration_ms = started.elapsed().as_millis() as u64,
        "support case context composed"
    );

    Ok(Json(SupportCaseContextResponse {
        case: support_case.into(),
        timeline: events.into_iter().map(EventResponse::from).collect(),
        history_sync: HistorySyncResponse {
            status: history_sync_status,
            pending_events: pending_event_count,
        },
        git_context,
    }))
}

pub(crate) async fn update_support_case_status(
    State(state): State<AppState>,
    Path(case_id): Path<String>,
    payload: Result<Json<UpdateSupportCaseStatusRequest>, JsonRejection>,
) -> AppResult<Json<SupportCaseResponse>> {
    let case_id = Uuid::parse_str(&case_id).map_err(|_| AppError::InvalidSupportCaseId)?;
    let Json(request) = payload.map_err(|error| {
        if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
            AppError::PayloadTooLarge
        } else {
            AppError::InvalidRequest
        }
    })?;
    let support_case = state
        .support_service()
        .update_case_status(case_id, request.status.into())
        .await
        .map_err(AppError::from)?
        .ok_or(AppError::SupportCaseNotFound)?;

    info!(
        operation = "update_support_case_status",
        result = "success",
        case_id = %support_case.case_id(),
        case_type = case_type_str(support_case.case_type()),
        project_id = support_case.project_id().unwrap_or_default(),
        service = support_case.service().unwrap_or_default(),
        status = status_str(support_case.status()),
        "support case status updated"
    );

    Ok(Json(support_case.into()))
}

impl From<CreateSupportCaseRequest> for CreateSupportCaseCommand {
    fn from(request: CreateSupportCaseRequest) -> Self {
        Self {
            case_type: request.case_type.into(),
            title: request.title,
            description: request.description,
            reporter_id: request.reporter_id,
            assignee_id: request.assignee_id,
            project_id: request.project_id,
            service: request.service,
            environment: request.environment,
            priority: request.priority.into(),
        }
    }
}

impl TryFrom<SearchSupportCasesRequest> for SupportCaseQuery {
    type Error = anisp_support::SupportCaseQueryError;

    fn try_from(request: SearchSupportCasesRequest) -> Result<Self, Self::Error> {
        Self::new(
            SupportCaseFilters {
                case_type: request.case_type.map(Into::into),
                status: request.status.map(Into::into),
                priority: request.priority.map(Into::into),
                reporter_id: request.reporter_id,
                assignee_id: request.assignee_id,
                project_id: request.project_id,
                service: request.service,
                environment: request.environment,
            },
            request.created_from,
            request.created_to,
            request.limit,
        )
    }
}

impl From<CaseTypeDto> for CaseType {
    fn from(value: CaseTypeDto) -> Self {
        match value {
            CaseTypeDto::Ticket => Self::Ticket,
            CaseTypeDto::Incident => Self::Incident,
            CaseTypeDto::Request => Self::Request,
        }
    }
}

impl From<PriorityDto> for Priority {
    fn from(value: PriorityDto) -> Self {
        match value {
            PriorityDto::Low => Self::Low,
            PriorityDto::Medium => Self::Medium,
            PriorityDto::High => Self::High,
            PriorityDto::Critical => Self::Critical,
        }
    }
}

impl From<CaseStatusDto> for CaseStatus {
    fn from(value: CaseStatusDto) -> Self {
        match value {
            CaseStatusDto::Open => Self::Open,
            CaseStatusDto::Investigating => Self::Investigating,
            CaseStatusDto::Resolved => Self::Resolved,
            CaseStatusDto::Closed => Self::Closed,
        }
    }
}

impl From<SupportCase> for SupportCaseResponse {
    fn from(support_case: SupportCase) -> Self {
        Self {
            case_id: support_case.case_id(),
            case_type: case_type_str(support_case.case_type()),
            title: support_case.title().to_owned(),
            description: support_case.description().to_owned(),
            reporter_id: support_case.reporter_id().to_owned(),
            assignee_id: support_case.assignee_id().map(str::to_owned),
            project_id: support_case.project_id().map(str::to_owned),
            service: support_case.service().map(str::to_owned),
            environment: support_case.environment().map(str::to_owned),
            priority: priority_str(support_case.priority()),
            status: status_str(support_case.status()),
            created_at: support_case.created_at(),
            updated_at: support_case.updated_at(),
            resolved_at: support_case.resolved_at(),
        }
    }
}

fn case_type_str(case_type: CaseType) -> &'static str {
    match case_type {
        CaseType::Ticket => "ticket",
        CaseType::Incident => "incident",
        CaseType::Request => "request",
    }
}

fn priority_str(priority: Priority) -> &'static str {
    match priority {
        Priority::Low => "low",
        Priority::Medium => "medium",
        Priority::High => "high",
        Priority::Critical => "critical",
    }
}

fn status_str(status: CaseStatus) -> &'static str {
    match status {
        CaseStatus::Open => "open",
        CaseStatus::Investigating => "investigating",
        CaseStatus::Resolved => "resolved",
        CaseStatus::Closed => "closed",
    }
}
