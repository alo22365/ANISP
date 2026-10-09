use crate::{AppError, AppResult, AppState};
use anisp_git_context::{ChangeType, GitCommitFilters, GitCommitQuery, GitCommitView};
use axum::{
    Json,
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

/// HTTP representation stays separate from the validated Domain query.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GitQueryRequest {
    repository_id: Option<String>,
    project_id: Option<String>,
    service: Option<String>,
    branch: Option<String>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    limit: Option<u32>,
}
#[derive(Serialize)]
pub(crate) struct GitCommitResponse {
    event_id: Uuid,
    repository_id: String,
    repository_name: String,
    project_id: String,
    service: String,
    branch: Option<String>,
    commit_sha: String,
    parent_shas: Vec<String>,
    author_name: String,
    author_email: Option<String>,
    message: String,
    committed_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
    changed_file_count: u64,
    additions: Option<u64>,
    deletions: Option<u64>,
    changes_truncated: bool,
    changed_files: Vec<GitFileResponse>,
}
#[derive(Serialize)]
struct GitFileResponse {
    path: String,
    old_path: Option<String>,
    change_type: &'static str,
    additions: Option<u64>,
    deletions: Option<u64>,
}
impl From<GitCommitView> for GitCommitResponse {
    fn from(view: GitCommitView) -> Self {
        Self {
            event_id: view.event_id(),
            repository_id: view.repository_id().into(),
            repository_name: view.repository_name().into(),
            project_id: view.project_id().into(),
            service: view.service().into(),
            branch: view.branch().map(str::to_owned),
            commit_sha: view.commit_sha().as_str().into(),
            parent_shas: view.parent_shas().iter().map(ToString::to_string).collect(),
            author_name: view.author_name().into(),
            author_email: view.author_email().map(str::to_owned),
            message: view.message().into(),
            committed_at: view.committed_at(),
            observed_at: view.observed_at(),
            changed_file_count: view.changed_file_count(),
            additions: view.additions(),
            deletions: view.deletions(),
            changes_truncated: view.changes_truncated(),
            changed_files: view
                .changed_files()
                .iter()
                .map(|f| GitFileResponse {
                    path: f.path().into(),
                    old_path: f.old_path().map(str::to_owned),
                    change_type: match f.change_type() {
                        ChangeType::Added => "added",
                        ChangeType::Modified => "modified",
                        ChangeType::Deleted => "deleted",
                        ChangeType::Renamed => "renamed",
                    },
                    additions: f.additions(),
                    deletions: f.deletions(),
                })
                .collect(),
        }
    }
}
#[derive(Serialize)]
pub(crate) struct GitListResponse {
    items: Vec<GitCommitResponse>,
    count: usize,
}

pub(crate) async fn search_git_commits(
    State(state): State<AppState>,
    request: Result<Query<GitQueryRequest>, QueryRejection>,
) -> AppResult<Json<GitListResponse>> {
    let Query(request) = request.map_err(|_| AppError::InvalidGitQuery)?;
    let query = GitCommitQuery::new(
        GitCommitFilters {
            repository_id: request.repository_id,
            project_id: request.project_id,
            service: request.service,
            branch: request.branch,
        },
        request.from,
        request.to,
        request.limit,
    )
    .map_err(|_| AppError::InvalidGitQuery)?;
    let views = state
        .git_context_query_service()
        .search_commits(&query)
        .await?;
    info!(
        operation = "git.commit.search",
        result = "success",
        count = views.len(),
        "Git commits fetched"
    );
    Ok(Json(GitListResponse {
        count: views.len(),
        items: views.into_iter().map(Into::into).collect(),
    }))
}
pub(crate) async fn get_git_commit(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> AppResult<Json<GitCommitResponse>> {
    let Path((repository_id, sha)) = path.map_err(|_| AppError::InvalidGitQuery)?;
    let view = state
        .git_context_query_service()
        .get_commit(&repository_id, &sha)
        .await?
        .ok_or(AppError::GitCommitNotFound)?;
    info!(operation = "git.commit.get", result = "success", event_id = %view.event_id(), "Git commit fetched");
    Ok(Json(view.into()))
}
