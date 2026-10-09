use std::fmt;

use anisp_context::SupportCaseContextError;
use anisp_event::{EventServiceError, RepositoryError, ValidationError};
use anisp_git_context::{GitContextQueryError, GitReaderError};
use anisp_git_ingest::GitOutboxError;
use anisp_support::{SupportCaseError, SupportRepositoryError, SupportServiceError};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tracing::warn;

pub type AppResult<T> = Result<T, AppError>;

/// Opaque transport error details. Request middleware adds the request ID.
#[derive(Clone)]
pub(crate) struct HttpError {
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum AppError {
    RouteNotFound,
    EventNotFound,
    SupportCaseNotFound,
    GitCommitNotFound,
    InvalidGitQuery,
    GitReader(GitReaderError),
    GitOutbox(GitOutboxError),
    InvalidRequest,
    InvalidEventQuery,
    InvalidSupportCaseQuery,
    InvalidEventId,
    InvalidSupportCaseId,
    PayloadTooLarge,
    Validation(ValidationError),
    SupportValidation(SupportCaseError),
    SupportInvalidTransition(SupportCaseError),
    Internal,
    EventRepositoryUnavailable(RepositoryError),
    EventRepositoryTimeout(RepositoryError),
    EventRepositoryInternal(RepositoryError),
    SupportRepositoryUnavailable(SupportRepositoryError),
    SupportRepositoryTimeout(SupportRepositoryError),
    SupportRepositoryConflict(SupportRepositoryError),
    SupportRepositoryInternal(SupportRepositoryError),
}

impl fmt::Display for AppError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RouteNotFound => formatter.write_str("resource not found"),
            Self::EventNotFound => formatter.write_str("event not found"),
            Self::SupportCaseNotFound => formatter.write_str("support case not found"),
            Self::GitCommitNotFound => formatter.write_str("Git commit not found"),
            Self::InvalidGitQuery => formatter.write_str("invalid Git commit identity or query"),
            Self::GitReader(error) => error.fmt(formatter),
            Self::GitOutbox(error) => error.fmt(formatter),
            Self::InvalidRequest => formatter.write_str("invalid request body"),
            Self::InvalidEventQuery => formatter.write_str("invalid event query"),
            Self::InvalidSupportCaseQuery => formatter.write_str("invalid support case query"),
            Self::InvalidEventId => formatter.write_str("invalid event_id"),
            Self::InvalidSupportCaseId => formatter.write_str("invalid case_id"),
            Self::PayloadTooLarge => formatter.write_str("request payload too large"),
            Self::Validation(error) => error.fmt(formatter),
            Self::SupportValidation(error) => error.fmt(formatter),
            Self::SupportInvalidTransition(error) => error.fmt(formatter),
            Self::Internal => formatter.write_str("internal server error"),
            Self::EventRepositoryUnavailable(_) => {
                formatter.write_str("event repository is unavailable")
            }
            Self::EventRepositoryTimeout(_) => formatter.write_str("event repository timed out"),
            Self::EventRepositoryInternal(_) => {
                formatter.write_str("event repository internal error")
            }
            Self::SupportRepositoryUnavailable(_) => {
                formatter.write_str("support repository is unavailable")
            }
            Self::SupportRepositoryTimeout(_) => {
                formatter.write_str("support repository timed out")
            }
            Self::SupportRepositoryConflict(_) => {
                formatter.write_str("support case update conflicted")
            }
            Self::SupportRepositoryInternal(_) => {
                formatter.write_str("support repository internal error")
            }
        }
    }
}

impl std::error::Error for AppError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Validation(source) => Some(source),
            Self::SupportValidation(source) => Some(source),
            Self::SupportInvalidTransition(source) => Some(source),
            Self::EventRepositoryUnavailable(source)
            | Self::EventRepositoryTimeout(source)
            | Self::EventRepositoryInternal(source) => Some(source),
            Self::SupportRepositoryUnavailable(source)
            | Self::SupportRepositoryTimeout(source)
            | Self::SupportRepositoryConflict(source)
            | Self::SupportRepositoryInternal(source) => Some(source),
            _ => None,
        }
    }
}

impl From<EventServiceError> for AppError {
    fn from(error: EventServiceError) -> Self {
        match error {
            EventServiceError::Validation(error) => Self::Validation(error),
            EventServiceError::Repository(error @ RepositoryError::Unavailable(_)) => {
                Self::EventRepositoryUnavailable(error)
            }
            EventServiceError::Repository(error @ RepositoryError::Timeout(_)) => {
                Self::EventRepositoryTimeout(error)
            }
            EventServiceError::Repository(
                error @ (RepositoryError::Decode(_) | RepositoryError::Internal(_)),
            ) => Self::EventRepositoryInternal(error),
        }
    }
}

impl From<GitContextQueryError> for AppError {
    fn from(error: GitContextQueryError) -> Self {
        match error {
            GitContextQueryError::InvalidQuery(_) => Self::InvalidGitQuery,
            GitContextQueryError::Reader(error) => Self::GitReader(error),
        }
    }
}

impl From<GitOutboxError> for AppError {
    fn from(error: GitOutboxError) -> Self {
        Self::GitOutbox(error)
    }
}

impl From<SupportServiceError> for AppError {
    fn from(error: SupportServiceError) -> Self {
        match error {
            SupportServiceError::Validation(error @ SupportCaseError::InvalidTransition { .. }) => {
                Self::SupportInvalidTransition(error)
            }
            SupportServiceError::Validation(error) => Self::SupportValidation(error),
            SupportServiceError::Repository(error @ SupportRepositoryError::Unavailable(_)) => {
                Self::SupportRepositoryUnavailable(error)
            }
            SupportServiceError::Repository(error @ SupportRepositoryError::Timeout(_)) => {
                Self::SupportRepositoryTimeout(error)
            }
            SupportServiceError::Repository(error @ SupportRepositoryError::Conflict(_)) => {
                Self::SupportRepositoryConflict(error)
            }
            SupportServiceError::Repository(
                error @ (SupportRepositoryError::Decode(_) | SupportRepositoryError::Internal(_)),
            ) => Self::SupportRepositoryInternal(error),
        }
    }
}

impl From<SupportRepositoryError> for AppError {
    fn from(error: SupportRepositoryError) -> Self {
        match error {
            error @ SupportRepositoryError::Unavailable(_) => {
                Self::SupportRepositoryUnavailable(error)
            }
            error @ SupportRepositoryError::Timeout(_) => Self::SupportRepositoryTimeout(error),
            error @ SupportRepositoryError::Conflict(_) => Self::SupportRepositoryConflict(error),
            error @ (SupportRepositoryError::Decode(_) | SupportRepositoryError::Internal(_)) => {
                Self::SupportRepositoryInternal(error)
            }
        }
    }
}

impl From<SupportCaseContextError> for AppError {
    fn from(error: SupportCaseContextError) -> Self {
        match error {
            SupportCaseContextError::Support(error) => error.into(),
            SupportCaseContextError::Git(error) => error.into(),
            SupportCaseContextError::GitSync(error) => Self::GitReader(error),
            SupportCaseContextError::InvalidGitWindow => Self::Internal,
            SupportCaseContextError::Event(error) => error.into(),
            SupportCaseContextError::Sync(error) => match error {
                error @ SupportRepositoryError::Unavailable(_) => {
                    Self::SupportRepositoryUnavailable(error)
                }
                error @ SupportRepositoryError::Timeout(_) => Self::SupportRepositoryTimeout(error),
                error @ SupportRepositoryError::Conflict(_) => {
                    Self::SupportRepositoryConflict(error)
                }
                error @ (SupportRepositoryError::Decode(_)
                | SupportRepositoryError::Internal(_)) => Self::SupportRepositoryInternal(error),
            },
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::GitOutbox(error) => {
                warn!(error_category = %error, "Git outbox operational read failed");
                match error {
                    GitOutboxError::Unavailable => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "service_unavailable",
                        "Git outbox status is unavailable".to_owned(),
                    ),
                    GitOutboxError::Timeout => (
                        StatusCode::GATEWAY_TIMEOUT,
                        "request_timeout",
                        "Git outbox status query timed out".to_owned(),
                    ),
                    GitOutboxError::Decode
                    | GitOutboxError::Internal
                    | GitOutboxError::Conflict => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal_error",
                        "internal server error".to_owned(),
                    ),
                }
            }
            Self::GitCommitNotFound => (
                StatusCode::NOT_FOUND,
                "git_commit_not_found",
                "Git commit not found".to_owned(),
            ),
            Self::InvalidGitQuery => (
                StatusCode::BAD_REQUEST,
                "invalid_git_commit_query",
                "invalid Git commit identity or query".to_owned(),
            ),
            Self::GitReader(error) => {
                warn!(error_category = %error, "Git context read failed");
                match error {
                    GitReaderError::Unavailable => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "service_unavailable",
                        "Git context is unavailable".to_owned(),
                    ),
                    GitReaderError::Timeout => (
                        StatusCode::GATEWAY_TIMEOUT,
                        "request_timeout",
                        "Git context query timed out".to_owned(),
                    ),
                    GitReaderError::Decode | GitReaderError::Internal => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal_error",
                        "internal server error".to_owned(),
                    ),
                }
            }
            Self::RouteNotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "resource not found".to_owned(),
            ),
            Self::EventNotFound => (
                StatusCode::NOT_FOUND,
                "event_not_found",
                "event not found".to_owned(),
            ),
            Self::SupportCaseNotFound => (
                StatusCode::NOT_FOUND,
                "support_case_not_found",
                "support case not found".to_owned(),
            ),
            Self::InvalidRequest => (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "invalid request body".to_owned(),
            ),
            Self::InvalidEventQuery => (
                StatusCode::BAD_REQUEST,
                "invalid_event_query",
                "invalid event query".to_owned(),
            ),
            Self::InvalidSupportCaseQuery => (
                StatusCode::BAD_REQUEST,
                "invalid_support_case_query",
                "invalid support case query".to_owned(),
            ),
            Self::InvalidEventId => (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "invalid event_id".to_owned(),
            ),
            Self::InvalidSupportCaseId => (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "invalid case_id".to_owned(),
            ),
            Self::PayloadTooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                "request payload too large".to_owned(),
            ),
            Self::Validation(error) => {
                (StatusCode::BAD_REQUEST, "invalid_event", error.to_string())
            }
            Self::SupportValidation(error) => (
                StatusCode::BAD_REQUEST,
                "invalid_support_case",
                error.to_string(),
            ),
            Self::SupportInvalidTransition(_) => (
                StatusCode::CONFLICT,
                "invalid_support_case_transition",
                "invalid support case transition".to_owned(),
            ),
            Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "internal server error".to_owned(),
            ),
            Self::EventRepositoryUnavailable(source) => {
                warn!(error = %source, "event repository operation failed");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "service_unavailable",
                    "event repository is unavailable".to_owned(),
                )
            }
            Self::EventRepositoryTimeout(source) => {
                warn!(error = %source, "event repository timed out");
                (
                    StatusCode::GATEWAY_TIMEOUT,
                    "request_timeout",
                    "event repository timed out".to_owned(),
                )
            }
            Self::EventRepositoryInternal(source) => {
                warn!(error = %source, "event repository internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "internal server error".to_owned(),
                )
            }
            Self::SupportRepositoryUnavailable(source) => {
                warn!(error = %source, "support repository operation failed");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "service_unavailable",
                    "support repository is unavailable".to_owned(),
                )
            }
            Self::SupportRepositoryTimeout(source) => {
                warn!(error = %source, "support repository timed out");
                (
                    StatusCode::GATEWAY_TIMEOUT,
                    "request_timeout",
                    "support repository timed out".to_owned(),
                )
            }
            Self::SupportRepositoryConflict(source) => {
                warn!(error = %source, "support case update conflicted");
                (
                    StatusCode::CONFLICT,
                    "support_case_conflict",
                    "support case was updated concurrently".to_owned(),
                )
            }
            Self::SupportRepositoryInternal(source) => {
                warn!(error = %source, "support repository internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "internal server error".to_owned(),
                )
            }
        };

        let mut response = status.into_response();
        response
            .extensions_mut()
            .insert(HttpError { code, message });
        response
    }
}
