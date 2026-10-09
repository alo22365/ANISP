//! Shared transport types that do not depend on the server framework.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
}

impl HealthResponse {
    pub fn ok() -> Self {
        Self {
            status: "ok".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadinessResponse {
    pub status: String,
    pub dependencies: DependencyReadiness,
}

impl ReadinessResponse {
    pub fn ready() -> Self {
        Self::from_availability(true, true)
    }

    pub fn from_availability(clickhouse: bool, postgres: bool) -> Self {
        Self {
            status: if clickhouse && postgres {
                "ready"
            } else {
                "not_ready"
            }
            .to_owned(),
            dependencies: DependencyReadiness {
                clickhouse: dependency_status(clickhouse),
                postgres: dependency_status(postgres),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyReadiness {
    pub clickhouse: String,
    pub postgres: String,
}

fn dependency_status(available: bool) -> String {
    if available { "ok" } else { "unavailable" }.to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: ErrorDetails,
}

impl ErrorResponse {
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        request_id: impl Into<String>,
    ) -> Self {
        Self {
            error: ErrorDetails {
                code: code.into(),
                message: message.into(),
                request_id: request_id.into(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorDetails {
    pub code: String,
    pub message: String,
    pub request_id: String,
}
