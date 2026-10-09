use std::{error::Error, fmt};

use anisp_k8s_context::K8sContextError;

/// Stable adapter categories. Provider response bodies and credentials are
/// never retained in errors or their source chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollectorError {
    TargetMismatch,
    UnsupportedWorkloadKind,
    WorkloadNotFound,
    InvalidSelector,
    Forbidden,
    ApiUnavailable,
    Mapping(&'static str),
    Domain(K8sContextError),
    Internal,
}

impl fmt::Display for CollectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TargetMismatch => f.write_str("target cluster does not match collector"),
            Self::UnsupportedWorkloadKind => f.write_str("only Deployment snapshots are supported"),
            Self::WorkloadNotFound => f.write_str("deployment not found"),
            Self::InvalidSelector => f.write_str("deployment selector cannot be safely converted"),
            Self::Forbidden => f.write_str("Kubernetes API access denied"),
            Self::ApiUnavailable => f.write_str("Kubernetes API unavailable"),
            Self::Mapping(field) => write!(f, "invalid or missing Kubernetes fact: {field}"),
            Self::Domain(error) => write!(f, "invalid runtime domain fact: {error}"),
            Self::Internal => f.write_str("Kubernetes adapter internal error"),
        }
    }
}

impl Error for CollectorError {}

impl From<K8sContextError> for CollectorError {
    fn from(value: K8sContextError) -> Self {
        Self::Domain(value)
    }
}

pub(crate) fn api_error(error: kube::Error, fetching_workload: bool) -> CollectorError {
    match error {
        kube::Error::Api(response) => match response.code {
            401 | 403 => CollectorError::Forbidden,
            404 if fetching_workload => CollectorError::WorkloadNotFound,
            _ => CollectorError::ApiUnavailable,
        },
        kube::Error::SerdeError(_) => CollectorError::Mapping("API response"),
        kube::Error::BuildRequest(_) => CollectorError::Internal,
        _ => CollectorError::ApiUnavailable,
    }
}
