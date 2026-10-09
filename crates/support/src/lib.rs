//! IT Support domain model.
//!
//! This crate is intentionally independent from transport and persistence.

mod case_type;
mod command;
mod error;
mod lifecycle;
mod model;
mod priority;
mod query;
mod repository;
mod service;
mod status;

pub use case_type::CaseType;
pub use command::CreateSupportCaseCommand;
pub use error::{SupportCaseError, SupportRepositoryError, SupportServiceError};
pub use lifecycle::{
    StoredSupportCaseLifecycleEvent, SupportCaseLifecycleEvent, SupportCaseLifecycleKind,
};
pub use model::{StoredSupportCase, SupportCase};
pub use priority::Priority;
pub use query::{
    DEFAULT_SUPPORT_CASE_LIMIT, MAX_SUPPORT_CASE_LIMIT, SupportCaseFilters, SupportCaseQuery,
    SupportCaseQueryError,
};
pub use repository::{
    ClaimedSupportLifecycleEvent, OutboxFailureDisposition, SupportCaseRepository,
    SupportOutboxOperationalReader, SupportOutboxOperationalStatus, SupportOutboxRepository,
};
pub use service::SupportService;
pub use status::CaseStatus;
