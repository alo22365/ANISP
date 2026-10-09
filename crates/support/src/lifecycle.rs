use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{CaseStatus, CaseType, Priority, SupportCase};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupportCaseLifecycleKind {
    Created,
    InvestigationStarted,
    Resolved,
    Closed,
}

/// Storage- and framework-independent lifecycle event persisted through the outbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportCaseLifecycleEvent {
    event_id: Uuid,
    case_id: Uuid,
    case_type: CaseType,
    kind: SupportCaseLifecycleKind,
    occurred_at: DateTime<Utc>,
    previous_updated_at: Option<DateTime<Utc>>,
    previous_status: Option<CaseStatus>,
    current_status: CaseStatus,
    title: String,
    reporter_id: String,
    assignee_id: Option<String>,
    project_id: Option<String>,
    service: Option<String>,
    environment: Option<String>,
    priority: Priority,
}

/// Storage-neutral values used to restore a pending outbox event.
pub struct StoredSupportCaseLifecycleEvent {
    pub event_id: Uuid,
    pub case_id: Uuid,
    pub case_type: CaseType,
    pub kind: SupportCaseLifecycleKind,
    pub occurred_at: DateTime<Utc>,
    pub previous_updated_at: Option<DateTime<Utc>>,
    pub previous_status: Option<CaseStatus>,
    pub current_status: CaseStatus,
    pub title: String,
    pub reporter_id: String,
    pub assignee_id: Option<String>,
    pub project_id: Option<String>,
    pub service: Option<String>,
    pub environment: Option<String>,
    pub priority: Priority,
}

impl SupportCaseLifecycleEvent {
    pub(crate) fn created(support_case: &SupportCase) -> Self {
        Self::from_case(
            support_case,
            SupportCaseLifecycleKind::Created,
            support_case.created_at(),
            None,
            None,
        )
    }

    pub(crate) fn transitioned(
        support_case: &SupportCase,
        kind: SupportCaseLifecycleKind,
        occurred_at: DateTime<Utc>,
        previous_updated_at: DateTime<Utc>,
        previous_status: CaseStatus,
    ) -> Self {
        Self::from_case(
            support_case,
            kind,
            occurred_at,
            Some(previous_updated_at),
            Some(previous_status),
        )
    }

    fn from_case(
        support_case: &SupportCase,
        kind: SupportCaseLifecycleKind,
        occurred_at: DateTime<Utc>,
        previous_updated_at: Option<DateTime<Utc>>,
        previous_status: Option<CaseStatus>,
    ) -> Self {
        Self {
            event_id: Uuid::now_v7(),
            case_id: support_case.case_id(),
            case_type: support_case.case_type(),
            kind,
            occurred_at,
            previous_updated_at,
            previous_status,
            current_status: support_case.status(),
            title: support_case.title().to_owned(),
            reporter_id: support_case.reporter_id().to_owned(),
            assignee_id: support_case.assignee_id().map(str::to_owned),
            project_id: support_case.project_id().map(str::to_owned),
            service: support_case.service().map(str::to_owned),
            environment: support_case.environment().map(str::to_owned),
            priority: support_case.priority(),
        }
    }

    pub fn rehydrate(stored: StoredSupportCaseLifecycleEvent) -> Self {
        Self {
            event_id: stored.event_id,
            case_id: stored.case_id,
            case_type: stored.case_type,
            kind: stored.kind,
            occurred_at: stored.occurred_at,
            previous_updated_at: stored.previous_updated_at,
            previous_status: stored.previous_status,
            current_status: stored.current_status,
            title: stored.title,
            reporter_id: stored.reporter_id,
            assignee_id: stored.assignee_id,
            project_id: stored.project_id,
            service: stored.service,
            environment: stored.environment,
            priority: stored.priority,
        }
    }

    pub fn event_id(&self) -> Uuid {
        self.event_id
    }

    pub fn case_id(&self) -> Uuid {
        self.case_id
    }

    pub fn case_type(&self) -> CaseType {
        self.case_type
    }

    pub fn kind(&self) -> SupportCaseLifecycleKind {
        self.kind
    }

    pub fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    pub fn previous_updated_at(&self) -> Option<DateTime<Utc>> {
        self.previous_updated_at
    }

    pub fn previous_status(&self) -> Option<CaseStatus> {
        self.previous_status
    }

    pub fn current_status(&self) -> CaseStatus {
        self.current_status
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn reporter_id(&self) -> &str {
        &self.reporter_id
    }

    pub fn assignee_id(&self) -> Option<&str> {
        self.assignee_id.as_deref()
    }

    pub fn project_id(&self) -> Option<&str> {
        self.project_id.as_deref()
    }

    pub fn service(&self) -> Option<&str> {
        self.service.as_deref()
    }

    pub fn environment(&self) -> Option<&str> {
        self.environment.as_deref()
    }

    pub fn priority(&self) -> Priority {
        self.priority
    }

    pub fn event_type(&self) -> String {
        format!(
            "support.{}.{}",
            case_type_segment(self.case_type),
            lifecycle_segment(self.kind)
        )
    }
}

fn case_type_segment(case_type: CaseType) -> &'static str {
    match case_type {
        CaseType::Ticket => "ticket",
        CaseType::Incident => "incident",
        CaseType::Request => "request",
    }
}

fn lifecycle_segment(kind: SupportCaseLifecycleKind) -> &'static str {
    match kind {
        SupportCaseLifecycleKind::Created => "created",
        SupportCaseLifecycleKind::InvestigationStarted => "investigating",
        SupportCaseLifecycleKind::Resolved => "resolved",
        SupportCaseLifecycleKind::Closed => "closed",
    }
}
