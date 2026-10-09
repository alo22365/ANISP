use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{
    CaseStatus, CaseType, CreateSupportCaseCommand, Priority, SupportCaseError,
    SupportCaseLifecycleEvent, SupportCaseLifecycleKind,
};

/// Framework- and storage-independent IT support case aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportCase {
    case_id: Uuid,
    case_type: CaseType,
    title: String,
    description: String,
    reporter_id: String,
    assignee_id: Option<String>,
    project_id: Option<String>,
    service: Option<String>,
    environment: Option<String>,
    priority: Priority,
    status: CaseStatus,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    resolved_at: Option<DateTime<Utc>>,
}

/// Storage-neutral state used to restore a persisted support case.
pub struct StoredSupportCase {
    pub case_id: Uuid,
    pub case_type: CaseType,
    pub title: String,
    pub description: String,
    pub reporter_id: String,
    pub assignee_id: Option<String>,
    pub project_id: Option<String>,
    pub service: Option<String>,
    pub environment: Option<String>,
    pub priority: Priority,
    pub status: CaseStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
}

impl SupportCase {
    pub fn create(command: CreateSupportCaseCommand) -> Result<Self, SupportCaseError> {
        validate_required(&command.title, &command.description, &command.reporter_id)?;
        validate_optional(
            command.assignee_id.as_deref(),
            command.project_id.as_deref(),
            command.service.as_deref(),
            command.environment.as_deref(),
        )?;

        let now = truncate_to_micros(Utc::now());
        Ok(Self {
            case_id: Uuid::now_v7(),
            case_type: command.case_type,
            title: command.title,
            description: command.description,
            reporter_id: command.reporter_id,
            assignee_id: command.assignee_id,
            project_id: command.project_id,
            service: command.service,
            environment: command.environment,
            priority: command.priority,
            status: CaseStatus::Open,
            created_at: now,
            updated_at: now,
            resolved_at: None,
        })
    }

    /// Restores an existing persisted aggregate without generating new values.
    pub fn rehydrate(stored: StoredSupportCase) -> Result<Self, SupportCaseError> {
        validate_required(&stored.title, &stored.description, &stored.reporter_id)?;
        validate_optional(
            stored.assignee_id.as_deref(),
            stored.project_id.as_deref(),
            stored.service.as_deref(),
            stored.environment.as_deref(),
        )?;

        Ok(Self {
            case_id: stored.case_id,
            case_type: stored.case_type,
            title: stored.title,
            description: stored.description,
            reporter_id: stored.reporter_id,
            assignee_id: stored.assignee_id,
            project_id: stored.project_id,
            service: stored.service,
            environment: stored.environment,
            priority: stored.priority,
            status: stored.status,
            created_at: stored.created_at,
            updated_at: stored.updated_at,
            resolved_at: stored.resolved_at,
        })
    }

    pub fn case_id(&self) -> Uuid {
        self.case_id
    }

    pub fn case_type(&self) -> CaseType {
        self.case_type
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn description(&self) -> &str {
        &self.description
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

    pub fn status(&self) -> CaseStatus {
        self.status
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    pub fn resolved_at(&self) -> Option<DateTime<Utc>> {
        self.resolved_at
    }

    pub fn created_event(&self) -> SupportCaseLifecycleEvent {
        SupportCaseLifecycleEvent::created(self)
    }

    pub fn transition_to(
        &mut self,
        target: CaseStatus,
    ) -> Result<SupportCaseLifecycleEvent, SupportCaseError> {
        let (expected, kind) = match target {
            CaseStatus::Investigating => (
                CaseStatus::Open,
                SupportCaseLifecycleKind::InvestigationStarted,
            ),
            CaseStatus::Resolved => (
                CaseStatus::Investigating,
                SupportCaseLifecycleKind::Resolved,
            ),
            CaseStatus::Closed => (CaseStatus::Resolved, SupportCaseLifecycleKind::Closed),
            CaseStatus::Open => {
                return Err(SupportCaseError::InvalidTransition {
                    from: self.status,
                    to: target,
                });
            }
        };

        if self.status != expected {
            return Err(SupportCaseError::InvalidTransition {
                from: self.status,
                to: target,
            });
        }

        let previous_status = self.status;
        let previous_updated_at = self.updated_at;
        let occurred_at = truncate_to_micros(Utc::now());
        self.status = target;
        self.updated_at = occurred_at;
        if target == CaseStatus::Resolved {
            self.resolved_at = Some(occurred_at);
        }

        Ok(SupportCaseLifecycleEvent::transitioned(
            self,
            kind,
            occurred_at,
            previous_updated_at,
            previous_status,
        ))
    }
}

fn validate_required(
    title: &str,
    description: &str,
    reporter_id: &str,
) -> Result<(), SupportCaseError> {
    if title.trim().is_empty() {
        return Err(SupportCaseError::EmptyTitle);
    }
    if description.trim().is_empty() {
        return Err(SupportCaseError::EmptyDescription);
    }
    if reporter_id.trim().is_empty() {
        return Err(SupportCaseError::EmptyReporter);
    }

    for (field, value, max_bytes) in [
        ("title", title, 256),
        ("description", description, 64 * 1024),
        ("reporter_id", reporter_id, 128),
    ] {
        validate_length(field, value, max_bytes)?;
    }
    Ok(())
}

fn validate_optional(
    assignee_id: Option<&str>,
    project_id: Option<&str>,
    service: Option<&str>,
    environment: Option<&str>,
) -> Result<(), SupportCaseError> {
    for (field, value, max_bytes) in [
        ("assignee_id", assignee_id, 128),
        ("project_id", project_id, 128),
        ("service", service, 128),
        ("environment", environment, 64),
    ] {
        if let Some(value) = value {
            validate_length(field, value, max_bytes)?;
        }
    }
    Ok(())
}

fn validate_length(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), SupportCaseError> {
    if value.len() > max_bytes {
        return Err(SupportCaseError::FieldTooLong { field, max_bytes });
    }
    Ok(())
}

fn truncate_to_micros(time: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(time.timestamp_micros())
        .expect("valid datetime remains in range")
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn valid_command() -> CreateSupportCaseCommand {
        CreateSupportCaseCommand {
            case_type: CaseType::Incident,
            title: "Checkout unavailable".to_owned(),
            description: "Customers cannot complete checkout".to_owned(),
            reporter_id: "user-42".to_owned(),
            assignee_id: Some("oncall-7".to_owned()),
            project_id: Some("xpa".to_owned()),
            service: Some("xpa-finance".to_owned()),
            environment: Some("prod".to_owned()),
            priority: Priority::Critical,
        }
    }

    fn stored_case() -> StoredSupportCase {
        StoredSupportCase {
            case_id: Uuid::parse_str("018f57d0-7b9d-7000-8000-000000000001").unwrap(),
            case_type: CaseType::Request,
            title: "Access request".to_owned(),
            description: "Grant production read access".to_owned(),
            reporter_id: "user-001".to_owned(),
            assignee_id: Some("agent-001".to_owned()),
            project_id: Some("xpa".to_owned()),
            service: Some("xpa-finance".to_owned()),
            environment: Some("prod".to_owned()),
            priority: Priority::High,
            status: CaseStatus::Investigating,
            created_at: Utc.with_ymd_and_hms(2026, 9, 24, 10, 0, 0).unwrap(),
            updated_at: Utc.with_ymd_and_hms(2026, 9, 24, 10, 5, 0).unwrap(),
            resolved_at: None,
        }
    }

    #[test]
    fn create_success() {
        let before = Utc::now();
        let support_case = SupportCase::create(valid_command()).unwrap();
        let after = Utc::now();

        assert_eq!(support_case.case_type(), CaseType::Incident);
        assert_eq!(support_case.title(), "Checkout unavailable");
        assert_eq!(
            support_case.description(),
            "Customers cannot complete checkout"
        );
        assert_eq!(support_case.reporter_id(), "user-42");
        assert_eq!(support_case.assignee_id(), Some("oncall-7"));
        assert_eq!(support_case.project_id(), Some("xpa"));
        assert_eq!(support_case.service(), Some("xpa-finance"));
        assert_eq!(support_case.environment(), Some("prod"));
        assert_eq!(support_case.priority(), Priority::Critical);
        assert!(support_case.created_at() >= before);
        assert!(support_case.created_at() <= after);
        assert_eq!(support_case.updated_at(), support_case.created_at());
        assert_eq!(support_case.resolved_at(), None);
    }

    #[test]
    fn empty_title_is_rejected() {
        let mut command = valid_command();
        command.title = "  ".to_owned();
        assert_eq!(
            SupportCase::create(command).unwrap_err(),
            SupportCaseError::EmptyTitle
        );
    }

    #[test]
    fn empty_description_is_rejected() {
        let mut command = valid_command();
        command.description = "\n\t".to_owned();
        assert_eq!(
            SupportCase::create(command).unwrap_err(),
            SupportCaseError::EmptyDescription
        );
    }

    #[test]
    fn empty_reporter_is_rejected() {
        let mut command = valid_command();
        command.reporter_id.clear();
        assert_eq!(
            SupportCase::create(command).unwrap_err(),
            SupportCaseError::EmptyReporter
        );
    }

    #[test]
    fn default_status_is_open() {
        let support_case = SupportCase::create(valid_command()).unwrap();
        assert_eq!(support_case.status(), CaseStatus::Open);
    }

    #[test]
    fn generated_case_id_is_uuid_v7() {
        let support_case = SupportCase::create(valid_command()).unwrap();
        assert_eq!(support_case.case_id().get_version_num(), 7);
    }

    #[test]
    fn rehydrate_preserves_persisted_identity_state_and_times() {
        let stored = stored_case();
        let expected_id = stored.case_id;
        let expected_created_at = stored.created_at;
        let expected_updated_at = stored.updated_at;

        let support_case = SupportCase::rehydrate(stored).unwrap();

        assert_eq!(support_case.case_id(), expected_id);
        assert_eq!(support_case.case_type(), CaseType::Request);
        assert_eq!(support_case.status(), CaseStatus::Investigating);
        assert_eq!(support_case.created_at(), expected_created_at);
        assert_eq!(support_case.updated_at(), expected_updated_at);
    }

    #[test]
    fn rehydrate_enforces_domain_invariants() {
        let mut stored = stored_case();
        stored.reporter_id = "  ".to_owned();

        assert_eq!(
            SupportCase::rehydrate(stored).unwrap_err(),
            SupportCaseError::EmptyReporter
        );
    }

    #[test]
    fn created_lifecycle_event_uses_case_identity() {
        let support_case = SupportCase::create(valid_command()).unwrap();
        let event = support_case.created_event();

        assert_eq!(event.event_id().get_version_num(), 7);
        assert_eq!(event.case_id(), support_case.case_id());
        assert_eq!(event.kind(), SupportCaseLifecycleKind::Created);
        assert_eq!(event.previous_status(), None);
        assert_eq!(event.current_status(), CaseStatus::Open);
        assert_eq!(event.event_type(), "support.incident.created");
    }

    #[test]
    fn lifecycle_follows_open_investigating_resolved_closed() {
        let mut support_case = SupportCase::create(valid_command()).unwrap();
        let created_at = support_case.created_at();

        let investigating = support_case
            .transition_to(CaseStatus::Investigating)
            .unwrap();
        assert_eq!(investigating.previous_status(), Some(CaseStatus::Open));
        assert_eq!(investigating.current_status(), CaseStatus::Investigating);
        assert_eq!(investigating.event_type(), "support.incident.investigating");
        assert!(support_case.updated_at() >= created_at);
        assert_eq!(support_case.resolved_at(), None);

        let resolved = support_case.transition_to(CaseStatus::Resolved).unwrap();
        let resolved_at = support_case.resolved_at().unwrap();
        assert_eq!(resolved.previous_status(), Some(CaseStatus::Investigating));
        assert_eq!(resolved.current_status(), CaseStatus::Resolved);
        assert_eq!(resolved.event_type(), "support.incident.resolved");
        assert_eq!(resolved_at, support_case.updated_at());

        let closed = support_case.transition_to(CaseStatus::Closed).unwrap();
        assert_eq!(closed.previous_status(), Some(CaseStatus::Resolved));
        assert_eq!(closed.current_status(), CaseStatus::Closed);
        assert_eq!(closed.event_type(), "support.incident.closed");
        assert_eq!(support_case.resolved_at(), Some(resolved_at));
        assert!(support_case.updated_at() >= resolved_at);
    }

    #[test]
    fn invalid_transition_is_rejected_without_mutation() {
        let mut support_case = SupportCase::create(valid_command()).unwrap();
        let original_updated_at = support_case.updated_at();

        let error = support_case
            .transition_to(CaseStatus::Resolved)
            .unwrap_err();

        assert_eq!(
            error,
            SupportCaseError::InvalidTransition {
                from: CaseStatus::Open,
                to: CaseStatus::Resolved,
            }
        );
        assert_eq!(support_case.status(), CaseStatus::Open);
        assert_eq!(support_case.updated_at(), original_updated_at);
        assert_eq!(support_case.resolved_at(), None);
    }
}
