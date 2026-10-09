use std::sync::Arc;

use uuid::Uuid;

use crate::CaseStatus;
use crate::{
    CreateSupportCaseCommand, SupportCase, SupportCaseQuery, SupportCaseRepository,
    SupportServiceError,
};

/// Application service coordinating SupportCase domain creation and persistence.
#[derive(Clone)]
pub struct SupportService {
    repository: Arc<dyn SupportCaseRepository>,
}

impl SupportService {
    pub fn new(repository: Arc<dyn SupportCaseRepository>) -> Self {
        Self { repository }
    }

    pub async fn create_case(
        &self,
        command: CreateSupportCaseCommand,
    ) -> Result<SupportCase, SupportServiceError> {
        let support_case = SupportCase::create(command)?;
        let lifecycle_event = support_case.created_event();
        self.repository
            .insert(&support_case, &lifecycle_event)
            .await?;
        Ok(support_case)
    }

    pub async fn get_case(
        &self,
        case_id: Uuid,
    ) -> Result<Option<SupportCase>, SupportServiceError> {
        self.repository
            .find_by_id(case_id)
            .await
            .map_err(Into::into)
    }

    pub async fn update_case_status(
        &self,
        case_id: Uuid,
        target: CaseStatus,
    ) -> Result<Option<SupportCase>, SupportServiceError> {
        let Some(mut support_case) = self.repository.find_by_id(case_id).await? else {
            return Ok(None);
        };
        let lifecycle_event = support_case.transition_to(target)?;
        self.repository
            .update(&support_case, &lifecycle_event)
            .await?;
        Ok(Some(support_case))
    }

    pub async fn search_cases(
        &self,
        query: &SupportCaseQuery,
    ) -> Result<Vec<SupportCase>, SupportServiceError> {
        self.repository.search(query).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use crate::{CaseType, Priority, SupportRepositoryError, SupportServiceError};

    use super::*;

    #[derive(Clone, Copy)]
    enum Failure {
        Unavailable,
        Timeout,
    }

    struct FakeRepository {
        cases: Mutex<Vec<SupportCase>>,
        events: Mutex<Vec<crate::SupportCaseLifecycleEvent>>,
        failure: Option<Failure>,
    }

    impl FakeRepository {
        fn available() -> Self {
            Self {
                cases: Mutex::new(Vec::new()),
                events: Mutex::new(Vec::new()),
                failure: None,
            }
        }

        fn failing(failure: Failure) -> Self {
            Self {
                cases: Mutex::new(Vec::new()),
                events: Mutex::new(Vec::new()),
                failure: Some(failure),
            }
        }

        fn failure(&self) -> Result<(), SupportRepositoryError> {
            match self.failure {
                Some(Failure::Unavailable) => Err(SupportRepositoryError::unavailable()),
                Some(Failure::Timeout) => Err(SupportRepositoryError::timeout()),
                None => Ok(()),
            }
        }
    }

    #[async_trait]
    impl SupportCaseRepository for FakeRepository {
        async fn insert(
            &self,
            support_case: &SupportCase,
            lifecycle_event: &crate::SupportCaseLifecycleEvent,
        ) -> Result<(), SupportRepositoryError> {
            self.failure()?;
            self.cases.lock().unwrap().push(support_case.clone());
            self.events.lock().unwrap().push(lifecycle_event.clone());
            Ok(())
        }

        async fn update(
            &self,
            support_case: &SupportCase,
            lifecycle_event: &crate::SupportCaseLifecycleEvent,
        ) -> Result<(), SupportRepositoryError> {
            self.failure()?;
            let mut cases = self.cases.lock().unwrap();
            let persisted = cases
                .iter_mut()
                .find(|persisted| persisted.case_id() == support_case.case_id())
                .ok_or_else(SupportRepositoryError::internal)?;
            *persisted = support_case.clone();
            self.events.lock().unwrap().push(lifecycle_event.clone());
            Ok(())
        }

        async fn find_by_id(
            &self,
            case_id: Uuid,
        ) -> Result<Option<SupportCase>, SupportRepositoryError> {
            self.failure()?;
            Ok(self
                .cases
                .lock()
                .unwrap()
                .iter()
                .find(|support_case| support_case.case_id() == case_id)
                .cloned())
        }

        async fn search(
            &self,
            query: &SupportCaseQuery,
        ) -> Result<Vec<SupportCase>, SupportRepositoryError> {
            self.failure()?;
            let filters = query.filters();
            let mut cases: Vec<_> = self
                .cases
                .lock()
                .unwrap()
                .iter()
                .filter(|support_case| {
                    filters
                        .case_type
                        .is_none_or(|value| support_case.case_type() == value)
                        && filters
                            .status
                            .is_none_or(|value| support_case.status() == value)
                        && filters
                            .priority
                            .is_none_or(|value| support_case.priority() == value)
                        && filters
                            .reporter_id
                            .as_deref()
                            .is_none_or(|value| support_case.reporter_id() == value)
                        && filters
                            .assignee_id
                            .as_deref()
                            .is_none_or(|value| support_case.assignee_id() == Some(value))
                        && filters
                            .project_id
                            .as_deref()
                            .is_none_or(|value| support_case.project_id() == Some(value))
                        && filters
                            .service
                            .as_deref()
                            .is_none_or(|value| support_case.service() == Some(value))
                        && filters
                            .environment
                            .as_deref()
                            .is_none_or(|value| support_case.environment() == Some(value))
                        && query
                            .created_from()
                            .is_none_or(|from| support_case.created_at() >= from)
                        && query
                            .created_to()
                            .is_none_or(|to| support_case.created_at() < to)
                })
                .cloned()
                .collect();
            cases.sort_by(|left, right| {
                right
                    .created_at()
                    .cmp(&left.created_at())
                    .then_with(|| right.case_id().cmp(&left.case_id()))
            });
            cases.truncate(usize::from(query.limit()));
            Ok(cases)
        }
    }

    fn valid_command() -> CreateSupportCaseCommand {
        CreateSupportCaseCommand {
            case_type: CaseType::Incident,
            title: "xpa-finance unavailable".to_owned(),
            description: "Users cannot access xpa-finance production service.".to_owned(),
            reporter_id: "user-001".to_owned(),
            assignee_id: None,
            project_id: Some("xpa".to_owned()),
            service: Some("xpa-finance".to_owned()),
            environment: Some("prod".to_owned()),
            priority: Priority::High,
        }
    }

    #[tokio::test]
    async fn create_success() {
        let repository = Arc::new(FakeRepository::available());
        let service = SupportService::new(repository.clone());

        let support_case = service.create_case(valid_command()).await.unwrap();

        assert_eq!(support_case.case_type(), CaseType::Incident);
        assert_eq!(repository.cases.lock().unwrap().as_slice(), &[support_case]);
        let events = repository.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind(), crate::SupportCaseLifecycleKind::Created);
        assert_eq!(events[0].current_status(), CaseStatus::Open);
    }

    #[tokio::test]
    async fn create_repository_unavailable() {
        let service = SupportService::new(Arc::new(FakeRepository::failing(Failure::Unavailable)));

        let error = service.create_case(valid_command()).await.unwrap_err();

        assert!(matches!(
            error,
            SupportServiceError::Repository(SupportRepositoryError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn create_repository_timeout() {
        let service = SupportService::new(Arc::new(FakeRepository::failing(Failure::Timeout)));

        let error = service.create_case(valid_command()).await.unwrap_err();

        assert!(matches!(
            error,
            SupportServiceError::Repository(SupportRepositoryError::Timeout(_))
        ));
    }

    #[tokio::test]
    async fn get_existing() {
        let service = SupportService::new(Arc::new(FakeRepository::available()));
        let created = service.create_case(valid_command()).await.unwrap();

        assert_eq!(
            service.get_case(created.case_id()).await.unwrap(),
            Some(created)
        );
    }

    #[tokio::test]
    async fn get_missing() {
        let service = SupportService::new(Arc::new(FakeRepository::available()));

        assert_eq!(service.get_case(Uuid::now_v7()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn updates_status_and_persists_lifecycle_event() {
        let repository = Arc::new(FakeRepository::available());
        let service = SupportService::new(repository.clone());
        let created = service.create_case(valid_command()).await.unwrap();

        let updated = service
            .update_case_status(created.case_id(), CaseStatus::Investigating)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(updated.status(), CaseStatus::Investigating);
        let events = repository.events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[1].kind(),
            crate::SupportCaseLifecycleKind::InvestigationStarted
        );
    }

    #[tokio::test]
    async fn searches_empty_and_filtered_cases() {
        let repository = Arc::new(FakeRepository::available());
        let service = SupportService::new(repository);
        let incident = service.create_case(valid_command()).await.unwrap();
        let mut request_command = valid_command();
        request_command.case_type = CaseType::Request;
        request_command.priority = Priority::Low;
        request_command.reporter_id = "user-002".to_owned();
        let request = service.create_case(request_command).await.unwrap();

        assert_eq!(
            service
                .search_cases(&SupportCaseQuery::default())
                .await
                .unwrap()
                .len(),
            2
        );
        let query = SupportCaseQuery::new(
            crate::SupportCaseFilters {
                case_type: Some(CaseType::Incident),
                priority: Some(Priority::High),
                reporter_id: Some("user-001".to_owned()),
                ..crate::SupportCaseFilters::default()
            },
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            service.search_cases(&query).await.unwrap(),
            vec![incident.clone()]
        );
        assert_ne!(incident.case_id(), request.case_id());
    }
}
