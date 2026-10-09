use anisp_config::AppConfig;
use anisp_context::SupportContextSyncReader;
use anisp_postgres_store::{PostgresStore, PostgresSupportCaseRepository};
use anisp_support::{
    CaseStatus, CaseType, CreateSupportCaseCommand, OutboxFailureDisposition, Priority,
    StoredSupportCaseLifecycleEvent, SupportCase, SupportCaseFilters, SupportCaseLifecycleEvent,
    SupportCaseLifecycleKind, SupportCaseQuery, SupportCaseRepository,
    SupportOutboxOperationalReader, SupportOutboxRepository, SupportRepositoryError,
};
use chrono::{Duration, Utc};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a migrated PostgreSQL instance"]
async fn insert_and_find_support_case() {
    let config = AppConfig::from_env().expect("valid test configuration");
    let store = PostgresStore::connect(&config.postgres)
        .await
        .expect("connect to PostgreSQL");
    store.ping().await.expect("ping PostgreSQL");
    let repository = PostgresSupportCaseRepository::new(&store);

    let expected = SupportCase::create(CreateSupportCaseCommand {
        case_type: CaseType::Incident,
        title: "xpa-finance unavailable".to_owned(),
        description: "The xpa-finance service is unavailable".to_owned(),
        reporter_id: "user-001".to_owned(),
        assignee_id: None,
        project_id: Some("xpa".to_owned()),
        service: Some("xpa-finance".to_owned()),
        environment: Some("prod".to_owned()),
        priority: Priority::High,
    })
    .expect("create support case");
    let created_event = expected.created_event();

    repository
        .insert(&expected, &created_event)
        .await
        .expect("insert case and outbox event");
    let actual = repository
        .find_by_id(expected.case_id())
        .await
        .expect("find case")
        .expect("inserted case exists");

    assert_eq!(actual, expected);
    assert_eq!(actual.status(), CaseStatus::Open);
    assert_eq!(
        repository
            .pending_count_for_case(expected.case_id())
            .await
            .expect("count pending events"),
        1
    );

    let mut investigating = actual.clone();
    let investigating_event = investigating
        .transition_to(CaseStatus::Investigating)
        .expect("valid transition");
    repository
        .update(&investigating, &investigating_event)
        .await
        .expect("atomic case update and outbox insert");
    assert_eq!(
        repository
            .find_by_id(investigating.case_id())
            .await
            .unwrap(),
        Some(investigating.clone())
    );
    assert_eq!(
        repository
            .pending_count_for_case(investigating.case_id())
            .await
            .unwrap(),
        2
    );

    let rollback_case = SupportCase::create(CreateSupportCaseCommand {
        case_type: CaseType::Request,
        title: "rollback create".to_owned(),
        description: "outbox conflict must roll back case insert".to_owned(),
        reporter_id: "user-rollback".to_owned(),
        assignee_id: None,
        project_id: Some("xpa".to_owned()),
        service: Some("xpa-finance".to_owned()),
        environment: Some("prod".to_owned()),
        priority: Priority::Low,
    })
    .unwrap();
    let duplicate_created = lifecycle_with_id(
        created_event.event_id(),
        &rollback_case,
        SupportCaseLifecycleKind::Created,
        None,
        None,
    );
    assert!(
        repository
            .insert(&rollback_case, &duplicate_created)
            .await
            .is_err()
    );
    assert_eq!(
        repository
            .find_by_id(rollback_case.case_id())
            .await
            .unwrap(),
        None,
        "case insert must roll back when outbox insert fails"
    );

    let mut rolled_back_update = investigating.clone();
    rolled_back_update
        .transition_to(CaseStatus::Resolved)
        .unwrap();
    let duplicate_resolved = lifecycle_with_id(
        created_event.event_id(),
        &rolled_back_update,
        SupportCaseLifecycleKind::Resolved,
        Some(investigating.updated_at()),
        Some(CaseStatus::Investigating),
    );
    assert!(
        repository
            .update(&rolled_back_update, &duplicate_resolved)
            .await
            .is_err()
    );
    let after_failed_update = repository
        .find_by_id(investigating.case_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_failed_update.status(), CaseStatus::Investigating);
    assert_eq!(after_failed_update.resolved_at(), None);
    assert_eq!(
        repository
            .find_by_id(Uuid::now_v7())
            .await
            .expect("query unknown case"),
        None
    );
}

#[tokio::test]
#[ignore = "requires a migrated PostgreSQL instance"]
async fn search_and_optimistic_conflict_real_postgres() {
    let config = AppConfig::from_env().expect("valid test configuration");
    let store = PostgresStore::connect(&config.postgres)
        .await
        .expect("connect to PostgreSQL");
    let repository = PostgresSupportCaseRepository::new(&store);
    let token = Uuid::now_v7().to_string();
    let project = format!("search-{token}");

    let first = SupportCase::create(CreateSupportCaseCommand {
        case_type: CaseType::Incident,
        title: "first search case".to_owned(),
        description: "first".to_owned(),
        reporter_id: format!("reporter-a-{token}"),
        assignee_id: Some(format!("assignee-a-{token}")),
        project_id: Some(project.clone()),
        service: Some("service-a".to_owned()),
        environment: Some("prod".to_owned()),
        priority: Priority::Critical,
    })
    .unwrap();
    repository
        .insert(&first, &first.created_event())
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    let second = SupportCase::create(CreateSupportCaseCommand {
        case_type: CaseType::Request,
        title: "second search case".to_owned(),
        description: "second".to_owned(),
        reporter_id: format!("reporter-b-{token}"),
        assignee_id: Some(format!("assignee-b-{token}")),
        project_id: Some(project.clone()),
        service: Some("service-b".to_owned()),
        environment: Some("staging".to_owned()),
        priority: Priority::Low,
    })
    .unwrap();
    repository
        .insert(&second, &second.created_event())
        .await
        .unwrap();

    let by_project = SupportCaseQuery::new(
        SupportCaseFilters {
            project_id: Some(project.clone()),
            ..SupportCaseFilters::default()
        },
        None,
        None,
        None,
    )
    .unwrap();
    let results = repository.search(&by_project).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].case_id(), second.case_id());
    assert_eq!(results[1].case_id(), first.case_id());

    for filters in [
        SupportCaseFilters {
            case_type: Some(CaseType::Incident),
            project_id: Some(project.clone()),
            ..SupportCaseFilters::default()
        },
        SupportCaseFilters {
            priority: Some(Priority::Critical),
            reporter_id: Some(format!("reporter-a-{token}")),
            assignee_id: Some(format!("assignee-a-{token}")),
            project_id: Some(project.clone()),
            service: Some("service-a".to_owned()),
            environment: Some("prod".to_owned()),
            ..SupportCaseFilters::default()
        },
    ] {
        let results = repository
            .search(&SupportCaseQuery::new(filters, None, None, None).unwrap())
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].case_id(), first.case_id());
    }

    let range = SupportCaseQuery::new(
        SupportCaseFilters {
            project_id: Some(project.clone()),
            ..SupportCaseFilters::default()
        },
        Some(first.created_at()),
        Some(second.created_at()),
        None,
    )
    .unwrap();
    assert_eq!(
        repository.search(&range).await.unwrap()[0].case_id(),
        first.case_id()
    );
    let limited = SupportCaseQuery::new(
        SupportCaseFilters {
            project_id: Some(project.clone()),
            ..SupportCaseFilters::default()
        },
        None,
        None,
        Some(1),
    )
    .unwrap();
    assert_eq!(repository.search(&limited).await.unwrap().len(), 1);

    let mut winner = repository
        .find_by_id(first.case_id())
        .await
        .unwrap()
        .unwrap();
    let mut stale = winner.clone();
    let winner_event = winner.transition_to(CaseStatus::Investigating).unwrap();
    repository.update(&winner, &winner_event).await.unwrap();
    let stale_event = stale.transition_to(CaseStatus::Investigating).unwrap();
    assert!(matches!(
        repository.update(&stale, &stale_event).await,
        Err(SupportRepositoryError::Conflict(_))
    ));

    let status_query = SupportCaseQuery::new(
        SupportCaseFilters {
            status: Some(CaseStatus::Investigating),
            project_id: Some(project),
            ..SupportCaseFilters::default()
        },
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(repository.search(&status_query).await.unwrap().len(), 1);
    assert!(
        repository
            .pending_count_for_case(first.case_id())
            .await
            .unwrap()
            >= 2
    );
}

#[tokio::test]
#[ignore = "requires all PostgreSQL migrations"]
async fn outbox_claim_lease_backoff_and_operational_status_real_postgres() {
    let config = AppConfig::from_env().expect("valid test configuration");
    let store = PostgresStore::connect(&config.postgres)
        .await
        .expect("connect to PostgreSQL");
    let repository = PostgresSupportCaseRepository::new(&store);
    let support_case = SupportCase::create(CreateSupportCaseCommand {
        case_type: CaseType::Incident,
        title: "outbox claim integration".to_owned(),
        description: "claim lease and retry verification".to_owned(),
        reporter_id: format!("outbox-test-{}", Uuid::now_v7()),
        assignee_id: None,
        project_id: Some("outbox-integration".to_owned()),
        service: Some("publisher".to_owned()),
        environment: Some("test".to_owned()),
        priority: Priority::High,
    })
    .unwrap();
    let lifecycle = support_case.created_event();
    let target_id = lifecycle.event_id();
    repository.insert(&support_case, &lifecycle).await.unwrap();

    let now = Utc::now() + Duration::seconds(1);
    let claimed_a = repository
        .claim_pending(10_000, "integration-a", now, now - Duration::seconds(30))
        .await
        .unwrap();
    assert!(
        claimed_a
            .iter()
            .any(|item| item.event().event_id() == target_id)
    );
    release_other_claims(&repository, claimed_a, target_id, "integration-a", now).await;

    let claimed_b = repository
        .claim_pending(10_000, "integration-b", now, now - Duration::seconds(30))
        .await
        .unwrap();
    assert!(
        claimed_b
            .iter()
            .all(|item| item.event().event_id() != target_id)
    );
    release_all_claims(&repository, claimed_b, "integration-b", now).await;

    let reclaimed_at = now + Duration::seconds(31);
    let reclaimed = repository
        .claim_pending(
            10_000,
            "integration-b",
            reclaimed_at,
            reclaimed_at - Duration::seconds(30),
        )
        .await
        .unwrap();
    assert!(
        reclaimed
            .iter()
            .any(|item| item.event().event_id() == target_id)
    );
    release_other_claims(
        &repository,
        reclaimed,
        target_id,
        "integration-b",
        reclaimed_at,
    )
    .await;
    assert!(matches!(
        repository
            .mark_published(target_id, "integration-a", reclaimed_at)
            .await,
        Err(SupportRepositoryError::Conflict(_))
    ));

    let retry_at = reclaimed_at + Duration::seconds(8);
    repository
        .record_failure(
            target_id,
            "integration-b",
            reclaimed_at,
            OutboxFailureDisposition::RetryAt(retry_at),
        )
        .await
        .unwrap();
    let too_early = repository
        .claim_pending(
            10_000,
            "integration-c",
            retry_at - Duration::milliseconds(1),
            retry_at,
        )
        .await
        .unwrap();
    assert!(
        too_early
            .iter()
            .all(|item| item.event().event_id() != target_id)
    );
    release_all_claims(
        &repository,
        too_early,
        "integration-c",
        retry_at - Duration::milliseconds(1),
    )
    .await;

    let retry_claim = repository
        .claim_pending(10_000, "integration-c", retry_at, retry_at)
        .await
        .unwrap();
    assert!(
        retry_claim
            .iter()
            .any(|item| item.event().event_id() == target_id)
    );
    release_other_claims(
        &repository,
        retry_claim,
        target_id,
        "integration-c",
        retry_at,
    )
    .await;
    repository
        .mark_published(target_id, "integration-c", retry_at)
        .await
        .unwrap();

    let status = repository.operational_status(retry_at).await.unwrap();
    assert!(status.last_publish_success_at().is_some());
}

async fn release_other_claims(
    repository: &PostgresSupportCaseRepository,
    claims: Vec<anisp_support::ClaimedSupportLifecycleEvent>,
    target_id: Uuid,
    owner: &str,
    now: chrono::DateTime<Utc>,
) {
    for claim in claims {
        if claim.event().event_id() != target_id {
            repository
                .record_failure(
                    claim.event().event_id(),
                    owner,
                    now,
                    OutboxFailureDisposition::RetryAt(now),
                )
                .await
                .unwrap();
        }
    }
}

async fn release_all_claims(
    repository: &PostgresSupportCaseRepository,
    claims: Vec<anisp_support::ClaimedSupportLifecycleEvent>,
    owner: &str,
    now: chrono::DateTime<Utc>,
) {
    for claim in claims {
        repository
            .record_failure(
                claim.event().event_id(),
                owner,
                now,
                OutboxFailureDisposition::RetryAt(now),
            )
            .await
            .unwrap();
    }
}

fn lifecycle_with_id(
    event_id: Uuid,
    support_case: &SupportCase,
    kind: SupportCaseLifecycleKind,
    previous_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    previous_status: Option<CaseStatus>,
) -> SupportCaseLifecycleEvent {
    SupportCaseLifecycleEvent::rehydrate(StoredSupportCaseLifecycleEvent {
        event_id,
        case_id: support_case.case_id(),
        case_type: support_case.case_type(),
        kind,
        occurred_at: support_case.updated_at(),
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
    })
}
