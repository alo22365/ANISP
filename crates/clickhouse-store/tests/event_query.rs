use std::sync::Arc;

use anisp_clickhouse_store::{ClickHouseEventRepository, ClickHouseStore};
use anisp_config::AppConfig;
use anisp_event::{CreateEventRequest, EventFilters, EventQuery, EventRepository, EventService};
use chrono::{DateTime, Duration, Utc};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires migrated ClickHouse at the configured URL"]
async fn insert_find_and_search_real_clickhouse() {
    let config = AppConfig::from_env().unwrap();
    let store = ClickHouseStore::new(&config.clickhouse);
    let repository = Arc::new(ClickHouseEventRepository::new(&store));
    let service = EventService::new(repository.clone());
    let project = format!("query-test-{}", Uuid::now_v7());
    let first_time = DateTime::parse_from_rfc3339("2026-09-21T10:31:22.123Z")
        .unwrap()
        .to_utc();

    let first = service
        .create_event(request(&project, "service-a", first_time))
        .await
        .unwrap();
    let second = service
        .create_event(request(
            &project,
            "service-a",
            first_time + Duration::milliseconds(1),
        ))
        .await
        .unwrap();
    let third = service
        .create_event(request(
            &project,
            "service-b",
            first_time + Duration::milliseconds(2),
        ))
        .await
        .unwrap();
    service
        .create_event(request("other-project", "service-a", first_time))
        .await
        .unwrap();

    let found = repository
        .find_by_id(first.event_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found, first);
    assert_eq!(found.trace_id(), None);
    assert_eq!(found.metadata()["integration"], true);

    let project_query = EventQuery::new(
        EventFilters {
            project_id: Some(project.clone()),
            ..Default::default()
        },
        None,
        None,
        None,
    )
    .unwrap();
    let project_results = repository.search(&project_query).await.unwrap();
    assert_eq!(
        project_results
            .iter()
            .map(|event| event.event_id())
            .collect::<Vec<_>>(),
        vec![third.event_id(), second.event_id(), first.event_id()]
    );

    let service_query = EventQuery::new(
        EventFilters {
            project_id: Some(project.clone()),
            service: Some("service-a".into()),
            ..Default::default()
        },
        None,
        None,
        None,
    )
    .unwrap();
    let service_results = repository.search(&service_query).await.unwrap();
    assert_eq!(service_results.len(), 2);

    let range_query = EventQuery::new(
        EventFilters {
            project_id: Some(project),
            service: Some("service-a".into()),
            ..Default::default()
        },
        Some(first_time + Duration::milliseconds(1)),
        Some(first_time + Duration::milliseconds(2)),
        None,
    )
    .unwrap();
    let range_results = repository.search(&range_query).await.unwrap();
    assert_eq!(range_results.len(), 1);
    assert_eq!(range_results[0].event_id(), second.event_id());

    let escaped_query = EventQuery::new(
        EventFilters {
            project_id: Some("' OR 1 = 1 --".into()),
            ..Default::default()
        },
        None,
        None,
        None,
    )
    .unwrap();
    assert!(repository.search(&escaped_query).await.unwrap().is_empty());
}

fn request(project_id: &str, service: &str, event_time: DateTime<Utc>) -> CreateEventRequest {
    CreateEventRequest {
        event_time,
        source: "integration".into(),
        event_type: "test.event.created".into(),
        project_id: project_id.into(),
        service: service.into(),
        environment: "test".into(),
        subject_type: "test".into(),
        subject_id: "test-1".into(),
        title: "Test event".into(),
        content: "Real ClickHouse query test".into(),
        trace_id: None,
        correlation_id: None,
        metadata: json!({"integration": true}),
    }
}
