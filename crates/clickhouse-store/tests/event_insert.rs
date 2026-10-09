use std::sync::Arc;

use anisp_clickhouse_store::{ClickHouseEventRepository, ClickHouseStore};
use anisp_config::AppConfig;
use anisp_event::{CreateEventRequest, EventService};
use chrono::{TimeZone, Utc};
use serde_json::json;

#[tokio::test]
#[ignore = "requires migrated ClickHouse at the configured URL"]
async fn inserts_context_event_into_real_clickhouse() {
    let config = AppConfig::from_env().unwrap();
    let store = ClickHouseStore::new(&config.clickhouse);
    let repository = ClickHouseEventRepository::new(&store);
    let service = EventService::new(Arc::new(repository));

    let event = service
        .create_event(CreateEventRequest {
            event_time: Utc.with_ymd_and_hms(2026, 9, 21, 10, 31, 22).unwrap(),
            source: "kubernetes".to_owned(),
            event_type: "k8s.pod.restart".to_owned(),
            project_id: "integration-test".to_owned(),
            service: "anisp".to_owned(),
            environment: "test".to_owned(),
            subject_type: "pod".to_owned(),
            subject_id: "anisp-integration-test".to_owned(),
            title: "Integration test event".to_owned(),
            content: "Created by the ignored ClickHouse integration test.".to_owned(),
            trace_id: None,
            correlation_id: None,
            metadata: json!({ "test": true }),
        })
        .await
        .unwrap();

    assert_eq!(event.event_id().get_version_num(), 7);
}
