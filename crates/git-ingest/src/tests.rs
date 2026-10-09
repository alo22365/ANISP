use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anisp_event::{
    ContextEvent, EventQuery, EventRepository, EventService, MAX_EVENT_METADATA_BYTES,
    RepositoryError,
};
use anisp_git_context::{
    ChangeType, GitCommit, GitCommitInput, GitFileChange, GitRepository, ObservedGitCommit,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::*;

fn observation(
    branch: Option<&str>,
    message: &str,
    changes: Vec<GitFileChange>,
) -> ObservedGitCommit {
    let repository = GitRepository::new(
        "repo-xpa",
        "finance",
        Some("https://do-not-copy.invalid/repo".into()),
        Some("xpa".into()),
        Some("xpa-finance".into()),
    )
    .unwrap();
    let commit = GitCommit::new(GitCommitInput {
        repository_id: "repo-xpa".into(),
        commit_sha: "a".repeat(40),
        parent_shas: vec!["b".repeat(40), "c".repeat(40)],
        author_name: "Author".into(),
        author_email: Some("author@example.com".into()),
        message: message.into(),
        committed_at: "2026-10-06T08:00:00Z".parse().unwrap(),
        changes,
    })
    .unwrap();
    ObservedGitCommit::new(repository, commit, branch.map(str::to_owned), Utc::now()).unwrap()
}
fn one_change() -> Vec<GitFileChange> {
    vec![
        GitFileChange::new(
            "after.rs",
            Some("before.rs".into()),
            ChangeType::Renamed,
            Some(2),
            Some(1),
        )
        .unwrap(),
    ]
}

struct Delivery {
    projection: GitCommitProjection,
    published: bool,
    created_at: DateTime<Utc>,
    published_at: Option<DateTime<Utc>>,
    attempts: u32,
    owner: Option<Uuid>,
    claimed_at: Option<DateTime<Utc>>,
    next: DateTime<Utc>,
    exhausted: bool,
    corrupt: bool,
}
#[derive(Default)]
struct FakeOutbox {
    rows: Mutex<HashMap<(String, String), Delivery>>,
    unavailable: AtomicBool,
}
impl FakeOutbox {
    fn check(&self) -> Result<(), GitOutboxError> {
        if self.unavailable.load(Ordering::SeqCst) {
            Err(GitOutboxError::Unavailable)
        } else {
            Ok(())
        }
    }
}
#[async_trait]
impl GitOutboxRepository for FakeOutbox {
    async fn find_ingested_event_id(
        &self,
        repository_id: &str,
        commit_sha: &str,
    ) -> Result<Option<Uuid>, GitOutboxError> {
        self.check()?;
        Ok(self
            .rows
            .lock()
            .unwrap()
            .get(&(repository_id.into(), commit_sha.into()))
            .map(|row| row.projection.event_id()))
    }
    async fn insert_if_absent(
        &self,
        projection: &GitCommitProjection,
    ) -> Result<GitIngestOutcome, GitOutboxError> {
        self.check()?;
        let mut rows = self.rows.lock().unwrap();
        let key = (
            projection.repository_id().into(),
            projection.commit_sha().into(),
        );
        if let Some(row) = rows.get(&key) {
            return Ok(GitIngestOutcome::AlreadyIngested(row.projection.event_id()));
        }
        rows.insert(
            key,
            Delivery {
                projection: projection.clone(),
                published: false,
                created_at: Utc::now(),
                published_at: None,
                attempts: 0,
                owner: None,
                claimed_at: None,
                next: Utc::now(),
                exhausted: false,
                corrupt: false,
            },
        );
        Ok(GitIngestOutcome::Accepted(projection.event_id()))
    }
    async fn claim_pending(
        &self,
        limit: u32,
        instance_id: Uuid,
        now: DateTime<Utc>,
        expired: DateTime<Utc>,
        max_attempts: u32,
    ) -> Result<Vec<ClaimedGitEvent>, GitOutboxError> {
        self.check()?;
        let mut claims = Vec::new();
        for row in self.rows.lock().unwrap().values_mut() {
            if !row.published
                && row.attempts >= max_attempts
                && row.claimed_at.is_none_or(|time| time <= expired)
            {
                row.exhausted = true;
                row.owner = None;
                row.claimed_at = None;
            }
            if claims.len() >= limit as usize {
                break;
            }
            if row.published
                || row.exhausted
                || row.next > now
                || row.claimed_at.is_some_and(|time| time > expired)
            {
                continue;
            }
            row.owner = Some(instance_id);
            row.claimed_at = Some(now);
            row.attempts += 1;
            claims.push(ClaimedGitEvent {
                event_id: row.projection.event_id(),
                projection: if row.corrupt {
                    Err(GitOutboxError::Decode)
                } else {
                    Ok(row.projection.clone())
                },
                attempt_count: row.attempts,
            });
        }
        Ok(claims)
    }
    async fn mark_published(
        &self,
        claim: &ClaimedGitEvent,
        owner: Uuid,
        published_at: DateTime<Utc>,
    ) -> Result<(), GitOutboxError> {
        self.check()?;
        let mut rows = self.rows.lock().unwrap();
        let row = rows
            .values_mut()
            .find(|row| row.projection.event_id() == claim.event_id)
            .unwrap();
        if row.owner != Some(owner) || row.attempts != claim.attempt_count || row.published {
            return Err(GitOutboxError::Conflict);
        }
        row.published = true;
        row.published_at = Some(published_at);
        row.owner = None;
        row.claimed_at = None;
        Ok(())
    }
    async fn record_failure(
        &self,
        claim: &ClaimedGitEvent,
        owner: Uuid,
        disposition: GitFailureDisposition,
    ) -> Result<(), GitOutboxError> {
        self.check()?;
        let mut rows = self.rows.lock().unwrap();
        let row = rows
            .values_mut()
            .find(|row| row.projection.event_id() == claim.event_id)
            .unwrap();
        if row.owner != Some(owner) || row.attempts != claim.attempt_count || row.published {
            return Err(GitOutboxError::Conflict);
        }
        row.owner = None;
        row.claimed_at = None;
        match disposition {
            GitFailureDisposition::RetryAt(time) => row.next = time,
            GitFailureDisposition::Exhausted => row.exhausted = true,
        }
        Ok(())
    }
    async fn operational_status(&self) -> Result<GitOutboxStatus, GitOutboxError> {
        self.check()?;
        let rows = self.rows.lock().unwrap();
        Ok(GitOutboxStatus {
            pending_events: rows
                .values()
                .filter(|row| !row.published && !row.exhausted)
                .count() as u64,
            exhausted_events: rows
                .values()
                .filter(|row| !row.published && row.exhausted)
                .count() as u64,
        })
    }
}

#[async_trait]
impl GitOutboxOperationalReader for FakeOutbox {
    async fn read_operational_status(
        &self,
        now: DateTime<Utc>,
        expired: DateTime<Utc>,
    ) -> Result<GitOutboxOperationalStatus, GitOutboxError> {
        self.check()?;
        let rows = self.rows.lock().unwrap();
        let pending = rows
            .values()
            .filter(|r| !r.published && !r.exhausted)
            .collect::<Vec<_>>();
        Ok(GitOutboxOperationalStatus {
            pending_events: pending.len() as u64,
            exhausted_events: rows
                .values()
                .filter(|r| !r.published && r.exhausted)
                .count() as u64,
            active_claims: pending
                .iter()
                .filter(|r| r.owner.is_some() && r.claimed_at.is_some_and(|t| t > expired))
                .count() as u64,
            oldest_pending_age_seconds: pending
                .iter()
                .map(|r| r.created_at)
                .min()
                .map(|t| (now - t).num_seconds().max(0) as u64),
            last_publish_success_at: rows.values().filter_map(|r| r.published_at).max(),
        })
    }
}

#[derive(Default)]
struct FakeEvents {
    inserted: Mutex<Vec<ContextEvent>>,
    unavailable: AtomicBool,
    last_failure_at: Mutex<Option<DateTime<Utc>>>,
}
#[async_trait]
impl EventRepository for FakeEvents {
    async fn insert(&self, event: &ContextEvent) -> Result<(), RepositoryError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(RepositoryError::unavailable());
        }
        self.inserted.lock().unwrap().push(event.clone());
        Ok(())
    }
    async fn find_by_id(&self, id: Uuid) -> Result<Option<ContextEvent>, RepositoryError> {
        if self.unavailable.load(Ordering::SeqCst) {
            *self.last_failure_at.lock().unwrap() = Some(Utc::now());
            return Err(RepositoryError::unavailable());
        }
        Ok(self
            .inserted
            .lock()
            .unwrap()
            .iter()
            .find(|event| event.event_id() == id)
            .cloned())
    }
    async fn search(&self, _: &EventQuery) -> Result<Vec<ContextEvent>, RepositoryError> {
        Ok(self.inserted.lock().unwrap().clone())
    }
}

#[test]
fn projection_mapping_has_complete_metadata_without_remote_or_diff() {
    let observed = observation(Some("main"), "First line\n\nDetails", one_change());
    let id = Uuid::now_v7();
    let projection = GitCommitProjection::from_observation(&observed, id).unwrap();
    let event = projection.prepared_event();
    assert_eq!(
        (
            event.source.as_str(),
            event.event_type.as_str(),
            event.subject_type.as_str()
        ),
        ("git", "git.commit.created", "commit")
    );
    assert_eq!(event.event_id, id);
    assert_eq!(event.event_time, observed.commit().committed_at());
    assert_eq!(event.subject_id, "a".repeat(40));
    assert_eq!(event.title, "First line");
    assert_eq!(event.content, "First line\n\nDetails");
    assert_eq!(
        (
            event.project_id.as_str(),
            event.service.as_str(),
            event.environment.as_str()
        ),
        ("xpa", "xpa-finance", "unassigned")
    );
    assert_eq!(event.correlation_id, None);
    let metadata = event.metadata;
    assert_eq!(metadata["repository_id"], "repo-xpa");
    assert_eq!(metadata["repository_name"], "finance");
    assert_eq!(metadata["branch"], "main");
    assert_eq!(metadata["parent_shas"][0], "b".repeat(40));
    assert_eq!(metadata["parent_shas"][1], "c".repeat(40));
    assert_eq!(metadata["author_name"], "Author");
    assert_eq!(metadata["author_email"], "author@example.com");
    assert_eq!(
        metadata["observed_at"],
        serde_json::json!(observed.observed_at())
    );
    assert_eq!(metadata["changed_file_count"], 1);
    assert_eq!(
        (
            metadata["additions"].as_u64(),
            metadata["deletions"].as_u64()
        ),
        (Some(2), Some(1))
    );
    assert_eq!(metadata["changed_files"][0]["change_type"], "renamed");
    assert_eq!(metadata["changed_files"][0]["old_path"], "before.rs");
    assert_eq!(metadata["changed_files"][0]["path"], "after.rs");
    assert_eq!(metadata["changes_truncated"], false);
    for field in ["remote_url", "diff", "patch", "source_content"] {
        assert!(metadata.get(field).is_none());
    }
}

#[test]
fn empty_message_uses_fallback_and_detached_branch_is_null() {
    let event =
        GitCommitProjection::from_observation(&observation(None, "", vec![]), Uuid::now_v7())
            .unwrap()
            .prepared_event();
    assert_eq!(event.title, "Git commit aaaaaaaaaaaa");
    assert_eq!(event.content, event.title);
    assert!(event.metadata["branch"].is_null());
    assert_eq!(event.metadata["additions"], 0);
}

#[test]
fn binary_statistics_are_unknown_in_file_and_aggregate() {
    let mut changes = one_change();
    changes.push(GitFileChange::new("binary.bin", None, ChangeType::Added, None, None).unwrap());
    let projection = GitCommitProjection::from_observation(
        &observation(None, "Binary", changes),
        Uuid::now_v7(),
    )
    .unwrap();
    assert!(projection.metadata()["additions"].is_null());
    assert!(projection.metadata()["deletions"].is_null());
    assert!(projection.metadata()["changed_files"][1]["additions"].is_null());
}

#[test]
fn large_file_list_is_explicitly_truncated_with_true_total_and_size_budget() {
    let changes = (0..3000)
        .map(|index| {
            GitFileChange::new(
                format!("{index}-{}", "中".repeat(80)),
                None,
                ChangeType::Added,
                Some(1),
                Some(0),
            )
            .unwrap()
        })
        .collect();
    let projection =
        GitCommitProjection::from_observation(&observation(None, "Large", changes), Uuid::now_v7())
            .unwrap();
    let metadata = projection.metadata();
    assert_eq!(metadata["changed_file_count"], 3000);
    assert_eq!(metadata["additions"], 3000);
    assert_eq!(metadata["changes_truncated"], true);
    let files = metadata["changed_files"].as_array().unwrap();
    assert!(!files.is_empty() && files.len() < 3000);
    assert!(serde_json::to_vec(metadata).unwrap().len() <= MAX_EVENT_METADATA_BYTES);
}

#[test]
fn mandatory_metadata_cannot_be_silently_truncated() {
    let original = observation(None, "Large", vec![]);
    let repository = GitRepository::new(
        "repo-xpa",
        "x".repeat(MAX_EVENT_METADATA_BYTES),
        None,
        None,
        None,
    )
    .unwrap();
    let observed =
        ObservedGitCommit::new(repository, original.commit().clone(), None, Utc::now()).unwrap();
    assert!(matches!(
        GitCommitProjection::from_observation(&observed, Uuid::now_v7()),
        Err(GitProjectionError::MetadataTooLarge)
    ));
}

#[tokio::test]
async fn first_and_duplicate_ingest_preserve_id_and_first_branch() {
    let outbox = Arc::new(FakeOutbox::default());
    let service = GitIngestService::new(outbox.clone());
    let first = service
        .ingest(observation(Some("main"), "First", one_change()))
        .await
        .unwrap();
    assert!(matches!(first, GitIngestOutcome::Accepted(_)));
    assert_eq!(first.event_id().get_version_num(), 7);
    let second = service
        .ingest(observation(Some("other"), "Different observation", vec![]))
        .await
        .unwrap();
    assert_eq!(second, GitIngestOutcome::AlreadyIngested(first.event_id()));
    let rows = outbox.rows.lock().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.values().next().unwrap().projection.metadata()["branch"],
        "main"
    );
}

#[tokio::test]
async fn concurrent_ingestion_has_one_winner() {
    let outbox = Arc::new(FakeOutbox::default());
    let service = GitIngestService::new(outbox.clone());
    let (left, right) = tokio::join!(
        service.ingest(observation(None, "One", vec![])),
        service.ingest(observation(None, "One", vec![]))
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, GitIngestOutcome::Accepted(_)))
            .count(),
        1
    );
    assert_eq!(outcomes[0].event_id(), outcomes[1].event_id());
    assert_eq!(outbox.rows.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn invalid_projection_never_enters_outbox_and_repository_errors_are_stable() {
    let outbox = Arc::new(FakeOutbox::default());
    let service = GitIngestService::new(outbox.clone());
    for message in ["x".repeat(257), format!("Title\n{}", "x".repeat(64 * 1024))] {
        assert!(matches!(
            service.ingest(observation(None, &message, vec![])).await,
            Err(GitIngestError::Projection(_))
        ));
    }
    assert!(outbox.rows.lock().unwrap().is_empty());
    outbox.unavailable.store(true, Ordering::SeqCst);
    assert!(matches!(
        service.ingest(observation(None, "Good", vec![])).await,
        Err(GitIngestError::Repository(GitOutboxError::Unavailable))
    ));
}

async fn publication_fixture(
    max_attempts: u32,
) -> (
    Arc<FakeOutbox>,
    Arc<FakeEvents>,
    GitOutboxPublisher,
    DateTime<Utc>,
    Uuid,
) {
    let outbox = Arc::new(FakeOutbox::default());
    let events = Arc::new(FakeEvents::default());
    let id = GitIngestService::new(outbox.clone())
        .ingest(observation(Some("main"), "Commit", one_change()))
        .await
        .unwrap()
        .event_id();
    let options = GitPublisherOptions::new(
        10,
        Duration::from_secs(1),
        Duration::from_secs(30),
        max_attempts,
    )
    .unwrap();
    let publisher =
        GitOutboxPublisher::new(outbox.clone(), EventService::new(events.clone()), options);
    (outbox, events, publisher, Utc::now(), id)
}

#[tokio::test]
async fn publisher_success_and_repeat_do_not_insert_a_second_event() {
    let (outbox, events, publisher, now, id) = publication_fixture(10).await;
    assert_eq!(
        publisher
            .publish_batch_at(now)
            .await
            .unwrap()
            .published_count,
        1
    );
    assert_eq!(publisher.publish_batch_at(now).await.unwrap().batch_size, 0);
    assert_eq!(outbox.operational_status().await.unwrap().pending_events, 0);
    assert_eq!(events.inserted.lock().unwrap()[0].event_id(), id);
    // Crash after insert but before PostgreSQL acknowledgement.
    outbox
        .rows
        .lock()
        .unwrap()
        .values_mut()
        .next()
        .unwrap()
        .published = false;
    assert_eq!(
        publisher
            .publish_batch_at(now)
            .await
            .unwrap()
            .published_count,
        1
    );
    assert_eq!(events.inserted.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn unavailable_then_recovery_obeys_backoff_without_sleeping() {
    let (outbox, events, publisher, now, _) = publication_fixture(10).await;
    events.unavailable.store(true, Ordering::SeqCst);
    assert_eq!(
        publisher.publish_batch_at(now).await.unwrap().failed_count,
        1
    );
    assert_eq!(outbox.operational_status().await.unwrap().pending_events, 1);
    assert_eq!(publisher.publish_batch_at(now).await.unwrap().batch_size, 0);
    events.unavailable.store(false, Ordering::SeqCst);
    assert_eq!(
        publisher
            .publish_batch_at(now + chrono::Duration::seconds(1))
            .await
            .unwrap()
            .published_count,
        1
    );
    assert_eq!(outbox.operational_status().await.unwrap().pending_events, 0);
}

#[tokio::test]
async fn exhausted_rows_are_retained_and_not_reclaimed() {
    let (outbox, events, publisher, now, _) = publication_fixture(1).await;
    events.unavailable.store(true, Ordering::SeqCst);
    assert_eq!(
        publisher
            .publish_batch_at(now)
            .await
            .unwrap()
            .exhausted_count,
        1
    );
    assert_eq!(
        publisher
            .publish_batch_at(now + chrono::Duration::hours(1))
            .await
            .unwrap()
            .batch_size,
        0
    );
    assert_eq!(
        outbox.operational_status().await.unwrap(),
        GitOutboxStatus {
            pending_events: 0,
            exhausted_events: 1
        }
    );
    assert_eq!(outbox.rows.lock().unwrap().len(), 1);
    let status = GitOutboxStatusService::new(outbox, Duration::from_secs(30))
        .unwrap()
        .status_at(now)
        .await
        .unwrap();
    assert_eq!(status.exhausted_events, 1);
    assert_eq!(status.pending_events, 0);
    assert_eq!(status.active_claims, 0);
    assert_eq!(status.oldest_pending_age_seconds, None);
}

#[tokio::test]
async fn repeated_failures_exhaust_and_operational_service_preserves_the_ledger() {
    let (outbox, events, publisher, now, event_id) = publication_fixture(2).await;
    let service = GitOutboxStatusService::new(outbox.clone(), Duration::from_secs(30)).unwrap();
    events.unavailable.store(true, Ordering::SeqCst);
    assert_eq!(
        publisher.publish_batch_at(now).await.unwrap().failed_count,
        1
    );
    assert_eq!(service.status_at(now).await.unwrap().pending_events, 1);
    assert_eq!(publisher.publish_batch_at(now).await.unwrap().batch_size, 0);
    let retry = now + chrono::Duration::seconds(1);
    assert_eq!(
        publisher
            .publish_batch_at(retry)
            .await
            .unwrap()
            .exhausted_count,
        1
    );
    let status = service.status_at(retry).await.unwrap();
    assert_eq!(
        (
            status.pending_events,
            status.exhausted_events,
            status.active_claims
        ),
        (0, 1, 0)
    );
    events.unavailable.store(false, Ordering::SeqCst);
    assert_eq!(
        publisher
            .publish_batch_at(now + chrono::Duration::days(1))
            .await
            .unwrap()
            .batch_size,
        0
    );
    let duplicate = GitIngestService::new(outbox.clone())
        .ingest(observation(Some("other"), "duplicate", vec![]))
        .await
        .unwrap();
    assert_eq!(duplicate, GitIngestOutcome::AlreadyIngested(event_id));
    assert_eq!(outbox.rows.lock().unwrap().len(), 1);
    outbox.unavailable.store(true, Ordering::SeqCst);
    assert_eq!(
        service.status_at(now).await.unwrap_err(),
        GitOutboxError::Unavailable
    );
}

#[tokio::test]
async fn corrupt_claim_is_recorded_and_exhausted_not_a_batch_poison() {
    let (outbox, events, publisher, now, _) = publication_fixture(1).await;
    outbox
        .rows
        .lock()
        .unwrap()
        .values_mut()
        .next()
        .unwrap()
        .corrupt = true;
    let result = publisher.publish_batch_at(now).await.unwrap();
    assert_eq!((result.failed_count, result.exhausted_count), (1, 1));
    assert!(events.inserted.lock().unwrap().is_empty());
}

#[tokio::test]
async fn two_publishers_and_lease_expiry_preserve_claim_ownership() {
    let (outbox, events, first, now, _) = publication_fixture(10).await;
    let second = GitOutboxPublisher::new(
        outbox.clone(),
        EventService::new(events.clone()),
        GitPublisherOptions::default(),
    );
    assert_ne!(first.instance_id(), second.instance_id());
    let old = outbox
        .claim_pending(
            1,
            first.instance_id(),
            now,
            now - chrono::Duration::seconds(30),
            10,
        )
        .await
        .unwrap()
        .remove(0);
    assert!(
        outbox
            .claim_pending(
                1,
                second.instance_id(),
                now,
                now - chrono::Duration::seconds(30),
                10,
            )
            .await
            .unwrap()
            .is_empty()
    );
    let future = now + chrono::Duration::seconds(31);
    let replacement = outbox
        .claim_pending(
            1,
            first.instance_id(),
            future,
            now + chrono::Duration::seconds(1),
            10,
        )
        .await
        .unwrap()
        .remove(0);
    assert_eq!(
        outbox
            .mark_published(&old, first.instance_id(), future)
            .await,
        Err(GitOutboxError::Conflict)
    );
    outbox
        .record_failure(
            &replacement,
            first.instance_id(),
            GitFailureDisposition::RetryAt(future),
        )
        .await
        .unwrap();
    let (left, right) = tokio::join!(
        first.publish_batch_at(future),
        second.publish_batch_at(future)
    );
    assert_eq!(
        left.unwrap().published_count + right.unwrap().published_count,
        1
    );
    assert_eq!(events.inserted.lock().unwrap().len(), 1);
}

#[test]
fn retry_backoff_and_options_are_bounded() {
    assert_eq!(
        (1..=8)
            .map(|attempt| retry_backoff(attempt).num_seconds())
            .collect::<Vec<_>>(),
        vec![1, 2, 4, 8, 16, 32, 60, 60]
    );
    assert_eq!(retry_backoff(u32::MAX).num_seconds(), 60);
    assert!(
        GitPublisherOptions::new(501, Duration::from_secs(1), Duration::from_secs(30), 10).is_err()
    );
    assert!(GitPublisherOptions::new(1, Duration::ZERO, Duration::from_secs(30), 10).is_err());
    assert!(GitPublisherOptions::new(1, Duration::from_secs(1), Duration::ZERO, 10).is_err());
    assert!(
        GitPublisherOptions::new(1, Duration::from_secs(1), Duration::from_secs(30), 0).is_err()
    );
}

#[test]
fn unassigned_labels_and_persisted_payload_validation() {
    let original = observation(None, "Commit", one_change());
    let repository = GitRepository::new("repo-xpa", "finance", None, None, None).unwrap();
    let observed = ObservedGitCommit::new(
        repository,
        original.commit().clone(),
        None,
        original.observed_at(),
    )
    .unwrap();
    let projection = GitCommitProjection::from_observation(&observed, Uuid::now_v7()).unwrap();
    assert_eq!(projection.prepared_event().project_id, "unassigned");
    assert_eq!(projection.prepared_event().service, "unassigned");
    let value = serde_json::to_value(&projection).unwrap();
    for (field, replacement) in [
        ("parent_shas", serde_json::json!(["not-a-sha"])),
        ("changed_files", serde_json::json!([])),
        ("observed_at", serde_json::json!("not-a-time")),
    ] {
        let mut corrupt = value.clone();
        corrupt["metadata"][field] = replacement;
        let decoded: GitCommitProjection = serde_json::from_value(corrupt).unwrap();
        assert!(matches!(
            decoded.validate(),
            Err(GitProjectionError::InvalidMetadata)
        ));
    }
}

#[tokio::test]
async fn crash_on_final_attempt_exhausts_after_lease_without_unbounded_reclaim() {
    let (outbox, _, publisher, now, _) = publication_fixture(1).await;
    outbox
        .claim_pending(
            1,
            publisher.instance_id(),
            now,
            now - chrono::Duration::seconds(30),
            1,
        )
        .await
        .unwrap();
    assert_eq!(outbox.operational_status().await.unwrap().pending_events, 1);
    assert_eq!(
        publisher
            .publish_batch_at(now + chrono::Duration::seconds(31))
            .await
            .unwrap()
            .batch_size,
        0
    );
    assert_eq!(
        outbox.operational_status().await.unwrap().exhausted_events,
        1
    );
}

#[tokio::test]
async fn real_clock_backoff_starts_after_the_individual_failure() {
    let (outbox, events, publisher, _, _) = publication_fixture(10).await;
    events.unavailable.store(true, Ordering::SeqCst);
    assert_eq!(publisher.publish_batch().await.unwrap().failed_count, 1);
    let failed_at = events.last_failure_at.lock().unwrap().unwrap();
    let next = outbox.rows.lock().unwrap().values().next().unwrap().next;
    assert!(next >= failed_at + chrono::Duration::seconds(1));
}
