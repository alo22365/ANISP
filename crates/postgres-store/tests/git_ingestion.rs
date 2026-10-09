use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    process::Command,
    sync::Arc,
    time::Duration,
};

use anisp_clickhouse_store::{ClickHouseEventRepository, ClickHouseStore};
use anisp_config::AppConfig;
use anisp_event::{EventFilters, EventQuery, EventService, MAX_EVENT_METADATA_BYTES};
use anisp_git_collector::LocalGitCollector;
use anisp_git_context::GitRepository;
use anisp_git_ingest::{
    GitCommitProjection, GitIngestOutcome, GitIngestService, GitOutboxPublisher,
    GitOutboxRepository, GitPublisherOptions,
};
use anisp_postgres_store::{PostgresGitOutboxRepository, PostgresStore};
use chrono::Utc;
use git2::{Oid, Repository, RepositoryInitOptions, Signature, Time};
use serde_json::Value;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use tempfile::TempDir;
use uuid::Uuid;

struct GitFixture {
    directory: TempDir,
    native: Repository,
    domain: GitRepository,
}
impl GitFixture {
    fn new(project: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let native = Repository::init_opts(
            directory.path(),
            RepositoryInitOptions::new().initial_head("main"),
        )
        .unwrap();
        let domain = GitRepository::new(
            format!("repo-{}", Uuid::now_v7()),
            "finance-local",
            Some("https://never-copy.invalid/repo".into()),
            Some(project.into()),
            Some("xpa-finance".into()),
        )
        .unwrap();
        Self {
            directory,
            native,
            domain,
        }
    }
    fn commit(
        &self,
        parents: &[Oid],
        files: &[(String, Vec<u8>)],
        message: &str,
        seconds: i64,
    ) -> Oid {
        let mut builder = self.native.treebuilder(None).unwrap();
        for (path, content) in files {
            let blob = self.native.blob(content).unwrap();
            builder.insert(path, blob, 0o100644).unwrap();
        }
        let tree = self.native.find_tree(builder.write().unwrap()).unwrap();
        let author = Signature::new(
            "Git Author",
            "author@example.com",
            &Time::new(seconds - 1, 480),
        )
        .unwrap();
        let committer = Signature::new(
            "Git Committer",
            "committer@example.com",
            &Time::new(seconds, 330),
        )
        .unwrap();
        let parents = parents
            .iter()
            .map(|id| self.native.find_commit(*id).unwrap())
            .collect::<Vec<_>>();
        let parent_refs = parents.iter().collect::<Vec<_>>();
        self.native
            .commit(
                Some("HEAD"),
                &author,
                &committer,
                message,
                &tree,
                &parent_refs,
            )
            .unwrap()
    }
    fn collector(&self) -> LocalGitCollector {
        LocalGitCollector::new(self.domain.clone(), self.directory.path()).unwrap()
    }
}

async fn dependencies() -> (
    AppConfig,
    PgPool,
    Arc<PostgresGitOutboxRepository>,
    EventService,
    ClickHouseStore,
) {
    let config = AppConfig::from_env().unwrap();
    let postgres = PostgresStore::connect(&config.postgres).await.unwrap();
    postgres.ping().await.unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(config.postgres.url())
        .await
        .unwrap();
    let outbox = Arc::new(PostgresGitOutboxRepository::new(&postgres));
    let clickhouse = ClickHouseStore::new(&config.clickhouse);
    clickhouse.ping().await.unwrap();
    let events = EventService::new(Arc::new(ClickHouseEventRepository::new(&clickhouse)));
    (config, pool, outbox, events, clickhouse)
}

async fn count_rows(pool: &PgPool, repository_id: &str, pending_only: bool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM git_commit_outbox WHERE repository_id = $1 AND ($2 = false OR published_at IS NULL)")
        .bind(repository_id).bind(pending_only).fetch_one(pool).await.unwrap()
}
async fn payload(pool: &PgPool, event_id: Uuid) -> Value {
    let text: String =
        sqlx::query_scalar("SELECT payload_json::text FROM git_commit_outbox WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(pool)
            .await
            .unwrap();
    serde_json::from_str(&text).unwrap()
}
fn publisher(outbox: Arc<PostgresGitOutboxRepository>, events: EventService) -> GitOutboxPublisher {
    GitOutboxPublisher::new(
        outbox,
        events,
        GitPublisherOptions::new(500, Duration::from_secs(1), Duration::from_secs(60), 10).unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL + ClickHouse; uses temporary real Git repositories"]
async fn real_git_idempotent_ingestion_and_publication() {
    let (_, pool, outbox, events, _) = dependencies().await;
    let project = format!("git-e2e-{}", Uuid::now_v7());
    let fixture = GitFixture::new(&project);
    let seconds = Utc::now().timestamp() - 10;
    let a = fixture.commit(
        &[],
        &[("old.txt".into(), b"one\ntwo\n".to_vec())],
        "Commit A\n\nRoot",
        seconds,
    );
    let b = fixture.commit(
        &[a],
        &[("old.txt".into(), b"one\ntwo\nthree\n".to_vec())],
        "Commit B",
        seconds + 1,
    );
    let c = fixture.commit(
        &[b],
        &[
            ("renamed.txt".into(), b"one\ntwo\nthree\n".to_vec()),
            ("binary.bin".into(), b"\0binary\x01".to_vec()),
        ],
        "Commit C",
        seconds + 2,
    );
    let collector = fixture.collector();
    let ingest = GitIngestService::new(outbox.clone());
    let mut ids = HashMap::new();
    for observation in collector.collect_recent_commits(None, 3).unwrap() {
        let sha = observation.commit().commit_sha().as_str().to_owned();
        let outcome = ingest.ingest(observation).await.unwrap();
        assert!(matches!(outcome, GitIngestOutcome::Accepted(_)));
        assert_eq!(outcome.event_id().get_version_num(), 7);
        ids.insert(sha, outcome.event_id());
    }
    assert_eq!(
        count_rows(&pool, fixture.domain.repository_id(), false).await,
        3
    );
    for observation in collector.collect_recent_commits(None, 3).unwrap() {
        let id = ids[observation.commit().commit_sha().as_str()];
        assert_eq!(
            ingest.ingest(observation).await.unwrap(),
            GitIngestOutcome::AlreadyIngested(id)
        );
    }
    assert_eq!(
        count_rows(&pool, fixture.domain.repository_id(), false).await,
        3
    );
    fixture
        .native
        .branch(
            "also-observed",
            &fixture.native.find_commit(c).unwrap(),
            false,
        )
        .unwrap();
    assert_eq!(
        ingest
            .ingest(
                collector
                    .collect_commit(&c.to_string(), Some("also-observed"))
                    .unwrap()
            )
            .await
            .unwrap(),
        GitIngestOutcome::AlreadyIngested(ids[&c.to_string()])
    );
    assert_eq!(
        payload(&pool, ids[&c.to_string()]).await["metadata"]["branch"],
        "main"
    );

    let publisher = publisher(outbox.clone(), events.clone());
    assert!(publisher.publish_batch().await.unwrap().published_count >= 3);
    assert_eq!(
        count_rows(&pool, fixture.domain.repository_id(), true).await,
        0
    );
    let query = EventQuery::new(
        EventFilters {
            project_id: Some(project.clone()),
            source: Some("git".into()),
            ..Default::default()
        },
        None,
        None,
        Some(500),
    )
    .unwrap();
    let items = events.search_events(&query).await.unwrap();
    assert_eq!(items.len(), 3);
    assert_eq!(
        items
            .iter()
            .map(|event| event.event_id())
            .collect::<HashSet<_>>()
            .len(),
        3
    );
    let c_event = items
        .iter()
        .find(|event| event.event_id() == ids[&c.to_string()])
        .unwrap();
    assert_eq!(c_event.subject_id(), c.to_string());
    assert_eq!(c_event.correlation_id(), None);
    let metadata = c_event.metadata();
    assert_eq!(metadata["branch"], "main");
    assert_eq!(metadata["parent_shas"][0], b.to_string());
    assert_eq!(metadata["author_name"], "Git Author");
    assert_eq!(metadata["author_email"], "author@example.com");
    assert_eq!(metadata["changed_file_count"], 2);
    assert!(metadata.get("remote_url").is_none());
    let files = metadata["changed_files"].as_array().unwrap();
    let rename = files
        .iter()
        .find(|file| file["change_type"] == "renamed")
        .unwrap();
    assert_eq!(rename["path"], "renamed.txt");
    assert_eq!(rename["old_path"], "old.txt");
    let binary = files
        .iter()
        .find(|file| file["path"] == "binary.bin")
        .unwrap();
    assert!(binary["additions"].is_null() && binary["deletions"].is_null());
    assert!(metadata["additions"].is_null() && metadata["deletions"].is_null());
    for observation in collector.collect_recent_commits(None, 3).unwrap() {
        assert!(matches!(
            ingest.ingest(observation).await.unwrap(),
            GitIngestOutcome::AlreadyIngested(_)
        ));
    }
    publisher.publish_batch().await.unwrap();
    assert_eq!(events.search_events(&query).await.unwrap().len(), 3);
    println!(
        "DUPLICATE_SCAN repository={} project={} outbox=3 clickhouse=3 distinct_event_ids=3",
        fixture.domain.repository_id(),
        project
    );

    // Concurrent conflict-safe INSERTs with different candidate UUIDs.
    let concurrent_project = format!("git-race-{}", Uuid::now_v7());
    let concurrent = GitFixture::new(&concurrent_project);
    let commit = concurrent.commit(&[], &[], "Concurrent", seconds);
    let observed = concurrent
        .collector()
        .collect_commit(&commit.to_string(), None)
        .unwrap();
    let left = GitCommitProjection::from_observation(&observed, Uuid::now_v7()).unwrap();
    let right = GitCommitProjection::from_observation(&observed, Uuid::now_v7()).unwrap();
    let (left, right) = tokio::join!(
        outbox.insert_if_absent(&left),
        outbox.insert_if_absent(&right)
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(outcomes[0].event_id(), outcomes[1].event_id());
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, GitIngestOutcome::Accepted(_)))
            .count(),
        1
    );
    assert_eq!(
        count_rows(&pool, concurrent.domain.repository_id(), false).await,
        1
    );

    // Truncation using real Git trees, not fabricated Domain data.
    let large_project = format!("git-large-{}", Uuid::now_v7());
    let large = GitFixture::new(&large_project);
    let files = (0..1200)
        .map(|index| {
            (
                format!("{index:04}-{}.txt", "x".repeat(180)),
                b"line\n".to_vec(),
            )
        })
        .collect::<Vec<_>>();
    let id = large.commit(&[], &files, "Many changed files", seconds);
    let result = ingest
        .ingest(
            large
                .collector()
                .collect_commit(&id.to_string(), None)
                .unwrap(),
        )
        .await
        .unwrap();
    publisher.publish_batch().await.unwrap();
    let event = events.get_event(result.event_id()).await.unwrap().unwrap();
    assert_eq!(event.metadata()["changed_file_count"], 1200);
    assert_eq!(event.metadata()["changes_truncated"], true);
    assert!(event.metadata()["changed_files"].as_array().unwrap().len() < 1200);
    assert!(serde_json::to_vec(event.metadata()).unwrap().len() <= MAX_EVENT_METADATA_BYTES);
    println!(
        "LARGE_METADATA total=1200 included={} bytes={} changes_truncated=true",
        event.metadata()["changed_files"].as_array().unwrap().len(),
        serde_json::to_vec(event.metadata()).unwrap().len()
    );
}

fn compose_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/docker/docker-compose.yml")
}
fn compose(arguments: &[&str]) {
    let output = Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg(compose_path())
        .args(arguments)
        .output()
        .expect("Docker Compose is required for failure recovery test");
    assert!(
        output.status.success(),
        "Compose operation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
struct RestoreClickHouse {
    active: bool,
}
impl Drop for RestoreClickHouse {
    fn drop(&mut self) {
        if self.active {
            let _ = Command::new("docker")
                .arg("compose")
                .arg("-f")
                .arg(compose_path())
                .args(["start", "clickhouse"])
                .output();
        }
    }
}

#[tokio::test]
#[ignore = "requires Compose PostgreSQL + ClickHouse; temporarily stops and restores ClickHouse; run serially"]
async fn real_git_outbox_clickhouse_failure_recovery() {
    let (config, pool, outbox, events, clickhouse) = dependencies().await;
    assert_eq!(
        config.clickhouse.url(),
        "http://127.0.0.1:8123",
        "failure recovery test must target the local Compose endpoint"
    );
    let project = format!("git-recovery-{}", Uuid::now_v7());
    let fixture = GitFixture::new(&project);
    let seconds = Utc::now().timestamp() - 10;
    let a = fixture.commit(
        &[],
        &[("a.txt".into(), b"A\n".to_vec())],
        "Before outage",
        seconds,
    );
    let ingest = GitIngestService::new(outbox.clone());
    ingest
        .ingest(
            fixture
                .collector()
                .collect_commit(&a.to_string(), None)
                .unwrap(),
        )
        .await
        .unwrap();
    let publisher = publisher(outbox, events.clone());
    publisher.publish_batch().await.unwrap();
    let mut restore = RestoreClickHouse { active: true };
    compose(&["stop", "--timeout", "3", "clickhouse"]);
    assert!(clickhouse.ping().await.is_err());
    let d = fixture.commit(
        &[a],
        &[
            ("a.txt".into(), b"A\n".to_vec()),
            ("d.txt".into(), b"D\n".to_vec()),
        ],
        "Commit D during outage",
        seconds + 1,
    );
    let accepted = ingest
        .ingest(
            fixture
                .collector()
                .collect_commit(&d.to_string(), None)
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(accepted, GitIngestOutcome::Accepted(_)));
    assert_eq!(
        count_rows(&pool, fixture.domain.repository_id(), true).await,
        1
    );
    let batch = publisher.publish_batch().await.unwrap();
    assert!(batch.failed_count >= 1);
    assert_eq!(
        count_rows(&pool, fixture.domain.repository_id(), true).await,
        1
    );
    let row = sqlx::query("SELECT attempt_count, last_attempt_at, next_attempt_at, claimed_by FROM git_commit_outbox WHERE event_id = $1").bind(accepted.event_id()).fetch_one(&pool).await.unwrap();
    let attempts: i32 = row.get("attempt_count");
    let last_attempt: chrono::DateTime<Utc> = row.get("last_attempt_at");
    let next_attempt: chrono::DateTime<Utc> = row.get("next_attempt_at");
    assert_eq!(attempts, 1);
    assert!(next_attempt >= last_attempt + chrono::Duration::seconds(1));
    assert_eq!(row.get::<Option<Uuid>, _>("claimed_by"), None);
    println!(
        "FAILURE_RECOVERY ClickHouse=down ingest=Accepted event_id={} pending=1 attempts=1 backoff=1s",
        accepted.event_id()
    );
    compose(&["start", "clickhouse"]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    while clickhouse.ping().await.is_err() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "ClickHouse failed to restart"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    restore.active = false;
    loop {
        publisher.publish_batch().await.unwrap();
        if count_rows(&pool, fixture.domain.repository_id(), true).await == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Git event did not recover"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let event = events
        .get_event(accepted.event_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.subject_id(), d.to_string());
    assert_eq!(event.event_type(), "git.commit.created");
    println!(
        "FAILURE_RECOVERY ClickHouse=up same_event_id={} pending=0",
        event.event_id()
    );
}
