use anisp_config::AppConfig;
use anisp_git_context::{
    GitCommit, GitCommitInput, GitContextSyncReader, GitRepository, ObservedGitCommit,
};
use anisp_git_ingest::{GitCommitProjection, GitOutboxRepository};
use anisp_postgres_store::{PostgresGitOutboxRepository, PostgresStore};
use chrono::{DateTime, Duration, Utc};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

fn projection(
    repo: &str,
    project: &str,
    service: &str,
    number: usize,
    time: DateTime<Utc>,
) -> GitCommitProjection {
    let repository = GitRepository::new(
        repo,
        "sync-fixture",
        None,
        Some(project.into()),
        Some(service.into()),
    )
    .unwrap();
    let commit = GitCommit::new(GitCommitInput {
        repository_id: repo.into(),
        commit_sha: format!("{number:040x}"),
        parent_shas: vec![],
        author_name: "Author".into(),
        author_email: None,
        message: "freshness fixture".into(),
        committed_at: time,
        changes: vec![],
    })
    .unwrap();
    GitCommitProjection::from_observation(
        &ObservedGitCommit::new(repository, commit, None, Utc::now()).unwrap(),
        Uuid::now_v7(),
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires migrated PostgreSQL; verifies half-open scoped unpublished-intent count"]
async fn real_git_pending_window_filters_include_claimed_backoff_and_exhausted() {
    let config = AppConfig::from_env().unwrap();
    let store = PostgresStore::connect(&config.postgres).await.unwrap();
    let repo = PostgresGitOutboxRepository::new(&store);
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(config.postgres.url())
        .await
        .unwrap();
    let project = format!("git-sync-e2e-{}", Uuid::now_v7());
    let repository = format!("sync-repo-{}", Uuid::now_v7());
    let service = "xpa-finance";
    let to = DateTime::from_timestamp_millis(Utc::now().timestamp_millis()).unwrap();
    let from = to - Duration::hours(1);
    let middle = from + Duration::minutes(30);
    let mut ids = vec![];
    for (i, p, s, t) in [
        (0, project.as_str(), service, from),
        (1, project.as_str(), service, middle),
        (2, project.as_str(), service, to),
        (
            3,
            project.as_str(),
            service,
            from - Duration::milliseconds(1),
        ),
        (4, project.as_str(), service, to + Duration::milliseconds(1)),
        (5, "other-project", service, middle),
        (6, project.as_str(), "other-service", middle),
        (7, project.as_str(), service, middle),
        (8, project.as_str(), service, middle),
        (9, project.as_str(), service, middle),
    ] {
        let p = projection(&repository, p, s, i, t);
        ids.push(p.event_id());
        repo.insert_if_absent(&p).await.unwrap();
    }
    sqlx::query("UPDATE git_commit_outbox SET next_attempt_at = $2 WHERE event_id = $1")
        .bind(ids[1])
        .bind(to + Duration::days(1))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE git_commit_outbox SET published_at = $2 WHERE event_id = $1")
        .bind(ids[7])
        .bind(to)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE git_commit_outbox SET exhausted = true WHERE event_id = $1")
        .bind(ids[8])
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE git_commit_outbox SET claimed_at = $2, claimed_by = $3 WHERE event_id = $1",
    )
    .bind(ids[9])
    .bind(to)
    .bind(Uuid::now_v7())
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        repo.pending_count_for_window(&project, service, from, to)
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        repo.pending_count_for_window(&project, service, from + Duration::microseconds(1), to)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        repo.pending_count_for_window(&project, service, from, to + Duration::microseconds(1))
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        repo.pending_count_for_window(&project, service, to, to)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        repo.pending_count_for_window("' OR 1=1 --", service, from, to)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        repo.pending_count_for_window(&project, "' OR 1=1 --", from, to)
            .await
            .unwrap(),
        0
    );
    println!(
        "GIT_SYNC_PORT pending=4 (inclusive from + backoff + exhausted + claimed); published/other labels/outside window excluded; exact microsecond predicates and safe binding verified"
    );
}
