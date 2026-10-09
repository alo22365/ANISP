//! Explicit, bounded one-shot ingestion; it does not poll or need ClickHouse.
//! Stable repository IDs preserve ledger identity and first-observed branch.
use std::{error::Error, sync::Arc};

use anisp_config::AppConfig;
use anisp_git_collector::LocalGitCollector;
use anisp_git_context::GitRepository;
use anisp_git_ingest::{GitIngestOutcome, GitIngestService};
use anisp_postgres_store::{PostgresGitOutboxRepository, PostgresStore};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if !(4..=6).contains(&args.len()) {
        return Err(
            "usage: ingest_local_git PATH REPOSITORY_ID PROJECT_ID SERVICE [BRANCH|-] [LIMIT]"
                .into(),
        );
    }
    let path = args[0].clone();
    let repository_id = args[1].clone();
    let repository = GitRepository::new(
        &repository_id,
        &repository_id,
        None,
        (args[2] != "-").then(|| args[2].clone()),
        (args[3] != "-").then(|| args[3].clone()),
    )?;
    let branch = args.get(4).filter(|value| value.as_str() != "-").cloned();
    let limit = args
        .get(5)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(100);
    let observations = tokio::task::spawn_blocking(move || {
        LocalGitCollector::new(repository, path)?.collect_recent_commits(branch.as_deref(), limit)
    })
    .await??;
    let config = AppConfig::from_env()?;
    let store = PostgresStore::connect(&config.postgres).await?;
    let service = GitIngestService::new(Arc::new(PostgresGitOutboxRepository::new(&store)));
    for observation in observations {
        let commit_sha = observation.commit().commit_sha().as_str().to_owned();
        let outcome = service.ingest(observation).await?;
        println!(
            "{}",
            serde_json::json!({
                "repository_id": repository_id, "commit_sha": commit_sha,
                "event_id": outcome.event_id(),
                "result": match outcome { GitIngestOutcome::Accepted(_) => "accepted", GitIngestOutcome::AlreadyIngested(_) => "already_ingested" },
            })
        );
    }
    Ok(())
}
