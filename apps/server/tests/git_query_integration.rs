//! Explicit, serial-only E2E: real temporary Git, PostgreSQL outbox,
//! ClickHouse projection and Axum HTTP requests. Briefly stops local ClickHouse.
use anisp_clickhouse_store::{
    ClickHouseEventRepository, ClickHouseGitCommitReader, ClickHouseStore,
};
use anisp_config::{AppConfig, ContextGitConfig};
use anisp_context::SupportCaseContextService;
use anisp_event::EventService;
use anisp_git_collector::LocalGitCollector;
use anisp_git_context::{GitContextQueryService, GitContextSyncReader, GitRepository};
use anisp_git_ingest::{
    GitCommitProjection, GitIngestOutcome, GitIngestService, GitOutboxPublisher,
    GitOutboxRepository, GitOutboxStatusService, GitPublisherOptions,
};
use anisp_postgres_store::{
    PostgresGitOutboxRepository, PostgresStore, PostgresSupportCaseRepository,
};
use anisp_server::{AppState, build_app};
use anisp_support::SupportService;
use anisp_support_outbox::{SupportOutboxPublisher, SupportOutboxStatusService};
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use chrono::{DateTime, SecondsFormat, Utc};
use git2::{Oid, Repository, RepositoryInitOptions, Signature, Time};
use serde_json::{Value, json};
use std::{path::PathBuf, process::Command, sync::Arc, time::Duration};
use tower::ServiceExt;
use uuid::Uuid;

fn commit(
    repo: &Repository,
    parents: &[Oid],
    files: &[(String, Vec<u8>)],
    message: &str,
    seconds: i64,
) -> Oid {
    let mut builder = repo.treebuilder(None).unwrap();
    for (path, bytes) in files {
        builder
            .insert(path, repo.blob(bytes).unwrap(), 0o100644)
            .unwrap();
    }
    let tree = repo.find_tree(builder.write().unwrap()).unwrap();
    let signature =
        Signature::new("Query Author", "author@example.com", &Time::new(seconds, 0)).unwrap();
    let parents = parents
        .iter()
        .map(|id| repo.find_commit(*id).unwrap())
        .collect::<Vec<_>>();
    repo.commit(
        Some("HEAD"),
        &signature,
        &signature,
        message,
        &tree,
        &parents.iter().collect::<Vec<_>>(),
    )
    .unwrap()
}
fn accepted(outcome: GitIngestOutcome) -> Uuid {
    match outcome {
        GitIngestOutcome::Accepted(id) | GitIngestOutcome::AlreadyIngested(id) => id,
    }
}
async fn request(
    app: &Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
    status: StatusCode,
) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(body.map_or_else(Body::empty, |v| Body::from(v.to_string())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), status, "{method} {uri}");
    let id = response
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let value: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    if value.get("error").is_some() {
        assert_eq!(value["error"]["request_id"], id);
    }
    value
}
async fn get(app: &Router, uri: &str) -> Value {
    request(app, "GET", uri, None, StatusCode::OK).await
}
fn compose(action: &str) -> bool {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/docker/docker-compose.yml");
    let mut cmd = Command::new("docker");
    cmd.args(["compose", "-f"]).arg(path);
    if action == "up" {
        cmd.args(["up", "-d", "--wait", "clickhouse"]);
    } else {
        cmd.args(["stop", "clickhouse"]);
    }
    cmd.status().unwrap().success()
}
struct RestoreClickHouse;
impl Drop for RestoreClickHouse {
    fn drop(&mut self) {
        if !compose("up") {
            eprintln!("Could not restore local ClickHouse; run make infra-up");
        }
    }
}

/// Only the deliberately corrupt negative fixture is removed. Keeping it
/// would poison unfiltered public reads in the developer database. UUID comes
/// from this test, never user input; normal E2E fixtures remain available.
struct CorruptFixture {
    event_id: Uuid,
    removed: bool,
}
impl CorruptFixture {
    fn remove(&mut self) -> bool {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/docker/docker-compose.yml");
        let sql = format!(
            "ALTER TABLE context_event DELETE WHERE event_id = toUUID('{}') SETTINGS mutations_sync = 1",
            self.event_id
        );
        self.removed = Command::new("docker").args(["compose", "-f"]).arg(path)
            .args(["exec", "-T", "clickhouse", "sh", "-c", "clickhouse-client --user \"$CLICKHOUSE_USER\" --password \"$CLICKHOUSE_PASSWORD\" --database \"$CLICKHOUSE_DB\" --query \"$1\"", "sh"])
            .arg(sql).status().is_ok_and(|status| status.success());
        self.removed
    }
}
impl Drop for CorruptFixture {
    fn drop(&mut self) {
        if !self.removed && !self.remove() {
            eprintln!("Could not remove corrupt E2E fixture {}", self.event_id);
        }
    }
}

#[tokio::test]
#[ignore = "requires migrated local PostgreSQL + ClickHouse and no active publishers; stops/restores ClickHouse; run serially"]
async fn real_git_pipeline_query_api_and_failure_isolation() {
    let config = AppConfig::from_env().unwrap();
    assert!(
        config.clickhouse.url().starts_with("http://127.0.0.1:8123"),
        "outage test requires local Compose ClickHouse"
    );
    let ch = ClickHouseStore::new(&config.clickhouse);
    ch.ping().await.unwrap();
    let pg = PostgresStore::connect(&config.postgres).await.unwrap();
    pg.ping().await.unwrap();
    let events = EventService::new(Arc::new(ClickHouseEventRepository::new(&ch)));
    let git_query = GitContextQueryService::new(Arc::new(ClickHouseGitCommitReader::new(&ch)));
    let support_repo = Arc::new(PostgresSupportCaseRepository::new(&pg));
    let support = SupportService::new(support_repo.clone());
    let context = SupportCaseContextService::new(
        support.clone(),
        events.clone(),
        support_repo.clone(),
        git_query.clone(),
        Arc::new(PostgresGitOutboxRepository::new(&pg)),
        config.context_git.clone(),
    );
    let app = build_app(AppState::new(
        config,
        ch.clone(),
        pg.clone(),
        events.clone(),
        support,
        context,
        SupportOutboxStatusService::new(support_repo),
        git_query,
        GitOutboxStatusService::new(
            Arc::new(PostgresGitOutboxRepository::new(&pg)),
            Duration::from_secs(30),
        )
        .unwrap(),
    ));
    let config = AppConfig::from_env().unwrap();
    let pg = PostgresStore::connect(&config.postgres).await.unwrap();
    let outbox = Arc::new(PostgresGitOutboxRepository::new(&pg));
    let ingest = GitIngestService::new(outbox.clone());
    let publisher = GitOutboxPublisher::new(
        outbox.clone(),
        events.clone(),
        GitPublisherOptions::new(500, Duration::from_secs(1), Duration::from_secs(30), 10).unwrap(),
    );
    let project = format!("git-query-e2e-{}", Uuid::now_v7());
    let repo_a = format!("query-repo-a-{}", Uuid::now_v7());
    let repo_b = format!("query-repo-b-{}", Uuid::now_v7());
    let dir = tempfile::tempdir().unwrap();
    let native = Repository::init_opts(
        dir.path(),
        RepositoryInitOptions::new().initial_head("main"),
    )
    .unwrap();
    let t = Utc::now().timestamp() - 20;
    let a = commit(
        &native,
        &[],
        &[("old.txt".into(), b"one\ntwo\n".to_vec())],
        "Commit A",
        t,
    );
    let b = commit(
        &native,
        &[a],
        &[("old.txt".into(), b"one\ntwo\nthree\n".to_vec())],
        "Commit B\n\nFull message",
        t + 1,
    );
    let c = commit(
        &native,
        &[b],
        &[
            ("renamed.txt".into(), b"one\ntwo\nthree\n".to_vec()),
            ("binary.bin".into(), b"\0binary\x01".to_vec()),
        ],
        "Commit C",
        t + 2,
    );
    let domain = GitRepository::new(
        &repo_a,
        "finance-local",
        None,
        Some(project.clone()),
        Some("xpa-finance".into()),
    )
    .unwrap();
    let collector = LocalGitCollector::new(domain, dir.path()).unwrap();
    let observed_b = collector.collect_commit(&b.to_string(), None).unwrap();
    let expected_observed = observed_b.observed_at();
    let mut b_event = None;
    for id in [a, b, c] {
        let observed = if id == b {
            observed_b.clone()
        } else {
            collector.collect_commit(&id.to_string(), None).unwrap()
        };
        let event_id = accepted(ingest.ingest(observed).await.unwrap());
        if id == b {
            b_event = Some(event_id);
        }
    }
    for observed in collector.collect_recent_commits(None, 3).unwrap() {
        assert!(matches!(
            ingest.ingest(observed).await.unwrap(),
            GitIngestOutcome::AlreadyIngested(_)
        ));
    }
    // Outbox is deliberately not a read model: accepted but unpublished = empty.
    assert_eq!(
        get(&app, &format!("/api/v1/git/commits?repository_id={repo_a}")).await["count"],
        0
    );
    publisher.publish_batch().await.unwrap();
    let list_url = format!("/api/v1/git/commits?repository_id={repo_a}");
    let list = get(&app, &list_url).await;
    assert_eq!(list["count"], 3);
    assert_eq!(
        list["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["commit_sha"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![c.to_string(), b.to_string(), a.to_string()]
    );
    let detail_url = format!("/api/v1/git/commits/{repo_a}/{b}");
    let detail = get(&app, &detail_url).await;
    assert_eq!(detail["event_id"], b_event.unwrap().to_string());
    assert_eq!(detail["parent_shas"], json!([a.to_string()]));
    assert_eq!(detail["author_name"], "Query Author");
    assert_eq!(detail["author_email"], "author@example.com");
    assert_eq!(detail["message"], "Commit B\n\nFull message");
    assert_eq!(detail["branch"], "main");
    assert_eq!(detail["observed_at"], json!(expected_observed));
    assert_eq!(detail["changed_files"][0]["change_type"], "modified");
    assert_eq!(detail["changed_files"][0]["additions"], 1);
    assert_eq!(detail["changed_files"][0]["deletions"], 0);
    let upper = get(
        &app,
        &format!(
            "/api/v1/git/commits/{repo_a}/{}",
            b.to_string().to_uppercase()
        ),
    )
    .await;
    assert_eq!(upper, detail);
    for (query, count) in [
        (format!("project_id={project}"), 3),
        (format!("project_id={project}&service=xpa-finance"), 3),
        (format!("repository_id={repo_a}&branch=main"), 3),
        (format!("repository_id={repo_a}&branch=other"), 0),
        (format!("repository_id={repo_a}&limit=1"), 1),
        (format!("project_id={project}&service=absent"), 0),
    ] {
        assert_eq!(
            get(&app, &format!("/api/v1/git/commits?{query}")).await["count"],
            count
        );
    }
    let from = DateTime::from_timestamp(t + 1, 0)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, true);
    let to = DateTime::from_timestamp(t + 2, 0)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, true);
    let range = get(&app, &format!("{list_url}&from={from}&to={to}")).await;
    assert_eq!(range["count"], 1);
    assert_eq!(range["items"][0]["commit_sha"], b.to_string());
    assert_eq!(
        get(
            &app,
            "/api/v1/git/commits?repository_id=%27%20OR%201%3D1%20--"
        )
        .await["count"],
        0
    );
    let c_detail = get(&app, &format!("/api/v1/git/commits/{repo_a}/{c}")).await;
    let files = c_detail["changed_files"].as_array().unwrap();
    let rename = files
        .iter()
        .find(|v| v["change_type"] == "renamed")
        .unwrap();
    assert_eq!(rename["path"], "renamed.txt");
    assert_eq!(rename["old_path"], "old.txt");
    let binary = files.iter().find(|v| v["path"] == "binary.bin").unwrap();
    assert!(binary["additions"].is_null() && binary["deletions"].is_null());
    assert!(c_detail["additions"].is_null());

    let collector_b = LocalGitCollector::new(
        GitRepository::new(
            &repo_b,
            "same-physical-repo",
            None,
            Some(project.clone()),
            Some("other-service".into()),
        )
        .unwrap(),
        dir.path(),
    )
    .unwrap();
    let second_id = accepted(
        ingest
            .ingest(collector_b.collect_commit(&b.to_string(), None).unwrap())
            .await
            .unwrap(),
    );
    publisher.publish_batch().await.unwrap();
    let second = get(&app, &format!("/api/v1/git/commits/{repo_b}/{b}")).await;
    assert_eq!(second["repository_id"], repo_b);
    assert_eq!(second["event_id"], second_id.to_string());
    assert_ne!(second["event_id"], detail["event_id"]);
    assert_eq!(get(&app, &detail_url).await, detail);
    request(
        &app,
        "GET",
        &format!("/api/v1/git/commits/{repo_b}/{a}"),
        None,
        StatusCode::NOT_FOUND,
    )
    .await;

    let many = (0..1200)
        .map(|i| {
            (
                format!("file-{i:04}-{}.txt", "x".repeat(175)),
                b"line\n".to_vec(),
            )
        })
        .collect::<Vec<_>>();
    let d = commit(&native, &[c], &many, "Commit D large metadata", t + 3);
    let observed = collector.collect_commit(&d.to_string(), None).unwrap();
    let full_count = observed.commit().changes().len();
    ingest.ingest(observed).await.unwrap();
    publisher.publish_batch().await.unwrap();
    let large = get(&app, &format!("/api/v1/git/commits/{repo_a}/{d}")).await;
    assert_eq!(large["changes_truncated"], true);
    assert_eq!(large["changed_file_count"], full_count);
    assert!(large["changed_files"].as_array().unwrap().len() < full_count);
    publisher.publish_batch().await.unwrap();
    assert_eq!(get(&app, &list_url).await["count"], 4);

    // Strict source selection: a structurally valid Git-like payload belonging
    // to another source/event/subject must not leak into Git queries.
    for (source, kind, subject) in [
        ("support", "git.commit.created", "commit"),
        ("git", "git.commit.updated", "commit"),
        ("git", "git.commit.created", "other"),
    ] {
        let mut p = GitCommitProjection::from_observation(&observed_b, Uuid::now_v7())
            .unwrap()
            .prepared_event();
        p.source = source.into();
        p.event_type = kind.into();
        p.subject_type = subject.into();
        events.persist_prepared_event(p).await.unwrap();
    }
    assert_eq!(get(&app, &list_url).await["count"], 4);
    let mut corrupt = GitCommitProjection::from_observation(&observed_b, Uuid::now_v7())
        .unwrap()
        .prepared_event();
    let bad_repo = format!("corrupt-{}", Uuid::now_v7());
    let mut cleanup = CorruptFixture {
        event_id: corrupt.event_id,
        removed: false,
    };
    corrupt.metadata["repository_id"] = json!(bad_repo);
    corrupt
        .metadata
        .as_object_mut()
        .unwrap()
        .remove("author_name");
    events.persist_prepared_event(corrupt).await.unwrap();
    for uri in [
        format!("/api/v1/git/commits?repository_id={bad_repo}"),
        format!("/api/v1/git/commits/{bad_repo}/{b}"),
    ] {
        let error = request(&app, "GET", &uri, None, StatusCode::INTERNAL_SERVER_ERROR).await;
        assert_eq!(error["error"]["code"], "internal_error");
    }
    assert!(
        cleanup.remove(),
        "remove this test's corrupt negative fixture"
    );

    let restore = RestoreClickHouse;
    assert!(compose("stop"));
    for uri in [list_url.clone(), detail_url.clone()] {
        assert_eq!(
            request(&app, "GET", &uri, None, StatusCode::SERVICE_UNAVAILABLE).await["error"]["code"],
            "service_unavailable"
        );
    }
    request(&app, "GET", "/health", None, StatusCode::OK).await;
    request(&app, "GET", "/ready", None, StatusCode::SERVICE_UNAVAILABLE).await;
    let case = request(&app, "POST", "/api/v1/support/cases", Some(json!({"case_type":"incident", "title":"Git query outage isolation", "description":"PostgreSQL-only case remains available", "reporter_id":"git-query-e2e", "priority":"high"})), StatusCode::CREATED).await;
    let read_case = get(
        &app,
        &format!(
            "/api/v1/support/cases/{}",
            case["case_id"].as_str().unwrap()
        ),
    )
    .await;
    assert_eq!(read_case, case);
    drop(restore);
    assert_eq!(get(&app, &detail_url).await, detail);
    println!(
        "Git Query E2E PASS: C/B/A, compound identity A/B, strict metadata, rename, binary null, truncation {full_count} files, CH outage 503 + Support POST 201/GET 200, restored data. repository={repo_a}, commit_b={b}"
    );
    // Touch the port to explicitly check that the idempotency ledger still
    // retains B, but never use it to answer a public Git query.
    assert_eq!(
        outbox
            .find_ingested_event_id(&repo_a, &b.to_string())
            .await
            .unwrap(),
        b_event
    );
}

#[tokio::test]
#[ignore = "requires migrated local PostgreSQL + ClickHouse and no active publishers; real Git/Case enrichment and ClickHouse outage; run serially"]
async fn real_support_case_git_enrichment_window_freshness_and_failure_isolation() {
    let config = AppConfig::from_env().unwrap();
    assert!(config.clickhouse.url().starts_with("http://127.0.0.1:8123"));
    let ch = ClickHouseStore::new(&config.clickhouse);
    ch.ping().await.unwrap();
    let pg = PostgresStore::connect(&config.postgres).await.unwrap();
    pg.ping().await.unwrap();
    let events = EventService::new(Arc::new(ClickHouseEventRepository::new(&ch)));
    let git_query = GitContextQueryService::new(Arc::new(ClickHouseGitCommitReader::new(&ch)));
    let support_repo = Arc::new(PostgresSupportCaseRepository::new(&pg));
    let support = SupportService::new(support_repo.clone());
    let outbox = Arc::new(PostgresGitOutboxRepository::new(&pg));
    let settings = ContextGitConfig::default();
    let context = SupportCaseContextService::new(
        support.clone(),
        events.clone(),
        support_repo.clone(),
        git_query.clone(),
        outbox.clone(),
        settings,
    );
    let app = build_app(AppState::new(
        config,
        ch.clone(),
        pg.clone(),
        events.clone(),
        support,
        context,
        SupportOutboxStatusService::new(support_repo.clone()),
        git_query,
        GitOutboxStatusService::new(outbox.clone(), Duration::from_secs(30)).unwrap(),
    ));
    let ingest = GitIngestService::new(outbox.clone());
    let publisher = GitOutboxPublisher::new(
        outbox.clone(),
        events.clone(),
        GitPublisherOptions::new(500, Duration::from_secs(1), Duration::from_secs(30), 10).unwrap(),
    );
    let support_publisher = SupportOutboxPublisher::new(
        support_repo,
        events,
        Duration::from_secs(1),
        500,
        Duration::from_secs(30),
        10,
    );
    // Unique project isolates this automated test from earlier scoped fixtures.
    // Both repositories share the exact project/service labels.
    let project = format!("xpa-git-enrichment-{}", Uuid::now_v7());
    let service = "xpa-finance";
    let repo_a = format!("enrichment-a-{}", Uuid::now_v7());
    let repo_b = format!("enrichment-b-{}", Uuid::now_v7());
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let native_a = Repository::init_opts(
        dir_a.path(),
        RepositoryInitOptions::new().initial_head("main"),
    )
    .unwrap();
    let native_b = Repository::init_opts(
        dir_b.path(),
        RepositoryInitOptions::new().initial_head("main"),
    )
    .unwrap();
    let t = Utc::now().timestamp();
    let old = commit(
        &native_a,
        &[],
        &[("old.txt".into(), b"one\ntwo\n".to_vec())],
        "Old commit",
        t - 172800,
    );
    let a = commit(
        &native_a,
        &[old],
        &[("old.txt".into(), b"one\ntwo\nthree\n".to_vec())],
        "Commit A",
        t - 180,
    );
    let b = commit(
        &native_b,
        &[],
        &[("build.txt".into(), b"build\n".to_vec())],
        "Commit B",
        t - 120,
    );
    let c = commit(
        &native_a,
        &[a],
        &[
            ("renamed.txt".into(), b"one\ntwo\nthree\n".to_vec()),
            ("binary.bin".into(), b"\0binary\x01".to_vec()),
        ],
        "Commit C",
        t - 60,
    );
    let collect_a = LocalGitCollector::new(
        GitRepository::new(
            &repo_a,
            "finance-api",
            None,
            Some(project.clone()),
            Some(service.into()),
        )
        .unwrap(),
        dir_a.path(),
    )
    .unwrap();
    let collect_b = LocalGitCollector::new(
        GitRepository::new(
            &repo_b,
            "finance-build",
            None,
            Some(project.clone()),
            Some(service.into()),
        )
        .unwrap(),
        dir_b.path(),
    )
    .unwrap();
    for oid in [old, a, c] {
        ingest
            .ingest(collect_a.collect_commit(&oid.to_string(), None).unwrap())
            .await
            .unwrap();
    }
    ingest
        .ingest(collect_b.collect_commit(&b.to_string(), None).unwrap())
        .await
        .unwrap();
    let case=request(&app,"POST","/api/v1/support/cases",Some(json!({"case_type":"incident","title":"xpa-finance unavailable","description":"Time and label association only; no root cause judgment.","reporter_id":"git-enrichment-e2e","project_id":project,"service":service,"environment":"prod","priority":"high"})),StatusCode::CREATED).await;
    let id = case["case_id"].as_str().unwrap();
    let created: DateTime<Utc> = serde_json::from_value(case["created_at"].clone()).unwrap();
    let from = created - chrono::Duration::hours(24);
    let future = commit(
        &native_b,
        &[b],
        &[("build.txt".into(), b"future\n".to_vec())],
        "Future commit",
        created.timestamp() + 3600,
    );
    ingest
        .ingest(collect_b.collect_commit(&future.to_string(), None).unwrap())
        .await
        .unwrap();
    let uri = format!("/api/v1/support/cases/{id}/context");
    let pending = get(&app, &uri).await;
    assert_eq!(pending["git_context"]["status"], "pending");
    assert_eq!(pending["git_context"]["pending_events"], 3);
    assert_eq!(pending["git_context"]["count"], 0);
    publisher.publish_batch().await.unwrap();
    support_publisher.publish_batch().await.unwrap();
    let initial = get(&app, &uri).await;
    let g = &initial["git_context"];
    assert_eq!(g["status"], "synced");
    assert_eq!(g["pending_events"], 0);
    assert_eq!(g["count"], 3);
    assert_eq!(g["window"], json!({"from":from,"to":created}));
    assert_eq!(
        g["commits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["commit_sha"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![a.to_string(), b.to_string(), c.to_string()]
    );
    assert_eq!(g["commits"][0]["repository_id"], repo_a);
    assert_eq!(g["commits"][1]["repository_id"], repo_b);
    assert_eq!(g["commits"][2]["repository_id"], repo_a);
    assert_eq!(
        initial["history_sync"],
        json!({"status":"synced","pending_events":0})
    );
    assert_eq!(initial["timeline"].as_array().unwrap().len(), 1);
    assert_eq!(
        initial["timeline"][0]["event_type"],
        "support.incident.created"
    );
    let binary = g["commits"][2]["changed_files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["path"] == "binary.bin")
        .unwrap();
    assert!(binary["additions"].is_null() && binary["deletions"].is_null());
    assert!(g["commits"][2]["additions"].is_null());
    // Observed/ingested after Case creation, but committed inside its original
    // fixed window. Freshness must use commit time, not outbox.created_at.
    let d = commit(
        &native_b,
        &[future],
        &[("build.txt".into(), b"late observed\n".to_vec())],
        "Commit D late observation",
        created.timestamp() - 30,
    );
    let observed_d = collect_b.collect_commit(&d.to_string(), None).unwrap();
    assert!(observed_d.observed_at() > created);
    ingest.ingest(observed_d).await.unwrap();
    assert_eq!(
        outbox
            .pending_count_for_window(&project, service, from, created)
            .await
            .unwrap(),
        1
    );
    let pending = get(&app, &uri).await;
    assert_eq!(pending["git_context"]["status"], "pending");
    assert_eq!(pending["git_context"]["pending_events"], 1);
    assert_eq!(pending["git_context"]["count"], 3);
    assert_eq!(pending["history_sync"], initial["history_sync"]);
    publisher.publish_batch().await.unwrap();
    let synced = get(&app, &uri).await;
    assert_eq!(synced["git_context"]["status"], "synced");
    assert_eq!(synced["git_context"]["pending_events"], 0);
    assert_eq!(synced["git_context"]["count"], 4);
    assert_eq!(
        synced["git_context"]["commits"][3]["commit_sha"],
        d.to_string()
    );
    assert_eq!(
        synced["git_context"]["window"],
        initial["git_context"]["window"]
    );
    request(
        &app,
        "PATCH",
        &format!("/api/v1/support/cases/{id}/status"),
        Some(json!({"status":"investigating"})),
        StatusCode::OK,
    )
    .await;
    support_publisher.publish_batch().await.unwrap();
    let advanced = get(&app, &uri).await;
    assert_eq!(advanced["case"]["status"], "investigating");
    assert_eq!(
        advanced["git_context"]["window"],
        initial["git_context"]["window"]
    );
    assert_eq!(
        advanced["timeline"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["event_type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["support.incident.created", "support.incident.investigating"]
    );
    let git_list = get(
        &app,
        &format!("/api/v1/git/commits?project_id={project}&service={service}"),
    )
    .await;
    assert_eq!(git_list["items"][0]["commit_sha"], future.to_string());
    assert_eq!(git_list["items"][1]["commit_sha"], d.to_string());
    let restore = RestoreClickHouse;
    assert!(compose("stop"));
    request(&app, "GET", &uri, None, StatusCode::SERVICE_UNAVAILABLE).await;
    get(&app, &format!("/api/v1/support/cases/{id}")).await;
    request(&app, "GET", "/health", None, StatusCode::OK).await;
    drop(restore);
    let restored = get(&app, &uri).await;
    assert_eq!(restored["git_context"], advanced["git_context"]);
    println!(
        "CASE_GIT_E2E PASS project={project} service={service} case_id={id}: initial A/B/C ASC across two repositories; old/future excluded; pending=1 then synced=0 with late-observed D; fixed window after transition; CH outage context=503/plain case=200; support timeline preserved"
    );
}

/// Read-only assertions on this test's generated identities. No outbox row is
/// deleted: publication and repeat scans must retain the same durable ledger.
fn batch_snapshot(repos: &[String], clickhouse: bool) -> Value {
    assert!(
        repos
            .iter()
            .all(|r| r.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'))
    );
    let identities = repos
        .iter()
        .map(|r| format!("'{r}'"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = if clickhouse {
        format!(
            "SELECT count() AS rows, uniqExact(event_id) AS distinct_events FROM context_event WHERE source = 'git' AND event_type = 'git.commit.created' AND JSONExtractString(metadata_json, 'repository_id') IN ({identities}) FORMAT JSONEachRow"
        )
    } else {
        format!(
            "SELECT json_build_object('rows', count(*), 'pending', count(*) FILTER (WHERE published_at IS NULL), 'ids', json_object_agg(repository_id || ':' || commit_sha, event_id::text), 'branches', json_object_agg(repository_id || ':' || commit_sha, payload_json -> 'metadata' -> 'branch')) FROM git_commit_outbox WHERE repository_id IN ({identities})"
        )
    };
    let compose_file =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/docker/docker-compose.yml");
    let shell = if clickhouse {
        "clickhouse-client --user \"$CLICKHOUSE_USER\" --password \"$CLICKHOUSE_PASSWORD\" --database \"$CLICKHOUSE_DB\" --query \"$1\""
    } else {
        "psql --username \"$POSTGRES_USER\" --dbname \"$POSTGRES_DB\" -At --set ON_ERROR_STOP=1 --command \"$1\""
    };
    let output = Command::new("docker")
        .args(["compose", "-f"])
        .arg(compose_file)
        .args([
            "exec",
            "-T",
            if clickhouse { "clickhouse" } else { "postgres" },
            "sh",
            "-c",
            shell,
            "sh",
        ])
        .arg(sql)
        .output()
        .unwrap();
    assert!(output.status.success(), "batch assertion query failed");
    serde_json::from_slice(&output.stdout).unwrap()
}

fn aggregate_count(value: &Value) -> u64 {
    // ClickHouse JSON may encode UInt64 as a number or string.
    value
        .as_u64()
        .unwrap_or_else(|| value.as_str().unwrap().parse().unwrap())
}

#[tokio::test]
#[ignore = "MVP3 final acceptance: local Compose databases, real Git, duplicate ledger and outage recovery; stop ANISP servers and run serially"]
async fn real_mvp3_full_chain_duplicate_scan_and_two_commit_recovery() {
    let config = AppConfig::from_env().unwrap();
    assert_eq!(
        config.clickhouse.database(),
        "anisp",
        "batch assertions use default Compose databases"
    );
    assert!(config.clickhouse.url().starts_with("http://127.0.0.1:8123"));
    let ch = ClickHouseStore::new(&config.clickhouse);
    ch.ping().await.unwrap();
    let pg = PostgresStore::connect(&config.postgres).await.unwrap();
    pg.ping().await.unwrap();
    let outbox = Arc::new(PostgresGitOutboxRepository::new(&pg));
    let ingest = GitIngestService::new(outbox.clone());
    let events = EventService::new(Arc::new(ClickHouseEventRepository::new(&ch)));
    let git = GitContextQueryService::new(Arc::new(ClickHouseGitCommitReader::new(&ch)));
    let support_repo = Arc::new(PostgresSupportCaseRepository::new(&pg));
    let support = SupportService::new(support_repo.clone());
    let context = SupportCaseContextService::new(
        support.clone(),
        events.clone(),
        support_repo.clone(),
        git.clone(),
        outbox.clone(),
        ContextGitConfig::default(),
    );
    let app = build_app(AppState::new(
        config,
        ch.clone(),
        pg,
        events.clone(),
        support,
        context,
        SupportOutboxStatusService::new(support_repo.clone()),
        git,
        GitOutboxStatusService::new(outbox.clone(), Duration::from_secs(30)).unwrap(),
    ));
    let publisher = GitOutboxPublisher::new(
        outbox.clone(),
        events.clone(),
        GitPublisherOptions::new(500, Duration::from_secs(1), Duration::from_secs(30), 100)
            .unwrap(),
    );
    let support_publisher = SupportOutboxPublisher::new(
        support_repo,
        events,
        Duration::from_secs(1),
        500,
        Duration::from_secs(30),
        10,
    );
    // Automated runs use an isolated label to be repeatable without deleting
    // published ledgers. The documented/manual acceptance uses exact xpa labels.
    let project = format!("xpa-mvp3-final-{}", Uuid::now_v7());
    let repos = [
        format!("final-a-{}", Uuid::now_v7()),
        format!("final-b-{}", Uuid::now_v7()),
    ];
    let directories = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let natives = directories
        .iter()
        .map(|dir| {
            Repository::init_opts(
                dir.path(),
                RepositoryInitOptions::new().initial_head("main"),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let t = Utc::now().timestamp();
    let files = [("finance.txt".into(), b"finance\n".to_vec())];
    let old = commit(&natives[0], &[], &files, "OLD commit", t - 172800);
    let a = commit(&natives[0], &[old], &files, "Commit A", t - 180);
    let b = commit(&natives[1], &[], &files, "Commit B", t - 120);
    let c = commit(&natives[0], &[a], &files, "Commit C", t - 60);
    let collectors = repos
        .iter()
        .zip(&directories)
        .map(|(id, dir)| {
            LocalGitCollector::new(
                GitRepository::new(
                    id,
                    id,
                    None,
                    Some(project.clone()),
                    Some("xpa-finance".into()),
                )
                .unwrap(),
                dir.path(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    for collector in &collectors {
        for observation in collector.collect_recent_commits(Some("main"), 10).unwrap() {
            assert!(matches!(
                ingest.ingest(observation).await.unwrap(),
                GitIngestOutcome::Accepted(_)
            ));
        }
    }
    let case = request(&app, "POST", "/api/v1/support/cases", Some(json!({"case_type":"incident","title":"xpa-finance unavailable","description":"Final acceptance; association is not causality.","reporter_id":"mvp3-final","project_id":project,"service":"xpa-finance","environment":"prod","priority":"high"})), StatusCode::CREATED).await;
    let id = case["case_id"].as_str().unwrap();
    let created: DateTime<Utc> = serde_json::from_value(case["created_at"].clone()).unwrap();
    let future = commit(
        &natives[1],
        &[b],
        &files,
        "FUTURE commit",
        created.timestamp() + 3600,
    );
    ingest
        .ingest(
            collectors[1]
                .collect_commit(&future.to_string(), Some("main"))
                .unwrap(),
        )
        .await
        .unwrap();
    publisher.publish_batch().await.unwrap();
    support_publisher.publish_batch().await.unwrap();
    let context_uri = format!("/api/v1/support/cases/{id}/context");
    let initial = get(&app, &context_uri).await;
    let commits = &initial["git_context"]["commits"];
    assert_eq!(
        commits
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["commit_sha"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [a.to_string(), b.to_string(), c.to_string()]
    );
    assert_eq!(initial["git_context"]["status"], "synced");
    assert_eq!(initial["git_context"]["pending_events"], 0);
    let before = batch_snapshot(&repos, false);
    let ch_before = batch_snapshot(&repos, true);
    assert_eq!(before["rows"], 5);
    assert_eq!(before["pending"], 0);
    assert_eq!(aggregate_count(&ch_before["rows"]), 5);
    assert_eq!(ch_before["rows"], ch_before["distinct_events"]);
    for native in &natives {
        native
            .branch(
                "rescan",
                &native.head().unwrap().peel_to_commit().unwrap(),
                false,
            )
            .unwrap();
    }
    for collector in &collectors {
        for observation in collector
            .collect_recent_commits(Some("rescan"), 10)
            .unwrap()
        {
            let key = format!(
                "{}:{}",
                observation.repository().repository_id(),
                observation.commit().commit_sha()
            );
            let outcome = ingest.ingest(observation).await.unwrap();
            assert!(matches!(outcome, GitIngestOutcome::AlreadyIngested(_)));
            assert_eq!(
                outcome.event_id().to_string(),
                before["ids"][&key].as_str().unwrap()
            );
        }
    }
    publisher.publish_batch().await.unwrap();
    assert_eq!(batch_snapshot(&repos, false), before);
    assert_eq!(batch_snapshot(&repos, true), ch_before);
    // The first-observation branch remains main in BOTH the durable ledger and
    // the published views after a full rescan through another branch.
    assert!(
        before["branches"]
            .as_object()
            .unwrap()
            .values()
            .all(|b| b == "main")
    );
    let list_uri = format!("/api/v1/git/commits?project_id={project}&service=xpa-finance");
    assert!(
        get(&app, &list_uri).await["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v["branch"] == "main")
    );
    let restore = RestoreClickHouse;
    assert!(compose("stop"));
    let baseline = get(&app, "/api/v1/internal/git/outbox/status").await["pending_events"]
        .as_u64()
        .unwrap();
    let d = commit(
        &natives[0],
        &[c],
        &files,
        "Commit D",
        created.timestamp() + 2,
    );
    let e = commit(
        &natives[1],
        &[future],
        &files,
        "Commit E",
        created.timestamp() + 3,
    );
    for (collector, oid) in [(&collectors[0], d), (&collectors[1], e)] {
        assert!(matches!(
            ingest
                .ingest(
                    collector
                        .collect_commit(&oid.to_string(), Some("main"))
                        .unwrap()
                )
                .await
                .unwrap(),
            GitIngestOutcome::Accepted(_)
        ));
    }
    assert_eq!(batch_snapshot(&repos, false)["pending"], 2);
    assert_eq!(
        get(&app, "/api/v1/internal/git/outbox/status").await["pending_events"],
        baseline + 2
    );
    assert!(publisher.publish_batch().await.unwrap().failed_count >= 2);
    get(&app, &format!("/api/v1/support/cases/{id}")).await;
    request(
        &app,
        "GET",
        &context_uri,
        None,
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;
    request(&app, "GET", "/ready", None, StatusCode::SERVICE_UNAVAILABLE).await;
    get(&app, "/health").await;
    drop(restore);
    // Advance only the retry clock; no long sleeps in the test suite.
    publisher
        .publish_batch_at(Utc::now() + chrono::Duration::minutes(1))
        .await
        .unwrap();
    let after = batch_snapshot(&repos, false);
    assert_eq!(after["rows"], 7);
    assert_eq!(after["pending"], 0);
    let after_ch = batch_snapshot(&repos, true);
    assert_eq!(aggregate_count(&after_ch["rows"]), 7);
    assert_eq!(after_ch["rows"], after_ch["distinct_events"]);
    let list = get(&app, &list_uri).await;
    assert!(
        list["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["commit_sha"] == d.to_string())
    );
    assert!(
        list["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["commit_sha"] == e.to_string())
    );
    let final_context = get(&app, &context_uri).await;
    assert_eq!(final_context["git_context"], initial["git_context"]);
    assert_eq!(final_context["history_sync"], initial["history_sync"]);
    get(&app, "/ready").await;
    println!(
        "MVP3_FINAL_E2E PASS project={project} case_id={id}: A/B/C only ASC across two repositories; duplicate scan 5 ledger rows + 5 distinct events with identical IDs/main branch; CH down D/E accepted pending=2 and plain Case GET=200; recovery pending=0 and 7 rows=7 distinct IDs; fixed-window Context stays A/B/C synced; all ledger rows retained"
    );
}
