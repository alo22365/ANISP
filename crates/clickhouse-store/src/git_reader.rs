use crate::ClickHouseStore;
use anisp_git_context::{
    ChangeType, CommitSha, GitCommitInput, GitCommitQuery, GitCommitReader, GitCommitView,
    GitCommitViewInput, GitFileChange, GitReaderError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse::{Client, Row};
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

const SELECT: &str = "SELECT ?fields FROM context_event WHERE source = 'git' AND event_type = 'git.commit.created' AND subject_type = 'commit'";

#[derive(Clone)]
pub struct ClickHouseGitCommitReader {
    client: Client,
    request_timeout: Duration,
}
impl ClickHouseGitCommitReader {
    pub fn new(store: &ClickHouseStore) -> Self {
        Self {
            client: store.client.clone(),
            request_timeout: store.request_timeout,
        }
    }
}

#[async_trait]
impl GitCommitReader for ClickHouseGitCommitReader {
    async fn find_by_identity(
        &self,
        repository_id: &str,
        commit_sha: &CommitSha,
    ) -> Result<Option<GitCommitView>, GitReaderError> {
        let sql = format!(
            "{SELECT} AND subject_id = ? AND JSONExtractString(metadata_json, 'repository_id') = ? ORDER BY event_time DESC, event_id DESC LIMIT 1"
        );
        let row = timeout(
            self.request_timeout,
            self.client
                .query(&sql)
                .bind(commit_sha.as_str())
                .bind(repository_id)
                .fetch_optional::<GitEventRow>(),
        )
        .await
        .map_err(|_| GitReaderError::Timeout)?
        .map_err(client_error)?;
        row.map(GitCommitView::try_from).transpose()
    }
    async fn search(&self, query: &GitCommitQuery) -> Result<Vec<GitCommitView>, GitReaderError> {
        let filters = query.filters();
        // These clauses are static. All caller-supplied values use escaped bind.
        let mut sql = SELECT.to_owned();
        for (value, clause) in [
            (
                &filters.repository_id,
                " AND JSONExtractString(metadata_json, 'repository_id') = ?",
            ),
            (&filters.project_id, " AND project_id = ?"),
            (&filters.service, " AND service = ?"),
            (
                &filters.branch,
                " AND JSONExtractString(metadata_json, 'branch') = ?",
            ),
        ] {
            if value.is_some() {
                sql.push_str(clause);
            }
        }
        if query.from().is_some() {
            sql.push_str(" AND event_time >= fromUnixTimestamp64Milli(?)");
        }
        if query.to().is_some() {
            sql.push_str(" AND event_time < fromUnixTimestamp64Milli(?)");
        }
        sql.push_str(" ORDER BY event_time DESC, event_id DESC LIMIT ?");
        let mut request = self.client.query(&sql);
        for value in [
            &filters.repository_id,
            &filters.project_id,
            &filters.service,
            &filters.branch,
        ]
        .into_iter()
        .flatten()
        {
            request = request.bind(value);
        }
        if let Some(from) = query.from() {
            request = request.bind(from.timestamp_millis());
        }
        if let Some(to) = query.to() {
            request = request.bind(to.timestamp_millis());
        }
        let rows = timeout(
            self.request_timeout,
            request.bind(query.limit()).fetch_all::<GitEventRow>(),
        )
        .await
        .map_err(|_| GitReaderError::Timeout)?
        .map_err(client_error)?;
        rows.into_iter().map(GitCommitView::try_from).collect()
    }
}

fn client_error(error: clickhouse::error::Error) -> GitReaderError {
    use clickhouse::error::Error;
    match error {
        Error::TimedOut => GitReaderError::Timeout,
        Error::Network(_) | Error::BadResponse(_) => GitReaderError::Unavailable,
        Error::NotEnoughData
        | Error::InvalidUtf8Encoding(_)
        | Error::InvalidTagEncoding(_)
        | Error::Custom(_)
        | Error::Decompression(_) => GitReaderError::Decode,
        _ => GitReaderError::Internal,
    }
}

/// A projection-specific row, not the Domain model or a public Event DTO.
#[derive(Debug, Row, Serialize, Deserialize)]
struct GitEventRow {
    #[serde(with = "clickhouse::serde::uuid")]
    event_id: Uuid,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    event_time: DateTime<Utc>,
    project_id: String,
    service: String,
    subject_id: String,
    content: String,
    metadata_json: String,
}

// Unlike Option<T>, this wrapper rejects missing keys while accepting explicit
// JSON null. Binary/unknown stats remain None, never fabricated zeroes.
#[derive(Debug)]
struct RequiredNullable<T>(Option<T>);
impl<'de, T: DeserializeOwned> Deserialize<'de> for RequiredNullable<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // deserialize_any rejects MissingFieldDeserializer; directly delegating
        // to Option::deserialize would also accept an absent field.
        let value = serde_json::Value::deserialize(d)?;
        serde_json::from_value(value)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}
#[derive(Deserialize)]
struct GitMetadata {
    repository_id: String,
    repository_name: String,
    branch: RequiredNullable<String>,
    commit_sha: String,
    parent_shas: Vec<String>,
    author_name: String,
    author_email: RequiredNullable<String>,
    observed_at: DateTime<Utc>,
    changed_file_count: u64,
    additions: RequiredNullable<u64>,
    deletions: RequiredNullable<u64>,
    changed_files: Vec<FileMetadata>,
    changes_truncated: bool,
}
#[derive(Deserialize)]
struct FileMetadata {
    path: String,
    old_path: RequiredNullable<String>,
    change_type: String,
    additions: RequiredNullable<u64>,
    deletions: RequiredNullable<u64>,
}
impl TryFrom<GitEventRow> for GitCommitView {
    type Error = GitReaderError;
    fn try_from(row: GitEventRow) -> Result<Self, Self::Error> {
        let metadata: GitMetadata =
            serde_json::from_str(&row.metadata_json).map_err(|_| GitReaderError::Decode)?;
        if metadata.commit_sha != row.subject_id
            || CommitSha::new(&metadata.commit_sha)
                .map_err(|_| GitReaderError::Decode)?
                .as_str()
                != metadata.commit_sha
        {
            return Err(GitReaderError::Decode);
        }
        for sha in &metadata.parent_shas {
            if CommitSha::new(sha)
                .map_err(|_| GitReaderError::Decode)?
                .as_str()
                != sha
            {
                return Err(GitReaderError::Decode);
            }
        }
        let changes = metadata
            .changed_files
            .into_iter()
            .map(|file| {
                let kind = match file.change_type.as_str() {
                    "added" => ChangeType::Added,
                    "modified" => ChangeType::Modified,
                    "deleted" => ChangeType::Deleted,
                    "renamed" => ChangeType::Renamed,
                    _ => return Err(GitReaderError::Decode),
                };
                GitFileChange::new(
                    file.path,
                    file.old_path.0,
                    kind,
                    file.additions.0,
                    file.deletions.0,
                )
                .map_err(|_| GitReaderError::Decode)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(GitCommitViewInput {
            event_id: row.event_id,
            repository_name: metadata.repository_name,
            project_id: row.project_id,
            service: row.service,
            branch: metadata.branch.0,
            commit: GitCommitInput {
                repository_id: metadata.repository_id,
                commit_sha: metadata.commit_sha,
                parent_shas: metadata.parent_shas,
                author_name: metadata.author_name,
                author_email: metadata.author_email.0,
                message: row.content,
                committed_at: row.event_time,
                changes,
            },
            observed_at: metadata.observed_at,
            changed_file_count: metadata.changed_file_count,
            additions: metadata.additions.0,
            deletions: metadata.deletions.0,
            changes_truncated: metadata.changes_truncated,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anisp_git_context::GitCommitFilters;
    use clickhouse::test::{Mock, handlers};
    use serde_json::{Value, json};

    fn metadata() -> Value {
        json!({"repository_id":"repo-a", "repository_name":"finance", "branch":"main", "commit_sha":"a".repeat(40),
            "parent_shas":["b".repeat(40)], "author_name":"Author", "author_email":"author@example.com",
            "observed_at":"2026-10-07T10:00:00.123Z", "changed_file_count":2, "additions":null,"deletions":null,
            "changes_truncated":false, "changed_files":[
                {"path":"new.txt","old_path":"old.txt","change_type":"renamed","additions":1,"deletions":2},
                {"path":"binary.bin","old_path":null,"change_type":"added","additions":null,"deletions":null}]})
    }
    fn row(metadata: &str) -> GitEventRow {
        GitEventRow {
            event_id: Uuid::now_v7(),
            event_time: DateTime::from_timestamp_millis(1000).unwrap(),
            project_id: "xpa".into(),
            service: "finance".into(),
            subject_id: "a".repeat(40),
            content: "Message\n\nBody".into(),
            metadata_json: metadata.into(),
        }
    }
    fn reader(mock: &Mock) -> ClickHouseGitCommitReader {
        ClickHouseGitCommitReader::new(&ClickHouseStore::from_client(
            Client::default()
                .with_url(mock.url())
                .with_compression(clickhouse::Compression::None),
        ))
    }
    #[test]
    fn metadata_parent_author_rename_binary_and_observation() {
        let view = GitCommitView::try_from(row(&metadata().to_string())).unwrap();
        assert_eq!(view.parent_shas()[0].as_str(), "b".repeat(40));
        assert_eq!(view.author_name(), "Author");
        assert_eq!(view.author_email(), Some("author@example.com"));
        assert_eq!(view.changed_files()[0].change_type(), ChangeType::Renamed);
        assert_eq!(view.changed_files()[0].path(), "new.txt");
        assert_eq!(view.changed_files()[0].old_path(), Some("old.txt"));
        assert_eq!(view.changed_files()[1].additions(), None);
        assert_eq!(view.changed_files()[1].deletions(), None);
        assert_eq!(view.additions(), None);
        assert_ne!(view.committed_at(), view.observed_at());
    }
    #[test]
    fn truncated_views_preserve_true_total() {
        let mut m = metadata();
        m["changed_file_count"] = json!(1000);
        m["changes_truncated"] = json!(true);
        let view = GitCommitView::try_from(row(&m.to_string())).unwrap();
        assert_eq!(view.changed_file_count(), 1000);
        assert_eq!(view.changed_files().len(), 2);
        assert!(view.changes_truncated());
    }
    #[test]
    fn every_metadata_key_is_required_even_nullable_keys() {
        for key in metadata().as_object().unwrap().keys() {
            let mut m = metadata();
            m.as_object_mut().unwrap().remove(key);
            assert_eq!(
                GitCommitView::try_from(row(&m.to_string())).unwrap_err(),
                GitReaderError::Decode,
                "{key}"
            );
        }
        for key in ["path", "old_path", "change_type", "additions", "deletions"] {
            let mut m = metadata();
            m["changed_files"][0].as_object_mut().unwrap().remove(key);
            assert_eq!(
                GitCommitView::try_from(row(&m.to_string())).unwrap_err(),
                GitReaderError::Decode,
                "file.{key}"
            );
        }
    }
    #[test]
    fn malformed_types_and_invariants_are_decode_errors() {
        for raw in ["{broken", "[]", "null", "{}"] {
            assert_eq!(
                GitCommitView::try_from(row(raw)).unwrap_err(),
                GitReaderError::Decode
            );
        }
        for (key, value) in [
            ("repository_id", json!("")),
            ("repository_name", json!(" ")),
            ("branch", json!(7)),
            ("parent_shas", json!(null)),
            ("parent_shas", json!(["bad"])),
            ("author_name", json!("")),
            ("author_email", json!(false)),
            ("observed_at", json!("invalid")),
            ("changed_file_count", json!(-1)),
            ("additions", json!("0")),
            ("changed_files", json!({})),
            ("changes_truncated", json!(true)),
            ("commit_sha", json!("A".repeat(40))),
        ] {
            let mut m = metadata();
            m[key] = value;
            assert_eq!(
                GitCommitView::try_from(row(&m.to_string())).unwrap_err(),
                GitReaderError::Decode,
                "{key}"
            );
        }
        for (key, value) in [
            ("path", json!("")),
            ("old_path", json!(null)),
            ("change_type", json!("unknown")),
            ("additions", json!(-1)),
        ] {
            let mut m = metadata();
            m["changed_files"][0][key] = value;
            assert_eq!(
                GitCommitView::try_from(row(&m.to_string())).unwrap_err(),
                GitReaderError::Decode,
                "file.{key}"
            );
        }
        let mut r = row(&metadata().to_string());
        r.subject_id = "b".repeat(40);
        assert_eq!(
            GitCommitView::try_from(r).unwrap_err(),
            GitReaderError::Decode
        );
    }
    #[tokio::test]
    async fn reader_existing_missing_and_empty_list() {
        let mock = Mock::new();
        let reader = reader(&mock);
        mock.add(handlers::provide([row(&metadata().to_string())]));
        assert!(
            reader
                .find_by_identity("repo-a", &CommitSha::new("a".repeat(40)).unwrap())
                .await
                .unwrap()
                .is_some()
        );
        mock.add(handlers::provide(Vec::<GitEventRow>::new()));
        assert!(
            reader
                .find_by_identity("repo-a", &CommitSha::new("b".repeat(40)).unwrap())
                .await
                .unwrap()
                .is_none()
        );
        mock.add(handlers::provide(Vec::<GitEventRow>::new()));
        assert!(reader.search(&Default::default()).await.unwrap().is_empty());
    }
    #[tokio::test]
    async fn detail_and_list_sql_bind_identity_filters_range_and_limit() {
        let mock = Mock::new();
        let reader = reader(&mock);
        // Force the client's read-only request to POST so record_ddl captures SQL.
        let hostile = format!("' OR 1=1 --{}", "x".repeat(8200));
        let control = mock.add(handlers::record_ddl());
        reader
            .find_by_identity(&hostile, &CommitSha::new("a".repeat(40)).unwrap())
            .await
            .unwrap();
        let sql = control.query().await;
        assert!(sql.contains(
            "source = 'git' AND event_type = 'git.commit.created' AND subject_type = 'commit'"
        ));
        assert!(sql.contains(&format!("subject_id = '{}'", "a".repeat(40))));
        assert!(sql.contains("JSONExtractString(metadata_json, 'repository_id') = '\\' OR 1=1 --"));
        let q = GitCommitQuery::new(
            GitCommitFilters {
                repository_id: Some(hostile),
                project_id: Some("xpa".into()),
                service: Some("finance".into()),
                branch: Some("main".into()),
            },
            Some(DateTime::from_timestamp_millis(1000).unwrap()),
            Some(DateTime::from_timestamp_millis(2000).unwrap()),
            Some(500),
        )
        .unwrap();
        let control = mock.add(handlers::record_ddl());
        reader.search(&q).await.unwrap();
        let sql = control.query().await;
        for clause in [
            "project_id = 'xpa'",
            "service = 'finance'",
            "JSONExtractString(metadata_json, 'branch') = 'main'",
            "event_time >= fromUnixTimestamp64Milli(1000)",
            "event_time < fromUnixTimestamp64Milli(2000)",
            "ORDER BY event_time DESC, event_id DESC LIMIT 500",
        ] {
            assert!(sql.contains(clause), "{clause}: {sql}");
        }
    }
    #[tokio::test]
    async fn corrupt_projection_is_decode_error_not_empty_result() {
        let mock = Mock::new();
        let reader = reader(&mock);
        mock.add(handlers::provide([row("{}")]));
        assert_eq!(
            reader.search(&Default::default()).await.unwrap_err(),
            GitReaderError::Decode
        );
    }
    #[tokio::test]
    async fn adapter_timeout_applies_to_both_operations_without_long_sleep() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let reader = ClickHouseGitCommitReader {
            client: Client::default()
                .with_url(format!("http://{}", listener.local_addr().unwrap())),
            request_timeout: Duration::from_millis(5),
        };
        assert_eq!(
            reader.search(&Default::default()).await.unwrap_err(),
            GitReaderError::Timeout
        );
        assert_eq!(
            reader
                .find_by_identity("repo", &CommitSha::new("a".repeat(40)).unwrap())
                .await
                .unwrap_err(),
            GitReaderError::Timeout
        );
    }
    #[test]
    fn opaque_native_error_categories() {
        assert_eq!(
            client_error(clickhouse::error::Error::TimedOut),
            GitReaderError::Timeout
        );
        assert_eq!(
            client_error(clickhouse::error::Error::BadResponse("secret".into())),
            GitReaderError::Unavailable
        );
        assert_eq!(
            client_error(clickhouse::error::Error::NotEnoughData),
            GitReaderError::Decode
        );
        assert_eq!(
            client_error(clickhouse::error::Error::InvalidParams(
                std::io::Error::other("secret").into()
            )),
            GitReaderError::Internal
        );
    }
}
