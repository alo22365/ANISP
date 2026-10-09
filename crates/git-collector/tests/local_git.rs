use std::error::Error;

use anisp_git_collector::{CollectorError, LocalGitCollector, MAX_RECENT_COMMITS};
use anisp_git_context::{ChangeType, GitContextError, GitRepository};
use chrono::Utc;
use git2::{ObjectType, Oid, Repository, RepositoryInitOptions, Signature, Time};
use tempfile::TempDir;

const BASE_TIME: i64 = 1_700_000_000;

struct Fixture {
    directory: TempDir,
    native: Repository,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let mut options = RepositoryInitOptions::new();
        options.initial_head("main");
        let native = Repository::init_opts(directory.path(), &options).unwrap();
        Self { directory, native }
    }

    // Build real Git objects and refs in an isolated temporary repository.
    // Nothing invokes the Git CLI or depends on the user's global identity.
    fn commit(
        &self,
        reference: &str,
        parents: &[Oid],
        files: &[(&str, &[u8])],
        seconds: i64,
    ) -> Oid {
        self.commit_with_author(
            reference,
            parents,
            files,
            seconds,
            "Test Author",
            "change\n",
        )
    }

    fn commit_with_author(
        &self,
        reference: &str,
        parents: &[Oid],
        files: &[(&str, &[u8])],
        seconds: i64,
        author_name: &str,
        message: &str,
    ) -> Oid {
        let mut builder = self.native.treebuilder(None).unwrap();
        for (name, bytes) in files {
            let blob = self.native.blob(bytes).unwrap();
            builder.insert(name, blob, 0o100644).unwrap();
        }
        let tree = self.native.find_tree(builder.write().unwrap()).unwrap();
        let author = Signature::new(
            author_name,
            "author@example.com",
            &Time::new(seconds - 30, 480),
        )
        .unwrap();
        let committer = Signature::new(
            "Test Committer",
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
                Some(reference),
                &author,
                &committer,
                message,
                &tree,
                &parent_refs,
            )
            .unwrap()
    }

    fn collector(&self) -> LocalGitCollector {
        LocalGitCollector::new(domain_repository(), self.directory.path()).unwrap()
    }
}

fn domain_repository() -> GitRepository {
    GitRepository::new(
        "repo-xpa",
        "xpa-finance",
        Some("https://example.invalid/never-fetch.git".to_owned()),
        Some("xpa".to_owned()),
        Some("xpa-finance".to_owned()),
    )
    .unwrap()
}

#[test]
fn root_commit_add_and_full_metadata() {
    let fixture = Fixture::new();
    let id = fixture.commit("HEAD", &[], &[("root.txt", b"one\ntwo\n")], BASE_TIME);
    let collector = fixture.collector();
    let before = Utc::now();
    let observed = collector
        .collect_commit(&id.to_string().to_uppercase(), None)
        .unwrap();
    let after = Utc::now();
    let commit = observed.commit();

    assert_eq!(observed.repository(), &domain_repository());
    assert_eq!(commit.repository_id(), "repo-xpa");
    assert_eq!(commit.commit_sha().as_str(), id.to_string());
    assert!(commit.parent_shas().is_empty());
    assert_eq!(commit.author_name(), "Test Author");
    assert_eq!(commit.author_email(), Some("author@example.com"));
    assert_eq!(commit.message(), "change\n");
    assert_eq!(commit.committed_at().timestamp(), BASE_TIME);
    assert_eq!(commit.committed_at().timestamp_subsec_nanos(), 0);
    assert!(observed.observed_at() >= before && observed.observed_at() <= after);
    assert_ne!(observed.observed_at(), commit.committed_at());
    assert_eq!(observed.branch(), Some("main"));
    let changes = commit.changes();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].change_type(), ChangeType::Added);
    assert_eq!(changes[0].path(), "root.txt");
    assert_eq!(changes[0].old_path(), None);
    assert_eq!(changes[0].additions(), Some(2));
    assert_eq!(changes[0].deletions(), Some(0));
}

#[test]
fn modify_and_add_use_first_parent_and_per_file_counts() {
    let fixture = Fixture::new();
    let root = fixture.commit("HEAD", &[], &[("existing.txt", b"old\n")], BASE_TIME);
    let id = fixture.commit(
        "HEAD",
        &[root],
        &[("existing.txt", b"new\nextra\n"), ("added.txt", b"added\n")],
        BASE_TIME + 1,
    );
    let observed = fixture
        .collector()
        .collect_commit(&id.to_string(), None)
        .unwrap();
    assert_eq!(
        observed.commit().parent_shas()[0].as_str(),
        root.to_string()
    );
    let changes = observed.commit().changes();
    assert_eq!(changes.len(), 2);
    let modified = changes
        .iter()
        .find(|change| change.path() == "existing.txt")
        .unwrap();
    assert_eq!(modified.change_type(), ChangeType::Modified);
    assert_eq!(modified.additions(), Some(2));
    assert_eq!(modified.deletions(), Some(1));
    let added = changes
        .iter()
        .find(|change| change.path() == "added.txt")
        .unwrap();
    assert_eq!(added.change_type(), ChangeType::Added);
    assert_eq!(added.additions(), Some(1));
    assert_eq!(added.deletions(), Some(0));
}

#[test]
fn rename_detection_preserves_old_and_new_path() {
    let fixture = Fixture::new();
    let root = fixture.commit(
        "HEAD",
        &[],
        &[("old name.txt", b"same\ncontent\n")],
        BASE_TIME,
    );
    let id = fixture.commit(
        "HEAD",
        &[root],
        &[("new name.txt", b"same\ncontent\n")],
        BASE_TIME + 1,
    );
    // A user's diff.renames=false must not disable the adapter's contract.
    fixture
        .native
        .config()
        .unwrap()
        .set_bool("diff.renames", false)
        .unwrap();
    let observed = fixture
        .collector()
        .collect_commit(&id.to_string(), None)
        .unwrap();
    let changes = observed.commit().changes();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].change_type(), ChangeType::Renamed);
    assert_eq!(changes[0].path(), "new name.txt");
    assert_eq!(changes[0].old_path(), Some("old name.txt"));
    assert_eq!(changes[0].additions(), Some(0));
    assert_eq!(changes[0].deletions(), Some(0));
}

#[test]
fn delete_uses_previous_path_and_counts_deleted_lines() {
    let fixture = Fixture::new();
    let root = fixture.commit("HEAD", &[], &[("deleted.txt", b"one\ntwo\n")], BASE_TIME);
    let id = fixture.commit("HEAD", &[root], &[], BASE_TIME + 1);
    let observed = fixture
        .collector()
        .collect_commit(&id.to_string(), None)
        .unwrap();
    let changes = observed.commit().changes();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].change_type(), ChangeType::Deleted);
    assert_eq!(changes[0].path(), "deleted.txt");
    assert_eq!(changes[0].additions(), Some(0));
    assert_eq!(changes[0].deletions(), Some(2));
}

#[test]
fn binary_changes_have_unknown_line_counts() {
    let fixture = Fixture::new();
    let root = fixture.commit("HEAD", &[], &[("data.bin", b"\0\x01\x02binary")], BASE_TIME);
    let modified = fixture.commit(
        "HEAD",
        &[root],
        &[("data.bin", b"\0\x03\x04binary")],
        BASE_TIME + 1,
    );
    let renamed = fixture.commit(
        "HEAD",
        &[modified],
        &[("renamed.bin", b"\0\x03\x04binary")],
        BASE_TIME + 2,
    );
    let deleted = fixture.commit("HEAD", &[renamed], &[], BASE_TIME + 3);
    for (id, change_type) in [
        (root, ChangeType::Added),
        (modified, ChangeType::Modified),
        (renamed, ChangeType::Renamed),
        (deleted, ChangeType::Deleted),
    ] {
        let observed = fixture
            .collector()
            .collect_commit(&id.to_string(), None)
            .unwrap();
        let changes = observed.commit().changes();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].change_type(), change_type);
        assert_eq!(changes[0].additions(), None);
        assert_eq!(changes[0].deletions(), None);
    }
}

#[test]
fn empty_text_file_has_known_zero_counts() {
    let fixture = Fixture::new();
    let id = fixture.commit("HEAD", &[], &[("empty.txt", b"")], BASE_TIME);
    let observed = fixture
        .collector()
        .collect_commit(&id.to_string(), None)
        .unwrap();
    assert_eq!(observed.commit().changes()[0].additions(), Some(0));
    assert_eq!(observed.commit().changes()[0].deletions(), Some(0));
}

#[test]
fn branch_resolution_and_head_use_requested_observation_path() {
    let fixture = Fixture::new();
    let root = fixture.commit("HEAD", &[], &[], BASE_TIME);
    fixture
        .native
        .branch(
            "feature/test",
            &fixture.native.find_commit(root).unwrap(),
            false,
        )
        .unwrap();
    let main = fixture.commit("HEAD", &[root], &[("main.txt", b"main\n")], BASE_TIME + 1);
    let feature = fixture.commit(
        "refs/heads/feature/test",
        &[root],
        &[("feature.txt", b"feature\n")],
        BASE_TIME + 2,
    );
    let collector = fixture.collector();
    let main_items = collector.collect_recent_commits(None, 1).unwrap();
    assert_eq!(
        main_items[0].commit().commit_sha().as_str(),
        main.to_string()
    );
    assert_eq!(main_items[0].branch(), Some("main"));
    for name in ["feature/test", "refs/heads/feature/test"] {
        let items = collector.collect_recent_commits(Some(name), 1).unwrap();
        assert_eq!(items[0].commit().commit_sha().as_str(), feature.to_string());
        assert_eq!(items[0].branch(), Some("feature/test"));
    }
    // Same commit may be observed through either branch. No membership claim.
    assert_eq!(
        collector
            .collect_commit(&root.to_string(), Some("main"))
            .unwrap()
            .branch(),
        Some("main")
    );
    assert_eq!(
        collector
            .collect_commit(&root.to_string(), Some("feature/test"))
            .unwrap()
            .branch(),
        Some("feature/test")
    );
}

#[test]
fn detached_head_returns_no_branch_for_both_methods() {
    let fixture = Fixture::new();
    let id = fixture.commit("HEAD", &[], &[], BASE_TIME);
    fixture.native.set_head_detached(id).unwrap();
    let collector = fixture.collector();
    assert_eq!(
        collector
            .collect_commit(&id.to_string(), None)
            .unwrap()
            .branch(),
        None
    );
    let items = collector.collect_recent_commits(None, 1).unwrap();
    assert_eq!(items[0].branch(), None);
    assert_eq!(items[0].commit().commit_sha().as_str(), id.to_string());
}

#[test]
fn recent_commits_are_newest_first_and_bounded() {
    let fixture = Fixture::new();
    let root = fixture.commit("HEAD", &[], &[], BASE_TIME);
    let middle = fixture.commit("HEAD", &[root], &[], BASE_TIME + 1);
    let tip = fixture.commit("HEAD", &[middle], &[], BASE_TIME + 2);
    let collector = fixture.collector();
    let items = collector.collect_recent_commits(None, 2).unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].commit().commit_sha().as_str(), tip.to_string());
    assert_eq!(items[1].commit().commit_sha().as_str(), middle.to_string());
    assert_eq!(
        collector.collect_recent_commits(None, 500).unwrap().len(),
        3
    );
}

#[test]
fn maximum_limit_and_invalid_limits() {
    let fixture = Fixture::new();
    let collector = fixture.collector();
    for limit in [0, MAX_RECENT_COMMITS + 1, usize::MAX] {
        assert_eq!(
            collector.collect_recent_commits(None, limit).unwrap_err(),
            CollectorError::InvalidLimit {
                limit,
                maximum: 500
            }
        );
    }
    let mut parents = Vec::new();
    for offset in 0..505 {
        let id = fixture.commit("HEAD", &parents, &[], BASE_TIME + offset);
        parents = vec![id];
    }
    assert_eq!(
        collector.collect_recent_commits(None, 500).unwrap().len(),
        500
    );
}

#[test]
fn bounded_walk_does_not_read_the_entire_history() {
    let fixture = Fixture::new();
    let tree_id = fixture.native.treebuilder(None).unwrap().write().unwrap();
    // A valid first parent with a missing older ancestor simulates a shallow or
    // incomplete object store. Taking one item must not traverse that ancestor.
    let raw = format!(
        "tree {tree_id}\nparent {}\nauthor A <a@example.com> {BASE_TIME} +0000\ncommitter C <c@example.com> {BASE_TIME} +0000\n\nancestor\n",
        "1".repeat(40)
    );
    let parent = fixture
        .native
        .odb()
        .unwrap()
        .write(ObjectType::Commit, raw.as_bytes())
        .unwrap();
    let tip = fixture.commit("HEAD", &[parent], &[], BASE_TIME + 1);
    let items = fixture.collector().collect_recent_commits(None, 1).unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].commit().commit_sha().as_str(), tip.to_string());
}

#[test]
fn merge_preserves_all_parents_and_diffs_only_first_parent() {
    let fixture = Fixture::new();
    let base = fixture.commit("HEAD", &[], &[("base.txt", b"base\n")], BASE_TIME);
    fixture
        .native
        .branch("feature", &fixture.native.find_commit(base).unwrap(), false)
        .unwrap();
    let first = fixture.commit(
        "HEAD",
        &[base],
        &[("base.txt", b"base\n"), ("main.txt", b"main\n")],
        BASE_TIME + 1,
    );
    let second = fixture.commit(
        "refs/heads/feature",
        &[base],
        &[("base.txt", b"base\n"), ("feature.txt", b"feature\n")],
        BASE_TIME + 2,
    );
    let merge = fixture.commit(
        "HEAD",
        &[first, second],
        &[
            ("base.txt", b"base\n"),
            ("main.txt", b"main\n"),
            ("feature.txt", b"feature\n"),
        ],
        BASE_TIME + 3,
    );
    let collector = fixture.collector();
    let observed = collector.collect_commit(&merge.to_string(), None).unwrap();
    assert_eq!(
        observed
            .commit()
            .parent_shas()
            .iter()
            .map(|sha| sha.as_str())
            .collect::<Vec<_>>(),
        vec![first.to_string(), second.to_string()]
    );
    let changes = observed.commit().changes();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].path(), "feature.txt");
    assert_eq!(changes[0].change_type(), ChangeType::Added);
    assert_eq!(changes[0].additions(), Some(1));
    let items = collector.collect_recent_commits(None, 4).unwrap();
    assert_eq!(
        items
            .iter()
            .map(|item| item.commit().commit_sha().as_str())
            .collect::<Vec<_>>(),
        vec![
            merge.to_string(),
            second.to_string(),
            first.to_string(),
            base.to_string()
        ]
    );
}

#[test]
fn empty_message_is_valid_but_empty_author_cannot_bypass_domain() {
    let fixture = Fixture::new();
    let root = fixture.commit_with_author("HEAD", &[], &[], BASE_TIME, "A", "");
    assert_eq!(
        fixture
            .collector()
            .collect_commit(&root.to_string(), None)
            .unwrap()
            .commit()
            .message(),
        ""
    );
    // Native signature constructors reject empty names; an existing malformed
    // persisted commit can still carry one. Read it through the adapter.
    let tree_id = fixture.native.find_commit(root).unwrap().tree_id();
    let raw = format!(
        "tree {tree_id}\nparent {root}\nauthor  <a@example.com> {BASE_TIME} +0000\ncommitter C <c@example.com> {BASE_TIME} +0000\n\nmessage\n"
    );
    let id = fixture
        .native
        .odb()
        .unwrap()
        .write(ObjectType::Commit, raw.as_bytes())
        .unwrap();
    assert_eq!(
        fixture
            .collector()
            .collect_commit(&id.to_string(), None)
            .unwrap_err(),
        CollectorError::InvalidDomainData(GitContextError::EmptyAuthorName)
    );
}

#[test]
fn invalid_and_missing_sha_and_branch_are_stable_errors() {
    let fixture = Fixture::new();
    let root = fixture.commit("HEAD", &[], &[], BASE_TIME);
    let collector = fixture.collector();
    for sha in ["", "abc", &"z".repeat(40)] {
        assert_eq!(
            collector.collect_commit(sha, None).unwrap_err(),
            CollectorError::InvalidCommitSha
        );
    }
    assert_eq!(
        collector.collect_commit(&"a".repeat(64), None).unwrap_err(),
        CollectorError::UnsupportedObjectFormat
    );
    let missing = collector.collect_commit(&"f".repeat(40), None).unwrap_err();
    assert_eq!(missing, CollectorError::CommitNotFound);
    assert!(missing.source().is_none());
    assert_eq!(
        collector
            .collect_commit(&root.to_string(), Some("missing"))
            .unwrap_err(),
        CollectorError::BranchNotFound
    );
    for branch in ["", "main~1", "main..other", "a b", "refs/heads/"] {
        assert_eq!(
            collector
                .collect_recent_commits(Some(branch), 1)
                .unwrap_err(),
            CollectorError::InvalidBranch
        );
    }
}

#[test]
fn empty_repository_has_stable_head_unavailable_error() {
    let fixture = Fixture::new();
    assert_eq!(
        fixture
            .collector()
            .collect_recent_commits(None, 1)
            .unwrap_err(),
        CollectorError::HeadUnavailable
    );
}

#[test]
fn opens_git_directory_and_bare_repository_without_ancestor_discovery() {
    let fixture = Fixture::new();
    let id = fixture.commit("HEAD", &[], &[], BASE_TIME);
    let collector = LocalGitCollector::new(domain_repository(), fixture.native.path()).unwrap();
    assert_eq!(
        collector
            .collect_commit(&id.to_string(), None)
            .unwrap()
            .commit()
            .commit_sha()
            .as_str(),
        id.to_string()
    );
    let child = fixture.directory.path().join("not-a-repository");
    std::fs::create_dir(&child).unwrap();
    assert!(matches!(
        LocalGitCollector::new(domain_repository(), &child),
        Err(CollectorError::RepositoryUnavailable)
    ));
    assert!(matches!(
        LocalGitCollector::new(domain_repository(), child.join("missing")),
        Err(CollectorError::RepositoryUnavailable)
    ));
    let bare_directory = tempfile::tempdir().unwrap();
    let mut options = RepositoryInitOptions::new();
    options.bare(true).initial_head("main");
    let bare = Repository::init_opts(bare_directory.path(), &options).unwrap();
    let tree_id = bare.treebuilder(None).unwrap().write().unwrap();
    let tree = bare.find_tree(tree_id).unwrap();
    let signature = Signature::new("A", "a@example.com", &Time::new(BASE_TIME, 0)).unwrap();
    let id = bare
        .commit(Some("HEAD"), &signature, &signature, "bare", &tree, &[])
        .unwrap();
    let collector = LocalGitCollector::new(domain_repository(), bare_directory.path()).unwrap();
    assert_eq!(
        collector
            .collect_commit(&id.to_string(), None)
            .unwrap()
            .commit()
            .commit_sha()
            .as_str(),
        id.to_string()
    );
}

#[test]
fn invalid_utf8_message_is_not_lossily_replaced() {
    let fixture = Fixture::new();
    let tree_id = fixture.native.treebuilder(None).unwrap().write().unwrap();
    let mut raw = format!("tree {tree_id}\nauthor A <a@example.com> {BASE_TIME} +0000\ncommitter C <c@example.com> {BASE_TIME} +0000\n\n").into_bytes();
    raw.extend_from_slice(b"invalid \xff\n");
    let id = fixture
        .native
        .odb()
        .unwrap()
        .write(ObjectType::Commit, &raw)
        .unwrap();
    fixture
        .native
        .reference("refs/heads/main", id, true, "fixture")
        .unwrap();
    assert_eq!(
        fixture
            .collector()
            .collect_commit(&id.to_string(), None)
            .unwrap_err(),
        CollectorError::InvalidEncoding { field: "message" }
    );
}

#[test]
fn rename_with_edits_has_per_file_stats() {
    let fixture = Fixture::new();
    let original = b"one\ntwo\nthree\nfour\nfive\nsix\nseven\neight\nnine\nten\n";
    let updated = b"one\ntwo\nthree\nfour\nfive\nsix\nseven\neight\nnine\nchanged\n";
    let root = fixture.commit("HEAD", &[], &[("before.txt", original)], BASE_TIME);
    let id = fixture.commit("HEAD", &[root], &[("after.txt", updated)], BASE_TIME + 1);
    let observed = fixture
        .collector()
        .collect_commit(&id.to_string(), None)
        .unwrap();
    let changes = observed.commit().changes();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].change_type(), ChangeType::Renamed);
    assert_eq!(changes[0].old_path(), Some("before.txt"));
    assert_eq!(changes[0].path(), "after.txt");
    assert_eq!(changes[0].additions(), Some(1));
    assert_eq!(changes[0].deletions(), Some(1));
}

#[test]
fn empty_author_email_maps_to_none() {
    let fixture = Fixture::new();
    let tree_id = fixture.native.treebuilder(None).unwrap().write().unwrap();
    let raw = format!(
        "tree {tree_id}\nauthor A <> {BASE_TIME} +0000\ncommitter C <c@example.com> {BASE_TIME} +0000\n\nmessage\n"
    );
    let id = fixture
        .native
        .odb()
        .unwrap()
        .write(ObjectType::Commit, raw.as_bytes())
        .unwrap();
    fixture
        .native
        .reference("refs/heads/main", id, true, "fixture")
        .unwrap();
    let observed = fixture
        .collector()
        .collect_commit(&id.to_string(), None)
        .unwrap();
    assert_eq!(observed.commit().author_name(), "A");
    assert_eq!(observed.commit().author_email(), None);
}

#[test]
fn file_paths_go_through_utf8_and_domain_validation() {
    let fixture = Fixture::new();
    for (filename, expected) in [
        (
            b"invalid-\xff.txt".as_slice(),
            CollectorError::InvalidEncoding { field: "file_path" },
        ),
        (
            b"   ".as_slice(),
            CollectorError::InvalidDomainData(GitContextError::EmptyFilePath),
        ),
    ] {
        let mut builder = fixture.native.treebuilder(None).unwrap();
        let blob = fixture.native.blob(b"line\n").unwrap();
        builder.insert(filename, blob, 0o100644).unwrap();
        let tree = fixture.native.find_tree(builder.write().unwrap()).unwrap();
        let signature = Signature::new("A", "a@example.com", &Time::new(BASE_TIME, 0)).unwrap();
        let id = fixture
            .native
            .commit(None, &signature, &signature, "root", &tree, &[])
            .unwrap();
        fixture
            .native
            .reference("refs/heads/main", id, true, "fixture")
            .unwrap();
        assert_eq!(
            fixture
                .collector()
                .collect_commit(&id.to_string(), None)
                .unwrap_err(),
            expected
        );
    }
}

#[test]
fn gitlink_counts_are_unknown_not_synthetic_diff_lines() {
    let fixture = Fixture::new();
    let base = fixture.commit("HEAD", &[], &[], BASE_TIME);
    let mut builder = fixture.native.treebuilder(None).unwrap();
    builder.insert("submodule", base, 0o160000).unwrap();
    let tree = fixture.native.find_tree(builder.write().unwrap()).unwrap();
    let signature = Signature::new("A", "a@example.com", &Time::new(BASE_TIME + 1, 0)).unwrap();
    let parent = fixture.native.find_commit(base).unwrap();
    let id = fixture
        .native
        .commit(
            Some("HEAD"),
            &signature,
            &signature,
            "submodule",
            &tree,
            &[&parent],
        )
        .unwrap();
    let observed = fixture
        .collector()
        .collect_commit(&id.to_string(), None)
        .unwrap();
    let change = &observed.commit().changes()[0];
    assert_eq!(change.path(), "submodule");
    assert_eq!(change.change_type(), ChangeType::Added);
    assert_eq!(change.additions(), None);
    assert_eq!(change.deletions(), None);
}

#[test]
fn linked_worktree_resolves_its_own_head() {
    let fixture = Fixture::new();
    let id = fixture.commit("HEAD", &[], &[], BASE_TIME);
    let linked_directory = tempfile::tempdir().unwrap();
    let path = linked_directory.path().join("linked");
    fixture.native.worktree("linked", &path, None).unwrap();
    let collector = LocalGitCollector::new(domain_repository(), &path).unwrap();
    let observed = collector.collect_commit(&id.to_string(), None).unwrap();
    assert_eq!(observed.branch(), Some("linked"));
    assert_eq!(observed.commit().commit_sha().as_str(), id.to_string());
}

#[test]
fn bounded_results_are_sorted_even_when_commit_clocks_are_skewed() {
    let fixture = Fixture::new();
    let root = fixture.commit("HEAD", &[], &[], BASE_TIME + 100);
    let tip = fixture.commit("HEAD", &[root], &[], BASE_TIME);
    let items = fixture.collector().collect_recent_commits(None, 2).unwrap();
    assert_eq!(items[0].commit().commit_sha().as_str(), root.to_string());
    assert_eq!(items[1].commit().commit_sha().as_str(), tip.to_string());
}
