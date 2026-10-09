# Local Git Collector V1

`anisp-git-collector` is a synchronous, on-demand infrastructure adapter from
an explicitly selected local Git repository to `anisp-git-context` Domain
objects. It uses git2/libgit2 internally; its public API exposes no native Git
types. It does not fetch remotes, run the Git CLI, poll, persist events, or
integrate with the server.

```rust,no_run
use anisp_git_collector::LocalGitCollector;
use anisp_git_context::GitRepository;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let repository = GitRepository::new("repo-xpa", "finance", None, None, None)?;
let collector = LocalGitCollector::new(repository, "/path/to/local/repository")?;
let recent = collector.collect_recent_commits(None, 100)?;
let commit = collector.collect_commit(
    "0123456789abcdef0123456789abcdef01234567",
    Some("main"),
)?;
# Ok(())
# }
```

## Resolution and identity

Paths are canonicalized and opened without ancestor discovery. A worktree
root, `.git` directory, linked worktree, or bare repository is supported.
The caller-provided Domain repository identity is retained independently of
the path and remote URL; natural commit identity remains repository ID + SHA.

Branches are local names or `refs/heads/<name>`, not arbitrary revspecs.
`None` resolves HEAD. Detached HEAD returns `branch=None`; unborn/invalid HEAD
returns `HeadUnavailable`. Branch is the observation path, not an assertion
that a commit belongs only to that branch. An explicit SHA need not be the
branch tip or exclusively reachable through that branch.

V1 uses stable SHA-1 libgit2 support. The Domain's SHA-256 support is unchanged;
a valid 64-digit SHA returns `UnsupportedObjectFormat` from this adapter.
Native git2 SSH/HTTPS features are disabled, and libgit2 is vendored. Building
requires a C compiler/toolchain as well as Rust.

## Extraction and diff semantics

All objects are created through validating Domain constructors. Author name,
optional email, raw message, and all parent SHAs are retained. `committed_at`
uses the committer's Unix timestamp converted to UTC (Git precision: seconds);
its timezone offset is not applied again. `observed_at` is the actual UTC
collection start time for each observation.

Root diff: empty tree → commit. Other diffs, including merges: first parent →
commit. Rename detection is explicitly enabled. A rename records the new
path and previous path. Type changes map to `Modified`. Text line counts are
calculated from callback origins without copying line content or retaining
patches. Binary and gitlink counts are `None`, not synthetic zeroes. No full
diff, source, or binary content is exposed or stored. libgit2 may inspect
blobs internally for binary classification, text statistics, and similarity.

## Bounded recent collection

`limit` must be 1–500. A lazy ancestry frontier starts at the selected tip,
prioritizes discovered parents by commit time, and deduplicates visits across
merges. The collector stops expanding when the requested count is reached;
it never preloads the entire history and then truncates it. Only the bounded
results are sorted by `committed_at DESC, commit_sha DESC`.

With skewed commit clocks, this is not a global newest-N timestamp search
over all reachable ancestors; that would require inspecting the full graph.
Objects needed for a selected commit's first-parent diff must still exist.
Invalid/non-UTF-8 metadata or paths fail explicitly instead of silently being
rewritten. Native errors are translated to stable `CollectorError` categories
without exposing native messages or error sources.

## Testing

```sh
cargo test -p anisp-git-collector
```

Tests build real objects and references in temporary repositories, including
bare and linked worktrees. They need neither the Git CLI nor a database.
Calling this synchronous collector from an async application should use a
blocking worker; no runtime wiring is introduced in this milestone.
