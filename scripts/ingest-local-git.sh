#!/usr/bin/env bash
# One bounded local scan; configuration/credentials are loaded by Rust.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
: "${ANISP_GIT_LOCAL_PATH:?Set ANISP_GIT_LOCAL_PATH to a local repository path}"
: "${ANISP_GIT_REPOSITORY_ID:?Set a stable ANISP_GIT_REPOSITORY_ID across scans}"

cd "$repo_root"
exec cargo run -p anisp-postgres-store --example ingest_local_git -- \
  "$ANISP_GIT_LOCAL_PATH" "$ANISP_GIT_REPOSITORY_ID" \
  "${ANISP_GIT_PROJECT_ID:--}" "${ANISP_GIT_SERVICE:--}" \
  "${ANISP_GIT_BRANCH:--}" "${ANISP_GIT_LIMIT:-100}"
