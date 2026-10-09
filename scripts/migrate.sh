#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
compose_file="$repo_root/deploy/docker/docker-compose.yml"
clickhouse_migration="$repo_root/migrations/clickhouse/001_context_event.sql"

# Credentials and database come from the running Compose service environment.
docker compose -f "$compose_file" exec -T clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database "$CLICKHOUSE_DB" --multiquery' \
  < "$clickhouse_migration"

printf 'Applied %s\n' "$clickhouse_migration"

for postgres_migration in "$repo_root"/migrations/postgres/*.sql; do
  docker compose -f "$compose_file" exec -T postgres sh -c \
    'psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --set ON_ERROR_STOP=1' \
    < "$postgres_migration"
  printf 'Applied %s\n' "$postgres_migration"
done
