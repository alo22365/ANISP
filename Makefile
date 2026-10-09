.PHONY: infra-up infra-down migrate run check test test-integration seed-events ingest-local-git

COMPOSE := docker compose -f deploy/docker/docker-compose.yml

infra-up:
	$(COMPOSE) up -d --wait clickhouse postgres

infra-down:
	$(COMPOSE) down

migrate:
	./scripts/migrate.sh

run:
	cargo run -p anisp-server

check:
	cargo fmt --all -- --check
	cargo check --workspace

test:
	cargo test --workspace

test-integration:
	$(MAKE) infra-up
	$(MAKE) migrate
	# Stop ANISP servers first; ignored recovery tests stop/restore ClickHouse.
	cargo test --workspace -- --ignored --test-threads=1

seed-events:
	./scripts/seed-events.sh

# Pass ANISP_GIT_LOCAL_PATH and a stable ANISP_GIT_REPOSITORY_ID in the environment.
ingest-local-git:
	./scripts/ingest-local-git.sh
