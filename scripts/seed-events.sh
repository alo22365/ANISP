#!/usr/bin/env bash
set -euo pipefail

for command in curl jq; do
  if ! command -v "$command" >/dev/null 2>&1; then
    printf 'Required command not found: %s\n' "$command" >&2
    exit 1
  fi
done

base_url=${ANISP_BASE_URL:-http://127.0.0.1:${ANISP_SERVER_PORT:-3000}}
base_url=${base_url%/}
timestamp=$(date -u +%Y-%m-%dT%H:%M:%S)
batch_id="seed-$(date -u +%Y%m%dT%H%M%S)-$$"

event_types=(
  git.commit.created
  cicd.build.started
  cicd.build.success
  cicd.deploy.success
  k8s.pod.restart
  log.error.detected
)

printf 'seed_subject_id=%s\n' "$batch_id"
for index in "${!event_types[@]}"; do
  sequence=$((index + 1))
  event_type=${event_types[$index]}
  event_time=$(printf '%s.%03dZ' "$timestamp" "$sequence")
  payload=$(jq -nc \
    --arg event_time "$event_time" \
    --arg event_type "$event_type" \
    --arg subject_id "$batch_id" \
    --argjson sequence "$sequence" \
    '{event_time:$event_time,source:"demo_seed",event_type:$event_type,project_id:"xpa",service:"xpa-finance",environment:"prod",subject_type:"demo_batch",subject_id:$subject_id,title:$event_type,content:"Synthetic MVP1 acceptance event",metadata:{seed_batch:$subject_id,sequence:$sequence}}')
  response=$(curl --fail-with-body --silent --show-error --max-time 10 \
    -H 'Content-Type: application/json' \
    -X POST "$base_url/api/v1/events" \
    --data-binary "$payload")
  event_id=$(jq -er '.event_id' <<< "$response")
  printf 'event_type=%s event_id=%s\n' "$event_type" "$event_id"
done

printf 'query=%s/api/v1/events?project_id=xpa&service=xpa-finance&subject_id=%s\n' \
  "$base_url" "$batch_id"
