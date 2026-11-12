#!/usr/bin/env bash
# Deploy the current main to a VM: deploy/deploy.sh user@host
# Assumes the one-time setup in deploy/README.md (Docker, a checkout, .env).
set -euo pipefail
host=${1:?usage: deploy/deploy.sh user@host}
dir=${LLMGW_REMOTE_DIR:-llm-gateway}

ssh "$host" bash -s -- "$dir" <<'REMOTE'
set -euo pipefail
cd "$1"
git pull --ff-only
compose="docker compose -f docker-compose.yml -f deploy/docker-compose.deploy.yml --profile stack"
$compose up -d --build --remove-orphans
domain=$(grep '^LLMGW_DOMAIN=' .env | cut -d= -f2-)
for _ in $(seq 30); do
  if curl -fsS "https://$domain/readyz"; then echo; exit 0; fi
  sleep 2
done
echo "readyz did not answer on https://$domain" >&2
$compose ps
exit 1
REMOTE
