#!/usr/bin/env bash
# Safe launcher for the isolated BL-21 RAG-quality experiment only.
set -euo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly ROOT_HASH="$(printf '%s' "$ROOT_DIR" | sha256sum | cut -c1-16)"
readonly PROJECT_NAME="telegram-parser-bl21-${ROOT_HASH}"
readonly PGDATA_VOLUME="${PROJECT_NAME}-pgdata"
readonly BASE_COMPOSE="${ROOT_DIR}/docker-compose.yml"
readonly EXPERIMENT_COMPOSE="${ROOT_DIR}/docker-compose.experiment.yml"

usage() {
  cat <<'EOF'
Usage: ./docker/bl21-experiment-compose.sh {config|identity|up|down|ps|logs} [compose arguments]

Uses only the isolated experiment overlay. It clears the caller environment,
derives a unique Compose project and named volume from the canonical clone path,
disables bot polling and the
scheduler, publishes no database port, and never joins the production Caddy
network. `up` is intentionally opt-in; this setup does not run it.
EOF
}

if [[ $# -eq 0 ]]; then
  usage >&2
  exit 2
fi

case "$1" in
  identity)
    if [[ $# -ne 1 ]]; then
      usage >&2
      exit 2
    fi
    printf 'BL21_COMPOSE_PROJECT_NAME=%s\nBL21_EXPERIMENT_PGDATA_VOLUME=%s\n' "$PROJECT_NAME" "$PGDATA_VOLUME"
    exit 0
    ;;
  config|up|down|ps|logs)
    ;;
  -h|--help|help)
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

command=("$@")
exec env -i \
  PATH="$PATH" \
  HOME="${HOME:-/root}" \
  BL21_COMPOSE_PROJECT_NAME="$PROJECT_NAME" \
  BL21_EXPERIMENT_PGDATA_VOLUME="$PGDATA_VOLUME" \
  docker compose \
  --project-name "$PROJECT_NAME" \
  --file "$BASE_COMPOSE" \
  --file "$EXPERIMENT_COMPOSE" \
  "${command[@]}"
