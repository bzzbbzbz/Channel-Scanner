#!/usr/bin/env bash
# Safe launcher for the isolated BL-21 RAG-quality experiment only.
set -euo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly PROJECT_NAME="telegram-parser-bl21-experiments"
readonly BASE_COMPOSE="${ROOT_DIR}/docker-compose.yml"
readonly EXPERIMENT_COMPOSE="${ROOT_DIR}/docker-compose.experiment.yml"

usage() {
  cat <<'EOF'
Usage: ./docker/bl21-experiment-compose.sh {config|up|down|ps|logs} [compose arguments]

Uses only the isolated experiment overlay. It clears the caller environment,
uses a unique Compose project and named volume, disables bot polling and the
scheduler, publishes no database port, and never joins the production Caddy
network. `up` is intentionally opt-in; this setup does not run it.
EOF
}

if [[ $# -eq 0 ]]; then
  usage >&2
  exit 2
fi

case "$1" in
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
  docker compose \
  --project-name "$PROJECT_NAME" \
  --file "$BASE_COMPOSE" \
  --file "$EXPERIMENT_COMPOSE" \
  "${command[@]}"
