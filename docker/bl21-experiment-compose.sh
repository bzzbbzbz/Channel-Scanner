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
Usage:
  ./docker/bl21-experiment-compose.sh identity
  ./docker/bl21-experiment-compose.sh config
  ./docker/bl21-experiment-compose.sh build-app
  ./docker/bl21-experiment-compose.sh db-up
  ./docker/bl21-experiment-compose.sh migrate
  ./docker/bl21-experiment-compose.sh evaluate -- \
    --experiment-root /app \
    --database-url postgresql+asyncpg://bot:experiment-only-password@db:5432/telegram_bot_bl21_experiment?experiment=bl21 \
    --dataset /app/.data-experiment/inputs/<manifest-declared-dataset>.jsonl \
    --channel <telegram_username> \
    --campaign-id <safe_identifier> \
    (--dry-run|--execute)

Uses only the isolated experiment overlay. It clears the caller environment,
derives a unique Compose project and named volume from the canonical clone path,
disables bot polling and the
scheduler, publishes no database port, and never joins the production Caddy
network. It never exposes arbitrary Compose subcommands, services, flags, or
container entrypoints.
EOF
}

if [[ $# -eq 0 ]]; then
  usage >&2
  exit 2
fi

readonly EXPERIMENT_DATABASE_URL='postgresql+asyncpg://bot:experiment-only-password@db:5432/telegram_bot_bl21_experiment?experiment=bl21'
readonly EXPERIMENT_DATASET_PREFIX='/app/.data-experiment/inputs/'
readonly EXPERIMENT_DATASET_PATTERN='^/app/\.data-experiment/inputs/[A-Za-z0-9][A-Za-z0-9._-]*\.jsonl$'
readonly TELEGRAM_USERNAME_PATTERN='^@?[A-Za-z0-9_]{5,32}$'
readonly SAFE_IDENTIFIER_PATTERN='^[a-z][a-z0-9_-]*$'

compose_command() {
  exec env -i \
    PATH="$PATH" \
    HOME="${HOME:-/root}" \
    BL21_COMPOSE_PROJECT_NAME="$PROJECT_NAME" \
    BL21_EXPERIMENT_PGDATA_VOLUME="$PGDATA_VOLUME" \
    docker compose \
    --project-name "$PROJECT_NAME" \
    --file "$BASE_COMPOSE" \
    --file "$EXPERIMENT_COMPOSE" \
    "$@"
}

reject_evaluate_arguments() {
  printf 'error: evaluate accepts only the documented experiment-runner arguments\n' >&2
  usage >&2
  exit 2
}

validate_evaluate_arguments() {
  local option value mode="" experiment_root="" database_url="" dataset="" channel="" campaign_id=""
  local seen_experiment_root=0 seen_database_url=0 seen_dataset=0 seen_channel=0 seen_campaign_id=0

  [[ $# -ge 1 ]] || reject_evaluate_arguments
  while [[ $# -gt 0 ]]; do
    option="$1"
    case "$option" in
      --experiment-root|--database-url|--dataset|--channel|--campaign-id)
        [[ $# -ge 2 ]] || reject_evaluate_arguments
        value="$2"
        case "$option" in
          --experiment-root)
            [[ $seen_experiment_root -eq 0 && "$value" == "/app" ]] || reject_evaluate_arguments
            experiment_root="$value"
            seen_experiment_root=1
            ;;
          --database-url)
            [[ $seen_database_url -eq 0 && "$value" == "$EXPERIMENT_DATABASE_URL" ]] || reject_evaluate_arguments
            database_url="$value"
            seen_database_url=1
            ;;
          --dataset)
            [[ $seen_dataset -eq 0 && "$value" == "$EXPERIMENT_DATASET_PREFIX"*.jsonl && "$value" =~ $EXPERIMENT_DATASET_PATTERN ]] || reject_evaluate_arguments
            dataset="$value"
            seen_dataset=1
            ;;
          --channel)
            [[ $seen_channel -eq 0 && "$value" =~ $TELEGRAM_USERNAME_PATTERN ]] || reject_evaluate_arguments
            channel="$value"
            seen_channel=1
            ;;
          --campaign-id)
            [[ $seen_campaign_id -eq 0 && "$value" =~ $SAFE_IDENTIFIER_PATTERN ]] || reject_evaluate_arguments
            campaign_id="$value"
            seen_campaign_id=1
            ;;
        esac
        shift 2
        ;;
      --dry-run|--execute)
        [[ -z "$mode" ]] || reject_evaluate_arguments
        mode="$option"
        shift
        ;;
      *)
        reject_evaluate_arguments
        ;;
    esac
  done

  [[ -n "$experiment_root" && -n "$database_url" && -n "$dataset" && -n "$channel" && -n "$campaign_id" && -n "$mode" ]] || reject_evaluate_arguments
  EVALUATE_ARGUMENTS=(
    --experiment-root "$experiment_root"
    --database-url "$database_url"
    --dataset "$dataset"
    --channel "$channel"
    --campaign-id "$campaign_id"
    "$mode"
  )
}

case "$1" in
  identity)
    if [[ $# -ne 1 ]]; then
      usage >&2
      exit 2
    fi
    printf 'BL21_COMPOSE_PROJECT_NAME=%s\nBL21_EXPERIMENT_PGDATA_VOLUME=%s\n' "$PROJECT_NAME" "$PGDATA_VOLUME"
    exit 0
    ;;
  config)
    if [[ $# -ne 1 ]]; then
      usage >&2
      exit 2
    fi
    compose_command config
    ;;
  build-app)
    if [[ $# -ne 1 ]]; then
      usage >&2
      exit 2
    fi
    compose_command build app
    ;;
  db-up)
    if [[ $# -ne 1 ]]; then
      usage >&2
      exit 2
    fi
    compose_command up --detach --no-deps db
    ;;
  migrate)
    if [[ $# -ne 1 ]]; then
      usage >&2
      exit 2
    fi
    compose_command run --rm --no-deps --entrypoint alembic app upgrade head
    ;;
  evaluate)
    [[ $# -ge 3 && "$2" == "--" ]] || reject_evaluate_arguments
    shift 2
    validate_evaluate_arguments "$@"
    compose_command run --rm --no-deps --entrypoint python app -m src.knowledge.experiment_runner "${EVALUATE_ARGUMENTS[@]}"
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
