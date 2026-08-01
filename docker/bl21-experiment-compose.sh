#!/usr/bin/env bash
# Safe launcher for the isolated BL-21 RAG-quality experiment only.
set -euo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly ROOT_HASH="$(printf '%s' "$ROOT_DIR" | sha256sum | cut -c1-16)"
readonly PROJECT_NAME="telegram-parser-bl21-${ROOT_HASH}"
readonly PGDATA_VOLUME="${PROJECT_NAME}-pgdata"
readonly BASE_COMPOSE="${ROOT_DIR}/docker-compose.yml"
readonly EXPERIMENT_COMPOSE="${ROOT_DIR}/docker-compose.experiment.yml"
readonly LOCAL_DOCKER_SOCKET='/var/run/docker.sock'
readonly LAUNCHER_HOME="${ROOT_DIR}/.data-experiment/launcher-home"
readonly LAUNCHER_DOCKER_CONFIG="${LAUNCHER_HOME}/docker-config"
readonly DB_HEALTH_MAX_ATTEMPTS=30
readonly DB_HEALTH_POLL_SECONDS=1
readonly EXPERIMENT_DATABASE_NAME='telegram_bot_bl21_experiment'
readonly EXPERIMENT_DATABASE_USER='bot'
readonly EXPERIMENT_DATABASE_PASSWORD='experiment-only-password'
readonly SNAPSHOT_VALIDATOR="${ROOT_DIR}/docker/bl21-validate-snapshot.py"
readonly SNAPSHOT_MANIFEST_WRITER="${ROOT_DIR}/docker/bl21-write-snapshot-manifest.py"
readonly DB_IDENTITY_VALIDATOR="${ROOT_DIR}/docker/bl21-validate-db-identity.py"
readonly SNAPSHOT_CONTAINER_DUMP_PATH='/bl21-snapshot/snapshot.pgdump'
readonly SOURCE_SNAPSHOT_DIR="${ROOT_DIR}/.data-experiment/snapshots/bl21-local"
readonly SOURCE_SNAPSHOT_DUMP="${SOURCE_SNAPSHOT_DIR}/source.pgdump"
readonly SOURCE_SNAPSHOT_MANIFEST="${SOURCE_SNAPSHOT_DIR}/source-manifest.json"
readonly FEATURE_BRANCH='feature/bl-21-rag-quality-experiments'
readonly RUNNER_GIT_BRANCH_ENV='BL21_EXPERIMENT_GIT_BRANCH'
readonly RUNNER_GIT_REVISION_ENV='BL21_EXPERIMENT_GIT_REVISION'

usage() {
  cat <<'EOF'
Usage:
  ./docker/bl21-experiment-compose.sh identity
  ./docker/bl21-experiment-compose.sh config
  ./docker/bl21-experiment-compose.sh build-app
  ./docker/bl21-experiment-compose.sh db-up
  ./docker/bl21-experiment-compose.sh snapshot-export
  ./docker/bl21-experiment-compose.sh db-restore
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
pins every Docker action to the local Unix socket and launcher-controlled Docker
configuration, disables bot polling and the
scheduler, publishes no database port, and never joins the production Caddy
network. It never exposes arbitrary Compose subcommands, services, flags, or
container entrypoints.

db-restore has no arguments. It validates only
.data-experiment/snapshots/bl21-local/{snapshot-manifest.json,snapshot.pgdump}
and restores that read-only local snapshot into the isolated db with a fixed
pg_restore invocation. Snapshot acquisition is intentionally separate.

snapshot-export has no arguments. It health-checks only the derived isolated db,
writes only .data-experiment/snapshots/bl21-local/source.pgdump and its
content-free manifest atomically, and never contacts source or production.
EOF
}

if [[ $# -eq 0 ]]; then
  usage >&2
  exit 2
fi

readonly EXPERIMENT_DATABASE_URL="postgresql+asyncpg://${EXPERIMENT_DATABASE_USER}:${EXPERIMENT_DATABASE_PASSWORD}@db:5432/${EXPERIMENT_DATABASE_NAME}?experiment=bl21"
readonly EXPERIMENT_DATASET_PREFIX='/app/.data-experiment/inputs/'
readonly EXPERIMENT_DATASET_PATTERN='^/app/\.data-experiment/inputs/[A-Za-z0-9][A-Za-z0-9._-]*\.jsonl$'
readonly TELEGRAM_USERNAME_PATTERN='^@?[A-Za-z0-9_]{5,32}$'
readonly SAFE_IDENTIFIER_PATTERN='^[a-z][a-z0-9_-]*$'

die() {
  printf 'error: %s\n' "$*" >&2
  exit 2
}

prepare_docker_environment() {
  local path mode owner
  for path in "${ROOT_DIR}/.data-experiment" "$LAUNCHER_HOME" "$LAUNCHER_DOCKER_CONFIG"; do
    if [[ -L "$path" || ( -e "$path" && ! -d "$path" ) ]]; then
      die 'launcher Docker home/config path is unsafe'
    fi
  done
  umask 077
  mkdir -p "$LAUNCHER_DOCKER_CONFIG"
  chmod 700 "$LAUNCHER_HOME" "$LAUNCHER_DOCKER_CONFIG"

  if [[ -L "$LOCAL_DOCKER_SOCKET" || ! -S "$LOCAL_DOCKER_SOCKET" || ! -r "$LOCAL_DOCKER_SOCKET" || ! -w "$LOCAL_DOCKER_SOCKET" ]]; then
    die 'local Docker Unix socket is unavailable or unsafe'
  fi
  owner="$(stat -c '%u' "$LOCAL_DOCKER_SOCKET")"
  mode="$(stat -c '%a' "$LOCAL_DOCKER_SOCKET")"
  if [[ "$owner" != '0' ]] || (( (8#$mode & 0002) != 0 )); then
    die 'local Docker Unix socket is unavailable or unsafe'
  fi
}

docker_environment() {
  prepare_docker_environment
  env -i \
    PATH="$PATH" \
    HOME="$LAUNCHER_HOME" \
    XDG_CONFIG_HOME="$LAUNCHER_HOME/.config" \
    DOCKER_CONFIG="$LAUNCHER_DOCKER_CONFIG" \
    DOCKER_HOST="unix://${LOCAL_DOCKER_SOCKET}" \
    DOCKER_CONTEXT=default \
    BL21_COMPOSE_PROJECT_NAME="$PROJECT_NAME" \
    BL21_EXPERIMENT_PGDATA_VOLUME="$PGDATA_VOLUME" \
    "$@"
}

compose_command() {
  docker_environment docker compose \
    --project-name "$PROJECT_NAME" \
    --file "$BASE_COMPOSE" \
    --file "$EXPERIMENT_COMPOSE" \
    "$@"
}

docker_command() {
  docker_environment docker "$@"
}

resolve_launcher_git_metadata() {
  local git_root branch revision
  git_root="$(env -i PATH="$PATH" HOME="$LAUNCHER_HOME" GIT_CONFIG_NOSYSTEM=1 git -C "$ROOT_DIR" rev-parse --show-toplevel)" \
    || die 'experiment clone Git root is unavailable'
  [[ "$(cd "$git_root" && pwd -P)" == "$ROOT_DIR" ]] || die 'experiment clone Git root is not canonical'
  branch="$(env -i PATH="$PATH" HOME="$LAUNCHER_HOME" GIT_CONFIG_NOSYSTEM=1 git -C "$ROOT_DIR" symbolic-ref --quiet --short HEAD)" \
    || die 'experiment clone must be on the BL-21 feature branch'
  [[ "$branch" == "$FEATURE_BRANCH" ]] || die 'experiment clone must be on the BL-21 feature branch'
  revision="$(env -i PATH="$PATH" HOME="$LAUNCHER_HOME" GIT_CONFIG_NOSYSTEM=1 git -C "$ROOT_DIR" rev-parse --verify 'HEAD^{commit}')" \
    || die 'experiment clone commit is unavailable'
  [[ "$revision" =~ ^[0-9a-f]{40}$ ]] || die 'experiment clone commit is invalid'
  ONE_OFF_GIT_METADATA=(
    --env "${RUNNER_GIT_BRANCH_ENV}=${branch}"
    --env "${RUNNER_GIT_REVISION_ENV}=${revision}"
  )
}

wait_for_db_health() {
  local container_id status attempt identity_json
  container_id="$(compose_command ps --quiet db)"
  [[ "$container_id" =~ ^[0-9a-f]{64}$ ]] || die 'isolated db container identity is unavailable'
  identity_json="$(docker_command inspect --format '{{json .}}' "$container_id")" || die 'isolated db identity inspection failed'
  printf '%s' "$identity_json" | env -i PATH="$PATH" python3 "$DB_IDENTITY_VALIDATOR" \
    "$container_id" "$PROJECT_NAME" "$PGDATA_VOLUME" "$SOURCE_SNAPSHOT_DIR" \
    >/dev/null 2>&1 || die 'isolated db project, service, image, container, or mount identity is invalid'
  for ((attempt = 1; attempt <= DB_HEALTH_MAX_ATTEMPTS; attempt++)); do
    status="$(docker_command inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}' "$container_id")" || die 'isolated db health inspection failed'
    if [[ "$status" == 'healthy' ]]; then
      return 0
    fi
    if [[ "$status" != 'starting' && "$status" != 'unhealthy' ]]; then
      die 'isolated db health status is invalid'
    fi
    if (( attempt < DB_HEALTH_MAX_ATTEMPTS )); then
      sleep "$DB_HEALTH_POLL_SECONDS"
    fi
  done
  die 'timed out waiting for isolated db health'
}

prepare_source_snapshot_directory() {
  local path
  for path in "${ROOT_DIR}/.data-experiment" "${ROOT_DIR}/.data-experiment/snapshots" "$SOURCE_SNAPSHOT_DIR"; do
    if [[ -L "$path" || ( -e "$path" && ! -d "$path" ) ]]; then
      die 'source snapshot path is unsafe'
    fi
    mkdir -p "$path"
    chmod 700 "$path"
  done
}

snapshot_db_value() {
  local query="$1" value
  value="$(compose_command exec -T -e "PGPASSWORD=${EXPERIMENT_DATABASE_PASSWORD}" db \
    psql --host=127.0.0.1 --port=5432 --username="$EXPERIMENT_DATABASE_USER" --dbname="$EXPERIMENT_DATABASE_NAME" \
    --tuples-only --no-align --command "$query")" || die 'isolated db snapshot metadata query failed'
  printf '%s' "$value"
}

export_isolated_snapshot() {
  local temporary_dump="" transaction_dir="" postgres_version alembic_version table_names table_name table_count previous_table=""
  local previous_dump_moved=0 previous_manifest_moved=0
  local -a table_count_pairs=()
  local table_counts_json

  prepare_source_snapshot_directory
  wait_for_db_health
  temporary_dump="$(mktemp "${SOURCE_SNAPSHOT_DIR}/.source.pgdump.XXXXXX")"
  trap 'cleanup_failed_snapshot_export $?' EXIT
  chmod 600 "$temporary_dump"
  compose_command exec -T -e "PGPASSWORD=${EXPERIMENT_DATABASE_PASSWORD}" db \
    pg_dump --format=custom --compress=6 --no-owner --no-privileges \
    --host=127.0.0.1 --port=5432 --username="$EXPERIMENT_DATABASE_USER" \
    --dbname="$EXPERIMENT_DATABASE_NAME" > "$temporary_dump" || die 'isolated db logical dump failed'
  [[ -s "$temporary_dump" ]] || die 'isolated db logical dump is empty'
  chmod 600 "$temporary_dump"

  postgres_version="$(snapshot_db_value 'SHOW server_version_num')"
  [[ "$postgres_version" =~ ^[0-9]{6}$ ]] || die 'isolated db PostgreSQL version is invalid'
  alembic_version="$(snapshot_db_value 'SELECT version_num FROM alembic_version')"
  [[ "$alembic_version" =~ ^[a-z0-9][a-z0-9_]{0,127}$ ]] || die 'isolated db Alembic version is invalid'
  table_names="$(snapshot_db_value "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' ORDER BY tablename")"
  [[ -n "$table_names" ]] || die 'isolated db table list is empty'
  while IFS= read -r table_name; do
    [[ "$table_name" =~ ^[a-z][a-z0-9_]{0,62}$ && "$table_name" > "$previous_table" ]] || die 'isolated db table list is invalid'
    previous_table="$table_name"
    table_count="$(snapshot_db_value "SELECT count(*) FROM public.\"${table_name}\"")"
    [[ "$table_count" =~ ^[0-9]+$ ]] || die 'isolated db table count is invalid'
    table_count_pairs+=("${table_name}:${table_count}")
  done <<< "$table_names"
  table_counts_json="$(env -i PATH="$PATH" TABLE_COUNT_PAIRS="${table_count_pairs[*]}" python3 -c 'import json, os; print(json.dumps({pair.split(":", 1)[0]: int(pair.split(":", 1)[1]) for pair in os.environ["TABLE_COUNT_PAIRS"].split()} , sort_keys=True, separators=(",", ":")))')"

  if [[ -e "$SOURCE_SNAPSHOT_DUMP" || -e "$SOURCE_SNAPSHOT_MANIFEST" ]]; then
    if [[ -f "$SOURCE_SNAPSHOT_DUMP" && ! -L "$SOURCE_SNAPSHOT_DUMP" && -f "$SOURCE_SNAPSHOT_MANIFEST" && ! -L "$SOURCE_SNAPSHOT_MANIFEST" ]] \
      && python3 "$SNAPSHOT_VALIDATOR" --export >/dev/null 2>&1; then
      transaction_dir="$(mktemp -d "${SOURCE_SNAPSHOT_DIR}/.snapshot-export.XXXXXX")"
      chmod 700 "$transaction_dir"
      mv -f "$SOURCE_SNAPSHOT_DUMP" "${transaction_dir}/previous.pgdump"
      previous_dump_moved=1
      mv -f "$SOURCE_SNAPSHOT_MANIFEST" "${transaction_dir}/previous-manifest.json"
      previous_manifest_moved=1
    fi
  fi

  mv -f "$temporary_dump" "$SOURCE_SNAPSHOT_DUMP"
  temporary_dump=""
  env -i PATH="$PATH" BL21_POSTGRES_VERSION="$postgres_version" BL21_ALEMBIC_VERSION="$alembic_version" BL21_TABLE_COUNTS="$table_counts_json" \
    python3 "$SNAPSHOT_MANIFEST_WRITER" || die 'isolated db snapshot manifest write failed'
  python3 "$SNAPSHOT_VALIDATOR" --export || die 'isolated db snapshot validation failed'
  trap - EXIT
  if [[ -n "$transaction_dir" ]]; then
    rm -f -- "${transaction_dir}/previous.pgdump" "${transaction_dir}/previous-manifest.json"
    rmdir "$transaction_dir"
  fi
}

cleanup_failed_snapshot_export() {
  local exit_code="$1"
  trap - EXIT
  rm -f -- "$temporary_dump" "$SOURCE_SNAPSHOT_DUMP" "$SOURCE_SNAPSHOT_MANIFEST" || :
  if [[ "$previous_dump_moved" -eq 1 && -f "${transaction_dir}/previous.pgdump" ]]; then
    mv -f "${transaction_dir}/previous.pgdump" "$SOURCE_SNAPSHOT_DUMP" || :
  fi
  if [[ "$previous_manifest_moved" -eq 1 && -f "${transaction_dir}/previous-manifest.json" ]]; then
    mv -f "${transaction_dir}/previous-manifest.json" "$SOURCE_SNAPSHOT_MANIFEST" || :
  fi
  if [[ -n "$transaction_dir" ]]; then
    rmdir "$transaction_dir" 2>/dev/null || :
  fi
  exit "$exit_code"
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
    wait_for_db_health
    ;;
  snapshot-export)
    if [[ $# -ne 1 ]]; then
      usage >&2
      exit 2
    fi
    [[ -f "$SNAPSHOT_VALIDATOR" && ! -L "$SNAPSHOT_VALIDATOR" && -f "$SNAPSHOT_MANIFEST_WRITER" && ! -L "$SNAPSHOT_MANIFEST_WRITER" && -f "$DB_IDENTITY_VALIDATOR" && ! -L "$DB_IDENTITY_VALIDATOR" ]] || die 'snapshot export helpers are unavailable'
    export_isolated_snapshot
    ;;
  db-restore)
    if [[ $# -ne 1 ]]; then
      usage >&2
      exit 2
    fi
    [[ -f "$SNAPSHOT_VALIDATOR" && ! -L "$SNAPSHOT_VALIDATOR" ]] || die 'snapshot validator is unavailable'
    python3 "$SNAPSHOT_VALIDATOR"
    compose_command exec -T -e "PGPASSWORD=${EXPERIMENT_DATABASE_PASSWORD}" db \
      pg_restore --exit-on-error --clean --if-exists --no-owner --no-privileges \
      --host=127.0.0.1 --port=5432 --username="$EXPERIMENT_DATABASE_USER" \
      --dbname="$EXPERIMENT_DATABASE_NAME" "$SNAPSHOT_CONTAINER_DUMP_PATH"
    ;;
  migrate)
    if [[ $# -ne 1 ]]; then
      usage >&2
      exit 2
    fi
    resolve_launcher_git_metadata
    compose_command run --rm --no-deps "${ONE_OFF_GIT_METADATA[@]}" --entrypoint alembic app upgrade head
    ;;
  evaluate)
    [[ $# -ge 3 && "$2" == "--" ]] || reject_evaluate_arguments
    shift 2
    validate_evaluate_arguments "$@"
    resolve_launcher_git_metadata
    compose_command run --rm --no-deps "${ONE_OFF_GIT_METADATA[@]}" --entrypoint python app -m src.knowledge.experiment_runner "${EVALUATE_ARGUMENTS[@]}"
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
