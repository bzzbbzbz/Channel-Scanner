# Kafka shadow operations: runbook

BL-29 принят 2026-08-24 только для shadow-наблюдения. В production должны оставаться `KAFKA_ENABLED=1`, `RELIABLE_DIGEST_ENABLED=0`, `MEMORY_ENABLED=1`, `delivery_path=legacy`. Не включайте reliable delivery без отдельного решения о rollout.

## Ежедневная проверка

Откройте authenticated admin dashboard, вкладку `Kafka`, и зафиксируйте:

- mode: `kafka=true`, `reliable=false`, `memory=true`, `delivery_path=legacy`;
- broker доступен; все четыре фиксированных topic доступны и без drift;
- `scheduler`, `outbox-relay`, `digest-worker`, `telegram-delivery-worker` имеют состояние `ready`, heartbeat age не больше 30 секунд;
- consumer groups могут быть отсутствующими/неактивными: это ожидаемо в shadow mode;
- unpublished queues, retries, expired leases, open DLQ и recent safe errors равны `0`;
- число `DigestRun` не выросло относительно предыдущего наблюдения;
- Overview, app scheduler, Telegram polling, mem0 и admin остаются healthy.

Начните расследование при любом из условий:

- broker unavailable;
- отсутствует хотя бы один topic или обнаружен любой topic drift;
- heartbeat любой non-terminal роли старше 30 секунд, либо роль `stopped`/`failed`;
- любая queue, retry, expired lease или open DLQ больше `0`;
- появился любой recent safe error;
- создан хотя бы один новый reliable `DigestRun`.

## Read-only диагностика

Команды не изменяют product state:

```bash
docker compose ps
docker compose ps app
docker compose logs --since=30m app
docker compose exec -T db psql -U bot -d telegram_bot -c "select count(*) as reliable_digest_runs from digest_runs;"
docker compose exec -T db psql -U bot -d telegram_bot -c "select role, state, heartbeat_at, stopped_at, last_error_code from reliability_role_heartbeats order by role;"
docker top telegram-parser-bot-app-1 -eo pid,ppid,stat,etime,cmd
```

Для Kafka используйте authenticated dashboard, а не raw payload, Docker socket или неограниченный доступ к логам. Сравнивайте `reliable_digest_runs` с предыдущей записью наблюдения, а не только с абсолютным значением.

## Конфигурационные ограничения

- `RELIABLE_DIGEST_ENABLED` и `RELIABLE_DIGEST_ALL_SUBSCRIPTIONS` должны оставаться `0`.
- `RELIABLE_DIGEST_SUBSCRIPTION_IDS` должен оставаться `[]` в shadow mode.
- Значение allowlist является строгим JSON-массивом целых чисел: допустимы `[]` и `[123,456]`; недопустимы `123,456`, `["123"]`, объекты и boolean. Это описание формата, не разрешение заполнять allowlist.

## Неинвазивный rollback shadow-инфраструктуры

Rollback отключает только Kafka probe и профильные сервисы, сохраняя app, PostgreSQL и volumes.

1. В `.env` установите:

```dotenv
KAFKA_ENABLED=0
RELIABLE_DIGEST_ENABLED=0
RELIABLE_DIGEST_SUBSCRIPTION_IDS=[]
RELIABLE_DIGEST_ALL_SUBSCRIPTIONS=0
MEMORY_ENABLED=1
```

2. Пересоздайте только app с новым значением, не останавливая БД:

```bash
docker compose up -d --no-deps --force-recreate app
```

3. Остановите только shadow/profile services:

```bash
docker compose --profile bl22 stop scheduler outbox-relay digest-worker telegram-delivery-worker kafka kafka-init
```

4. Убедитесь, что app и db продолжают работать:

```bash
docker compose ps app db
docker compose logs --since=30m app
```

Не используйте для этого rollback `docker compose down`, `--volumes`, удаление `pgdata`/`kafka_data` или команды очистки Docker: они не нужны и могут затронуть app, БД или сохранённые данные.

## Журнал наблюдений

```text
UTC time:
Operator:
Mode (kafka/reliable/memory/delivery_path):
Broker:
Topics (4, drift):
Roles and heartbeat ages:
Groups (inactive expected):
Queues/retries/expired leases/open DLQ:
Recent safe errors:
DigestRun count and delta:
App scheduler/polling/mem0/admin:
Decision/action:
```
