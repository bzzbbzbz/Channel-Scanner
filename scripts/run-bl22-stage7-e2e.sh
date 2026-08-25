#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
python3 -m src.reliability.stage7_harness preflight "$ROOT/.env"
unset BOT_TOKEN TELEGRAM_TOKEN E2E_TELEGRAM_TOKEN BL22_STAGE7_PRODUCT_TOKEN
BL22_STAGE7_E2E=1 python3 -m pytest -q -s "$ROOT/tests/e2e/test_bl22_stage7_real_telegram.py"
