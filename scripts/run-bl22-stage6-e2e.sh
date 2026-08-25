#!/bin/sh
set -eu

npm ci
npx playwright install chromium
BL22_STAGE6_E2E=1 python3 -m pytest -q -s tests/e2e/test_kafka_reliable_digest.py
