#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

echo "Running apex-queue worker e2e suite from $REPO_ROOT"
echo "Prerequisite: deploy e2e/main before running this script."

run_apex "$E2E_TEST_ROOT/worker/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/worker/10_enqueue_weather_smoke.apex"
wait_for_async 20
run_apex "$E2E_TEST_ROOT/worker/20_assert_weather_smoke.apex"
run_apex "$E2E_TEST_ROOT/worker/30_enqueue_unsupported_city.apex"
wait_for_async 20
run_apex "$E2E_TEST_ROOT/worker/40_assert_unsupported_city.apex"

run_apex "$E2E_TEST_ROOT/worker/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/worker/50_should_keep_existing_worker_when_later_delayed_job_is_added.apex"
run_apex "$E2E_TEST_ROOT/worker/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/worker/52_should_replace_delayed_worker_and_cancel_previous_schedule.apex"
run_apex "$E2E_TEST_ROOT/worker/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/worker/60_should_enqueue_catastrophic_batch_failure.apex"
wait_for_async 20
run_apex "$E2E_TEST_ROOT/worker/61_should_assert_catastrophic_batch_failure_is_recorded.apex"
run_apex "$E2E_TEST_ROOT/worker/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/worker/70_should_enqueue_retryable_worker_job_with_ten_attempts.apex"
wait_for_async 60
run_apex "$E2E_TEST_ROOT/worker/71_should_assert_retryable_worker_job_with_ten_attempts_failed.apex"
run_apex "$E2E_TEST_ROOT/worker/00_cleanup.apex"

echo
echo "Worker e2e suite completed successfully."
