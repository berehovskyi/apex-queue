#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

echo "Running apex-queue queue admin e2e suite from $REPO_ROOT"
echo "Prerequisite: deploy e2e/main before running this script."

run_apex "$E2E_TEST_ROOT/queue_admin/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/queue_admin/10_should_add_job.apex"
run_apex "$E2E_TEST_ROOT/queue_admin/20_should_add_bulk_jobs_all_or_none.apex"
run_apex "$E2E_TEST_ROOT/queue_admin/30_should_pause_and_resume_queue.apex"
run_apex "$E2E_TEST_ROOT/queue_admin/31_should_cancel_future_scheduled_worker_when_pausing_queue.apex"
run_apex "$E2E_TEST_ROOT/queue_admin/40_should_drain_queue.apex"
run_apex "$E2E_TEST_ROOT/queue_admin/41_should_cancel_future_scheduled_worker_when_draining_queue.apex"
run_apex "$E2E_TEST_ROOT/queue_admin/50_should_wake_queue.apex"
run_apex "$E2E_TEST_ROOT/queue_admin/00_cleanup.apex"

echo
echo "Queue admin e2e suite completed successfully."
