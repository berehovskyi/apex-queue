#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

echo "Running apex-queue job e2e suite from $REPO_ROOT"
echo "Prerequisite: deploy e2e/main before running this script."

run_apex "$E2E_TEST_ROOT/job/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/job/10_should_add_job_and_update_data.apex"
run_apex "$E2E_TEST_ROOT/job/20_should_reuse_existing_job_when_job_id_matches.apex"
run_apex "$E2E_TEST_ROOT/job/30_should_add_delayed_job.apex"
run_apex "$E2E_TEST_ROOT/job/40_should_defer_and_promote_job.apex"
run_apex "$E2E_TEST_ROOT/job/41_should_reschedule_future_worker_when_deferring_delayed_head_job.apex"
run_apex "$E2E_TEST_ROOT/job/60_should_remove_job.apex"
run_apex "$E2E_TEST_ROOT/job/61_should_reschedule_future_worker_when_removing_delayed_head_job.apex"

run_apex "$E2E_TEST_ROOT/job/50_should_enqueue_jobs_for_priority_order.apex"
wait_for_async 15
run_apex "$E2E_TEST_ROOT/job/51_should_assert_higher_priority_job_executed_first.apex"

run_apex "$E2E_TEST_ROOT/job/70_should_enqueue_retryable_job.apex"
wait_for_async 20
run_apex "$E2E_TEST_ROOT/job/71_should_assert_retry_exhaustion_and_manual_retry_reset.apex"

run_apex "$E2E_TEST_ROOT/job/80_should_enqueue_unrecoverable_job.apex"
wait_for_async 15
run_apex "$E2E_TEST_ROOT/job/81_should_assert_unrecoverable_job_does_not_retry.apex"
run_apex "$E2E_TEST_ROOT/job/00_cleanup.apex"

echo
echo "Job e2e suite completed successfully."
