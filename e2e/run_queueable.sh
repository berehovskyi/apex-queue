#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

echo "Running apex-queue queueable e2e suite from $REPO_ROOT"
echo "Prerequisite: deploy e2e/main before running this script."

run_apex "$E2E_TEST_ROOT/queueable/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/queueable/05_should_dedupe_queueable_dispatch_on_double_click.apex"
run_apex "$E2E_TEST_ROOT/queueable/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/queueable/10_should_enqueue_queueable_smoke.apex"
wait_for_async 20
run_apex "$E2E_TEST_ROOT/queueable/20_should_assert_queueable_smoke_completed.apex"

run_apex "$E2E_TEST_ROOT/queueable/30_should_enqueue_retryable_queueable_job.apex"
wait_for_async 20
run_apex "$E2E_TEST_ROOT/queueable/31_should_assert_retryable_queueable_job_failed.apex"

run_apex "$E2E_TEST_ROOT/queueable/70_should_enqueue_retryable_queueable_job_with_ten_attempts.apex"
wait_for_async 60
run_apex "$E2E_TEST_ROOT/queueable/71_should_assert_retryable_queueable_job_with_ten_attempts_failed.apex"

run_apex "$E2E_TEST_ROOT/queueable/80_should_enqueue_native_stack_depth_probe.apex"
wait_for_async 20
run_apex "$E2E_TEST_ROOT/queueable/81_should_assert_native_stack_depth_probe.apex"

run_apex "$E2E_TEST_ROOT/queueable/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/queueable/50_should_keep_delayed_dispatches_isolated_when_later_delayed_job_is_added.apex"
run_apex "$E2E_TEST_ROOT/queueable/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/queueable/52_should_enqueue_immediate_queueable_without_canceling_delayed_queueable.apex"
wait_for_async 20
run_apex "$E2E_TEST_ROOT/queueable/53_should_assert_immediate_queueable_completed_and_delayed_queueable_waits.apex"
run_apex "$E2E_TEST_ROOT/queueable/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/queueable/55_should_skip_already_initiated_retry_candidate_when_dispatching_remaining_job.apex"
run_apex "$E2E_TEST_ROOT/queueable/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/queueable/60_should_enqueue_catastrophic_queueable_failure.apex"
wait_for_async 20
run_apex "$E2E_TEST_ROOT/queueable/61_should_assert_catastrophic_queueable_failure_is_recorded.apex"

run_apex "$E2E_TEST_ROOT/queueable/40_should_reject_invalid_queueable_delay.apex"
run_apex "$E2E_TEST_ROOT/queueable/00_cleanup.apex"

echo
echo "Queueable e2e suite completed successfully."
