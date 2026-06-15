#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

echo "Running apex-queue invocable e2e suite from $REPO_ROOT"
echo "Prerequisite: deploy e2e/main before running this script."

run_apex "$E2E_TEST_ROOT/invocable/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/invocable/10_should_enqueue_invocable_smoke.apex"
wait_for_async 20
run_apex_until_success "$E2E_TEST_ROOT/invocable/20_should_assert_invocable_smoke_completed.apex" 180 15

run_apex "$E2E_TEST_ROOT/invocable/30_should_enqueue_retryable_invocable_job.apex"
wait_for_async 180
run_apex_until_success "$E2E_TEST_ROOT/invocable/31_should_assert_retryable_invocable_job_failed.apex" 180 15

if [[ "${RUN_INVOCABLE_MAX_RETRY:-0}" == "1" ]]; then
    run_apex "$E2E_TEST_ROOT/invocable/00_cleanup.apex"
    run_apex "$E2E_TEST_ROOT/invocable/80_should_enqueue_retryable_invocable_job_with_five_attempts.apex"
    wait_for_async 600
    run_apex_until_success "$E2E_TEST_ROOT/invocable/81_should_assert_retryable_invocable_job_with_five_attempts_failed.apex" 300 60
fi

run_apex "$E2E_TEST_ROOT/invocable/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/invocable/50_should_reschedule_invocable_job_by_available_at.apex"
wait_for_async 90
run_apex_until_success "$E2E_TEST_ROOT/invocable/51_should_assert_rescheduled_invocable_job_completed.apex" 180 15

run_apex "$E2E_TEST_ROOT/invocable/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/invocable/60_should_enqueue_invocable_stale_wake_guards.apex"
run_apex "$E2E_TEST_ROOT/invocable/61_should_mutate_invocable_stale_wake_guards.apex"
wait_for_async 90
run_apex_until_success "$E2E_TEST_ROOT/invocable/62_should_assert_invocable_stale_wake_guards.apex" 180 15

run_apex "$E2E_TEST_ROOT/invocable/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/invocable/70_should_recover_expired_active_invocable_lease.apex"
wait_for_async 90
run_apex_until_success "$E2E_TEST_ROOT/invocable/71_should_assert_recovered_invocable_lease_completed.apex" 180 15

run_apex "$E2E_TEST_ROOT/invocable/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/invocable/72_should_seed_lost_invocable_wake.apex"
run_apex "$E2E_TEST_ROOT/invocable/73_should_repair_lost_invocable_wake.apex"
run_apex_until_success "$E2E_TEST_ROOT/invocable/74_should_assert_repaired_lost_invocable_wake_completed.apex" 180 15
run_apex "$E2E_TEST_ROOT/invocable/00_cleanup.apex"

echo
echo "Invocable e2e suite completed successfully."
