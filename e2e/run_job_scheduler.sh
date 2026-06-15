#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

echo "Running apex-queue job scheduler e2e suite from $REPO_ROOT"
echo "Prerequisite: deploy e2e/main before running this script."

run_apex "$E2E_TEST_ROOT/job_scheduler/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/job_scheduler/10_should_schedule_regular_cron_scheduler.apex"
run_apex_until_success "$E2E_TEST_ROOT/job_scheduler/11_should_assert_regular_cron_scheduler_materialized.apex" 210 10
run_apex "$E2E_TEST_ROOT/job_scheduler/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/job_scheduler/20_should_schedule_granular_scheduler.apex"
run_apex_until_success "$E2E_TEST_ROOT/job_scheduler/21_should_assert_granular_scheduler_self_rescheduled.apex" 210 10
run_apex "$E2E_TEST_ROOT/job_scheduler/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/job_scheduler/22_should_schedule_cancel_after_tick_scheduler.apex"
run_apex_until_success "$E2E_TEST_ROOT/job_scheduler/23_should_cancel_scheduler_after_real_tick.apex" 210 10
run_apex_until_success "$E2E_TEST_ROOT/job_scheduler/24_should_assert_canceled_scheduler_did_not_resurrect.apex" 150 10
run_apex "$E2E_TEST_ROOT/job_scheduler/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/job_scheduler/30_should_remove_scheduler_and_jobs.apex"
run_apex "$E2E_TEST_ROOT/job_scheduler/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/job_scheduler/31_should_cancel_scheduler_and_pending_jobs.apex"
run_apex "$E2E_TEST_ROOT/job_scheduler/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/job_scheduler/35_should_replace_scheduler_on_reupsert.apex"
run_apex "$E2E_TEST_ROOT/job_scheduler/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/job_scheduler/36_should_change_scheduler_execution_mode_on_reupsert.apex"
run_apex "$E2E_TEST_ROOT/job_scheduler/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/job_scheduler/40_should_schedule_granular_limit_five.apex"
run_apex "$E2E_TEST_ROOT/job_scheduler/41_should_materialize_granular_limit_next.apex"
run_apex "$E2E_TEST_ROOT/job_scheduler/41_should_materialize_granular_limit_next.apex"
run_apex "$E2E_TEST_ROOT/job_scheduler/41_should_materialize_granular_limit_next.apex"
run_apex "$E2E_TEST_ROOT/job_scheduler/41_should_materialize_granular_limit_next.apex"
run_apex "$E2E_TEST_ROOT/job_scheduler/42_should_stop_granular_limit_at_five.apex"
run_apex "$E2E_TEST_ROOT/job_scheduler/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/job_scheduler/50_should_schedule_queueable_scheduler.apex"
run_apex "$E2E_TEST_ROOT/job_scheduler/51_should_dispatch_due_queueable_scheduler.apex"
run_apex "$E2E_TEST_ROOT/job_scheduler/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/job_scheduler/52_should_schedule_queueable_serial_scheduler.apex"
run_apex "$E2E_TEST_ROOT/job_scheduler/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/job_scheduler/60_should_schedule_invocable_scheduler.apex"
run_apex "$E2E_TEST_ROOT/job_scheduler/00_cleanup.apex"

echo
echo "Job scheduler e2e suite completed successfully."
