#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

echo "Running apex-queue mixed transport stress e2e suite from $REPO_ROOT"
echo "Prerequisite: deploy e2e/main before running this script."

run_apex "$E2E_TEST_ROOT/stress/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/stress/11_enqueue_worker.apex" &
worker_setup_pid=$!
run_apex "$E2E_TEST_ROOT/stress/12_enqueue_queueable_serial.apex" &
queueable_serial_setup_pid=$!
run_apex "$E2E_TEST_ROOT/stress/13_enqueue_queueable_concurrent.apex" &
queueable_concurrent_setup_pid=$!
run_apex "$E2E_TEST_ROOT/stress/14_enqueue_invocable.apex" &
invocable_setup_pid=$!

wait "$worker_setup_pid"
wait "$queueable_serial_setup_pid"
wait "$queueable_concurrent_setup_pid"
wait "$invocable_setup_pid"
run_apex "$E2E_TEST_ROOT/stress/15_upsert_schedulers_00.apex"
run_apex "$E2E_TEST_ROOT/stress/15_upsert_schedulers_15.apex"
run_apex "$E2E_TEST_ROOT/stress/15_upsert_schedulers_30.apex"
run_apex "$E2E_TEST_ROOT/stress/15_upsert_schedulers_45.apex"
run_apex "$E2E_TEST_ROOT/stress/15_upsert_schedulers_60.apex"
run_apex "$E2E_TEST_ROOT/stress/15_upsert_schedulers_75.apex"

started_at="$SECONDS"
while true; do
    run_apex "$E2E_TEST_ROOT/stress/80_run_runtime_maintenance.apex"
    run_apex "$E2E_TEST_ROOT/stress/81_run_job_scheduler_maintenance.apex"
    if run_apex "$E2E_TEST_ROOT/stress/90_assert_converged.apex"; then
        break
    fi
    if (( SECONDS - started_at >= 900 )); then
        echo "Timed out waiting for the mixed transport stress workload to converge." >&2
        exit 1
    fi
    sleep 20
done

run_apex "$E2E_TEST_ROOT/stress/80_run_runtime_maintenance.apex"
run_apex "$E2E_TEST_ROOT/stress/81_run_job_scheduler_maintenance.apex"
run_apex "$E2E_TEST_ROOT/stress/91_assert_runtime_maintenance_noop.apex"
run_apex "$E2E_TEST_ROOT/stress/92_assert_job_scheduler_maintenance_noop.apex"

echo
echo "Mixed transport stress e2e suite completed successfully."
