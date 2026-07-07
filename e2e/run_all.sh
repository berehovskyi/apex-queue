#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

echo "Running all apex-queue e2e suites from $REPO_ROOT"
echo "Prerequisite: deploy e2e/main before running this script."

echo
echo "==> Assigning QueuesAdmin"
assign_permission_set "QueuesAdmin" || echo "Could not assign QueuesAdmin; continuing."

bash "$SCRIPT_DIR/run_queue_admin.sh"
bash "$SCRIPT_DIR/run_job.sh"
bash "$SCRIPT_DIR/run_flow_producer.sh"
bash "$SCRIPT_DIR/run_job_scheduler.sh"
bash "$SCRIPT_DIR/run_worker.sh"
bash "$SCRIPT_DIR/run_queueable.sh"
bash "$SCRIPT_DIR/run_queueable_concurrent.sh"
bash "$SCRIPT_DIR/run_invocable.sh"
bash "$SCRIPT_DIR/run_queue_events.sh"
bash "$SCRIPT_DIR/run_maintenance.sh"

echo
echo "All apex-queue e2e suites completed successfully."
