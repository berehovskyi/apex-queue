#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

echo "Running apex-queue queue-events e2e suite from $REPO_ROOT"
echo "Prerequisite: deploy e2e/main before running this script."

assign_permission_set "QueuesAdmin" || echo "Could not assign QueuesAdmin; continuing."
assign_permission_set "QueueEventCaptureAccess" || echo "Could not assign QueueEventCaptureAccess; continuing."

run_apex "$E2E_TEST_ROOT/queue_events/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/queue_events/10_should_publish_job_lifecycle_events.apex"
run_apex_until_success "$E2E_TEST_ROOT/queue_events/20_should_assert_job_lifecycle_events_delivered.apex" 120 5
run_apex "$E2E_TEST_ROOT/queue_events/00_cleanup.apex"

echo
echo "Queue-events e2e suite completed successfully."
