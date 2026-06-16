#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

echo "Running apex-queue maintenance e2e suite from $REPO_ROOT"
echo "Prerequisite: deploy e2e/main before running this script."

run_apex "$E2E_TEST_ROOT/maintenance/00_cleanup.apex"
run_apex "$E2E_TEST_ROOT/maintenance/10_should_apply_terminal_cleanup_by_queue_policy.apex"
run_apex "$E2E_TEST_ROOT/maintenance/20_should_seed_lost_invocable_wake.apex"
run_apex "$E2E_TEST_ROOT/maintenance/21_should_repair_lost_invocable_wake.apex"
run_apex_until_success "$E2E_TEST_ROOT/maintenance/22_should_assert_repaired_lost_invocable_wake_completed.apex" 180 15
run_apex "$E2E_TEST_ROOT/maintenance/00_cleanup.apex"

echo
echo "Maintenance e2e suite completed successfully."
