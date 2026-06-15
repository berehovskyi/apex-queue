#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

echo "Running apex-queue flow producer e2e suite from $REPO_ROOT"
echo "Prerequisite: deploy e2e/main before running this script."

run_apex "$E2E_TEST_ROOT/flow_producer/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/flow_producer/10_should_add_worker_flow.apex"
wait_for_async 30
run_apex_until_success "$E2E_TEST_ROOT/flow_producer/11_should_assert_worker_flow_completed.apex" 180 10
run_apex "$E2E_TEST_ROOT/flow_producer/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/flow_producer/12_should_add_cross_queue_worker_flow.apex"
wait_for_async 30
run_apex_until_success "$E2E_TEST_ROOT/flow_producer/13_should_assert_cross_queue_worker_flow_completed.apex" 180 10
run_apex "$E2E_TEST_ROOT/flow_producer/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/flow_producer/20_should_add_queueable_flow.apex"
wait_for_async 30
run_apex_until_success "$E2E_TEST_ROOT/flow_producer/21_should_assert_queueable_flow_completed.apex" 180 10
run_apex "$E2E_TEST_ROOT/flow_producer/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/flow_producer/22_should_add_deep_queueable_flow.apex"
wait_for_async 30
run_apex_until_success "$E2E_TEST_ROOT/flow_producer/23_should_assert_deep_queueable_flow_completed.apex" 180 10
run_apex "$E2E_TEST_ROOT/flow_producer/00_cleanup.apex"

run_apex "$E2E_TEST_ROOT/flow_producer/30_should_add_worker_policy_flows.apex"
wait_for_async 30
run_apex_until_success "$E2E_TEST_ROOT/flow_producer/31_should_assert_worker_policy_flows.apex" 180 10
run_apex "$E2E_TEST_ROOT/flow_producer/00_cleanup.apex"

run_parallel_apex_pair "$E2E_TEST_ROOT/flow_producer/40_should_submit_idempotent_replay_graph.apex" 2
run_apex "$E2E_TEST_ROOT/flow_producer/41_should_assert_idempotent_replay_graph.apex"
run_apex "$E2E_TEST_ROOT/flow_producer/00_cleanup.apex"

run_parallel_apex_pair "$E2E_TEST_ROOT/flow_producer/42_should_submit_anonymous_child_graph.apex" 1
run_apex "$E2E_TEST_ROOT/flow_producer/43_should_assert_anonymous_child_extension_rolled_back.apex"
run_apex "$E2E_TEST_ROOT/flow_producer/00_cleanup.apex"

echo
echo "Flow producer e2e suite completed successfully."
