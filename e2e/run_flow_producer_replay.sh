#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

echo "Running apex-queue FlowProducer replay e2e suite from $REPO_ROOT"
echo "Prerequisite: deploy e2e/main before running this script."

run_apex "$E2E_TEST_ROOT/flow_producer/00_cleanup.apex"

run_parallel_apex_pair "$E2E_TEST_ROOT/flow_producer/40_should_submit_idempotent_replay_graph.apex" 2
run_apex "$E2E_TEST_ROOT/flow_producer/41_should_assert_idempotent_replay_graph.apex"
run_apex "$E2E_TEST_ROOT/flow_producer/00_cleanup.apex"

run_parallel_apex_pair "$E2E_TEST_ROOT/flow_producer/42_should_submit_anonymous_child_graph.apex" 1
run_apex "$E2E_TEST_ROOT/flow_producer/43_should_assert_anonymous_child_extension_rolled_back.apex"
run_apex "$E2E_TEST_ROOT/flow_producer/00_cleanup.apex"

echo
echo "FlowProducer replay e2e suite completed successfully."
