#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
E2E_TEST_ROOT="e2e/test"
SF_BIN="${SF_BIN:-sf}"
SF_TARGET_ORG="${SF_TARGET_ORG:-}"

ensure_sf_cli() {
    if ! command -v "$SF_BIN" >/dev/null 2>&1; then
        echo "Could not find Salesforce CLI command: $SF_BIN" >&2
        exit 1
    fi
}

assign_permission_set() {
    local permission_set_name="$1"

    if [[ -n "$SF_TARGET_ORG" ]]; then
        "$SF_BIN" org assign permset --target-org "$SF_TARGET_ORG" --name "$permission_set_name"
    else
        "$SF_BIN" org assign permset --name "$permission_set_name"
    fi
}

run_apex() {
    local file_path="$1"
    echo
    echo "==> Running ${file_path}"

    if [[ -n "$SF_TARGET_ORG" ]]; then
        "$SF_BIN" apex run --target-org "$SF_TARGET_ORG" --file "$file_path"
    else
        "$SF_BIN" apex run --file "$file_path"
    fi
}

run_apex_until_success() {
    local file_path="$1"
    local timeout_seconds="$2"
    local interval_seconds="${3:-15}"
    local start_seconds="$SECONDS"
    local attempt=1
    local last_output=""

    while true; do
        echo
        echo "==> Assertion attempt ${attempt}: ${file_path}"
        local output=""
        if output="$(run_apex "$file_path" 2>&1)"; then
            printf '%s\n' "$output"
            return 0
        fi
        last_output="$output"

        local elapsed_seconds=$((SECONDS - start_seconds))
        if (( elapsed_seconds >= timeout_seconds )); then
            echo "Timed out after ${elapsed_seconds}s waiting for ${file_path} to pass." >&2
            printf '%s\n' "$last_output" >&2
            return 1
        fi

        echo "Assertion not ready after ${elapsed_seconds}s; retrying in ${interval_seconds}s."
        sleep "$interval_seconds"
        attempt=$((attempt + 1))
    done
}

run_parallel_apex_pair() {
    local file_path="$1"
    local expected_successes="$2"
    local first_output
    local second_output
    first_output="$(mktemp)"
    second_output="$(mktemp)"

    echo
    echo "==> Running two concurrent submissions: ${file_path}"
    run_apex "$file_path" >"$first_output" 2>&1 &
    local first_pid=$!
    run_apex "$file_path" >"$second_output" 2>&1 &
    local second_pid=$!

    set +e
    wait "$first_pid"
    local first_status=$?
    wait "$second_pid"
    local second_status=$?
    set -e

    cat "$first_output"
    cat "$second_output"
    rm -f "$first_output" "$second_output"

    local success_count=0
    if (( first_status == 0 )); then
        success_count=$((success_count + 1))
    fi
    if (( second_status == 0 )); then
        success_count=$((success_count + 1))
    fi
    if (( success_count != expected_successes )); then
        echo "Expected ${expected_successes} successful concurrent submissions, got ${success_count}." >&2
        return 1
    fi
}

wait_for_async() {
    local seconds="$1"
    echo
    echo "==> Waiting ${seconds}s for async worker completion"
    sleep "$seconds"
}

enter_repo_root() {
    cd "$REPO_ROOT"
}
