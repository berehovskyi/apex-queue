#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

RAW_RUN_KEY="${RAW_RUN_KEY:-raw-performance}"
RAW_TEST_ROOT="$E2E_TEST_ROOT/performance/raw"
RAW_WAIT_TIMEOUT_SECONDS="${RAW_WAIT_TIMEOUT_SECONDS:-1800}"
RAW_POLL_INTERVAL_SECONDS="${RAW_POLL_INTERVAL_SECONDS:-5}"

raw_query_csv() {
    local query="$1"
    if [[ -n "$SF_TARGET_ORG" ]]; then
        "$SF_BIN" data query --target-org "$SF_TARGET_ORG" --query "$query" --result-format csv
    else
        "$SF_BIN" data query --query "$query" --result-format csv
    fi
}

raw_unresolved_summary() {
    local query_csv="$1"
    printf "%s\n" "$query_csv" | awk -F, '
        NR == 1 {
            next;
        }
        {
            gsub(/\r/, "", $0);
            variant = $1 == "" ? "NULL" : $1;
            status = $2 == "" ? "NULL" : $2;
            countValue = $3;
            gsub(/^"|"$/, "", variant);
            gsub(/^"|"$/, "", status);
            gsub(/^"|"$/, "", countValue);
            count = countValue + 0;
            if (count <= 0) {
                next;
            }
            total += count;
            parts = parts " " variant ":" status "=" count;
        }
        END {
            print total + 0 parts;
        }
    '
}

print_raw_status_progress() {
    local query_csv="$1"
    printf "%s\n" "$query_csv" | awk -F, '
        NR == 1 {
            next;
        }
        {
            gsub(/\r/, "", $0);
            variant = $1 == "" ? "NULL" : $1;
            status = $2 == "" ? "NULL" : $2;
            countValue = $3;
            gsub(/^"|"$/, "", variant);
            gsub(/^"|"$/, "", status);
            gsub(/^"|"$/, "", countValue);
            count = countValue + 0;
            if (count <= 0) {
                next;
            }
            counts[variant, status] = count;
            totals[variant] += count;
            if (!(variant in seen)) {
                seen[variant] = 1;
                variants[++variantCount] = variant;
            }
        }
        END {
            timestampCommand = "date -u +%Y-%m-%dT%H:%M:%SZ";
            timestampCommand | getline timestamp;
            close(timestampCommand);
            for (variantIndex = 1; variantIndex <= variantCount; variantIndex++) {
                variant = variants[variantIndex];
                printf "%s %-18s total=%d WAITING=%d COMPLETED=%d FAILED=%d\n",
                    timestamp,
                    variant,
                    totals[variant] + 0,
                    counts[variant, "WAITING"] + 0,
                    counts[variant, "COMPLETED"] + 0,
                    counts[variant, "FAILED"] + 0;
            }
        }
    '
}

wait_for_raw_runs_resolved() {
    local variant="$1"
    local started_at
    started_at="$(date -u +%s)"

    while true; do
        local status_query
        local status_csv
        local unresolved_query
        local unresolved_csv
        local summary
        local unresolved_count

        status_query="SELECT Variant__c, Status__c, COUNT(Id) FROM RawPerformanceRun__c WHERE RunKey__c = '$RAW_RUN_KEY' AND Variant__c = '$variant' GROUP BY Variant__c, Status__c ORDER BY Variant__c, Status__c"
        unresolved_query="SELECT Variant__c, Status__c, COUNT(Id) FROM RawPerformanceRun__c WHERE RunKey__c = '$RAW_RUN_KEY' AND Variant__c = '$variant' AND Status__c NOT IN ('COMPLETED', 'FAILED') GROUP BY Variant__c, Status__c ORDER BY Variant__c, Status__c"

        if status_csv="$(raw_query_csv "$status_query" 2>/dev/null)"; then
            print_raw_status_progress "$status_csv"
        else
            echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) raw status query failed"
        fi

        if unresolved_csv="$(raw_query_csv "$unresolved_query" 2>/dev/null)"; then
            summary="$(raw_unresolved_summary "$unresolved_csv")"
            unresolved_count="${summary%% *}"
            if [[ "$unresolved_count" == "0" ]]; then
                return
            fi
        else
            summary="query failed"
            unresolved_count="-"
        fi

        local now
        now="$(date -u +%s)"
        local elapsed_seconds=$((now - started_at))
        if (( elapsed_seconds >= RAW_WAIT_TIMEOUT_SECONDS )); then
            echo
            echo "Timed out after ${RAW_WAIT_TIMEOUT_SECONDS}s waiting for ${variant}. Unresolved:${summary}" >&2
            return 1
        fi

        sleep "$RAW_POLL_INTERVAL_SECONDS"
    done
}

run_raw_variant() {
    local variant="$1"
    local file_path="$2"

    run_apex "$file_path"
    echo
    echo "==> Polling ${variant} every ${RAW_POLL_INTERVAL_SECONDS}s"
    echo "Resolver: wait until ${variant} rows are COMPLETED or FAILED, timeout ${RAW_WAIT_TIMEOUT_SECONDS}s"
    wait_for_raw_runs_resolved "$variant"
}

echo "Running raw async performance e2e suite from $REPO_ROOT"
echo "Prerequisite: deploy e2e/main before running this script."
echo "This suite is opt-in and is intentionally not included in run_all.sh."

assign_permission_set "RawPerformanceRunAccess" || echo "Could not assign RawPerformanceRunAccess; continuing."

run_apex "$RAW_TEST_ROOT/00_cleanup.apex"
run_raw_variant "BATCH_ITERATOR" "$RAW_TEST_ROOT/10_enqueue_batch_iterator.apex"
run_raw_variant "BATCH_CURSOR" "$RAW_TEST_ROOT/20_enqueue_batch_cursor.apex"
run_raw_variant "BATCH_SERIAL" "$RAW_TEST_ROOT/25_enqueue_batch_serial.apex"
run_raw_variant "QUEUEABLE_SERIAL" "$RAW_TEST_ROOT/30_enqueue_queueable_serial.apex"
run_raw_variant "QUEUEABLE_FUTURE" "$RAW_TEST_ROOT/40_enqueue_queueable_future.apex"
run_raw_variant "INVOCABLE" "$RAW_TEST_ROOT/50_enqueue_invokable.apex"

run_apex "$RAW_TEST_ROOT/90_assert_completed.apex"
run_apex "$RAW_TEST_ROOT/95_print_summary.apex"

echo
echo "Raw performance e2e suite completed successfully."
