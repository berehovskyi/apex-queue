#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

PERFORMANCE_WORKER_QUEUE_NAME="'e2e-performance-worker'"
PERFORMANCE_PARALLEL_WORKER_QUEUE_NAMES="'e2e-performance-worker-parallel-1', 'e2e-performance-worker-parallel-2', 'e2e-performance-worker-parallel-3', 'e2e-performance-worker-parallel-4', 'e2e-performance-worker-parallel-5'"
PERFORMANCE_QUEUEABLE_SERIAL_QUEUE_NAME="'e2e-performance-queueable'"
PERFORMANCE_QUEUEABLE_CONCURRENT_QUEUE_NAME="'e2e-performance-queueable-concurrent'"
PERFORMANCE_INVOCABLE_QUEUE_NAME="'e2e-performance-invocable'"
PERFORMANCE_QUEUEABLE_QUEUE_NAMES="$PERFORMANCE_QUEUEABLE_SERIAL_QUEUE_NAME, $PERFORMANCE_QUEUEABLE_CONCURRENT_QUEUE_NAME"
PERFORMANCE_QUEUE_NAMES="$PERFORMANCE_WORKER_QUEUE_NAME, $PERFORMANCE_PARALLEL_WORKER_QUEUE_NAMES, $PERFORMANCE_QUEUEABLE_QUEUE_NAMES, $PERFORMANCE_INVOCABLE_QUEUE_NAME"
ACTIVE_PERFORMANCE_QUEUE_NAMES="$PERFORMANCE_QUEUE_NAMES"
VISIBLE_PERFORMANCE_QUEUE_NAMES="$PERFORMANCE_QUEUE_NAMES"
JOB_RUN_DURATION_QUERY="SELECT StartedAt__c, FinishedAt__c, Job__r.ExecutionMode__c, Job__r.QueueName__c FROM JobRun__c WHERE Job__r.QueueName__c IN ($PERFORMANCE_QUEUE_NAMES) AND StartedAt__c != NULL AND FinishedAt__c != NULL"
JOB_STATE_POLL_PID=""
JOB_STATE_PROGRESS_WIDTH="${JOB_STATE_PROGRESS_WIDTH:-60}"
JOB_STATE_PROGRESS_REPLACE="${JOB_STATE_PROGRESS_REPLACE:-true}"
PERFORMANCE_WAIT_TIMEOUT_SECONDS="${PERFORMANCE_WAIT_TIMEOUT_SECONDS:-3600}"

job_state_query_csv() {
    local query="SELECT QueueName__c, ExecutionMode__c, State__c, COUNT(Id) FROM Job__c WHERE QueueName__c IN ($VISIBLE_PERFORMANCE_QUEUE_NAMES) GROUP BY QueueName__c, ExecutionMode__c, State__c ORDER BY QueueName__c, ExecutionMode__c"
    if [[ -n "$SF_TARGET_ORG" ]]; then
        "$SF_BIN" data query --target-org "$SF_TARGET_ORG" --query "$query" --result-format csv
    else
        "$SF_BIN" data query --query "$query" --result-format csv
    fi
}

job_run_duration_query_json() {
    if [[ -n "$SF_TARGET_ORG" ]]; then
        "$SF_BIN" data query --target-org "$SF_TARGET_ORG" --query "$JOB_RUN_DURATION_QUERY" --json
    else
        "$SF_BIN" data query --query "$JOB_RUN_DURATION_QUERY" --json
    fi
}

job_unresolved_query_csv() {
    local query="SELECT State__c, COUNT(Id) FROM Job__c WHERE QueueName__c IN ($ACTIVE_PERFORMANCE_QUEUE_NAMES) AND State__c NOT IN ('COMPLETED', 'FAILED') GROUP BY State__c"
    if [[ -n "$SF_TARGET_ORG" ]]; then
        "$SF_BIN" data query --target-org "$SF_TARGET_ORG" --query "$query" --result-format csv
    else
        "$SF_BIN" data query --query "$query" --result-format csv
    fi
}

unresolved_job_summary() {
    local query_csv="$1"
    printf "%s\n" "$query_csv" | awk -F, '
        NR == 1 {
            next;
        }
        {
            gsub(/\r/, "", $0);
            state = $1 == "" ? "NULL" : $1;
            countValue = $2;
            gsub(/^"|"$/, "", state);
            gsub(/^"|"$/, "", countValue);
            count = countValue + 0;
            if (count <= 0) {
                next;
            }
            total += count;
            parts = parts " " state "=" count;
        }
        END {
            print total + 0 parts;
        }
    '
}

wait_for_performance_jobs_resolved() {
    local timeout_seconds="${1:-$PERFORMANCE_WAIT_TIMEOUT_SECONDS}"
    local started_at
    started_at="$(date -u +%s)"

    while true; do
        local query_csv
        local summary
        local unresolved_count
        if query_csv="$(job_unresolved_query_csv 2>/dev/null)"; then
            summary="$(unresolved_job_summary "$query_csv")"
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
        if (( elapsed_seconds >= timeout_seconds )); then
            echo
            echo "Timed out after ${timeout_seconds}s waiting for performance jobs to resolve. Unresolved:${summary}" >&2
            return 1
        fi

        sleep 1
    done
}

print_job_run_durations() {
    local query_json
    echo
    echo "==> JobRun__c total duration by execution mode"
    if ! query_json="$(job_run_duration_query_json 2>/dev/null)"; then
        echo "Duration query failed."
        return
    fi

    # shellcheck disable=SC2016
    printf "%s\n" "$query_json" | node -e '
        let input = "";
        process.stdin.on("data", (chunk) => input += chunk);
        process.stdin.on("end", () => {
            const raw = JSON.parse(input);
            const records = raw?.result?.records ?? raw?.records ?? [];
            const groups = new Map();
            const resolveMode = (record) => {
                const job = record.Job__r ?? {};
                if (job.QueueName__c?.startsWith("e2e-performance-worker-parallel-")) return "WORKER_PARALLEL";
                if (job.ExecutionMode__c) return job.ExecutionMode__c;
                if (job.QueueName__c === "e2e-performance-worker") return "WORKER";
                if (job.QueueName__c === "e2e-performance-queueable") return "QUEUEABLE_SERIAL";
                if (job.QueueName__c === "e2e-performance-queueable-concurrent") return "QUEUEABLE_CONCURRENT";
                if (job.QueueName__c === "e2e-performance-invocable") return "INVOCABLE";
                return "NULL";
            };
            for (const record of records) {
                const startedAtMs = Date.parse(record.StartedAt__c);
                const finishedAtMs = Date.parse(record.FinishedAt__c);
                if (!Number.isFinite(startedAtMs) || !Number.isFinite(finishedAtMs)) continue;
                const mode = resolveMode(record);
                const current = groups.get(mode) ?? { mode, startedAtMs, finishedAtMs };
                current.startedAtMs = Math.min(current.startedAtMs, startedAtMs);
                current.finishedAtMs = Math.max(current.finishedAtMs, finishedAtMs);
                groups.set(mode, current);
            }
            const rank = (mode) =>
                mode === "WORKER" ? 0 :
                mode === "WORKER_PARALLEL" ? 1 :
                mode === "QUEUEABLE_SERIAL" ? 2 :
                mode === "QUEUEABLE_CONCURRENT" ? 3 :
                mode === "INVOCABLE" ? 4 :
                5;
            const formatDate = (value) => new Date(value).toISOString().replace("Z", "+0000");
            [...groups.values()]
                .sort((left, right) => rank(left.mode) - rank(right.mode) || left.mode.localeCompare(right.mode))
                .forEach((group) => {
                    const durationSeconds = Math.max(0, Math.round((group.finishedAtMs - group.startedAtMs) / 1000));
                    console.log(
                        `${group.mode.padEnd(18)} ${String(durationSeconds).padStart(4)}s  ` +
                        `${formatDate(group.startedAtMs)} -> ${formatDate(group.finishedAtMs)}`
                    );
                });
        });
    '
}

render_job_state_progress() {
    local query_csv="$1"
    printf "%s\n" "$query_csv" | PROGRESS_WIDTH="$JOB_STATE_PROGRESS_WIDTH" awk -F, '
        BEGIN {
            width = ENVIRON["PROGRESS_WIDTH"] + 0;
            if (width < 10) {
                width = 10;
            }
            orderedStateCount = split("COMPLETED ACTIVE FAILED WAITING DELAYED PAUSED", orderedStates, " ");
            for (knownStateIndex = 1; knownStateIndex <= orderedStateCount; knownStateIndex++) {
                knownStates[orderedStates[knownStateIndex]] = 1;
            }
            colors["WAITING"] = "\033[0m";
            colors["DELAYED"] = "\033[0m";
            colors["PAUSED"] = "\033[0m";
            colors["ACTIVE"] = "\033[33m";
            colors["COMPLETED"] = "\033[32m";
            colors["FAILED"] = "\033[31m";
            reset = "\033[0m";
            orderedModeCount = split("WORKER WORKER_PARALLEL QUEUEABLE_SERIAL QUEUEABLE_CONCURRENT INVOCABLE", orderedModes, " ");
            labels["WORKER"] = "WORKER            ";
            labels["WORKER_PARALLEL"] = "WORKER_PARALLEL   ";
            labels["QUEUEABLE_SERIAL"] = "QUEUEABLE_SERIAL ";
            labels["QUEUEABLE_CONCURRENT"] = "QUEUEABLE_CONCURRENT";
            labels["INVOCABLE"] = "INVOCABLE         ";
        }
        NR == 1 {
            next;
        }
        {
            gsub(/\r/, "", $0);
            queueName = $1 == "" ? "NULL" : $1;
            mode = $2 == "" ? "NULL" : $2;
            state = $3 == "" ? "NULL" : $3;
            countValue = $4;
            gsub(/^"|"$/, "", queueName);
            gsub(/^"|"$/, "", mode);
            gsub(/^"|"$/, "", state);
            gsub(/^"|"$/, "", countValue);
            if (queueName ~ /^e2e-performance-worker-parallel-/) {
                mode = "WORKER_PARALLEL";
            }
            count = countValue + 0;
            if (count <= 0) {
                next;
            }
            counts[mode, state] += count;
            totals[mode] += count;
            if (!(mode in seenMode)) {
                seenMode[mode] = 1;
                seenModes[++seenModeCount] = mode;
            }
            if (!(state in seenState)) {
                seenState[state] = 1;
                seenStates[++seenStateCount] = state;
            }
        }
        END {
            timestampCommand = "date -u +%Y-%m-%dT%H:%M:%SZ";
            timestampCommand | getline timestamp;
            close(timestampCommand);
            for (modeIndex = 1; modeIndex <= orderedModeCount; modeIndex++) {
                renderMode(orderedModes[modeIndex]);
            }
            for (modeIndex = 1; modeIndex <= seenModeCount; modeIndex++) {
                mode = seenModes[modeIndex];
                if (mode != "WORKER" && mode != "WORKER_PARALLEL" && mode != "QUEUEABLE_SERIAL" && mode != "QUEUEABLE_CONCURRENT" && mode != "INVOCABLE") {
                    renderMode(mode);
                }
            }
        }
        function renderMode(mode, label) {
            remaining = width;
            bar = "";
            parts = "";
            total = totals[mode] + 0;
            for (stateIndex = 1; stateIndex <= orderedStateCount; stateIndex++) {
                appendState(mode, orderedStates[stateIndex]);
            }
            for (stateIndex = 1; stateIndex <= seenStateCount; stateIndex++) {
                state = seenStates[stateIndex];
                if (!(state in knownStates)) {
                    appendState(mode, state);
                }
            }
            while (remaining > 0) {
                bar = bar ".";
                remaining--;
            }
            label = (mode in labels) ? labels[mode] : mode;
            print timestamp " " label " [" bar reset "] total=" total parts;
        }
        function appendState(mode, state, count, segmentWidth, color, i) {
            count = counts[mode, state] + 0;
            if (count <= 0 || remaining <= 0) {
                return;
            }
            segmentWidth = total > 0 ? int((count / total) * width + 0.5) : 0;
            if (segmentWidth < 1) {
                segmentWidth = 1;
            }
            if (segmentWidth > remaining) {
                segmentWidth = remaining;
            }
            remaining -= segmentWidth;
            color = (state in colors) ? colors[state] : reset;
            bar = bar color;
            for (i = 0; i < segmentWidth; i++) {
                bar = bar "=";
            }
            bar = bar reset;
            parts = parts " " state "=" count;
        }
    '
}

print_job_state_progress() {
    local progress="$1"
    if [[ "$JOB_STATE_PROGRESS_REPLACE" == "true" ]]; then
        printf "\033[5A\033[2K%s\n\033[2K" "$(printf "%s" "$progress" | sed -n '1p')"
        printf "%s\n\033[2K" "$(printf "%s" "$progress" | sed -n '2p')"
        printf "%s\n\033[2K" "$(printf "%s" "$progress" | sed -n '3p')"
        printf "%s\n\033[2K" "$(printf "%s" "$progress" | sed -n '4p')"
        printf "%s\n" "$(printf "%s" "$progress" | sed -n '5p')"
        return
    fi

    printf "%s\n" "$progress"
}

poll_job_states() {
    while true; do
        if query_csv="$(job_state_query_csv 2>/dev/null)"; then
            print_job_state_progress "$(render_job_state_progress "$query_csv")"
        else
            echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [poll failed]"
        fi
        sleep 1
    done
}

start_job_state_polling() {
    poll_job_states &
    JOB_STATE_POLL_PID="$!"
}

stop_job_state_polling() {
    if [[ -z "$JOB_STATE_POLL_PID" ]]; then
        return
    fi

    kill "$JOB_STATE_POLL_PID" 2>/dev/null || true
    wait "$JOB_STATE_POLL_PID" 2>/dev/null || true
    JOB_STATE_POLL_PID=""
}

trap stop_job_state_polling EXIT

run_performance_phase() {
    local phase_name="$1"
    local queue_names="$2"
    local enqueue_script="$3"
    shift 3

    ACTIVE_PERFORMANCE_QUEUE_NAMES="$queue_names"

    run_apex "$enqueue_script"
    echo
    echo "==> Polling $phase_name Job__c state progress every second"
    echo "Partition: ExecutionMode__c, with parallel worker shard queues shown as WORKER_PARALLEL"
    echo "Legend: left-to-right green=COMPLETED yellow=ACTIVE red=FAILED plain=WAITING/DELAYED/PAUSED"
    echo "Resolver: wait until $phase_name jobs are COMPLETED or FAILED, timeout ${PERFORMANCE_WAIT_TIMEOUT_SECONDS}s"
    if [[ "$JOB_STATE_PROGRESS_REPLACE" == "true" ]]; then
        printf "Preparing worker progress...\nPreparing parallel worker progress...\nPreparing queueable serial progress...\nPreparing queueable concurrent progress...\nPreparing invocable progress...\n"
    fi
    start_job_state_polling
    wait_for_performance_jobs_resolved "$@"
    stop_job_state_polling
    echo
    echo "==> $phase_name performance jobs resolved"
}

echo "Running apex-queue performance e2e suite from $REPO_ROOT"
echo "Prerequisite: deploy e2e/main before running this script."
echo "This suite is opt-in and is intentionally not included in run_all.sh."

run_apex "$E2E_TEST_ROOT/performance/00_cleanup.apex"
run_performance_phase "queueable serial" "$PERFORMANCE_QUEUEABLE_SERIAL_QUEUE_NAME" "$E2E_TEST_ROOT/performance/20_enqueue_queueable_load.apex" "$@"
run_performance_phase "queueable concurrent" "$PERFORMANCE_QUEUEABLE_CONCURRENT_QUEUE_NAME" "$E2E_TEST_ROOT/performance/25_enqueue_queueable_concurrent_load.apex" "$@"
run_performance_phase "invocable" "$PERFORMANCE_INVOCABLE_QUEUE_NAME" "$E2E_TEST_ROOT/performance/30_enqueue_invokable_load.apex" "$@"
run_performance_phase "worker" "$PERFORMANCE_WORKER_QUEUE_NAME" "$E2E_TEST_ROOT/performance/10_enqueue_worker_load.apex" "$@"
run_performance_phase "parallel worker" "$PERFORMANCE_PARALLEL_WORKER_QUEUE_NAMES" "$E2E_TEST_ROOT/performance/15_enqueue_parallel_worker_load.apex" "$@"
run_apex "$E2E_TEST_ROOT/performance/16_assert_parallel_worker_load_completed.apex"
run_apex "$E2E_TEST_ROOT/performance/40_assert_load_completed.apex"
print_job_run_durations
bash "$SCRIPT_DIR/run_performance_report.sh"
#run_apex "$E2E_TEST_ROOT/performance/00_cleanup.apex"

echo
echo "Performance e2e suite completed successfully."
