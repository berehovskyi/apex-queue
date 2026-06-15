#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

ensure_sf_cli
enter_repo_root

ARTIFACT_DIR="${E2E_ARTIFACT_DIR:-e2e/.artifacts/performance}"
RAW_JSON_PATH="$ARTIFACT_DIR/jobrun-spans.raw.json"
DISPATCH_JSON_PATH="$ARTIFACT_DIR/queueable-dispatch-spans.raw.json"
DISPATCH_JOB_JSON_PATH="$ARTIFACT_DIR/queueable-dispatch-jobs.raw.json"
REPORT_PATH="$ARTIFACT_DIR/jobrun-spans.html"

mkdir -p "$ARTIFACT_DIR"

SOQL="
SELECT
    Id,
    AttemptNumber__c,
    AsyncApexJobId__c,
    DurationMs__c,
    FinishedAt__c,
    Job__c,
    Job__r.ExecutionMode__c,
    Job__r.ExternalId__c,
    Job__r.Progress__c,
    Job__r.QueueName__c,
    Job__r.QueueableDispatch__c,
    StartedAt__c,
    Status__c
FROM JobRun__c
WHERE Job__r.QueueName__c IN (
    'e2e-performance-worker',
    'e2e-performance-worker-parallel-1',
    'e2e-performance-worker-parallel-2',
    'e2e-performance-worker-parallel-3',
    'e2e-performance-worker-parallel-4',
    'e2e-performance-worker-parallel-5',
    'e2e-performance-queueable',
    'e2e-performance-queueable-concurrent',
    'e2e-performance-invocable'
)
AND StartedAt__c != NULL
AND FinishedAt__c != NULL
ORDER BY StartedAt__c ASC, Job__r.QueueName__c ASC, Job__r.ExternalId__c ASC
"

DISPATCH_SOQL="
SELECT
    Id,
    DispatchDepth__c,
    DispatchedCount__c,
    DispatcherAsyncApexJobId__c,
    FailedReason__c,
    FinishedAt__c,
    Mode__c,
    QueueName__c,
    RequestedCount__c,
    StartedAt__c,
    State__c
FROM QueueableDispatch__c
WHERE QueueName__c IN ('e2e-performance-queueable', 'e2e-performance-queueable-concurrent')
AND StartedAt__c != NULL
AND FinishedAt__c != NULL
ORDER BY StartedAt__c ASC, Id ASC
"

DISPATCH_JOB_SOQL="
SELECT
    Id,
    AsyncApexJobId__c,
    ExternalId__c,
    QueueName__c,
    QueueableDispatch__c,
    QueueableDispatchInitiatedAt__c,
    State__c
FROM Job__c
WHERE QueueName__c IN ('e2e-performance-queueable', 'e2e-performance-queueable-concurrent')
AND QueueableDispatch__c != NULL
AND QueueableDispatchInitiatedAt__c != NULL
ORDER BY QueueableDispatchInitiatedAt__c ASC, ExternalId__c ASC
"

echo
echo "==> Querying raw JobRun__c span data"
if [[ -n "$SF_TARGET_ORG" ]]; then
    "$SF_BIN" data query --target-org "$SF_TARGET_ORG" --query "$SOQL" --json > "$RAW_JSON_PATH"
else
    "$SF_BIN" data query --query "$SOQL" --json > "$RAW_JSON_PATH"
fi

echo "==> Querying raw QueueableDispatch__c span data"
if [[ -n "$SF_TARGET_ORG" ]]; then
    "$SF_BIN" data query --target-org "$SF_TARGET_ORG" --query "$DISPATCH_SOQL" --json > "$DISPATCH_JSON_PATH"
else
    "$SF_BIN" data query --query "$DISPATCH_SOQL" --json > "$DISPATCH_JSON_PATH"
fi

echo "==> Querying raw queueable dispatch job handoff data"
if [[ -n "$SF_TARGET_ORG" ]]; then
    "$SF_BIN" data query --target-org "$SF_TARGET_ORG" --query "$DISPATCH_JOB_SOQL" --json > "$DISPATCH_JOB_JSON_PATH"
else
    "$SF_BIN" data query --query "$DISPATCH_JOB_SOQL" --json > "$DISPATCH_JOB_JSON_PATH"
fi

{
    cat <<'HTML_HEAD'
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Apex Queue Performance Spans</title>
    <style>
        :root {
            color-scheme: light;
            --bg: #f7f8fa;
            --panel: #ffffff;
            --ink: #1e293b;
            --muted: #64748b;
            --rule: #d7dde6;
            --worker: #2563eb;
            --worker-parallel: #0f766e;
            --queueable-serial: #16a34a;
            --queueable-concurrent: #7c3aed;
            --invocable: #ea580c;
            --dispatcher: #0891b2;
            --failed: #dc2626;
            --active: #d97706;
        }

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            background: var(--bg);
            color: var(--ink);
            font-family:
                Inter,
                ui-sans-serif,
                system-ui,
                -apple-system,
                BlinkMacSystemFont,
                "Segoe UI",
                sans-serif;
        }

        header {
            padding: 24px 28px 14px;
            border-bottom: 1px solid var(--rule);
            background: var(--panel);
        }

        h1 {
            margin: 0 0 8px;
            font-size: 22px;
            font-weight: 700;
            letter-spacing: 0;
        }

        .subtle {
            color: var(--muted);
            font-size: 13px;
        }

        .summary {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 10px;
            padding: 16px 28px;
        }

        .metric {
            background: var(--panel);
            border: 1px solid var(--rule);
            border-radius: 8px;
            padding: 12px;
        }

        .metric-label {
            color: var(--muted);
            font-size: 12px;
        }

        .metric-value {
            margin-top: 4px;
            font-size: 20px;
            font-weight: 700;
        }

        .toolbar {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 14px;
            padding: 0 28px 16px;
            color: var(--muted);
            font-size: 13px;
        }

        .toolbar label {
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }

        main {
            padding: 0 28px 28px;
        }

        .chart-wrap {
            overflow: auto;
            border: 1px solid var(--rule);
            border-radius: 8px;
            background: var(--panel);
        }

        svg {
            display: block;
            min-width: 980px;
        }

        .axis text,
        .row-label,
        .section-label,
        .run-meta {
            font-family:
                "SFMono-Regular",
                Consolas,
                "Liberation Mono",
                monospace;
        }

        .axis text {
            fill: var(--muted);
            font-size: 11px;
        }

        .grid {
            stroke: #edf1f6;
            stroke-width: 1;
        }

        .row-label {
            fill: var(--ink);
            font-size: 11px;
        }

        .run-meta {
            fill: var(--muted);
            font-size: 10px;
        }

        .section-label {
            fill: var(--muted);
            font-size: 12px;
            font-weight: 700;
        }

        .bar {
            rx: 3;
            ry: 3;
        }

        .bar.worker {
            fill: var(--worker);
        }

        .bar.worker-parallel {
            fill: var(--worker-parallel);
        }

        .bar.queueable-serial {
            fill: var(--queueable-serial);
        }

        .bar.queueable-concurrent {
            fill: var(--queueable-concurrent);
        }

        .bar.invocable {
            fill: var(--invocable);
        }

        .bar.dispatcher {
            fill: var(--dispatcher);
        }

        .bar.failed {
            fill: var(--failed);
        }

        .bar.active {
            fill: var(--active);
        }

        .legend {
            display: flex;
            flex-wrap: wrap;
            gap: 14px;
            padding: 12px 28px 18px;
            color: var(--muted);
            font-size: 12px;
        }

        .legend-item {
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }

        .swatch {
            width: 18px;
            height: 10px;
            border-radius: 3px;
        }

        .dispatch-point {
            stroke: var(--panel);
            stroke-width: 1;
            fill: var(--dispatcher);
        }
    </style>
</head>
<body>
    <header>
        <h1>Apex Queue Performance Spans</h1>
        <div class="subtle">
            Raw <code>JobRun__c</code> data queried from Salesforce CLI. Each row aligns worker,
            queueable serial, queueable concurrent, and invocable spans by sorted start order.
        </div>
    </header>

    <section class="summary" id="summary"></section>
    <section class="legend">
        <span class="legend-item"><span class="swatch" style="background: var(--worker)"></span>WORKER</span>
        <span class="legend-item"><span class="swatch" style="background: var(--worker-parallel)"></span>WORKER_PARALLEL</span>
        <span class="legend-item"><span class="swatch" style="background: var(--queueable-serial)"></span>QUEUEABLE_SERIAL</span>
        <span class="legend-item"><span class="swatch" style="background: var(--queueable-concurrent)"></span>QUEUEABLE_CONCURRENT</span>
        <span class="legend-item"><span class="swatch" style="background: var(--invocable)"></span>INVOCABLE</span>
        <span class="legend-item"><span class="swatch" style="background: var(--dispatcher)"></span>DISPATCH</span>
        <span class="legend-item"><span class="swatch" style="background: var(--failed)"></span>FAILED</span>
        <span class="legend-item"><span class="swatch" style="background: var(--active)"></span>other status</span>
    </section>
    <section class="toolbar">
        <label><input type="checkbox" id="compactToggle"> Compact rows</label>
        <label><input type="checkbox" id="showDispatchToggle" checked> Dispatch</label>
        <label><input type="checkbox" id="showWorkerToggle" checked> Worker</label>
        <label><input type="checkbox" id="showParallelWorkerToggle" checked> Parallel worker</label>
        <label><input type="checkbox" id="showQueueableSerialToggle" checked> Queueable serial</label>
        <label><input type="checkbox" id="showQueueableConcurrentToggle" checked> Queueable concurrent</label>
        <label><input type="checkbox" id="showInvocableToggle" checked> Invocable</label>
        <span id="rangeLabel"></span>
    </section>
    <main>
        <div class="chart-wrap">
            <svg id="chart" role="img" aria-label="JobRun span chart"></svg>
        </div>
    </main>

    <script id="jobrun-data" type="application/json">
HTML_HEAD
    cat "$RAW_JSON_PATH"
    cat <<'HTML_DISPATCH'
    </script>
    <script id="dispatch-data" type="application/json">
HTML_DISPATCH
    cat "$DISPATCH_JSON_PATH"
    cat <<'HTML_DISPATCH_JOBS'
    </script>
    <script id="dispatch-job-data" type="application/json">
HTML_DISPATCH_JOBS
    cat "$DISPATCH_JOB_JSON_PATH"
    cat <<'HTML_TAIL'
    </script>
    <script>
        const raw = JSON.parse(document.getElementById('jobrun-data').textContent);
        const dispatchRaw = JSON.parse(document.getElementById('dispatch-data').textContent);
        const dispatchJobRaw = JSON.parse(document.getElementById('dispatch-job-data').textContent);
        const records = raw?.result?.records ?? raw?.records ?? [];
        const dispatchRecords = dispatchRaw?.result?.records ?? dispatchRaw?.records ?? [];
        const dispatchJobRecords = dispatchJobRaw?.result?.records ?? dispatchJobRaw?.records ?? [];

        const getJob = (record) => record.Job__r ?? {};
        const parseDate = (value) => new Date(value).getTime();
        const parseJsonObject = (value) => {
            if (!value) return {};
            try {
                const parsed = JSON.parse(value);
                return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
            } catch (error) {
                return {};
            }
        };
        const parseMillis = (value) => {
            const millis = Number(value);
            return Number.isFinite(millis) ? millis : NaN;
        };
        const formatDuration = (ms) => {
            if (!Number.isFinite(ms)) return '0ms';
            if (ms < 1000) return `${Math.round(ms)}ms`;
            if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
            return `${(ms / 60000).toFixed(2)}m`;
        };
        const formatTime = (ms) =>
            new Date(ms).toISOString().replace('T', ' ').replace('Z', ' UTC');
        const escapeText = (value) => String(value ?? '').replace(/[&<>"']/g, (char) => ({
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#39;'
        })[char]);
        const resolvePartition = (job) => {
            const executionMode = job.ExecutionMode__c ?? '';
            if (job.QueueName__c?.startsWith('e2e-performance-worker-parallel-')) {
                return 'WORKER_PARALLEL';
            }
            if (
                executionMode === 'QUEUEABLE_SERIAL' ||
                executionMode === 'QUEUEABLE_CONCURRENT' ||
                executionMode === 'INVOCABLE'
            ) {
                return executionMode;
            }
            if (job.QueueName__c === 'e2e-performance-queueable') {
                return 'QUEUEABLE_SERIAL';
            }
            if (job.QueueName__c === 'e2e-performance-queueable-concurrent') {
                return 'QUEUEABLE_CONCURRENT';
            }
            if (job.QueueName__c === 'e2e-performance-invocable') {
                return 'INVOCABLE';
            }
            return 'WORKER';
        };

        const allRuns = records
            .map((record, index) => {
                const job = getJob(record);
                const progress = parseJsonObject(job.Progress__c);
                const progressStartedAtMs = parseMillis(progress.startedAtMs);
                const durationMs = Number(record.DurationMs__c);
                const startedAtMs = Number.isFinite(progressStartedAtMs)
                    ? progressStartedAtMs
                    : parseDate(record.StartedAt__c);
                const finishedAtMs = Number.isFinite(progressStartedAtMs) && Number.isFinite(durationMs)
                    ? progressStartedAtMs + durationMs
                    : parseDate(record.FinishedAt__c);
                const partition = resolvePartition(job);
                const status = String(record.Status__c ?? '').toUpperCase();
                const label = job.ExternalId__c || record.Job__c || record.Id;
                return {
                    id: record.Id,
                    index,
                    partition,
                    status,
                    queueName: job.QueueName__c ?? '',
                    label,
                    asyncApexJobId: record.AsyncApexJobId__c ?? '',
                    attempt: Number(record.AttemptNumber__c ?? 0),
                    startedAtMs,
                    finishedAtMs,
                    durationMs: Number.isFinite(durationMs) ? durationMs : finishedAtMs - startedAtMs
                };
            })
            .filter((run) => Number.isFinite(run.startedAtMs) && Number.isFinite(run.finishedAtMs))
            .sort((left, right) =>
                left.startedAtMs - right.startedAtMs ||
                partitionRank(left.partition) - partitionRank(right.partition) ||
                left.label.localeCompare(right.label)
            );

        const allDispatchJobs = dispatchJobRecords
            .map((record) => {
                const initiatedAtMs = parseDate(record.QueueableDispatchInitiatedAt__c);
                return {
                    id: record.Id,
                    dispatchId: record.QueueableDispatch__c,
                    queueName: record.QueueName__c ?? '',
                    label: record.ExternalId__c || record.Id,
                    asyncApexJobId: record.AsyncApexJobId__c ?? '',
                    state: record.State__c ?? '',
                    initiatedAtMs
                };
            })
            .filter((job) => job.dispatchId && Number.isFinite(job.initiatedAtMs))
            .sort((left, right) =>
                left.initiatedAtMs - right.initiatedAtMs ||
                left.label.localeCompare(right.label)
            );

        const dispatchJobsByDispatchId = new Map();
        for (const job of allDispatchJobs) {
            if (!dispatchJobsByDispatchId.has(job.dispatchId)) {
                dispatchJobsByDispatchId.set(job.dispatchId, []);
            }
            dispatchJobsByDispatchId.get(job.dispatchId).push(job);
        }

        const allDispatchSpans = dispatchRecords
            .map((record, index) => {
                const startedAtMs = parseDate(record.StartedAt__c);
                const finishedAtMs = parseDate(record.FinishedAt__c);
                const durationMs = finishedAtMs - startedAtMs;
                const dispatchedCount = Number(record.DispatchedCount__c ?? 0);
                return {
                    id: record.Id,
                    index,
                    queueName: record.QueueName__c ?? '',
                    mode: record.Mode__c ?? '',
                    state: String(record.State__c ?? '').toUpperCase(),
                    startedAtMs,
                    finishedAtMs,
                    durationMs,
                    requestedCount: Number(record.RequestedCount__c ?? 0),
                    dispatchedCount,
                    dispatchDepth: Number(record.DispatchDepth__c ?? 0),
                    asyncApexJobId: record.DispatcherAsyncApexJobId__c ?? '',
                    jobs: dispatchJobsByDispatchId.get(record.Id) ?? []
                };
            })
            .filter((dispatch) => Number.isFinite(dispatch.startedAtMs) && Number.isFinite(dispatch.finishedAtMs))
            .sort((left, right) => left.startedAtMs - right.startedAtMs || left.id.localeCompare(right.id));

        const runStartComparator = (left, right) =>
            left.startedAtMs - right.startedAtMs ||
            left.label.localeCompare(right.label);

        function isChecked(id) {
            const input = document.getElementById(id);
            return input ? input.checked : true;
        }

        function buildVisibleModel() {
            const showDispatch = isChecked('showDispatchToggle');
            const showWorker = isChecked('showWorkerToggle');
            const showParallelWorker = isChecked('showParallelWorkerToggle');
            const showQueueableSerial = isChecked('showQueueableSerialToggle');
            const showQueueableConcurrent = isChecked('showQueueableConcurrentToggle');
            const showInvocable = isChecked('showInvocableToggle');
            const runs = allRuns.filter((run) =>
                (run.partition === 'WORKER' && showWorker) ||
                (run.partition === 'WORKER_PARALLEL' && showParallelWorker) ||
                (run.partition === 'QUEUEABLE_SERIAL' && showQueueableSerial) ||
                (run.partition === 'QUEUEABLE_CONCURRENT' && showQueueableConcurrent) ||
                (run.partition === 'INVOCABLE' && showInvocable)
            );
            const dispatchSpans = showDispatch ? allDispatchSpans : [];
            const dispatchJobs = showDispatch ? allDispatchJobs : [];
            const workerRunsByStart = runs.filter((run) => run.partition === 'WORKER').sort(runStartComparator);
            const parallelWorkerRunsByStart = runs
                .filter((run) => run.partition === 'WORKER_PARALLEL')
                .sort(runStartComparator);
            const queueableSerialRunsByStart = runs
                .filter((run) => run.partition === 'QUEUEABLE_SERIAL')
                .sort(runStartComparator);
            const queueableConcurrentRunsByStart = runs
                .filter((run) => run.partition === 'QUEUEABLE_CONCURRENT')
                .sort(runStartComparator);
            const invocableRunsByStart = runs.filter((run) => run.partition === 'INVOCABLE').sort(runStartComparator);
            const pairedRows = Array.from(
                {
                    length: Math.max(
                        workerRunsByStart.length,
                        parallelWorkerRunsByStart.length,
                        queueableSerialRunsByStart.length,
                        queueableConcurrentRunsByStart.length,
                        invocableRunsByStart.length
                    )
                },
                (_, index) => {
                    const rowRuns = [
                        workerRunsByStart[index],
                        parallelWorkerRunsByStart[index],
                        queueableSerialRunsByStart[index],
                        queueableConcurrentRunsByStart[index],
                        invocableRunsByStart[index]
                    ].filter(Boolean);
                    return {
                        key: index,
                        label: `set ${String(index + 1).padStart(3, '0')}`,
                        startedAtMs: Math.min(...rowRuns.map((run) => run.startedAtMs)),
                        runs: rowRuns
                    };
                }
            ).sort((left, right) => left.startedAtMs - right.startedAtMs || left.key - right.key);
            const timelineStarts = [
                ...runs.map((run) => run.startedAtMs),
                ...dispatchSpans.map((dispatch) => dispatch.startedAtMs),
                ...dispatchJobs.map((job) => job.initiatedAtMs)
            ];
            const timelineFinishes = [
                ...runs.map((run) => run.finishedAtMs),
                ...dispatchSpans.map((dispatch) => dispatch.finishedAtMs),
                ...dispatchJobs.map((job) => job.initiatedAtMs)
            ];
            const hasTimeline = timelineStarts.length > 0 && timelineFinishes.length > 0;
            const minTime = hasTimeline ? Math.min(...timelineStarts) : Date.now();
            const maxTime = hasTimeline ? Math.max(...timelineFinishes) : minTime + 1;
            return {
                runs,
                dispatchSpans,
                dispatchJobs,
                pairedRows,
                minTime,
                maxTime,
                totalWallMs: Math.max(maxTime - minTime, 1),
                hasTimeline
            };
        }

        function peakConcurrency(runs, partition) {
            const events = [];
            for (const run of runs.filter((item) => item.partition === partition)) {
                events.push({ at: run.startedAtMs, delta: 1 });
                events.push({ at: run.finishedAtMs, delta: -1 });
            }
            events.sort((left, right) => left.at - right.at || right.delta - left.delta);
            let active = 0;
            let peak = 0;
            for (const event of events) {
                active += event.delta;
                peak = Math.max(peak, active);
            }
            return peak;
        }

        function partitionWall(runs, partition) {
            const partitionRuns = runs.filter((run) => run.partition === partition);
            if (!partitionRuns.length) return 0;
            return Math.max(...partitionRuns.map((run) => run.finishedAtMs)) -
                Math.min(...partitionRuns.map((run) => run.startedAtMs));
        }

        function dispatchWall(dispatchSpans) {
            if (!dispatchSpans.length) return 0;
            return Math.max(...dispatchSpans.map((dispatch) => dispatch.finishedAtMs)) -
                Math.min(...dispatchSpans.map((dispatch) => dispatch.startedAtMs));
        }

        function dispatchRate(dispatchSpans) {
            const dispatchedCount = dispatchSpans.reduce((sum, dispatch) => sum + dispatch.dispatchedCount, 0);
            const wallSeconds = dispatchWall(dispatchSpans) / 1000;
            if (!dispatchedCount || wallSeconds <= 0) return '0/s';
            return `${(dispatchedCount / wallSeconds).toFixed(2)}/s`;
        }

        function partitionRank(partition) {
            return partition === 'WORKER' ? 0 :
                partition === 'WORKER_PARALLEL' ? 1 :
                partition === 'QUEUEABLE_SERIAL' ? 2 :
                partition === 'QUEUEABLE_CONCURRENT' ? 3 :
                partition === 'INVOCABLE' ? 4 :
                5;
        }

        function partitionClass(partition) {
            return partition.toLowerCase().replace(/_/g, '-');
        }

        function renderSummary(model) {
            const workerRuns = model.runs.filter((run) => run.partition === 'WORKER');
            const parallelWorkerRuns = model.runs.filter((run) => run.partition === 'WORKER_PARALLEL');
            const queueableSerialRuns = model.runs.filter((run) => run.partition === 'QUEUEABLE_SERIAL');
            const queueableConcurrentRuns = model.runs.filter((run) => run.partition === 'QUEUEABLE_CONCURRENT');
            const invocableRuns = model.runs.filter((run) => run.partition === 'INVOCABLE');
            const metrics = [
                ['Visible runs', model.runs.length],
                ['Visible wall', model.hasTimeline ? formatDuration(model.totalWallMs) : '0ms'],
                ['Worker runs', workerRuns.length],
                ['Worker wall', formatDuration(partitionWall(model.runs, 'WORKER'))],
                ['Worker peak', peakConcurrency(model.runs, 'WORKER')],
                ['Parallel worker runs', parallelWorkerRuns.length],
                ['Parallel worker wall', formatDuration(partitionWall(model.runs, 'WORKER_PARALLEL'))],
                ['Parallel worker peak', peakConcurrency(model.runs, 'WORKER_PARALLEL')],
                ['Queueable serial runs', queueableSerialRuns.length],
                ['Queueable serial wall', formatDuration(partitionWall(model.runs, 'QUEUEABLE_SERIAL'))],
                ['Queueable serial peak', peakConcurrency(model.runs, 'QUEUEABLE_SERIAL')],
                ['Queueable concurrent runs', queueableConcurrentRuns.length],
                ['Queueable concurrent wall', formatDuration(partitionWall(model.runs, 'QUEUEABLE_CONCURRENT'))],
                ['Queueable concurrent peak', peakConcurrency(model.runs, 'QUEUEABLE_CONCURRENT')],
                ['Invocable runs', invocableRuns.length],
                ['Invocable wall', formatDuration(partitionWall(model.runs, 'INVOCABLE'))],
                ['Invocable peak', peakConcurrency(model.runs, 'INVOCABLE')],
                ['Dispatches', model.dispatchSpans.length],
                ['Dispatch wall', formatDuration(dispatchWall(model.dispatchSpans))],
                ['Dispatch rate', dispatchRate(model.dispatchSpans)]
            ];
            document.getElementById('summary').innerHTML = metrics
                .map(([label, value]) => `
                    <div class="metric">
                        <div class="metric-label">${escapeText(label)}</div>
                        <div class="metric-value">${escapeText(value)}</div>
                    </div>
                `)
                .join('');
            document.getElementById('rangeLabel').textContent =
                model.hasTimeline ? `${formatTime(model.minTime)} -> ${formatTime(model.maxTime)}` : 'No visible spans';
        }

        function renderChart() {
            const model = buildVisibleModel();
            renderSummary(model);
            const compact = document.getElementById('compactToggle').checked;
            const rowHeight = compact ? 30 : 46;
            const barHeight = compact ? 4 : 7;
            const laneGap = compact ? 2 : 3;
            const sectionGap = compact ? 22 : 30;
            const top = 44;
            const left = 300;
            const right = 28;
            const chartWidth = Math.max(1100, window.innerWidth - 96);
            const plotWidth = chartWidth - left - right;
            const dispatchRows = model.dispatchSpans.map((dispatch, index) => ({
                key: index,
                label: `${dispatch.mode || dispatch.queueName} ${String(index + 1).padStart(3, '0')}`,
                dispatch
            }));
            const grouped = [
                ['QUEUEABLE DISPATCH HANDOFFS', dispatchRows],
                ['WORKER / WORKER PARALLEL / QUEUEABLE SERIAL / QUEUEABLE CONCURRENT / INVOCABLE BY SORTED START', model.pairedRows]
            ].filter(([, rows]) => rows.length > 0);
            const totalRows = model.pairedRows.length + dispatchRows.length + grouped.length;
            const height = top + totalRows * rowHeight + grouped.length * sectionGap + 34;
            const x = (ms) => left + ((ms - model.minTime) / model.totalWallMs) * plotWidth;
            const svg = document.getElementById('chart');
            svg.setAttribute('viewBox', `0 0 ${chartWidth} ${height}`);
            svg.setAttribute('width', chartWidth);
            svg.setAttribute('height', height);

            const lines = [];
            if (!model.hasTimeline) {
                lines.push(`<text x="18" y="34" class="section-label">No visible spans</text>`);
                svg.innerHTML = lines.join('');
                return;
            }
            const tickCount = 8;
            for (let tick = 0; tick <= tickCount; tick++) {
                const at = model.minTime + (model.totalWallMs * tick) / tickCount;
                const tx = x(at);
                lines.push(`
                    <line class="grid" x1="${tx}" y1="24" x2="${tx}" y2="${height - 22}"></line>
                    <text x="${tx}" y="18" text-anchor="middle" class="axis">${escapeText(
                        new Date(at).toISOString().slice(11, 23)
                    )}</text>
                `);
            }

            let y = top;
            for (const [section, sectionRuns] of grouped) {
                lines.push(`<text x="18" y="${y}" class="section-label">${section}</text>`);
                y += compact ? 12 : 18;
                for (const row of sectionRuns) {
                    const rowLabel = `${row.label}`;
                    const shortLabel = rowLabel.length > 38 ? `${rowLabel.slice(0, 35)}...` : rowLabel;
                    lines.push(`
                        <text x="18" y="${y + rowHeight - 5}" class="row-label">${escapeText(shortLabel)}</text>
                    `);
                    if (row.dispatch) {
                        const dispatch = row.dispatch;
                        const startX = x(dispatch.startedAtMs);
                        const endX = x(dispatch.finishedAtMs);
                        const width = Math.max(2, endX - startX);
                        const laneY = y + Math.max(3, Math.floor((rowHeight - barHeight) / 2));
                        const rate = dispatch.durationMs > 0 && dispatch.dispatchedCount > 0
                            ? `${(dispatch.dispatchedCount / (dispatch.durationMs / 1000)).toFixed(2)}/s`
                            : '0/s';
                        lines.push(`
                        <rect class="bar dispatcher" x="${startX}" y="${laneY}" width="${width}" height="${barHeight}">
                            <title>${escapeText(`DISPATCH ${dispatch.id}
queue=${dispatch.queueName}
mode=${dispatch.mode}
state=${dispatch.state}
requested=${dispatch.requestedCount}
dispatched=${dispatch.dispatchedCount}
depth=${dispatch.dispatchDepth}
duration=${formatDuration(dispatch.durationMs)}
rate=${rate}
async=${dispatch.asyncApexJobId}`)}</title>
                        </rect>
                        <text x="${Math.min(endX + 5, chartWidth - 150)}" y="${laneY + barHeight}" class="run-meta">
                            ${escapeText(`${dispatch.dispatchedCount} jobs / ${formatDuration(dispatch.durationMs)} / ${rate}`)}
                        </text>
                    `);
                        for (const job of dispatch.jobs) {
                            const pointX = x(job.initiatedAtMs);
                            lines.push(`
                        <circle class="dispatch-point" cx="${pointX}" cy="${laneY + barHeight / 2}" r="${compact ? 1.4 : 2}">
                            <title>${escapeText(`${job.label}
queue=${job.queueName}
state=${job.state}
handoff=${formatTime(job.initiatedAtMs)}
async=${job.asyncApexJobId}`)}</title>
                        </circle>
                    `);
                        }
                        y += rowHeight;
                        continue;
                    }
                    for (const run of row.runs) {
                        const startX = x(run.startedAtMs);
                        const endX = x(run.finishedAtMs);
                        const width = Math.max(2, endX - startX);
                        const laneY = y + 3 + partitionRank(run.partition) * (barHeight + laneGap);
                        const statusClass =
                            run.status === 'COMPLETED' ? partitionClass(run.partition) :
                            run.status === 'FAILED' ? 'failed' :
                            'active';
                        lines.push(`
                        <rect class="bar ${statusClass}" x="${startX}" y="${laneY}" width="${width}" height="${barHeight}">
                            <title>${escapeText(`${run.partition} ${run.label}
status=${run.status}
attempt=${run.attempt}
duration=${formatDuration(run.durationMs)}
async=${run.asyncApexJobId}`)}</title>
                        </rect>
                        <text x="${Math.min(endX + 5, chartWidth - 92)}" y="${laneY + barHeight}" class="run-meta">
                            ${escapeText(formatDuration(run.durationMs))}
                        </text>
                    `);
                    }
                    y += rowHeight;
                }
                y += sectionGap;
            }

            svg.innerHTML = lines.join('');
        }

        renderChart();
        document.getElementById('compactToggle').addEventListener('change', renderChart);
        document.getElementById('showDispatchToggle').addEventListener('change', renderChart);
        document.getElementById('showWorkerToggle').addEventListener('change', renderChart);
        document.getElementById('showParallelWorkerToggle').addEventListener('change', renderChart);
        document.getElementById('showQueueableSerialToggle').addEventListener('change', renderChart);
        document.getElementById('showQueueableConcurrentToggle').addEventListener('change', renderChart);
        document.getElementById('showInvocableToggle').addEventListener('change', renderChart);
        window.addEventListener('resize', renderChart);
    </script>
</body>
</html>
HTML_TAIL
} > "$REPORT_PATH"

echo
echo "Performance span report written to $REPORT_PATH"
echo "Raw query payload written to $RAW_JSON_PATH"
echo "Raw dispatch payload written to $DISPATCH_JSON_PATH"
echo "Raw dispatch job payload written to $DISPATCH_JOB_JSON_PATH"
