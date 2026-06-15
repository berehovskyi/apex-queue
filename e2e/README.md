# Queue E2E

This folder contains deployable end-to-end queue assets and anonymous Apex
scripts for real org-level smoke flows.

## Layout

- `e2e/main`
  Deployable metadata for the e2e queue, processor, remote-site setting, and
  weather callout processor classes.
- `e2e/test`
  Idempotent anonymous Apex scripts grouped by feature area.
- `e2e/test/queue_admin`
  Queue-admin scripts for `add`, `addBulk`, `pause`, `resume`, `drain`, and
  `wake`.
- `e2e/test/job`
  Job-focused scripts for `updateData`/`updateProgress`, idempotent `jobId`, delayed jobs,
  `defer`/`promote`, priority ordering, `remove`, retry exhaustion/reset with
  stable idempotency-key proof across attempts, and unrecoverable failures.
- `e2e/test/job_scheduler`
  Job-scheduler scripts for cron-based schedulers, granular self-rescheduling
  schedulers, and scheduler removal with pending job cleanup.
- `e2e/test/worker`
  Worker-focused scripts for processor execution, callouts, delayed worker
  scheduling, durable completion state, processor-handled failures, and
  catastrophic batch-scope failures.
- `e2e/test/queueable`
  Queueable serial transport scripts for `BulkJobOptions.queueable()` enqueue,
  queueable dispatch,
  executor completion, retry exhaustion, delayed dispatch isolation, retry
  dispatch-capture prevention, catastrophic executor finalizer recovery, and
  native delay validation.
- `e2e/test/queueable_concurrent`
  Queueable concurrent transport scripts for `BulkJobOptions.queueableConcurrent()`
  enqueue and the same executor completion, retry exhaustion, delayed dispatch
  isolation, retry dispatch-capture prevention, and catastrophic executor
  finalizer recovery paths. Pure validation fast-fail coverage stays in the
  serial queueable suite.
- `e2e/test/invocable`
  Invocable transport scripts for `BulkJobOptions.invocable()` enqueue, Flow
  Scheduled Path handoff, reschedule-by-`AvailableAt__c`, stale wake guards,
  no-longer-due guards, and expired active lease recovery through maintenance.
- `e2e/test/performance`
  Opt-in load scripts for worker and queueable transport. These are not wired
  into `run_all.sh`.

## What It Covers

- Real `Queues.of(...).add(...)` enqueue flow
- Real `Queues.of(...).upsertJobScheduler(...)` schedulable materialization flow
- Real `BatchableWorker` async execution path
- Real queueable dispatcher/executor async execution path
- Real queueable executor finalizer failure path
- Real invocable Flow Scheduled Path execution path
- Expired active INVOCABLE lease recovery through independent maintenance
- Real outbound processor callouts to the Open-Meteo forecast API
- Durable completion state and `JobRun__c` assertions
- Negative unsupported-city processor failure path without framework
  `QueueError__c` noise

## Deploy

Deploy the e2e metadata before running the scripts:

```powershell
sf project deploy start --source-dir e2e/main --test-level RunLocalTests
```

## Run

Recommended sequence:

```powershell
sf apex run --file e2e/test/worker/00_cleanup.apex
sf apex run --file e2e/test/worker/10_enqueue_weather_smoke.apex
Start-Sleep -Seconds 20
sf apex run --file e2e/test/worker/20_assert_weather_smoke.apex
sf apex run --file e2e/test/worker/30_enqueue_unsupported_city.apex
Start-Sleep -Seconds 20
sf apex run --file e2e/test/worker/40_assert_unsupported_city.apex
```

If an assertion script reports unfinished jobs, wait a few more seconds and run
that assertion script again. The suite stays idempotent because the cleanup
step removes prior queue state, and the enqueue steps use stable job ids.

Queue feature scripts are self-contained and can be run independently:

```powershell
sf apex run --file e2e/test/queue_admin/00_cleanup.apex
sf apex run --file e2e/test/queue_admin/10_should_add_job.apex
sf apex run --file e2e/test/queue_admin/20_should_add_bulk_jobs_all_or_none.apex
sf apex run --file e2e/test/queue_admin/30_should_pause_and_resume_queue.apex
sf apex run --file e2e/test/queue_admin/40_should_drain_queue.apex
sf apex run --file e2e/test/queue_admin/50_should_wake_queue.apex
```

Job feature scripts are split by synchronous vs async behavior:

```powershell
sf apex run --file e2e/test/job/00_cleanup.apex
sf apex run --file e2e/test/job/10_should_add_job_and_update_data.apex
sf apex run --file e2e/test/job/20_should_reuse_existing_job_when_job_id_matches.apex
sf apex run --file e2e/test/job/30_should_add_delayed_job.apex
sf apex run --file e2e/test/job/40_should_defer_and_promote_job.apex
sf apex run --file e2e/test/job/60_should_remove_job.apex

sf apex run --file e2e/test/job/50_should_enqueue_jobs_for_priority_order.apex
Start-Sleep -Seconds 15
sf apex run --file e2e/test/job/51_should_assert_higher_priority_job_executed_first.apex

sf apex run --file e2e/test/job/70_should_enqueue_retryable_job.apex
Start-Sleep -Seconds 20
sf apex run --file e2e/test/job/71_should_assert_retry_exhaustion_and_manual_retry_reset.apex

sf apex run --file e2e/test/job/80_should_enqueue_unrecoverable_job.apex
Start-Sleep -Seconds 15
sf apex run --file e2e/test/job/81_should_assert_unrecoverable_job_does_not_retry.apex
```

Job scheduler scripts cover both scheduler variants. The assertion scripts are
designed to be retried because native Scheduled Apex can fire with platform
timing jitter:

```powershell
sf apex run --file e2e/test/job_scheduler/00_cleanup.apex
sf apex run --file e2e/test/job_scheduler/10_should_schedule_regular_cron_scheduler.apex
Start-Sleep -Seconds 100
sf apex run --file e2e/test/job_scheduler/11_should_assert_regular_cron_scheduler_materialized.apex

sf apex run --file e2e/test/job_scheduler/00_cleanup.apex
sf apex run --file e2e/test/job_scheduler/20_should_schedule_granular_scheduler.apex
Start-Sleep -Seconds 100
sf apex run --file e2e/test/job_scheduler/21_should_assert_granular_scheduler_self_rescheduled.apex

sf apex run --file e2e/test/job_scheduler/00_cleanup.apex
sf apex run --file e2e/test/job_scheduler/30_should_remove_scheduler_and_jobs.apex
sf apex run --file e2e/test/job_scheduler/00_cleanup.apex

sf apex run --file e2e/test/job_scheduler/31_should_cancel_scheduler_and_pending_jobs.apex
sf apex run --file e2e/test/job_scheduler/00_cleanup.apex

sf apex run --file e2e/test/job_scheduler/35_should_replace_scheduler_on_reupsert.apex
sf apex run --file e2e/test/job_scheduler/00_cleanup.apex

sf apex run --file e2e/test/job_scheduler/40_should_schedule_granular_limit_five.apex
sf apex run --file e2e/test/job_scheduler/41_should_materialize_granular_limit_next.apex
sf apex run --file e2e/test/job_scheduler/41_should_materialize_granular_limit_next.apex
sf apex run --file e2e/test/job_scheduler/41_should_materialize_granular_limit_next.apex
sf apex run --file e2e/test/job_scheduler/41_should_materialize_granular_limit_next.apex
sf apex run --file e2e/test/job_scheduler/42_should_stop_granular_limit_at_five.apex
sf apex run --file e2e/test/job_scheduler/00_cleanup.apex
```

Worker scripts cover real processor execution, delayed scheduling, and
catastrophic batch failure handling:

```powershell
sf apex run --file e2e/test/worker/00_cleanup.apex
sf apex run --file e2e/test/worker/10_enqueue_weather_smoke.apex
Start-Sleep -Seconds 20
sf apex run --file e2e/test/worker/20_assert_weather_smoke.apex
sf apex run --file e2e/test/worker/30_enqueue_unsupported_city.apex
Start-Sleep -Seconds 20
sf apex run --file e2e/test/worker/40_assert_unsupported_city.apex

sf apex run --file e2e/test/worker/00_cleanup.apex
sf apex run --file e2e/test/worker/50_should_keep_existing_worker_when_later_delayed_job_is_added.apex

sf apex run --file e2e/test/worker/00_cleanup.apex
sf apex run --file e2e/test/worker/60_should_enqueue_catastrophic_batch_failure.apex
Start-Sleep -Seconds 20
sf apex run --file e2e/test/worker/61_should_assert_catastrophic_batch_failure_is_recorded.apex
sf apex run --file e2e/test/worker/00_cleanup.apex

sf apex run --file e2e/test/worker/70_should_enqueue_retryable_worker_job_with_ten_attempts.apex
Start-Sleep -Seconds 60
sf apex run --file e2e/test/worker/71_should_assert_retryable_worker_job_with_ten_attempts_failed.apex
```

Queueable serial scripts cover the isolated queueable transport path:

```powershell
sf apex run --file e2e/test/queueable/00_cleanup.apex
sf apex run --file e2e/test/queueable/10_should_enqueue_queueable_smoke.apex
Start-Sleep -Seconds 20
sf apex run --file e2e/test/queueable/20_should_assert_queueable_smoke_completed.apex
sf apex run --file e2e/test/queueable/30_should_enqueue_retryable_queueable_job.apex
Start-Sleep -Seconds 20
sf apex run --file e2e/test/queueable/31_should_assert_retryable_queueable_job_failed.apex
sf apex run --file e2e/test/queueable/70_should_enqueue_retryable_queueable_job_with_ten_attempts.apex
Start-Sleep -Seconds 60
sf apex run --file e2e/test/queueable/71_should_assert_retryable_queueable_job_with_ten_attempts_failed.apex

sf apex run --file e2e/test/queueable/00_cleanup.apex
sf apex run --file e2e/test/queueable/50_should_keep_delayed_dispatches_isolated_when_later_delayed_job_is_added.apex
sf apex run --file e2e/test/queueable/00_cleanup.apex

sf apex run --file e2e/test/queueable/52_should_enqueue_immediate_queueable_without_canceling_delayed_queueable.apex
Start-Sleep -Seconds 20
sf apex run --file e2e/test/queueable/53_should_assert_immediate_queueable_completed_and_delayed_queueable_waits.apex
sf apex run --file e2e/test/queueable/00_cleanup.apex

sf apex run --file e2e/test/queueable/55_should_skip_already_initiated_retry_candidate_when_dispatching_remaining_job.apex
sf apex run --file e2e/test/queueable/00_cleanup.apex

sf apex run --file e2e/test/queueable/60_should_enqueue_catastrophic_queueable_failure.apex
Start-Sleep -Seconds 20
sf apex run --file e2e/test/queueable/61_should_assert_catastrophic_queueable_failure_is_recorded.apex

sf apex run --file e2e/test/queueable/40_should_reject_invalid_queueable_delay.apex
```

Queueable concurrent scripts mirror the queueable execution paths while using
`BulkJobOptions.queueableConcurrent()` and short 5-second waits:

```powershell
sf apex run --file e2e/test/queueable_concurrent/00_cleanup.apex
sf apex run --file e2e/test/queueable_concurrent/10_should_enqueue_queueable_smoke.apex
Start-Sleep -Seconds 5
sf apex run --file e2e/test/queueable_concurrent/20_should_assert_queueable_smoke_completed.apex
sf apex run --file e2e/test/queueable_concurrent/30_should_enqueue_retryable_queueable_job.apex
Start-Sleep -Seconds 5
sf apex run --file e2e/test/queueable_concurrent/31_should_assert_retryable_queueable_job_failed.apex
sf apex run --file e2e/test/queueable_concurrent/70_should_enqueue_retryable_queueable_job_with_ten_attempts.apex
Start-Sleep -Seconds 5
sf apex run --file e2e/test/queueable_concurrent/71_should_assert_retryable_queueable_job_with_ten_attempts_failed.apex

sf apex run --file e2e/test/queueable_concurrent/00_cleanup.apex
sf apex run --file e2e/test/queueable_concurrent/50_should_keep_delayed_dispatches_isolated_when_later_delayed_job_is_added.apex
sf apex run --file e2e/test/queueable_concurrent/00_cleanup.apex

sf apex run --file e2e/test/queueable_concurrent/52_should_enqueue_immediate_queueable_without_canceling_delayed_queueable.apex
Start-Sleep -Seconds 5
sf apex run --file e2e/test/queueable_concurrent/53_should_assert_immediate_queueable_completed_and_delayed_queueable_waits.apex
sf apex run --file e2e/test/queueable_concurrent/00_cleanup.apex

sf apex run --file e2e/test/queueable_concurrent/55_should_skip_already_initiated_retry_candidate_when_dispatching_remaining_job.apex
sf apex run --file e2e/test/queueable_concurrent/00_cleanup.apex

sf apex run --file e2e/test/queueable_concurrent/60_should_enqueue_catastrophic_queueable_failure.apex
Start-Sleep -Seconds 5
sf apex run --file e2e/test/queueable_concurrent/61_should_assert_catastrophic_queueable_failure_is_recorded.apex
```

Invocable scripts cover the Flow Scheduled Path transport and its recovery
boundary:

```powershell
sf apex run --file e2e/test/invocable/00_cleanup.apex
sf apex run --file e2e/test/invocable/10_should_enqueue_invocable_smoke.apex
Start-Sleep -Seconds 20
sf apex run --file e2e/test/invocable/20_should_assert_invocable_smoke_completed.apex

sf apex run --file e2e/test/invocable/30_should_enqueue_retryable_invocable_job.apex
Start-Sleep -Seconds 300
sf apex run --file e2e/test/invocable/31_should_assert_retryable_invocable_job_failed.apex

sf apex run --file e2e/test/invocable/00_cleanup.apex
sf apex run --file e2e/test/invocable/50_should_reschedule_invocable_job_by_available_at.apex
Start-Sleep -Seconds 150
sf apex run --file e2e/test/invocable/51_should_assert_rescheduled_invocable_job_completed.apex

sf apex run --file e2e/test/invocable/00_cleanup.apex
sf apex run --file e2e/test/invocable/60_should_enqueue_invocable_stale_wake_guards.apex
sf apex run --file e2e/test/invocable/61_should_mutate_invocable_stale_wake_guards.apex
Start-Sleep -Seconds 150
sf apex run --file e2e/test/invocable/62_should_assert_invocable_stale_wake_guards.apex

sf apex run --file e2e/test/invocable/00_cleanup.apex
sf apex run --file e2e/test/invocable/70_should_recover_expired_active_invocable_lease.apex
Start-Sleep -Seconds 150
sf apex run --file e2e/test/invocable/71_should_assert_recovered_invocable_lease_completed.apex
sf apex run --file e2e/test/invocable/00_cleanup.apex
```

This slice mirrors the other transport suites with smoke and retry coverage,
then adds INVOCABLE-specific proofs: reschedule-by-`AvailableAt__c`, stale wake
guards, no-longer-due guards, and independent maintenance recovery of an
already-durable expired `ACTIVE` INVOCABLE lease. It does not prove recovery
from a lost or suppressed Scheduled Path wake for a `WAITING` or `DELAYED` job.

The production INVOCABLE shape uses two active record-triggered Scheduled Path
flows. `Queues_Invocable_Claim` wakes due `WAITING` / `DELAYED` jobs with
`maxBatchSize=100`; `Queues_Invocable_Execute` wakes `ACTIVE` jobs with
`maxBatchSize=1`. Claim may batch, but execute must stay single-input to
preserve one processor transaction per durable job.

Prefer `bash e2e/run_invocable.sh` for the full slice because it retries strict
assertion scripts across Flow Scheduled Path timing jitter.

The worker/queueable-style retry exhaustion parity scripts
exist for INVOCABLE too, but they are long-running because each retry attempt
requires separate Scheduled Path claim and execute wakes:

```bash
RUN_INVOCABLE_MAX_RETRY=1 bash e2e/run_invocable.sh
```

To run every queue-admin, job, worker, queueable, and invocable script in one
shot from Git Bash or WSL:

```bash
bash e2e/run_all.sh
```

You can also run each slice independently:

```bash
bash e2e/run_queue_admin.sh
bash e2e/run_job.sh
./e2e/run_job_scheduler.sh
bash e2e/run_worker.sh
bash e2e/run_queueable.sh
bash e2e/run_queueable_concurrent.sh
bash e2e/run_invocable.sh
```

Performance scripts are intentionally opt-in and are not included in
`run_all.sh`:

```bash
bash e2e/run_performance.sh
```

The performance runner queries raw `JobRun__c`, `QueueableDispatch__c`, and
queueable handoff timestamp data before cleanup and writes a standalone
JavaScript/SVG report to `e2e/.artifacts/performance`.
It runs queueable serial, queueable concurrent, invocable, and worker loads as separate
phases, waiting for each phase to resolve before enqueueing the next one so
the lanes do not compete for async admission during the same measurement
phase.
The report partitions worker, `QUEUEABLE_SERIAL`, `QUEUEABLE_CONCURRENT`, and `INVOCABLE`
runs so concurrency can be compared. For performance jobs, the report prefers
exact `Job__r.Progress__c.startedAtMs` plus `JobRun__c.DurationMs__c` over
coarse `DateTime` rendering. Each report row aligns the next worker,
queueable serial, queueable concurrent, and invocable span after each lane is sorted by
start time; rows are ordered by the earliest start time in each aligned set.
The report also includes a dispatcher handoff section that renders each
`QueueableDispatch__c.StartedAt__c` -> `FinishedAt__c` span and overlays per-job
`Job__c.QueueableDispatchInitiatedAt__c` markers, making dispatcher enqueue
cadence visible separately from executor start/finish cadence.
The report toolbar can hide dispatch, worker, queueable serial, queueable
concurrent, or invocable spans independently; the timeline rescales to the
currently visible groups.

While the performance jobs are settling, the runner also polls state aggregates
every second and renders separate progress bars by `ExecutionMode__c`.
The bar moves left to right: completed segments are green, active segments are
yellow, failed segments are red, and waiting/delayed/paused segments are plain:

```soql
SELECT ExecutionMode__c, State__c, COUNT(Id)
FROM Job__c
WHERE QueueName__c IN (
    'e2e-performance-worker',
    'e2e-performance-queueable',
    'e2e-performance-queueable-concurrent',
    'e2e-performance-invocable'
)
GROUP BY ExecutionMode__c, State__c
ORDER BY ExecutionMode__c
```

By default, the progress bars update in place instead of stacking log lines.
Set `JOB_STATE_PROGRESS_REPLACE=false` to keep every poll snapshot in the
terminal log. The runner waits for the current phase's jobs to become
`COMPLETED` or `FAILED` instead of sleeping for a fixed duration. Set
`PERFORMANCE_WAIT_TIMEOUT_SECONDS` to override the default 3600 second cap.

After completion, the runner also prints total wall duration by execution mode:

```soql
SELECT
    StartedAt__c,
    FinishedAt__c,
    Job__r.ExecutionMode__c,
    Job__r.QueueName__c
FROM JobRun__c
WHERE Job__r.QueueName__c IN (
    'e2e-performance-worker',
    'e2e-performance-queueable',
    'e2e-performance-queueable-concurrent',
    'e2e-performance-invocable'
)
AND StartedAt__c != NULL
AND FinishedAt__c != NULL
```

The shell aggregates those rows locally. This avoids Salesforce CLI aggregate
CSV flattening quirks where `GROUP BY Job__r.ExecutionMode__c` can display a
blank mode even though the grouped rows are distinct.

To regenerate only the report while performance rows still exist:

```bash
bash e2e/run_performance_report.sh
```

For raw Salesforce async primitive comparison, use the separate raw performance
runner. It does not use `Job__c` processing and writes timing rows to the
e2e-only `RawPerformanceRun__c` object:

```bash
bash e2e/run_performance_raw.sh
```

The raw suite compares six variants with the same artificial processor delay:
batch iterator, batch query cursor, chained serial batch, serial queueable, and
queueable fanout via future-launched executors, plus Flow Scheduled Path
invocable action. The runner executes one variant to completion before starting
the next so async scheduler interference is kept out of the comparison. It
records requested time, actual start/finish, start delay, duration, and async
job id per row, then prints an Apex-side summary so the shell runner does not
need field-level access to probe-only timing columns.

If your shell does not pick up the default Salesforce org automatically, pass
it explicitly:

```bash
SF_TARGET_ORG=test-nqentdxtu8fx@example.com bash e2e/run_worker.sh
```
