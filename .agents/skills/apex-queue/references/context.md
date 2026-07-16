# Queue Framework Context

Companion to `invariants.md`. If the two disagree, the invariants
file wins.

## Purpose

This file records the current package shape, known platform observations, test
entrypoints, and next work seams for the Apex Queue framework. It should stay
compact enough to read before changing queue/runtime behavior.

## Glossary

- `queue`: named logical stream of work plus metadata defaults.
- `job`: durable work item stored in `Job__c`.
- `processor`: user Apex implementation that performs business work.
- `runtime`: per-queue coordination row in `QueueRuntime__c`.
- `worker`: `BatchableWorker` batch job for `WORKER` jobs.
- `queueable dispatch`: ephemeral `QueueableDispatch__c` envelope for
  `QUEUEABLE_SERIAL` / `QUEUEABLE_CONCURRENT` jobs from one enqueue request.
- `invocable handoff`: Flow Scheduled Path transport for `INVOCABLE` jobs.
- `scheduler`: `JobScheduler__c` plus native Scheduled Apex wake artifacts that
  materialize future `Job__c` rows.
- `job run`: one execution attempt in `JobRun__c`.
- `queue error`: framework-operational telemetry in `QueueError__c`, usually
  persisted through `QueueErrorEvent__e`.
- `flow`: atomically inserted `FlowProducer` dependency graph whose parents
  wait for child resolution.

## Package Map

- Public facade/value objects: `Queues.cls`.
- Thin orchestrator: `QueuesService.cls`.
- Repositories own SOQL/DML only:
  `QueuesDefinitionRepository`, `QueuesJobRepository`,
  `QueuesJobRunRepository`, `QueuesProcessorRepository`,
  `QueuesQueueableDispatchRepository`, `QueuesRuntimeRepository`,
  `QueuesScheduleRepository`, `QueuesAsyncApexJobRepository`,
  `QueuesErrorRepository`, `QueuesErrorEventRepository`.
- Business rules live in granular services:
  definition, job, job admin, job execution, job run, processor, queue admin,
  runtime, stats, worker, queueable transport/dispatch/execution, invocable
  execution, maintenance, stalled recovery, scheduler.
- `workers/BatchableWorker.cls` is the only worker implementation for
  `WORKER` jobs.
- `schedulers/` is reserved for concrete `implements Schedulable` classes.
  Scheduling helpers/facades live under `scheduling/`.

## Durable State

- `QueueDefinition__mdt`: queue defaults and claim settings.
- `Processor__mdt`: processor binding metadata.
- `QueueRuntime__c`: worker ownership, pause state, wake plan, recovery state.
- `Job__c`: durable work item. `AvailableAt__c` is also the invocable Flow wake
  timestamp. `CANCELED` is a terminal observability state for abandoned future
  scheduler occurrences.
- `Job__c.QueueScopedExternalId__c`: internal queue-scoped unique key for
  race-safe idempotent enqueue.
- `QueueableDispatch__c`: queueable handoff envelope only; not a runtime.
  `DedupeKey__c` is optional async handoff metadata, not worker bulk
  idempotency.
- `JobScheduler__c`: scheduler cadence/template state. Scheduler templates
  persist regular `JobOptions`; execution mode is an explicit
  `Queues.ExecutionMode` upsert argument stored in `ExecutionMode__c`.
- `JobRun__c`: execution attempt log owned by `Job__c` through master-detail.
- `QueueError__c` and `QueueErrorEvent__e`: durable operational telemetry.
- `Queues_Invocable_Claim` / `Queues_Invocable_Execute`: Flow transport
  primitives, not durable state tables.

## Execution Modes

- `WORKER`: default. Durable batch-worker lane for normal background work, long
  delays, queue admin ownership, maintenance recovery, and uncapped retry
  delays.
- `QUEUEABLE_SERIAL`: isolated queueable lane. Dispatcher submits one executor
  per durable job in framework order, but Salesforce may start/finish executors
  out of order after submission.
- `QUEUEABLE_CONCURRENT`: explicit queueable future fanout handoff. Use only
  when measured and desired; keep retry/finalizer durability intact.
- `INVOCABLE`: Flow Scheduled Path lane. Useful when processors need measured
  `INVOCABLE_ACTION` traits while the framework still owns attempts, retry,
  backoff, and stale wake guards.

Processor async budgets by lane:

- `WORKER`: runs inside `batch.execute()`. No future calls or starting/scheduling
  another batch. One queueable enqueue may be used for a single follow-up async
  handoff.
- Queueable modes: run inside framework queueable executor. Processor code must
  not enqueue another queueable because the framework owns the queueable chain;
  future calls and scheduled/batch handoffs remain available subject to platform
  limits.
- `INVOCABLE`: observed quiddity is `INVOCABLE_ACTION`; probes observed 50
  queueable enqueues, 50 futures, 150 DML statements, 10k DML rows, 100 callouts,
  200 SOQL queries, 60s CPU, and 12 MB heap. Treat as measured behavior, not as
  classic synchronous Apex.

Priority controls claim/dispatch order only. It does not guarantee processor
start or finish order after Salesforce async scheduling accepts the work.

## Implemented Behavior

### Enqueue

- `Queues.BulkJobOptions` owns bulk execution mode and optional queueable
  dispatch dedupe. `Queues.JobOptions` remains per-job only.
- Enqueue is execution-mode homogeneous per bulk request.
- Reusing a `JobOptions.jobId(...)` that maps to an existing job in a different
  execution mode is rejected.
- Caller job ids are capped at 174 chars so the internal queue-scoped key fits
  the 255-char limit.
- Duplicate insert races are reconciled by rereading the winning row
  case-insensitively.

### Worker

- Worker batch scope is fixed at 1: one durable job per processor transaction.
- `BatchClaimSize__c` controls how many due jobs `batch.start()` claims; it is
  not a parallelism knob and should not be capped by lease minutes.
- Worker scopes behaved serially in measured org probes. Plan leases and
  throughput accordingly.
- `batch.start()` may update durable runtime ownership and adopt the real
  batch `AsyncApexJobId`; `batch.finish()` owns continuation.
- Catastrophic `batch.execute()` failures are handled by
  `BatchApexErrorEvent` using `EXECUTE` events and `JobScope`.

### Queueable

- Enqueue creates a `QueueableDispatch__c`; dispatchers enqueue executors;
  executors process one durable job.
- Dispatcher finalizers continue dispatch stacks. Executor finalizers recover
  catastrophic processor failures.
- Dispatcher dedupe uses Salesforce `DuplicateSignature` for the initial async
  artifact. It uses readable `queueName:dedupeKey` when it fits 32 bytes,
  otherwise deterministic `q:` + truncated SHA-256. Original key remains on
  `QueueableDispatch__c.DedupeKey__c`.
- Once a dispatcher submits a job, it stamps
  `Job__c.QueueableDispatchInitiatedAt__c`; retry jobs must not be picked up by
  dispatch again.
- User-provided queueable initial delay must fit native `0..10` minutes and
  fails fast. Framework retry delay saturates to that native cap.
- Queueable stack-depth probes are guarded by quiddity before touching
  `System.AsyncInfo`; native retry enqueue `System.AsyncException` is treated
  as bridgeable through future.

### Invocable

- Uses two flows:
  `Queues_Invocable_Claim` for due `WAITING` / `DELAYED` jobs and
  `Queues_Invocable_Execute` for `ACTIVE` jobs.
- Claim Scheduled Path uses `maxBatchSize=100` to preserve DML headroom.
  Execute Scheduled Path uses `maxBatchSize=1` to preserve one processor
  transaction per durable job.
- Claim Apex bulk-locks, validates, and updates the complete Flow input batch;
  it must not issue per-job queries.
- Claim and execute must remain separate transactions. Single-flow probes failed
  the isolation and/or single-job execution invariants.
- Wake source is `Job__c.AvailableAt__c + 0 minutes`. Precision is scheduler
  tick-like, usually next minute boundary.
- Stale Flow resumes are harmless because Apex rereads and validates state,
  due time, mode, and lease ownership.
- Maintenance repairs lost pre-claim Scheduled Path wakes after the queue lease
  grace by moving stale due jobs to a one-minute future `AvailableAt__c`.
- Expired `ACTIVE` leases are recovered by maintenance.

### Scheduler

- Scheduler materializes jobs only. It never executes processors and does not
  replace any transport lane.
- `pattern(...)`: native recurring Scheduled Apex.
- `every(N)`: one-shot self-rescheduling Scheduled Apex.
- Both variants share the same materialization body and use `JobScheduler__c`
  as source of truth.
- Scheduler jobs use framework-owned ids:
  `scheduler:<JobScheduler__c.Id>:<iteration>`.
- Re-upsert keeps the same `JobScheduler__c.Id`, cancels old pending jobs,
  aborts old matching native schedules, advances the monotonic iteration, and
  materializes from the new repeat/template/mode state.
- Removing a scheduler aborts native wakes and deletes the scheduler plus
  linked jobs. Canceling preserves the scheduler and marks pending occurrences
  `CANCELED`.
- `lim(...)` caps lifetime materialized iterations for the durable scheduler
  identity; re-upsert does not reset the counter.
- Scheduler execution mode is explicit on upsert and persisted on
  `JobScheduler__c.ExecutionMode__c`. `WORKER` occurrences wake the worker,
  queueable occurrences dispatch when due, and `INVOCABLE` occurrences rely on
  Flow Scheduled Path from `Job__c.AvailableAt__c`.
- `runJobSchedulerMaintenance()` is repair-only. It bulk-loads active
  schedulers, queue definitions, and native schedule metadata before per-row
  repair decisions.

### FlowProducer

- Inserts dependency graphs atomically in parent-first waves.
- Graph edges are defined with `FlowJob.child(...)`; direct
  `JobOptions.parent(...)` remains provenance-only and is rejected inside a
  submitted flow.
- Existing flow jobs are replay-only. A submission that adds a newly inserted
  child beneath an existing node is rejected and rolled back atomically.
- Parent release is lock-safe when invoked. Destructive removal and queue-level
  cancel/drain paths invoke the same dependency resolver.
- Multi-level propagation is bounded per transaction and continues through a
  queueable resolver. Near an inherited queueable stack ceiling, resolver
  submission bridges through future for a fresh native stack budget.
- Worker runtimes lock before worker-mode parent rows. Cross-queue levels that
  need another runtime lock continue in a fresh resolver transaction.
- Maintenance selectively redrives stale `WAITING_CHILDREN` parents by queue
  and lease age. `DependencyRepairCheckedAt__c` provides fair repair rotation.
- Failed `REMOVE_DEPENDENCY` edges resolve parents immediately. Sparse, bounded
  physical edge cleanup continues durably and has a stale-edge maintenance
  backstop.

## Operational Rules

- Preserve one singular live worker plan per queue runtime.
- `Job__c` and `QueueRuntime__c` are the durable source of truth; async artifacts
  are wake or handoff hints.
- Runtime-first lock discipline applies to durable queue/job admin mutations.
- Processor side effects must be externally idempotent. Use
  `JobContext.idempotencyKey()` as the stable logical-job key.
- Durable queue-error telemetry uses publish-immediate events from outer
  framework seams; do not add telemetry DML to clean processor transactions.
- Maintenance/admin loops must keep CPU, query, DML row, and DML statement
  headroom.

## Tests And Commands

- Unit tests: `sfdx-source/apex-queue/test/unit/**/*Test.cls`.
- Integration tests: `sfdx-source/apex-queue/test/integration/**/*Test.cls`.
- Shared test support: `sfdx-source/apex-queue/test/utils`.
- E2E deployable metadata: `e2e/main`.
- E2E anonymous Apex scripts: `e2e/test`.
- E2E runners:
  `e2e/run_job_scheduler.sh`, `e2e/run_queueable.sh`,
  `e2e/run_queueable_concurrent.sh`, `e2e/run_invocable.sh`, etc.
- Job scheduler e2e covers: cron materialization, granular self-reschedule,
  removal, re-upsert replacement, execution-mode re-upsert,
  `lim(5)`, queueable concurrent dispatch, queueable serial scheduling,
  invocable scheduling, and cleanup.
- Performance e2e is opt-in under `e2e/test/performance` and is not part of
  `e2e/run_all.sh`.

Recent validation:

- May 27, 2026: Apex deploy with specified tests passed; coverage run passed
  400/400 tests, 100% test-run coverage, and no `sfdx-source/apex-queue`
  source classes below 100%.
- May 27, 2026: `./e2e/run_job_scheduler.sh` passed through Git Bash.
- May 9, 2026: `bash e2e/run_invocable.sh` passed; later `RunLocalTests`
  passed 353/353 with 86% org-wide coverage and 100% test-run coverage.

## Next Work

- Dependent-job semantics should build on the stabilized job/admin/scheduler
  surfaces, not invent another runtime.
- Flow work should depend on existing invocable/scheduler/admin behavior.
- Scheduler e2e should remain the guardrail for scheduler-backed changes.
- Delete provably unreachable defensive code instead of covering impossible
  branches.
