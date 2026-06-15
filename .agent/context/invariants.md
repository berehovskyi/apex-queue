# Framework Invariants

Architectural rules for Apex Queue. If a change would violate this file, stop
and update the invariant deliberately before changing implementation.

## Priorities

- Preserve one singular live worker plan per `QueueRuntime__c`.
- Preserve framework durability through `Job__c`, `QueueRuntime__c`, and other
  durable framework state, not async artifact visibility.
- Treat Scheduled Apex, Flow interviews, queueable jobs, and batch jobs as wake
  or handoff artifacts unless explicitly stated otherwise.
- Keep caller idempotency race-safe at the database boundary.
- Preserve one processor execution transaction per durable job.
- Prefer deleting impossible branches over adding artificial coverage.

## Runtime Boundaries

- `WORKER`, `QUEUEABLE_SERIAL`, `QUEUEABLE_CONCURRENT`, and `INVOCABLE` are
  distinct execution lanes.
- `BatchableWorker` is the only framework worker for `WORKER` jobs.
- Queueable lanes may use `QueueableDispatch__c`; they must not share or mutate
  batch-worker `QueueRuntime__c` ownership.
- `INVOCABLE` must not use `QueueRuntime__c` ownership or
  `QueueableDispatch__c`.
- Do not introduce another durable runtime object casually. If a new runtime
  model is needed, document it here first.
- Priority means framework claim/dispatch order, not guaranteed processor start
  or finish order after Salesforce async scheduling.

### Matrix

Legend:

- <span style="color: red">red</span> = transaction limit for future / queueable
  depth
- <span style="color: blue">blue</span> = org flex queue capacity
- <span style="color: teal">teal</span> = org scheduled-job capacity
- <span style="color: purple">purple</span> = nested async context restrictions

| Caller context       | CPU left (ms) | SOQL left | Heap left (Mb) | `callFuture`                                                                                                                        | `executeBatch`                                                                                                                                                    | `scheduleBatch`                                                                                                                                                    | `schedule`                                                                                                                                                       | `enqueueJob`                                                                                                        |
| -------------------- | ------------: | --------: | -------------: | ----------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `sync / schedulable` |         10000 |       100 |              6 | 50 allowed<br><span style="color: red">System.LimitException: Too many future calls: 51</span>                                      | 100 allowed<br><span style="color: blue">System.AsyncException: You've exceeded the limit of 100 jobs in the flex queue for org.</span>                           | 100 allowed<br><span style="color: teal">System.AsyncException: You have exceeded the maximum number (100) of Apex scheduled jobs.</span>                          | 100 allowed<br><span style="color: teal">System.AsyncException: You have exceeded the maximum number (100) of Apex scheduled jobs.</span>                        | 50 allowed<br><span style="color: red">System.LimitException: Too many queueable jobs added to the queue: 51</span> |
| `future`             |         60000 |       200 |             12 | 0 allowed<br><span style="color: purple">System.AsyncException: Future method cannot be called from a future or batch method</span> | 0 allowed<br><span style="color: purple">System.AsyncException: Database.executeBatch cannot be called from a batch start, batch execute, or future method</span> | 0 allowed<br><span style="color: purple">System.AsyncException: Database.scheduleBatch cannot be called from a batch start, batch execute, or future method</span> | 100 allowed<br><span style="color: teal">System.AsyncException: You have exceeded the maximum number (100) of Apex scheduled jobs.</span>                        | 1 allowed<br><span style="color: red">System.LimitException: Too many queueable jobs added to the queue: 2</span>   |
| `batch-start`        |         60000 |       200 |             12 | 0 allowed<br><span style="color: purple">System.AsyncException: Future method cannot be called from a future or batch method</span> | 0 allowed<br><span style="color: purple">System.AsyncException: Database.executeBatch cannot be called from a batch start, batch execute, or future method</span> | 0 allowed<br><span style="color: purple">System.AsyncException: Database.scheduleBatch cannot be called from a batch start, batch execute, or future method</span> | 100 allowed<br><span style="color: teal">System.AsyncException: You have exceeded the maximum number (100) of Apex scheduled jobs.</span>                        | 1 allowed<br><span style="color: red">System.LimitException: Too many queueable jobs added to the queue: 2</span>   |
| `batch-execute`      |         60000 |       200 |             12 | 0 allowed<br><span style="color: purple">System.AsyncException: Future method cannot be called from a future or batch method</span> | 0 allowed<br><span style="color: purple">System.AsyncException: Database.executeBatch cannot be called from a batch start, batch execute, or future method</span> | 0 allowed<br><span style="color: purple">System.AsyncException: Database.scheduleBatch cannot be called from a batch start, batch execute, or future method</span> | 0 allowed<br><span style="color: purple">System.AsyncException: System.scheduleBatch cannot be called from a batch start, batch execute, or future method</span> | 1 allowed<br><span style="color: red">System.LimitException: Too many queueable jobs added to the queue: 2</span>   |
| `batch-finish`       |         60000 |       200 |             12 | 0 allowed<br><span style="color: purple">System.AsyncException: Future method cannot be called from a future or batch method</span> | 100 allowed<br><span style="color: blue">System.AsyncException: You've exceeded the limit of 100 jobs in the flex queue for org.</span>                           | 100 allowed<br><span style="color: teal">System.AsyncException: You have exceeded the maximum number (100) of Apex scheduled jobs.</span>                          | 100 allowed<br><span style="color: teal">System.AsyncException: You have exceeded the maximum number (100) of Apex scheduled jobs.</span>                        | 1 allowed<br><span style="color: red">System.LimitException: Too many queueable jobs added to the queue: 2</span>   |
| `queueable`          |         60000 |       200 |             12 | 50 allowed<br><span style="color: red">System.LimitException: Too many future calls: 51</span>                                      | 100 allowed<br><span style="color: blue">System.AsyncException: You've exceeded the limit of 100 jobs in the flex queue for org.</span>                           | 100 allowed<br><span style="color: teal">System.AsyncException: You have exceeded the maximum number (100) of Apex scheduled jobs.</span>                          | 100 allowed<br><span style="color: teal">System.AsyncException: You have exceeded the maximum number (100) of Apex scheduled jobs.</span>                        | 1 allowed<br><span style="color: red">System.LimitException: Too many queueable jobs added to the queue: 2</span>   |
| `finalizer`          |         10000 |       100 |             12 | 1 allowed<br><span style="color: red">System.LimitException: Too many future calls: 2</span>                                        | 100 allowed<br><span style="color: blue">System.AsyncException: You've exceeded the limit of 100 jobs in the flex queue for org.</span>                           | 100 allowed<br><span style="color: teal">System.AsyncException: You have exceeded the maximum number (100) of Apex scheduled jobs.</span>                          | 100 allowed<br><span style="color: teal">System.AsyncException: You have exceeded the maximum number (100) of Apex scheduled jobs.</span>                        | 1 allowed<br><span style="color: red">System.LimitException: Too many queueable jobs added to the queue: 2</span>   |

**Daily allocations** (org-wide, rolling 24 h — orthogonal to the per-transaction limits in the matrix below):

- **`DailyAsyncApexExecutions`** — 250.000 or (user licenses × 200), whichever is greater. Base pool for all async Apex (Batch, Scheduled, Queueable, future). Synchronous Apex does not draw from it.
- **`DailyAsyncApexElasticExecutions`** (beta) — **Queueable + future only**: `DailyAsyncApexExecutions + min(licensed daily async executions, 10.000.000)` — i.e. up to **+10 M** extra "elastic" executions, processed at a throttled rate. Batch and Scheduled get no elastic headroom; they stay bounded by `DailyAsyncApexExecutions`.
- One org-wide pool shared by `@future`, `Queueable`, `Batch`, and `Scheduled` Apex. Synchronous Apex does not draw from it.
- `finalizer`: not charged to the pool

## Durable State And Idempotency

- `Job__c` is the source of truth for job state.
- `QueueRuntime__c` is the source of truth for worker ownership, pause state,
  and worker wake planning.
- `JobScheduler__c` is the source of truth for scheduler cadence/template/mode.
- `QueueableDispatch__c` is an ephemeral queueable handoff envelope only.
- `Job__c.JobScheduler__c` is the primary scheduler provenance link.
- `Job__c.JobScheduler__c` is a restrict lookup; service code must choose
  cancellation or purge semantics before deleting schedulers.
- `Job__c.Parent__c` is a self-lookup with `SetNull`, because Salesforce does
  not allow restrict/cascade on same-object lookups.
- `Job__c.Parent__c` and `WAITING_CHILDREN` are dependency graph primitives.
  `FlowProducer` is the only path that may create waiting-children semantics.
  Direct `JobOptions.parent(...)` usage is provenance-only and must not imply
  waiting or failure propagation.
- `JobRun__c.Job__c` is master-detail; hard job deletion cascades attempt logs.
- `CANCELED` preserves abandoned future work for observability; hard
  remove/delete paths are explicitly destructive.
- Scheduler occurrence ids are framework-owned:
  `scheduler:<JobScheduler__c.Id>:<iteration>`.
- Caller-provided `JobOptions.jobId(...)` values are enforced through a
  queue-scoped unique key, reconciled case-insensitively after duplicate insert
  races, and capped at 174 chars.
- `BulkJobOptions.dedupeKey(...)` is queueable dispatcher async handoff dedupe
  only. It must not synthesize per-job ids for worker bulk insertion.
- Bulk enqueue must be execution-mode homogeneous. A duplicate job id that
  resolves to a different mode must fail rather than returning a mixed-mode
  result.

## FlowProducer Graphs

- `FlowProducer` inserts graphs atomically in parent-first waves.
- Graph width and depth are caller-controlled and naturally bounded by
  Salesforce governor limits. Do not add arbitrary graph-size constraints.
- Any graph insertion failure must roll back the complete graph.
- Existing flow jobs may only participate in an idempotent replay. If an
  existing node has any newly inserted child, reject and roll back the complete
  graph.
- Reject structural cycles, reused `FlowJob` instances, and duplicate
  queue-scoped `jobId` values within one submitted graph before insertion.
- `Job__c.DependencyFailurePolicy__c` and `Job__c.DependencyCanceledPolicy__c`
  store the child-to-parent edge behavior as separate restricted picklists.
- Default failed or canceled dependencies keep the parent in
  `WAITING_CHILDREN`. BullMQ-aligned `JobOptions` methods opt into parent
  failure, parent cancellation, continuation, dependency ignore, or failed-edge
  removal semantics.
- Parent dependency propagation is bounded per transaction. Remaining parent
  ids continue durably in a queueable resolver; resolver submission bridges
  through future when it would inherit an existing queueable stack.
- Batch execution and queueable-finalizer failure paths must use the deferred
  parent dependency-resolution seam before mutating parents. Submit at most one
  guarded resolver; when no submission slot remains, leave parents in
  `WAITING_CHILDREN` for maintenance.
- Worker runtimes must be locked before worker-mode parent rows. If a deeper
  cross-queue level needs a new runtime lock, defer it to the durable resolver.
- Maintenance selectively redrives stale `WAITING_CHILDREN` parents by queue
  and lease age. `DependencyRepairCheckedAt__c` rotates checked parents fairly
  so unresolved parents cannot starve newer repair candidates. It must not scan
  or resolve fresh parents globally.
- Failed `REMOVE_DEPENDENCY` edges count as resolved immediately. Physical edge
  removal uses sparse, bounded updates and continues durably without delaying
  parent release. Maintenance repairs stale edges left by dropped continuations.
- Queue drain/cancel may resolve same-queue parents without scheduling a worker
  wake for the queue currently being drained or canceled.

## Security Boundaries

- Public facade/admin/observability reads must honor caller visibility with
  user-mode queries or equivalent sharing-aware checks.
- Public facade/admin mutations must use user-mode DML for durable queue
  records; internal framework paths must opt into explicit `*AsSystem` methods.
- Worker, scheduler, maintenance, queueable, invocable, and error-recovery
  internals may use system-mode persistence so private OWD does not break
  framework execution.

## Worker Lane

- Worker batch scope is fixed at `1`.
- `batch.start()` owns claim and durable worker-state updates.
- `batch.execute()` is the clean processor transaction.
- `batch.finish()` owns worker continuation.
- `batch.start()` and `batch.execute()` must not initiate worker-plan
  transitions or call framework async scheduling APIs.
- `BatchClaimSize__c` is a job-count claim limit, not a parallelism or time
  window setting. Do not cap it by lease minutes.
- Worker scopes must be treated as serial for lease and throughput planning.
- Job claim, execution completion, and stalled-job recovery must reread target
  rows under lock before mutating durable state.
- `batch.execute()` must reverify current lease ownership before committing job
  results.
- Normal processor exceptions are handled inside the execution service path.
- Catastrophic batch `EXECUTE` failures are recovered through
  `BatchApexErrorEvent.JobScope`.
- Batch error handling must resolve `BatchApexErrorEvent.AsyncApexJobId`
  through `AsyncApexJob.ParentJobId` when events point at `BatchApexWorker`
  child rows.
- `batch.finish()` must not inspect job outcomes,
  `BatchApexErrorEvent`, or `AsyncApexJob.NumberOfErrors` to decide whether the
  worker pump may continue.

## Concurrency, Parallelism, And Flex Queue

- Concurrency means processor transactions may overlap. Parallelism means they
  actually execute simultaneously. The framework may create independent
  concurrent work, but Salesforce controls whether and when it runs in
  parallel.
- One `WORKER` queue owns one serial Batch Apex worker lane. Parallel worker
  execution must be requested by partitioning independent jobs across separate
  queue names.
- Salesforce admits up to five Batch Apex jobs from the Flex Queue into queued
  or active processing at a time. This org-wide capacity is shared with
  unrelated Batch Apex work and creates an opportunity for parallelism, not a
  guarantee of five simultaneous processor transactions.
- `BatchClaimSize__c`, `ACTIVE` job counts, multiple submitted async jobs, and
  Flow siblings do not prove parallel execution.
- `QUEUEABLE_SERIAL` guarantees ordered dispatcher submission only; submitted
  executor transactions may still overlap or finish out of order.
- `QUEUEABLE_CONCURRENT` and `INVOCABLE` create independent concurrent
  processor transactions. Salesforce still controls actual parallelism.
- Actual parallelism must be measured from overlapping `JobRun__c.StartedAt__c`
  and `JobRun__c.FinishedAt__c` ranges. Distinct `AsyncApexJobId__c` values
  identify native executions but do not prove overlap.
- Flex Queue management is scoped to the current framework-owned worker stored
  on `QueueRuntime__c.AsyncApexJobId__c`. The public facade must not expose
  arbitrary org-wide Batch Apex jobs or relative before/after reordering.
- Worker reordering is valid only while the current `AsyncApexJob` is
  `Holding`. Commands must lock the queue runtime before resolving the current
  worker, tolerate native status races, and return `false` when Salesforce
  cannot reorder it.
- Worker status inspection may remain read-only. Reordering requires the
  `QueuesManageWorkers` custom permission, granted by `QueuesAdmin` but not
  `QueuesReadOnly`.
- The framework must never automatically promote workers in the Flex Queue;
  doing so could starve unrelated Batch Apex work in the org.

## Queueable Lanes

- `QUEUEABLE_SERIAL` is ordered dispatcher submission, not strict serialized
  processor execution.
- Dispatchers own handoff. Executors own one job's execution and retry/finalizer
  behavior. Executors must not call dispatchers.
- Queueable dispatchers initiate only original, not-yet-initiated dispatch jobs.
  Once submitted, `Job__c.QueueableDispatchInitiatedAt__c` must prevent
  dispatcher recapture during retries.
- Shared queueable transport code may own native delay validation, executor
  submission, stack-depth checks, and simple submission-result mutation. It
  must not own dispatcher progression, retry policy, repositories, or processor
  execution.
- Queueable processor code must not enqueue another queueable job. The
  framework owns the queueable chain for retries and finalizers.
- Queueable initial delay from user options must validate within native
  `0..10` minutes and fail fast. Framework retry delays saturate to that cap.
- Backoff jitter applies only to retry eligibility by updating
  `Job__c.AvailableAt__c`; it does not promise exact pickup or execution time.
- Queueable stack-depth probes may call `System.AsyncInfo` only from
  `QUEUEABLE` or `TRANSACTION_FINALIZER_QUEUEABLE` contexts.
- If native retry enqueue throws `System.AsyncException`, treat it as
  bridgeable through future rather than parsing platform error text.
- `Future` is only a queueable stack-depth escape bridge; it is not a processor
  execution lane.
- Queueable dispatcher dedupe may use `DuplicateSignature`; raw
  `queueName:dedupeKey` is preferred when it fits 32 bytes, otherwise use a
  deterministic compressed signature while preserving the original key on
  `QueueableDispatch__c.DedupeKey__c`.

## Invocable Lane

- `INVOCABLE` exists for Flow Scheduled Path / `INVOCABLE_ACTION` execution
  with measured sync-like limits, while durability remains framework-owned.
- Durable ownership, attempts, retry, backoff, leases, and final state remain
  on `Job__c` / `JobRun__c`.
- Claim and execute must be separate transactions.
- `Queues_Invocable_Claim` handles due `WAITING` / `DELAYED` jobs.
- `Queues_Invocable_Execute` handles `ACTIVE` jobs.
- Claim Flow `maxBatchSize` must stay capped with DML headroom; current source
  uses `100`.
- Execute Flow `maxBatchSize` must stay `1`.
- An execute action receiving more than one input is a configuration error.
- Wake source is `Job__c.AvailableAt__c`; scheduled-path latency is
  scheduler-granular.
- Claim must reread under lock, verify mode/state/due time, and only then move
  a job to `ACTIVE`.
- Claim must bulk-lock, validate, and update the complete Flow input batch;
  query cost must not scale with the number of inputs.
- Execute must reread, verify mode/state/lease ownership, and no-op stale
  resumes.
- Do not collapse the two-flow design without a new passing e2e proof. Probed
  single-flow variants failed isolation or single-job execution.
- Maintenance moves stale due `WAITING` / `DELAYED` invocable jobs to a
  one-minute future `AvailableAt__c` to repair lost pre-claim Flow wakes after
  the queue lease grace.
- Expired `ACTIVE` invocable leases are recovered by maintenance.
- `JobRun__c.AsyncApexJobId__c` should remain blank for Scheduled Path runs
  unless Salesforce exposes a meaningful async id.

## Scheduler Model

- Runtime recovery and scheduler materialization are separate systems.
- Scheduler-backed `Schedulable` classes may materialize `Job__c` only.
- Scheduler-backed `Schedulable` classes must never execute processors or
  replace the batch worker loop.
- `schedulers/` is reserved for concrete `implements Schedulable` entrypoints.
  Scheduling facades/helpers belong under `scheduling/`.
- Scheduler templates persist regular `JobOptions`.
- Scheduler execution mode is an explicit `Queues.ExecutionMode` upsert
  argument stored on `JobScheduler__c.ExecutionMode__c`.
- `WORKER` scheduler occurrences wake the worker; queueable occurrences
  dispatch when due; `INVOCABLE` occurrences rely on Flow Scheduled Path from
  `Job__c.AvailableAt__c`.
- `pattern(...)` maps to native recurring `Schedulable`.
- `every(...)` maps to one-shot self-rescheduling `Schedulable`.
- `every(...)` next fire must be computed from persisted scheduler state, not
  only current wall clock.
- `pattern(...)` and `every(...)` must share one materializer body.
- `runJobSchedulerMaintenance()` is repair-only. It must not become a cadence
  engine.
- Scheduler maintenance must bulk/cache shared lookups before per-scheduler
  repair decisions.
- `ScheduledJobId__c` is the primary healthy-chain anchor for repair.
- Scheduler materialization, lifecycle commands, and repair must lock the
  durable `JobScheduler__c` row before mutation. After locking, materialization
  rechecks the triggering scheduled-job id and repair rechecks active state plus
  scheduled-job identity before creating or aborting native schedules.
- Scheduler tick and repair writes must be sparse so stale in-memory scheduler
  snapshots cannot overwrite unrelated durable state.
- Re-upsert must keep `JobScheduler__c.Id`, cancel pending jobs from the old
  schedule, abort old native schedules, reset durable state from new options,
  and materialize using the next monotonic iteration.
- Remove/delete paths are destructive. `removeJobScheduler(...)` aborts native
  schedules and deletes the scheduler plus linked jobs; active linked jobs must
  block hard removal.
- Cancel paths preserve durable records. `cancelJobScheduler(...)` aborts native
  schedules, deactivates the scheduler, and marks pending linked jobs
  `CANCELED`.
- `Queue.drain()` deletes drainable jobs across execution lanes;
  `Queue.cancel()` marks drainable jobs across execution lanes `CANCELED`.
- Re-upsert must not reset `lim(...)` lifetime counting.
- When next iteration would exceed `lim(...)`, deactivate/reject without
  creating a new `Job__c`; pattern schedulers also abort the native schedule.

## Scheduling And Maintenance

- Scheduled Apex artifacts are wake hints.
- Worker scheduling is optimistic: pull an existing wake earlier when needed,
  but do not push a scheduled worker later only to keep artifacts tidy.
- Do not assume `System.scheduleBatch(...)` is limited to 60 minutes; org probes
  accepted the full Apex `Integer` minute range.
- Runtime maintenance is for stalled jobs, dead workers, expired leases,
  orphaned artifacts, and recovery only.
- Scheduled runtime maintenance and job-scheduler maintenance must run in
  separate transactions. Runtime recovery owns the scheduled transaction;
  job-scheduler repair runs through a queueable handoff with a fresh governor
  budget. Submit that handoff before runtime recovery so its queueable slot is
  reserved.
- Every queueable submission reachable from runtime maintenance must check the
  shared remaining-slot budget. Exhaustion must leave durable work for a later
  sweep instead of attempting an uncatchable over-limit submission.
- Runtime maintenance discovery must stay runtime-driven; do not reintroduce
  unbounded global `Job__c` queue discovery.
- Maintenance may abort framework-owned scheduled worker hints and old
  unreferenced `BatchableWorker` async jobs after a grace window.
- If cleanup clears runtime ownership while non-expired `ACTIVE` leases remain,
  keep the runtime maintenance-discoverable until those leases can expire.
- Maintenance/admin loops must reserve CPU, query, DML row, and DML statement
  headroom. Savepoints count against DML statement budget.
- Queue admin recovery and drain are bounded-progress operations.

## Transaction And Telemetry

- Processor business logic must begin in a clean transaction, not after
  framework DML in the same transaction.
- Processor side effects must be externally idempotent. Use
  `JobContext.idempotencyKey()` as the stable logical-job key.
- Durable `Job__c` / `JobRun__c` commits happen after processor work; replay is
  possible after remote side effects if the transaction dies.
- Admin and queue-admin commands (cancel, remove, retry, promote, resume, defer,
  change-priority, drain, queue-cancel) are transaction-boundary atomic, not
  catch-safe atomic, and must not be wrapped in a broad command-level savepoint.
  They call shared flows (e.g. `resolveParentDependenciesAsSystem`) that interleave
  durable DML with native async side effects (`System.enqueueJob`,
  `System.scheduleBatch` / `Database.executeBatch`, `System.abortJob`) that a
  rollback cannot undo, so a savepoint would create the DB/native mismatch the
  framework otherwise avoids.
- A caller that catches such a command's exception may observe partial state.
  `Job__c` stays the source of truth; partial state is reconciled by maintenance
  backstops (stale `WAITING_CHILDREN` repair, removed-dependency cleanup repair,
  stale queueable dispatch recovery, orphaned worker/scheduler cleanup, runtime
  maintenance). This is a deliberate boundary, not a gap to close with
  savepoints.
- Queue-error telemetry may use publish-immediate platform events from outer
  framework seams only.
- Do not add optional telemetry DML to the clean processor transaction.
- Duplicate logical queue errors within one transaction should be coalesced
  before publish.
- Publish-failure fallback should stay low overhead: bulk insert first, then
  salvage per row.
- Freshly dispatched worker jobs must be aborted by id on failed handoff even
  if async artifact visibility has not caught up.
- Queue stats should remain exact aggregate counts; do not reintroduce stale
  truncation flags.

## Change Control

- Repositories own persistence only; business rules belong in services,
  workers, handlers, schedulers, and transport classes.
- Keep `Queues.cls` as the public facade and `QueuesService` as the thin
  orchestrator.
- Do not partially rename public concepts. Sweep completely or defer.
- Do not choose queueable mode only for speculative speed. Choose it for
  isolation, one executor per durable job, finalizers, native delay, or
  dispatch dedupe; measure fanout changes.
- Do not introduce new scheduler/runtime state in ad hoc places. Add metadata
  first when invariants require durable state.
