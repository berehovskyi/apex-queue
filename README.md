# Apex Queue

![](https://img.shields.io/github/v/release/berehovskyi/apex-queue?include_prereleases)
[![build](https://github.com/berehovskyi/apex-queue/actions/workflows/validate.yml/badge.svg?branch=main&event=push)](https://github.com/berehovskyi/apex-queue/actions/workflows/validate.yml)
[![coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fberehovskyi%2Fapex-queue%2Fcoverage%2Fcoverage.json)](https://github.com/berehovskyi/apex-queue/actions/workflows/validate.yml)

Durable queue framework for Salesforce Apex jobs inspired by BullMQ.

Apex Queue stores work in `Job__c`, executes user processors through the
public `Queues` facade, records attempts in `JobRun__c`, and treats Salesforce
async artifacts as wake or handoff mechanisms instead of the source of truth.

## Table of Contents

- [Installation](#installation)
    - [Configuration](#configuration)
- [Quick Start](#quick-start)
- [Features](#features)
- [Usage](#usage)
    - [Queue](#queue)
        - [Queue Settings](#queue-settings)
        - [Adding a Job](#adding-a-job)
        - [Adding Jobs in Bulk](#adding-jobs-in-bulk)
        - [Bulk Options](#bulk-options)
        - [Operating the Queue](#operating-the-queue)
    - [Workers](#workers)
        - [Execution Modes](#execution-modes)
        - [Worker Priority](#worker-priority)
    - [Processors](#processors)
        - [Processor Settings](#processor-settings)
        - [Processor Retrying](#processor-retrying)
            - [Stop Processor Retrying](#stop-processor-retrying)
        - [Error Handling](#error-handling)
    - [Job](#job)
        - [Job IDs](#job-ids)
        - [Idempotent Job IDs](#idempotent-job-ids)
        - [Job Data](#job-data)
        - [Progress](#progress)
        - [Job Options](#job-options)
        - [Delayed Jobs](#delayed-jobs)
        - [Prioritized Jobs](#prioritized-jobs)
        - [Pausing, Resuming, and Checkpoints](#pausing-resuming-and-checkpoints)
        - [Retrying](#retrying)
            - [Stop Retrying](#stop-retrying)
            - [Manual Retrying](#manual-retrying)
        - [Removing and Canceling](#removing-and-canceling)
        - [Stalled Jobs](#stalled-jobs)
        - [Getters](#getters)
    - [Job Scheduler](#job-scheduler)
        - [Repeat Strategies](#repeat-strategies)
        - [Repeat Options](#repeat-options)
        - [Job Template](#job-template)
            - [Template Job Options](#template-job-options)
        - [Manage Job Schedulers](#manage-job-schedulers)
    - [Flows](#flows)
        - [Adding Flows](#adding-flows)
        - [Get Flow Tree](#get-flow-tree)
        - [Dependency Policies](#dependency-policies)
    - [Maintenance](#maintenance)
        - [Recovery](#recovery)
        - [Job Retention Policy](#job-retention-policy)
        - [Schedule Maintenance](#schedule-maintenance)
        - [Run Maintenance Manually](#run-maintenance-manually)
    - [Metrics](#metrics)
        - [Operational Errors](#operational-errors)
    - [Concurrency and Parallelism](#concurrency-and-parallelism)
        - [Creating Parallelism](#creating-parallelism)
- [Architecture](#architecture)
    - [Durable State](#durable-state)
    - [Recovery Model](#recovery-model)
    - [Delete vs Cancel](#delete-vs-cancel)
    - [Security Model](#security-model)
- [Documentation](#documentation)

## Installation

Deploy the framework metadata into the target org:

```sh
sf project deploy start -d sfdx-source/apex-queue -o <org-alias>
```

or install as an Unlocked Package:

```sh pkg::apex-queue
sf package install -p 04tJ5000000DA2tIAG -o <org-alias> -r -w 10
```

Assign one of the packaged permission sets to users who need framework access:

```sh
sf org assign permset --name QueuesUser
sf org assign permset --name QueuesAdmin
sf org assign permset --name QueuesReadOnly
```

`QueuesUser` is the least-privilege role for applications that enqueue and
manage their own jobs and schedulers. It can coordinate the shared queue
runtime used by all jobs in a queue. `QueuesAdmin` adds cross-record operational
access and worker reordering. `QueuesReadOnly` is intended for users who inspect
queue state without mutating it.

### Configuration

Apex Queue is configured through deployable custom metadata. Version records
alongside your source:

- [Queue settings](#queue-settings) create active queues and define queue
  defaults.
- [Processor settings](#processor-settings) allowlist processor classes that
  jobs are allowed to run.

The [Quick Start](#quick-start) shows the minimum metadata needed for the first
queue and processor.

Production orgs should also [schedule maintenance](#schedule-maintenance) for
recovery and repair.

> [!IMPORTANT]
> Schedule maintenance in every production org. Without it, durable recovery
> and repair remain available but do not run automatically.

## Quick Start

Create the minimum custom metadata records:

- `QueueDefinition__mdt`: `QueueName__c = invoice-sync`, `IsActive__c = true`.
- `Processor__mdt`: `Name__c = Invoice Sync Processor`,
  `ProcessorClass__c = InvoiceSyncProcessor`, `IsActive__c = true`.

Then add the processor class:

```apex
public class InvoiceSyncProcessor implements Queues.JobProcessor {
    public Queues.ProcessResult process(final Queues.JobContext ctx) {
        Map<String, Object> data = (Map<String, Object>) ctx.data();
        // Execute the job logic, then return the result.
        Map<String, Object> result = new Map<String, Object>{
            'invoiceId' => data.get('invoiceId'),
            'status' => 'synced'
        };

        return ctx.complete(result);
    }
}
```

Enqueue work through the public facade:

```apex
Queues.Job job = Queues.of('invoice-sync').add(
    'sync-invoice',
    'InvoiceSyncProcessor',
    new Map<String, Object>{ 'invoiceId' => 'INV-001' }
);

Id jobId = job.getId();
```

For production recovery, [schedule maintenance](#schedule-maintenance) once per org:

```apex
Queues.scheduleMaintenance();
```

## Features

Everything you need to run background work on Salesforce reliably, with a
BullMQ-inspired API.

- **Durable by design** — committed work is represented by `Job__c`, the
  framework's source of truth for recovery and inspection.
- **Self-healing** — with scheduled maintenance enabled, Apex Queue recovers
  stalled jobs, orphaned async handoffs, and dependency graphs that need repair.
- **The right lane for every job** — run work on batch workers, queueables, or
  invocable Flow paths, and switch with a single option.
- **Clean processor boundary** — each attempt runs through an isolated processor
  context with callout-capable transports and durable attempt results.
- **Smart retries** — fixed or exponential backoff with optional jitter, so
  recovering downstream systems do not get hit by a retry stampede.
- **Recurring work** — use cron expressions or simple every-N-minute repeats,
  materialized into durable, trackable jobs.
- **Workflows, not just jobs** — compose atomic dependency graphs with
  `FlowProducer`; parents wait until their children finish.
- **Full control** — pause, resume, drain, cancel, wake, prioritize, delay, promote,
  and retry jobs on demand.
- **Idempotent by key** — queue-scoped job ids dedupe at the database boundary, so
  retries and replays do not double-enqueue the same logical job.
- **Observable** — per-attempt history, durable failure reasons, queue stats, and
  job counts by state out of the box.

## Usage

### Queue

A queue is a named stream of durable jobs. Use `Queues.of(queueName)` to create a
lightweight queue handle:

```apex
Queues.Queue queue = Queues.of('invoice-sync');
```

The queue must exist as an active `QueueDefinition__mdt` record before jobs can
be added.

#### Queue Settings

`QueueDefinition__mdt` declares a queue. The record also holds the queue's
default retry, backoff, lease, and claim policy, which individual jobs may
override.

Create a `QueueDefinition__mdt` record for every queue name you want to use:

| Field                                            | Default | Purpose                                          |
| ------------------------------------------------ | ------- | ------------------------------------------------ |
| `QueueName__c`<span style="color: red">\*</span> | -       | Public queue name used in `Queues.of(...)`.      |
| `Description__c`                                 | -       | Optional admin-facing queue description.         |
| `IsActive__c`                                    | `false` | Whether new work can be added to the queue.      |
| `DefaultAttempts__c`                             | `1`     | Attempts used when a job does not override them. |
| `DefaultBackoffType__c`                          | `NONE`  | Default retry backoff strategy.                  |
| `DefaultBackoffValue__c`                         | `0`     | Default retry backoff value in minutes.          |
| `DefaultBackoffJitter__c`                        | `0`     | Default retry jitter ratio from `0` to `1`.      |
| `DefaultLeaseMinutes__c`                         | `5`     | Lease age before active work can be recovered.   |
| `BatchClaimSize__c`                              | `1`     | Worker claim size for `WORKER` jobs.             |
| `TerminalCleanupPolicy__c`                       | `KEEP`  | Terminal job cleanup policy: `KEEP` or `DELETE`. |
| `CompletedRetentionDays__c`                      | -       | Days to keep `COMPLETED` jobs when deleting.     |
| `FailedRetentionDays__c`                         | -       | Days to keep `FAILED` jobs when deleting.        |
| `CanceledRetentionDays__c`                       | -       | Days to keep `CANCELED` jobs when deleting.      |

#### Adding a Job

The most common operation is adding a job to a queue:

```apex
Queues.Job job = Queues.of('invoice-sync').add(
    'sync-invoice',
    'InvoiceSyncProcessor',
    new Map<String, Object>{ 'invoiceId' => 'INV-001' }
);
```

The first argument is the job name, the second is the processor class name, and
the third is JSON-serializable job data.

#### Adding Jobs in Bulk

Use `addBulk(...)` when the request should insert several jobs atomically:

```apex
List<Queues.Job> jobs = Queues.of('invoice-sync').addBulk(
    new List<Queues.JobData>{
        new Queues.JobData(
            'sync-invoice',
            'InvoiceSyncProcessor',
            new Map<String, Object>{ 'invoiceId' => 'INV-001' }
        ),
        new Queues.JobData(
            'sync-invoice',
            'InvoiceSyncProcessor',
            new Map<String, Object>{ 'invoiceId' => 'INV-002' }
        )
    }
);
```

If one job in the bulk request cannot be inserted, the whole request rolls back.

> [!NOTE]
> `addBulk(...)` is atomic. Salesforce governor limits still bound the size of a
> single request.

#### Bulk Options

`BulkJobOptions` controls the [execution mode](#execution-modes) for a bulk
request. `WORKER` is the default:

```apex
new Queues.BulkJobOptions().worker();
new Queues.BulkJobOptions().queueable();
new Queues.BulkJobOptions().queueableConcurrent();
new Queues.BulkJobOptions().invocable();
```

Queueable bulk requests can also provide a dispatcher dedupe key:

```apex
new Queues.BulkJobOptions()
    .queueable()
    .dedupeKey('invoice-sync:batch-2026-06-04');
```

`dedupeKey(...)` dedupes the initial queueable dispatcher handoff. It is not a
per-job idempotency key. Use `JobOptions.jobId(...)` for durable per-job
idempotency.

> [!IMPORTANT]
> A dispatcher `dedupeKey(...)` and a job `jobId(...)` solve different problems.
> Do not use the dispatcher key as protection against processing a durable job
> twice.

#### Operating the Queue

Queue handles expose the common admin operations:

```apex
Queues.Queue queue = Queues.of('invoice-sync');

queue.pause();
queue.resume();

Integer woken = queue.wake();
Integer recovered = queue.recoverStalled();
Integer canceled = queue.cancel();
Integer drained = queue.drain();
```

The integer return values are counts of jobs affected by the operation.

`cancel()` marks drainable jobs as `CANCELED`. `drain()` deletes drainable jobs
and cascades their `JobRun__c` records through master-detail.

> [!CAUTION]
> `drain()` is destructive and removes attempt history. Use `cancel()` when the
> durable records must remain available for inspection.

### Workers

Workers are framework-owned transports. The execution mode selects which
Salesforce runtime will call the processor.

#### Execution Modes

| Mode                   | Runtime                | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ---------------------- | ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WORKER`               | Framework batch worker | Default lane. Claims due jobs and executes one job per `execute()` transaction.                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `QUEUEABLE_SERIAL`     | Queueable dispatch     | Executes one queueable executor per durable job. Preserves ordered submission, but does not guarantee start or finish order after Salesforce accepts executor jobs.                                                                                                                                                                                                                                                                                                                                                  |
| `QUEUEABLE_CONCURRENT` | Queueable dispatch     | Fastest queueable lane. Executes one queueable executor per durable job and allows concurrent submission, so use it carefully around Salesforce async limits and downstream capacity.                                                                                                                                                                                                                                                                                                                                |
| `INVOCABLE`            | Flow Scheduled Paths   | Fast after its Scheduled Path wake. Jobs are independently dispatched and can execute concurrently, with each processor running in a clean `INVOCABLE_ACTION` transaction; execution order and simultaneous start are not guaranteed. Avoids batch, queueable, and future execution-lane constraints for processor code and does not spend those daily async allocations. The trade-off is higher start latency: it consumes Scheduled Path capacity and requires at least one Scheduled Path wake before execution. |

Choose `WORKER` unless a job specifically needs queueable isolation or
invocable transaction traits.

> [!WARNING]
> `QUEUEABLE_CONCURRENT` can submit work very quickly. Size workloads for
> Salesforce async limits and for the capacity of downstream systems.

> [!TIP]
> Choose `INVOCABLE` when clean concurrent processor transactions and avoiding
> batch, queueable, and future daily allocations matter more than immediate
> start latency.

#### Worker Priority

Worker-mode queues use Batch Apex. When the current worker is waiting in
Salesforce's Flex Queue, it can be moved to the front or end:

```apex
Queues.Queue queue = Queues.of('invoice-sync');
Queues.WorkerStatus worker = queue.getWorkerStatus();
if (worker.isHolding()) {
    queue.moveWorkerToFront();
}
```

`moveWorkerToFront()` and `moveWorkerToEnd()` return `true` only when Salesforce
successfully reorders the current framework-owned worker. They return `false`
when no worker exists, the worker is scheduled for later, it has already left
the Flex Queue, or another transaction wins the race.

> [!IMPORTANT]
> Reordering changes the priority of one queue's current Batch Apex worker
> relative to every other holding Batch Apex job in the org. Apex Queue never
> promotes workers automatically because doing so could starve unrelated work.
> Reordering requires the `QueuesManageWorkers` custom permission, which is
> granted by `QueuesAdmin` but not `QueuesUser` or `QueuesReadOnly`.

> [!NOTE]
> This API does not expose the org-wide Flex Queue or arbitrary Batch Apex jobs.
> Queueable and invocable execution modes do not use the Flex Queue.

### Processors

#### Processor Settings

`Processor__mdt` registers an Apex class the framework is allowed to run as a
job processor. Enqueuing fails with a `ConfigurationException` unless a
matching, active record exists, so it doubles as an allowlist and a
per-processor kill switch.

Create a `Processor__mdt` record for every Apex processor class:

| Field                                                 | Default | Purpose                                                              |
| ----------------------------------------------------- | ------- | -------------------------------------------------------------------- |
| `Name__c`<span style="color: red">\*</span>           | -       | Friendly processor name.                                             |
| `ProcessorClass__c`<span style="color: red">\*</span> | -       | Fully qualified Apex class name passed to `Queues.of(...).add(...)`. |
| `Description__c`                                      | -       | Optional admin-facing processor description.                         |
| `IsActive__c`                                         | `false` | Whether this processor mapping can be used.                          |

Processors implement `Queues.JobProcessor`:

Apex Queue runs one `process(ctx)` call per transaction for one durable job
attempt. Bulk claiming and dispatch can happen before execution, but processor
code is not invoked for multiple jobs in the same transaction.

```apex
public class InvoiceSyncProcessor implements Queues.JobProcessor {
    public Queues.ProcessResult process(final Queues.JobContext ctx) {
        Map<String, Object> data = (Map<String, Object>) ctx.data();
        String invoiceId = (String) data.get('invoiceId');
        Map<String, Object> result = new Map<String, Object>{ 'invoiceId' => invoiceId, 'status' => 'synced' };

        return ctx.complete(result);
    }
}
```

Processors can perform callouts. Configure the endpoint with the normal
Salesforce callout mechanisms, such as a Named Credential or Remote Site
Setting.

```apex
public class InvoiceSyncProcessor implements Queues.JobProcessor {
    public Queues.ProcessResult process(final Queues.JobContext ctx) {
        // HTTP callout work
        Map<String, Object> result = new Map<String, Object>{ 'status' => 'posted' };

        return ctx.complete(result);
    }
}
```

> [!WARNING]
> Avoid unmanaged native async chains in processors. Apex Queue can recover and
> cancel framework-owned async work, but not native async work created directly
> by processor code.

`JobContext` exposes the durable job record id, queue name, processor name,
idempotency key, attempts, data, checkpoint, and progress.

Processors return one of the framework result types:

```apex
return ctx.complete(returnValue);
return ctx.fail('retryable reason');
return ctx.pause(checkpoint);
return ctx.defer(checkpoint, 5);
```

#### Processor Retrying

Processor retrying starts when an attempt fails normally:

- `ctx.fail(reason)` is an intentional retryable processor result. It records a
  failed attempt and retries while attempts remain.
- A regular exception thrown by `process(ctx)` is caught by the framework and
  handled the same way: failed attempt, retry if attempts remain.

##### Stop Processor Retrying

Throw `Queues.UnrecoverableJobException` when a processor detects a permanent
failure and the job should not retry:

```apex
throw new Queues.UnrecoverableJobException('Invoice is permanently invalid.');
```

The framework marks the job as `FAILED` immediately, regardless of remaining
attempts.

> [!IMPORTANT]
> Use `UnrecoverableJobException` only for permanent failures. It bypasses every
> remaining automatic retry.

#### Error Handling

Catastrophic async failure is different from a normal processor failure. It
means the Salesforce transaction died before normal finalization could
complete, for example a limit failure, platform async failure, or batch scope
crash. The framework repairs these paths through queueable finalizers,
`BatchApexErrorEvent`, leases, and maintenance.

If a transaction dies after an external callout succeeds but before the job
result is saved, the same durable job can run again. External writes and
callouts should tolerate duplicates. `ctx.idempotencyKey()` returns a stable
queue-scoped key for the durable job. It uses `JobOptions.jobId(...)` when the
job has one, otherwise it falls back to the queue name plus `Job__c.Id`.

Send that key, or a stable business key such as an invoice id, to external
systems when they support idempotent requests:

```apex
request.setHeader('Idempotency-Key', ctx.idempotencyKey());
```

> [!WARNING]
> A catastrophic transaction failure can replay a durable job after an external
> side effect already succeeded. Make external writes idempotent whenever
> possible.

Processor failures are job attempt failures, not `QueueError__c` records. Use
`JobRun__c`, `Job__c.FailedReason__c`, and final job state to inspect them.
`QueueError__c` is for framework operational failures.

### Job

`Queues.Job` is a lightweight handle around a durable `Job__c` record:

```apex
Queues.Job createdJob = Queues.of('invoice-sync').add(
    'sync-invoice',
    'InvoiceSyncProcessor',
    data
);

Id jobId = createdJob.getId();
Queues.Job job = Queues.job(jobId);
Queues.State state = job.getState();
```

#### Job IDs

`Queues.Job.getId()` returns the Salesforce `Job__c.Id`. Use this record id
when operating on an existing durable job:

```apex
Id jobId = job.getId();

Queues.job(jobId).pause();
Queues.job(jobId).resume();
```

In this guide, `jobId` means the Salesforce `Job__c.Id` unless it refers to the
`JobOptions.jobId(...)` method.

> [!NOTE]
> `Queues.Job.getId()` returns a Salesforce record id. `JobOptions.jobId(...)`
> accepts a caller-owned, queue-scoped idempotency key.

#### Idempotent Job IDs

`JobOptions.jobId(...)` sets a caller-owned, queue-scoped idempotency key:

```apex
Queues.Job job = Queues.of('invoice-sync').add(
    'sync-invoice',
    'InvoiceSyncProcessor',
    data,
    new Queues.JobOptions().jobId('invoice:INV-001')
);
```

If the same queue receives the same `jobId(...)` again, the framework
reconciles the duplicate at the database boundary and returns the existing job.
The same id can be reused in a different queue.

#### Job Data

Job data is serialized into `Job__c.Data__c` and deserialized through
`ctx.data()`:

```apex
new Map<String, Object>{
    'invoiceId' => 'INV-001',
    'force' => true
};
```

Non-active jobs can update data before the next execution:

```apex
Queues.job(jobId).updateData(new Map<String, Object>{ 'force' => false });
```

#### Progress

Processors can persist progress with a non-terminal result:

```apex
return ctx.defer(
        new Map<String, Object>{ 'lastCompletedStep' => 'posted-invoice' },
        5
    )
    .withProgress(new Map<String, Object>{ 'step' => 'waiting-for-ledger' });
```

The next execution can read progress and checkpoint data from the context:

```apex
Object progress = ctx.progress();
Object checkpoint = ctx.checkpoint();
```

Use this for multi-step processors that can safely resume after partial work.
For example, if the first callout succeeded and the second external system is
temporarily unavailable, defer with a checkpoint instead of failing the job.

Non-active jobs can also update progress through the job handle:

```apex
Queues.job(jobId).updateProgress(new Map<String, Object>{ 'step' => 'review' });
```

#### Job Options

Common job options are chainable:

```apex
new Queues.JobOptions()
    .jobId('invoice:INV-001')
    .attempts(5)
    .priority(2)
    .delay(3)
    .backoff(Queues.Backoff.FIXED, 5, 0.25);
```

Queue metadata supplies defaults when an option is not provided.

`category(...)`, `groupKey(...)`, and `correlationId(...)` populate indexed
`Job__c` fields for SOQL lookups. Use them for operational lookup dimensions;
`Data__c` is payload, not a query model.

#### Delayed Jobs

Use `delay(minutes)` for a relative delay:

```apex
new Queues.JobOptions().delay(10);
```

Use `availableAt(datetime)` for an absolute availability time:

```apex
new Queues.JobOptions().availableAt(System.now().addHours(1));
```

`AvailableAt__c` controls when the job becomes eligible. Exact pickup still
depends on the selected execution lane and Salesforce scheduling.

> [!NOTE]
> Availability is not an execution-time guarantee. A due job may still wait for
> Salesforce or for its selected transport.

Use `promote()` when a delayed job should run now instead of waiting for its
`AvailableAt__c` timestamp. The job moves from `DELAYED` to `WAITING` and the
queue is woken if needed:

```apex
Queues.job(jobId).promote();
```

#### Prioritized Jobs

Lower `priority(...)` values are claimed before higher values when jobs are
otherwise due:

```apex
new Queues.JobOptions().priority(1);
```

Waiting, delayed, or paused jobs can change priority:

```apex
Queues.job(jobId).changePriority(0);
```

Priority controls framework claim or dispatch order, but does not guarantee
execution order.

> [!NOTE]
> Salesforce controls native async execution after the framework dispatches a
> job, so priority cannot guarantee start or completion order.

#### Pausing, Resuming, and Checkpoints

Pause a job directly:

```apex
Queues.job(jobId).pause();
```

For waiting, delayed, or paused jobs, the state changes immediately. For active
jobs, the framework records a pause request. If the processor leaves the job in
a runnable state, the framework pauses it before the next execution.

Processors can pause themselves with a checkpoint:

```apex
return ctx.pause(new Map<String, Object>{ 'lastInvoiceId' => 'INV-001' });
```

The next execution can resume from that checkpoint:

```apex
Map<String, Object> checkpoint = (Map<String, Object>) ctx.checkpoint();
```

Resume a paused job when it should become runnable again:

```apex
Queues.job(jobId).resume();
```

#### Retrying

Handled failures retry until attempts are exhausted:

```apex
new Queues.JobOptions()
    .attempts(5)
    .backoff(Queues.Backoff.EXPONENTIAL, 2);
```

Backoff values are expressed in minutes. `FIXED` uses the same delay for every
retry. `EXPONENTIAL` grows from the provided base.

Backoff jitter can spread retry eligibility:

```apex
new Queues.JobOptions()
    .attempts(5)
    .backoff(Queues.Backoff.FIXED, 10, 0.5);
```

With jitter `0.5`, a retry with a calculated delay of 10 minutes becomes
eligible somewhere inside the 5-to-10-minute window. Jitter updates
`AvailableAt__c`; it does not promise exact execution timing.

Jitter is most visible when the jittered backoff window is wider than the
transport's pickup cadence. A 1-minute backoff may still be picked up in the
same scheduler or worker sweep.

Queueable retry backoff is capped at 10 minutes by Salesforce's native
queueable delay limit. `WORKER` retry delays are not framework-capped.

##### Stop Retrying

Throw `Queues.UnrecoverableJobException` to fail a job without another retry:

```apex
throw new Queues.UnrecoverableJobException('Invoice is permanently invalid.');
```

Use `discard()` to stop future retries for a non-active job:

```apex
Queues.job(jobId).discard();
```

##### Manual Retrying

Retry a failed job manually with:

```apex
Queues.job(jobId).retry();
```

Manual retry resets attempt state and moves the job back to runnable work.

#### Removing and Canceling

Use `cancel()` when observability matters:

```apex
Queues.job(jobId).cancel();
```

Use `remove()` when the job and its attempt logs should be deleted:

```apex
Queues.job(jobId).remove();
```

> [!CAUTION]
> `remove()` permanently deletes the job and cascades its `JobRun__c` attempt
> history. This cannot be replaced by later maintenance.

#### Stalled Jobs

A job is considered stale when its active lease expires before completion.
Maintenance can recover stalled work by retrying it or failing it when attempts
are exhausted.

You can also recover one queue explicitly:

```apex
Integer recovered = Queues.of('invoice-sync').recoverStalled();
```

#### Getters

Job handles expose state helpers:

```apex
Queues.Job job = Queues.job(jobId);

Boolean isWaiting = job.isWaiting();
Boolean isDelayed = job.isDelayed();
Boolean isActive = job.isActive();
Boolean isCompleted = job.isCompleted();
Boolean isFailed = job.isFailed();
Boolean isCanceled = job.isCanceled();
Boolean isPaused = job.isPaused();
Boolean isWaitingChildren = job.isWaitingChildren();
```

Use queue stats for aggregate counts.

### Job Scheduler

A job scheduler is a `Schedulable` materializer. It creates `Job__c` rows from a
template; it does not execute processors itself.

#### Repeat Strategies

Use `pattern(...)` for Salesforce cron expressions:

```apex
Queues.Job firstJob = Queues.of('invoice-sync').upsertJobScheduler(
    'nightly-invoice-sync',
    new Queues.RepeatOptions().pattern('0 0 2 * * ?'),
    new Queues.JobTemplate('sync-invoice', 'InvoiceSyncProcessor')
);
```

Use `every(minutes)` for minute-based schedules:

```apex
Queues.Job firstJob = Queues.of('invoice-sync').upsertJobScheduler(
    'invoice-sync-every-15',
    new Queues.RepeatOptions().every(15),
    new Queues.JobTemplate('sync-invoice', 'InvoiceSyncProcessor')
);
```

`every(minutes)` schedulers use one-shot Scheduled Apex wakes and self-reschedule from
durable scheduler state.

#### Repeat Options

`RepeatOptions` supports:

```apex
new Queues.RepeatOptions()
    .every(15)
    .startAt(System.now().addMinutes(15))
    .endAt(System.now().addDays(1))
    .lim(10);
```

`lim(count)` caps lifetime materializations for that scheduler identity.

#### Job Template

The repeat options decide when a scheduler materializes work. `JobTemplate`
decides what job is materialized:

```apex
new Queues.JobTemplate('sync-invoice', 'InvoiceSyncProcessor')
    .data(new Map<String, Object>{ 'source' => 'scheduler' });
```

The constructor values are the generated job name and processor. Use
`data(...)` for the generated job payload.

##### Template Job Options

Use `opts(...)` to apply normal job options to every materialized occurrence:

```apex
new Queues.JobTemplate('sync-invoice', 'InvoiceSyncProcessor')
    .data(new Map<String, Object>{ 'source' => 'scheduler' })
    .opts(
        new Queues.JobOptions()
            .attempts(3)
            .backoff(Queues.Backoff.EXPONENTIAL, 5)
    );
```

Template options may set attempts, priority, parent provenance, backoff, and
other execution behavior. They may not set `jobId(...)`, `delay(...)`, or
`availableAt(...)`; the scheduler owns occurrence ids and timing.

#### Manage Job Schedulers

`upsertJobScheduler(...)` creates or replaces the scheduler with the same
queue-scoped scheduler id:

```apex
Queues.of('invoice-sync').upsertJobScheduler(
    'invoice-sync-queueable',
    new Queues.RepeatOptions().every(15),
    new Queues.JobTemplate('sync-invoice', 'InvoiceSyncProcessor'),
    Queues.ExecutionMode.QUEUEABLE_SERIAL
);
```

Schedulers default to `WORKER`. Pass an execution mode when materialized jobs
should use `QUEUEABLE_SERIAL`, `QUEUEABLE_CONCURRENT`, or `INVOCABLE`.

Read scheduler state:

```apex
Queues.JobScheduler scheduler = Queues.of('invoice-sync')
    .getJobScheduler('invoice-sync-every-15');

Integer count = scheduler.getIterationCount();
Datetime nextRunAt = scheduler.getNextRunAt();
```

Canceling preserves scheduler and job observability:

```apex
Queues.of('invoice-sync').cancelJobScheduler('invoice-sync-every-15');
```

Removing is destructive:

```apex
Queues.of('invoice-sync').removeJobScheduler('invoice-sync-every-15');
```

> [!CAUTION]
> Removing a scheduler deletes its linked jobs and their cascaded attempt
> history. Cancel the scheduler when observability must be preserved.

### Flows

`FlowProducer` inserts dependency graphs atomically. Parents wait in
`WAITING_CHILDREN` until their children resolve.

#### Adding Flows

```apex
Queues.JobNode root = Queues.flowProducer().add(
    new Queues.FlowJob('finish-import', 'invoice-sync', 'FinishImportProcessor')
        .child(
            new Queues.FlowJob('load-lines', 'invoice-sync', 'LoadLinesProcessor')
        )
        .child(
            new Queues.FlowJob('sync-summary', 'invoice-sync', 'SyncSummaryProcessor')
        )
);
```

Graphs are inserted parent-first in waves. Width and depth are caller-controlled
and naturally bounded by Salesforce governor limits. Any insertion failure rolls
back the complete graph.

> [!NOTE]
> The framework adds no arbitrary graph-size limit. The caller is responsible
> for keeping each atomic graph within Salesforce transaction limits.

> [!IMPORTANT]
> Existing flow jobs may only participate in an idempotent replay. Every child
> beneath an existing node must already exist; adding new children to a durable
> graph is rejected and rolls back the complete submission.

Use `FlowOpts` to select one execution mode for the graph:

```apex
Queues.flowProducer().add(
    flow,
    new Queues.FlowOpts().queueable()
);
```

#### Get Flow Tree

`add(...)` returns a `JobNode` tree containing the durable jobs created by the
request. Later, use `getFlow(jobId)` to read the complete durable tree again:

```apex
Queues.JobNode root = Queues.flowProducer().getFlow(jobId);
renderFlow(root);

private static void renderFlow(final Queues.JobNode node) {
    Queues.Job job = node.getJob();
    // Execute reporting, repair, or UI-mapping logic for this job.
    System.debug(job.getId() + ': ' + job.getState());
    for (Queues.JobNode child : node.getChildren()) {
        renderFlow(child);
    }
}
```

`getFlow(...)` takes a Salesforce `Job__c.Id` for the root job and returns the
complete tree visible to the current user. Each node contains a `Queues.Job`
handle plus child nodes.

#### Dependency Policies

By default, failed or canceled children keep the parent in `WAITING_CHILDREN`.
Opt into explicit edge behavior on the child job options:

> [!IMPORTANT]
> Dependency policies evaluate terminal child state, not individual attempts. A
> retryable failed attempt does not fail or release the parent; the policy runs
> only after the child enters `FAILED` or `CANCELED`.

```apex
new Queues.FlowJob('load-lines', 'invoice-sync', 'LoadLinesProcessor')
    .opts(new Queues.JobOptions().failParentOnFailure());
```

Failure policies:

| Method                        | Behavior                                                                                  |
| ----------------------------- | ----------------------------------------------------------------------------------------- |
| `failParentOnFailure()`       | Fails the parent when this child fails.                                                   |
| `continueParentOnFailure()`   | Releases the parent when this child fails, even if other children are still unresolved.   |
| `ignoreDependencyOnFailure()` | Treats only this failed child as resolved; the parent still waits for remaining children. |
| `removeDependencyOnFailure()` | Treats this failed child as resolved and removes the parent link.                         |

Cancellation policies:

| Method                         | Behavior                                                                                     |
| ------------------------------ | -------------------------------------------------------------------------------------------- |
| `cancelParentOnCanceled()`     | Cancels the parent when this child is canceled, even if other children are still unresolved. |
| `failParentOnCanceled()`       | Fails the parent when this child is canceled, even if other children are still unresolved.   |
| `ignoreDependencyOnCanceled()` | Treats only this canceled child as resolved; the parent still waits for remaining children.  |

`opts.parent(...)` is not accepted inside submitted flow graphs. Define
dependencies with `child(...)`.

### Maintenance

#### Recovery

The framework recovers from stale durable state, not from assumptions about
native async visibility. Recovery includes:

- expired `ACTIVE` worker and invocable jobs,
- stale queueable dispatches,
- dropped queueable stack-depth handoffs,
- stale `WAITING_CHILDREN` dependency parents,
- orphaned worker and scheduler native artifacts.

#### Job Retention Policy

Terminal jobs are kept by default. Set `TerminalCleanupPolicy__c` to `DELETE`
on a `QueueDefinition__mdt` record when terminal jobs should be removed by
maintenance.

Use the per-state retention fields to control when each terminal state becomes
eligible:

| Field                       | Applies to  |
| --------------------------- | ----------- |
| `CompletedRetentionDays__c` | `COMPLETED` |
| `FailedRetentionDays__c`    | `FAILED`    |
| `CanceledRetentionDays__c`  | `CANCELED`  |

Blank retention values omit that state from cleanup. `0` means eligible on the
next cleanup cycle; `N` means eligible after `N * 24 * 60 * 60` seconds since
the job was last modified. Editing a terminal job updates `LastModifiedDate`
and extends its retention window.

Cleanup deletes eligible `Job__c` records and their `JobRun__c` history.
`QueueError__c` records are retained and lose their job link if the job is
deleted.

> [!NOTE]
> Retention cleanup is maintenance-owned. It runs from scheduled maintenance or
> from `Queues.runTerminalCleanupMaintenance()`, not at the moment a job
> completes.

#### Schedule Maintenance

Schedule maintenance once per org when you want automatic recovery:

```apex
Queues.scheduleMaintenance();
```

> [!IMPORTANT]
> Maintenance durability is part of the production operating model. Schedule it
> even when normal async processing appears healthy.

The default schedule is hourly: `0 0 * * * ?`.

You can pass a cron expression when the org needs a different hourly-or-slower
cadence:

```apex
Queues.scheduleMaintenance('0 0 0,6,12,18 * * ?');
```

Salesforce Scheduled Apex is intended for hourly-or-slower recurring jobs, so maintenance cron expressions should not be sub-hourly.

`scheduleMaintenance()` is idempotent. It returns the existing scheduled job id
when maintenance is already scheduled. Use these helpers to inspect or remove
the maintenance job:

```apex
Boolean scheduled = Queues.isMaintenanceScheduled();
Integer removed = Queues.unscheduleMaintenance();
```

Most recovery is driven by the scheduled maintenance job. Queue-level
`recoverStalled()` can be used for a specific queue, but it is not a substitute
for org-level maintenance.

Until maintenance is scheduled, stalled-job recovery, queueable dispatch
recovery, dependency repair, scheduler maintenance, and terminal cleanup do not
run automatically.

#### Run Maintenance Manually

Run maintenance manually when you need immediate org-level recovery or cleanup
instead of waiting for the scheduled maintenance job:

```apex
Queues.MaintenanceResult runtime = Queues.runRuntimeMaintenance();
Queues.MaintenanceResult schedulers = Queues.runJobSchedulerMaintenance();
Queues.MaintenanceResult cleanup = Queues.runTerminalCleanupMaintenance();
```

Runtime maintenance recovers stalled jobs, async handoffs, dependencies, and
worker runtime state. Job scheduler maintenance repairs native schedules for
durable job schedulers. Terminal cleanup removes eligible terminal jobs and
their job runs according to queue metadata. Run runtime maintenance first, then
scheduler maintenance, then cleanup, matching the scheduled maintenance chain.

> [!WARNING]
> Manual maintenance performs framework-owned system-mode recovery. Restrict
> access to callers trusted to operate every configured queue.

Each result exposes `total`, `done`, and per-queue counts through
`getCount(queueName)`. A `done` value of `false` means maintenance stopped early
to preserve governor-limit headroom; run it again to continue.

> [!TIP]
> For immediate repair, run runtime maintenance first and repeat any phase
> while its result has `done == false`.

### Metrics

Queues expose job counts by state for a queue:

```apex
Queues.QueueStats stats = Queues.of('invoice-sync').getStats();

Integer total = stats.total;
Integer waiting = stats.waiting;
Integer failed = stats.failed;
```

Convenience methods return the same job counts from the queue handle:

```apex
Integer waiting = Queues.of('invoice-sync').getWaitingCount();
Map<Queues.State, Integer> counts = Queues.of('invoice-sync').getJobCounts();
```

#### Operational Errors

`QueueError__c` records framework operational failures: worker scheduling,
queueable dispatch, invocable enqueue, scheduler maintenance, runtime
maintenance, and other recovery paths.

Processor failures are not queue errors. They are stored on the durable job and
attempt history through `Job__c`, `JobRun__c`, `FailedReason__c`, and final job
state.

Framework error telemetry is usually published through `QueueErrorEvent__e` and
persisted asynchronously into `QueueError__c`, so operational logging does not
pollute normal processor execution transactions.

### Concurrency and Parallelism

Concurrency and parallelism describe different properties:

- **Concurrency** means multiple jobs are in progress during overlapping periods.
- **Parallelism** means multiple processor transactions are executing at the
  same instant.

Apex Queue can request concurrency, but Salesforce decides when submitted async
work actually runs. Concurrent jobs may therefore execute in parallel, take
turns, or wait behind other org workloads.

| Mode                   | Dispatching model                                                                                                                         | Claiming model                                                                                                     | Execution model                                                                                                                                           |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WORKER`               | **Serial per queue.** One Batch Apex worker lane is dispatched for each queue. Multiple queue names request multiple independent workers. | **Bulk claim.** `batch.start()` locks and leases up to `BatchClaimSize__c` due jobs.                               | **Serial per worker.** Fixed scope size `1`; every job runs in a clean `batch.execute()` transaction, but scopes within one worker are treated as serial. |
| `QUEUEABLE_SERIAL`     | **Serial submission.** The dispatcher submits one executor at a time and continues in submission order.                                   | **One job per submission.** The dispatcher marks the submitted job `ACTIVE` and assigns ownership to its executor. | **Potentially concurrent execution.** One job runs per queueable transaction; ordered submission does not guarantee start or finish order.                |
| `QUEUEABLE_CONCURRENT` | **Concurrent submission.** The dispatcher submits independent executors aggressively, using future bridges where needed.                  | **One job per submission.** Each submitted executor independently receives durable `ACTIVE` ownership of one job.  | **Concurrent execution.** Independent queueable transactions can overlap, while Salesforce controls actual parallelism.                                   |
| `INVOCABLE`            | **Independent scheduled dispatch.** Flow Scheduled Paths wake due jobs for the Claim Flow.                                                | **Bulk claim.** The Claim Flow locks and leases up to `100` jobs per transaction.                                  | **Concurrent execution.** The Execute Flow receives one `ACTIVE` job per clean `INVOCABLE_ACTION` transaction; transactions can overlap.                  |

#### Creating Parallelism

True parallelism exists only when processor transactions execute at the same
instant. Apex Queue creates independent concurrent work, but Salesforce controls
whether and when that work actually runs in parallel.

Use `QUEUEABLE_CONCURRENT` or `INVOCABLE` when independent jobs should be
submitted and executed **concurrently**. For worker-mode **parallelism**, split
independent work across separate queues as explicit worker shards:

```apex
Queues.of('invoice-sync-1').addBulk(firstShard);
Queues.of('invoice-sync-2').addBulk(secondShard);
Queues.of('invoice-sync-3').addBulk(thirdShard);
```

Each queue owns one worker lane. Salesforce admits up to `5` Batch Apex jobs
from the Flex Queue into queued or active processing at a time; additional
workers remain `Holding`. That creates an opportunity for parallel execution,
not a guarantee that five processor transactions run simultaneously. The
capacity is shared with unrelated Batch Apex work in the org. Use
[Worker Priority](#worker-priority) to manually reorder a `Holding` framework
worker when needed; reordering cannot force Salesforce to admit it and may delay
unrelated Batch Apex work.

> [!IMPORTANT]
> More concurrency does not always improve throughput. Record-lock contention,
> downstream throttling, and shared Salesforce async capacity can make excessive
> concurrency slower or less reliable.

Measure observed parallelism from overlapping `JobRun__c.StartedAt__c` and
`JobRun__c.FinishedAt__c` ranges. Distinct `AsyncApexJobId__c` values identify
different native async executions, but do not by themselves prove that those
executions overlapped.

## Architecture

### Durable State

The framework uses Salesforce records as durable state:

| Record                                 | Role                                                                       |
| -------------------------------------- | -------------------------------------------------------------------------- |
| `Job__c`                               | Source of truth for job state, data, retry policy, timing, and provenance. |
| `JobRun__c`                            | Master-detail attempt log for each processor execution.                    |
| `QueueRuntime__c`                      | Worker ownership, pause state, and wake planning for `WORKER`.             |
| `QueueableDispatch__c`                 | Ephemeral queueable handoff envelope.                                      |
| `JobScheduler__c`                      | Scheduler cadence, template, execution mode, and next run state.           |
| `QueueError__c` / `QueueErrorEvent__e` | Framework operational telemetry.                                           |

### Recovery Model

Salesforce async artifacts can be delayed, hidden, aborted, or not rolled back
with DML. Apex Queue keeps durable records as the source of truth and uses
maintenance to reconcile native artifacts back to that state.

Admin commands are transaction-boundary atomic, not catch-safe savepoint
wrappers. Some paths intentionally interleave DML with native async side
effects, so rollback-based compensation would create mismatches that maintenance
is designed to repair.

### Delete vs Cancel

Delete/remove paths are destructive. Cancel paths preserve records for
observability:

| Operation                    | Result                                                 |
| ---------------------------- | ------------------------------------------------------ |
| `Queues.job(jobId).remove()` | Deletes the job and cascades `JobRun__c`.              |
| `Queues.job(jobId).cancel()` | Marks cancelable future work `CANCELED`.               |
| `queue.drain()`              | Deletes drainable jobs.                                |
| `queue.cancel()`             | Cancels drainable jobs.                                |
| `removeJobScheduler(...)`    | Deletes scheduler and linked jobs.                     |
| `cancelJobScheduler(...)`    | Deactivates scheduler and cancels pending linked jobs. |
| Terminal cleanup             | Deletes expired terminal jobs by queue policy.         |

### Security Model

Public facade operations use sharing and repository access modes to honor the
current caller where appropriate. Internal maintenance and async recovery use
explicit system-mode paths when the framework must reconcile durable state
independently of an end user's record visibility.

`QueuesUser` can create and manage caller-owned jobs and schedulers. It does not
grant cross-owner job or scheduler visibility, or worker reordering. Queue
runtimes are shared coordination records, so `QueuesUser` can access every
`QueueRuntime__c` record. Job-level commands authorize the caller against the
job first, then coordinate that shared runtime.

Application code should depend on `Queues.cls` only. Classes under
`classes/internal` are framework internals and can change between releases.

## Documentation

- [Apex Documentation](/docs/index.md).
