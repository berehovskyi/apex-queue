# Queues Class

`APIVERSION: 67`
`STATUS: ACTIVE`

Durable, BullMQ-inspired background job framework for Salesforce Apex. `Queues` is the single
supported public facade: it creates queue handles, job handles, and dependency-graph producers,
and exposes org-level maintenance. All committed work is stored durably in `Job__c` , the
framework's source of truth; native async artifacts (Batch, Queueable, Scheduled Apex, Flow)
are treated as wake or handoff hints, never as the source of truth.

Application code should depend only on this class and its nested value objects. Classes under
`classes/internal` are framework internals and may change between releases.

**See** Queues.Queue

**See** Queues.Job

**See** Queues.FlowProducer

**See** Queues.JobProcessor

## Example

// Enqueue a job, then inspect and operate on it later
Queues.Job job = Queues.of('invoice-sync').add(
'sync-invoice',
'InvoiceSyncProcessor',
new Map<String, Object>{ 'invoiceId' => 'INV-001' }
);
Id jobId = job.getId();
Queues.State state = Queues.job(jobId).getState();

## Methods

### `of(queueName)`

Creates a lightweight handle to a named queue. The queue must already exist as an active
`QueueDefinition__mdt` record before jobs can be added.

#### Signature

```apex
public static Queue of(String queueName);
```

#### Parameters

| Name      | Type   | Description                                          |
| --------- | ------ | ---------------------------------------------------- |
| queueName | String | public queue name declared in QueueDefinition\_\_mdt |

#### Return Type

**Queue**

queue handle bound to queueName

#### Throws

QueueException: when queueName is blank

#### Example

```apex
Queues.Queue queue = Queues.of('invoice-sync');
```

---

### `job(jobId)`

Creates a lightweight handle to an existing durable job by its Salesforce record id. The
handle is inert until a method is called; it does not verify that the job exists up front.

#### Signature

```apex
public static Job job(Id jobId);
```

#### Parameters

| Name  | Type | Description                               |
| ----- | ---- | ----------------------------------------- |
| jobId | Id   | Salesforce Job\_\_c.Id of an existing job |

#### Return Type

**Job**

job handle bound to jobId

#### Throws

QueueException: when jobId is null

#### Example

```apex
Queues.Job job = Queues.job(existingJobId);
```

---

### `flowProducer()`

Creates a `FlowProducer` used to insert atomic dependency graphs whose parents wait for
their children to resolve.

#### Signature

```apex
public static FlowProducer flowProducer();
```

#### Return Type

**FlowProducer**

a new FlowProducer

#### Example

```apex
Queues.JobNode root = Queues.flowProducer().add(
new Queues.FlowJob('finish-import', 'invoice-sync', 'FinishImportProcessor')
.child(new Queues.FlowJob('load-lines', 'invoice-sync', 'LoadLinesProcessor'))
);
```

---

### `runRuntimeMaintenance()`

Runs one bounded runtime-recovery sweep across queues in the current transaction: stalled
jobs, expired leases, stale queueable dispatches, dropped async handoffs, stale dependency
parents, and orphaned worker artifacts. Uses framework-owned system-mode persistence, so
restrict access to callers trusted to operate every configured queue.

A `done` value of `false` means the sweep stopped early to preserve governor headroom. Do not
re-invoke it in the same transaction: governor limits are per-transaction and looping cannot
replenish them. Let the next scheduled maintenance run, or call it again from a fresh
transaction, to continue.

#### Signature

```apex
public static MaintenanceResult runRuntimeMaintenance();
```

#### Return Type

**MaintenanceResult**

result whose done is false when the sweep stopped early and must be continued in a later transaction

---

### `runJobSchedulerMaintenance()`

Runs one bounded job-scheduler repair sweep in the current transaction: it reconciles durable
`JobScheduler__c` state with native Scheduled Apex artifacts. Repair-only; it never
materializes occurrences itself. The scheduled maintenance job instead runs this through a
queueable handoff so scheduler repair gets an independent governor budget; calling this method
directly does not.

#### Signature

```apex
public static MaintenanceResult runJobSchedulerMaintenance();
```

#### Return Type

**MaintenanceResult**

result whose done is false when the sweep stopped early and must be continued in a later transaction

---

### `runTerminalCleanupMaintenance()`

Runs one bounded terminal-cleanup sweep in the current transaction. Queues whose
`QueueDefinition__mdt.TerminalCleanupPolicy__c` is `DELETE` remove eligible terminal
`Job__c` rows according to their per-state retention fields. `JobRun__c` rows are removed
by master-detail cascade.

#### Signature

```apex
public static MaintenanceResult runTerminalCleanupMaintenance();
```

#### Return Type

**MaintenanceResult**

result whose done is false when cleanup stopped early and must be continued in a later transaction

---

### `scheduleMaintenance()`

Schedules the recurring maintenance job at the default hourly cadence ( `0 0 * * * ?` ).
Idempotent: returns the existing scheduled job id when maintenance is already scheduled.

#### Signature

```apex
public static Id scheduleMaintenance();
```

#### Return Type

**Id**

CronTrigger id of the scheduled (or already-scheduled) maintenance job

#### Example

```apex
Queues.scheduleMaintenance();
```

---

### `scheduleMaintenance(cronExpression)`

Schedules the recurring maintenance job with a custom cron expression. Idempotent: returns
the existing scheduled job id when maintenance is already scheduled, without re-applying the
expression. As an operational recommendation, prefer hourly-or-slower cadences; sub-hourly
expressions are not enforced by the framework.

#### Signature

```apex
public static Id scheduleMaintenance(String cronExpression);
```

#### Parameters

| Name           | Type   | Description                                            |
| -------------- | ------ | ------------------------------------------------------ |
| cronExpression | String | Salesforce cron expression for the maintenance cadence |

#### Return Type

**Id**

CronTrigger id of the scheduled (or already-scheduled) maintenance job

#### Throws

QueueException: when cronExpression is blank

#### Example

```apex
Queues.scheduleMaintenance('0 0 0,6,12,18 * * ?');
```

---

### `unscheduleMaintenance()`

Aborts the scheduled maintenance job if present.

#### Signature

```apex
public static Integer unscheduleMaintenance();
```

#### Return Type

**Integer**

number of maintenance schedules aborted (0 when none was scheduled)

---

### `isMaintenanceScheduled()`

Reports whether the recurring maintenance job is currently scheduled.

#### Signature

```apex
public static Boolean isMaintenanceScheduled();
```

#### Return Type

**Boolean**

true when a maintenance schedule exists

## Classes

### MaintenanceResult Class

Outcome of a maintenance sweep. `total` is the count of items processed,
`done` reports whether the sweep finished or stopped early to preserve governor headroom,
and `countsByQueueName` breaks the total down per queue.

#### Example

Queues.MaintenanceResult result = Queues.runRuntimeMaintenance();
Integer invoiceSync = result.getCount('invoice-sync');
Boolean finished = result.done;

#### Properties

##### `total`

Total number of items processed across all queues.

###### Signature

```apex
public total;
```

###### Type

Integer

---

##### `done`

`false` when the sweep stopped early for governor headroom and should be re-run.

###### Signature

```apex
public done;
```

###### Type

Boolean

---

##### `countsByQueueName`

Per-queue breakdown of processed items, keyed by queue name.

###### Signature

```apex
public countsByQueueName;
```

###### Type

Map<String,Integer>

#### Constructors

##### `MaintenanceResult()`

Creates an empty result with `total` `0` and `done` `true` .

###### Signature

```apex
public MaintenanceResult();
```

#### Methods

##### `add(queueName, count)`

Adds processed items for a queue, updating `total` and the per-queue count.

###### Signature

```apex
public MaintenanceResult add(String queueName, Integer count);
```

###### Parameters

| Name      | Type    | Description                                    |
| --------- | ------- | ---------------------------------------------- |
| queueName | String  | queue the items belong to; ignored when blank  |
| count     | Integer | number of items to add; treated as 0 when null |

###### Return Type

**MaintenanceResult**

this result for fluent chaining

---

##### `markIncomplete()`

Marks the sweep as stopped early so callers know to run maintenance again.

###### Signature

```apex
public MaintenanceResult markIncomplete();
```

###### Return Type

**MaintenanceResult**

this result for fluent chaining

---

##### `getCount(queueName)`

Returns the number of items processed for a single queue.

###### Signature

```apex
public Integer getCount(String queueName);
```

###### Parameters

| Name      | Type   | Description           |
| --------- | ------ | --------------------- |
| queueName | String | queue name to look up |

###### Return Type

**Integer**

processed count for queueName, or 0 when none

### WorkerStatus Class

Read-only status of a queue's current framework-owned Batch Apex worker, as seen in the
Salesforce Flex Queue. Returned by `Queue.getWorkerStatus()` and used to decide whether the
worker can be reordered.

**See** Queues.Queue

#### Constructors

##### `WorkerStatus(queueName, jobId, status)`

###### Signature

```apex
public WorkerStatus(String queueName, Id jobId, String status);
```

###### Parameters

| Name      | Type   | Description                                                          |
| --------- | ------ | -------------------------------------------------------------------- |
| queueName | String | queue this worker serves                                             |
| jobId     | Id     | AsyncApexJob id of the current worker, or null when there is none    |
| status    | String | native AsyncApexJob.Status of the worker, or null when there is none |

#### Methods

##### `getQueueName()`

###### Signature

```apex
public String getQueueName();
```

###### Return Type

**String**

queue this worker serves

---

##### `getJobId()`

###### Signature

```apex
public Id getJobId();
```

###### Return Type

**Id**

AsyncApexJob id of the current framework worker, or null when none exists

---

##### `getStatus()`

###### Signature

```apex
public String getStatus();
```

###### Return Type

**String**

raw native AsyncApexJob.Status, or null when no worker exists

---

##### `hasWorker()`

###### Signature

```apex
public Boolean hasWorker();
```

###### Return Type

**Boolean**

true when a framework-owned worker currently exists for the queue

---

##### `isHolding()`

Reports whether the worker is waiting in the Flex Queue and therefore reorderable.

###### Signature

```apex
public Boolean isHolding();
```

###### Return Type

**Boolean**

true when the worker status is Holding

### Queue Class

Handle to a named queue. Adds jobs, schedules recurring work, and performs queue-level
administration (pause, resume, wake, drain, cancel, stalled recovery, worker reordering,
stats). Obtain one with `Queues.of(queueName)` . The backing `QueueDefinition__mdt` supplies
default attempts, backoff, lease, and claim policy that individual jobs may override.

**See** [Queues](Queues.md)

#### Example

Queues.Queue queue = Queues.of('invoice-sync');
queue.add('sync-invoice', 'InvoiceSyncProcessor', new Map<String, Object>{ 'id' => 'INV-001' });
Integer waiting = queue.getWaitingCount();

#### Constructors

##### `Queue(queueName)`

###### Signature

```apex
public Queue(String queueName);
```

###### Parameters

| Name      | Type   | Description                                          |
| --------- | ------ | ---------------------------------------------------- |
| queueName | String | public queue name declared in QueueDefinition\_\_mdt |

###### Throws

QueueException: when queueName is blank

#### Methods

##### `getName()`

###### Signature

```apex
public String getName();
```

###### Return Type

**String**

this queue's name

---

##### `add(name, processor, data)`

Adds a single job with default options on the default `WORKER` lane.

###### Signature

```apex
public Job add(String name, String processor, Object data);
```

###### Parameters

| Name      | Type   | Description                                                           |
| --------- | ------ | --------------------------------------------------------------------- |
| name      | String | display name of the job                                               |
| processor | String | Processor**mdt.ProcessorClass**c allowlisted to run the job           |
| data      | Object | JSON-serializable payload exposed to the processor through ctx.data() |

###### Return Type

**Job**

handle to the created (or idempotently reused) job

###### Throws

ConfigurationException: when the queue is missing or inactive, or the processor is not an active mapping

###### Example

```apex
Queues.Job job = Queues.of('invoice-sync').add(
'sync-invoice',
'InvoiceSyncProcessor',
new Map<String, Object>{ 'invoiceId' => 'INV-001' }
);
```

---

##### `add(name, processor, data, opts)`

Adds a single job with per-job options on the default `WORKER` lane.

###### Signature

```apex
public Job add(String name, String processor, Object data, JobOptions opts);
```

###### Parameters

| Name      | Type       | Description                                                       |
| --------- | ---------- | ----------------------------------------------------------------- |
| name      | String     | display name of the job                                           |
| processor | String     | allowlisted processor class name                                  |
| data      | Object     | JSON-serializable payload                                         |
| opts      | JobOptions | per-job options such as jobId, attempts, priority, delay, backoff |

###### Return Type

**Job**

handle to the created (or idempotently reused) job

###### Throws

ConfigurationException: when the queue or processor is not configured or inactive

###### Example

```apex
Queues.of('invoice-sync').add(
'sync-invoice', 'InvoiceSyncProcessor', data,
new Queues.JobOptions().jobId('invoice:INV-001').attempts(5)
);
```

---

##### `add(name, processor, data, opts)`

Adds a single job with bulk options that select the execution lane.

###### Signature

```apex
public Job add(String name, String processor, Object data, BulkJobOptions opts);
```

###### Parameters

| Name      | Type           | Description                                                                  |
| --------- | -------------- | ---------------------------------------------------------------------------- |
| name      | String         | display name of the job                                                      |
| processor | String         | allowlisted processor class name                                             |
| data      | Object         | JSON-serializable payload                                                    |
| opts      | BulkJobOptions | bulk options selecting the execution mode and optional dispatcher dedupe key |

###### Return Type

**Job**

handle to the created (or idempotently reused) job

###### Throws

ConfigurationException: when the queue or processor is not configured or inactive

---

##### `add(name, processor, data, jobOpts, bulkOpts)`

Adds a single job with both per-job and bulk options.

###### Signature

```apex
public Job add(String name, String processor, Object data, JobOptions jobOpts, BulkJobOptions bulkOpts);
```

###### Parameters

| Name      | Type           | Description                               |
| --------- | -------------- | ----------------------------------------- |
| name      | String         | display name of the job                   |
| processor | String         | allowlisted processor class name          |
| data      | Object         | JSON-serializable payload                 |
| jobOpts   | JobOptions     | per-job options                           |
| bulkOpts  | BulkJobOptions | bulk options selecting the execution mode |

###### Return Type

**Job**

handle to the created (or idempotently reused) job

###### Throws

ConfigurationException: when the queue or processor is not configured or inactive

---

##### `addBulk(jobs)`

Atomically adds several jobs on the default `WORKER` lane. If any job cannot be inserted,
the whole request rolls back. Salesforce governor limits still bound a single request.

###### Signature

```apex
public List<Job> addBulk(List<JobData> jobs);
```

###### Parameters

| Name | Type          | Description     |
| ---- | ------------- | --------------- |
| jobs | List<JobData> | jobs to enqueue |

###### Return Type

**List<Job>**

handles to the created (or idempotently reused) jobs, in request order

###### Throws

ConfigurationException: when the queue or any processor is not configured or inactive

---

##### `addBulk(jobs, opts)`

Atomically adds several jobs with bulk options selecting the execution lane. The request
is execution-mode homogeneous: a reused job id resolving to a different mode is rejected.
If any job cannot be inserted, the whole request rolls back.

###### Signature

```apex
public List<Job> addBulk(List<JobData> jobs, BulkJobOptions opts);
```

###### Parameters

| Name | Type           | Description                                                                  |
| ---- | -------------- | ---------------------------------------------------------------------------- |
| jobs | List<JobData>  | jobs to enqueue                                                              |
| opts | BulkJobOptions | bulk options selecting the execution mode and optional dispatcher dedupe key |

###### Return Type

**List<Job>**

handles to the created (or idempotently reused) jobs, in request order

###### Throws

QueueException: when jobs or opts is null, or a duplicate job id maps to a different execution mode

ConfigurationException: when the queue or any processor is not configured or inactive

###### Example

```apex
List<Queues.Job> jobs = Queues.of('invoice-sync').addBulk(
new List<Queues.JobData>{
new Queues.JobData('sync', 'InvoiceSyncProcessor', new Map<String, Object>{ 'id' => 'A' }),
new Queues.JobData('sync', 'InvoiceSyncProcessor', new Map<String, Object>{ 'id' => 'B' })
},
new Queues.BulkJobOptions().queueable()
);
```

---

##### `pause()`

Pauses the queue. New work can still be added but no worker is scheduled and any pending
worker wake is canceled until the queue is resumed.

###### Signature

```apex
public Queue pause();
```

###### Return Type

**Queue**

this queue for fluent chaining

###### Throws

ConfigurationException: when the queue is not configured

---

##### `resume()`

Resumes a paused queue and schedules a worker when due work exists.

###### Signature

```apex
public Queue resume();
```

###### Return Type

**Queue**

this queue for fluent chaining

###### Throws

ConfigurationException: when the queue is not configured or is inactive

---

##### `wake()`

Wakes the queue now, scheduling a worker for any currently due work.

###### Signature

```apex
public Integer wake();
```

###### Return Type

**Integer**

1 when a worker was scheduled, otherwise 0

###### Throws

ConfigurationException: when the queue is not configured or is inactive

---

##### `recoverStalled()`

Recovers stalled jobs for this queue whose active lease has expired, retrying them or
failing them when attempts are exhausted. Bounded-progress; not a substitute for
org-level scheduled maintenance.

###### Signature

```apex
public Integer recoverStalled();
```

###### Return Type

**Integer**

number of stalled jobs recovered

###### Throws

ConfigurationException: when the queue is not configured

---

##### `cancel()`

Marks drainable jobs ( `WAITING` , `DELAYED` , `PAUSED` , `WAITING_CHILDREN` ) as `CANCELED` ,
preserving the records for observability. Bounded-progress; run again while it keeps
returning a full batch.

###### Signature

```apex
public Integer cancel();
```

###### Return Type

**Integer**

number of jobs canceled in this call

###### Throws

ConfigurationException: when the queue is not configured

---

##### `drain()`

Deletes drainable jobs and cascades their `JobRun__c` attempt history. Destructive and
not recoverable by later maintenance; use `cancel()` to preserve records. Bounded-progress.

###### Signature

```apex
public Integer drain();
```

###### Return Type

**Integer**

number of jobs deleted in this call

###### Throws

ConfigurationException: when the queue is not configured

---

##### `getWorkerStatus()`

Returns the status of this queue's current framework-owned Batch Apex worker. Read-only
and does not require the worker-management permission.

###### Signature

```apex
public WorkerStatus getWorkerStatus();
```

###### Return Type

**WorkerStatus**

worker status; hasWorker() is false when no worker exists

###### Throws

ConfigurationException: when the queue is not configured

---

##### `moveWorkerToFront()`

Moves this queue's current worker to the front of the Flex Queue. Only valid while the
worker is `Holding` ; the framework never promotes workers automatically.

###### Signature

```apex
public Boolean moveWorkerToFront();
```

###### Return Type

**Boolean**

true only when Salesforce reordered the worker; false when no worker exists, it is not holding, it already left the Flex Queue, or another transaction wins the race

###### Throws

QueueException: when the caller lacks the QueuesManageWorkers custom permission

ConfigurationException: when the queue is not configured

---

##### `moveWorkerToEnd()`

Moves this queue's current worker to the end of the Flex Queue. Only valid while the
worker is `Holding` .

###### Signature

```apex
public Boolean moveWorkerToEnd();
```

###### Return Type

**Boolean**

true only when Salesforce reordered the worker; false otherwise

###### Throws

QueueException: when the caller lacks the QueuesManageWorkers custom permission

ConfigurationException: when the queue is not configured

---

##### `getStats()`

Returns exact aggregate statistics for the queue: pause state, active worker count, and
job counts by state.

###### Signature

```apex
public QueueStats getStats();
```

###### Return Type

**QueueStats**

queue statistics snapshot

###### Throws

ConfigurationException: when the queue is not configured

###### Example

```apex
Queues.QueueStats stats = Queues.of('invoice-sync').getStats();
Integer failed = stats.failed;
```

---

##### `getJobCounts()`

Returns job counts keyed by every state.

###### Signature

```apex
public Map<State,Integer> getJobCounts();
```

###### Return Type

**Map<State,Integer>**

map of each State to its job count

###### Throws

ConfigurationException: when the queue is not configured

---

##### `getJobCounts(states)`

Returns job counts for the requested states only.

###### Signature

```apex
public Map<State,Integer> getJobCounts(Set<State> states);
```

###### Parameters

| Name   | Type       | Description                                                    |
| ------ | ---------- | -------------------------------------------------------------- |
| states | Set<State> | states to include; null or empty returns counts for all states |

###### Return Type

**Map<State,Integer>**

map of each requested State to its job count

###### Throws

ConfigurationException: when the queue is not configured

---

##### `getWaitingCount()`

###### Signature

```apex
public Integer getWaitingCount();
```

###### Return Type

**Integer**

number of WAITING jobs in the queue

###### Throws

ConfigurationException: when the queue is not configured

---

##### `getDelayedCount()`

###### Signature

```apex
public Integer getDelayedCount();
```

###### Return Type

**Integer**

number of DELAYED jobs in the queue

###### Throws

ConfigurationException: when the queue is not configured

---

##### `getActiveCount()`

###### Signature

```apex
public Integer getActiveCount();
```

###### Return Type

**Integer**

number of ACTIVE jobs in the queue

###### Throws

ConfigurationException: when the queue is not configured

---

##### `getCompletedCount()`

###### Signature

```apex
public Integer getCompletedCount();
```

###### Return Type

**Integer**

number of COMPLETED jobs in the queue

###### Throws

ConfigurationException: when the queue is not configured

---

##### `getFailedCount()`

###### Signature

```apex
public Integer getFailedCount();
```

###### Return Type

**Integer**

number of FAILED jobs in the queue

###### Throws

ConfigurationException: when the queue is not configured

---

##### `getPausedCount()`

###### Signature

```apex
public Integer getPausedCount();
```

###### Return Type

**Integer**

number of PAUSED jobs in the queue

###### Throws

ConfigurationException: when the queue is not configured

---

##### `getWaitingChildrenCount()`

###### Signature

```apex
public Integer getWaitingChildrenCount();
```

###### Return Type

**Integer**

number of WAITING_CHILDREN jobs in the queue

###### Throws

ConfigurationException: when the queue is not configured

---

##### `getCanceledCount()`

###### Signature

```apex
public Integer getCanceledCount();
```

###### Return Type

**Integer**

number of CANCELED jobs in the queue

###### Throws

ConfigurationException: when the queue is not configured

---

##### `count()`

###### Signature

```apex
public Integer count();
```

###### Return Type

**Integer**

total number of jobs in the queue across all states

###### Throws

ConfigurationException: when the queue is not configured

---

##### `getJob(jobId)`

Creates a handle to a job by record id. Does not verify queue membership or existence.

###### Signature

```apex
public Job getJob(Id jobId);
```

###### Parameters

| Name  | Type | Description            |
| ----- | ---- | ---------------------- |
| jobId | Id   | Salesforce Job\_\_c.Id |

###### Return Type

**Job**

job handle bound to jobId

###### Throws

QueueException: when jobId is null

---

##### `getJobState(jobId)`

Returns the current durable state of a job.

###### Signature

```apex
public State getJobState(Id jobId);
```

###### Parameters

| Name  | Type | Description            |
| ----- | ---- | ---------------------- |
| jobId | Id   | Salesforce Job\_\_c.Id |

###### Return Type

**State**

current state of the job

###### Throws

JobNotFoundException: when no job exists for jobId

---

##### `upsertJobScheduler(jobSchedulerId, repeatOpts)`

Creates or replaces a `WORKER` -mode scheduler that materializes a job from a default
template named after the scheduler id.

###### Signature

```apex
public Job upsertJobScheduler(String jobSchedulerId, RepeatOptions repeatOpts);
```

###### Parameters

| Name           | Type          | Description                                                        |
| -------------- | ------------- | ------------------------------------------------------------------ |
| jobSchedulerId | String        | queue-scoped scheduler identity; reused to replace an existing one |
| repeatOpts     | RepeatOptions | cadence: pattern(...) for cron or every(...) for minute-based      |

###### Return Type

**Job**

handle to the first materialized occurrence job

###### Throws

ConfigurationException: when the queue is not configured, is inactive, or the repeat options are invalid

InvalidOperationException: when an existing scheduler has too many pending jobs to replace safely

---

##### `upsertJobScheduler(jobSchedulerId, repeatOpts, jobTemplate)`

Creates or replaces a `WORKER` -mode scheduler with an explicit job template.

###### Signature

```apex
public Job upsertJobScheduler(String jobSchedulerId, RepeatOptions repeatOpts, JobTemplate jobTemplate);
```

###### Parameters

| Name           | Type          | Description                                                    |
| -------------- | ------------- | -------------------------------------------------------------- |
| jobSchedulerId | String        | queue-scoped scheduler identity                                |
| repeatOpts     | RepeatOptions | cadence: pattern(...) or every(...)                            |
| jobTemplate    | JobTemplate   | template describing the job name, processor, data, and options |

###### Return Type

**Job**

handle to the first materialized occurrence job

###### Throws

ConfigurationException: when the queue, processor, repeat options, or template options are invalid

InvalidOperationException: when an existing scheduler has too many pending jobs to replace safely

---

##### `upsertJobScheduler(jobSchedulerId, repeatOpts, jobTemplate, executionMode)`

Creates or replaces a scheduler with an explicit execution mode. Re-upsert keeps the
scheduler identity, cancels pending jobs from the old schedule, aborts old native
schedules, resets durable state from the new options, and advances the monotonic
iteration without resetting the lifetime `lim(...)` counter.

###### Signature

```apex
public Job upsertJobScheduler(String jobSchedulerId, RepeatOptions repeatOpts, JobTemplate jobTemplate, ExecutionMode executionMode);
```

###### Parameters

| Name           | Type          | Description                                                    |
| -------------- | ------------- | -------------------------------------------------------------- |
| jobSchedulerId | String        | queue-scoped scheduler identity                                |
| repeatOpts     | RepeatOptions | cadence: pattern(...) or every(...)                            |
| jobTemplate    | JobTemplate   | template describing the job name, processor, data, and options |
| executionMode  | ExecutionMode | lane the materialized occurrences run on                       |

###### Return Type

**Job**

handle to the first materialized occurrence job

###### Throws

ConfigurationException: when the queue, processor, repeat options, or template options are invalid

InvalidOperationException: when an existing scheduler has too many pending jobs to replace safely

###### Example

```apex
Queues.of('invoice-sync').upsertJobScheduler(
'nightly-sync',
new Queues.RepeatOptions().pattern('0 0 2 * * ?'),
new Queues.JobTemplate('sync', 'InvoiceSyncProcessor'),
Queues.ExecutionMode.QUEUEABLE_SERIAL
);
```

---

##### `removeJobScheduler(jobSchedulerId)`

Removes a scheduler and its linked jobs, cascading their attempt history, and aborts the
native schedule. Destructive; use `cancelJobScheduler(...)` to preserve observability.

###### Signature

```apex
public Boolean removeJobScheduler(String jobSchedulerId);
```

###### Parameters

| Name           | Type   | Description                     |
| -------------- | ------ | ------------------------------- |
| jobSchedulerId | String | queue-scoped scheduler identity |

###### Return Type

**Boolean**

true when the scheduler was fully removed; false when none existed, or when the scheduler was deactivated but bounded cleanup did not finish and another call (or maintenance) is required

###### Throws

InvalidOperationException: when the scheduler has ACTIVE linked jobs that block hard removal

---

##### `cancelJobScheduler(jobSchedulerId)`

Cancels a scheduler: aborts native schedules, deactivates it, and marks pending linked
jobs `CANCELED` while preserving all records. A canceled scheduler may later be
re-upserted with the same identity.

###### Signature

```apex
public Boolean cancelJobScheduler(String jobSchedulerId);
```

###### Parameters

| Name           | Type   | Description                     |
| -------------- | ------ | ------------------------------- |
| jobSchedulerId | String | queue-scoped scheduler identity |

###### Return Type

**Boolean**

true when cancellation fully completed; false when none existed, there was nothing to cancel, or the scheduler was deactivated but bounded cleanup did not finish and another call (or maintenance) is required

---

##### `getJobScheduler(jobSchedulerId)`

Reads a single scheduler's durable state.

###### Signature

```apex
public JobScheduler getJobScheduler(String jobSchedulerId);
```

###### Parameters

| Name           | Type   | Description                     |
| -------------- | ------ | ------------------------------- |
| jobSchedulerId | String | queue-scoped scheduler identity |

###### Return Type

**JobScheduler**

scheduler value object, or null when none exists

---

##### `getJobSchedulers()`

Lists up to the first `25` schedulers for this queue, ascending by name.

###### Signature

```apex
public List<JobScheduler> getJobSchedulers();
```

###### Return Type

**List<JobScheduler>**

schedulers for this queue

---

##### `getJobSchedulers(startIndex, endIndex, ascending)`

Lists schedulers for this queue within an inclusive index window.

###### Signature

```apex
public List<JobScheduler> getJobSchedulers(Integer startIndex, Integer endIndex, Boolean ascending);
```

###### Parameters

| Name       | Type    | Description                                           |
| ---------- | ------- | ----------------------------------------------------- |
| startIndex | Integer | zero-based start index, clamped to 0                  |
| endIndex   | Integer | zero-based inclusive end index                        |
| ascending  | Boolean | true to order by name ascending, false for descending |

###### Return Type

**List<JobScheduler>**

schedulers in the requested window

---

##### `getJobSchedulersCount()`

###### Signature

```apex
public Integer getJobSchedulersCount();
```

###### Return Type

**Integer**

number of schedulers defined for this queue

### FlowProducer Class

Inserts atomic dependency graphs whose parents wait in `WAITING_CHILDREN` until their
children resolve. Graphs are inserted parent-first in waves; any insertion failure rolls back
the complete graph. Structural cycles, reused `FlowJob` instances, and duplicate queue-scoped
job ids within one graph are rejected. Existing jobs referenced by a graph may only
participate in an idempotent replay; adding new children to an existing node is rejected.
Obtain one with `Queues.flowProducer()` .

**See** Queues.FlowJob

**See** Queues.JobNode

#### Example

Queues.JobNode root = Queues.flowProducer().add(
new Queues.FlowJob('finish-import', 'invoice-sync', 'FinishImportProcessor')
.child(new Queues.FlowJob('load-lines', 'invoice-sync', 'LoadLinesProcessor'))
.child(new Queues.FlowJob('sync-summary', 'invoice-sync', 'SyncSummaryProcessor'))
);

#### Methods

##### `add(flow)`

Atomically inserts one dependency graph on the default `WORKER` lane.

###### Signature

```apex
public JobNode add(FlowJob flow);
```

###### Parameters

| Name | Type    | Description                        |
| ---- | ------- | ---------------------------------- |
| flow | FlowJob | root flow job describing the graph |

###### Return Type

**JobNode**

root node of the created durable job tree

###### Throws

QueueException: when the graph has cycles, reused FlowJob instances, duplicate queue-scoped job ids, an , or tries to add new children to an existing job

ConfigurationException: when a queue or processor in the graph is not configured or inactive

---

##### `add(flow, opts)`

Atomically inserts one dependency graph on the lane selected by `opts` .

###### Signature

```apex
public JobNode add(FlowJob flow, FlowOpts opts);
```

###### Parameters

| Name | Type     | Description                                                   |
| ---- | -------- | ------------------------------------------------------------- |
| flow | FlowJob  | root flow job describing the graph                            |
| opts | FlowOpts | flow options selecting one execution mode for the whole graph |

###### Return Type

**JobNode**

root node of the created durable job tree

###### Throws

QueueException: when the graph is structurally invalid or extends an existing job

ConfigurationException: when a queue or processor in the graph is not configured or inactive

---

##### `addBulk(flows)`

Atomically inserts several dependency graphs on the default `WORKER` lane. If any graph
fails, the entire bulk request rolls back.

###### Signature

```apex
public List<JobNode> addBulk(List<FlowJob> flows);
```

###### Parameters

| Name  | Type          | Description                          |
| ----- | ------------- | ------------------------------------ |
| flows | List<FlowJob> | root flow jobs describing each graph |

###### Return Type

**List<JobNode>**

root nodes of the created durable job trees, in request order

###### Throws

QueueException: when any graph is structurally invalid or extends an existing job

ConfigurationException: when a queue or processor in any graph is not configured or inactive

---

##### `addBulk(flows, opts)`

Atomically inserts several dependency graphs on the lane selected by `opts` . If any graph
fails, the entire bulk request rolls back.

###### Signature

```apex
public List<JobNode> addBulk(List<FlowJob> flows, FlowOpts opts);
```

###### Parameters

| Name  | Type          | Description                                               |
| ----- | ------------- | --------------------------------------------------------- |
| flows | List<FlowJob> | root flow jobs describing each graph                      |
| opts  | FlowOpts      | flow options selecting one execution mode for every graph |

###### Return Type

**List<JobNode>**

root nodes of the created durable job trees, in request order

###### Throws

QueueException: when any graph is structurally invalid or extends an existing job

ConfigurationException: when a queue or processor in any graph is not configured or inactive

---

##### `getFlow(rootJobId)`

Reads the complete durable dependency tree rooted at a job, as visible to the current user.

###### Signature

```apex
public JobNode getFlow(Id rootJobId);
```

###### Parameters

| Name      | Type | Description                            |
| --------- | ---- | -------------------------------------- |
| rootJobId | Id   | Salesforce Job\_\_c.Id of the root job |

###### Return Type

**JobNode**

root node of the durable job tree, each node carrying a Job handle and children

###### Throws

JobNotFoundException: when no job exists for rootJobId

###### Example

Queues.JobNode root = Queues.flowProducer().getFlow(rootJobId);

### Job Class

Lightweight handle to a single durable job, identified by its Salesforce `Job__c.Id` .
Exposes admin operations (retry, pause, resume, promote, discard, cancel, remove, defer,
change priority, update data or progress) and state getters. Obtain one with
`Queues.job(jobId)` . Each mutating call validates the job's current state and throws when the
transition is not allowed.

**See** [Queues](Queues.md)

#### Example

Queues.Job job = Queues.job(jobId);
if (job.isFailed()) {
job.retry();
}

#### Constructors

##### `Job(jobId)`

###### Signature

```apex
public Job(Id jobId);
```

###### Parameters

| Name  | Type | Description            |
| ----- | ---- | ---------------------- |
| jobId | Id   | Salesforce Job\_\_c.Id |

###### Throws

QueueException: when jobId is null

#### Methods

##### `getId()`

###### Signature

```apex
public Id getId();
```

###### Return Type

**Id**

Salesforce Job\_\_c.Id this handle is bound to

---

##### `getState()`

###### Signature

```apex
public State getState();
```

###### Return Type

**State**

current durable state of the job

###### Throws

JobNotFoundException: when the job no longer exists

---

##### `retry()`

Manually retries a failed, completed, or paused job: resets attempt state and moves it
back to runnable work.

###### Signature

```apex
public Job retry();
```

###### Return Type

**Job**

this handle for fluent chaining

###### Throws

JobNotFoundException: when the job no longer exists

InvalidOperationException: when the job is not failed, completed, or paused

---

##### `pause()`

Pauses the job. Waiting, delayed, or paused jobs pause immediately; an active job records
a pause request and is paused before its next execution if it stays runnable.

###### Signature

```apex
public Job pause();
```

###### Return Type

**Job**

this handle for fluent chaining

###### Throws

JobNotFoundException: when the job no longer exists

InvalidOperationException: when the job is not waiting, delayed, active, or paused

---

##### `resume()`

Resumes a paused job, returning it to runnable (or delayed) state.

###### Signature

```apex
public Job resume();
```

###### Return Type

**Job**

this handle for fluent chaining

###### Throws

JobNotFoundException: when the job no longer exists

InvalidOperationException: when the job is not paused

---

##### `promote()`

Promotes a delayed job to run now: moves it from `DELAYED` to `WAITING` and wakes the
queue if needed.

###### Signature

```apex
public Job promote();
```

###### Return Type

**Job**

this handle for fluent chaining

###### Throws

JobNotFoundException: when the job no longer exists

InvalidOperationException: when the job is not delayed

---

##### `discard()`

Discards future retries for a non-active job by capping its remaining attempts.

###### Signature

```apex
public Job discard();
```

###### Return Type

**Job**

this handle for fluent chaining

###### Throws

JobNotFoundException: when the job no longer exists

InvalidOperationException: when the job is active

---

##### `cancel()`

Cancels cancelable future work, marking the job `CANCELED` and preserving the record.
Resolves any parent dependency that was waiting on this job.

###### Signature

```apex
public Job cancel();
```

###### Return Type

**Job**

this handle for fluent chaining

###### Throws

JobNotFoundException: when the job no longer exists

InvalidOperationException: when the job is not waiting, delayed, paused, or waiting-children

---

##### `remove()`

Permanently deletes the job and cascades its `JobRun__c` attempt history. Cannot be
replaced by later maintenance; use `cancel()` to preserve records.

###### Signature

```apex
public void remove();
```

###### Return Type

**void**

###### Throws

JobNotFoundException: when the job no longer exists

InvalidOperationException: when the job is active

---

##### `defer(delay)`

Defers a runnable or paused job by a relative delay.

###### Signature

```apex
public Job defer(Integer delay);
```

###### Parameters

| Name  | Type    | Description                                             |
| ----- | ------- | ------------------------------------------------------- |
| delay | Integer | minutes to delay; 0 makes the job immediately available |

###### Return Type

**Job**

this handle for fluent chaining

###### Throws

QueueException: when delay is negative, or for a queueable job when it exceeds the native 10-minute queueable delay limit

JobNotFoundException: when the job no longer exists

InvalidOperationException: when the job is not waiting, delayed, or paused

---

##### `changePriority(priority)`

Changes claim or dispatch priority for a waiting, delayed, or paused job. Lower values
are claimed first; priority does not guarantee execution order.

###### Signature

```apex
public Job changePriority(Integer priority);
```

###### Parameters

| Name     | Type    | Description                   |
| -------- | ------- | ----------------------------- |
| priority | Integer | new priority, zero or greater |

###### Return Type

**Job**

this handle for fluent chaining

###### Throws

QueueException: when priority is negative

JobNotFoundException: when the job no longer exists

InvalidOperationException: when the job is not waiting, delayed, or paused

---

##### `updateData(data)`

Replaces a non-active job's payload before its next execution.

###### Signature

```apex
public Job updateData(Object data);
```

###### Parameters

| Name | Type   | Description                        |
| ---- | ------ | ---------------------------------- |
| data | Object | JSON-serializable payload to store |

###### Return Type

**Job**

this handle for fluent chaining

###### Throws

JobNotFoundException: when the job no longer exists

InvalidOperationException: when the job is active

---

##### `updateProgress(progress)`

Replaces a non-active job's progress value.

###### Signature

```apex
public Job updateProgress(Object progress);
```

###### Parameters

| Name     | Type   | Description                         |
| -------- | ------ | ----------------------------------- |
| progress | Object | JSON-serializable progress to store |

###### Return Type

**Job**

this handle for fluent chaining

###### Throws

JobNotFoundException: when the job no longer exists

InvalidOperationException: when the job is active

---

##### `isWaiting()`

###### Signature

```apex
public Boolean isWaiting();
```

###### Return Type

**Boolean**

true when the job is currently WAITING

###### Throws

JobNotFoundException: when the job no longer exists

---

##### `isDelayed()`

###### Signature

```apex
public Boolean isDelayed();
```

###### Return Type

**Boolean**

true when the job is currently DELAYED

###### Throws

JobNotFoundException: when the job no longer exists

---

##### `isActive()`

###### Signature

```apex
public Boolean isActive();
```

###### Return Type

**Boolean**

true when the job is currently ACTIVE

###### Throws

JobNotFoundException: when the job no longer exists

---

##### `isPaused()`

###### Signature

```apex
public Boolean isPaused();
```

###### Return Type

**Boolean**

true when the job is currently PAUSED

###### Throws

JobNotFoundException: when the job no longer exists

---

##### `isCompleted()`

###### Signature

```apex
public Boolean isCompleted();
```

###### Return Type

**Boolean**

true when the job is currently COMPLETED

###### Throws

JobNotFoundException: when the job no longer exists

---

##### `isFailed()`

###### Signature

```apex
public Boolean isFailed();
```

###### Return Type

**Boolean**

true when the job is currently FAILED

###### Throws

JobNotFoundException: when the job no longer exists

---

##### `isWaitingChildren()`

###### Signature

```apex
public Boolean isWaitingChildren();
```

###### Return Type

**Boolean**

true when the job is currently WAITING_CHILDREN

###### Throws

JobNotFoundException: when the job no longer exists

---

##### `isCanceled()`

###### Signature

```apex
public Boolean isCanceled();
```

###### Return Type

**Boolean**

true when the job is currently CANCELED

###### Throws

JobNotFoundException: when the job no longer exists

### JobNode Class

Node in a durable dependency tree returned by `FlowProducer.add(...)` and
`FlowProducer.getFlow(...)` . Each node carries a `Job` handle and its child nodes.

**See** Queues.FlowProducer

#### Example

void render(Queues.JobNode node) {
System.debug(node.getJob().getId() + ': ' + node.getJob().getState());
for (Queues.JobNode child : node.getChildren()) {
render(child);
}
}

#### Constructors

##### `JobNode(job)`

Creates a leaf node with no children.

###### Signature

```apex
public JobNode(Job job);
```

###### Parameters

| Name | Type | Description              |
| ---- | ---- | ------------------------ |
| job  | Job  | job handle for this node |

###### Throws

QueueException: when job is null

---

##### `JobNode(job, children)`

Creates a node with children.

###### Signature

```apex
public JobNode(Job job, List<JobNode> children);
```

###### Parameters

| Name     | Type          | Description                                   |
| -------- | ------------- | --------------------------------------------- |
| job      | Job           | job handle for this node                      |
| children | List<JobNode> | child nodes; null is treated as an empty list |

###### Throws

QueueException: when job is null

#### Methods

##### `getJob()`

###### Signature

```apex
public Job getJob();
```

###### Return Type

**Job**

job handle for this node

---

##### `getChildren()`

###### Signature

```apex
public List<JobNode> getChildren();
```

###### Return Type

**List<JobNode>**

child nodes of this node

---

##### `hasChildren()`

###### Signature

```apex
public Boolean hasChildren();
```

###### Return Type

**Boolean**

true when this node has at least one child

### JobData Class

One job specification within an `addBulk(...)` request: a name, the processor class to run,
the payload, and optional per-job options.

**See** Queues.Queue

#### Example

new Queues.JobData('sync-invoice', 'InvoiceSyncProcessor', new Map<String, Object>{ 'id' => 'A' });

#### Constructors

##### `JobData(name, processor, data)`

Creates a job specification with default options.

###### Signature

```apex
public JobData(String name, String processor, Object data);
```

###### Parameters

| Name      | Type   | Description                      |
| --------- | ------ | -------------------------------- |
| name      | String | display name of the job          |
| processor | String | allowlisted processor class name |
| data      | Object | JSON-serializable payload        |

###### Throws

QueueException: when name or processor is blank

---

##### `JobData(name, processor, data, opts)`

Creates a job specification with explicit options.

###### Signature

```apex
public JobData(String name, String processor, Object data, JobOptions opts);
```

###### Parameters

| Name      | Type       | Description                                         |
| --------- | ---------- | --------------------------------------------------- |
| name      | String     | display name of the job                             |
| processor | String     | allowlisted processor class name                    |
| data      | Object     | JSON-serializable payload                           |
| opts      | JobOptions | per-job options; null is treated as default options |

###### Throws

QueueException: when name or processor is blank

#### Methods

##### `getName()`

###### Signature

```apex
public String getName();
```

###### Return Type

**String**

display name of the job

---

##### `getData()`

###### Signature

```apex
public Object getData();
```

###### Return Type

**Object**

JSON-serializable payload

---

##### `getProcessor()`

###### Signature

```apex
public String getProcessor();
```

###### Return Type

**String**

allowlisted processor class name

---

##### `getOpts()`

###### Signature

```apex
public JobOptions getOpts();
```

###### Return Type

**JobOptions**

per-job options

### FlowJob Class

Builder for one node of a `FlowProducer` dependency graph. Unlike `JobData` , a flow job
carries its own queue name so a single graph can span queues, and its dependencies are
defined with `child(...)` . `opts.parent(...)` is not accepted inside a submitted graph;
define edges with `child(...)` .

**See** Queues.FlowProducer

#### Example

new Queues.FlowJob('finish-import', 'invoice-sync', 'FinishImportProcessor')
.data(new Map<String, Object>{ 'orderId' => 'O-1' })
.child(new Queues.FlowJob('load-lines', 'invoice-sync', 'LoadLinesProcessor'));

#### Constructors

##### `FlowJob(name, queueName, processor)`

###### Signature

```apex
public FlowJob(String name, String queueName, String processor);
```

###### Parameters

| Name      | Type   | Description                      |
| --------- | ------ | -------------------------------- |
| name      | String | display name of the job          |
| queueName | String | queue this node belongs to       |
| processor | String | allowlisted processor class name |

###### Throws

QueueException: when name, queueName, or processor is blank

#### Methods

##### `data(data)`

Sets the JSON-serializable payload for this node.

###### Signature

```apex
public FlowJob data(Object data);
```

###### Parameters

| Name | Type   | Description                                         |
| ---- | ------ | --------------------------------------------------- |
| data | Object | payload exposed to the processor through ctx.data() |

###### Return Type

**FlowJob**

this flow job for fluent chaining

---

##### `opts(opts)`

Sets the per-job options for this node, including dependency policies.

###### Signature

```apex
public FlowJob opts(JobOptions opts);
```

###### Parameters

| Name | Type       | Description                                         |
| ---- | ---------- | --------------------------------------------------- |
| opts | JobOptions | per-job options; null is treated as default options |

###### Return Type

**FlowJob**

this flow job for fluent chaining

---

##### `children(children)`

Replaces this node's children with the given list.

###### Signature

```apex
public FlowJob children(List<FlowJob> children);
```

###### Parameters

| Name     | Type          | Description                          |
| -------- | ------------- | ------------------------------------ |
| children | List<FlowJob> | child flow jobs this node depends on |

###### Return Type

**FlowJob**

this flow job for fluent chaining

###### Throws

QueueException: when children or any element is null

---

##### `child(child)`

Adds one child this node depends on.

###### Signature

```apex
public FlowJob child(FlowJob child);
```

###### Parameters

| Name  | Type    | Description    |
| ----- | ------- | -------------- |
| child | FlowJob | child flow job |

###### Return Type

**FlowJob**

this flow job for fluent chaining

###### Throws

QueueException: when child is null

---

##### `getName()`

###### Signature

```apex
public String getName();
```

###### Return Type

**String**

display name of the job

---

##### `getQueueName()`

###### Signature

```apex
public String getQueueName();
```

###### Return Type

**String**

queue this node belongs to

---

##### `getProcessor()`

###### Signature

```apex
public String getProcessor();
```

###### Return Type

**String**

allowlisted processor class name

---

##### `getData()`

###### Signature

```apex
public Object getData();
```

###### Return Type

**Object**

JSON-serializable payload, or null when unset

---

##### `getOpts()`

###### Signature

```apex
public JobOptions getOpts();
```

###### Return Type

**JobOptions**

per-job options, never null

---

##### `getChildren()`

###### Signature

```apex
public List<FlowJob> getChildren();
```

###### Return Type

**List<FlowJob>**

child flow jobs this node depends on

### RepeatOptions Class

Cadence options for a scheduler. Provide exactly one of `pattern(...)` (native cron) or
`every(...)` (minute-based self-rescheduling). `lim(...)` caps lifetime occurrences for both
strategies; `startAt` / `endAt` further bound an `every(...)` schedule only and are not
supported with `pattern(...)` .

**See** Queues.Queue

#### Example

new Queues.RepeatOptions().every(15).startAt(System.now().addMinutes(15)).lim(10);
new Queues.RepeatOptions().pattern('0 0 2 \* \* ?');

#### Methods

##### `every(minutes)`

Sets a minute-based cadence (alias of `everyMinutes` ).

###### Signature

```apex
public RepeatOptions every(Integer minutes);
```

###### Parameters

| Name    | Type    | Description                         |
| ------- | ------- | ----------------------------------- |
| minutes | Integer | interval in minutes, greater than 0 |

###### Return Type

**RepeatOptions**

this options object for fluent chaining

###### Throws

QueueException: when minutes is not greater than 0

---

##### `everyMinutes(minutes)`

Sets a minute-based cadence.

###### Signature

```apex
public RepeatOptions everyMinutes(Integer minutes);
```

###### Parameters

| Name    | Type    | Description                         |
| ------- | ------- | ----------------------------------- |
| minutes | Integer | interval in minutes, greater than 0 |

###### Return Type

**RepeatOptions**

this options object for fluent chaining

###### Throws

QueueException: when minutes is not greater than 0

---

##### `pattern(cronExpression)`

Sets a native cron cadence.

###### Signature

```apex
public RepeatOptions pattern(String cronExpression);
```

###### Parameters

| Name           | Type   | Description                |
| -------------- | ------ | -------------------------- |
| cronExpression | String | Salesforce cron expression |

###### Return Type

**RepeatOptions**

this options object for fluent chaining

###### Throws

QueueException: when cronExpression is blank

---

##### `lim(limitCount)`

Caps the lifetime number of materialized occurrences for the scheduler identity.
Re-upsert does not reset this counter.

###### Signature

```apex
public RepeatOptions lim(Integer limitCount);
```

###### Parameters

| Name       | Type    | Description                                                           |
| ---------- | ------- | --------------------------------------------------------------------- |
| limitCount | Integer | maximum lifetime occurrences; null means unlimited, otherwise greater |
| than `0`   |

###### Return Type

**RepeatOptions**

this options object for fluent chaining

###### Throws

QueueException: when limitCount is non-null and not greater than 0

---

##### `startAt(startAt)`

Sets the earliest time an `every(...)` schedule may first fire.

###### Signature

```apex
public RepeatOptions startAt(Datetime startAt);
```

###### Parameters

| Name    | Type     | Description         |
| ------- | -------- | ------------------- |
| startAt | Datetime | absolute start time |

###### Return Type

**RepeatOptions**

this options object for fluent chaining

###### Throws

QueueException: when startAt is null

---

##### `endAt(endAt)`

Sets the latest time an `every(...)` schedule may fire.

###### Signature

```apex
public RepeatOptions endAt(Datetime endAt);
```

###### Parameters

| Name  | Type     | Description       |
| ----- | -------- | ----------------- |
| endAt | Datetime | absolute end time |

###### Return Type

**RepeatOptions**

this options object for fluent chaining

###### Throws

QueueException: when endAt is null

---

##### `getEveryMinutes()`

###### Signature

```apex
public Integer getEveryMinutes();
```

###### Return Type

**Integer**

configured minute interval, or null when a cron pattern is used

---

##### `getPattern()`

###### Signature

```apex
public String getPattern();
```

###### Return Type

**String**

configured cron expression, or null when a minute interval is used

---

##### `getLimit()`

###### Signature

```apex
public Integer getLimit();
```

###### Return Type

**Integer**

lifetime occurrence cap, or null when unlimited

---

##### `getStartAt()`

###### Signature

```apex
public Datetime getStartAt();
```

###### Return Type

**Datetime**

configured start time, or null when unset

---

##### `getEndAt()`

###### Signature

```apex
public Datetime getEndAt();
```

###### Return Type

**Datetime**

configured end time, or null when unset

### JobTemplate Class

Describes the job a scheduler materializes on each occurrence: the generated job name and
processor, the payload, and the options applied to every occurrence. Template options may set
attempts, priority, parent provenance, and backoff, but not `jobId(...)` , `delay(...)` , or
`availableAt(...)` , because the scheduler owns occurrence ids and timing.

**See** Queues.Queue

#### Example

new Queues.JobTemplate('sync-invoice', 'InvoiceSyncProcessor')
.data(new Map<String, Object>{ 'source' => 'scheduler' })
.opts(new Queues.JobOptions().attempts(3).backoff(Queues.Backoff.EXPONENTIAL, 5));

#### Constructors

##### `JobTemplate(processor)`

Creates a template whose job name equals the processor class name.

###### Signature

```apex
public JobTemplate(String processor);
```

###### Parameters

| Name      | Type   | Description                                                 |
| --------- | ------ | ----------------------------------------------------------- |
| processor | String | allowlisted processor class name, also used as the job name |

###### Throws

QueueException: when processor is blank

---

##### `JobTemplate(name, processor)`

Creates a template with an explicit job name.

###### Signature

```apex
public JobTemplate(String name, String processor);
```

###### Parameters

| Name      | Type   | Description                      |
| --------- | ------ | -------------------------------- |
| name      | String | generated job name               |
| processor | String | allowlisted processor class name |

###### Throws

QueueException: when name or processor is blank

#### Methods

##### `data(data)`

Sets the payload generated for every occurrence.

###### Signature

```apex
public JobTemplate data(Object data);
```

###### Parameters

| Name | Type   | Description               |
| ---- | ------ | ------------------------- |
| data | Object | JSON-serializable payload |

###### Return Type

**JobTemplate**

this template for fluent chaining

---

##### `opts(opts)`

Sets the options applied to every materialized occurrence.

###### Signature

```apex
public JobTemplate opts(JobOptions opts);
```

###### Parameters

| Name                                 | Type       | Description                                                       |
| ------------------------------------ | ---------- | ----------------------------------------------------------------- |
| opts                                 | JobOptions | per-job options; null is treated as default options. Must not set |
| `jobId` , `delay` , or `availableAt` |

###### Return Type

**JobTemplate**

this template for fluent chaining

---

##### `getName()`

###### Signature

```apex
public String getName();
```

###### Return Type

**String**

generated job name

---

##### `getProcessor()`

###### Signature

```apex
public String getProcessor();
```

###### Return Type

**String**

allowlisted processor class name

---

##### `getData()`

###### Signature

```apex
public Object getData();
```

###### Return Type

**Object**

JSON-serializable payload, or null when unset

---

##### `getOpts()`

###### Signature

```apex
public JobOptions getOpts();
```

###### Return Type

**JobOptions**

per-occurrence options, never null

### JobScheduler Class

Read-only snapshot of a durable scheduler's state, returned by `Queue.getJobScheduler(...)`
and `Queue.getJobSchedulers(...)` . Reports cadence, lifetime iteration count and cap, window,
next run time, active state, and the job template.

**See** Queues.Queue

#### Example

Queues.JobScheduler s = Queues.of('invoice-sync').getJobScheduler('nightly-sync');
Integer runs = s.getIterationCount();
Datetime nextRunAt = s.getNextRunAt();

#### Constructors

##### `JobScheduler(jobSchedulerId, jobName, processor, iterationCount, limitCount, startAt, endAt, everyMinutes, pattern, nextRunAt, active, template)`

Framework constructor used when mapping durable scheduler state into a value object.

###### Signature

```apex
public JobScheduler(String jobSchedulerId, String jobName, String processor, Integer iterationCount, Integer limitCount, Datetime startAt, Datetime endAt, Integer everyMinutes, String pattern, Datetime nextRunAt, Boolean active, JobTemplate template);
```

###### Parameters

| Name           | Type        | Description                                                  |
| -------------- | ----------- | ------------------------------------------------------------ |
| jobSchedulerId | String      | queue-scoped scheduler identity                              |
| jobName        | String      | generated job name                                           |
| processor      | String      | processor class name                                         |
| iterationCount | Integer     | lifetime materialized occurrence count; null is treated as 0 |
| limitCount     | Integer     | lifetime occurrence cap, or null when unlimited              |
| startAt        | Datetime    | configured start time, or null                               |
| endAt          | Datetime    | configured end time, or null                                 |
| everyMinutes   | Integer     | minute interval, or null when a cron pattern is used         |
| pattern        | String      | cron expression, or null when a minute interval is used      |
| nextRunAt      | Datetime    | next scheduled run time, or null                             |
| active         | Boolean     | whether the scheduler is active; null is treated as false    |
| template       | JobTemplate | materialization template                                     |

#### Methods

##### `getJobSchedulerId()`

###### Signature

```apex
public String getJobSchedulerId();
```

###### Return Type

**String**

queue-scoped scheduler identity

---

##### `getJobName()`

###### Signature

```apex
public String getJobName();
```

###### Return Type

**String**

generated job name

---

##### `getProcessor()`

###### Signature

```apex
public String getProcessor();
```

###### Return Type

**String**

processor class name

---

##### `getIterationCount()`

###### Signature

```apex
public Integer getIterationCount();
```

###### Return Type

**Integer**

lifetime number of occurrences materialized so far

---

##### `getLimit()`

###### Signature

```apex
public Integer getLimit();
```

###### Return Type

**Integer**

lifetime occurrence cap, or null when unlimited

---

##### `getStartAt()`

###### Signature

```apex
public Datetime getStartAt();
```

###### Return Type

**Datetime**

configured start time, or null when unset

---

##### `getEndAt()`

###### Signature

```apex
public Datetime getEndAt();
```

###### Return Type

**Datetime**

configured end time, or null when unset

---

##### `getEveryMinutes()`

###### Signature

```apex
public Integer getEveryMinutes();
```

###### Return Type

**Integer**

minute interval, or null when a cron pattern is used

---

##### `getPattern()`

###### Signature

```apex
public String getPattern();
```

###### Return Type

**String**

cron expression, or null when a minute interval is used

---

##### `getNextRunAt()`

###### Signature

```apex
public Datetime getNextRunAt();
```

###### Return Type

**Datetime**

next scheduled run time, or null when none is planned

---

##### `isActive()`

###### Signature

```apex
public Boolean isActive();
```

###### Return Type

**Boolean**

true when the scheduler is active

---

##### `getTemplate()`

###### Signature

```apex
public JobTemplate getTemplate();
```

###### Return Type

**JobTemplate**

materialization template

### JobOptions Class

Per-job options applied at enqueue time: idempotency key, attempts, priority, delay or
absolute availability, retry backoff, parent provenance, and dependency policies. All setters
are chainable. Queue metadata supplies defaults for any option left unset.

**See** Queues.Queue

#### Example

new Queues.JobOptions()
.jobId('invoice:INV-001')
.attempts(5)
.priority(2)
.delay(3)
.backoff(Queues.Backoff.FIXED, 5, 0.25);

#### Methods

##### `jobId(jobId)`

Sets a caller-owned, queue-scoped idempotency key. Re-adding the same key to the same
queue reconciles to the existing job at the database boundary.

###### Signature

```apex
public JobOptions jobId(String jobId);
```

###### Parameters

| Name  | Type   | Description                                           |
| ----- | ------ | ----------------------------------------------------- |
| jobId | String | queue-scoped idempotency key, 174 characters or fewer |

###### Return Type

**JobOptions**

this options object for fluent chaining

###### Throws

QueueException: when jobId is blank or longer than 174 characters

---

##### `category(category)`

Sets an optional SOQL-friendly job category.

###### Signature

```apex
public JobOptions category(String category);
```

###### Parameters

| Name     | Type   | Description                            |
| -------- | ------ | -------------------------------------- |
| category | String | category value, 80 characters or fewer |

###### Return Type

**JobOptions**

this options object for fluent chaining

###### Throws

QueueException: when category is blank or too long

---

##### `groupKey(groupKey)`

Sets an optional indexed business grouping key, such as an order or customer id.

###### Signature

```apex
public JobOptions groupKey(String groupKey);
```

###### Parameters

| Name     | Type   | Description                           |
| -------- | ------ | ------------------------------------- |
| groupKey | String | grouping key, 255 characters or fewer |

###### Return Type

**JobOptions**

this options object for fluent chaining

###### Throws

QueueException: when groupKey is blank or too long

---

##### `correlationId(correlationId)`

Sets an optional indexed correlation id for cross-system tracing.

###### Signature

```apex
public JobOptions correlationId(String correlationId);
```

###### Parameters

| Name          | Type   | Description                             |
| ------------- | ------ | --------------------------------------- |
| correlationId | String | correlation id, 255 characters or fewer |

###### Return Type

**JobOptions**

this options object for fluent chaining

###### Throws

QueueException: when correlationId is blank or too long

---

##### `attempts(attempts)`

Sets the maximum number of execution attempts.

###### Signature

```apex
public JobOptions attempts(Integer attempts);
```

###### Parameters

| Name     | Type    | Description                   |
| -------- | ------- | ----------------------------- |
| attempts | Integer | attempt count, greater than 0 |

###### Return Type

**JobOptions**

this options object for fluent chaining

###### Throws

QueueException: when attempts is not greater than 0

---

##### `priority(priority)`

Sets the claim or dispatch priority. Lower values are claimed first; priority does not
guarantee execution order.

###### Signature

```apex
public JobOptions priority(Integer priority);
```

###### Parameters

| Name     | Type    | Description               |
| -------- | ------- | ------------------------- |
| priority | Integer | priority, zero or greater |

###### Return Type

**JobOptions**

this options object for fluent chaining

###### Throws

QueueException: when priority is negative

---

##### `delay(delay)`

Sets a relative availability delay in minutes.

###### Signature

```apex
public JobOptions delay(Integer delay);
```

###### Parameters

| Name  | Type    | Description                                                       |
| ----- | ------- | ----------------------------------------------------------------- |
| delay | Integer | minutes to wait before the job becomes available, zero or greater |

###### Return Type

**JobOptions**

this options object for fluent chaining

###### Throws

QueueException: when delay is negative

---

##### `availableAt(availableAt)`

Sets an absolute availability time. Takes precedence over `delay(...)` .

###### Signature

```apex
public JobOptions availableAt(Datetime availableAt);
```

###### Parameters

| Name        | Type     | Description                             |
| ----------- | -------- | --------------------------------------- |
| availableAt | Datetime | absolute time the job becomes available |

###### Return Type

**JobOptions**

this options object for fluent chaining

###### Throws

QueueException: when availableAt is null

---

##### `backoff(type, delay)`

Sets the retry backoff strategy and base delay with no jitter.

###### Signature

```apex
public JobOptions backoff(Backoff type, Integer delay);
```

###### Parameters

| Name  | Type    | Description                            |
| ----- | ------- | -------------------------------------- |
| type  | Backoff | backoff strategy                       |
| delay | Integer | base delay in minutes, zero or greater |

###### Return Type

**JobOptions**

this options object for fluent chaining

###### Throws

QueueException: when type is null or delay is negative

---

##### `backoff(type, delay, jitter)`

Sets the retry backoff strategy, base delay, and jitter ratio. Queueable retry delay is
capped at 10 minutes by the native queueable delay limit.

###### Signature

```apex
public JobOptions backoff(Backoff type, Integer delay, Decimal jitter);
```

###### Parameters

| Name   | Type    | Description                                        |
| ------ | ------- | -------------------------------------------------- |
| type   | Backoff | backoff strategy                                   |
| delay  | Integer | base delay in minutes, zero or greater             |
| jitter | Decimal | jitter ratio between 0 and 1; null is treated as 0 |

###### Return Type

**JobOptions**

this options object for fluent chaining

###### Throws

QueueException: when type is null, delay is negative, jitter is outside , or jitter is set with a backoff

---

##### `parent(parentJobId)`

Sets a provenance-only parent link. This does not create waiting-children or failure
propagation semantics and is rejected inside a submitted `FlowProducer` graph; use
`FlowJob.child(...)` to define dependencies.

###### Signature

```apex
public JobOptions parent(Id parentJobId);
```

###### Parameters

| Name        | Type | Description                          |
| ----------- | ---- | ------------------------------------ |
| parentJobId | Id   | Salesforce Job\_\_c.Id of the parent |

###### Return Type

**JobOptions**

this options object for fluent chaining

###### Throws

QueueException: when parentJobId is null

---

##### `failParentOnFailure()`

Flow dependency policy: fail the parent when this child reaches terminal `FAILED` .

###### Signature

```apex
public JobOptions failParentOnFailure();
```

###### Return Type

**JobOptions**

this options object for fluent chaining

---

##### `continueParentOnFailure()`

Flow dependency policy: release the parent when this child fails, even if other children
are still unresolved.

###### Signature

```apex
public JobOptions continueParentOnFailure();
```

###### Return Type

**JobOptions**

this options object for fluent chaining

---

##### `ignoreDependencyOnFailure()`

Flow dependency policy: treat only this failed child as resolved; the parent still waits
for remaining children.

###### Signature

```apex
public JobOptions ignoreDependencyOnFailure();
```

###### Return Type

**JobOptions**

this options object for fluent chaining

---

##### `removeDependencyOnFailure()`

Flow dependency policy: treat this failed child as resolved and remove the parent link.

###### Signature

```apex
public JobOptions removeDependencyOnFailure();
```

###### Return Type

**JobOptions**

this options object for fluent chaining

---

##### `cancelParentOnCanceled()`

Flow dependency policy: cancel the parent when this child reaches terminal `CANCELED` .

###### Signature

```apex
public JobOptions cancelParentOnCanceled();
```

###### Return Type

**JobOptions**

this options object for fluent chaining

---

##### `failParentOnCanceled()`

Flow dependency policy: fail the parent when this child reaches terminal `CANCELED` .

###### Signature

```apex
public JobOptions failParentOnCanceled();
```

###### Return Type

**JobOptions**

this options object for fluent chaining

---

##### `ignoreDependencyOnCanceled()`

Flow dependency policy: treat only this canceled child as resolved; the parent still
waits for remaining children.

###### Signature

```apex
public JobOptions ignoreDependencyOnCanceled();
```

###### Return Type

**JobOptions**

this options object for fluent chaining

---

##### `getJobId()`

###### Signature

```apex
public String getJobId();
```

###### Return Type

**String**

configured idempotency key, or null when unset

---

##### `getCategory()`

###### Signature

```apex
public String getCategory();
```

###### Return Type

**String**

configured job category, or null when unset

---

##### `getGroupKey()`

###### Signature

```apex
public String getGroupKey();
```

###### Return Type

**String**

configured grouping key, or null when unset

---

##### `getCorrelationId()`

###### Signature

```apex
public String getCorrelationId();
```

###### Return Type

**String**

configured correlation id, or null when unset

---

##### `getAttempts()`

###### Signature

```apex
public Integer getAttempts();
```

###### Return Type

**Integer**

configured attempt count, or null when unset

---

##### `getPriority()`

###### Signature

```apex
public Integer getPriority();
```

###### Return Type

**Integer**

configured priority, or null when unset

---

##### `getDelay()`

###### Signature

```apex
public Integer getDelay();
```

###### Return Type

**Integer**

configured relative delay in minutes, or null when unset

---

##### `getAvailableAt()`

###### Signature

```apex
public Datetime getAvailableAt();
```

###### Return Type

**Datetime**

configured absolute availability time, or null when unset

---

##### `getBackoffType()`

###### Signature

```apex
public Backoff getBackoffType();
```

###### Return Type

**Backoff**

configured backoff strategy, or null when unset

---

##### `getBackoffValue()`

###### Signature

```apex
public Integer getBackoffValue();
```

###### Return Type

**Integer**

configured backoff base delay in minutes, or null when unset

---

##### `getBackoffJitter()`

###### Signature

```apex
public Decimal getBackoffJitter();
```

###### Return Type

**Decimal**

configured backoff jitter ratio, or null when unset

---

##### `getParentJobId()`

###### Signature

```apex
public Id getParentJobId();
```

###### Return Type

**Id**

configured provenance parent id, or null when unset

---

##### `getDependencyFailurePolicy()`

###### Signature

```apex
public DependencyFailurePolicy getDependencyFailurePolicy();
```

###### Return Type

**DependencyFailurePolicy**

configured dependency failure policy, or null for the default

---

##### `getDependencyCanceledPolicy()`

###### Signature

```apex
public DependencyCanceledPolicy getDependencyCanceledPolicy();
```

###### Return Type

**DependencyCanceledPolicy**

configured dependency canceled policy, or null for the default

### BulkJobOptions Class

Bulk-request options: the execution lane for the whole request and an optional dispatcher
dedupe key. `WORKER` is the default. The dedupe key dedupes the initial queueable dispatcher
handoff only; it is not a per-job idempotency key (use `JobOptions.jobId(...)` for that).

**See** Queues.Queue

#### Example

new Queues.BulkJobOptions().queueable().dedupeKey('invoice-sync:batch-2026-06-04');

#### Methods

##### `dedupeKey(dedupeKey)`

Sets the dispatcher dedupe key for the initial queueable handoff.

###### Signature

```apex
public BulkJobOptions dedupeKey(String dedupeKey);
```

###### Parameters

| Name      | Type   | Description           |
| --------- | ------ | --------------------- |
| dedupeKey | String | dispatcher dedupe key |

###### Return Type

**BulkJobOptions**

this options object for fluent chaining

###### Throws

QueueException: when dedupeKey is blank

---

##### `worker()`

Selects the `WORKER` lane (the default).

###### Signature

```apex
public BulkJobOptions worker();
```

###### Return Type

**BulkJobOptions**

this options object for fluent chaining

---

##### `queueable()`

Selects the `QUEUEABLE_SERIAL` lane.

###### Signature

```apex
public BulkJobOptions queueable();
```

###### Return Type

**BulkJobOptions**

this options object for fluent chaining

---

##### `queueableConcurrent()`

Selects the `QUEUEABLE_CONCURRENT` lane.

###### Signature

```apex
public BulkJobOptions queueableConcurrent();
```

###### Return Type

**BulkJobOptions**

this options object for fluent chaining

---

##### `invocable()`

Selects the `INVOCABLE` lane.

###### Signature

```apex
public BulkJobOptions invocable();
```

###### Return Type

**BulkJobOptions**

this options object for fluent chaining

---

##### `getExecutionMode()`

###### Signature

```apex
public ExecutionMode getExecutionMode();
```

###### Return Type

**ExecutionMode**

selected execution mode, defaulting to WORKER when unset

---

##### `getDedupeKey()`

###### Signature

```apex
public String getDedupeKey();
```

###### Return Type

**String**

configured dispatcher dedupe key, or null when unset

### FlowOpts Class

Flow-request options: selects the single execution lane applied to every job in a
`FlowProducer` graph. `WORKER` is the default.

**See** Queues.FlowProducer

#### Example

Queues.flowProducer().add(flow, new Queues.FlowOpts().queueable());

#### Methods

##### `worker()`

Selects the `WORKER` lane (the default).

###### Signature

```apex
public FlowOpts worker();
```

###### Return Type

**FlowOpts**

this options object for fluent chaining

---

##### `queueable()`

Selects the `QUEUEABLE_SERIAL` lane.

###### Signature

```apex
public FlowOpts queueable();
```

###### Return Type

**FlowOpts**

this options object for fluent chaining

---

##### `queueableConcurrent()`

Selects the `QUEUEABLE_CONCURRENT` lane.

###### Signature

```apex
public FlowOpts queueableConcurrent();
```

###### Return Type

**FlowOpts**

this options object for fluent chaining

---

##### `invocable()`

Selects the `INVOCABLE` lane.

###### Signature

```apex
public FlowOpts invocable();
```

###### Return Type

**FlowOpts**

this options object for fluent chaining

---

##### `getExecutionMode()`

###### Signature

```apex
public ExecutionMode getExecutionMode();
```

###### Return Type

**ExecutionMode**

selected execution mode, defaulting to WORKER when unset

### JobContext Class

Durable, read-only context passed to `JobProcessor.process(...)` for a single attempt. Exposes
the job's identity, idempotency key, attempt counters, pause request, and deserialized data,
checkpoint, and progress, plus the factory methods that build the processor's result.

**See** Queues.JobProcessor

**See** Queues.ProcessResult

#### Example

public Queues.ProcessResult process(final Queues.JobContext ctx) {
Map<String, Object> data = (Map<String, Object>) ctx.data();
request.setHeader('Idempotency-Key', ctx.idempotencyKey());
return ctx.complete(new Map<String, Object>{ 'status' => 'synced' });
}

#### Constructors

##### `JobContext(job)`

Framework constructor that snapshots a durable job into an attempt context.

###### Signature

```apex
public JobContext(Job__c job);
```

###### Parameters

| Name | Type                                    | Description                       |
| ---- | --------------------------------------- | --------------------------------- |
| job  | [Job\_\_c](..\custom-objects\Job__c.md) | durable job record being executed |

###### Throws

QueueException: when job is null

#### Methods

##### `jobId()`

###### Signature

```apex
public Id jobId();
```

###### Return Type

**Id**

Salesforce Job\_\_c.Id of the durable job

---

##### `queueName()`

###### Signature

```apex
public String queueName();
```

###### Return Type

**String**

queue this job belongs to

---

##### `name()`

###### Signature

```apex
public String name();
```

###### Return Type

**String**

display name of the job

---

##### `processorClass()`

###### Signature

```apex
public String processorClass();
```

###### Return Type

**String**

processor class name running the job

---

##### `idempotencyKey()`

Returns a stable, queue-scoped logical key for the job. The format is the queue name, a
colon, and the caller `jobId(...)` when set ( `queueName + ':' + jobId` ), otherwise the
queue name, a colon, and the record id ( `queueName + ':' + Job__c.Id` ). Send it to
external systems that support idempotent requests so a replayed attempt does not
double-apply a side effect.

###### Signature

```apex
public String idempotencyKey();
```

###### Return Type

**String**

stable queue-scoped idempotency key for the durable job

---

##### `attemptsMade()`

Returns the current attempt number, counting this attempt: the first attempt sees `1` .

###### Signature

```apex
public Integer attemptsMade();
```

###### Return Type

**Integer**

current attempt number, including this attempt

---

##### `maxAttempts()`

###### Signature

```apex
public Integer maxAttempts();
```

###### Return Type

**Integer**

configured maximum number of attempts

---

##### `isPauseRequested()`

###### Signature

```apex
public Boolean isPauseRequested();
```

###### Return Type

**Boolean**

true when a pause was requested while the job was active

---

##### `data()`

###### Signature

```apex
public Object data();
```

###### Return Type

**Object**

deserialized job payload, or null when there is none

---

##### `checkpoint()`

###### Signature

```apex
public Object checkpoint();
```

###### Return Type

**Object**

deserialized checkpoint saved by a previous pause or defer, or null when none

---

##### `progress()`

###### Signature

```apex
public Object progress();
```

###### Return Type

**Object**

deserialized progress saved by a previous attempt, or null when none

---

##### `complete(returnValue)`

Builds a completed result carrying a return value.

###### Signature

```apex
public ProcessResult complete(Object returnValue);
```

###### Parameters

| Name        | Type   | Description                                        |
| ----------- | ------ | -------------------------------------------------- |
| returnValue | Object | JSON-serializable value to store as the job result |

###### Return Type

**ProcessResult**

completed process result

---

##### `fail(failedReason)`

Builds a retryable failed result. The job retries while attempts remain.

###### Signature

```apex
public ProcessResult fail(String failedReason);
```

###### Parameters

| Name         | Type   | Description                                                   |
| ------------ | ------ | ------------------------------------------------------------- |
| failedReason | String | human-readable failure reason recorded on the attempt and job |

###### Return Type

**ProcessResult**

failed process result

---

##### `pause(checkpoint)`

Builds a paused result carrying a checkpoint. The job pauses until resumed and the next
attempt can read the checkpoint.

###### Signature

```apex
public ProcessResult pause(Object checkpoint);
```

###### Parameters

| Name       | Type   | Description                                 |
| ---------- | ------ | ------------------------------------------- |
| checkpoint | Object | JSON-serializable checkpoint to resume from |

###### Return Type

**ProcessResult**

paused process result

---

##### `defer(checkpoint, delay)`

Builds a deferred result carrying a checkpoint and a delay. The job becomes available
again after the delay and the next attempt can read the checkpoint.

###### Signature

```apex
public ProcessResult defer(Object checkpoint, Integer delay);
```

###### Parameters

| Name       | Type    | Description                                                        |
| ---------- | ------- | ------------------------------------------------------------------ |
| checkpoint | Object  | JSON-serializable checkpoint to resume from                        |
| delay      | Integer | minutes to wait before the job is available again, zero or greater |

###### Return Type

**ProcessResult**

deferred process result

###### Throws

QueueException: when delay is negative

### ProcessResult Class

Outcome a `JobProcessor` returns from `process(...)` , built through the `JobContext` factories
( `complete` , `fail` , `pause` , `defer` ) and optionally enriched with progress. The framework
persists the durable job and attempt result from this value after the processor returns.

**See** Queues.JobContext

#### Example

return ctx.defer(new Map<String, Object>{ 'step' => 'posted' }, 5)
.withProgress(new Map<String, Object>{ 'step' => 'waiting-for-ledger' });

#### Methods

##### `withCheckpoint(checkpoint)`

Attaches a checkpoint to resume from on the next attempt.

###### Signature

```apex
public ProcessResult withCheckpoint(Object checkpoint);
```

###### Parameters

| Name       | Type   | Description                  |
| ---------- | ------ | ---------------------------- |
| checkpoint | Object | JSON-serializable checkpoint |

###### Return Type

**ProcessResult**

this result for fluent chaining

---

##### `withProgress(progress)`

Attaches progress that is stored durably and readable by later attempts through
`ctx.progress()` .

###### Signature

```apex
public ProcessResult withProgress(Object progress);
```

###### Parameters

| Name     | Type   | Description                |
| -------- | ------ | -------------------------- |
| progress | Object | JSON-serializable progress |

###### Return Type

**ProcessResult**

this result for fluent chaining

---

##### `withReturnValue(returnValue)`

Attaches the value stored as the completed job's result.

###### Signature

```apex
public ProcessResult withReturnValue(Object returnValue);
```

###### Parameters

| Name        | Type   | Description                    |
| ----------- | ------ | ------------------------------ |
| returnValue | Object | JSON-serializable return value |

###### Return Type

**ProcessResult**

this result for fluent chaining

---

##### `withFailedReason(failedReason)`

Attaches the failure reason recorded on the attempt and job.

###### Signature

```apex
public ProcessResult withFailedReason(String failedReason);
```

###### Parameters

| Name         | Type   | Description                   |
| ------------ | ------ | ----------------------------- |
| failedReason | String | human-readable failure reason |

###### Return Type

**ProcessResult**

this result for fluent chaining

---

##### `withDelay(delay)`

Attaches the delay before a deferred job becomes available again.

###### Signature

```apex
public ProcessResult withDelay(Integer delay);
```

###### Parameters

| Name  | Type    | Description                      |
| ----- | ------- | -------------------------------- |
| delay | Integer | minutes to wait, zero or greater |

###### Return Type

**ProcessResult**

this result for fluent chaining

###### Throws

QueueException: when delay is negative

---

##### `getOutcome()`

###### Signature

```apex
public Outcome getOutcome();
```

###### Return Type

**Outcome**

outcome this result represents

---

##### `getCheckpointJson()`

###### Signature

```apex
public String getCheckpointJson();
```

###### Return Type

**String**

serialized checkpoint, or null when none was attached

---

##### `getProgressJson()`

###### Signature

```apex
public String getProgressJson();
```

###### Return Type

**String**

serialized progress, or null when none was attached

---

##### `getReturnValueJson()`

###### Signature

```apex
public String getReturnValueJson();
```

###### Return Type

**String**

serialized return value, or null when none was attached

---

##### `getFailedReason()`

###### Signature

```apex
public String getFailedReason();
```

###### Return Type

**String**

failure reason, or null when none was attached

---

##### `getDelay()`

###### Signature

```apex
public Integer getDelay();
```

###### Return Type

**Integer**

delay in minutes for a deferred result, or null when none was attached

### QueueStats Class

Exact aggregate statistics for a queue: pause state, active worker count, total job count,
and per-state job counts. Returned by `Queue.getStats()` .

**See** Queues.Queue

#### Example

Queues.QueueStats stats = Queues.of('invoice-sync').getStats();
Integer failed = stats.getCount(Queues.State.FAILED);

#### Properties

##### `queueName`

Queue these statistics describe.

###### Signature

```apex
public queueName;
```

###### Type

String

---

##### `isPaused`

Whether the queue is currently paused.

###### Signature

```apex
public isPaused;
```

###### Type

Boolean

---

##### `activeWorkerCount`

Number of active framework workers for the queue.

###### Signature

```apex
public activeWorkerCount;
```

###### Type

Integer

---

##### `total`

Total number of jobs across all states.

###### Signature

```apex
public total;
```

###### Type

Integer

---

##### `waiting`

Number of `WAITING` jobs.

###### Signature

```apex
public waiting;
```

###### Type

Integer

---

##### `delayed`

Number of `DELAYED` jobs.

###### Signature

```apex
public delayed;
```

###### Type

Integer

---

##### `active`

Number of `ACTIVE` jobs.

###### Signature

```apex
public active;
```

###### Type

Integer

---

##### `completed`

Number of `COMPLETED` jobs.

###### Signature

```apex
public completed;
```

###### Type

Integer

---

##### `failed`

Number of `FAILED` jobs.

###### Signature

```apex
public failed;
```

###### Type

Integer

---

##### `paused`

Number of `PAUSED` jobs.

###### Signature

```apex
public paused;
```

###### Type

Integer

---

##### `waitingChildren`

Number of `WAITING_CHILDREN` jobs.

###### Signature

```apex
public waitingChildren;
```

###### Type

Integer

---

##### `canceled`

Number of `CANCELED` jobs.

###### Signature

```apex
public canceled;
```

###### Type

Integer

#### Constructors

##### `QueueStats(queueName)`

Creates an empty statistics object for a queue.

###### Signature

```apex
public QueueStats(String queueName);
```

###### Parameters

| Name      | Type   | Description                     |
| --------- | ------ | ------------------------------- |
| queueName | String | queue these statistics describe |

#### Methods

##### `withRuntime(isPaused, activeWorkerCount)`

Sets the runtime fields (pause state and active worker count).

###### Signature

```apex
public QueueStats withRuntime(Boolean isPaused, Integer activeWorkerCount);
```

###### Parameters

| Name              | Type    | Description                                           |
| ----------------- | ------- | ----------------------------------------------------- |
| isPaused          | Boolean | whether the queue is paused; null is treated as false |
| activeWorkerCount | Integer | number of active workers; null is treated as 0        |

###### Return Type

**QueueStats**

this statistics object for fluent chaining

---

##### `withStateCount(state, count)`

Adds a count for one state, updating the per-state field and `total` .

###### Signature

```apex
public QueueStats withStateCount(State state, Integer count);
```

###### Parameters

| Name  | Type    | Description                                 |
| ----- | ------- | ------------------------------------------- |
| state | State   | state the count belongs to                  |
| count | Integer | number of jobs to add; null is treated as 0 |

###### Return Type

**QueueStats**

this statistics object for fluent chaining

---

##### `getCount(state)`

Returns the job count for a single state.

###### Signature

```apex
public Integer getCount(State state);
```

###### Parameters

| Name  | Type  | Description      |
| ----- | ----- | ---------------- |
| state | State | state to look up |

###### Return Type

**Integer**

job count for state, or 0 for an unrecognized state

---

##### `asJobCounts()`

Returns job counts keyed by every state.

###### Signature

```apex
public Map<State,Integer> asJobCounts();
```

###### Return Type

**Map<State,Integer>**

map of each State to its job count

---

##### `asJobCounts(states)`

Returns job counts for the requested states only.

###### Signature

```apex
public Map<State,Integer> asJobCounts(Set<State> states);
```

###### Parameters

| Name   | Type       | Description                                                    |
| ------ | ---------- | -------------------------------------------------------------- |
| states | Set<State> | states to include; null or empty returns counts for all states |

###### Return Type

**Map<State,Integer>**

map of each requested State to its job count, with 0 for absent states

### QueueException Class

Base exception for all framework errors. Catch this to handle any `Queues` failure.

### ConfigurationException Class

Thrown when a queue or processor is missing or inactive in custom metadata.

### JobNotFoundException Class

Thrown when an operation targets a job id that does not exist.

### InvalidOperationException Class

Thrown when an operation is not allowed for the job's current state, for example pausing a
completed job or removing an active one.

### UnrecoverableJobException Class

Thrown by a processor to fail its job immediately without consuming further retries.

#### Example

throw new Queues.UnrecoverableJobException('Invoice is permanently invalid.');

## Enums

### State Enum

Durable lifecycle state of a job, stored on `Job__c.State__c` . `COMPLETED` , `FAILED` , and
`CANCELED` are terminal; `WAITING` , `DELAYED` , and `PAUSED` are runnable or holdable;
`WAITING_CHILDREN` is a `FlowProducer` parent awaiting its children.

#### Values

| Value            | Description                                         |
| ---------------- | --------------------------------------------------- |
| WAITING          | Due or soon-due, eligible to be claimed.            |
| DELAYED          | Not yet available; waits for its available-at time. |
| ACTIVE           | Claimed and leased to an executor.                  |
| COMPLETED        | Processor returned a completed result.              |
| FAILED           | Attempts exhausted or unrecoverable failure.        |
| PAUSED           | Held until resumed.                                 |
| WAITING_CHILDREN | Flow parent awaiting child resolution.              |
| CANCELED         | Future work abandoned, preserved for observability. |

### Backoff Enum

Retry backoff strategy. Delay values are expressed in minutes. Queueable retry delay is
capped at 10 minutes by the native queueable delay limit; `WORKER` retry delay is uncapped.

#### Values

| Value       | Description                              |
| ----------- | ---------------------------------------- |
| NONE        | Retry immediately when attempts remain.  |
| FIXED       | Same delay between every retry.          |
| EXPONENTIAL | Delay grows from the base on each retry. |

### ExecutionMode Enum

Transport lane that runs the processor. Selected per bulk request or flow graph. Lanes are
isolated and must not share durable runtime ownership.

#### Values

| Value                | Description                                 |
| -------------------- | ------------------------------------------- |
| WORKER               | Default Batch Apex worker lane.             |
| QUEUEABLE_SERIAL     | Ordered queueable dispatcher submission.    |
| QUEUEABLE_CONCURRENT | Aggressive concurrent queueable submission. |
| INVOCABLE            | Flow Scheduled Path lane.                   |

### DependencyFailurePolicy Enum

Parent behavior when a `FlowProducer` child reaches terminal `FAILED` . Evaluated only after
the child is terminal, never per attempt. Default (unset) keeps the parent in
`WAITING_CHILDREN` .

#### Values

| Value             | Description                                         |
| ----------------- | --------------------------------------------------- |
| FAIL_PARENT       | Fail the parent when this child fails.              |
| CONTINUE_PARENT   | Release the parent even if siblings are unresolved. |
| IGNORE_DEPENDENCY | Treat only this child as resolved.                  |
| REMOVE_DEPENDENCY | Resolve this child and remove the parent link.      |

### DependencyCanceledPolicy Enum

Parent behavior when a `FlowProducer` child reaches terminal `CANCELED` . Evaluated only
after the child is terminal. Default (unset) keeps the parent in `WAITING_CHILDREN` .

#### Values

| Value             | Description                                    |
| ----------------- | ---------------------------------------------- |
| CANCEL_PARENT     | Cancel the parent when this child is canceled. |
| FAIL_PARENT       | Fail the parent when this child is canceled.   |
| IGNORE_DEPENDENCY | Treat only this child as resolved.             |

### QueueableDispatchState Enum

State of a `QueueableDispatch__c` handoff envelope for queueable-lane jobs.

#### Values

| Value     | Description                               |
| --------- | ----------------------------------------- |
| WAITING   | Created, dispatcher not yet submitted.    |
| ACTIVE    | Dispatcher running, submitting executors. |
| COMPLETED | Every requested job dispatched.           |
| FAILED    | Dispatch failed catastrophically.         |

### RunStatus Enum

Recorded status of a single execution attempt on `JobRun__c.Status__c` .

#### Values

| Value     | Description                                     |
| --------- | ----------------------------------------------- |
| ACTIVE    | Attempt in progress.                            |
| COMPLETED | Attempt completed successfully.                 |
| FAILED    | Attempt failed.                                 |
| PAUSED    | Processor paused with a checkpoint.             |
| DEFERRED  | Processor deferred with a checkpoint and delay. |

### Outcome Enum

Outcome a processor returns from `process(ctx)` , produced through the `JobContext` result
factories ( `complete` , `fail` , `pause` , `defer` ).

#### Values

| Value     | Description                           |
| --------- | ------------------------------------- |
| COMPLETED | Finished successfully.                |
| FAILED    | Retryable failure.                    |
| PAUSED    | Pause with checkpoint until resumed.  |
| DEFERRED  | Resume after a delay with checkpoint. |

## Interfaces

### JobProcessor Interface

Contract a user class implements to run job business logic. The framework calls `process`
once per transaction for a single durable job attempt. The implementation must have a public
no-argument constructor and be allowlisted by an active `Processor__mdt` record. Side effects
must be externally idempotent, because a catastrophic transaction failure can replay an
attempt after an external write already succeeded; use `JobContext.idempotencyKey()` as the
stable logical-job key.

#### Example

public class InvoiceSyncProcessor implements Queues.JobProcessor {
public Queues.ProcessResult process(final Queues.JobContext ctx) {
Map<String, Object> data = (Map<String, Object>) ctx.data();
// ... perform work, callouts, etc.
return ctx.complete(new Map<String, Object>{ 'status' => 'synced' });
}
}

#### Methods

##### `process(ctx)`

Runs the job's business logic for a single attempt.

###### Signature

```apex
public ProcessResult process(JobContext ctx);
```

###### Parameters

| Name             | Type       | Description                                                                |
| ---------------- | ---------- | -------------------------------------------------------------------------- |
| ctx              | JobContext | durable context for the attempt: data, checkpoint, progress, attempts, and |
| result factories |

###### Return Type

**ProcessResult**

one of the ctx result factories: complete, fail, pause, or defer

###### Throws

[Queues](Queues.md): .UnrecoverableJobException to fail the job immediately without further retries
