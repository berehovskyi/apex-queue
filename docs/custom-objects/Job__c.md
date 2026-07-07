# Job

Stores each durable queue job, including its payload, lifecycle state, retry policy, lease data, and resumable execution fields.

## API Name

`Job__c`

## Fields

### Async Apex Job Id

AsyncApexJob Id that currently owns execution: batch worker for WORKER jobs or queueable executor for queueable-mode jobs.

**API Name**

`AsyncApexJobId__c`

**Type**

_Text_

---

### Attempts Made

Number of attempts already used by this job.

**API Name**

`AttemptsMade__c`

**Type**

_Number_

---

### Available At

Earliest time the batch worker may claim this job.

**API Name**

`AvailableAt__c`

**Type**

_DateTime_

---

### Backoff Jitter

Retry backoff jitter ratio used to spread retry eligibility inside the calculated backoff window.

**API Name**

`BackoffJitter__c`

**Type**

_Number_

---

### Backoff Type

Retry backoff strategy selected for this job.

**API Name**

`BackoffType__c`

**Type**

_Picklist_

#### Possible values are

- EXPONENTIAL
- FIXED
- NONE

---

### Backoff Value

Base retry backoff value used together with the selected backoff type.

**API Name**

`BackoffValue__c`

**Type**

_Number_

---

### Category

Optional indexed job category for business SOQL lookups.

**API Name**

`Category__c`

**Type**

_Text_

---

### Checkpoint

Serialized processor-defined state persisted between cooperative job executions.

**API Name**

`Checkpoint__c`

**Type**

_LongTextArea_

---

### Correlation Id

Optional indexed correlation id for tracing work across systems or business processes.

**API Name**

`CorrelationId__c`

**Type**

_Text_

---

### Data

Serialized job payload persisted for processor execution.

**API Name**

`Data__c`

**Type**

_LongTextArea_

---

### Dependency Canceled Policy

**Required**

Controls how this child job affects its parent when the child is canceled.

**API Name**

`DependencyCanceledPolicy__c`

**Type**

_Picklist_

#### Possible values are

- CANCEL_PARENT
- DEFAULT
- FAIL_PARENT
- IGNORE_DEPENDENCY

---

### Dependency Failure Policy

**Required**

Controls how this child job affects its parent when the child fails.

**API Name**

`DependencyFailurePolicy__c`

**Type**

_Picklist_

#### Possible values are

- CONTINUE_PARENT
- DEFAULT
- FAIL_PARENT
- IGNORE_DEPENDENCY
- REMOVE_DEPENDENCY

---

### Dependency Repair Checked At

Timestamp when maintenance last checked whether this waiting parent could be released.

**API Name**

`DependencyRepairCheckedAt__c`

**Type**

_DateTime_

---

### Execution Mode

**Required**

Required execution lane. WORKER uses the durable batch worker. QUEUEABLE_SERIAL and QUEUEABLE_CONCURRENT use isolated queueable transport. INVOCABLE uses Flow Scheduled Path handoff.

**API Name**

`ExecutionMode__c`

**Type**

_Picklist_

#### Possible values are

- INVOCABLE
- QUEUEABLE_CONCURRENT
- QUEUEABLE_SERIAL
- WORKER

---

### External Id

Optional caller-provided external identifier used for dedupe or idempotent enqueue behavior. Length is capped so QueueName**c + &quot;:&quot; + ExternalId**c fits the internal queue-scoped unique key.

**API Name**

`ExternalId__c`

**Type**

_Text_

---

### Failed Reason

Failure reason captured from the most recent failed attempt.

**API Name**

`FailedReason__c`

**Type**

_LongTextArea_

---

### Group Key

Optional indexed business grouping key, such as an order, invoice, customer, or tenant id.

**API Name**

`GroupKey__c`

**Type**

_Text_

---

### Job Scheduler

Optional scheduler provenance for jobs materialized from JobScheduler\_\_c.

**API Name**

`JobScheduler__c`

**Type**

_Lookup_

---

### Lease Expires At

Time when the current lease expires and runtime recovery may treat the job as stalled.

**API Name**

`LeaseExpiresAt__c`

**Type**

_DateTime_

---

### Lease Token

Opaque lease ownership token used to prove that the current worker still owns this active job.

**API Name**

`LeaseToken__c`

**Type**

_Text_

---

### Max Attempts

Maximum number of attempts allowed before the job is terminally failed.

**API Name**

`MaxAttempts__c`

**Type**

_Number_

---

### Name

**Required**

Logical job name resolved within a queue.

**API Name**

`Name__c`

**Type**

_Text_

---

### Parent

Optional parent job reference reserved for dependency graph provenance.

**API Name**

`Parent__c`

**Type**

_Lookup_

---

### Pause Requested

Signals that a cooperative processor should stop at a safe boundary and defer remaining work.

**API Name**

`PauseRequested__c`

**Type**

_Checkbox_

---

### Priority

Lower numbers should be claimed ahead of higher numbers.

**API Name**

`Priority__c`

**Type**

_Number_

---

### Processor Class

**Required**

Fully qualified Apex class name resolved for the job attempt.

**API Name**

`ProcessorClass__c`

**Type**

_Text_

---

### Progress

Serialized progress information reported by the processor.

**API Name**

`Progress__c`

**Type**

_LongTextArea_

---

### Queueable Dispatch Initiated At

Timestamp when the queueable dispatcher first submitted this job to a queueable executor.

**API Name**

`QueueableDispatchInitiatedAt__c`

**Type**

_DateTime_

---

### Queueable Dispatch

Ephemeral dispatch envelope that submitted this job through queueable transport.

**API Name**

`QueueableDispatch__c`

**Type**

_Lookup_

---

### Queue Name

**Required**

Logical queue name that owns this job.

**API Name**

`QueueName__c`

**Type**

_Text_

---

### Queue Scoped External Id

Internal queue-scoped unique key composed from QueueName**c and ExternalId**c for race-safe idempotent enqueue behavior.

**API Name**

`QueueScopedExternalId__c`

**Type**

_Text_

---

### Return Value

Serialized processor return payload for completed jobs.

**API Name**

`ReturnValue__c`

**Type**

_LongTextArea_

---

### Stalled Count

Number of times the sweeper has recovered this job from an expired lease.

**API Name**

`StalledCount__c`

**Type**

_Number_

---

### State

**Required**

Durable runtime lifecycle state for this job.

**API Name**

`State__c`

**Type**

_Picklist_

#### Possible values are

- ACTIVE
- CANCELED
- COMPLETED
- DELAYED
- FAILED
- PAUSED
- WAITING
- WAITING_CHILDREN
