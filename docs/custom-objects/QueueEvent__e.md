# Queue Event

Opt-in platform event emitted after durable queue state changes. Events are best-effort telemetry and do not participate in job durability.

## API Name

`QueueEvent__e`

## Publish Behavior

**Publish After Commit**

## Fields

### Attempt Number

Attempt number associated with the job run when the event is tied to a processor attempt.

**API Name**

`AttemptNumber__c`

**Type**

_Number_

---

### Category

Optional query dimension copied from the queue job.

**API Name**

`Category__c`

**Type**

_Text_

---

### Component

Framework component or recovery path that emitted the event.

**API Name**

`Component__c`

**Type**

_Text_

---

### Correlation Id

Optional correlation identifier copied from the queue job.

**API Name**

`CorrelationId__c`

**Type**

_Text_

---

### Execution Mode

Execution mode of the queue job when the event is tied to a job.

**API Name**

`ExecutionMode__c`

**Type**

_Text_

---

### External Id

Queue-scoped idempotency key copied from the queue job.

**API Name**

`ExternalId__c`

**Type**

_Text_

---

### Group Key

Optional group key copied from the queue job for business-level lookup.

**API Name**

`GroupKey__c`

**Type**

_Text_

---

### Job Count

Number of jobs represented by an aggregate event. Blank for per-job events.

**API Name**

`JobCount__c`

**Type**

_Number_

---

### Job Id

Salesforce Job\_\_c record id for per-job events. Blank for aggregate events.

**API Name**

`JobId__c`

**Type**

_Text_

---

### Job Name

Queue job name copied from Job\_\_c when the event is tied to a job.

**API Name**

`JobName__c`

**Type**

_Text_

---

### Job Run Id

JobRun\_\_c record id for the attempt associated with this event, when available.

**API Name**

`JobRunId__c`

**Type**

_Text_

---

### Job Scheduler Id

JobScheduler\_\_c record id when the event was produced by scheduler materialization.

**API Name**

`JobSchedulerId__c`

**Type**

_Text_

---

### Occurred At

**Required**

Framework timestamp for when the event was created.

**API Name**

`OccurredAt__c`

**Type**

_DateTime_

---

### Processor Class

Fully qualified Apex processor class copied from the queue job.

**API Name**

`ProcessorClass__c`

**Type**

_Text_

---

### Queue Name

**Required**

Public queue name that owns the event.

**API Name**

`QueueName__c`

**Type**

_Text_

---

### Reason

Failure, cancellation, recovery, or cleanup reason when one is available.

**API Name**

`Reason__c`

**Type**

_LongTextArea_

---

### State

Job state associated with the event when the event is tied to a job.

**API Name**

`State__c`

**Type**

_Text_

---

### Type

**Required**

Queue event type. Valid values are ACTIVE, COMPLETED, FAILED, CANCELED, STALLED, RECOVERED, SCHEDULER_MATERIALIZED, and CLEANED.

**API Name**

`Type__c`

**Type**

_Text_
