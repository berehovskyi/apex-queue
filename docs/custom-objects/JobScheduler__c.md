# Job Scheduler

Stores job schedulers that materialize queue jobs on a repeat cadence.

## API Name

`JobScheduler__c`

## Fields

### Data

Serialized job data used when this scheduler materializes a queue job.

**API Name**

`Data__c`

**Type**

_LongTextArea_

---

### End At

Optional inclusive end of the scheduler run window for every-minute schedulers.

**API Name**

`EndAt__c`

**Type**

_DateTime_

---

### Every Minutes

Granular scheduler cadence in whole minutes. These schedulers self-reschedule one wake at a time.

**API Name**

`EveryMinutes__c`

**Type**

_Number_

---

### Execution Mode

**Required**

Execution lane used by jobs materialized from this scheduler.

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

Internal queue-scoped scheduler identifier composed from QueueName\_\_c and the public scheduler id.

**API Name**

`ExternalId__c`

**Type**

_Text_

---

### Is Active

Whether this scheduler should continue materializing queue jobs.

**API Name**

`IsActive__c`

**Type**

_Checkbox_

---

### Iteration Count

Count of scheduler occurrences that have been materialized into Job\_\_c rows.

**API Name**

`IterationCount__c`

**Type**

_Number_

---

### Job Name

Queue job name assigned to each materialized job.

**API Name**

`JobName__c`

**Type**

_Text_

---

### Last Enqueued At

Timestamp when the scheduler last materialized a queue job.

**API Name**

`LastEnqueuedAt__c`

**Type**

_DateTime_

---

### Limit

Optional maximum number of jobs this scheduler may materialize.

**API Name**

`Limit__c`

**Type**

_Number_

---

### Next Run At

Durable next occurrence represented by this scheduler state.

**API Name**

`NextRunAt__c`

**Type**

_DateTime_

---

### Options

Serialized allowed JobOptions applied to each materialized job.

**API Name**

`Options__c`

**Type**

_LongTextArea_

---

### Pattern

Cron expression for native recurring schedulers.

**API Name**

`Pattern__c`

**Type**

_Text_

---

### Processor Class

Processor class name assigned to each materialized job.

**API Name**

`ProcessorClass__c`

**Type**

_Text_

---

### Queue Name

**Required**

Logical queue name that owns this scheduler.

**API Name**

`QueueName__c`

**Type**

_Text_

---

### Scheduled Job Id

Current Scheduled Apex CronTrigger id that anchors this scheduler chain.

**API Name**

`ScheduledJobId__c`

**Type**

_Text_

---

### Start At

Optional start of the scheduler run window for every-minute schedulers.

**API Name**

`StartAt__c`

**Type**

_DateTime_
