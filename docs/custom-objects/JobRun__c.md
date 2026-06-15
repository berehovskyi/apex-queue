# Job Run

Stores per-attempt execution logs for queue jobs, including timing, status, and operator-facing messages.

## API Name

`JobRun__c`

## Fields

### Async Apex Job Id

Platform async job id associated with this attempt when the framework can correlate it to a native async execution.

**API Name**

`AsyncApexJobId__c`

**Type**

_Text_

---

### Attempt Number

**Required**

1-based attempt number for this run log.

**API Name**

`AttemptNumber__c`

**Type**

_Number_

---

### Duration Ms

Attempt duration in milliseconds.

**API Name**

`DurationMs__c`

**Type**

_Number_

---

### Finished At

Time the worker finished the attempt.

**API Name**

`FinishedAt__c`

**Type**

_DateTime_

---

### Job

Master queue job that owns this attempt log.

**API Name**

`Job__c`

**Type**

_MasterDetail_

---

### Message

Human-readable message captured for the attempt.

**API Name**

`Message__c`

**Type**

_LongTextArea_

---

### Started At

Time the worker began the attempt.

**API Name**

`StartedAt__c`

**Type**

_DateTime_

---

### Status

Outcome status recorded for the job attempt.

**API Name**

`Status__c`

**Type**

_Picklist_

#### Possible values are

- ACTIVE
- COMPLETED
- DEFERRED
- FAILED
- PAUSED
