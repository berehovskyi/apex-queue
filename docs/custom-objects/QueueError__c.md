# Queue Error

Stores queue-level operational failures, optionally linked to the related queue job or native async execution.

## API Name

`QueueError__c`

## Fields

### Async Apex Job Id

Platform async job id associated with the operational failure when the framework can correlate the error to native async execution.

**API Name**

`AsyncApexJobId__c`

**Type**

_Text_

---

### Component

**Required**

Queue subsystem component or phase that failed.

**API Name**

`Component__c`

**Type**

_Text_

---

### Job

Optional lookup to the queue job that was being operated on when this error was recorded.

**API Name**

`Job__c`

**Type**

_Lookup_

---

### Message

Failure message recorded for the queue-level error.

**API Name**

`Message__c`

**Type**

_LongTextArea_

---

### Occurred At

**Required**

Timestamp when the queue-level failure occurred.

**API Name**

`OccurredAt__c`

**Type**

_DateTime_

---

### Queue Name

**Required**

Queue name associated with the failure.

**API Name**

`QueueName__c`

**Type**

_Text_

---

### Stack Trace

Stack trace captured for the queue-level error.

**API Name**

`StackTrace__c`

**Type**

_LongTextArea_
