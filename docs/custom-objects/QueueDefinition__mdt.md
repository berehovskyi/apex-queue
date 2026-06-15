# Queue Definition

Defines queue-level defaults for the Apex queue runtime.

## API Name

`QueueDefinition__mdt`

## Fields

### Batch Claim Size

Maximum number of due jobs batch.start() may claim for this queue before any execute scopes begin.

**API Name**

`BatchClaimSize__c`

**Type**

_Number_

---

### Default Attempts

Default max attempts to apply when a job does not override attempts in its options.

**API Name**

`DefaultAttempts__c`

**Type**

_Number_

---

### Default Backoff Jitter

Default retry backoff jitter ratio used to spread retry eligibility inside the calculated backoff window.

**API Name**

`DefaultBackoffJitter__c`

**Type**

_Number_

---

### Default Backoff Type

Default retry backoff strategy for jobs that do not provide one explicitly.

**API Name**

`DefaultBackoffType__c`

**Type**

_Picklist_

#### Possible values are

- EXPONENTIAL
- FIXED
- NONE

---

### Default Backoff Value

Base retry backoff value used with the selected default backoff strategy.

**API Name**

`DefaultBackoffValue__c`

**Type**

_Number_

---

### Default Lease Minutes

Default lease duration assigned when the batch worker claims a job for execution.

**API Name**

`DefaultLeaseMinutes__c`

**Type**

_Number_

---

### Description

Optional description of the queue purpose and ownership.

**API Name**

`Description__c`

**Type**

_LongTextArea_

---

### Is Active

Controls whether the queue definition is available for enqueue, claim, and scheduler operations.

**API Name**

`IsActive__c`

**Type**

_Checkbox_

---

### Queue Name

**Required**

Stable queue API name used by queue facades and runtime services.

**API Name**

`QueueName__c`

**Type**

_Text_
