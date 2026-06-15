# Queueable Dispatch

Ephemeral queueable handoff envelope for one enqueue or addBulk request. It is not a durable queue runtime.

## API Name

`QueueableDispatch__c`

## Fields

### Dedupe Key

Optional caller-provided dedupe key used when submitting the initial queueable dispatcher.

**API Name**

`DedupeKey__c`

**Type**

_LongTextArea_

---

### Dispatch Depth

Dispatcher continuation depth for this handoff. This is not executor retry depth.

**API Name**

`DispatchDepth__c`

**Type**

_Number_

---

### Dispatched Count

Number of dispatch jobs resolved by this handoff, either by enqueueing an executor or failing the job before enqueue.

**API Name**

`DispatchedCount__c`

**Type**

_Number_

---

### Dispatcher Async Apex Job Id

AsyncApexJob Id of the queueable dispatcher currently advancing this handoff, not an executor job id.

**API Name**

`DispatcherAsyncApexJobId__c`

**Type**

_Text_

---

### Failed Reason

Failure reason captured when dispatcher handoff fails before all jobs are resolved.

**API Name**

`FailedReason__c`

**Type**

_LongTextArea_

---

### Finished At

Time when dispatcher handoff reached COMPLETED or FAILED.

**API Name**

`FinishedAt__c`

**Type**

_DateTime_

---

### Mode

**Required**

Required queueable dispatch execution mode. QUEUEABLE_SERIAL hands off one executor per dispatcher chain step. QUEUEABLE_CONCURRENT fans out executor handoff through future bridge.

**API Name**

`Mode__c`

**Type**

_Picklist_

#### Possible values are

- QUEUEABLE_CONCURRENT
- QUEUEABLE_SERIAL

---

### Queue Name

**Required**

Logical queue name for the queueable-mode jobs in this queueable handoff envelope.

**API Name**

`QueueName__c`

**Type**

_Text_

---

### Requested Count

Number of queueable-mode jobs attached to this dispatch envelope.

**API Name**

`RequestedCount__c`

**Type**

_Number_

---

### Started At

Time when dispatcher handoff first moved from WAITING to ACTIVE.

**API Name**

`StartedAt__c`

**Type**

_DateTime_

---

### State

**Required**

Lifecycle state for this ephemeral queueable dispatch handoff.

**API Name**

`State__c`

**Type**

_Picklist_

#### Possible values are

- ACTIVE
- COMPLETED
- FAILED
- WAITING
