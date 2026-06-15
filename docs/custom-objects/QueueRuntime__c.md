# Queue Runtime

Stores mutable queue-level runtime state for the runner control loop, including pause status, wake timing, runner identity, sweep timing, and active worker counts.

## API Name

`QueueRuntime__c`

## Fields

### Active Worker Count

Cached count of in-flight worker executions currently associated with this queue.

**API Name**

`ActiveWorkerCount__c`

**Type**

_Number_

---

### Async Apex Job Id

Async Apex job identifier for the batch or scheduled wake currently carrying the queue runner plan.

**API Name**

`AsyncApexJobId__c`

**Type**

_Text_

---

### Is Paused

Stops the worker loop from claiming new jobs for this queue while in-flight work finishes.

**API Name**

`IsPaused__c`

**Type**

_Checkbox_

---

### Last Pump At

Last time the queue worker loop touched or refreshed this runtime row.

**API Name**

`LastPumpAt__c`

**Type**

_DateTime_

---

### Last Sweep At

Last time the sweeper inspected the queue for stalled jobs.

**API Name**

`LastSweepAt__c`

**Type**

_DateTime_

---

### Next Pump At

Next intended wake time for the queue worker loop.

**API Name**

`NextPumpAt__c`

**Type**

_DateTime_

---

### Queue Name

**Required**

Logical queue name that this runtime record governs.

**API Name**

`QueueName__c`

**Type**

_Text_

---

### Worker Token

Opaque token that identifies the currently valid queue worker plan and invalidates stale or replaced wakes.

**API Name**

`WorkerToken__c`

**Type**

_Text_
