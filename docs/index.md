# Reference Guide

## Core

### [Queues](core\Queues.md)

Durable, BullMQ-inspired background job framework for Salesforce Apex. `Queues` is the single
supported public facade: it creates queue handles, job handles, and dependency-graph producers,
and exposes org-level maintenance. All committed work is stored durably in `Job__c` , the
framework's source of truth; native async artifacts (Batch, Queueable, Scheduled Apex, Flow)
are treated as wake or handoff hints, never as the source of truth.

Application code should depend only on this class and its nested value objects. Classes under
`classes/internal` are framework internals and may change between releases.

## Custom Objects

### [JobRun\_\_c](custom-objects\JobRun__c.md)

Stores per-attempt execution logs for queue jobs, including timing, status, and operator-facing messages.

### [JobScheduler\_\_c](custom-objects\JobScheduler__c.md)

Stores job schedulers that materialize queue jobs on a repeat cadence.

### [Job\_\_c](custom-objects\Job__c.md)

Stores each durable queue job, including its payload, lifecycle state, retry policy, lease data, and resumable execution fields.

### [Processor\_\_mdt](custom-objects\Processor__mdt.md)

Registers job processor classes and execution settings used by the Apex queue runtime.

### [QueueableDispatch\_\_c](custom-objects\QueueableDispatch__c.md)

Ephemeral queueable handoff envelope for one enqueue or addBulk request. It is not a durable queue runtime.

### [QueueDefinition\_\_mdt](custom-objects\QueueDefinition__mdt.md)

Defines queue-level defaults for the Apex queue runtime.

### [QueueErrorEvent\_\_e](custom-objects\QueueErrorEvent__e.md)

Platform event emitted immediately when the framework records an operational queue error.

### [QueueError\_\_c](custom-objects\QueueError__c.md)

Stores queue-level operational failures, optionally linked to the related queue job or native async execution.

### [QueueEvent\_\_e](custom-objects\QueueEvent__e.md)

Opt-in platform event emitted after durable queue state changes. Events are best-effort telemetry and do not participate in job durability.

### [QueueRuntime\_\_c](custom-objects\QueueRuntime__c.md)

Stores mutable queue-level runtime state for the runner control loop, including pause status, wake timing, runner identity, sweep timing, and active worker counts.

## Triggers

### [QueuesBatchApexErrorEventTrigger](triggers\QueuesBatchApexErrorEventTrigger.md)

### [QueuesQueueErrorEventTrigger](triggers\QueuesQueueErrorEventTrigger.md)
