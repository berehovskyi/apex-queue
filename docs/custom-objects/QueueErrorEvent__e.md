# Queue Error Log

Platform event emitted immediately when the framework records an operational queue error.

## API Name

`QueueErrorEvent__e`

## Publish Behavior

**Publish Immediately**

## Fields

### Async Apex Job Id

Native AsyncApexJob id associated with the operational error, when available.

**API Name**

`AsyncApexJobId__c`

**Type**

_Text_

---

### Component

**Required**

Framework component or path that recorded the operational error.

**API Name**

`Component__c`

**Type**

_Text_

---

### Job Id

Salesforce Job\_\_c record id associated with the operational error, when available.

**API Name**

`JobId__c`

**Type**

_Text_

---

### Message

Error message captured by the framework.

**API Name**

`Message__c`

**Type**

_LongTextArea_

---

### Occurred At

**Required**

Framework timestamp for when the operational error was recorded.

**API Name**

`OccurredAt__c`

**Type**

_DateTime_

---

### Queue Name

**Required**

Public queue name associated with the operational error.

**API Name**

`QueueName__c`

**Type**

_Text_

---

### Stack Trace

Stack trace captured for the operational error, when available.

**API Name**

`StackTrace__c`

**Type**

_LongTextArea_
