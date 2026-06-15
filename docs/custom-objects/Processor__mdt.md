# Processor

Registers job processor classes and execution settings used by the Apex queue runtime.

## API Name

`Processor__mdt`

## Fields

### Description

Optional description of the processor behavior and expected work.

**API Name**

`Description__c`

**Type**

_LongTextArea_

---

### Is Active

Controls whether this queue and job mapping may be resolved to an Apex processor at runtime.

**API Name**

`IsActive__c`

**Type**

_Checkbox_

---

### Name

**Required**

Job name handled by the registered processor mapping.

**API Name**

`Name__c`

**Type**

_Text_

---

### Processor Class

**Required**

Fully qualified Apex class name that implements the queue processor contract.

**API Name**

`ProcessorClass__c`

**Type**

_Text_
