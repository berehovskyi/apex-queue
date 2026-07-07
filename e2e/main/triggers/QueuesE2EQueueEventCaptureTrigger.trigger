trigger QueuesE2EQueueEventCaptureTrigger on QueueEvent__e(after insert) {
    List<QueueEventCapture__c> captures = new List<QueueEventCapture__c>();
    for (QueueEvent__e eventRecord : Trigger.new) {
        if (eventRecord.QueueName__c != 'e2e-queue-events') {
            continue;
        }

        captures.add(
            new QueueEventCapture__c(
                EventUuid__c = eventRecord.EventUuid,
                Type__c = eventRecord.Type__c,
                QueueName__c = eventRecord.QueueName__c,
                JobId__c = eventRecord.JobId__c,
                ExternalId__c = eventRecord.ExternalId__c,
                CorrelationId__c = eventRecord.CorrelationId__c,
                State__c = eventRecord.State__c,
                OccurredAt__c = eventRecord.OccurredAt__c
            )
        );
    }

    if (!captures.isEmpty()) {
        insert captures;
    }
}
