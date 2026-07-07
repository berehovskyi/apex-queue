String queueName = 'e2e-queue-events';
String correlationId = 'queue-events:happy-path';
String completedExternalId = 'queue-events:completed';
String failedExternalId = 'queue-events:failed';
String canceledExternalId = 'queue-events:canceled';
Set<String> externalIds = new Set<String>{ completedExternalId, failedExternalId, canceledExternalId };

Map<String, String> expectedJobStates = new Map<String, String>{
    completedExternalId => Queues.State.COMPLETED.name(),
    failedExternalId => Queues.State.FAILED.name(),
    canceledExternalId => Queues.State.CANCELED.name()
};
Map<String, Set<String>> expectedEventTypes = new Map<String, Set<String>>{
    completedExternalId => new Set<String>{ 'ACTIVE', 'COMPLETED' },
    failedExternalId => new Set<String>{ 'ACTIVE', 'FAILED' },
    canceledExternalId => new Set<String>{ 'CANCELED' }
};

Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
for (Job__c job : [
    SELECT Id, ExternalId__c, State__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
]) {
    jobsByExternalId.put(job.ExternalId__c, job);
}
Assert.areEqual(externalIds.size(), jobsByExternalId.size());
for (String externalId : externalIds) {
    Assert.areEqual(expectedJobStates.get(externalId), jobsByExternalId.get(externalId).State__c);
}

Map<String, QueueEventCapture__c> capturesByEventUuid = new Map<String, QueueEventCapture__c>();
for (QueueEventCapture__c capture : [
    SELECT EventUuid__c, Type__c, QueueName__c, JobId__c, ExternalId__c, CorrelationId__c, State__c, OccurredAt__c
    FROM QueueEventCapture__c
    WHERE CorrelationId__c = :correlationId
]) {
    capturesByEventUuid.put(capture.EventUuid__c, capture);
}

Map<String, Set<String>> actualEventTypes = new Map<String, Set<String>>();
for (QueueEventCapture__c capture : capturesByEventUuid.values()) {
    Assert.isTrue(externalIds.contains(capture.ExternalId__c));
    Assert.areEqual(queueName, capture.QueueName__c);
    Assert.areEqual(correlationId, capture.CorrelationId__c);
    Assert.areEqual(jobsByExternalId.get(capture.ExternalId__c).Id, capture.JobId__c);
    Assert.areNotEqual(null, capture.OccurredAt__c);

    String expectedEventState = capture.Type__c == 'ACTIVE'
        ? Queues.State.ACTIVE.name()
        : expectedJobStates.get(capture.ExternalId__c);
    Assert.areEqual(expectedEventState, capture.State__c);

    Set<String> types = actualEventTypes.get(capture.ExternalId__c) ?? new Set<String>();
    types.add(capture.Type__c);
    actualEventTypes.put(capture.ExternalId__c, types);
}

Assert.areEqual(5, capturesByEventUuid.size());
for (String externalId : externalIds) {
    Assert.areEqual(expectedEventTypes.get(externalId), actualEventTypes.get(externalId));
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queue-events.assert',
            'queueName' => queueName,
            'correlationId' => correlationId,
            'uniqueEventCount' => capturesByEventUuid.size(),
            'eventTypes' => actualEventTypes
        }
    )
);
