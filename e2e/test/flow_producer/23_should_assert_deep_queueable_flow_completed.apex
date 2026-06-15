String queueName = 'e2e-queueable-probe';
List<String> executionOrder = new List<String>{
    'flow-producer:queueable:deep:leaf',
    'flow-producer:queueable:deep:parent-two',
    'flow-producer:queueable:deep:parent-one',
    'flow-producer:queueable:deep:root'
};
Set<String> externalIds = new Set<String>(executionOrder);

Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
Set<Id> dispatchIds = new Set<Id>();
for (Job__c job : [
    SELECT ExternalId__c, State__c, FailedReason__c, Progress__c, QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
]) {
    jobsByExternalId.put(job.ExternalId__c, job);
    if (job.QueueableDispatch__c != null) {
        dispatchIds.add(job.QueueableDispatch__c);
    }
}
Assert.areEqual(externalIds.size(), jobsByExternalId.size());

Integer executorDepth;
Integer maximumDepth;
for (String externalId : executionOrder) {
    Job__c job = jobsByExternalId.get(externalId);
    Assert.areEqual(
        Queues.State.COMPLETED.name(),
        job.State__c,
        externalId + ' did not complete: ' + job.FailedReason__c
    );
    Assert.areNotEqual(null, job.QueueableDispatch__c);

    Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(job.Progress__c);
    Integer depth = QueuesSupport.toInteger(progress.get('queueableStackDepth'), 0);
    Integer currentMaximumDepth = QueuesSupport.toInteger(progress.get('maximumQueueableStackDepth'), 0);
    Assert.isTrue(depth > 0, externalId + ' did not capture queueable stack depth.');
    Assert.areEqual(5, currentMaximumDepth);
    executorDepth = executorDepth ?? depth;
    maximumDepth = maximumDepth ?? currentMaximumDepth;
    Assert.areEqual(executorDepth, depth);
}
Assert.isTrue(executionOrder.size() * executorDepth > maximumDepth);
Assert.areEqual(externalIds.size(), dispatchIds.size());

for (QueueableDispatch__c dispatch : [
    SELECT State__c, RequestedCount__c, DispatchedCount__c
    FROM QueueableDispatch__c
    WHERE Id IN :dispatchIds
]) {
    Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), dispatch.State__c);
    Assert.areEqual(dispatch.RequestedCount__c, dispatch.DispatchedCount__c);
}

Integer runCount = [
    SELECT COUNT()
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :queueName AND Job__r.ExternalId__c IN :externalIds
];
Assert.areEqual(externalIds.size(), runCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'flow-producer.queueable.deep.assert',
            'queueName' => queueName,
            'dispatchCount' => dispatchIds.size(),
            'jobCount' => jobsByExternalId.size()
        }
    )
);
