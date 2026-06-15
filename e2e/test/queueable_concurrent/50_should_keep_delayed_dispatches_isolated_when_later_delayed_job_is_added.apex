String queueName = 'e2e-queueable-concurrent-probe';
String processorName = 'queueable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
Set<String> externalIds = new Set<String>{ 'queueable-delayed-first', 'queueable-delayed-second' };

Queues.Job firstJob = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'first-delayed' },
        new Queues.JobOptions().jobId('queueable-delayed-first').delay(1),
        new Queues.BulkJobOptions().queueableConcurrent()
    );

Queues.Job secondJob = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'second-delayed' },
        new Queues.JobOptions().jobId('queueable-delayed-second').delay(2),
        new Queues.BulkJobOptions().queueableConcurrent()
    );

List<Job__c> storedJobs = [
    SELECT Id, ExternalId__c, State__c, AvailableAt__c, ExecutionMode__c, QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
    ORDER BY ExternalId__c ASC
];

Assert.isTrue(firstJob.getId() != null);
Assert.isTrue(secondJob.getId() != null);
Assert.areEqual(externalIds.size(), storedJobs.size());

Set<Id> dispatchIds = new Set<Id>();
for (Job__c job : storedJobs) {
    Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), job.ExecutionMode__c);
    Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(job.State__c));
    Assert.areNotEqual(null, job.QueueableDispatch__c);
    Assert.isTrue(job.AvailableAt__c > System.now());
    dispatchIds.add(job.QueueableDispatch__c);
}
Assert.areEqual(externalIds.size(), dispatchIds.size());

List<QueueableDispatch__c> dispatches = [
    SELECT State__c, RequestedCount__c, DispatchedCount__c, DispatcherAsyncApexJobId__c
    FROM QueueableDispatch__c
    WHERE Id IN :dispatchIds
];
Assert.areEqual(externalIds.size(), dispatches.size());
for (QueueableDispatch__c dispatch : dispatches) {
    Assert.areEqual(Queues.QueueableDispatchState.WAITING.name(), dispatch.State__c);
    Assert.areEqual(1, QueuesSupport.toInteger(dispatch.RequestedCount__c, 0));
    Assert.areEqual(0, QueuesSupport.toInteger(dispatch.DispatchedCount__c, 0));
    Assert.isTrue(String.isNotBlank(dispatch.DispatcherAsyncApexJobId__c));
}

Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Integer jobRunCount = [SELECT COUNT() FROM JobRun__c WHERE Job__r.QueueName__c = :queueName];
Assert.areEqual(0, runtimeCount);
Assert.areEqual(0, jobRunCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queueable_concurrent.keep-delayed-dispatches-isolated',
            'queueName' => queueName,
            'firstJobId' => firstJob.getId(),
            'secondJobId' => secondJob.getId(),
            'dispatchIds' => new List<Id>(dispatchIds)
        }
    )
);
