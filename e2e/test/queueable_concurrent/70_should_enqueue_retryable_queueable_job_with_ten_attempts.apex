String queueName = 'e2e-queueable-concurrent-probe';
String processorName = 'queueable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String externalId = 'queueable:retry:max-10';

Queues.Job job = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{
            'mode' => 'fail',
            'message' => 'Queueable max retry processor failure.',
            'includeIdempotencyKeyInFailureMessage' => true
        },
        new Queues.JobOptions().jobId(externalId).attempts(10).backoff(Queues.Backoff.NONE, 0),
        new Queues.BulkJobOptions().queueableConcurrent()
    );

Job__c storedJob = [
    SELECT Id, ExternalId__c, ExecutionMode__c, QueueableDispatch__c, MaxAttempts__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];
QueueableDispatch__c dispatch = [
    SELECT State__c, RequestedCount__c, DispatchedCount__c, DispatcherAsyncApexJobId__c
    FROM QueueableDispatch__c
    WHERE Id = :storedJob.QueueableDispatch__c
    LIMIT 1
];

Assert.isTrue(job.getId() != null);
Assert.areEqual(externalId, storedJob.ExternalId__c);
Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), storedJob.ExecutionMode__c);
Assert.areEqual(10, QueuesSupport.toInteger(storedJob.MaxAttempts__c, 0));
Assert.areNotEqual(null, storedJob.QueueableDispatch__c);
Assert.areEqual(Queues.QueueableDispatchState.WAITING.name(), dispatch.State__c);
Assert.areEqual(1, QueuesSupport.toInteger(dispatch.RequestedCount__c, 0));
Assert.areEqual(0, QueuesSupport.toInteger(dispatch.DispatchedCount__c, 0));
Assert.isTrue(String.isNotBlank(dispatch.DispatcherAsyncApexJobId__c));

Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Assert.areEqual(0, runtimeCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queueable_concurrent.retry.max-10.enqueue',
            'queueName' => queueName,
            'externalId' => externalId,
            'jobId' => job.getId(),
            'dispatchId' => storedJob.QueueableDispatch__c,
            'dispatcherAsyncApexJobId' => dispatch.DispatcherAsyncApexJobId__c
        }
    )
);
