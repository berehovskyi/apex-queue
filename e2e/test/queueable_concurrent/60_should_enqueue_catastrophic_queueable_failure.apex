String queueName = 'e2e-queueable-concurrent-probe';

Queues.Job job = Queues.of(queueName)
    .add(
        'broken-queueable-probe',
        'QueuesE2ECatastrophicProcessor',
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'catastrophic-queueable-failure' },
        new Queues.JobOptions().jobId('queueable-catastrophic').attempts(1),
        new Queues.BulkJobOptions().queueableConcurrent()
    );

Job__c storedJob = [
    SELECT Id, ExecutionMode__c, QueueableDispatch__c
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
Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), storedJob.ExecutionMode__c);
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
            'feature' => 'queueable_concurrent.catastrophic.enqueue',
            'queueName' => queueName,
            'jobId' => job.getId(),
            'dispatchId' => storedJob.QueueableDispatch__c,
            'dispatcherAsyncApexJobId' => dispatch.DispatcherAsyncApexJobId__c
        }
    )
);
