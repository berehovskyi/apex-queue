String queueName = 'e2e-invocable-probe';
String processorName = 'invocable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String externalId = 'invocable:retry:max-5';

Queues.Job job = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{
            'mode' => 'fail',
            'message' => 'Invocable max retry processor failure.',
            'includeIdempotencyKeyInFailureMessage' => true
        },
        new Queues.JobOptions().jobId(externalId).attempts(5).backoff(Queues.Backoff.NONE, 0),
        new Queues.BulkJobOptions().invocable()
    );

Job__c storedJob = [
    SELECT Id, ExternalId__c, ExecutionMode__c, State__c, MaxAttempts__c, LeaseToken__c, LeaseExpiresAt__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];

Assert.isTrue(job.getId() != null);
Assert.areEqual(externalId, storedJob.ExternalId__c);
Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), storedJob.ExecutionMode__c);
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(storedJob.State__c));
Assert.areEqual(5, QueuesSupport.toInteger(storedJob.MaxAttempts__c, 0));
Assert.areEqual(null, storedJob.LeaseToken__c);
Assert.areEqual(null, storedJob.LeaseExpiresAt__c);

Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Integer dispatchCount = [SELECT COUNT() FROM QueueableDispatch__c WHERE QueueName__c = :queueName];
Assert.areEqual(0, runtimeCount);
Assert.areEqual(0, dispatchCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'invocable.retry.max-5.enqueue',
            'queueName' => queueName,
            'externalId' => externalId,
            'jobId' => job.getId()
        }
    )
);
