String queueName = 'e2e-invocable-probe';
String processorName = 'invocable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String externalId = 'invocable:retry:failure';

Queues.Job job = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{
            'mode' => 'fail',
            'message' => 'Invocable retryable processor failure.',
            'includeIdempotencyKeyInFailureMessage' => true
        },
        new Queues.JobOptions().jobId(externalId).attempts(2).backoff(Queues.Backoff.NONE, 0),
        new Queues.BulkJobOptions().invocable()
    );

Job__c storedJob = [
    SELECT Id, ExternalId__c, ExecutionMode__c, State__c, MaxAttempts__c, LeaseToken__c, LeaseExpiresAt__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];

Assert.areEqual(externalId, storedJob.ExternalId__c);
Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), storedJob.ExecutionMode__c);
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(storedJob.State__c));
Assert.areEqual(2, QueuesSupport.toInteger(storedJob.MaxAttempts__c, 0));
Assert.areEqual(null, storedJob.LeaseToken__c);
Assert.areEqual(null, storedJob.LeaseExpiresAt__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'invocable.retry.enqueue',
            'queueName' => queueName,
            'externalId' => externalId,
            'jobId' => job.getId()
        }
    )
);
