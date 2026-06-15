String queueName = 'e2e-queueable-concurrent-probe';
String processorName = 'queueable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String externalId = 'queueable:retry:failure';

Queues.Job job = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{
            'mode' => 'fail',
            'message' => 'Queueable retryable processor failure.',
            'includeIdempotencyKeyInFailureMessage' => true
        },
        new Queues.JobOptions().jobId(externalId).attempts(3).backoff(Queues.Backoff.NONE, 0),
        new Queues.BulkJobOptions().queueableConcurrent()
    );

Job__c storedJob = [
    SELECT Id, ExternalId__c, ExecutionMode__c, QueueableDispatch__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];

Assert.areEqual(externalId, storedJob.ExternalId__c);
Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), storedJob.ExecutionMode__c);
Assert.areNotEqual(null, storedJob.QueueableDispatch__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queueable_concurrent.retry.enqueue',
            'queueName' => queueName,
            'externalId' => externalId,
            'jobId' => job.getId(),
            'dispatchId' => storedJob.QueueableDispatch__c
        }
    )
);
