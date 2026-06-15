String queueName = 'e2e-worker-probe';
String processorName = 'worker-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String externalId = 'worker:retry:max-10';

Queues.Job job = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{
            'mode' => 'fail',
            'message' => 'Worker max retry processor failure.',
            'includeIdempotencyKeyInFailureMessage' => true
        },
        new Queues.JobOptions().jobId(externalId).attempts(10).backoff(Queues.Backoff.NONE, 0)
    );

Job__c storedJob = [
    SELECT Id, ExternalId__c, ExecutionMode__c, MaxAttempts__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];
QueueRuntime__c runtime = [
    SELECT WorkerToken__c, AsyncApexJobId__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];

Assert.isTrue(job.getId() != null);
Assert.areEqual(externalId, storedJob.ExternalId__c);
Assert.areEqual(Queues.ExecutionMode.WORKER.name(), storedJob.ExecutionMode__c);
Assert.areEqual(10, QueuesSupport.toInteger(storedJob.MaxAttempts__c, 0));
Assert.isTrue(String.isNotBlank(runtime.WorkerToken__c));
Assert.isTrue(String.isNotBlank(runtime.AsyncApexJobId__c));

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'worker.retry.max-10.enqueue',
            'queueName' => queueName,
            'externalId' => externalId,
            'jobId' => job.getId(),
            'workerToken' => runtime.WorkerToken__c,
            'asyncApexJobId' => runtime.AsyncApexJobId__c
        }
    )
);
