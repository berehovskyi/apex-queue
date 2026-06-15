String queueName = 'e2e-worker-probe';

Queues.Job job = Queues.of(queueName)
    .add(
        'broken-worker-probe',
        'QueuesE2ECatastrophicProcessor',
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'catastrophic-worker-failure' },
        new Queues.JobOptions().jobId('worker-catastrophic').attempts(1)
    );

QueueRuntime__c runtime = [
    SELECT WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];

Assert.isTrue(job.getId() != null);
Assert.isTrue(String.isNotBlank(runtime.WorkerToken__c));
Assert.isTrue(String.isNotBlank(runtime.AsyncApexJobId__c));

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'worker.catastrophic.enqueue',
            'queueName' => queueName,
            'jobId' => String.valueOf(job.getId()),
            'workerToken' => runtime.WorkerToken__c,
            'asyncApexJobId' => runtime.AsyncApexJobId__c
        }
    )
);
