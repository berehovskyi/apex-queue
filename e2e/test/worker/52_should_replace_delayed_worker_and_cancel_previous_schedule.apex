String queueName = 'e2e-worker-probe';
String processorName = 'worker-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';

Queues.Job delayedJob = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'replace-delayed' },
        new Queues.JobOptions().jobId('worker-replace-delayed').delay(10)
    );

QueueRuntime__c runtimeAfterDelayedJob = [
    SELECT WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];

String replacedScheduledJobId = runtimeAfterDelayedJob.AsyncApexJobId__c;
String replacedWorkerToken = runtimeAfterDelayedJob.WorkerToken__c;

Queues.Job immediateJob = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'replace-immediate' },
        new Queues.JobOptions().jobId('worker-replace-immediate')
    );

List<CronTrigger> activeReplacedJobs = [
    SELECT Id, State
    FROM CronTrigger
    WHERE Id = :replacedScheduledJobId AND State NOT IN ('DELETED', 'COMPLETE', 'ERROR')
];

QueueRuntime__c runtimeAfterImmediateJob = [
    SELECT WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];

Assert.isTrue(delayedJob.getId() != null);
Assert.isTrue(immediateJob.getId() != null);
Assert.isTrue(String.isNotBlank(replacedScheduledJobId));
Assert.isTrue(String.isNotBlank(replacedWorkerToken));
Assert.areEqual(0, activeReplacedJobs.size());
Assert.areNotEqual(replacedWorkerToken, runtimeAfterImmediateJob.WorkerToken__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'worker.replace-delayed-worker',
            'queueName' => queueName,
            'delayedJobId' => delayedJob.getId(),
            'immediateJobId' => immediateJob.getId(),
            'replacedScheduledJobId' => replacedScheduledJobId,
            'currentWorkerToken' => runtimeAfterImmediateJob.WorkerToken__c
        }
    )
);
