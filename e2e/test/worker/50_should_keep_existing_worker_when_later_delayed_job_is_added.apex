String queueName = 'e2e-worker-probe';
String processorName = 'worker-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';

Queues.Job firstJob = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'first-delayed' },
        new Queues.JobOptions().jobId('worker-delayed-first').delay(2)
    );

Job__c firstStoredJob = [
    SELECT Id, State__c, AvailableAt__c
    FROM Job__c
    WHERE Id = :firstJob.getId()
    LIMIT 1
];
QueueRuntime__c runtimeAfterFirstJob = [
    SELECT WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c, IsPaused__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];

Queues.Job secondJob = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'second-delayed' },
        new Queues.JobOptions().jobId('worker-delayed-second').delay(5)
    );

Job__c secondStoredJob = [
    SELECT Id, State__c, AvailableAt__c
    FROM Job__c
    WHERE Id = :secondJob.getId()
    LIMIT 1
];
QueueRuntime__c runtimeAfterSecondJob = [
    SELECT WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c, IsPaused__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];
Integer jobRunCount = [SELECT COUNT() FROM JobRun__c WHERE Job__r.QueueName__c = :queueName];

Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(firstStoredJob.State__c));
Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(secondStoredJob.State__c));
Assert.areEqual(false, runtimeAfterFirstJob.IsPaused__c);
Assert.areEqual(false, runtimeAfterSecondJob.IsPaused__c);
Assert.isTrue(String.isNotBlank(runtimeAfterFirstJob.WorkerToken__c));
Assert.isTrue(String.isNotBlank(runtimeAfterFirstJob.AsyncApexJobId__c));
Assert.areEqual(runtimeAfterFirstJob.WorkerToken__c, runtimeAfterSecondJob.WorkerToken__c);
Assert.areEqual(runtimeAfterFirstJob.AsyncApexJobId__c, runtimeAfterSecondJob.AsyncApexJobId__c);
Assert.areEqual(runtimeAfterFirstJob.NextPumpAt__c, runtimeAfterSecondJob.NextPumpAt__c);
Assert.isTrue(runtimeAfterSecondJob.NextPumpAt__c < secondStoredJob.AvailableAt__c);
Assert.areEqual(0, jobRunCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'worker.keep-existing-delayed-worker',
            'queueName' => queueName,
            'firstJobId' => firstStoredJob.Id,
            'secondJobId' => secondStoredJob.Id,
            'workerToken' => runtimeAfterSecondJob.WorkerToken__c,
            'nextPumpAt' => String.valueOf(runtimeAfterSecondJob.NextPumpAt__c)
        }
    )
);
