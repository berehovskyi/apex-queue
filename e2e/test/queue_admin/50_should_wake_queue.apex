String queueName = 'e2e-weather-sync';
String processorName = 'fetch-weather';
String processorClass = 'QueuesE2EWeatherProcessor';
String jobExternalId = 'queue-wake-job-kyiv';
List<String> cleanupMessages = new List<String>();

// Arrange
List<QueueRuntime__c> runtimes = [
    SELECT Id, QueueName__c, AsyncApexJobId__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
];
for (QueueRuntime__c runtime : runtimes) {
    if (String.isBlank(runtime.AsyncApexJobId__c)) {
        continue;
    }

    try {
        System.abortJob(runtime.AsyncApexJobId__c);
        cleanupMessages.add('Aborted worker job ' + runtime.AsyncApexJobId__c + '.');
    } catch (Exception ex) {
        cleanupMessages.add(
            'Skipped abort for worker job ' +
                runtime.AsyncApexJobId__c +
                ': ' +
                QueuesSupport.resolveExceptionReason(ex)
        );
    }
}

List<JobRun__c> existingRuns = [
    SELECT Id
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :queueName
];
if (!existingRuns.isEmpty()) {
    delete existingRuns;
}

List<QueueError__c> existingErrors = [
    SELECT Id
    FROM QueueError__c
    WHERE QueueName__c = :queueName
];
if (!existingErrors.isEmpty()) {
    delete existingErrors;
}

List<Job__c> existingJobs = [
    SELECT Id
    FROM Job__c
    WHERE QueueName__c = :queueName
];
if (!existingJobs.isEmpty()) {
    delete existingJobs;
}

if (!runtimes.isEmpty()) {
    delete runtimes;
}

Queues.Queue queue = Queues.of(queueName);
queue.pause();
Queues.Job job = queue.add(
    processorName,
    processorClass,
    new Map<String, Object>{ 'city' => 'Kyiv' },
    new Queues.JobOptions().jobId(jobExternalId)
);

QueueRuntime__c pausedRuntime = [
    SELECT Id, IsPaused__c, WorkerToken__c, AsyncApexJobId__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];
Boolean wasPaused = pausedRuntime.IsPaused__c;
Boolean hadPausedWorkerToken = String.isNotBlank(pausedRuntime.WorkerToken__c);
Boolean hadPausedAsyncApexJobId = String.isNotBlank(pausedRuntime.AsyncApexJobId__c);
pausedRuntime.IsPaused__c = false;
update pausedRuntime;

// Act
Integer firstWakeCount = queue.wake();
QueueRuntime__c runtimeAfterFirstWake = [
    SELECT Id, IsPaused__c, WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];
Integer secondWakeCount = queue.wake();
Job__c waitingJob = [
    SELECT Id, State__c, ExternalId__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];

// Assert
Assert.areEqual(true, wasPaused);
Assert.areEqual(false, hadPausedWorkerToken);
Assert.areEqual(false, hadPausedAsyncApexJobId);
Assert.areEqual(1, firstWakeCount);
Assert.areEqual(false, runtimeAfterFirstWake.IsPaused__c);
Assert.isTrue(String.isNotBlank(runtimeAfterFirstWake.WorkerToken__c));
Assert.isTrue(String.isNotBlank(runtimeAfterFirstWake.AsyncApexJobId__c));
Assert.isTrue(runtimeAfterFirstWake.NextPumpAt__c != null);
Assert.areEqual(0, secondWakeCount);
Assert.areEqual('WAITING', waitingJob.State__c);
Assert.areEqual(jobExternalId, waitingJob.ExternalId__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queue.wake',
            'queueName' => queueName,
            'jobId' => job.getId(),
            'cleanup' => cleanupMessages
        }
    )
);
