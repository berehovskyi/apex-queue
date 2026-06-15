String queueName = 'e2e-weather-sync';
String processorName = 'fetch-weather';
String processorClass = 'QueuesE2EWeatherProcessor';
String jobExternalId = 'queue-pause-resume-job-kyiv';
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

// Act
QueueRuntime__c pausedRuntime = [
    SELECT Id, IsPaused__c, WorkerToken__c, AsyncApexJobId__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];
Job__c waitingJobWhilePaused = [
    SELECT Id, State__c, ExternalId__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];

queue.resume();

QueueRuntime__c resumedRuntime = [
    SELECT Id, IsPaused__c, WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];

// Assert
Assert.areEqual(true, pausedRuntime.IsPaused__c);
Assert.isTrue(String.isBlank(pausedRuntime.WorkerToken__c));
Assert.isTrue(String.isBlank(pausedRuntime.AsyncApexJobId__c));
Assert.areEqual('WAITING', waitingJobWhilePaused.State__c);
Assert.areEqual(jobExternalId, waitingJobWhilePaused.ExternalId__c);
Assert.areEqual(false, resumedRuntime.IsPaused__c);
Assert.isTrue(String.isNotBlank(resumedRuntime.WorkerToken__c));
Assert.isTrue(String.isNotBlank(resumedRuntime.AsyncApexJobId__c));
Assert.isTrue(resumedRuntime.NextPumpAt__c != null);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queue.pause-resume',
            'queueName' => queueName,
            'jobId' => job.getId(),
            'cleanup' => cleanupMessages
        }
    )
);
