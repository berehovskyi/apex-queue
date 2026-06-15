String queueName = 'e2e-weather-sync';
String processorName = 'fetch-weather';
String processorClass = 'QueuesE2EWeatherProcessor';
String delayedJobExternalId = 'queue-drain-cancel-delayed-paris';
Set<String> terminalCronStates = new Set<String>{ 'DELETED', 'COMPLETE', 'ERROR' };
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
Queues.Job delayedJob = queue.add(
    processorName,
    processorClass,
    new Map<String, Object>{ 'city' => 'Paris' },
    new Queues.JobOptions().jobId(delayedJobExternalId).delay(10)
);

QueueRuntime__c scheduledRuntime = [
    SELECT Id, WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c, ActiveWorkerCount__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];
String previousScheduledJobId = scheduledRuntime.AsyncApexJobId__c;

// Act
Integer drainedCount = queue.drain();

QueueRuntime__c drainedRuntime = [
    SELECT Id, WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c, ActiveWorkerCount__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];
Integer jobCountAfterDrain = [SELECT COUNT() FROM Job__c WHERE QueueName__c = :queueName];
Integer runCountAfterDrain = [
    SELECT COUNT()
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :queueName
];
List<CronTrigger> activeSchedulesAfterDrain = [
    SELECT Id, State
    FROM CronTrigger
    WHERE Id = :previousScheduledJobId AND State NOT IN :terminalCronStates
];

// Assert
Assert.isTrue(String.isNotBlank(delayedJob.getId()));
Assert.isTrue(String.isNotBlank(previousScheduledJobId));
Assert.isTrue(scheduledRuntime.NextPumpAt__c != null);
Assert.areEqual(0, QueuesSupport.toInteger(scheduledRuntime.ActiveWorkerCount__c, 0));

Assert.areEqual(1, drainedCount);
Assert.areEqual(0, jobCountAfterDrain);
Assert.areEqual(0, runCountAfterDrain);
Assert.areEqual(0, activeSchedulesAfterDrain.size());
Assert.isTrue(String.isBlank(drainedRuntime.WorkerToken__c));
Assert.isTrue(String.isBlank(drainedRuntime.AsyncApexJobId__c));
Assert.areEqual(null, drainedRuntime.NextPumpAt__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queue.drain-cancel-future-worker',
            'queueName' => queueName,
            'drainedCount' => drainedCount,
            'delayedJobId' => delayedJob.getId(),
            'previousScheduledJobId' => previousScheduledJobId,
            'cleanup' => cleanupMessages
        }
    )
);
