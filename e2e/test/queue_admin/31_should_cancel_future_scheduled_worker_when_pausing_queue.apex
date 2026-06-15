String queueName = 'e2e-weather-sync';
String processorName = 'fetch-weather';
String processorClass = 'QueuesE2EWeatherProcessor';
String delayedJobExternalId = 'queue-pause-cancel-delayed-paris';
String resumeJobExternalId = 'queue-pause-cancel-delayed-london';
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
    SELECT Id, IsPaused__c, WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];
String previousScheduledJobId = scheduledRuntime.AsyncApexJobId__c;

// Act
queue.pause();

QueueRuntime__c pausedRuntime = [
    SELECT Id, IsPaused__c, WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];
List<CronTrigger> activePreviousSchedulesAfterPause = [
    SELECT Id, State
    FROM CronTrigger
    WHERE Id = :previousScheduledJobId AND State NOT IN :terminalCronStates
];

Queues.Job resumedJob = queue.add(
    processorName,
    processorClass,
    new Map<String, Object>{ 'city' => 'London' },
    new Queues.JobOptions().jobId(resumeJobExternalId).delay(10)
);
queue.resume();

QueueRuntime__c resumedRuntime = [
    SELECT Id, IsPaused__c, WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];
List<CronTrigger> activeSchedulesAfterResume = [
    SELECT Id, State
    FROM CronTrigger
    WHERE
        Id IN :new Set<String>{ previousScheduledJobId, resumedRuntime.AsyncApexJobId__c }
        AND State NOT IN :terminalCronStates
];

// Assert
Assert.isTrue(String.isNotBlank(delayedJob.getId()));
Assert.areEqual(false, scheduledRuntime.IsPaused__c);
Assert.isTrue(String.isNotBlank(previousScheduledJobId));
Assert.isTrue(scheduledRuntime.NextPumpAt__c != null);

Assert.areEqual(true, pausedRuntime.IsPaused__c);
Assert.isTrue(String.isBlank(pausedRuntime.WorkerToken__c));
Assert.isTrue(String.isBlank(pausedRuntime.AsyncApexJobId__c));
Assert.areEqual(0, activePreviousSchedulesAfterPause.size());

Assert.isTrue(String.isNotBlank(resumedJob.getId()));
Assert.areEqual(false, resumedRuntime.IsPaused__c);
Assert.isTrue(String.isNotBlank(resumedRuntime.WorkerToken__c));
Assert.isTrue(String.isNotBlank(resumedRuntime.AsyncApexJobId__c));
Assert.areNotEqual(previousScheduledJobId, resumedRuntime.AsyncApexJobId__c);
Assert.areEqual(1, activeSchedulesAfterResume.size());
Assert.areEqual(resumedRuntime.AsyncApexJobId__c, activeSchedulesAfterResume[0].Id);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queue.pause-cancel-future-worker',
            'queueName' => queueName,
            'delayedJobId' => delayedJob.getId(),
            'resumedJobId' => resumedJob.getId(),
            'previousScheduledJobId' => previousScheduledJobId,
            'replacementScheduledJobId' => resumedRuntime.AsyncApexJobId__c,
            'cleanup' => cleanupMessages
        }
    )
);
