String queueName = 'e2e-job-probe';
String processorName = 'job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String earlierJobExternalId = 'job-remove-delayed-head-earlier';
String laterJobExternalId = 'job-remove-delayed-head-later';
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
Queues.Job earlierDelayedJob = queue.add(
    processorName,
    processorClass,
    new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'earlier-remove-me' },
    new Queues.JobOptions().jobId(earlierJobExternalId).delay(5)
);
Queues.Job laterDelayedJob = queue.add(
    processorName,
    processorClass,
    new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'later-keep-me' },
    new Queues.JobOptions().jobId(laterJobExternalId).delay(10)
);

QueueRuntime__c scheduledRuntime = [
    SELECT Id, WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c, ActiveWorkerCount__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];
String previousScheduledJobId = scheduledRuntime.AsyncApexJobId__c;
Datetime previousNextPumpAt = scheduledRuntime.NextPumpAt__c;

// Act
earlierDelayedJob.remove();

QueueRuntime__c rescheduledRuntime = [
    SELECT Id, WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c, ActiveWorkerCount__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];
Integer jobCountAfterRemove = [SELECT COUNT() FROM Job__c WHERE QueueName__c = :queueName];
List<Job__c> remainingJobs = [
    SELECT Id, ExternalId__c, AvailableAt__c
    FROM Job__c
    WHERE QueueName__c = :queueName
];
List<CronTrigger> stillActivePreviousSchedules = [
    SELECT Id, State
    FROM CronTrigger
    WHERE Id = :previousScheduledJobId AND State NOT IN :terminalCronStates
];

// Assert
Assert.isTrue(String.isNotBlank(earlierDelayedJob.getId()));
Assert.isTrue(String.isNotBlank(laterDelayedJob.getId()));
Assert.isTrue(String.isNotBlank(previousScheduledJobId));
Assert.isTrue(previousNextPumpAt != null);
Assert.areEqual(0, QueuesSupport.toInteger(scheduledRuntime.ActiveWorkerCount__c, 0));

Assert.areEqual(1, jobCountAfterRemove);
Assert.areEqual(1, remainingJobs.size());
Assert.areEqual(laterJobExternalId, remainingJobs[0].ExternalId__c);
Assert.areEqual(1, stillActivePreviousSchedules.size());
Assert.isTrue(String.isNotBlank(rescheduledRuntime.AsyncApexJobId__c));
Assert.areEqual(previousScheduledJobId, rescheduledRuntime.AsyncApexJobId__c);
Assert.areEqual(remainingJobs[0].AvailableAt__c, rescheduledRuntime.NextPumpAt__c);
Assert.isTrue(rescheduledRuntime.NextPumpAt__c > previousNextPumpAt);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job.remove-keeps-scheduled-worker-when-delayed-head-moves-later',
            'queueName' => queueName,
            'removedJobId' => earlierDelayedJob.getId(),
            'remainingJobId' => laterDelayedJob.getId(),
            'previousScheduledJobId' => previousScheduledJobId,
            'keptScheduledJobId' => rescheduledRuntime.AsyncApexJobId__c,
            'cleanup' => cleanupMessages
        }
    )
);
