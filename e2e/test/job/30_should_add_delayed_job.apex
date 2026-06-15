String queueName = 'e2e-job-probe';
String processorName = 'job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String jobExternalId = 'job-delayed';
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

Datetime currentNow = System.now();

// Act
Queues.Job job = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'delayed-payload' },
        new Queues.JobOptions().jobId(jobExternalId).delay(5)
    );
Job__c delayedJob = [
    SELECT Id, State__c, AvailableAt__c, ExternalId__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];
QueueRuntime__c runtime = [
    SELECT Id, IsPaused__c, WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];

// Assert
Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(delayedJob.State__c));
Assert.areEqual(jobExternalId, delayedJob.ExternalId__c);
Assert.areEqual(true, delayedJob.AvailableAt__c > currentNow.addMinutes(4));
Assert.areEqual(false, runtime.IsPaused__c);
Assert.isTrue(String.isNotBlank(runtime.WorkerToken__c));
Assert.isTrue(String.isNotBlank(runtime.AsyncApexJobId__c));
Assert.areEqual(true, runtime.NextPumpAt__c > currentNow.addMinutes(4));

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job.delayed',
            'queueName' => queueName,
            'jobId' => delayedJob.Id,
            'cleanup' => cleanupMessages
        }
    )
);
