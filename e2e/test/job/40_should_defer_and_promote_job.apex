String queueName = 'e2e-job-probe';
String processorName = 'job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String jobExternalId = 'job-defer-promote';
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
    new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'defer-promote' },
    new Queues.JobOptions().jobId(jobExternalId)
);

// Act
Datetime deferredBaseline = System.now();
job.defer(5);
Job__c deferredJob = [
    SELECT Id, State__c, AvailableAt__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];

job.promote();
Job__c promotedJob = [
    SELECT Id, State__c, AvailableAt__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];

// Assert
Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(deferredJob.State__c));
Assert.areEqual(true, deferredJob.AvailableAt__c > deferredBaseline.addMinutes(4));
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(promotedJob.State__c));
Assert.areEqual(true, promotedJob.AvailableAt__c <= System.now());

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job.defer-promote',
            'queueName' => queueName,
            'jobId' => promotedJob.Id,
            'cleanup' => cleanupMessages
        }
    )
);
