String queueName = 'e2e-weather-sync';
String processorName = 'fetch-weather';
String processorClass = 'QueuesE2EWeatherProcessor';
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

List<Queues.JobData> validJobs = new List<Queues.JobData>{
    new Queues.JobData(
        processorName,
        processorClass,
        new Map<String, Object>{ 'city' => 'Kyiv' },
        new Queues.JobOptions().jobId('queue-bulk-job-kyiv')
    ),
    new Queues.JobData(
        processorName,
        processorClass,
        new Map<String, Object>{ 'city' => 'London' },
        new Queues.JobOptions().jobId('queue-bulk-job-london')
    ),
    new Queues.JobData(
        processorName,
        processorClass,
        new Map<String, Object>{ 'city' => 'Tokyo' },
        new Queues.JobOptions().jobId('queue-bulk-job-tokyo')
    )
};

// Act
List<Queues.Job> createdJobs = Queues.of(queueName).addBulk(validJobs);
List<Job__c> createdJobRecords = [
    SELECT Id, ExternalId__c, QueueScopedExternalId__c, State__c
    FROM Job__c
    WHERE QueueName__c = :queueName
    ORDER BY ExternalId__c ASC
];

List<QueueRuntime__c> runtimesAfterSuccess = [
    SELECT Id, AsyncApexJobId__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
];
for (QueueRuntime__c runtime : runtimesAfterSuccess) {
    if (String.isBlank(runtime.AsyncApexJobId__c)) {
        continue;
    }

    try {
        System.abortJob(runtime.AsyncApexJobId__c);
    } catch (Exception ex) {
        cleanupMessages.add(
            'Skipped abort for worker job ' +
                runtime.AsyncApexJobId__c +
                ': ' +
                QueuesSupport.resolveExceptionReason(ex)
        );
    }
}

List<JobRun__c> successRuns = [
    SELECT Id
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :queueName
];
if (!successRuns.isEmpty()) {
    delete successRuns;
}

List<QueueError__c> successErrors = [
    SELECT Id
    FROM QueueError__c
    WHERE QueueName__c = :queueName
];
if (!successErrors.isEmpty()) {
    delete successErrors;
}

if (!createdJobRecords.isEmpty()) {
    delete createdJobRecords;
}

if (!runtimesAfterSuccess.isEmpty()) {
    delete runtimesAfterSuccess;
}

String failureMessage;
try {
    Queues.of(queueName)
        .addBulk(
            new List<Queues.JobData>{
                new Queues.JobData(
                    processorName,
                    processorClass,
                    new Map<String, Object>{ 'city' => 'Sydney' },
                    new Queues.JobOptions().jobId('queue-bulk-failure-sydney')
                ),
                new Queues.JobData(
                    'missing-processor',
                    'QueuesE2EMissingProcessor',
                    new Map<String, Object>{ 'city' => 'Kyiv' },
                    new Queues.JobOptions().jobId('queue-bulk-failure-missing-processor')
                )
            }
        );
    Assert.fail('Expected bulk enqueue with a missing processor to fail.');
} catch (Exception ex) {
    failureMessage = ex.getMessage();
}

Integer remainingJobsAfterFailure = [SELECT COUNT() FROM Job__c WHERE QueueName__c = :queueName];

// Assert
Assert.areEqual(3, createdJobs.size());
Assert.areEqual(3, createdJobRecords.size());
Assert.areEqual('WAITING', createdJobRecords[0].State__c);
Assert.areEqual('WAITING', createdJobRecords[1].State__c);
Assert.areEqual('WAITING', createdJobRecords[2].State__c);
Assert.areEqual(queueName + ':queue-bulk-job-kyiv', createdJobRecords[0].QueueScopedExternalId__c);
Assert.areEqual(queueName + ':queue-bulk-job-london', createdJobRecords[1].QueueScopedExternalId__c);
Assert.areEqual(queueName + ':queue-bulk-job-tokyo', createdJobRecords[2].QueueScopedExternalId__c);
Assert.isTrue(failureMessage.contains('does not have an active processor'));
Assert.areEqual(0, remainingJobsAfterFailure);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queue.addBulk',
            'createdJobCount' => createdJobs.size(),
            'failureMessage' => failureMessage,
            'cleanup' => cleanupMessages
        }
    )
);
