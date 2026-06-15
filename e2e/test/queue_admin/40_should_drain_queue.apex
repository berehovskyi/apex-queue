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

Queues.Queue queue = Queues.of(queueName);
queue.pause();
queue.add(
    processorName,
    processorClass,
    new Map<String, Object>{ 'city' => 'Kyiv' },
    new Queues.JobOptions().jobId('queue-drain-job-kyiv')
);
queue.add(
    processorName,
    processorClass,
    new Map<String, Object>{ 'city' => 'London' },
    new Queues.JobOptions().jobId('queue-drain-job-london')
);

Integer jobCountBeforeDrain = [SELECT COUNT() FROM Job__c WHERE QueueName__c = :queueName];

// Act
Integer drainedCount = queue.drain();
Integer jobCountAfterDrain = [SELECT COUNT() FROM Job__c WHERE QueueName__c = :queueName];
Integer runCountAfterDrain = [
    SELECT COUNT()
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :queueName
];

// Assert
Assert.areEqual(2, jobCountBeforeDrain);
Assert.areEqual(2, drainedCount);
Assert.areEqual(0, jobCountAfterDrain);
Assert.areEqual(0, runCountAfterDrain);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queue.drain',
            'queueName' => queueName,
            'drainedCount' => drainedCount,
            'cleanup' => cleanupMessages
        }
    )
);
