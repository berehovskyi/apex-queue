String queueName = 'e2e-weather-sync';
String processorName = 'fetch-weather';
String processorClass = 'QueuesE2EWeatherProcessor';
String jobExternalId = 'queue-add-job-kyiv';
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

Queues.Job job = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{ 'city' => 'Kyiv' },
        new Queues.JobOptions().jobId(jobExternalId)
    );

// Act
Job__c createdJob = [
    SELECT Id, QueueName__c, Name__c, State__c, ExternalId__c, QueueScopedExternalId__c, Data__c, ProcessorClass__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];
QueueRuntime__c runtimeAfterAdd = [
    SELECT Id, QueueName__c, IsPaused__c, WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];
Integer totalJobs = [SELECT COUNT() FROM Job__c WHERE QueueName__c = :queueName];

// Assert
Assert.areEqual(1, totalJobs);
Assert.areEqual(job.getId(), createdJob.Id);
Assert.areEqual(queueName, createdJob.QueueName__c);
Assert.areEqual(processorName, createdJob.Name__c);
Assert.areEqual('WAITING', createdJob.State__c);
Assert.areEqual(jobExternalId, createdJob.ExternalId__c);
Assert.areEqual(queueName + ':' + jobExternalId, createdJob.QueueScopedExternalId__c);
Assert.isTrue(createdJob.Data__c.contains('"city":"Kyiv"'));
Assert.areEqual('QueuesE2EWeatherProcessor', createdJob.ProcessorClass__c);
Assert.areEqual(queueName, runtimeAfterAdd.QueueName__c);
Assert.areEqual(false, runtimeAfterAdd.IsPaused__c);
Assert.isTrue(String.isNotBlank(runtimeAfterAdd.WorkerToken__c));
Assert.isTrue(String.isNotBlank(runtimeAfterAdd.AsyncApexJobId__c));
Assert.isTrue(runtimeAfterAdd.NextPumpAt__c != null);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queue.add',
            'queueName' => queueName,
            'jobId' => createdJob.Id,
            'externalId' => createdJob.ExternalId__c,
            'cleanup' => cleanupMessages
        }
    )
);
