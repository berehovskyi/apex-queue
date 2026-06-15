String queueName = 'e2e-job-probe';
String processorName = 'job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String jobExternalId = 'job-idempotent';
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
Queues.Job firstJob = queue.add(
    processorName,
    processorClass,
    new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'first-payload' },
    new Queues.JobOptions().jobId(jobExternalId)
);

// Act
Queues.Job secondJob = queue.add(
    processorName,
    processorClass,
    new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'second-payload' },
    new Queues.JobOptions().jobId(jobExternalId)
);
List<Job__c> matchedJobs = [
    SELECT Id, Data__c, ExternalId__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c = :jobExternalId
];

// Assert
Assert.areEqual(firstJob.getId(), secondJob.getId());
Assert.areEqual(1, matchedJobs.size());
Map<String, Object> persistedData = (Map<String, Object>) JSON.deserializeUntyped(matchedJobs[0].Data__c);
Assert.areEqual(firstJob.getId(), matchedJobs[0].Id);
Assert.areEqual(jobExternalId, matchedJobs[0].ExternalId__c);
Assert.areEqual('complete', String.valueOf(persistedData.get('mode')));
Assert.areEqual('first-payload', String.valueOf(persistedData.get('payload')));

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job.idempotency',
            'queueName' => queueName,
            'jobId' => firstJob.getId(),
            'cleanup' => cleanupMessages
        }
    )
);
