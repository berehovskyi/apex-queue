String queueName = 'e2e-job-probe';
String processorName = 'job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String jobExternalId = 'job-add-update-data';
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
    new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'before-update' },
    new Queues.JobOptions().jobId(jobExternalId)
);

// Act
job.updateData(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'after-update', 'city' => 'Kyiv' });
job.updateProgress(new Map<String, Object>{ 'step' => 'queued', 'percent' => 10 });
Job__c updatedJob = [
    SELECT Id, QueueName__c, Name__c, State__c, ExternalId__c, QueueScopedExternalId__c, Data__c, Progress__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];
Map<String, Object> updatedData = (Map<String, Object>) JSON.deserializeUntyped(updatedJob.Data__c);
Map<String, Object> updatedProgress = (Map<String, Object>) JSON.deserializeUntyped(updatedJob.Progress__c);

// Assert
Assert.areEqual(job.getId(), updatedJob.Id);
Assert.areEqual(queueName, updatedJob.QueueName__c);
Assert.areEqual(processorName, updatedJob.Name__c);
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(updatedJob.State__c));
Assert.areEqual(jobExternalId, updatedJob.ExternalId__c);
Assert.areEqual(queueName + ':' + jobExternalId, updatedJob.QueueScopedExternalId__c);
Assert.areEqual('Kyiv', String.valueOf(updatedData.get('city')));
Assert.areEqual('complete', String.valueOf(updatedData.get('mode')));
Assert.areEqual('after-update', String.valueOf(updatedData.get('payload')));
Assert.areEqual('queued', String.valueOf(updatedProgress.get('step')));
Assert.areEqual(10, Integer.valueOf(String.valueOf(updatedProgress.get('percent'))));

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job.add-update-data-progress',
            'queueName' => queueName,
            'jobId' => updatedJob.Id,
            'cleanup' => cleanupMessages
        }
    )
);
