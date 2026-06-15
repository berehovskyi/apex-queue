String queueName = 'e2e-job-probe';
String processorName = 'job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
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
Queues.Job lowPriorityJob = queue.add(
    processorName,
    processorClass,
    new Map<String, Object>{
        'mode' => 'assert-completed-job',
        'requiredExternalId' => 'job-priority-high',
        'message' => 'Priority ordering violated.'
    },
    new Queues.JobOptions().jobId('job-priority-low').priority(50).attempts(1)
);
Queues.Job highPriorityJob = queue.add(
    processorName,
    processorClass,
    new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'high-priority' },
    new Queues.JobOptions().jobId('job-priority-high').priority(0)
);

// Act
queue.resume();
QueueRuntime__c runtime = [
    SELECT Id, WorkerToken__c, AsyncApexJobId__c, NextPumpAt__c, IsPaused__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];

// Assert
Assert.areNotEqual(lowPriorityJob.getId(), highPriorityJob.getId());
Assert.areEqual(false, runtime.IsPaused__c);
Assert.isTrue(String.isNotBlank(runtime.WorkerToken__c));
Assert.isTrue(String.isNotBlank(runtime.AsyncApexJobId__c));
Assert.isTrue(runtime.NextPumpAt__c != null);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job.priority.enqueue',
            'queueName' => queueName,
            'highPriorityJobId' => highPriorityJob.getId(),
            'lowPriorityJobId' => lowPriorityJob.getId(),
            'cleanup' => cleanupMessages
        }
    )
);
