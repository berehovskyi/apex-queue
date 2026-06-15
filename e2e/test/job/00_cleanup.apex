String queueName = 'e2e-job-probe';
List<String> cleanupMessages = new List<String>();

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

List<QueueError__c> queueErrors = [
    SELECT Id
    FROM QueueError__c
    WHERE QueueName__c = :queueName
];
if (!queueErrors.isEmpty()) {
    delete queueErrors;
}

List<Job__c> jobs = [
    SELECT Id
    FROM Job__c
    WHERE QueueName__c = :queueName
];
if (!jobs.isEmpty()) {
    delete jobs;
}

if (!runtimes.isEmpty()) {
    delete runtimes;
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'queueName' => queueName,
            'aborts' => cleanupMessages,
            'deletedErrors' => queueErrors.size(),
            'deletedJobs' => jobs.size(),
            'deletedRuntimes' => runtimes.size()
        }
    )
);
