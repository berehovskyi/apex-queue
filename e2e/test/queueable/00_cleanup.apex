String queueName = 'e2e-queueable-probe';
List<String> cleanupMessages = new List<String>();
Set<String> asyncJobIds = new Set<String>();

for (Job__c job : [
    SELECT AsyncApexJobId__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND AsyncApexJobId__c != NULL
]) {
    asyncJobIds.add(job.AsyncApexJobId__c);
}
for (QueueableDispatch__c dispatch : [
    SELECT DispatcherAsyncApexJobId__c
    FROM QueueableDispatch__c
    WHERE QueueName__c = :queueName AND DispatcherAsyncApexJobId__c != NULL
]) {
    asyncJobIds.add(dispatch.DispatcherAsyncApexJobId__c);
}
for (String asyncJobId : asyncJobIds) {
    try {
        System.abortJob(asyncJobId);
        cleanupMessages.add('Aborted async job ' + asyncJobId + '.');
    } catch (Exception ex) {
        cleanupMessages.add(
            'Skipped abort for async job ' + asyncJobId + ': ' + QueuesSupport.resolveExceptionReason(ex)
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

List<QueueableDispatch__c> dispatches = [
    SELECT Id
    FROM QueueableDispatch__c
    WHERE QueueName__c = :queueName
];
if (!dispatches.isEmpty()) {
    delete dispatches;
}

List<QueueRuntime__c> runtimes = [
    SELECT Id
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
];
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
            'deletedDispatches' => dispatches.size(),
            'deletedRuntimes' => runtimes.size()
        }
    )
);
