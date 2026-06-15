Set<String> queueNames = new Set<String>{
    'e2e-performance-worker',
    'e2e-performance-worker-parallel-1',
    'e2e-performance-worker-parallel-2',
    'e2e-performance-worker-parallel-3',
    'e2e-performance-worker-parallel-4',
    'e2e-performance-worker-parallel-5',
    'e2e-performance-queueable',
    'e2e-performance-queueable-concurrent',
    'e2e-performance-invocable'
};
List<String> cleanupMessages = new List<String>();
Set<String> asyncJobIds = new Set<String>();

for (Job__c job : [
    SELECT AsyncApexJobId__c
    FROM Job__c
    WHERE QueueName__c IN :queueNames AND AsyncApexJobId__c != NULL
]) {
    asyncJobIds.add(job.AsyncApexJobId__c);
}
for (QueueableDispatch__c dispatch : [
    SELECT DispatcherAsyncApexJobId__c
    FROM QueueableDispatch__c
    WHERE QueueName__c IN :queueNames AND DispatcherAsyncApexJobId__c != NULL
]) {
    asyncJobIds.add(dispatch.DispatcherAsyncApexJobId__c);
}
for (QueueRuntime__c runtime : [
    SELECT AsyncApexJobId__c
    FROM QueueRuntime__c
    WHERE QueueName__c IN :queueNames AND AsyncApexJobId__c != NULL
]) {
    asyncJobIds.add(runtime.AsyncApexJobId__c);
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
    WHERE QueueName__c IN :queueNames
];
if (!queueErrors.isEmpty()) {
    delete queueErrors;
}

List<Job__c> jobs = [
    SELECT Id
    FROM Job__c
    WHERE QueueName__c IN :queueNames
];
if (!jobs.isEmpty()) {
    delete jobs;
}

List<QueueableDispatch__c> dispatches = [
    SELECT Id
    FROM QueueableDispatch__c
    WHERE QueueName__c IN :queueNames
];
if (!dispatches.isEmpty()) {
    delete dispatches;
}

List<QueueRuntime__c> runtimes = [
    SELECT Id
    FROM QueueRuntime__c
    WHERE QueueName__c IN :queueNames
];
if (!runtimes.isEmpty()) {
    delete runtimes;
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'performance.cleanup',
            'queueNames' => queueNames,
            'aborts' => cleanupMessages,
            'deletedErrors' => queueErrors.size(),
            'deletedJobs' => jobs.size(),
            'deletedDispatches' => dispatches.size(),
            'deletedRuntimes' => runtimes.size()
        }
    )
);
