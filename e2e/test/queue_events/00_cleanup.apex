String queueName = 'e2e-queue-events';
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
    } catch (Exception ex) {
        System.debug('Skipped abort for async job ' + asyncJobId + ': ' + QueuesSupport.resolveExceptionReason(ex));
    }
}

List<QueueError__c> errors = [SELECT Id FROM QueueError__c WHERE QueueName__c = :queueName];
if (!errors.isEmpty()) {
    delete errors;
}

List<Job__c> jobs = [SELECT Id FROM Job__c WHERE QueueName__c = :queueName];
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

List<QueueRuntime__c> runtimes = [SELECT Id FROM QueueRuntime__c WHERE QueueName__c = :queueName];
if (!runtimes.isEmpty()) {
    delete runtimes;
}

List<QueueEventCapture__c> captures = [
    SELECT Id
    FROM QueueEventCapture__c
    WHERE QueueName__c = :queueName
];
if (!captures.isEmpty()) {
    delete captures;
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queue-events.cleanup',
            'queueName' => queueName,
            'deletedJobs' => jobs.size(),
            'deletedCaptures' => captures.size()
        }
    )
);
