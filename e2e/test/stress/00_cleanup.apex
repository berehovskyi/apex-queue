Set<String> queueNames = QueuesE2EStressSupport.queueNames();
String schedulerNamePattern = QueuesE2EStressSupport.SCHEDULER_NAME_PREFIX + '%';
List<String> cleanupMessages = new List<String>();
Set<String> asyncJobIds = new Set<String>();
List<JobScheduler__c> schedulers = [
    SELECT Id, ScheduledJobId__c
    FROM JobScheduler__c
    WHERE Name LIKE :schedulerNamePattern
];
Set<Id> schedulerIds = new Map<Id, JobScheduler__c>(schedulers).keySet();

for (JobScheduler__c scheduler : schedulers) {
    if (String.isNotBlank(scheduler.ScheduledJobId__c)) {
        asyncJobIds.add(scheduler.ScheduledJobId__c);
    }
}
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
        cleanupMessages.add('Skipped abort for async job ' + asyncJobId + ': ' + ex.getMessage());
    }
}

List<QueueError__c> queueErrors = [SELECT Id FROM QueueError__c WHERE QueueName__c IN :queueNames];
if (!queueErrors.isEmpty()) {
    delete queueErrors;
}
List<Job__c> jobs = [
    SELECT Id
    FROM Job__c
    WHERE QueueName__c IN :queueNames OR JobScheduler__c IN :schedulerIds
];
if (!jobs.isEmpty()) {
    delete jobs;
}
if (!schedulers.isEmpty()) {
    delete schedulers;
}
List<QueueableDispatch__c> dispatches = [SELECT Id FROM QueueableDispatch__c WHERE QueueName__c IN :queueNames];
if (!dispatches.isEmpty()) {
    delete dispatches;
}
List<QueueRuntime__c> runtimes = [SELECT Id FROM QueueRuntime__c WHERE QueueName__c IN :queueNames];
if (!runtimes.isEmpty()) {
    delete runtimes;
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'stress.cleanup',
            'aborts' => cleanupMessages,
            'deletedErrors' => queueErrors.size(),
            'deletedJobs' => jobs.size(),
            'deletedSchedulers' => schedulers.size(),
            'deletedDispatches' => dispatches.size(),
            'deletedRuntimes' => runtimes.size()
        }
    )
);
