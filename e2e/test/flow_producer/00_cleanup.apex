Set<String> queueNames = new Set<String>{ 'e2e-job-probe', 'e2e-queueable-probe', 'e2e-worker-probe' };
String externalIdPattern = 'flow-producer:%';
List<String> cleanupMessages = new List<String>();

Set<String> asyncJobIds = new Set<String>();
for (QueueRuntime__c runtime : [
    SELECT AsyncApexJobId__c
    FROM QueueRuntime__c
    WHERE QueueName__c IN :queueNames AND AsyncApexJobId__c != NULL
]) {
    asyncJobIds.add(runtime.AsyncApexJobId__c);
}
for (QueueableDispatch__c dispatch : [
    SELECT DispatcherAsyncApexJobId__c
    FROM QueueableDispatch__c
    WHERE QueueName__c IN :queueNames AND DispatcherAsyncApexJobId__c != NULL
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

List<Job__c> keyedJobs = [
    SELECT Id, QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c IN :queueNames AND ExternalId__c LIKE :externalIdPattern
];
Map<Id, Job__c> jobsById = new Map<Id, Job__c>(keyedJobs);
Set<Id> queriedParentIds = new Set<Id>();
Set<Id> currentParentIds = new Set<Id>();
currentParentIds.addAll(jobsById.keySet());
while (!currentParentIds.isEmpty()) {
    currentParentIds.removeAll(queriedParentIds);
    if (currentParentIds.isEmpty()) {
        break;
    }
    queriedParentIds.addAll(currentParentIds);
    Set<Id> queryParentIds = currentParentIds;
    currentParentIds = new Set<Id>();

    for (Job__c child : [
        SELECT Id, QueueableDispatch__c
        FROM Job__c
        WHERE Parent__c IN :queryParentIds
    ]) {
        jobsById.put(child.Id, child);
        currentParentIds.add(child.Id);
    }
}
List<Job__c> jobs = jobsById.values();
Set<Id> dispatchIds = QueuesSupport.toIdSet(jobs, 'QueueableDispatch__c');

List<QueueError__c> errors = [
    SELECT Id
    FROM QueueError__c
    WHERE QueueName__c IN :queueNames
];
if (!errors.isEmpty()) {
    delete errors;
}

if (!jobs.isEmpty()) {
    delete jobs;
}

List<QueueableDispatch__c> dispatches = dispatchIds.isEmpty()
    ? new List<QueueableDispatch__c>()
    : [
          SELECT Id
          FROM QueueableDispatch__c
          WHERE Id IN :dispatchIds
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
            'feature' => 'flow-producer.cleanup',
            'queues' => new List<String>(queueNames),
            'aborts' => cleanupMessages,
            'deletedErrors' => errors.size(),
            'deletedJobs' => jobs.size(),
            'deletedDispatches' => dispatches.size(),
            'deletedRuntimes' => runtimes.size()
        }
    )
);
