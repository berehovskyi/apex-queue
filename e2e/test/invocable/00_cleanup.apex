String queueName = 'e2e-invocable-probe';
List<String> cleanupMessages = new List<String>();
Set<String> asyncJobIds = new Set<String>();

for (Job__c job : [
    SELECT AsyncApexJobId__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND AsyncApexJobId__c != NULL
]) {
    asyncJobIds.add(job.AsyncApexJobId__c);
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

List<JobRun__c> jobRuns = [
    SELECT Id
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :queueName
];

List<QueueError__c> queueErrors = [
    SELECT Id
    FROM QueueError__c
    WHERE QueueName__c = :queueName
];

List<Job__c> jobs = [
    SELECT Id
    FROM Job__c
    WHERE QueueName__c = :queueName
];

List<QueueableDispatch__c> dispatches = [
    SELECT Id
    FROM QueueableDispatch__c
    WHERE QueueName__c = :queueName
];

List<QueueRuntime__c> runtimes = [
    SELECT Id
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
];

List<List<SObject>> cleanupBatches = new List<List<SObject>>();
List<String> cleanupLabels = new List<String>();
Map<String, Integer> deletedCounts = new Map<String, Integer>{
    'JobRun__c' => 0,
    'QueueError__c' => 0,
    'Job__c' => 0,
    'QueueableDispatch__c' => 0,
    'QueueRuntime__c' => 0
};
List<String> deleteErrors = new List<String>();

List<SObject> runRecords = new List<SObject>();
for (JobRun__c jobRun : jobRuns) {
    runRecords.add(jobRun);
}
cleanupBatches.add(runRecords);
cleanupLabels.add('JobRun__c');

List<SObject> errorRecords = new List<SObject>();
for (QueueError__c queueError : queueErrors) {
    errorRecords.add(queueError);
}
cleanupBatches.add(errorRecords);
cleanupLabels.add('QueueError__c');

List<SObject> jobRecords = new List<SObject>();
for (Job__c job : jobs) {
    jobRecords.add(job);
}
cleanupBatches.add(jobRecords);
cleanupLabels.add('Job__c');

List<SObject> dispatchRecords = new List<SObject>();
for (QueueableDispatch__c dispatch : dispatches) {
    dispatchRecords.add(dispatch);
}
cleanupBatches.add(dispatchRecords);
cleanupLabels.add('QueueableDispatch__c');

List<SObject> runtimeRecords = new List<SObject>();
for (QueueRuntime__c runtime : runtimes) {
    runtimeRecords.add(runtime);
}
cleanupBatches.add(runtimeRecords);
cleanupLabels.add('QueueRuntime__c');

for (Integer batchIndex = 0; batchIndex < cleanupBatches.size(); batchIndex++) {
    List<SObject> cleanupBatch = cleanupBatches[batchIndex];
    if (cleanupBatch.isEmpty()) {
        continue;
    }

    String cleanupLabel = cleanupLabels[batchIndex];
    Database.DeleteResult[] deleteResults = Database.delete(cleanupBatch, false);
    for (Integer resultIndex = 0; resultIndex < deleteResults.size(); resultIndex++) {
        Database.DeleteResult deleteResult = deleteResults[resultIndex];
        if (deleteResult.isSuccess()) {
            deletedCounts.put(cleanupLabel, deletedCounts.get(cleanupLabel) + 1);
            continue;
        }

        for (Database.Error deleteError : deleteResult.getErrors()) {
            if (deleteError.getStatusCode() == StatusCode.ENTITY_IS_DELETED) {
                cleanupMessages.add(
                    'Ignored already deleted ' + cleanupLabel + ' ' + cleanupBatch[resultIndex].Id + '.'
                );
                continue;
            }
            deleteErrors.add(
                cleanupLabel +
                    ' ' +
                    cleanupBatch[resultIndex].Id +
                    ': ' +
                    deleteError.getStatusCode() +
                    ' ' +
                    deleteError.getMessage()
            );
        }
    }
}

System.assertEquals(new List<String>(), deleteErrors, 'Unexpected cleanup delete errors.');

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'invocable.cleanup',
            'queueName' => queueName,
            'aborts' => cleanupMessages,
            'deletedRuns' => deletedCounts.get('JobRun__c'),
            'deletedErrors' => deletedCounts.get('QueueError__c'),
            'deletedJobs' => deletedCounts.get('Job__c'),
            'deletedDispatches' => deletedCounts.get('QueueableDispatch__c'),
            'deletedRuntimes' => deletedCounts.get('QueueRuntime__c')
        }
    )
);
