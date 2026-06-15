String queueName = 'e2e-invocable-probe';
Set<String> externalIds = new Set<String>{ 'invocable:complete:one', 'invocable:complete:two' };

List<Job__c> jobs = [
    SELECT
        Id,
        ExternalId__c,
        State__c,
        FailedReason__c,
        ReturnValue__c,
        Progress__c,
        AsyncApexJobId__c,
        ExecutionMode__c,
        AttemptsMade__c,
        LeaseToken__c,
        LeaseExpiresAt__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
    ORDER BY ExternalId__c ASC
];
Assert.areEqual(externalIds.size(), jobs.size());

List<String> unfinishedJobs = new List<String>();
List<String> completedPayloads = new List<String>();
Set<String> transactionMarkers = new Set<String>();
for (Job__c job : jobs) {
    Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), job.ExecutionMode__c);

    if (QueuesSupport.toState(job.State__c) != Queues.State.COMPLETED) {
        unfinishedJobs.add(job.ExternalId__c + '=' + job.State__c + ' (' + job.FailedReason__c + ')');
        continue;
    }

    Assert.areEqual(null, job.LeaseToken__c);
    Assert.areEqual(null, job.LeaseExpiresAt__c);
    Assert.areEqual(1, QueuesSupport.toInteger(job.AttemptsMade__c, 0));
    Assert.isTrue(String.isBlank(job.AsyncApexJobId__c));
    Map<String, Object> returnValue = (Map<String, Object>) JSON.deserializeUntyped(job.ReturnValue__c);
    Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(job.Progress__c);
    String expectedIdempotencyKey = queueName + ':' + job.ExternalId__c;

    Assert.areEqual('complete', String.valueOf(returnValue.get('mode')));
    Assert.areEqual(queueName, String.valueOf(returnValue.get('queueName')));
    Assert.areEqual(expectedIdempotencyKey, String.valueOf(returnValue.get('idempotencyKey')));
    Assert.areEqual('completed', String.valueOf(progress.get('status')));
    Assert.areEqual(expectedIdempotencyKey, String.valueOf(progress.get('idempotencyKey')));
    String returnTransactionMarker = String.valueOf(returnValue.get('transactionMarker'));
    String progressTransactionMarker = String.valueOf(progress.get('transactionMarker'));
    Assert.isTrue(String.isNotBlank(returnTransactionMarker));
    Assert.areEqual(returnTransactionMarker, progressTransactionMarker);
    transactionMarkers.add(returnTransactionMarker);
    completedPayloads.add(String.valueOf(returnValue.get('payload')));
}

Assert.areEqual(
    0,
    unfinishedJobs.size(),
    'Invocable smoke jobs are not finished yet or failed: ' + String.join(unfinishedJobs, ', ')
);
Assert.areEqual(
    externalIds.size(),
    transactionMarkers.size(),
    'Each INVOCABLE job must execute in its own transaction.'
);

List<JobRun__c> runs = [
    SELECT Job__r.ExternalId__c, Status__c, AttemptNumber__c, AsyncApexJobId__c, Message__c
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :queueName AND Job__r.ExternalId__c IN :externalIds
];
Assert.areEqual(externalIds.size(), runs.size());
for (JobRun__c run : runs) {
    Assert.areEqual(Queues.RunStatus.COMPLETED.name(), QueuesSupport.normalizeEnumValue(run.Status__c));
    Assert.areEqual(1, QueuesSupport.toInteger(run.AttemptNumber__c, 0));
    Assert.areEqual(null, run.AsyncApexJobId__c);
    Assert.areEqual(null, run.Message__c);
}

Integer queueErrorCount = [SELECT COUNT() FROM QueueError__c WHERE QueueName__c = :queueName];
Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Integer dispatchCount = [SELECT COUNT() FROM QueueableDispatch__c WHERE QueueName__c = :queueName];
Assert.areEqual(0, queueErrorCount);
Assert.areEqual(0, runtimeCount);
Assert.areEqual(0, dispatchCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'invocable.smoke.assert',
            'queueName' => queueName,
            'completedPayloads' => completedPayloads,
            'jobCount' => jobs.size()
        }
    )
);
