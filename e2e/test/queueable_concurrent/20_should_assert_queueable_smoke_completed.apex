String queueName = 'e2e-queueable-concurrent-probe';
Set<String> externalIds = new Set<String>{ 'queueable:complete:one', 'queueable:complete:two' };

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
        QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
    ORDER BY ExternalId__c ASC
];
Assert.areEqual(externalIds.size(), jobs.size());

List<String> unfinishedJobs = new List<String>();
Set<Id> dispatchIds = new Set<Id>();
List<String> completedPayloads = new List<String>();
for (Job__c job : jobs) {
    Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), job.ExecutionMode__c);
    Assert.areNotEqual(null, job.QueueableDispatch__c);
    dispatchIds.add(job.QueueableDispatch__c);

    if (QueuesSupport.toState(job.State__c) != Queues.State.COMPLETED) {
        unfinishedJobs.add(job.ExternalId__c + '=' + job.State__c + ' (' + job.FailedReason__c + ')');
        continue;
    }

    Assert.isTrue(String.isBlank(job.AsyncApexJobId__c));
    Map<String, Object> returnValue = (Map<String, Object>) JSON.deserializeUntyped(job.ReturnValue__c);
    Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(job.Progress__c);
    String expectedIdempotencyKey = queueName + ':' + job.ExternalId__c;

    Assert.areEqual('complete', String.valueOf(returnValue.get('mode')));
    Assert.areEqual(queueName, String.valueOf(returnValue.get('queueName')));
    Assert.areEqual(expectedIdempotencyKey, String.valueOf(returnValue.get('idempotencyKey')));
    Assert.areEqual('completed', String.valueOf(progress.get('status')));
    Assert.areEqual(expectedIdempotencyKey, String.valueOf(progress.get('idempotencyKey')));
    completedPayloads.add(String.valueOf(returnValue.get('payload')));
}

Assert.areEqual(
    0,
    unfinishedJobs.size(),
    'Queueable smoke jobs are not finished yet or failed: ' + String.join(unfinishedJobs, ', ')
);
Assert.areEqual(1, dispatchIds.size());

QueueableDispatch__c dispatch = [
    SELECT
        State__c,
        RequestedCount__c,
        DispatchedCount__c,
        DispatcherAsyncApexJobId__c,
        StartedAt__c,
        FinishedAt__c,
        FailedReason__c
    FROM QueueableDispatch__c
    WHERE Id IN :dispatchIds
    LIMIT 1
];
Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), dispatch.State__c);
Assert.areEqual(externalIds.size(), QueuesSupport.toInteger(dispatch.RequestedCount__c, 0));
Assert.areEqual(externalIds.size(), QueuesSupport.toInteger(dispatch.DispatchedCount__c, 0));
Assert.areNotEqual(null, dispatch.DispatcherAsyncApexJobId__c);
Assert.areNotEqual(null, dispatch.StartedAt__c);
Assert.areNotEqual(null, dispatch.FinishedAt__c);
Assert.areEqual(null, dispatch.FailedReason__c);

List<JobRun__c> runs = [
    SELECT Job__r.ExternalId__c, Status__c, AsyncApexJobId__c
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :queueName AND Job__r.ExternalId__c IN :externalIds
];
Assert.areEqual(externalIds.size(), runs.size());
for (JobRun__c run : runs) {
    Assert.areEqual(Queues.RunStatus.COMPLETED.name(), QueuesSupport.normalizeEnumValue(run.Status__c));
    Assert.isTrue(String.isNotBlank(run.AsyncApexJobId__c));
}

Integer queueErrorCount = [SELECT COUNT() FROM QueueError__c WHERE QueueName__c = :queueName];
Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Assert.areEqual(0, queueErrorCount);
Assert.areEqual(0, runtimeCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queueable_concurrent.smoke.assert',
            'queueName' => queueName,
            'completedPayloads' => completedPayloads,
            'jobCount' => jobs.size()
        }
    )
);
