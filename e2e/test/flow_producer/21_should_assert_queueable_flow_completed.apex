String queueName = 'e2e-queueable-probe';
Set<String> externalIds = new Set<String>{
    'flow-producer:queueable:root',
    'flow-producer:queueable:child-one',
    'flow-producer:queueable:child-two'
};

List<Job__c> jobs = [
    SELECT
        Id,
        Parent__c,
        ExternalId__c,
        State__c,
        FailedReason__c,
        ReturnValue__c,
        Progress__c,
        ExecutionMode__c,
        QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
];
Assert.areEqual(externalIds.size(), jobs.size());

List<String> unfinishedJobs = new List<String>();
Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
Set<Id> dispatchIds = new Set<Id>();
for (Job__c job : jobs) {
    jobsByExternalId.put(job.ExternalId__c, job);
    Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), job.ExecutionMode__c);
    Assert.areNotEqual(null, job.QueueableDispatch__c);
    dispatchIds.add(job.QueueableDispatch__c);
    if (QueuesSupport.toState(job.State__c) != Queues.State.COMPLETED) {
        unfinishedJobs.add(job.ExternalId__c + '=' + job.State__c + ' (' + job.FailedReason__c + ')');
        continue;
    }

    Map<String, Object> returnValue = (Map<String, Object>) JSON.deserializeUntyped(job.ReturnValue__c);
    Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(job.Progress__c);
    Assert.areEqual('complete', String.valueOf(returnValue.get('mode')));
    Assert.areEqual(queueName + ':' + job.ExternalId__c, String.valueOf(returnValue.get('idempotencyKey')));
    Assert.areEqual('completed', String.valueOf(progress.get('status')));
}

Assert.areEqual(
    0,
    unfinishedJobs.size(),
    'Queueable flow jobs are not finished yet or failed: ' + String.join(unfinishedJobs, ', ')
);

Job__c root = jobsByExternalId.get('flow-producer:queueable:root');
Assert.areEqual(null, root.Parent__c);
Assert.areEqual(root.Id, jobsByExternalId.get('flow-producer:queueable:child-one').Parent__c);
Assert.areEqual(root.Id, jobsByExternalId.get('flow-producer:queueable:child-two').Parent__c);
Assert.areEqual(2, dispatchIds.size());

List<QueueableDispatch__c> dispatches = [
    SELECT State__c, RequestedCount__c, DispatchedCount__c
    FROM QueueableDispatch__c
    WHERE Id IN :dispatchIds
];
Assert.areEqual(dispatchIds.size(), dispatches.size());
for (QueueableDispatch__c dispatch : dispatches) {
    Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), dispatch.State__c);
    Assert.areEqual(dispatch.RequestedCount__c, dispatch.DispatchedCount__c);
}

Integer runCount = [
    SELECT COUNT()
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :queueName AND Job__r.ExternalId__c IN :externalIds
];
Assert.areEqual(externalIds.size(), runCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'flow-producer.queueable.assert',
            'queueName' => queueName,
            'jobCount' => jobs.size(),
            'dispatchCount' => dispatches.size()
        }
    )
);
