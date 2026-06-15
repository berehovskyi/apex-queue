String queueName = 'e2e-invocable-probe';
String externalId = 'invocable:recovery:lost-pre-claim-wake';

Job__c job = [
    SELECT
        Id,
        State__c,
        ExecutionMode__c,
        AttemptsMade__c,
        FailedReason__c,
        ReturnValue__c,
        Progress__c,
        LeaseToken__c,
        LeaseExpiresAt__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c = :externalId
    LIMIT 1
];

Assert.areEqual(Queues.State.COMPLETED, QueuesSupport.toState(job.State__c));
Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), job.ExecutionMode__c);
Assert.areEqual(1, QueuesSupport.toInteger(job.AttemptsMade__c, 0));
Assert.areEqual(null, job.FailedReason__c);
Assert.areEqual(null, job.LeaseToken__c);
Assert.areEqual(null, job.LeaseExpiresAt__c);

Map<String, Object> returnValue = (Map<String, Object>) JSON.deserializeUntyped(job.ReturnValue__c);
Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(job.Progress__c);
Assert.areEqual(externalId, String.valueOf(returnValue.get('payload')));
Assert.areEqual('completed', String.valueOf(progress.get('status')));

List<JobRun__c> runs = [
    SELECT Status__c, AttemptNumber__c, Message__c
    FROM JobRun__c
    WHERE Job__c = :job.Id
];
Assert.areEqual(1, runs.size());
Assert.areEqual(Queues.RunStatus.COMPLETED.name(), runs[0].Status__c);
Assert.areEqual(1, QueuesSupport.toInteger(runs[0].AttemptNumber__c, 0));
Assert.areEqual(null, runs[0].Message__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'invocable.lost-wake.assert',
            'queueName' => queueName,
            'externalId' => externalId,
            'jobId' => job.Id
        }
    )
);
