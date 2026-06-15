String queueName = 'e2e-invocable-probe';
String externalId = 'invocable:recovery:expired-active-lease';

Job__c job = [
    SELECT
        Id,
        State__c,
        AttemptsMade__c,
        StalledCount__c,
        FailedReason__c,
        ReturnValue__c,
        Progress__c,
        AsyncApexJobId__c,
        LeaseToken__c,
        LeaseExpiresAt__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c = :externalId
    LIMIT 1
];
Assert.areEqual(Queues.State.COMPLETED, QueuesSupport.toState(job.State__c));
Assert.areEqual(2, QueuesSupport.toInteger(job.AttemptsMade__c, 0));
Assert.areEqual(1, QueuesSupport.toInteger(job.StalledCount__c, 0));
Assert.areEqual(null, job.FailedReason__c);
Assert.areEqual(null, job.AsyncApexJobId__c);
Assert.areEqual(null, job.LeaseToken__c);
Assert.areEqual(null, job.LeaseExpiresAt__c);

Map<String, Object> returnValue = (Map<String, Object>) JSON.deserializeUntyped(job.ReturnValue__c);
Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(job.Progress__c);
String expectedIdempotencyKey = queueName + ':' + externalId;
Assert.areEqual('complete', String.valueOf(returnValue.get('mode')));
Assert.areEqual(externalId, String.valueOf(returnValue.get('payload')));
Assert.areEqual(expectedIdempotencyKey, String.valueOf(returnValue.get('idempotencyKey')));
Assert.areEqual('completed', String.valueOf(progress.get('status')));
Assert.areEqual(expectedIdempotencyKey, String.valueOf(progress.get('idempotencyKey')));

List<JobRun__c> runs = [
    SELECT Status__c, AttemptNumber__c, AsyncApexJobId__c, Message__c
    FROM JobRun__c
    WHERE Job__c = :job.Id
    ORDER BY CreatedDate
];
Assert.areEqual(2, runs.size());
Assert.areEqual(Queues.RunStatus.FAILED.name(), runs[0].Status__c);
Assert.areEqual(1, QueuesSupport.toInteger(runs[0].AttemptNumber__c, 0));
Assert.areEqual(null, runs[0].AsyncApexJobId__c);
Assert.areEqual('Lease expired before invocable action completed.', runs[0].Message__c);
Assert.areEqual(Queues.RunStatus.COMPLETED.name(), runs[1].Status__c);
Assert.areEqual(2, QueuesSupport.toInteger(runs[1].AttemptNumber__c, 0));
Assert.areEqual(null, runs[1].AsyncApexJobId__c);
Assert.areEqual(null, runs[1].Message__c);

Integer queueErrorCount = [SELECT COUNT() FROM QueueError__c WHERE QueueName__c = :queueName];
Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Integer dispatchCount = [SELECT COUNT() FROM QueueableDispatch__c WHERE QueueName__c = :queueName];
Assert.areEqual(0, queueErrorCount);
Assert.areEqual(0, runtimeCount);
Assert.areEqual(0, dispatchCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'invocable.expired-lease.assert',
            'queueName' => queueName,
            'jobId' => job.Id
        }
    )
);
