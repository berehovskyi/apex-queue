String queueName = 'e2e-queueable-probe';
String externalId = 'queueable-catastrophic';

Job__c failedJob = [
    SELECT
        Id,
        State__c,
        AttemptsMade__c,
        MaxAttempts__c,
        FailedReason__c,
        ReturnValue__c,
        Progress__c,
        Checkpoint__c,
        AsyncApexJobId__c,
        QueueableDispatch__c,
        ExecutionMode__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c = :externalId
    LIMIT 1
];
List<JobRun__c> runs = [
    SELECT Status__c, Message__c, AttemptNumber__c, AsyncApexJobId__c
    FROM JobRun__c
    WHERE Job__c = :failedJob.Id
    ORDER BY CreatedDate DESC
];
QueueableDispatch__c dispatch = [
    SELECT State__c, RequestedCount__c, DispatchedCount__c, FinishedAt__c, FailedReason__c
    FROM QueueableDispatch__c
    WHERE Id = :failedJob.QueueableDispatch__c
    LIMIT 1
];
Integer queueErrorCount = [
    SELECT COUNT()
    FROM QueueError__c
    WHERE QueueName__c = :queueName AND Job__c = :failedJob.Id
];
Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];

Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), failedJob.ExecutionMode__c);
Assert.areEqual(Queues.State.FAILED, QueuesSupport.toState(failedJob.State__c));
Assert.areEqual(1, QueuesSupport.toInteger(failedJob.AttemptsMade__c, 0));
Assert.areEqual(1, QueuesSupport.toInteger(failedJob.MaxAttempts__c, 0));
Assert.isTrue(failedJob.FailedReason__c.contains('Too many SOQL queries'));
Assert.areEqual(null, failedJob.ReturnValue__c);
Assert.areEqual(null, failedJob.Progress__c);
Assert.areEqual(null, failedJob.Checkpoint__c);
Assert.isTrue(String.isBlank(failedJob.AsyncApexJobId__c));
Assert.areEqual(1, runs.size());
Assert.areEqual(Queues.RunStatus.FAILED, Queues.RunStatus.valueOf(QueuesSupport.normalizeEnumValue(runs[0].Status__c)));
Assert.areEqual(1, QueuesSupport.toInteger(runs[0].AttemptNumber__c, 0));
Assert.isTrue(runs[0].Message__c.contains('Too many SOQL queries'));
Assert.isTrue(String.isNotBlank(runs[0].AsyncApexJobId__c));
Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), dispatch.State__c);
Assert.areEqual(1, QueuesSupport.toInteger(dispatch.RequestedCount__c, 0));
Assert.areEqual(1, QueuesSupport.toInteger(dispatch.DispatchedCount__c, 0));
Assert.areNotEqual(null, dispatch.FinishedAt__c);
Assert.areEqual(null, dispatch.FailedReason__c);
Assert.areEqual(0, queueErrorCount);
Assert.areEqual(0, runtimeCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queueable.catastrophic.assert',
            'queueName' => queueName,
            'jobId' => failedJob.Id,
            'failedReason' => failedJob.FailedReason__c,
            'asyncApexJobId' => runs[0].AsyncApexJobId__c
        }
    )
);
