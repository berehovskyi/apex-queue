String queueName = 'e2e-queueable-concurrent-probe';
String externalId = 'queueable:retry:failure';
String expectedIdempotencyKey = queueName + ':' + externalId;
String expectedFailureMessage =
    'Queueable retryable processor failure. [idempotencyKey=' +
    expectedIdempotencyKey +
    ']';

Job__c job = [
    SELECT
        Id,
        State__c,
        AttemptsMade__c,
        MaxAttempts__c,
        FailedReason__c,
        AsyncApexJobId__c,
        QueueableDispatch__c,
        ExecutionMode__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c = :externalId
    LIMIT 1
];

Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), job.ExecutionMode__c);
Assert.areEqual(Queues.State.FAILED, QueuesSupport.toState(job.State__c));
Assert.areEqual(3, QueuesSupport.toInteger(job.AttemptsMade__c, 0));
Assert.areEqual(3, QueuesSupport.toInteger(job.MaxAttempts__c, 0));
Assert.areEqual(expectedFailureMessage, job.FailedReason__c);
Assert.isTrue(String.isBlank(job.AsyncApexJobId__c));

List<JobRun__c> runs = [
    SELECT AttemptNumber__c, Status__c, Message__c, AsyncApexJobId__c
    FROM JobRun__c
    WHERE Job__c = :job.Id
    ORDER BY AttemptNumber__c ASC
];
Assert.areEqual(3, runs.size());
for (Integer index = 0; index < runs.size(); index++) {
    JobRun__c run = runs[index];
    Assert.areEqual(index + 1, QueuesSupport.toInteger(run.AttemptNumber__c, 0));
    Assert.areEqual(Queues.RunStatus.FAILED.name(), QueuesSupport.normalizeEnumValue(run.Status__c));
    Assert.areEqual(expectedFailureMessage, run.Message__c);
    Assert.isTrue(String.isNotBlank(run.AsyncApexJobId__c));
}

QueueableDispatch__c dispatch = [
    SELECT State__c, RequestedCount__c, DispatchedCount__c, FinishedAt__c, FailedReason__c
    FROM QueueableDispatch__c
    WHERE Id = :job.QueueableDispatch__c
    LIMIT 1
];
Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), dispatch.State__c);
Assert.areEqual(1, QueuesSupport.toInteger(dispatch.RequestedCount__c, 0));
Assert.areEqual(1, QueuesSupport.toInteger(dispatch.DispatchedCount__c, 0));
Assert.areNotEqual(null, dispatch.FinishedAt__c);
Assert.areEqual(null, dispatch.FailedReason__c);

Integer queueErrorCount = [SELECT COUNT() FROM QueueError__c WHERE QueueName__c = :queueName];
Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Assert.areEqual(0, queueErrorCount);
Assert.areEqual(0, runtimeCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queueable_concurrent.retry.assert',
            'queueName' => queueName,
            'jobId' => job.Id,
            'idempotencyKey' => expectedIdempotencyKey
        }
    )
);
