String queueName = 'e2e-job-probe';
String jobExternalId = 'job-unrecoverable';

// Arrange
Job__c failedJob = [
    SELECT Id, State__c, AttemptsMade__c, MaxAttempts__c, FailedReason__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c = :jobExternalId
    LIMIT 1
];
List<JobRun__c> failedRuns = [
    SELECT Id, Status__c, Message__c, AttemptNumber__c
    FROM JobRun__c
    WHERE Job__c = :failedJob.Id
    ORDER BY AttemptNumber__c ASC
];

// Act
Boolean failedWithoutRetry =
    QueuesSupport.toState(failedJob.State__c) == Queues.State.FAILED &&
    QueuesSupport.toInteger(failedJob.AttemptsMade__c, 0) == 1;

// Assert
Assert.areEqual(true, failedWithoutRetry);
Assert.areEqual(5, QueuesSupport.toInteger(failedJob.MaxAttempts__c, 0));
Assert.areEqual('Unrecoverable processor failure.', failedJob.FailedReason__c);
Assert.areEqual(1, failedRuns.size());
Assert.areEqual(
    Queues.RunStatus.FAILED,
    Queues.RunStatus.valueOf(QueuesSupport.normalizeEnumValue(failedRuns[0].Status__c))
);
Assert.areEqual('Unrecoverable processor failure.', failedRuns[0].Message__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job.unrecoverable.assert',
            'queueName' => queueName,
            'jobId' => failedJob.Id
        }
    )
);
