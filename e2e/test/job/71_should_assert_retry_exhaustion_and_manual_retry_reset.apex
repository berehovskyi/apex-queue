String queueName = 'e2e-job-probe';
String jobExternalId = 'job-retry-reset';
String expectedIdempotencyKey = queueName + ':' + jobExternalId;
String expectedFailureMessage = 'Retryable processor failure. [idempotencyKey=' + expectedIdempotencyKey + ']';

// Arrange
Job__c exhaustedJob = [
    SELECT
        Id,
        QueueName__c,
        State__c,
        AttemptsMade__c,
        MaxAttempts__c,
        FailedReason__c,
        ReturnValue__c,
        Progress__c,
        Checkpoint__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c = :jobExternalId
    LIMIT 1
];
List<JobRun__c> failedRuns = [
    SELECT Id, Status__c, Message__c, AttemptNumber__c
    FROM JobRun__c
    WHERE Job__c = :exhaustedJob.Id
    ORDER BY AttemptNumber__c ASC
];

// Act
Boolean wasFailedAfterExhaustion = QueuesSupport.toState(exhaustedJob.State__c) == Queues.State.FAILED;
Queues.of(queueName).pause();
Queues.job(exhaustedJob.Id).retry();

Job__c retriedJob = [
    SELECT
        Id,
        State__c,
        AttemptsMade__c,
        MaxAttempts__c,
        FailedReason__c,
        ReturnValue__c,
        Progress__c,
        Checkpoint__c,
        PauseRequested__c
    FROM Job__c
    WHERE Id = :exhaustedJob.Id
    LIMIT 1
];
QueueRuntime__c pausedRuntime = [
    SELECT Id, IsPaused__c, WorkerToken__c, AsyncApexJobId__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
    LIMIT 1
];

// Assert
Assert.areEqual(true, wasFailedAfterExhaustion);
Assert.areEqual(3, QueuesSupport.toInteger(exhaustedJob.AttemptsMade__c, 0));
Assert.areEqual(3, QueuesSupport.toInteger(exhaustedJob.MaxAttempts__c, 0));
Assert.areEqual(expectedFailureMessage, exhaustedJob.FailedReason__c);
Assert.areEqual(3, failedRuns.size());
Assert.areEqual(
    Queues.RunStatus.FAILED,
    Queues.RunStatus.valueOf(QueuesSupport.normalizeEnumValue(failedRuns[0].Status__c))
);
Assert.areEqual(
    Queues.RunStatus.FAILED,
    Queues.RunStatus.valueOf(QueuesSupport.normalizeEnumValue(failedRuns[1].Status__c))
);
Assert.areEqual(
    Queues.RunStatus.FAILED,
    Queues.RunStatus.valueOf(QueuesSupport.normalizeEnumValue(failedRuns[2].Status__c))
);
Assert.areEqual(expectedFailureMessage, failedRuns[0].Message__c);
Assert.areEqual(expectedFailureMessage, failedRuns[1].Message__c);
Assert.areEqual(expectedFailureMessage, failedRuns[2].Message__c);
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(retriedJob.State__c));
Assert.areEqual(0, QueuesSupport.toInteger(retriedJob.AttemptsMade__c, 0));
Assert.areEqual(3, QueuesSupport.toInteger(retriedJob.MaxAttempts__c, 0));
Assert.areEqual(null, retriedJob.FailedReason__c);
Assert.areEqual(null, retriedJob.ReturnValue__c);
Assert.areEqual(null, retriedJob.Progress__c);
Assert.areEqual(null, retriedJob.Checkpoint__c);
Assert.areEqual(false, retriedJob.PauseRequested__c);
Assert.areEqual(true, pausedRuntime.IsPaused__c);
Assert.isTrue(String.isBlank(pausedRuntime.WorkerToken__c));
Assert.isTrue(String.isBlank(pausedRuntime.AsyncApexJobId__c));

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job.retry.assert',
            'queueName' => queueName,
            'jobId' => retriedJob.Id,
            'idempotencyKey' => expectedIdempotencyKey
        }
    )
);
