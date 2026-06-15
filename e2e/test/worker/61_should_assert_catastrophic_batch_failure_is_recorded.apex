String queueName = 'e2e-worker-probe';
String externalId = 'worker-catastrophic';

Job__c failedJob = [
    SELECT Id, State__c, AttemptsMade__c, MaxAttempts__c, FailedReason__c, ReturnValue__c, Progress__c, Checkpoint__c
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
Integer queueErrorCount = [
    SELECT COUNT()
    FROM QueueError__c
    WHERE QueueName__c = :queueName AND Job__c = :failedJob.Id
];

Assert.areEqual(Queues.State.FAILED, QueuesSupport.toState(failedJob.State__c));
Assert.areEqual(1, QueuesSupport.toInteger(failedJob.AttemptsMade__c, 0));
Assert.areEqual(1, QueuesSupport.toInteger(failedJob.MaxAttempts__c, 0));
Assert.isTrue(failedJob.FailedReason__c.contains('Too many SOQL queries'));
Assert.areEqual(null, failedJob.ReturnValue__c);
Assert.areEqual(null, failedJob.Progress__c);
Assert.areEqual(null, failedJob.Checkpoint__c);
Assert.areEqual(1, runs.size());
Assert.areEqual(Queues.RunStatus.FAILED, Queues.RunStatus.valueOf(QueuesSupport.normalizeEnumValue(runs[0].Status__c)));
Assert.areEqual(1, QueuesSupport.toInteger(runs[0].AttemptNumber__c, 0));
Assert.isTrue(runs[0].Message__c.contains('Too many SOQL queries'));
Assert.isTrue(String.isNotBlank(runs[0].AsyncApexJobId__c));
Assert.areEqual(0, queueErrorCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'worker.catastrophic.assert',
            'queueName' => queueName,
            'jobId' => failedJob.Id,
            'failedReason' => failedJob.FailedReason__c,
            'asyncApexJobId' => runs[0].AsyncApexJobId__c
        }
    )
);
