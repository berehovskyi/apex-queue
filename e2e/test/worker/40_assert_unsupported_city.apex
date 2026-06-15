Job__c job = [
    SELECT Id, ExternalId__c, State__c, FailedReason__c, ReturnValue__c
    FROM Job__c
    WHERE QueueName__c = 'e2e-weather-sync' AND ExternalId__c = 'weather:unsupported-atlantis'
    LIMIT 1
];

QueuesSupport.isTrue(
    QueuesSupport.toState(job.State__c) == Queues.State.FAILED,
    'Unsupported-city job is not failed yet: ' + job.State__c + ' (' + job.FailedReason__c + ')'
);
QueuesSupport.isTrue(
    job.FailedReason__c != null && job.FailedReason__c.contains('Unsupported city'),
    'Unsupported-city job did not record the expected failure reason.'
);
QueuesSupport.isTrue(job.ReturnValue__c == null, 'Unsupported-city job should not have a return value.');

JobRun__c run = [
    SELECT Status__c, Message__c
    FROM JobRun__c
    WHERE Job__c = :job.Id
    ORDER BY CreatedDate DESC
    LIMIT 1
];
QueuesSupport.isTrue(
    QueuesSupport.normalizeEnumValue(run.Status__c) == Queues.RunStatus.FAILED.name(),
    'Unsupported-city run was not failed: ' + run.Status__c
);
QueuesSupport.isTrue(
    run.Message__c != null && run.Message__c.contains('Unsupported city'),
    'Unsupported-city run did not record the expected failure message.'
);

Integer queueErrorCount = [
    SELECT COUNT()
    FROM QueueError__c
    WHERE QueueName__c = 'e2e-weather-sync' AND Job__c = :job.Id
];
QueuesSupport.isTrue(
    queueErrorCount == 0,
    'Processor-handled unsupported-city failure should not create queue errors.'
);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'queueName' => 'e2e-weather-sync',
            'externalId' => job.ExternalId__c,
            'state' => job.State__c,
            'failedReason' => job.FailedReason__c
        }
    )
);
