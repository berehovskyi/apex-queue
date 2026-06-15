String queueName = 'e2e-invocable-probe';
String processorName = 'invocable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String externalId = 'invocable:recovery:expired-active-lease';
Datetime now = System.now();

Job__c stalledJob = new Job__c(
    QueueName__c = queueName,
    Name__c = processorName,
    State__c = Queues.State.ACTIVE.name(),
    ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name(),
    ExternalId__c = externalId,
    QueueScopedExternalId__c = QueuesSupport.buildQueueScopedExternalId(queueName, externalId),
    ProcessorClass__c = processorClass,
    Data__c = QueuesSupport.serialize(new Map<String, Object>{ 'mode' => 'complete', 'payload' => externalId }),
    AvailableAt__c = now.addMinutes(-10),
    Priority__c = 0,
    AttemptsMade__c = 1,
    MaxAttempts__c = 3,
    BackoffType__c = Queues.Backoff.NONE.name(),
    BackoffValue__c = 0,
    PauseRequested__c = false,
    StalledCount__c = 0,
    LeaseToken__c = 'expired-invocable-e2e-lease',
    LeaseExpiresAt__c = now.addMinutes(-2)
);
insert stalledJob;

Queues.MaintenanceResult maintenanceResult = new QueuesMaintenanceService().runRuntimeMaintenance();
Assert.isTrue(
    maintenanceResult.getCount(queueName) >= 1,
    'Expected maintenance to recover at least the seeded invocable job.'
);

Job__c recoveredJob = [
    SELECT
        Id,
        ExecutionMode__c,
        State__c,
        AttemptsMade__c,
        StalledCount__c,
        LeaseToken__c,
        LeaseExpiresAt__c,
        FailedReason__c
    FROM Job__c
    WHERE Id = :stalledJob.Id
    LIMIT 1
];
Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), recoveredJob.ExecutionMode__c);
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(recoveredJob.State__c));
Assert.areEqual(1, QueuesSupport.toInteger(recoveredJob.AttemptsMade__c, 0));
Assert.areEqual(1, QueuesSupport.toInteger(recoveredJob.StalledCount__c, 0));
Assert.areEqual(null, recoveredJob.LeaseToken__c);
Assert.areEqual(null, recoveredJob.LeaseExpiresAt__c);
Assert.areEqual(null, recoveredJob.FailedReason__c);

List<JobRun__c> runs = [
    SELECT Status__c, AttemptNumber__c, AsyncApexJobId__c, Message__c
    FROM JobRun__c
    WHERE Job__c = :stalledJob.Id
];
Assert.areEqual(1, runs.size());
Assert.areEqual(Queues.RunStatus.FAILED.name(), runs[0].Status__c);
Assert.areEqual(1, QueuesSupport.toInteger(runs[0].AttemptNumber__c, 0));
Assert.areEqual(null, runs[0].AsyncApexJobId__c);
Assert.areEqual('Lease expired before invocable action completed.', runs[0].Message__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'invocable.expired-lease.recover',
            'queueName' => queueName,
            'externalId' => externalId,
            'jobId' => stalledJob.Id,
            'recoveredCount' => maintenanceResult.getCount(queueName)
        }
    )
);
