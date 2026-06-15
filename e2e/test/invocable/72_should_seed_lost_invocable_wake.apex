String queueName = 'e2e-invocable-probe';
String processorName = 'invocable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String externalId = 'invocable:recovery:lost-pre-claim-wake';
Datetime staleAvailableAt = System.now().addMinutes(-10);

Job__c lostWakeJob = new Job__c(
    QueueName__c = queueName,
    Name__c = processorName,
    State__c = Queues.State.WAITING.name(),
    ExecutionMode__c = Queues.ExecutionMode.WORKER.name(),
    ExternalId__c = externalId,
    QueueScopedExternalId__c = QueuesSupport.buildQueueScopedExternalId(queueName, externalId),
    ProcessorClass__c = processorClass,
    Data__c = QueuesSupport.serialize(new Map<String, Object>{ 'mode' => 'complete', 'payload' => externalId }),
    AvailableAt__c = staleAvailableAt,
    Priority__c = 0,
    AttemptsMade__c = 0,
    MaxAttempts__c = 2,
    BackoffType__c = Queues.Backoff.NONE.name(),
    BackoffValue__c = 0,
    PauseRequested__c = false,
    StalledCount__c = 0
);
insert lostWakeJob;

// Changing only the transport does not satisfy the Claim Flow's state/availability-change filter.
update new Job__c(Id = lostWakeJob.Id, ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name());

Job__c strandedJob = [
    SELECT State__c, ExecutionMode__c, AvailableAt__c, AttemptsMade__c, LeaseToken__c
    FROM Job__c
    WHERE Id = :lostWakeJob.Id
    LIMIT 1
];
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(strandedJob.State__c));
Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), strandedJob.ExecutionMode__c);
Assert.areEqual(staleAvailableAt, strandedJob.AvailableAt__c);
Assert.areEqual(0, QueuesSupport.toInteger(strandedJob.AttemptsMade__c, 0));
Assert.areEqual(null, strandedJob.LeaseToken__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'invocable.lost-wake.seed',
            'queueName' => queueName,
            'externalId' => externalId,
            'jobId' => lostWakeJob.Id
        }
    )
);
