String queueName = 'e2e-invocable-probe';
String externalId = 'invocable:recovery:lost-pre-claim-wake';

Job__c strandedJob = [
    SELECT Id, State__c, AvailableAt__c, AttemptsMade__c, LeaseToken__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c = :externalId
    LIMIT 1
];
Datetime staleAvailableAt = strandedJob.AvailableAt__c;
Datetime repairStartedAt = System.now();
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(strandedJob.State__c));
Assert.areEqual(0, QueuesSupport.toInteger(strandedJob.AttemptsMade__c, 0));
Assert.areEqual(null, strandedJob.LeaseToken__c);

Queues.MaintenanceResult result = Queues.runRuntimeMaintenance();
Assert.isTrue(result.total >= 1, 'Expected maintenance to repair the seeded lost Invocable wake.');

Job__c repairedJob = [
    SELECT State__c, ExecutionMode__c, AvailableAt__c, AttemptsMade__c, LeaseToken__c
    FROM Job__c
    WHERE Id = :strandedJob.Id
    LIMIT 1
];
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(repairedJob.State__c));
Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), repairedJob.ExecutionMode__c);
Assert.isTrue(repairedJob.AvailableAt__c > staleAvailableAt);
Assert.isTrue(repairedJob.AvailableAt__c > repairStartedAt);
Assert.areEqual(0, QueuesSupport.toInteger(repairedJob.AttemptsMade__c, 0));
Assert.areEqual(null, repairedJob.LeaseToken__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'invocable.lost-wake.repair',
            'queueName' => queueName,
            'externalId' => externalId,
            'jobId' => strandedJob.Id,
            'maintenanceResult' => result
        }
    )
);
