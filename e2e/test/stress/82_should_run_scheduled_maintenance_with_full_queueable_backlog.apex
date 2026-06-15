String queueName = 'e2e-queueable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String externalIdPrefix = 'stress-maintenance-f2-';
String externalIdPattern = externalIdPrefix + '%';
Datetime now = System.now();
Datetime recoveredAfter = now.addMinutes(-1);
List<Job__c> jobs = new List<Job__c>();

for (Integer index = 0; index < Limits.getLimitQueueableJobs(); index++) {
    String externalId = externalIdPrefix + index;
    jobs.add(
        new Job__c(
            QueueName__c = queueName,
            Name__c = 'scheduled-maintenance-backlog-' + index,
            State__c = Queues.State.WAITING.name(),
            ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name(),
            ExternalId__c = externalId,
            QueueScopedExternalId__c = QueuesSupport.buildQueueScopedExternalId(queueName, externalId),
            Data__c = QueuesSupport.serialize(new Map<String, Object>{ 'mode' => 'complete', 'payload' => externalId }),
            ProcessorClass__c = processorClass,
            QueueableDispatchInitiatedAt__c = now.addMinutes(-10),
            AvailableAt__c = now,
            Priority__c = 0,
            AttemptsMade__c = 0,
            MaxAttempts__c = 1,
            BackoffType__c = Queues.Backoff.NONE.name(),
            BackoffValue__c = 0,
            PauseRequested__c = false
        )
    );
}
insert jobs;

new QueuesMaintenanceScheduler().execute(null);

Integer recoveredCount = [
    SELECT COUNT()
    FROM Job__c
    WHERE
        QueueName__c = :queueName
        AND ExternalId__c LIKE :externalIdPattern
        AND QueueableDispatchInitiatedAt__c > :recoveredAfter
];
Integer deferredCount = [
    SELECT COUNT()
    FROM Job__c
    WHERE
        QueueName__c = :queueName
        AND ExternalId__c LIKE :externalIdPattern
        AND QueueableDispatchInitiatedAt__c <= :recoveredAfter
];

Assert.areEqual(Limits.getLimitQueueableJobs(), Limits.getQueueableJobs());
Assert.areEqual(Limits.getLimitQueueableJobs() - 1, recoveredCount);
Assert.areEqual(1, deferredCount);
Assert.areEqual(0, [SELECT COUNT() FROM QueueError__c WHERE QueueName__c = :queueName]);
