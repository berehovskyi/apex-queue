Set<String> queueNames = new Set<String>{
    'e2e-maintenance-delete-all',
    'e2e-maintenance-delete-completed',
    'e2e-maintenance-delete-none',
    'e2e-maintenance-keep'
};
Set<String> terminalStates = new Set<String>{
    Queues.State.COMPLETED.name(),
    Queues.State.FAILED.name(),
    Queues.State.CANCELED.name()
};
List<String> statesToSeed = new List<String>{
    Queues.State.COMPLETED.name(),
    Queues.State.FAILED.name(),
    Queues.State.CANCELED.name(),
    Queues.State.WAITING.name()
};
Set<String> expectedDeletedExternalIds = new Set<String>();
Set<String> expectedKeptExternalIds = new Set<String>();
List<Job__c> jobs = new List<Job__c>();
Datetime availableAt = System.now().addMinutes(-5);

for (String queueName : queueNames) {
    for (String stateValue : statesToSeed) {
        String externalId = 'maintenance:cleanup:' + queueName + ':' + stateValue.toLowerCase();
        Boolean isCompleted = stateValue == Queues.State.COMPLETED.name();
        Boolean shouldDelete =
            (queueName == 'e2e-maintenance-delete-all' && terminalStates.contains(stateValue)) ||
            (queueName == 'e2e-maintenance-delete-completed' && isCompleted);
        if (shouldDelete) {
            expectedDeletedExternalIds.add(externalId);
        } else {
            expectedKeptExternalIds.add(externalId);
        }

        jobs.add(
            new Job__c(
                QueueName__c = queueName,
                Name__c = 'maintenance-cleanup-probe',
                State__c = stateValue,
                ExecutionMode__c = Queues.ExecutionMode.WORKER.name(),
                ExternalId__c = externalId,
                QueueScopedExternalId__c = QueuesSupport.buildQueueScopedExternalId(queueName, externalId),
                ProcessorClass__c = 'QueuesE2EJobProbeProcessor',
                Data__c = QueuesSupport.serialize(
                    new Map<String, Object>{ 'mode' => 'complete', 'payload' => externalId }
                ),
                AvailableAt__c = availableAt,
                Priority__c = 0,
                AttemptsMade__c = terminalStates.contains(stateValue) ? 1 : 0,
                MaxAttempts__c = 1,
                BackoffType__c = Queues.Backoff.NONE.name(),
                BackoffValue__c = 0,
                BackoffJitter__c = 0,
                PauseRequested__c = false,
                StalledCount__c = 0
            )
        );
    }
}
insert jobs;

Queues.MaintenanceResult result = Queues.runTerminalCleanupMaintenance();
Assert.areEqual(true, result.done);
Assert.areEqual(4, result.total);
Assert.areEqual(3, QueuesSupport.toInteger(result.countsByQueueName.get('e2e-maintenance-delete-all'), 0));
Assert.areEqual(1, QueuesSupport.toInteger(result.countsByQueueName.get('e2e-maintenance-delete-completed'), 0));
Assert.areEqual(0, QueuesSupport.toInteger(result.countsByQueueName.get('e2e-maintenance-delete-none'), 0));
Assert.areEqual(0, QueuesSupport.toInteger(result.countsByQueueName.get('e2e-maintenance-keep'), 0));

Set<String> actualRemainingExternalIds = new Set<String>();
for (Job__c job : [
    SELECT ExternalId__c
    FROM Job__c
    WHERE QueueName__c IN :queueNames
]) {
    actualRemainingExternalIds.add(job.ExternalId__c);
}

for (String externalId : expectedDeletedExternalIds) {
    Assert.areEqual(false, actualRemainingExternalIds.contains(externalId), 'Expected cleanup to delete ' + externalId);
}
for (String externalId : expectedKeptExternalIds) {
    Assert.areEqual(true, actualRemainingExternalIds.contains(externalId), 'Expected cleanup to keep ' + externalId);
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'maintenance.terminal-cleanup-policy',
            'result' => result,
            'deletedExternalIds' => expectedDeletedExternalIds,
            'keptExternalIds' => expectedKeptExternalIds
        }
    )
);
