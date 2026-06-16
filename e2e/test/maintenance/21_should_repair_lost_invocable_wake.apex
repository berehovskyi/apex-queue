String queueName = 'e2e-maintenance-repair';
String invocableExternalId = 'maintenance:repair:lost-invocable-wake';
String queueableExternalId = 'maintenance:repair:stale-queueable-handoff';
String workerExternalId = 'maintenance:repair:expired-worker-lease';
String schedulerName = 'maintenance-repair-scheduler';
Set<String> repairExternalIds = new Set<String>{ invocableExternalId, queueableExternalId, workerExternalId };
String failedRunStatus = Queues.RunStatus.FAILED.name();

Map<String, Job__c> jobsBeforeRepair = new Map<String, Job__c>();
for (Job__c job : [
    SELECT
        Id,
        ExternalId__c,
        State__c,
        AvailableAt__c,
        AttemptsMade__c,
        LeaseToken__c,
        LeaseExpiresAt__c,
        QueueableDispatchInitiatedAt__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :repairExternalIds
]) {
    jobsBeforeRepair.put(job.ExternalId__c, job);
}
Assert.areEqual(3, jobsBeforeRepair.size());

Datetime staleInvocableAvailableAt = jobsBeforeRepair.get(invocableExternalId).AvailableAt__c;
Datetime staleQueueableInitiatedAt = jobsBeforeRepair.get(queueableExternalId).QueueableDispatchInitiatedAt__c;
Datetime repairStartedAt = System.now();
Assert.areEqual(Queues.State.ACTIVE, QueuesSupport.toState(jobsBeforeRepair.get(workerExternalId).State__c));
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(jobsBeforeRepair.get(queueableExternalId).State__c));
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(jobsBeforeRepair.get(invocableExternalId).State__c));

Queues.MaintenanceResult runtimeResult = Queues.runRuntimeMaintenance();
Assert.isTrue(
    runtimeResult.total >= 3,
    'Expected runtime maintenance to repair worker, queueable, and invocable paths.'
);
Assert.isTrue(
    QueuesSupport.toInteger(runtimeResult.countsByQueueName.get(queueName), 0) >= 3,
    'Expected runtime maintenance to report repair work for ' + queueName + '.'
);

Queues.MaintenanceResult schedulerResult = Queues.runJobSchedulerMaintenance();
Assert.isTrue(schedulerResult.total >= 1, 'Expected job scheduler maintenance to repair the missing native schedule.');
Assert.isTrue(
    QueuesSupport.toInteger(schedulerResult.countsByQueueName.get(queueName), 0) >= 1,
    'Expected job scheduler maintenance to report repair work for ' + queueName + '.'
);

Map<String, Job__c> jobsAfterRepair = new Map<String, Job__c>();
for (Job__c job : [
    SELECT
        Id,
        ExternalId__c,
        State__c,
        ExecutionMode__c,
        AvailableAt__c,
        AttemptsMade__c,
        AsyncApexJobId__c,
        LeaseToken__c,
        LeaseExpiresAt__c,
        QueueableDispatchInitiatedAt__c,
        StalledCount__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :repairExternalIds
]) {
    jobsAfterRepair.put(job.ExternalId__c, job);
}

Job__c invocableJob = jobsAfterRepair.get(invocableExternalId);
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(invocableJob.State__c));
Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), invocableJob.ExecutionMode__c);
Assert.isTrue(invocableJob.AvailableAt__c > staleInvocableAvailableAt);
Assert.isTrue(invocableJob.AvailableAt__c > repairStartedAt);
Assert.areEqual(null, invocableJob.LeaseToken__c);

Job__c queueableJob = jobsAfterRepair.get(queueableExternalId);
Assert.areEqual(Queues.State.ACTIVE, QueuesSupport.toState(queueableJob.State__c));
Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), queueableJob.ExecutionMode__c);
Assert.isTrue(queueableJob.QueueableDispatchInitiatedAt__c > staleQueueableInitiatedAt);
Assert.isTrue(String.isNotBlank(queueableJob.AsyncApexJobId__c));

Job__c workerJob = jobsAfterRepair.get(workerExternalId);
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(workerJob.State__c));
Assert.areEqual(Queues.ExecutionMode.WORKER.name(), workerJob.ExecutionMode__c);
Assert.areEqual(null, workerJob.LeaseToken__c);
Assert.areEqual(null, workerJob.LeaseExpiresAt__c);
Assert.areEqual(1, QueuesSupport.toInteger(workerJob.StalledCount__c, 0));

Integer recoveredWorkerRunCount = [
    SELECT COUNT()
    FROM JobRun__c
    WHERE Job__c = :workerJob.Id AND Status__c = :failedRunStatus
];
Assert.areEqual(1, recoveredWorkerRunCount);

JobScheduler__c repairedScheduler = [
    SELECT Id, IsActive__c, ScheduledJobId__c, NextRunAt__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];
Assert.areEqual(true, repairedScheduler.IsActive__c);
Assert.isTrue(String.isNotBlank(repairedScheduler.ScheduledJobId__c));

List<CronTrigger> activeSchedulerJobs = [
    SELECT Id, State, NextFireTime
    FROM CronTrigger
    WHERE Id = :repairedScheduler.ScheduledJobId__c AND State NOT IN ('DELETED', 'COMPLETE', 'ERROR')
];
Assert.areEqual(1, activeSchedulerJobs.size());
Assert.isTrue(activeSchedulerJobs[0].NextFireTime >= repairedScheduler.NextRunAt__c.addSeconds(-5));

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'maintenance.repair',
            'queueName' => queueName,
            'runtimeResult' => runtimeResult,
            'schedulerResult' => schedulerResult,
            'jobIds' => new Map<String, Object>{
                'invocable' => invocableJob.Id,
                'queueable' => queueableJob.Id,
                'worker' => workerJob.Id
            },
            'schedulerId' => repairedScheduler.Id,
            'scheduledJobId' => repairedScheduler.ScheduledJobId__c
        }
    )
);
