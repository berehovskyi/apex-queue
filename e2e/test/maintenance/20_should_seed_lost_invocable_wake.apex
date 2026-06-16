String queueName = 'e2e-maintenance-repair';
String processorName = 'maintenance-repair-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String invocableExternalId = 'maintenance:repair:lost-invocable-wake';
String queueableExternalId = 'maintenance:repair:stale-queueable-handoff';
String workerExternalId = 'maintenance:repair:expired-worker-lease';
String schedulerName = 'maintenance-repair-scheduler';
Datetime staleAt = System.now().addMinutes(-10);
Datetime schedulerRunAt = System.now().addMinutes(10);
Set<String> repairExternalIds = new Set<String>{ invocableExternalId, queueableExternalId, workerExternalId };

insert new QueueRuntime__c(
    QueueName__c = queueName,
    IsPaused__c = false,
    ActiveWorkerCount__c = 1,
    LastPumpAt__c = staleAt,
    LastSweepAt__c = staleAt
);

List<Job__c> jobs = new List<Job__c>{
    new Job__c(
        QueueName__c = queueName,
        Name__c = processorName,
        State__c = Queues.State.WAITING.name(),
        ExecutionMode__c = Queues.ExecutionMode.WORKER.name(),
        ExternalId__c = invocableExternalId,
        QueueScopedExternalId__c = QueuesSupport.buildQueueScopedExternalId(queueName, invocableExternalId),
        ProcessorClass__c = processorClass,
        Data__c = QueuesSupport.serialize(
            new Map<String, Object>{ 'mode' => 'complete', 'payload' => invocableExternalId }
        ),
        AvailableAt__c = staleAt,
        Priority__c = 0,
        AttemptsMade__c = 0,
        MaxAttempts__c = 2,
        BackoffType__c = Queues.Backoff.NONE.name(),
        BackoffValue__c = 0,
        BackoffJitter__c = 0,
        PauseRequested__c = false,
        StalledCount__c = 0
    ),
    new Job__c(
        QueueName__c = queueName,
        Name__c = processorName,
        State__c = Queues.State.WAITING.name(),
        ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(),
        ExternalId__c = queueableExternalId,
        QueueScopedExternalId__c = QueuesSupport.buildQueueScopedExternalId(queueName, queueableExternalId),
        ProcessorClass__c = processorClass,
        Data__c = QueuesSupport.serialize(
            new Map<String, Object>{ 'mode' => 'complete', 'payload' => queueableExternalId }
        ),
        AvailableAt__c = staleAt,
        Priority__c = 0,
        AttemptsMade__c = 0,
        MaxAttempts__c = 2,
        BackoffType__c = Queues.Backoff.NONE.name(),
        BackoffValue__c = 0,
        BackoffJitter__c = 0,
        QueueableDispatchInitiatedAt__c = staleAt,
        PauseRequested__c = false,
        StalledCount__c = 0
    ),
    new Job__c(
        QueueName__c = queueName,
        Name__c = processorName,
        State__c = Queues.State.ACTIVE.name(),
        ExecutionMode__c = Queues.ExecutionMode.WORKER.name(),
        ExternalId__c = workerExternalId,
        QueueScopedExternalId__c = QueuesSupport.buildQueueScopedExternalId(queueName, workerExternalId),
        ProcessorClass__c = processorClass,
        Data__c = QueuesSupport.serialize(
            new Map<String, Object>{ 'mode' => 'complete', 'payload' => workerExternalId }
        ),
        AvailableAt__c = staleAt,
        Priority__c = 0,
        AttemptsMade__c = 1,
        MaxAttempts__c = 2,
        BackoffType__c = Queues.Backoff.NONE.name(),
        BackoffValue__c = 0,
        BackoffJitter__c = 0,
        LeaseToken__c = 'maintenance-stale-worker-token',
        LeaseExpiresAt__c = staleAt,
        PauseRequested__c = false,
        StalledCount__c = 0
    )
};
insert jobs;

// Switching only transport simulates a durable due job whose original invocable wake was lost.
update new Job__c(Id = jobs[0].Id, ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name());

JobScheduler__c brokenScheduler = new JobScheduler__c(
    Name = schedulerName,
    QueueName__c = queueName,
    ExternalId__c = QueuesSupport.buildQueueScopedExternalId(queueName, schedulerName),
    IsActive__c = true,
    JobName__c = processorName,
    ProcessorClass__c = processorClass,
    Data__c = QueuesSupport.serialize(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'scheduler-repair' }),
    ExecutionMode__c = Queues.ExecutionMode.WORKER.name(),
    EveryMinutes__c = 1,
    Limit__c = 2,
    StartAt__c = schedulerRunAt,
    NextRunAt__c = schedulerRunAt,
    IterationCount__c = 1
);
insert brokenScheduler;

Map<String, Job__c> seededJobsByExternalId = new Map<String, Job__c>();
for (Job__c job : [
    SELECT
        Id,
        ExternalId__c,
        State__c,
        ExecutionMode__c,
        AvailableAt__c,
        AttemptsMade__c,
        LeaseToken__c,
        LeaseExpiresAt__c,
        QueueableDispatchInitiatedAt__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :repairExternalIds
]) {
    seededJobsByExternalId.put(job.ExternalId__c, job);
}

Job__c invocableJob = seededJobsByExternalId.get(invocableExternalId);
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(invocableJob.State__c));
Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), invocableJob.ExecutionMode__c);
Assert.areEqual(staleAt, invocableJob.AvailableAt__c);
Assert.areEqual(0, QueuesSupport.toInteger(invocableJob.AttemptsMade__c, 0));
Assert.areEqual(null, invocableJob.LeaseToken__c);

Job__c queueableJob = seededJobsByExternalId.get(queueableExternalId);
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(queueableJob.State__c));
Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), queueableJob.ExecutionMode__c);
Assert.areEqual(staleAt, queueableJob.QueueableDispatchInitiatedAt__c);

Job__c workerJob = seededJobsByExternalId.get(workerExternalId);
Assert.areEqual(Queues.State.ACTIVE, QueuesSupport.toState(workerJob.State__c));
Assert.areEqual(Queues.ExecutionMode.WORKER.name(), workerJob.ExecutionMode__c);
Assert.areEqual(staleAt, workerJob.LeaseExpiresAt__c);
Assert.isTrue(String.isNotBlank(workerJob.LeaseToken__c));

JobScheduler__c seededScheduler = [
    SELECT Id, IsActive__c, ScheduledJobId__c, EveryMinutes__c, NextRunAt__c
    FROM JobScheduler__c
    WHERE Id = :brokenScheduler.Id
    LIMIT 1
];
Assert.areEqual(true, seededScheduler.IsActive__c);
Assert.areEqual(null, seededScheduler.ScheduledJobId__c);
Assert.areEqual(1, QueuesSupport.toInteger(seededScheduler.EveryMinutes__c, 0));
Assert.areEqual(schedulerRunAt, seededScheduler.NextRunAt__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'maintenance.repair.seed',
            'queueName' => queueName,
            'invocableJobId' => invocableJob.Id,
            'queueableJobId' => queueableJob.Id,
            'workerJobId' => workerJob.Id,
            'schedulerId' => seededScheduler.Id
        }
    )
);
