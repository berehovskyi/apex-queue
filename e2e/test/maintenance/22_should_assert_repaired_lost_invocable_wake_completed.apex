String queueName = 'e2e-maintenance-repair';
String invocableExternalId = 'maintenance:repair:lost-invocable-wake';
String queueableExternalId = 'maintenance:repair:stale-queueable-handoff';
String workerExternalId = 'maintenance:repair:expired-worker-lease';
String schedulerName = 'maintenance-repair-scheduler';
Set<String> repairExternalIds = new Set<String>{ invocableExternalId, queueableExternalId, workerExternalId };

Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
for (Job__c job : [
    SELECT
        Id,
        ExternalId__c,
        State__c,
        ExecutionMode__c,
        AttemptsMade__c,
        FailedReason__c,
        ReturnValue__c,
        Progress__c,
        LeaseToken__c,
        LeaseExpiresAt__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :repairExternalIds
]) {
    jobsByExternalId.put(job.ExternalId__c, job);
}
Assert.areEqual(3, jobsByExternalId.size());

for (String externalId : repairExternalIds) {
    Job__c job = jobsByExternalId.get(externalId);
    Assert.areEqual(
        Queues.State.COMPLETED,
        QueuesSupport.toState(job.State__c),
        'Expected completion for ' + externalId
    );
    Assert.areEqual(null, job.FailedReason__c);
    Assert.areEqual(null, job.LeaseToken__c);
    Assert.areEqual(null, job.LeaseExpiresAt__c);

    Map<String, Object> returnValue = (Map<String, Object>) JSON.deserializeUntyped(job.ReturnValue__c);
    Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(job.Progress__c);
    Assert.areEqual(externalId, String.valueOf(returnValue.get('payload')));
    Assert.areEqual('completed', String.valueOf(progress.get('status')));
}

Job__c invocableJob = jobsByExternalId.get(invocableExternalId);
Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), invocableJob.ExecutionMode__c);
Assert.areEqual(1, QueuesSupport.toInteger(invocableJob.AttemptsMade__c, 0));

Job__c queueableJob = jobsByExternalId.get(queueableExternalId);
Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), queueableJob.ExecutionMode__c);
Assert.areEqual(1, QueuesSupport.toInteger(queueableJob.AttemptsMade__c, 0));

Job__c workerJob = jobsByExternalId.get(workerExternalId);
Assert.areEqual(Queues.ExecutionMode.WORKER.name(), workerJob.ExecutionMode__c);
Assert.areEqual(2, QueuesSupport.toInteger(workerJob.AttemptsMade__c, 0));

Map<Id, Integer> completedRunsByJobId = new Map<Id, Integer>();
Map<Id, Integer> failedRunsByJobId = new Map<Id, Integer>();
Set<Id> repairedJobIds = new Map<Id, Job__c>(jobsByExternalId.values()).keySet();
for (AggregateResult row : [
    SELECT Job__c jobId, Status__c statusValue, COUNT(Id) rowCount
    FROM JobRun__c
    WHERE Job__c IN :repairedJobIds
    GROUP BY Job__c, Status__c
]) {
    Id jobId = (Id) row.get('jobId');
    Integer rowCount = QueuesSupport.toInteger(row.get('rowCount'), 0);
    if ((String) row.get('statusValue') == Queues.RunStatus.COMPLETED.name()) {
        completedRunsByJobId.put(jobId, rowCount);
    }
    if ((String) row.get('statusValue') == Queues.RunStatus.FAILED.name()) {
        failedRunsByJobId.put(jobId, rowCount);
    }
}
Assert.areEqual(1, QueuesSupport.toInteger(completedRunsByJobId.get(invocableJob.Id), 0));
Assert.areEqual(1, QueuesSupport.toInteger(completedRunsByJobId.get(queueableJob.Id), 0));
Assert.areEqual(1, QueuesSupport.toInteger(completedRunsByJobId.get(workerJob.Id), 0));
Assert.areEqual(1, QueuesSupport.toInteger(failedRunsByJobId.get(workerJob.Id), 0));

JobScheduler__c repairedScheduler = [
    SELECT Id, IsActive__c, ScheduledJobId__c, NextRunAt__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];
Assert.areEqual(true, repairedScheduler.IsActive__c);
Assert.isTrue(String.isNotBlank(repairedScheduler.ScheduledJobId__c));
Assert.isTrue(repairedScheduler.NextRunAt__c > System.now());

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'maintenance.repair.assert',
            'queueName' => queueName,
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
