String queueName = 'e2e-invocable-probe';
String completedBeforeWakeExternalId = 'invocable:stale:completed-before-wake';
String noLongerDueExternalId = 'invocable:stale:no-longer-due';
Set<String> externalIds = new Set<String>{ completedBeforeWakeExternalId, noLongerDueExternalId };

Map<String, Job__c> jobByExternalId = new Map<String, Job__c>();
for (Job__c job : [
    SELECT
        Id,
        ExternalId__c,
        State__c,
        AvailableAt__c,
        AttemptsMade__c,
        LeaseToken__c,
        LeaseExpiresAt__c,
        ReturnValue__c,
        FailedReason__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
]) {
    jobByExternalId.put(job.ExternalId__c, job);
}
Assert.areEqual(externalIds.size(), jobByExternalId.size());

Job__c completedBeforeWakeJob = jobByExternalId.get(completedBeforeWakeExternalId);
Assert.areEqual(Queues.State.COMPLETED, QueuesSupport.toState(completedBeforeWakeJob.State__c));
Assert.areEqual(0, QueuesSupport.toInteger(completedBeforeWakeJob.AttemptsMade__c, 0));
Assert.areEqual(null, completedBeforeWakeJob.LeaseToken__c);
Assert.areEqual(null, completedBeforeWakeJob.LeaseExpiresAt__c);
Assert.areEqual('{"manual":"completed-before-wake"}', completedBeforeWakeJob.ReturnValue__c);
Assert.areEqual(null, completedBeforeWakeJob.FailedReason__c);

Job__c noLongerDueJob = jobByExternalId.get(noLongerDueExternalId);
Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(noLongerDueJob.State__c));
Assert.areEqual(0, QueuesSupport.toInteger(noLongerDueJob.AttemptsMade__c, 0));
Assert.areEqual(null, noLongerDueJob.LeaseToken__c);
Assert.areEqual(null, noLongerDueJob.LeaseExpiresAt__c);
Assert.areEqual(null, noLongerDueJob.ReturnValue__c);
Assert.areEqual(null, noLongerDueJob.FailedReason__c);
Assert.isTrue(noLongerDueJob.AvailableAt__c > System.now().addMinutes(2));

Integer runCount = [
    SELECT COUNT()
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :queueName AND Job__r.ExternalId__c IN :externalIds
];
Assert.areEqual(0, runCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'invocable.stale-guards.assert',
            'queueName' => queueName,
            'completedBeforeWakeJobId' => completedBeforeWakeJob.Id,
            'noLongerDueJobId' => noLongerDueJob.Id
        }
    )
);
