String queueName = 'e2e-invocable-probe';
String completedBeforeWakeExternalId = 'invocable:stale:completed-before-wake';
String noLongerDueExternalId = 'invocable:stale:no-longer-due';
Set<String> externalIds = new Set<String>{ completedBeforeWakeExternalId, noLongerDueExternalId };

Map<String, Job__c> jobByExternalId = new Map<String, Job__c>();
for (Job__c job : [
    SELECT Id, ExternalId__c, State__c, AvailableAt__c, AttemptsMade__c, ReturnValue__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
    FOR UPDATE
]) {
    jobByExternalId.put(job.ExternalId__c, job);
}
Assert.areEqual(externalIds.size(), jobByExternalId.size());

Job__c completedBeforeWakeJob = jobByExternalId.get(completedBeforeWakeExternalId);
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(completedBeforeWakeJob.State__c));
completedBeforeWakeJob.State__c = Queues.State.COMPLETED.name();
completedBeforeWakeJob.ReturnValue__c = '{"manual":"completed-before-wake"}';

Job__c noLongerDueJob = jobByExternalId.get(noLongerDueExternalId);
Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(noLongerDueJob.State__c));
noLongerDueJob.State__c = Queues.State.DELAYED.name();
noLongerDueJob.AvailableAt__c = System.now().addMinutes(10);

update new List<Job__c>{ completedBeforeWakeJob, noLongerDueJob };

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'invocable.stale-guards.mutate',
            'queueName' => queueName,
            'completedBeforeWakeJobId' => completedBeforeWakeJob.Id,
            'noLongerDueJobId' => noLongerDueJob.Id,
            'noLongerDueAvailableAt' => noLongerDueJob.AvailableAt__c
        }
    )
);
