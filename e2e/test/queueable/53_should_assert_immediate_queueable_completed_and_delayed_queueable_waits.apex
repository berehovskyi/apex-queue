String queueName = 'e2e-queueable-probe';
String delayedExternalId = 'queueable-replace-delayed';
String immediateExternalId = 'queueable-replace-immediate';
Set<String> externalIds = new Set<String>{ delayedExternalId, immediateExternalId };

List<Job__c> jobs = [
    SELECT Id, ExternalId__c, State__c, ReturnValue__c, AsyncApexJobId__c, QueueableDispatch__c, FailedReason__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
];
Assert.areEqual(externalIds.size(), jobs.size());

Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
for (Job__c job : jobs) {
    jobsByExternalId.put(job.ExternalId__c, job);
}
Job__c delayedJob = jobsByExternalId.get(delayedExternalId);
Job__c immediateJob = jobsByExternalId.get(immediateExternalId);

Assert.areNotEqual(null, delayedJob);
Assert.areNotEqual(null, immediateJob);
Assert.areEqual(Queues.State.COMPLETED, QueuesSupport.toState(immediateJob.State__c));
Assert.isTrue(String.isBlank(immediateJob.AsyncApexJobId__c));

Map<String, Object> returnValue = (Map<String, Object>) JSON.deserializeUntyped(immediateJob.ReturnValue__c);
Assert.areEqual('replace-immediate', String.valueOf(returnValue.get('payload')));

Queues.State delayedState = QueuesSupport.toState(delayedJob.State__c);
Assert.isTrue(
    delayedState == Queues.State.DELAYED || delayedState == Queues.State.ACTIVE,
    'Expected delayed queueable job to still wait for its native queueable delay, but it was ' +
        delayedJob.State__c +
        ' (' +
        delayedJob.FailedReason__c +
        ').'
);

Integer delayedRunCount = [SELECT COUNT() FROM JobRun__c WHERE Job__c = :delayedJob.Id];
List<JobRun__c> immediateRuns = [
    SELECT Status__c, AsyncApexJobId__c
    FROM JobRun__c
    WHERE Job__c = :immediateJob.Id
];
Assert.areEqual(0, delayedRunCount);
Assert.areEqual(1, immediateRuns.size());
Assert.areEqual(Queues.RunStatus.COMPLETED.name(), QueuesSupport.normalizeEnumValue(immediateRuns[0].Status__c));
Assert.isTrue(String.isNotBlank(immediateRuns[0].AsyncApexJobId__c));

Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Assert.areEqual(0, runtimeCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queueable.immediate-completed-delayed-waits',
            'queueName' => queueName,
            'delayedJobId' => delayedJob.Id,
            'delayedState' => delayedJob.State__c,
            'immediateJobId' => immediateJob.Id
        }
    )
);
