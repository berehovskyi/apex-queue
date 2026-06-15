String queueName = 'e2e-invocable-probe';
String processorName = 'invocable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
Set<String> externalIds = new Set<String>{ 'invocable:stale:completed-before-wake', 'invocable:stale:no-longer-due' };

Queues.Job completedBeforeWakeJob = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'completed-before-wake' },
        new Queues.JobOptions().jobId('invocable:stale:completed-before-wake').attempts(2),
        new Queues.BulkJobOptions().invocable()
    );

Queues.Job noLongerDueJob = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'no-longer-due' },
        new Queues.JobOptions().jobId('invocable:stale:no-longer-due').attempts(2),
        new Queues.BulkJobOptions().invocable()
    );

List<Job__c> jobs = [
    SELECT Id, ExternalId__c, ExecutionMode__c, State__c, AttemptsMade__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
];
Assert.areEqual(externalIds.size(), jobs.size());
for (Job__c job : jobs) {
    Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), job.ExecutionMode__c);
    Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(job.State__c));
    Assert.areEqual(0, QueuesSupport.toInteger(job.AttemptsMade__c, 0));
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'invocable.stale-guards.enqueue',
            'queueName' => queueName,
            'completedBeforeWakeJobId' => completedBeforeWakeJob.getId(),
            'noLongerDueJobId' => noLongerDueJob.getId()
        }
    )
);
