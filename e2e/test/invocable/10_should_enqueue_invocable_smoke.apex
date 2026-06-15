String queueName = 'e2e-invocable-probe';
String processorName = 'invocable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
Set<String> externalIds = new Set<String>{ 'invocable:complete:one', 'invocable:complete:two' };

List<Queues.Job> jobs = Queues.of(queueName)
    .addBulk(
        new List<Queues.JobData>{
            new Queues.JobData(
                processorName,
                processorClass,
                new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'one' },
                new Queues.JobOptions().jobId('invocable:complete:one').attempts(2)
            ),
            new Queues.JobData(
                processorName,
                processorClass,
                new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'two' },
                new Queues.JobOptions().jobId('invocable:complete:two').attempts(2)
            )
        },
        new Queues.BulkJobOptions().invocable()
    );

List<Job__c> storedJobs = [
    SELECT Id, ExternalId__c, ExecutionMode__c, State__c, LeaseToken__c, LeaseExpiresAt__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
];
Assert.areEqual(externalIds.size(), storedJobs.size());
for (Job__c job : storedJobs) {
    Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), job.ExecutionMode__c);
    Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(job.State__c));
    Assert.areEqual(null, job.LeaseToken__c);
    Assert.areEqual(null, job.LeaseExpiresAt__c);
}

Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Integer dispatchCount = [SELECT COUNT() FROM QueueableDispatch__c WHERE QueueName__c = :queueName];
Assert.areEqual(0, runtimeCount);
Assert.areEqual(0, dispatchCount);

List<String> jobIds = new List<String>();
for (Queues.Job job : jobs) {
    jobIds.add(job.getId());
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'invocable.smoke.enqueue',
            'queueName' => queueName,
            'externalIds' => new List<String>(externalIds),
            'jobIds' => jobIds
        }
    )
);
