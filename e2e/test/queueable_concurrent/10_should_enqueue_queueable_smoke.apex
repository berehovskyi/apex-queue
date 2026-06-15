String queueName = 'e2e-queueable-concurrent-probe';
String processorName = 'queueable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
Set<String> externalIds = new Set<String>{ 'queueable:complete:one', 'queueable:complete:two' };

List<Queues.Job> jobs = Queues.of(queueName)
    .addBulk(
        new List<Queues.JobData>{
            new Queues.JobData(
                processorName,
                processorClass,
                new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'one' },
                new Queues.JobOptions().jobId('queueable:complete:one').attempts(2)
            ),
            new Queues.JobData(
                processorName,
                processorClass,
                new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'two' },
                new Queues.JobOptions().jobId('queueable:complete:two').attempts(2)
            )
        },
        new Queues.BulkJobOptions().queueableConcurrent()
    );

List<Job__c> storedJobs = [
    SELECT Id, ExternalId__c, ExecutionMode__c, QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
];
Assert.areEqual(externalIds.size(), storedJobs.size());
Set<Id> dispatchIds = new Set<Id>();
for (Job__c job : storedJobs) {
    Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), job.ExecutionMode__c);
    Assert.areNotEqual(null, job.QueueableDispatch__c);
    dispatchIds.add(job.QueueableDispatch__c);
}
Assert.areEqual(1, dispatchIds.size());

Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Assert.areEqual(0, runtimeCount);

List<String> jobIds = new List<String>();
for (Queues.Job job : jobs) {
    jobIds.add(job.getId());
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queueable_concurrent.smoke.enqueue',
            'queueName' => queueName,
            'externalIds' => new List<String>(externalIds),
            'jobIds' => jobIds,
            'dispatchIds' => new List<Id>(dispatchIds)
        }
    )
);
