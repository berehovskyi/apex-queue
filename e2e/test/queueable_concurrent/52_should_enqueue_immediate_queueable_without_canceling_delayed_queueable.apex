String queueName = 'e2e-queueable-concurrent-probe';
String processorName = 'queueable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
Set<String> externalIds = new Set<String>{ 'queueable-replace-delayed', 'queueable-replace-immediate' };

Queues.Job delayedJob = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'replace-delayed' },
        new Queues.JobOptions().jobId('queueable-replace-delayed').delay(1),
        new Queues.BulkJobOptions().queueableConcurrent()
    );

Queues.Job immediateJob = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'replace-immediate' },
        new Queues.JobOptions().jobId('queueable-replace-immediate'),
        new Queues.BulkJobOptions().queueableConcurrent()
    );

List<Job__c> storedJobs = [
    SELECT Id, ExternalId__c, State__c, AvailableAt__c, ExecutionMode__c, QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
];
Assert.areEqual(externalIds.size(), storedJobs.size());

Set<Id> dispatchIds = new Set<Id>();
for (Job__c job : storedJobs) {
    Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), job.ExecutionMode__c);
    Assert.areNotEqual(null, job.QueueableDispatch__c);
    dispatchIds.add(job.QueueableDispatch__c);
    if (job.ExternalId__c == 'queueable-replace-delayed') {
        Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(job.State__c));
        Assert.isTrue(job.AvailableAt__c > System.now());
    } else {
        Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(job.State__c));
    }
}
Assert.areEqual(externalIds.size(), dispatchIds.size());

Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Integer jobRunCount = [SELECT COUNT() FROM JobRun__c WHERE Job__r.QueueName__c = :queueName];
Assert.areEqual(0, runtimeCount);
Assert.areEqual(0, jobRunCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queueable_concurrent.immediate-does-not-cancel-delayed',
            'queueName' => queueName,
            'delayedJobId' => delayedJob.getId(),
            'immediateJobId' => immediateJob.getId(),
            'dispatchIds' => new List<Id>(dispatchIds)
        }
    )
);
