String queueName = 'e2e-performance-invocable';
String processorName = 'performance-invocable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
Integer jobCount = 500;
Integer delayMs = 1000;
List<Queues.JobData> jobs = new List<Queues.JobData>();

for (Integer index = 0; index < jobCount; index++) {
    String suffix = index.toString().leftPad(3, '0');
    jobs.add(
        new Queues.JobData(
            processorName,
            processorClass,
            new Map<String, Object>{
                'mode' => 'complete',
                'payload' => 'invocable-load-' + suffix,
                'delayMs' => delayMs
            },
            new Queues.JobOptions().jobId('perf-invocable-' + suffix).attempts(1)
        )
    );
}

List<Queues.Job> enqueuedJobs = Queues.of(queueName).addBulk(jobs, new Queues.BulkJobOptions().invocable());

List<Job__c> storedJobs = [
    SELECT Id, ExecutionMode__c, State__c, QueueableDispatch__c, LeaseToken__c, LeaseExpiresAt__c
    FROM Job__c
    WHERE QueueName__c = :queueName
];
Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Integer dispatchCount = [SELECT COUNT() FROM QueueableDispatch__c WHERE QueueName__c = :queueName];

Assert.areEqual(jobCount, enqueuedJobs.size());
Assert.areEqual(jobCount, storedJobs.size());
for (Job__c job : storedJobs) {
    Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), job.ExecutionMode__c);
    Assert.areEqual(Queues.State.WAITING.name(), job.State__c);
    Assert.areEqual(null, job.QueueableDispatch__c);
    Assert.areEqual(null, job.LeaseToken__c);
    Assert.areEqual(null, job.LeaseExpiresAt__c);
}
Assert.areEqual(0, runtimeCount);
Assert.areEqual(0, dispatchCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'performance.invocable.enqueue',
            'queueName' => queueName,
            'jobCount' => jobCount,
            'delayMs' => delayMs
        }
    )
);
