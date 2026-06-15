String queueName = 'e2e-performance-queueable';
String processorName = 'performance-queueable-probe';
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
                'payload' => 'queueable-load-' + suffix,
                'delayMs' => delayMs
            },
            new Queues.JobOptions().jobId('perf-queueable-' + suffix).attempts(1)
        )
    );
}

List<Queues.Job> enqueuedJobs = Queues.of(queueName).addBulk(jobs, new Queues.BulkJobOptions().queueable());

List<Job__c> storedJobs = [
    SELECT Id, ExecutionMode__c, State__c, QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c = :queueName
];
List<QueueableDispatch__c> dispatches = [
    SELECT Id, Mode__c, State__c, RequestedCount__c, DispatchedCount__c, DispatcherAsyncApexJobId__c
    FROM QueueableDispatch__c
    WHERE QueueName__c = :queueName
];
Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];

Assert.areEqual(jobCount, enqueuedJobs.size());
Assert.areEqual(jobCount, storedJobs.size());
for (Job__c job : storedJobs) {
    Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), job.ExecutionMode__c);
    Assert.areEqual(Queues.State.WAITING.name(), job.State__c);
    Assert.areNotEqual(null, job.QueueableDispatch__c);
}
Assert.areEqual(1, dispatches.size());
Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), dispatches[0].Mode__c);
Assert.areEqual(Queues.QueueableDispatchState.WAITING.name(), dispatches[0].State__c);
Assert.areEqual(jobCount, QueuesSupport.toInteger(dispatches[0].RequestedCount__c, 0));
Assert.areEqual(0, QueuesSupport.toInteger(dispatches[0].DispatchedCount__c, 0));
Assert.isTrue(String.isNotBlank(dispatches[0].DispatcherAsyncApexJobId__c));
Assert.areEqual(0, runtimeCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'performance.queueable-serial.enqueue',
            'queueName' => queueName,
            'jobCount' => jobCount,
            'delayMs' => delayMs,
            'dispatchId' => dispatches[0].Id,
            'dispatcherAsyncApexJobId' => dispatches[0].DispatcherAsyncApexJobId__c
        }
    )
);
