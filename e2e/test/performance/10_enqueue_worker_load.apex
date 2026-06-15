String queueName = 'e2e-performance-worker';
String processorName = 'performance-worker-probe';
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
            new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'worker-load-' + suffix, 'delayMs' => delayMs },
            new Queues.JobOptions().jobId('perf-worker-' + suffix).attempts(1)
        )
    );
}

List<Queues.Job> enqueuedJobs = Queues.of(queueName).addBulk(jobs);

List<Job__c> storedJobs = [
    SELECT Id, ExecutionMode__c, State__c, QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c = :queueName
];
List<QueueRuntime__c> runtimes = [
    SELECT WorkerToken__c, AsyncApexJobId__c, ActiveWorkerCount__c, IsPaused__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
];

Assert.areEqual(jobCount, enqueuedJobs.size());
Assert.areEqual(jobCount, storedJobs.size());
for (Job__c job : storedJobs) {
    Assert.areEqual(Queues.ExecutionMode.WORKER.name(), job.ExecutionMode__c);
    Assert.areEqual(Queues.State.WAITING.name(), job.State__c);
    Assert.areEqual(null, job.QueueableDispatch__c);
}
Assert.areEqual(1, runtimes.size());
Assert.areEqual(false, runtimes[0].IsPaused__c);
Assert.isTrue(String.isNotBlank(runtimes[0].WorkerToken__c));
Assert.isTrue(String.isNotBlank(runtimes[0].AsyncApexJobId__c));

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'performance.worker.enqueue',
            'queueName' => queueName,
            'jobCount' => jobCount,
            'delayMs' => delayMs,
            'workerToken' => runtimes[0].WorkerToken__c,
            'asyncApexJobId' => runtimes[0].AsyncApexJobId__c
        }
    )
);
