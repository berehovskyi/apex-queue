List<String> queueNames = new List<String>{
    'e2e-performance-worker-parallel-1',
    'e2e-performance-worker-parallel-2',
    'e2e-performance-worker-parallel-3',
    'e2e-performance-worker-parallel-4',
    'e2e-performance-worker-parallel-5'
};
String processorName = 'performance-worker-parallel-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
Integer jobCount = 500;
Integer jobsPerQueue = jobCount / queueNames.size();
Integer delayMs = 1000;
Integer enqueuedJobCount = 0;

for (Integer queueIndex = 0; queueIndex < queueNames.size(); queueIndex++) {
    String queueName = queueNames[queueIndex];
    List<Queues.JobData> jobs = new List<Queues.JobData>();
    for (Integer jobIndex = 0; jobIndex < jobsPerQueue; jobIndex++) {
        Integer globalIndex = queueIndex * jobsPerQueue + jobIndex;
        String suffix = globalIndex.toString().leftPad(3, '0');
        jobs.add(
            new Queues.JobData(
                processorName,
                processorClass,
                new Map<String, Object>{
                    'mode' => 'complete',
                    'payload' => 'worker-parallel-load-' + suffix,
                    'delayMs' => delayMs
                },
                new Queues.JobOptions().jobId('perf-worker-parallel-' + suffix).attempts(1)
            )
        );
    }
    enqueuedJobCount += Queues.of(queueName).addBulk(jobs).size();
}

List<Job__c> storedJobs = [
    SELECT Id, ExecutionMode__c, State__c, QueueName__c, QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c IN :queueNames
];
Map<String, Integer> jobCountsByQueue = new Map<String, Integer>();
for (Job__c job : storedJobs) {
    Assert.areEqual(Queues.ExecutionMode.WORKER.name(), job.ExecutionMode__c);
    Assert.areEqual(Queues.State.WAITING.name(), job.State__c);
    Assert.areEqual(null, job.QueueableDispatch__c);
    jobCountsByQueue.put(job.QueueName__c, (jobCountsByQueue.get(job.QueueName__c) ?? 0) + 1);
}

List<QueueRuntime__c> runtimes = [
    SELECT QueueName__c, WorkerToken__c, AsyncApexJobId__c, ActiveWorkerCount__c, IsPaused__c
    FROM QueueRuntime__c
    WHERE QueueName__c IN :queueNames
];
Set<String> workerTokens = new Set<String>();
Set<String> asyncApexJobIds = new Set<String>();
for (QueueRuntime__c runtime : runtimes) {
    Assert.areEqual(false, runtime.IsPaused__c);
    Assert.isTrue(String.isNotBlank(runtime.WorkerToken__c));
    Assert.isTrue(String.isNotBlank(runtime.AsyncApexJobId__c));
    workerTokens.add(runtime.WorkerToken__c);
    asyncApexJobIds.add(runtime.AsyncApexJobId__c);
}

Assert.areEqual(jobCount, enqueuedJobCount);
Assert.areEqual(jobCount, storedJobs.size());
Assert.areEqual(queueNames.size(), runtimes.size());
Assert.areEqual(queueNames.size(), workerTokens.size());
Assert.areEqual(queueNames.size(), asyncApexJobIds.size());
for (String queueName : queueNames) {
    Assert.areEqual(jobsPerQueue, jobCountsByQueue.get(queueName));
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'performance.worker.parallel.enqueue',
            'queueNames' => queueNames,
            'jobCount' => jobCount,
            'jobsPerQueue' => jobsPerQueue,
            'delayMs' => delayMs,
            'workerTokens' => workerTokens,
            'asyncApexJobIds' => asyncApexJobIds
        }
    )
);
