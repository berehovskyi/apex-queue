Set<String> queueNames = new Set<String>{
    'e2e-performance-worker-parallel-1',
    'e2e-performance-worker-parallel-2',
    'e2e-performance-worker-parallel-3',
    'e2e-performance-worker-parallel-4',
    'e2e-performance-worker-parallel-5'
};
Integer jobCount = 500;
Integer jobsPerQueue = jobCount / queueNames.size();

List<Job__c> jobs = [
    SELECT ExternalId__c, FailedReason__c, Progress__c, QueueName__c, State__c
    FROM Job__c
    WHERE QueueName__c IN :queueNames
    ORDER BY QueueName__c ASC, ExternalId__c ASC
];
Map<String, Integer> jobCountsByQueue = new Map<String, Integer>();
List<String> unfinishedJobs = new List<String>();
for (Job__c job : jobs) {
    jobCountsByQueue.put(job.QueueName__c, (jobCountsByQueue.get(job.QueueName__c) ?? 0) + 1);
    if (QueuesSupport.toState(job.State__c) != Queues.State.COMPLETED) {
        unfinishedJobs.add(
            job.QueueName__c + '/' + job.ExternalId__c + '=' + job.State__c + ' (' + job.FailedReason__c + ')'
        );
        continue;
    }

    Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(job.Progress__c);
    Assert.isTrue(String.isNotBlank((String) progress.get('startedAtMs')));
}
Assert.areEqual(jobCount, jobs.size());
Assert.areEqual(
    0,
    unfinishedJobs.size(),
    'Parallel worker jobs are unfinished or failed: ' + String.join(unfinishedJobs, ', ')
);
for (String queueName : queueNames) {
    Assert.areEqual(jobsPerQueue, jobCountsByQueue.get(queueName));
}

List<JobRun__c> runs = [
    SELECT AsyncApexJobId__c, DurationMs__c, Job__r.Progress__c, Job__r.QueueName__c, Status__c
    FROM JobRun__c
    WHERE Job__r.QueueName__c IN :queueNames
];
List<String> runQueueNames = new List<String>();
List<Long> runStartedAtMs = new List<Long>();
List<Long> runFinishedAtMs = new List<Long>();
Set<String> asyncApexJobIds = new Set<String>();
for (JobRun__c run : runs) {
    Assert.areEqual(Queues.RunStatus.COMPLETED.name(), QueuesSupport.normalizeEnumValue(run.Status__c));
    Assert.isTrue(String.isNotBlank(run.AsyncApexJobId__c));
    asyncApexJobIds.add(run.AsyncApexJobId__c);

    Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(run.Job__r.Progress__c);
    Long startedAtMs = Long.valueOf((String) progress.get('startedAtMs'));
    Long durationMs = run.DurationMs__c.longValue();
    runQueueNames.add(run.Job__r.QueueName__c);
    runStartedAtMs.add(startedAtMs);
    runFinishedAtMs.add(startedAtMs + durationMs);
}
Assert.areEqual(jobCount, runs.size());
Assert.isTrue(asyncApexJobIds.size() >= queueNames.size());

Set<String> overlappingQueueNames = new Set<String>();
for (Integer leftIndex = 0; leftIndex < runs.size(); leftIndex++) {
    for (Integer rightIndex = leftIndex + 1; rightIndex < runs.size(); rightIndex++) {
        if (runQueueNames[leftIndex] == runQueueNames[rightIndex]) {
            continue;
        }
        if (
            runStartedAtMs[leftIndex] < runFinishedAtMs[rightIndex] &&
            runStartedAtMs[rightIndex] < runFinishedAtMs[leftIndex]
        ) {
            overlappingQueueNames.add(runQueueNames[leftIndex]);
            overlappingQueueNames.add(runQueueNames[rightIndex]);
        }
    }
}
Assert.areEqual(
    queueNames,
    overlappingQueueNames,
    'Every worker shard must overlap actual job execution with at least one other shard.'
);

List<QueueRuntime__c> runtimes = [
    SELECT ActiveWorkerCount__c, AsyncApexJobId__c, IsPaused__c, QueueName__c, WorkerToken__c
    FROM QueueRuntime__c
    WHERE QueueName__c IN :queueNames
];
Assert.areEqual(queueNames.size(), runtimes.size());
for (QueueRuntime__c runtime : runtimes) {
    Assert.areEqual(false, runtime.IsPaused__c);
    Assert.areEqual(0, QueuesSupport.toInteger(runtime.ActiveWorkerCount__c, 0));
    Assert.isTrue(String.isBlank(runtime.WorkerToken__c));
    Assert.isTrue(String.isBlank(runtime.AsyncApexJobId__c));
}

Integer queueErrorCount = [
    SELECT COUNT()
    FROM QueueError__c
    WHERE QueueName__c IN :queueNames
];
Assert.areEqual(0, queueErrorCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'performance.worker.parallel.assert',
            'queueNames' => queueNames,
            'jobCount' => jobs.size(),
            'jobRunCount' => runs.size(),
            'workerAsyncApexJobCount' => asyncApexJobIds.size(),
            'overlappingQueueNames' => overlappingQueueNames
        }
    )
);
