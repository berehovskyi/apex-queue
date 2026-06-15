String workerQueueName = 'e2e-performance-worker';
String queueableSerialQueueName = 'e2e-performance-queueable';
String queueableConcurrentQueueName = 'e2e-performance-queueable-concurrent';
String invocableQueueName = 'e2e-performance-invocable';
Set<String> queueNames = new Set<String>{
    workerQueueName,
    queueableSerialQueueName,
    queueableConcurrentQueueName,
    invocableQueueName
};
Integer workerJobCount = 500;
Integer queueableSerialJobCount = 500;
Integer queueableConcurrentJobCount = 500;
Integer invocableJobCount = 500;

List<Job__c> workerJobs = [
    SELECT
        Id,
        ExternalId__c,
        State__c,
        FailedReason__c,
        ReturnValue__c,
        Progress__c,
        AsyncApexJobId__c,
        ExecutionMode__c,
        QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c = :workerQueueName
    ORDER BY ExternalId__c ASC
];
Assert.areEqual(workerJobCount, workerJobs.size());

List<String> unfinishedWorkerJobs = new List<String>();
for (Job__c job : workerJobs) {
    Assert.areEqual(Queues.ExecutionMode.WORKER.name(), job.ExecutionMode__c);
    Assert.areEqual(null, job.QueueableDispatch__c);
    if (QueuesSupport.toState(job.State__c) != Queues.State.COMPLETED) {
        unfinishedWorkerJobs.add(job.ExternalId__c + '=' + job.State__c + ' (' + job.FailedReason__c + ')');
        continue;
    }

    Assert.isTrue(String.isBlank(job.AsyncApexJobId__c));
    Map<String, Object> returnValue = (Map<String, Object>) JSON.deserializeUntyped(job.ReturnValue__c);
    Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(job.Progress__c);
    Assert.areEqual(workerQueueName, String.valueOf(returnValue.get('queueName')));
    Assert.isTrue(String.valueOf(returnValue.get('payload')).startsWith('worker-load-'));
    Assert.isTrue(progress.containsKey('startedAtMs'));
    Assert.isTrue(String.isNotBlank((String) progress.get('startedAtMs')));
}
Assert.areEqual(
    0,
    unfinishedWorkerJobs.size(),
    'Worker performance jobs are not finished yet or failed: ' + String.join(unfinishedWorkerJobs, ', ')
);

List<JobRun__c> workerRuns = [
    SELECT Status__c, AsyncApexJobId__c
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :workerQueueName
];
Assert.areEqual(workerJobCount, workerRuns.size());
for (JobRun__c run : workerRuns) {
    Assert.areEqual(Queues.RunStatus.COMPLETED.name(), QueuesSupport.normalizeEnumValue(run.Status__c));
    Assert.isTrue(String.isNotBlank(run.AsyncApexJobId__c));
}

List<QueueRuntime__c> workerRuntimes = [
    SELECT ActiveWorkerCount__c, WorkerToken__c, AsyncApexJobId__c, IsPaused__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :workerQueueName
];
Assert.areEqual(1, workerRuntimes.size());
Assert.areEqual(false, workerRuntimes[0].IsPaused__c);
Assert.areEqual(0, QueuesSupport.toInteger(workerRuntimes[0].ActiveWorkerCount__c, 0));
Assert.isTrue(String.isBlank(workerRuntimes[0].WorkerToken__c));
Assert.isTrue(String.isBlank(workerRuntimes[0].AsyncApexJobId__c));

List<Job__c> queueableSerialJobs = [
    SELECT
        Id,
        ExternalId__c,
        State__c,
        FailedReason__c,
        ReturnValue__c,
        Progress__c,
        AsyncApexJobId__c,
        ExecutionMode__c,
        QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c = :queueableSerialQueueName
    ORDER BY ExternalId__c ASC
];
Assert.areEqual(queueableSerialJobCount, queueableSerialJobs.size());

List<String> unfinishedQueueableSerialJobs = new List<String>();
for (Job__c job : queueableSerialJobs) {
    Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), job.ExecutionMode__c);
    Assert.areNotEqual(null, job.QueueableDispatch__c);
    if (QueuesSupport.toState(job.State__c) != Queues.State.COMPLETED) {
        unfinishedQueueableSerialJobs.add(job.ExternalId__c + '=' + job.State__c + ' (' + job.FailedReason__c + ')');
        continue;
    }

    Assert.isTrue(String.isBlank(job.AsyncApexJobId__c));
    Map<String, Object> returnValue = (Map<String, Object>) JSON.deserializeUntyped(job.ReturnValue__c);
    Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(job.Progress__c);
    Assert.areEqual(queueableSerialQueueName, String.valueOf(returnValue.get('queueName')));
    Assert.isTrue(String.valueOf(returnValue.get('payload')).startsWith('queueable-load-'));
    Assert.isTrue(progress.containsKey('startedAtMs'));
    Assert.isTrue(String.isNotBlank((String) progress.get('startedAtMs')));
}
Assert.areEqual(
    0,
    unfinishedQueueableSerialJobs.size(),
    'Queueable serial performance jobs are not finished yet or failed: ' +
    String.join(unfinishedQueueableSerialJobs, ', ')
);

List<JobRun__c> queueableSerialRuns = [
    SELECT Status__c, AsyncApexJobId__c
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :queueableSerialQueueName
];
Assert.areEqual(queueableSerialJobCount, queueableSerialRuns.size());
for (JobRun__c run : queueableSerialRuns) {
    Assert.areEqual(Queues.RunStatus.COMPLETED.name(), QueuesSupport.normalizeEnumValue(run.Status__c));
    Assert.isTrue(String.isNotBlank(run.AsyncApexJobId__c));
}

List<QueueableDispatch__c> queueableSerialDispatches = [
    SELECT State__c, RequestedCount__c, DispatchedCount__c, FinishedAt__c, FailedReason__c
    FROM QueueableDispatch__c
    WHERE QueueName__c = :queueableSerialQueueName
];
Assert.areEqual(1, queueableSerialDispatches.size());
Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), queueableSerialDispatches[0].State__c);
Assert.areEqual(queueableSerialJobCount, QueuesSupport.toInteger(queueableSerialDispatches[0].RequestedCount__c, 0));
Assert.areEqual(queueableSerialJobCount, QueuesSupport.toInteger(queueableSerialDispatches[0].DispatchedCount__c, 0));
Assert.areNotEqual(null, queueableSerialDispatches[0].FinishedAt__c);
Assert.areEqual(null, queueableSerialDispatches[0].FailedReason__c);

List<Job__c> queueableConcurrentJobs = [
    SELECT
        Id,
        ExternalId__c,
        State__c,
        FailedReason__c,
        ReturnValue__c,
        Progress__c,
        AsyncApexJobId__c,
        ExecutionMode__c,
        QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c = :queueableConcurrentQueueName
    ORDER BY ExternalId__c ASC
];
Assert.areEqual(queueableConcurrentJobCount, queueableConcurrentJobs.size());

List<String> unfinishedQueueableConcurrentJobs = new List<String>();
for (Job__c job : queueableConcurrentJobs) {
    Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), job.ExecutionMode__c);
    Assert.areNotEqual(null, job.QueueableDispatch__c);
    if (QueuesSupport.toState(job.State__c) != Queues.State.COMPLETED) {
        unfinishedQueueableConcurrentJobs.add(
            job.ExternalId__c + '=' + job.State__c + ' (' + job.FailedReason__c + ')'
        );
        continue;
    }

    Assert.isTrue(String.isBlank(job.AsyncApexJobId__c));
    Map<String, Object> returnValue = (Map<String, Object>) JSON.deserializeUntyped(job.ReturnValue__c);
    Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(job.Progress__c);
    Assert.areEqual(queueableConcurrentQueueName, String.valueOf(returnValue.get('queueName')));
    Assert.isTrue(String.valueOf(returnValue.get('payload')).startsWith('queueable-concurrent-load-'));
    Assert.isTrue(progress.containsKey('startedAtMs'));
    Assert.isTrue(String.isNotBlank((String) progress.get('startedAtMs')));
}
Assert.areEqual(
    0,
    unfinishedQueueableConcurrentJobs.size(),
    'Queueable concurrent performance jobs are not finished yet or failed: ' +
    String.join(unfinishedQueueableConcurrentJobs, ', ')
);

List<JobRun__c> queueableConcurrentRuns = [
    SELECT Status__c, AsyncApexJobId__c
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :queueableConcurrentQueueName
];
Assert.areEqual(queueableConcurrentJobCount, queueableConcurrentRuns.size());
for (JobRun__c run : queueableConcurrentRuns) {
    Assert.areEqual(Queues.RunStatus.COMPLETED.name(), QueuesSupport.normalizeEnumValue(run.Status__c));
    Assert.isTrue(String.isNotBlank(run.AsyncApexJobId__c));
}

List<QueueableDispatch__c> queueableConcurrentDispatches = [
    SELECT State__c, RequestedCount__c, DispatchedCount__c, FinishedAt__c, FailedReason__c
    FROM QueueableDispatch__c
    WHERE QueueName__c = :queueableConcurrentQueueName
];
Assert.areEqual(1, queueableConcurrentDispatches.size());
Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), queueableConcurrentDispatches[0].State__c);
Assert.areEqual(
    queueableConcurrentJobCount,
    QueuesSupport.toInteger(queueableConcurrentDispatches[0].RequestedCount__c, 0)
);
Assert.areEqual(
    queueableConcurrentJobCount,
    QueuesSupport.toInteger(queueableConcurrentDispatches[0].DispatchedCount__c, 0)
);
Assert.areNotEqual(null, queueableConcurrentDispatches[0].FinishedAt__c);
Assert.areEqual(null, queueableConcurrentDispatches[0].FailedReason__c);

List<Job__c> invocableJobs = [
    SELECT
        Id,
        ExternalId__c,
        State__c,
        FailedReason__c,
        ReturnValue__c,
        Progress__c,
        AsyncApexJobId__c,
        ExecutionMode__c,
        QueueableDispatch__c,
        LeaseToken__c,
        LeaseExpiresAt__c
    FROM Job__c
    WHERE QueueName__c = :invocableQueueName
    ORDER BY ExternalId__c ASC
];
Assert.areEqual(invocableJobCount, invocableJobs.size());

List<String> unfinishedInvocableJobs = new List<String>();
Set<String> invocableTransactionMarkers = new Set<String>();
for (Job__c job : invocableJobs) {
    Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), job.ExecutionMode__c);
    Assert.areEqual(null, job.QueueableDispatch__c);
    if (QueuesSupport.toState(job.State__c) != Queues.State.COMPLETED) {
        unfinishedInvocableJobs.add(job.ExternalId__c + '=' + job.State__c + ' (' + job.FailedReason__c + ')');
        continue;
    }

    Assert.isTrue(String.isBlank(job.AsyncApexJobId__c));
    Assert.areEqual(null, job.LeaseToken__c);
    Assert.areEqual(null, job.LeaseExpiresAt__c);
    Map<String, Object> returnValue = (Map<String, Object>) JSON.deserializeUntyped(job.ReturnValue__c);
    Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(job.Progress__c);
    Assert.areEqual(invocableQueueName, String.valueOf(returnValue.get('queueName')));
    Assert.isTrue(String.valueOf(returnValue.get('payload')).startsWith('invocable-load-'));
    Assert.isTrue(progress.containsKey('startedAtMs'));
    Assert.isTrue(String.isNotBlank((String) progress.get('startedAtMs')));
    String returnTransactionMarker = String.valueOf(returnValue.get('transactionMarker'));
    String progressTransactionMarker = String.valueOf(progress.get('transactionMarker'));
    Assert.isTrue(String.isNotBlank(returnTransactionMarker));
    Assert.areEqual(returnTransactionMarker, progressTransactionMarker);
    invocableTransactionMarkers.add(returnTransactionMarker);
}
Assert.areEqual(
    0,
    unfinishedInvocableJobs.size(),
    'Invocable performance jobs are not finished yet or failed: ' + String.join(unfinishedInvocableJobs, ', ')
);
Assert.areEqual(
    invocableJobCount,
    invocableTransactionMarkers.size(),
    'Each INVOCABLE performance job must execute in its own transaction.'
);

List<JobRun__c> invocableRuns = [
    SELECT Status__c, AsyncApexJobId__c
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :invocableQueueName
];
Assert.areEqual(invocableJobCount, invocableRuns.size());
for (JobRun__c run : invocableRuns) {
    Assert.areEqual(Queues.RunStatus.COMPLETED.name(), QueuesSupport.normalizeEnumValue(run.Status__c));
    Assert.areEqual(null, run.AsyncApexJobId__c);
}

Set<String> queueableQueueNames = new Set<String>{ queueableSerialQueueName, queueableConcurrentQueueName };
Integer queueableRuntimeCount = [
    SELECT COUNT()
    FROM QueueRuntime__c
    WHERE QueueName__c IN :queueableQueueNames
];
Integer invocableRuntimeCount = [
    SELECT COUNT()
    FROM QueueRuntime__c
    WHERE QueueName__c = :invocableQueueName
];
Integer invocableDispatchCount = [
    SELECT COUNT()
    FROM QueueableDispatch__c
    WHERE QueueName__c = :invocableQueueName
];
Integer queueErrorCount = [
    SELECT COUNT()
    FROM QueueError__c
    WHERE QueueName__c IN :queueNames
];
Assert.areEqual(0, queueableRuntimeCount);
Assert.areEqual(0, invocableRuntimeCount);
Assert.areEqual(0, invocableDispatchCount);
Assert.areEqual(0, queueErrorCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'performance.assert',
            'workerQueueName' => workerQueueName,
            'workerJobCount' => workerJobs.size(),
            'queueableSerialQueueName' => queueableSerialQueueName,
            'queueableSerialJobCount' => queueableSerialJobs.size(),
            'queueableConcurrentQueueName' => queueableConcurrentQueueName,
            'queueableConcurrentJobCount' => queueableConcurrentJobs.size(),
            'invocableQueueName' => invocableQueueName,
            'invocableJobCount' => invocableJobs.size()
        }
    )
);
