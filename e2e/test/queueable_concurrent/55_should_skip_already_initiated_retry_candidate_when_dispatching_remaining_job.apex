String queueName = 'e2e-queueable-concurrent-probe';
String processorName = 'queueable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String retryExternalId = 'queueable-dispatch-retry-candidate';
String originalExternalId = 'queueable-dispatch-original';
Set<String> externalIds = new Set<String>{ retryExternalId, originalExternalId };
Datetime now = System.now();

QueueableDispatch__c dispatch = new QueueableDispatch__c(
    QueueName__c = queueName,
    Mode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(),
    State__c = Queues.QueueableDispatchState.ACTIVE.name(),
    RequestedCount__c = 2,
    DispatchedCount__c = 1,
    DispatchDepth__c = 0,
    StartedAt__c = now.addMinutes(-1)
);
insert dispatch;

Job__c retryCandidate = new Job__c(
    QueueName__c = queueName,
    Name__c = processorName,
    State__c = Queues.State.WAITING.name(),
    ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(),
    ExternalId__c = retryExternalId,
    QueueScopedExternalId__c = QueuesSupport.buildQueueScopedExternalId(queueName, retryExternalId),
    Data__c = QueuesSupport.serialize(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'retry-candidate' }),
    ProcessorClass__c = processorClass,
    QueueableDispatch__c = dispatch.Id,
    QueueableDispatchInitiatedAt__c = now.addMinutes(-1),
    AvailableAt__c = now,
    Priority__c = 0,
    AttemptsMade__c = 1,
    MaxAttempts__c = 3,
    BackoffType__c = Queues.Backoff.NONE.name(),
    BackoffValue__c = 0,
    PauseRequested__c = false
);

Job__c originalJob = new Job__c(
    QueueName__c = queueName,
    Name__c = processorName,
    State__c = Queues.State.WAITING.name(),
    ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(),
    ExternalId__c = originalExternalId,
    QueueScopedExternalId__c = QueuesSupport.buildQueueScopedExternalId(queueName, originalExternalId),
    Data__c = QueuesSupport.serialize(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'original' }),
    ProcessorClass__c = processorClass,
    QueueableDispatch__c = dispatch.Id,
    AvailableAt__c = now,
    Priority__c = 1,
    AttemptsMade__c = 0,
    MaxAttempts__c = 1,
    BackoffType__c = Queues.Backoff.NONE.name(),
    BackoffValue__c = 0,
    PauseRequested__c = false
);
insert new List<Job__c>{ retryCandidate, originalJob };

QueuesQueueableDispatchService dispatchService = new QueuesQueueableDispatchService();
QueueableDispatch__c result = dispatchService.startDispatchAsSystem(dispatch.Id, '707000000000001AAA');

Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
for (Job__c job : [
    SELECT Id, ExternalId__c, State__c, AsyncApexJobId__c, QueueableDispatchInitiatedAt__c, AttemptsMade__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
]) {
    jobsByExternalId.put(job.ExternalId__c, job);
}
retryCandidate = jobsByExternalId.get(retryExternalId);
originalJob = jobsByExternalId.get(originalExternalId);

Assert.areEqual(Queues.QueueableDispatchState.COMPLETED.name(), result.State__c);
Assert.areEqual(2, QueuesSupport.toInteger(result.DispatchedCount__c, 0));
Assert.areNotEqual(null, result.FinishedAt__c);

Assert.areNotEqual(null, retryCandidate);
Assert.areEqual(Queues.State.WAITING.name(), retryCandidate.State__c);
Assert.areEqual(null, retryCandidate.AsyncApexJobId__c);
Assert.areNotEqual(null, retryCandidate.QueueableDispatchInitiatedAt__c);
Assert.areEqual(1, QueuesSupport.toInteger(retryCandidate.AttemptsMade__c, 0));

Assert.areNotEqual(null, originalJob);
Assert.areEqual(Queues.State.WAITING.name(), originalJob.State__c);
Assert.areEqual(null, originalJob.AsyncApexJobId__c);
Assert.areNotEqual(null, originalJob.QueueableDispatchInitiatedAt__c);
Assert.areEqual(0, QueuesSupport.toInteger(originalJob.AttemptsMade__c, 0));

Set<Id> jobIds = new Set<Id>{ retryCandidate.Id, originalJob.Id };
Integer runCount = [SELECT COUNT() FROM JobRun__c WHERE Job__c IN :jobIds];
Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Assert.areEqual(0, runCount);
Assert.areEqual(0, runtimeCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queueable_concurrent.dispatch-skips-initiated-retry',
            'queueName' => queueName,
            'dispatchId' => dispatch.Id,
            'retryCandidateId' => retryCandidate.Id,
            'originalJobId' => originalJob.Id,
            'originalAsyncApexJobId' => originalJob.AsyncApexJobId__c
        }
    )
);
