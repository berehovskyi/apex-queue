String queueName = 'e2e-queueable-probe';
String processorName = 'queueable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String dedupeKey = 'click-1';

List<Queues.JobData> jobs = new List<Queues.JobData>{
    new Queues.JobData(
        processorName,
        processorClass,
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'dedupe-one' },
        new Queues.JobOptions().attempts(1)
    ),
    new Queues.JobData(
        processorName,
        processorClass,
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'dedupe-two' },
        new Queues.JobOptions().attempts(1)
    )
};
Queues.BulkJobOptions bulkOptions = new Queues.BulkJobOptions().dedupeKey(dedupeKey).queueable();

List<Queues.Job> firstClickJobs = Queues.of(queueName).addBulk(jobs, bulkOptions);
List<String> firstClickJobIds = new List<String>();
for (Queues.Job job : firstClickJobs) {
    firstClickJobIds.add(job.getId());
}

Boolean duplicateClickRejected = false;
try {
    Queues.of(queueName).addBulk(jobs, bulkOptions);
    Assert.fail('Expected duplicate queueable dispatcher enqueue to be rejected.');
} catch (System.DuplicateMessageException ex) {
    duplicateClickRejected = true;
}

List<Job__c> storedJobs = [
    SELECT Id, State__c, ExecutionMode__c, QueueableDispatch__c, ExternalId__c
    FROM Job__c
    WHERE QueueName__c = :queueName
    ORDER BY CreatedDate ASC
];
List<QueueableDispatch__c> dispatches = [
    SELECT Id, Mode__c, State__c, RequestedCount__c, DispatchedCount__c, DedupeKey__c, DispatcherAsyncApexJobId__c
    FROM QueueableDispatch__c
    WHERE QueueName__c = :queueName
];

Assert.areEqual(true, duplicateClickRejected);
Assert.areEqual(2, firstClickJobs.size());
Assert.areEqual(2, storedJobs.size(), 'Duplicate addBulk click should have been rolled back.');
Assert.areEqual(1, dispatches.size(), 'Duplicate addBulk click should not create a second dispatch.');

QueueableDispatch__c dispatch = dispatches[0];
Assert.areEqual(dedupeKey, dispatch.DedupeKey__c);
Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), dispatch.Mode__c);
Assert.areEqual(2, QueuesSupport.toInteger(dispatch.RequestedCount__c, 0));
Assert.areEqual(0, QueuesSupport.toInteger(dispatch.DispatchedCount__c, 0));
Assert.isTrue(String.isNotBlank(dispatch.DispatcherAsyncApexJobId__c));

Set<Id> dispatchIds = new Set<Id>();
for (Job__c job : storedJobs) {
    Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), job.ExecutionMode__c);
    Assert.areEqual(null, job.ExternalId__c);
    Assert.areNotEqual(null, job.QueueableDispatch__c);
    dispatchIds.add(job.QueueableDispatch__c);
}
Assert.areEqual(new Set<Id>{ dispatch.Id }, dispatchIds);

Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Assert.areEqual(0, runtimeCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queueable.dedupe-key.double-click',
            'queueName' => queueName,
            'dedupeKey' => dedupeKey,
            'jobIds' => firstClickJobIds,
            'dispatchId' => dispatch.Id,
            'dispatcherAsyncApexJobId' => dispatch.DispatcherAsyncApexJobId__c
        }
    )
);
