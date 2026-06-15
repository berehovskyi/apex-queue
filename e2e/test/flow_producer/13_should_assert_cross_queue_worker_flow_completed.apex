String childQueueName = 'e2e-job-probe';
String parentQueueName = 'e2e-worker-probe';
String parentExternalId = 'flow-producer:cross-worker:parent';
String childExternalId = 'flow-producer:cross-worker:child';
Set<String> externalIds = new Set<String>{ parentExternalId, childExternalId };
String completedState = Queues.RunStatus.COMPLETED.name();

Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
for (Job__c job : [
    SELECT Id, Parent__c, QueueName__c, ExternalId__c, State__c, FailedReason__c
    FROM Job__c
    WHERE ExternalId__c IN :externalIds
]) {
    jobsByExternalId.put(job.ExternalId__c, job);
}

Assert.areEqual(2, jobsByExternalId.size());
Job__c parent = jobsByExternalId.get(parentExternalId);
Job__c child = jobsByExternalId.get(childExternalId);
Assert.areEqual(parentQueueName, parent.QueueName__c);
Assert.areEqual(childQueueName, child.QueueName__c);
Assert.areEqual(parent.Id, child.Parent__c);
Assert.areEqual(Queues.State.COMPLETED.name(), child.State__c, 'Cross-queue child failed: ' + child.FailedReason__c);
Assert.areEqual(Queues.State.COMPLETED.name(), parent.State__c, 'Cross-queue parent failed: ' + parent.FailedReason__c);
Assert.areEqual(
    2,
    [
        SELECT COUNT()
        FROM JobRun__c
        WHERE Job__r.ExternalId__c IN :externalIds AND Status__c = :completedState
    ]
);
Assert.areEqual(
    0,
    [
        SELECT COUNT()
        FROM QueueError__c
        WHERE QueueName__c IN :new Set<String>{ childQueueName, parentQueueName }
    ]
);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'flow-producer.cross-worker.assert',
            'parentQueueName' => parentQueueName,
            'childQueueName' => childQueueName,
            'completedJobs' => jobsByExternalId.size()
        }
    )
);
