String childQueueName = 'e2e-job-probe';
String parentQueueName = 'e2e-worker-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String parentExternalId = 'flow-producer:cross-worker:parent';
String childExternalId = 'flow-producer:cross-worker:child';

Queues.JobNode rootNode = Queues.flowProducer()
    .add(
        new Queues.FlowJob('Cross Queue Parent', parentQueueName, processorClass)
            .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'parent' })
            .opts(new Queues.JobOptions().jobId(parentExternalId))
            .child(
                new Queues.FlowJob('Cross Queue Child', childQueueName, processorClass)
                    .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'child' })
                    .opts(new Queues.JobOptions().jobId(childExternalId))
            )
    );

Assert.areEqual(1, rootNode.getChildren().size());

Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
for (Job__c job : [
    SELECT Id, Parent__c, QueueName__c, ExternalId__c, State__c, ExecutionMode__c
    FROM Job__c
    WHERE ExternalId__c IN :new Set<String>{ parentExternalId, childExternalId }
]) {
    jobsByExternalId.put(job.ExternalId__c, job);
    Assert.areEqual(Queues.ExecutionMode.WORKER.name(), job.ExecutionMode__c);
}

Job__c parent = jobsByExternalId.get(parentExternalId);
Job__c child = jobsByExternalId.get(childExternalId);
Assert.areEqual(2, jobsByExternalId.size());
Assert.areEqual(parentQueueName, parent.QueueName__c);
Assert.areEqual(childQueueName, child.QueueName__c);
Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), parent.State__c);
Assert.areEqual(Queues.State.WAITING.name(), child.State__c);
Assert.areEqual(parent.Id, child.Parent__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'flow-producer.cross-worker.add',
            'parentQueueName' => parentQueueName,
            'childQueueName' => childQueueName,
            'parentJobId' => parent.Id,
            'childJobId' => child.Id
        }
    )
);
