String queueName = 'e2e-queueable-probe';
String processorName = 'queueable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
Set<String> externalIds = new Set<String>{
    'flow-producer:queueable:root',
    'flow-producer:queueable:child-one',
    'flow-producer:queueable:child-two'
};

Queues.JobNode rootNode = Queues.flowProducer()
    .add(
        new Queues.FlowJob(processorName, queueName, processorClass)
            .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'queueable-root' })
            .opts(new Queues.JobOptions().jobId('flow-producer:queueable:root'))
            .child(
                new Queues.FlowJob(processorName, queueName, processorClass)
                    .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'queueable-child-one' })
                    .opts(new Queues.JobOptions().jobId('flow-producer:queueable:child-one'))
            )
            .child(
                new Queues.FlowJob(processorName, queueName, processorClass)
                    .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'queueable-child-two' })
                    .opts(new Queues.JobOptions().jobId('flow-producer:queueable:child-two'))
            ),
        new Queues.FlowOpts().queueable()
    );

Assert.areEqual(true, rootNode.hasChildren());

List<Job__c> jobs = [
    SELECT Id, Parent__c, ExternalId__c, State__c, ExecutionMode__c, QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
];
Assert.areEqual(externalIds.size(), jobs.size());

Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
Set<Id> dispatchIds = new Set<Id>();
for (Job__c job : jobs) {
    jobsByExternalId.put(job.ExternalId__c, job);
    Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), job.ExecutionMode__c);
    if (job.QueueableDispatch__c != null) {
        dispatchIds.add(job.QueueableDispatch__c);
    }
}

Job__c root = jobsByExternalId.get('flow-producer:queueable:root');
Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), root.State__c);
Assert.areEqual(null, root.Parent__c);
Assert.areEqual(null, root.QueueableDispatch__c);

for (
    String childExternalId : new List<String>{
        'flow-producer:queueable:child-one',
        'flow-producer:queueable:child-two'
    }
) {
    Job__c child = jobsByExternalId.get(childExternalId);
    Assert.areEqual(Queues.State.WAITING.name(), child.State__c);
    Assert.areEqual(root.Id, child.Parent__c);
    Assert.areNotEqual(null, child.QueueableDispatch__c);
}
Assert.areEqual(1, dispatchIds.size());

Integer runtimeCount = [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName];
Assert.areEqual(0, runtimeCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'flow-producer.queueable.add',
            'queueName' => queueName,
            'rootJobId' => root.Id,
            'dispatchIds' => new List<Id>(dispatchIds)
        }
    )
);
