String queueName = 'e2e-queueable-probe';
String processorName = 'queueable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
Set<String> externalIds = new Set<String>{
    'flow-producer:queueable:deep:root',
    'flow-producer:queueable:deep:parent-one',
    'flow-producer:queueable:deep:parent-two',
    'flow-producer:queueable:deep:leaf'
};

Queues.FlowJob leaf = new Queues.FlowJob(processorName, queueName, processorClass)
    .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'queueable-deep-leaf' })
    .opts(new Queues.JobOptions().jobId('flow-producer:queueable:deep:leaf'));
Queues.FlowJob parentTwo = new Queues.FlowJob(processorName, queueName, processorClass)
    .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'queueable-deep-parent-two' })
    .opts(new Queues.JobOptions().jobId('flow-producer:queueable:deep:parent-two'))
    .child(leaf);
Queues.FlowJob parentOne = new Queues.FlowJob(processorName, queueName, processorClass)
    .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'queueable-deep-parent-one' })
    .opts(new Queues.JobOptions().jobId('flow-producer:queueable:deep:parent-one'))
    .child(parentTwo);
Queues.FlowJob root = new Queues.FlowJob(processorName, queueName, processorClass)
    .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'queueable-deep-root' })
    .opts(new Queues.JobOptions().jobId('flow-producer:queueable:deep:root'))
    .child(parentOne);

Queues.JobNode rootNode = Queues.flowProducer().add(root, new Queues.FlowOpts().queueable());

Assert.areEqual(true, rootNode.hasChildren());
List<Job__c> jobs = [
    SELECT ExternalId__c, State__c, QueueableDispatch__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
];
Assert.areEqual(externalIds.size(), jobs.size());
for (Job__c job : jobs) {
    if (job.ExternalId__c == 'flow-producer:queueable:deep:leaf') {
        Assert.areEqual(Queues.State.WAITING.name(), job.State__c);
        Assert.areNotEqual(null, job.QueueableDispatch__c);
        continue;
    }

    Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), job.State__c);
    Assert.areEqual(null, job.QueueableDispatch__c);
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'flow-producer.queueable.deep.add',
            'queueName' => queueName,
            'rootJobId' => rootNode.getJob().getId()
        }
    )
);
