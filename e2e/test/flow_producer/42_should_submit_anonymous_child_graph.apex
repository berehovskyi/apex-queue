String queueName = 'e2e-job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String rootExternalId = 'flow-producer:anonymous-extension:root';
Datetime availableAt = System.now().addMinutes(10);

Queues.JobNode root = Queues.flowProducer()
    .add(
        new Queues.FlowJob('Anonymous Extension Root', queueName, processorClass)
            .opts(new Queues.JobOptions().jobId(rootExternalId).availableAt(availableAt))
            .child(
                new Queues.FlowJob('Anonymous Child', queueName, processorClass)
                    .opts(new Queues.JobOptions().availableAt(availableAt))
            ),
        new Queues.FlowOpts().invocable()
    );

Assert.areEqual(1, root.getChildren().size());

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'flow-producer.concurrent-anonymous-child.submit',
            'rootJobId' => root.getJob().getId(),
            'childJobId' => root.getChildren()[0].getJob().getId()
        }
    )
);
