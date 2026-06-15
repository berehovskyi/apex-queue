String queueName = 'e2e-job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String rootExternalId = 'flow-producer:replay:root';
String childExternalId = 'flow-producer:replay:child';
Datetime availableAt = System.now().addMinutes(10);

Queues.JobNode root = Queues.flowProducer()
    .add(
        new Queues.FlowJob('Replay Root', queueName, processorClass)
            .opts(new Queues.JobOptions().jobId(rootExternalId).availableAt(availableAt))
            .child(
                new Queues.FlowJob('Replay Child', queueName, processorClass)
                    .opts(new Queues.JobOptions().jobId(childExternalId).availableAt(availableAt))
            ),
        new Queues.FlowOpts().invocable()
    );

Assert.areEqual(1, root.getChildren().size());

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'flow-producer.concurrent-replay.submit',
            'rootJobId' => root.getJob().getId(),
            'childJobId' => root.getChildren()[0].getJob().getId()
        }
    )
);
