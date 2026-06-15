String queueName = 'e2e-job-probe';
String processorName = 'job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';

Queues.flowProducer()
    .add(
        new Queues.FlowJob(processorName, queueName, processorClass)
            .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'policy-default-root' })
            .opts(new Queues.JobOptions().jobId('flow-producer:policy:default:root'))
            .child(
                new Queues.FlowJob(processorName, queueName, processorClass)
                    .data(new Map<String, Object>{ 'mode' => 'unrecoverable', 'payload' => 'policy-default-child' })
                    .opts(new Queues.JobOptions().jobId('flow-producer:policy:default:child').attempts(1))
            )
    );

Queues.flowProducer()
    .add(
        new Queues.FlowJob(processorName, queueName, processorClass)
            .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'policy-fail-root' })
            .opts(new Queues.JobOptions().jobId('flow-producer:policy:fail:root'))
            .child(
                new Queues.FlowJob(processorName, queueName, processorClass)
                    .data(new Map<String, Object>{ 'mode' => 'unrecoverable', 'payload' => 'policy-fail-child' })
                    .opts(
                        new Queues.JobOptions()
                            .jobId('flow-producer:policy:fail:child')
                            .attempts(1)
                            .failParentOnFailure()
                    )
            )
    );

Queues.flowProducer()
    .add(
        new Queues.FlowJob(processorName, queueName, processorClass)
            .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'policy-continue-root' })
            .opts(new Queues.JobOptions().jobId('flow-producer:policy:continue:root'))
            .child(
                new Queues.FlowJob(processorName, queueName, processorClass)
                    .data(new Map<String, Object>{ 'mode' => 'unrecoverable', 'payload' => 'policy-continue-child' })
                    .opts(
                        new Queues.JobOptions()
                            .jobId('flow-producer:policy:continue:child')
                            .attempts(1)
                            .continueParentOnFailure()
                    )
            )
    );

Queues.flowProducer()
    .add(
        new Queues.FlowJob(processorName, queueName, processorClass)
            .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'policy-ignore-root' })
            .opts(new Queues.JobOptions().jobId('flow-producer:policy:ignore:root'))
            .child(
                new Queues.FlowJob(processorName, queueName, processorClass)
                    .data(new Map<String, Object>{ 'mode' => 'unrecoverable', 'payload' => 'policy-ignore-child' })
                    .opts(
                        new Queues.JobOptions()
                            .jobId('flow-producer:policy:ignore:child')
                            .attempts(1)
                            .ignoreDependencyOnFailure()
                    )
            )
    );

Queues.flowProducer()
    .add(
        new Queues.FlowJob(processorName, queueName, processorClass)
            .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'policy-remove-root' })
            .opts(new Queues.JobOptions().jobId('flow-producer:policy:remove:root'))
            .child(
                new Queues.FlowJob(processorName, queueName, processorClass)
                    .data(new Map<String, Object>{ 'mode' => 'unrecoverable', 'payload' => 'policy-remove-child' })
                    .opts(
                        new Queues.JobOptions()
                            .jobId('flow-producer:policy:remove:child')
                            .attempts(1)
                            .removeDependencyOnFailure()
                    )
            )
    );

Queues.JobNode cancelNode = Queues.flowProducer()
    .add(
        new Queues.FlowJob(processorName, queueName, processorClass)
            .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'policy-cancel-root' })
            .opts(new Queues.JobOptions().jobId('flow-producer:policy:cancel:root'))
            .child(
                new Queues.FlowJob(processorName, queueName, processorClass)
                    .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'policy-cancel-child' })
                    .opts(new Queues.JobOptions().jobId('flow-producer:policy:cancel:child').cancelParentOnCanceled())
            )
    );
cancelNode.getChildren()[0].getJob().cancel();

Queues.JobNode failCanceledNode = Queues.flowProducer()
    .add(
        new Queues.FlowJob(processorName, queueName, processorClass)
            .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'policy-fail-canceled-root' })
            .opts(new Queues.JobOptions().jobId('flow-producer:policy:fail-canceled:root'))
            .child(
                new Queues.FlowJob(processorName, queueName, processorClass)
                    .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'policy-fail-canceled-child' })
                    .opts(
                        new Queues.JobOptions().jobId('flow-producer:policy:fail-canceled:child').failParentOnCanceled()
                    )
            )
    );
failCanceledNode.getChildren()[0].getJob().cancel();

Queues.JobNode ignoreCanceledNode = Queues.flowProducer()
    .add(
        new Queues.FlowJob(processorName, queueName, processorClass)
            .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'policy-ignore-canceled-root' })
            .opts(new Queues.JobOptions().jobId('flow-producer:policy:ignore-canceled:root'))
            .child(
                new Queues.FlowJob(processorName, queueName, processorClass)
                    .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'policy-ignore-canceled-child' })
                    .opts(
                        new Queues.JobOptions()
                            .jobId('flow-producer:policy:ignore-canceled:child')
                            .ignoreDependencyOnCanceled()
                    )
            )
    );
ignoreCanceledNode.getChildren()[0].getJob().cancel();

System.debug(
    JSON.serializePretty(new Map<String, Object>{ 'feature' => 'flow-producer.policy.add', 'queueName' => queueName })
);
