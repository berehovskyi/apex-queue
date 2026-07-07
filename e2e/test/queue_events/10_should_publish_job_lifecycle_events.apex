String queueName = 'e2e-queue-events';
String processorName = 'queue-events-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String correlationId = 'queue-events:happy-path';
String completedExternalId = 'queue-events:completed';
String failedExternalId = 'queue-events:failed';
String canceledExternalId = 'queue-events:canceled';

Queues.Queue queue = Queues.of(queueName);
List<Queues.Job> submittedJobs = queue.addBulk(
    new List<Queues.JobData>{
        new Queues.JobData(
            processorName,
            processorClass,
            new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'event-completed' },
            new Queues.JobOptions().jobId(completedExternalId).attempts(1).correlationId(correlationId)
        ),
        new Queues.JobData(
            processorName,
            processorClass,
            new Map<String, Object>{ 'mode' => 'unrecoverable', 'message' => 'Expected event failure.' },
            new Queues.JobOptions().jobId(failedExternalId).attempts(1).correlationId(correlationId)
        )
    },
    new Queues.BulkJobOptions().queueable()
);

Queues.Job canceledJob = queue.add(
    processorName,
    processorClass,
    new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'event-canceled' },
    new Queues.JobOptions().jobId(canceledExternalId).delay(10).correlationId(correlationId),
    new Queues.BulkJobOptions().queueable()
);
canceledJob.cancel();

Assert.areEqual(2, submittedJobs.size());
Assert.areEqual(Queues.State.CANCELED, Queues.job(canceledJob.getId()).getState());

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queue-events.publish',
            'queueName' => queueName,
            'correlationId' => correlationId,
            'completedJobId' => submittedJobs[0].getId(),
            'failedJobId' => submittedJobs[1].getId(),
            'canceledJobId' => canceledJob.getId()
        }
    )
);
