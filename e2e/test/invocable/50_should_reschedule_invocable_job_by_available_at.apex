String queueName = 'e2e-invocable-probe';
String processorName = 'invocable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String externalId = 'invocable:reschedule:available-at';
Datetime originalAvailableAt = System.now().addMinutes(10);

Queues.Job job = Queues.of(queueName)
    .add(
        processorName,
        processorClass,
        new Map<String, Object>{ 'mode' => 'complete', 'payload' => externalId },
        new Queues.JobOptions().jobId(externalId).availableAt(originalAvailableAt).attempts(2),
        new Queues.BulkJobOptions().invocable()
    );

Job__c delayedJob = [
    SELECT Id, ExternalId__c, ExecutionMode__c, State__c, AvailableAt__c, AttemptsMade__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];
Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), delayedJob.ExecutionMode__c);
Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(delayedJob.State__c));
Assert.areEqual(0, QueuesSupport.toInteger(delayedJob.AttemptsMade__c, 0));
Assert.isTrue(delayedJob.AvailableAt__c > System.now().addMinutes(5));

delayedJob.AvailableAt__c = System.now();
update delayedJob;

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'invocable.reschedule.available-at',
            'queueName' => queueName,
            'externalId' => externalId,
            'jobId' => delayedJob.Id,
            'originalAvailableAt' => originalAvailableAt,
            'rescheduledAvailableAt' => delayedJob.AvailableAt__c
        }
    )
);
