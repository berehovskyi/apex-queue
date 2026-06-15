String queueName = 'e2e-job-scheduler-probe';
String schedulerName = 'granular-limit-five';
String processorName = 'job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
Datetime firstRunAt = System.now().addMinutes(5);

Queues.Queue queue = Queues.of(queueName);
queue.pause();

Queues.Job job = queue.upsertJobScheduler(
    schedulerName,
    new Queues.RepeatOptions().every(1).startAt(firstRunAt).lim(5),
    new Queues.JobTemplate(processorName, processorClass)
        .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'limit-five' })
);

JobScheduler__c scheduler = [
    SELECT Id, EveryMinutes__c, Limit__c, IterationCount__c, ScheduledJobId__c, NextRunAt__c, IsActive__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];

Job__c materializedJob = [
    SELECT Id, ExternalId__c, JobScheduler__c, AvailableAt__c, ExecutionMode__c, State__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];

String expectedExternalId = 'scheduler:' + scheduler.Id + ':1';

Assert.areEqual(1, QueuesSupport.toInteger(scheduler.EveryMinutes__c, 0));
Assert.areEqual(5, QueuesSupport.toInteger(scheduler.Limit__c, 0));
Assert.areEqual(1, QueuesSupport.toInteger(scheduler.IterationCount__c, 0));
Assert.areEqual(true, scheduler.IsActive__c);
Assert.isTrue(String.isNotBlank(scheduler.ScheduledJobId__c));
Assert.areEqual(expectedExternalId, materializedJob.ExternalId__c);
Assert.areEqual(scheduler.Id, materializedJob.JobScheduler__c);
Assert.areEqual(scheduler.NextRunAt__c, materializedJob.AvailableAt__c);
Assert.areEqual(Queues.ExecutionMode.WORKER.name(), materializedJob.ExecutionMode__c);
Assert.areEqual(Queues.State.DELAYED.name(), materializedJob.State__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job-scheduler.granular-limit-five.schedule',
            'queueName' => queueName,
            'schedulerId' => scheduler.Id,
            'scheduledJobId' => scheduler.ScheduledJobId__c,
            'jobId' => materializedJob.Id
        }
    )
);
