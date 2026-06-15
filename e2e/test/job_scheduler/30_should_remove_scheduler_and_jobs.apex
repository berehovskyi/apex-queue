String queueName = 'e2e-job-scheduler-probe';
String schedulerName = 'remove-pending-scheduler';
String processorName = 'job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';

Queues.Queue queue = Queues.of(queueName);
queue.pause();

Queues.Job job = queue.upsertJobScheduler(
    schedulerName,
    new Queues.RepeatOptions().every(5).startAt(System.now().addMinutes(5)),
    new Queues.JobTemplate(processorName, processorClass)
        .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'remove-pending' })
);

JobScheduler__c scheduler = [
    SELECT Id, ScheduledJobId__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];
String scheduledJobId = scheduler.ScheduledJobId__c;

Assert.isTrue(job.getId() != null);
Assert.isTrue(String.isNotBlank(scheduledJobId));
Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :scheduler.Id]);

Boolean removed = queue.removeJobScheduler(schedulerName);
Boolean alreadyRemoved = queue.removeJobScheduler(schedulerName);

List<CronTrigger> activeScheduledJobs = [
    SELECT Id
    FROM CronTrigger
    WHERE Id = :scheduledJobId AND State NOT IN ('DELETED', 'COMPLETE', 'ERROR')
];

Assert.areEqual(true, removed);
Assert.areEqual(false, alreadyRemoved);
Assert.areEqual(0, [SELECT COUNT() FROM JobScheduler__c WHERE Id = :scheduler.Id]);
Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :scheduler.Id]);
Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE Id = :job.getId()]);
Assert.areEqual(0, activeScheduledJobs.size());

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job-scheduler.remove',
            'queueName' => queueName,
            'schedulerId' => scheduler.Id,
            'scheduledJobId' => scheduledJobId
        }
    )
);
