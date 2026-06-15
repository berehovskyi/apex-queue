String queueName = 'e2e-job-scheduler-probe';
String schedulerName = 'cancel-pending-scheduler';
String processorName = 'job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';

Queues.Queue queue = Queues.of(queueName);
queue.pause();

Queues.Job job = queue.upsertJobScheduler(
    schedulerName,
    new Queues.RepeatOptions().every(5).startAt(System.now().addMinutes(5)),
    new Queues.JobTemplate(processorName, processorClass)
        .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'cancel-pending' })
);

JobScheduler__c scheduler = [
    SELECT Id, ScheduledJobId__c, NextRunAt__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];
String scheduledJobId = scheduler.ScheduledJobId__c;

Assert.isTrue(job.getId() != null);
Assert.isTrue(String.isNotBlank(scheduledJobId));
Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :scheduler.Id]);

Boolean canceled = queue.cancelJobScheduler(schedulerName);
Boolean alreadyCanceled = queue.cancelJobScheduler(schedulerName);

List<CronTrigger> activeScheduledJobs = [
    SELECT Id
    FROM CronTrigger
    WHERE Id = :scheduledJobId AND State NOT IN ('DELETED', 'COMPLETE', 'ERROR')
];

Assert.areEqual(true, canceled);
Assert.areEqual(false, alreadyCanceled);
JobScheduler__c canceledScheduler = [
    SELECT Id, IsActive__c, ScheduledJobId__c, NextRunAt__c
    FROM JobScheduler__c
    WHERE Id = :scheduler.Id
];
Job__c canceledJob = [
    SELECT Id, State__c, FailedReason__c, JobScheduler__c
    FROM Job__c
    WHERE Id = :job.getId()
];
Assert.areEqual(false, canceledScheduler.IsActive__c);
Assert.areEqual(null, canceledScheduler.ScheduledJobId__c);
Assert.areEqual(null, canceledScheduler.NextRunAt__c);
Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :scheduler.Id]);
Assert.areEqual(Queues.State.CANCELED.name(), canceledJob.State__c);
Assert.areEqual('Canceled by job scheduler cancel.', canceledJob.FailedReason__c);
Assert.areEqual(scheduler.Id, canceledJob.JobScheduler__c);
Assert.areEqual(0, activeScheduledJobs.size());

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job-scheduler.cancel',
            'queueName' => queueName,
            'schedulerId' => scheduler.Id,
            'scheduledJobId' => scheduledJobId
        }
    )
);
