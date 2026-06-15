String queueName = 'e2e-job-scheduler-probe';
String schedulerName = 'regular-cron-every-minute';
String processorName = 'job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
Datetime firstScheduledWake = System.now().addSeconds(75);
String cronExpression = firstScheduledWake.second() + ' ' + firstScheduledWake.minute() + ' * * * ?';

Queues.Queue queue = Queues.of(queueName);
queue.pause();

Queues.Job job = queue.upsertJobScheduler(
    schedulerName,
    new Queues.RepeatOptions().pattern(cronExpression).lim(3),
    new Queues.JobTemplate(processorName, processorClass)
        .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'regular-cron' })
        .opts(new Queues.JobOptions().attempts(2).priority(7))
);

JobScheduler__c scheduler = [
    SELECT
        Id,
        Name,
        QueueName__c,
        ExternalId__c,
        JobName__c,
        ProcessorClass__c,
        Pattern__c,
        EveryMinutes__c,
        Limit__c,
        ScheduledJobId__c,
        IterationCount__c,
        NextRunAt__c,
        IsActive__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];

Job__c materializedJob = [
    SELECT
        Id,
        QueueName__c,
        Name__c,
        ProcessorClass__c,
        ExternalId__c,
        QueueScopedExternalId__c,
        JobScheduler__c,
        ExecutionMode__c,
        State__c,
        Priority__c,
        MaxAttempts__c,
        AvailableAt__c
    FROM Job__c
    WHERE Id = :job.getId()
    LIMIT 1
];

List<CronTrigger> activeScheduledJobs = [
    SELECT Id, State, CronExpression, NextFireTime, CronJobDetail.Name
    FROM CronTrigger
    WHERE Id = :scheduler.ScheduledJobId__c AND State NOT IN ('DELETED', 'COMPLETE', 'ERROR')
];

Queues.JobScheduler facade = queue.getJobScheduler(schedulerName);
String expectedExternalId = 'scheduler:' + scheduler.Id + ':1';

Assert.areEqual(queueName, scheduler.QueueName__c);
Assert.areEqual(QueuesSupport.buildQueueScopedExternalId(queueName, schedulerName), scheduler.ExternalId__c);
Assert.areEqual(processorName, scheduler.JobName__c);
Assert.areEqual(processorClass, scheduler.ProcessorClass__c);
Assert.areEqual(cronExpression, scheduler.Pattern__c);
Assert.areEqual(null, scheduler.EveryMinutes__c);
Assert.areEqual(3, QueuesSupport.toInteger(scheduler.Limit__c, 0));
Assert.areEqual(1, QueuesSupport.toInteger(scheduler.IterationCount__c, 0));
Assert.areEqual(true, scheduler.IsActive__c);
Assert.isTrue(String.isNotBlank(scheduler.ScheduledJobId__c));
Assert.isTrue(scheduler.NextRunAt__c > System.now());

Assert.areEqual(1, activeScheduledJobs.size());
Assert.areEqual(cronExpression, activeScheduledJobs[0].CronExpression);
Assert.areEqual(
    true,
    activeScheduledJobs[0]
        .CronJobDetail.Name.startsWith(QueuesJobSchedulerSchedule.resolvePatternJobNamePrefix(scheduler.Id))
);

Assert.areEqual(scheduler.Id, materializedJob.JobScheduler__c);
Assert.areEqual(processorName, materializedJob.Name__c);
Assert.areEqual(processorClass, materializedJob.ProcessorClass__c);
Assert.areEqual(expectedExternalId, materializedJob.ExternalId__c);
Assert.areEqual(
    QueuesSupport.buildQueueScopedExternalId(queueName, expectedExternalId),
    materializedJob.QueueScopedExternalId__c
);
Assert.areEqual(Queues.ExecutionMode.WORKER.name(), materializedJob.ExecutionMode__c);
Assert.areEqual(Queues.State.DELAYED.name(), materializedJob.State__c);
Assert.areEqual(7, QueuesSupport.toInteger(materializedJob.Priority__c, 0));
Assert.areEqual(2, QueuesSupport.toInteger(materializedJob.MaxAttempts__c, 0));
Assert.areEqual(scheduler.NextRunAt__c, materializedJob.AvailableAt__c);

Assert.areEqual(schedulerName, facade.getJobSchedulerId());
Assert.areEqual(cronExpression, facade.getPattern());
Assert.areEqual(true, facade.isActive());

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job-scheduler.regular-cron.schedule',
            'queueName' => queueName,
            'schedulerId' => scheduler.Id,
            'scheduledJobId' => scheduler.ScheduledJobId__c,
            'materializedJobId' => materializedJob.Id,
            'cronExpression' => cronExpression,
            'nextRunAt' => scheduler.NextRunAt__c
        }
    )
);
