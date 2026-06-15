String queueName = 'e2e-job-scheduler-probe';
String schedulerName = 'replace-on-reupsert';
String processorName = 'job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
Datetime firstRunAt = System.now().addMinutes(5);
Datetime secondRunAt = System.now().addMinutes(10);

Queues.Queue queue = Queues.of(queueName);
queue.pause();

Queues.Job firstJob = queue.upsertJobScheduler(
    schedulerName,
    new Queues.RepeatOptions().every(5).startAt(firstRunAt),
    new Queues.JobTemplate(processorName, processorClass)
        .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'old-template' })
        .opts(new Queues.JobOptions().priority(1))
);

JobScheduler__c firstScheduler = [
    SELECT Id, ScheduledJobId__c, EveryMinutes__c, IterationCount__c, NextRunAt__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];
Job__c firstStoredJob = [
    SELECT Id, ExternalId__c, JobScheduler__c, Priority__c, Data__c
    FROM Job__c
    WHERE Id = :firstJob.getId()
    LIMIT 1
];
List<CronTrigger> activeFirstSchedules = [
    SELECT Id
    FROM CronTrigger
    WHERE Id = :firstScheduler.ScheduledJobId__c AND State NOT IN ('DELETED', 'COMPLETE', 'ERROR')
];

Assert.areEqual(1, activeFirstSchedules.size());
Assert.areEqual(5, QueuesSupport.toInteger(firstScheduler.EveryMinutes__c, 0));
Assert.areEqual(1, QueuesSupport.toInteger(firstScheduler.IterationCount__c, 0));
Assert.areEqual(firstScheduler.Id, firstStoredJob.JobScheduler__c);
Assert.areEqual(1, QueuesSupport.toInteger(firstStoredJob.Priority__c, 0));

String firstScheduledJobId = firstScheduler.ScheduledJobId__c;
String firstExternalId = firstStoredJob.ExternalId__c;
String secondExternalId = 'scheduler:' + firstScheduler.Id + ':2';

Queues.Job secondJob = queue.upsertJobScheduler(
    schedulerName,
    new Queues.RepeatOptions().every(10).startAt(secondRunAt).lim(5),
    new Queues.JobTemplate(processorName, processorClass)
        .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'new-template' })
        .opts(new Queues.JobOptions().attempts(3).priority(8))
);

JobScheduler__c updatedScheduler = [
    SELECT
        Id,
        ScheduledJobId__c,
        EveryMinutes__c,
        Limit__c,
        IterationCount__c,
        NextRunAt__c,
        Data__c,
        Options__c,
        IsActive__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];
Job__c secondStoredJob = [
    SELECT
        Id,
        ExternalId__c,
        QueueScopedExternalId__c,
        JobScheduler__c,
        Priority__c,
        MaxAttempts__c,
        AvailableAt__c,
        Data__c,
        ExecutionMode__c,
        State__c
    FROM Job__c
    WHERE Id = :secondJob.getId()
    LIMIT 1
];

List<CronTrigger> activeFirstSchedulesAfterReupsert = [
    SELECT Id
    FROM CronTrigger
    WHERE Id = :firstScheduledJobId AND State NOT IN ('DELETED', 'COMPLETE', 'ERROR')
];
List<CronTrigger> activeSecondSchedules = [
    SELECT Id
    FROM CronTrigger
    WHERE Id = :updatedScheduler.ScheduledJobId__c AND State NOT IN ('DELETED', 'COMPLETE', 'ERROR')
];

Map<String, Object> schedulerData = (Map<String, Object>) QueuesSupport.deserialize(updatedScheduler.Data__c);
Map<String, Object> schedulerOptions = (Map<String, Object>) QueuesSupport.deserialize(updatedScheduler.Options__c);
Map<String, Object> jobData = (Map<String, Object>) QueuesSupport.deserialize(secondStoredJob.Data__c);

Assert.areEqual(firstScheduler.Id, updatedScheduler.Id);
Assert.areNotEqual(firstScheduledJobId, updatedScheduler.ScheduledJobId__c);
Assert.areEqual(0, activeFirstSchedulesAfterReupsert.size());
Assert.areEqual(1, activeSecondSchedules.size());
Assert.areEqual(10, QueuesSupport.toInteger(updatedScheduler.EveryMinutes__c, 0));
Assert.areEqual(5, QueuesSupport.toInteger(updatedScheduler.Limit__c, 0));
Assert.areEqual(2, QueuesSupport.toInteger(updatedScheduler.IterationCount__c, 0));
Assert.areEqual(true, updatedScheduler.IsActive__c);
Assert.areEqual('new-template', schedulerData.get('payload'));
Assert.areEqual(8, QueuesSupport.toInteger(schedulerOptions.get('priority'), 0));
Assert.areEqual(3, QueuesSupport.toInteger(schedulerOptions.get('attempts'), 0));

Job__c canceledFirstJob = [
    SELECT Id, State__c, FailedReason__c, JobScheduler__c
    FROM Job__c
    WHERE Id = :firstStoredJob.Id
];
Assert.areEqual(Queues.State.CANCELED.name(), canceledFirstJob.State__c);
Assert.areEqual('Canceled by job scheduler replacement.', canceledFirstJob.FailedReason__c);
Assert.areEqual(firstScheduler.Id, canceledFirstJob.JobScheduler__c);
Assert.areNotEqual(firstStoredJob.Id, secondStoredJob.Id);
Assert.areNotEqual(firstExternalId, secondStoredJob.ExternalId__c);
Assert.areEqual(secondExternalId, secondStoredJob.ExternalId__c);
Assert.areEqual(firstScheduler.Id, secondStoredJob.JobScheduler__c);
Assert.areEqual(
    QueuesSupport.buildQueueScopedExternalId(queueName, secondExternalId),
    secondStoredJob.QueueScopedExternalId__c
);
Assert.areEqual(8, QueuesSupport.toInteger(secondStoredJob.Priority__c, 0));
Assert.areEqual(3, QueuesSupport.toInteger(secondStoredJob.MaxAttempts__c, 0));
Assert.areEqual(updatedScheduler.NextRunAt__c, secondStoredJob.AvailableAt__c);
Assert.areEqual('new-template', jobData.get('payload'));
Assert.areEqual(Queues.ExecutionMode.WORKER.name(), secondStoredJob.ExecutionMode__c);
Assert.areEqual(Queues.State.DELAYED.name(), secondStoredJob.State__c);
Assert.areEqual(2, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :firstScheduler.Id]);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job-scheduler.reupsert-replace',
            'queueName' => queueName,
            'schedulerId' => updatedScheduler.Id,
            'oldScheduledJobId' => firstScheduledJobId,
            'newScheduledJobId' => updatedScheduler.ScheduledJobId__c,
            'oldJobId' => firstStoredJob.Id,
            'newJobId' => secondStoredJob.Id
        }
    )
);
