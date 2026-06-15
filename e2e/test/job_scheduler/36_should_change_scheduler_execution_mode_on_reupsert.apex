String queueName = 'e2e-job-scheduler-probe';
String schedulerName = 'change-mode-on-reupsert';
String processorName = 'job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
Datetime firstRunAt = System.now().addMinutes(5);
Datetime secondRunAt = System.now().addMinutes(10);

Queues.Queue queue = Queues.of(queueName);

Queues.Job firstJob = queue.upsertJobScheduler(
    schedulerName,
    new Queues.RepeatOptions().every(5).startAt(firstRunAt),
    new Queues.JobTemplate(processorName, processorClass)
        .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'queueable-template' }),
    Queues.ExecutionMode.QUEUEABLE_CONCURRENT
);

JobScheduler__c firstScheduler = [
    SELECT Id, ScheduledJobId__c, EveryMinutes__c, IterationCount__c, ExecutionMode__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];
Job__c firstStoredJob = [
    SELECT Id, ExternalId__c, JobScheduler__c, ExecutionMode__c, QueueableDispatch__c
    FROM Job__c
    WHERE Id = :firstJob.getId()
    LIMIT 1
];

Assert.areEqual(5, QueuesSupport.toInteger(firstScheduler.EveryMinutes__c, 0));
Assert.areEqual(1, QueuesSupport.toInteger(firstScheduler.IterationCount__c, 0));
Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), firstScheduler.ExecutionMode__c);
Assert.areEqual(firstScheduler.Id, firstStoredJob.JobScheduler__c);
Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), firstStoredJob.ExecutionMode__c);
Assert.areEqual(null, firstStoredJob.QueueableDispatch__c);

String firstScheduledJobId = firstScheduler.ScheduledJobId__c;
String secondExternalId = 'scheduler:' + firstScheduler.Id + ':2';

Queues.Job secondJob = queue.upsertJobScheduler(
    schedulerName,
    new Queues.RepeatOptions().every(10).startAt(secondRunAt).lim(3),
    new Queues.JobTemplate(processorName, processorClass)
        .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'invocable-template' }),
    Queues.ExecutionMode.INVOCABLE
);

JobScheduler__c updatedScheduler = [
    SELECT Id, ScheduledJobId__c, EveryMinutes__c, Limit__c, IterationCount__c, ExecutionMode__c, IsActive__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];
Job__c secondStoredJob = [
    SELECT Id, ExternalId__c, JobScheduler__c, ExecutionMode__c, QueueableDispatch__c, State__c
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

Assert.areEqual(firstScheduler.Id, updatedScheduler.Id);
Assert.areNotEqual(firstScheduledJobId, updatedScheduler.ScheduledJobId__c);
Assert.areEqual(0, activeFirstSchedulesAfterReupsert.size());
Assert.areEqual(1, activeSecondSchedules.size());
Assert.areEqual(10, QueuesSupport.toInteger(updatedScheduler.EveryMinutes__c, 0));
Assert.areEqual(3, QueuesSupport.toInteger(updatedScheduler.Limit__c, 0));
Assert.areEqual(2, QueuesSupport.toInteger(updatedScheduler.IterationCount__c, 0));
Assert.areEqual(true, updatedScheduler.IsActive__c);
Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), updatedScheduler.ExecutionMode__c);

Job__c canceledFirstJob = [
    SELECT Id, State__c, FailedReason__c, JobScheduler__c
    FROM Job__c
    WHERE Id = :firstStoredJob.Id
];
Assert.areEqual(Queues.State.CANCELED.name(), canceledFirstJob.State__c);
Assert.areEqual('Canceled by job scheduler replacement.', canceledFirstJob.FailedReason__c);
Assert.areEqual(firstScheduler.Id, canceledFirstJob.JobScheduler__c);
Assert.areEqual(secondExternalId, secondStoredJob.ExternalId__c);
Assert.areEqual(firstScheduler.Id, secondStoredJob.JobScheduler__c);
Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), secondStoredJob.ExecutionMode__c);
Assert.areEqual(Queues.State.DELAYED.name(), secondStoredJob.State__c);
Assert.areEqual(null, secondStoredJob.QueueableDispatch__c);
Assert.areEqual(2, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :firstScheduler.Id]);
Assert.areEqual(0, [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName]);
Assert.areEqual(0, [SELECT COUNT() FROM QueueableDispatch__c WHERE QueueName__c = :queueName]);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job-scheduler.reupsert-change-mode',
            'queueName' => queueName,
            'schedulerId' => updatedScheduler.Id,
            'oldScheduledJobId' => firstScheduledJobId,
            'newScheduledJobId' => updatedScheduler.ScheduledJobId__c,
            'oldJobId' => firstStoredJob.Id,
            'newJobId' => secondStoredJob.Id
        }
    )
);
