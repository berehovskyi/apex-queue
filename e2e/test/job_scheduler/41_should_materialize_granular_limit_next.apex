String queueName = 'e2e-job-scheduler-probe';
String schedulerName = 'granular-limit-five';

JobScheduler__c scheduler = [
    SELECT Id, EveryMinutes__c, Limit__c, IterationCount__c, ScheduledJobId__c, NextRunAt__c, IsActive__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];

Integer previousIterationCount = QueuesSupport.toInteger(scheduler.IterationCount__c, 0);
Datetime previousNextRunAt = scheduler.NextRunAt__c;
String previousScheduledJobId = scheduler.ScheduledJobId__c;

Assert.areEqual(true, scheduler.IsActive__c);
Assert.areEqual(5, QueuesSupport.toInteger(scheduler.Limit__c, 0));
Assert.isTrue(previousIterationCount >= 1);
Assert.isTrue(previousIterationCount < 5);
Assert.isTrue(String.isNotBlank(previousScheduledJobId));

new QueuesJobSchedulerService().materializeJobSchedulerAndRecordError(scheduler.Id, scheduler.ScheduledJobId__c);

JobScheduler__c updatedScheduler = [
    SELECT Id, EveryMinutes__c, Limit__c, IterationCount__c, ScheduledJobId__c, NextRunAt__c, IsActive__c
    FROM JobScheduler__c
    WHERE Id = :scheduler.Id
    LIMIT 1
];

Integer expectedIterationCount = previousIterationCount + 1;
String expectedExternalId = 'scheduler:' + scheduler.Id + ':' + expectedIterationCount;
Job__c materializedJob = [
    SELECT Id, ExternalId__c, JobScheduler__c, AvailableAt__c, ExecutionMode__c, State__c
    FROM Job__c
    WHERE JobScheduler__c = :scheduler.Id AND ExternalId__c = :expectedExternalId
    LIMIT 1
];

Assert.areEqual(true, updatedScheduler.IsActive__c);
Assert.areEqual(expectedIterationCount, QueuesSupport.toInteger(updatedScheduler.IterationCount__c, 0));
Assert.areEqual(1, QueuesSupport.toInteger(updatedScheduler.EveryMinutes__c, 0));
Assert.areEqual(5, QueuesSupport.toInteger(updatedScheduler.Limit__c, 0));
Assert.isTrue(String.isNotBlank(updatedScheduler.ScheduledJobId__c));
Assert.areNotEqual(previousScheduledJobId, updatedScheduler.ScheduledJobId__c);
Assert.areEqual(previousNextRunAt.addMinutes(1), updatedScheduler.NextRunAt__c);
Assert.areEqual(expectedExternalId, materializedJob.ExternalId__c);
Assert.areEqual(scheduler.Id, materializedJob.JobScheduler__c);
Assert.areEqual(updatedScheduler.NextRunAt__c, materializedJob.AvailableAt__c);
Assert.areEqual(Queues.ExecutionMode.WORKER.name(), materializedJob.ExecutionMode__c);
Assert.areEqual(Queues.State.DELAYED.name(), materializedJob.State__c);
Assert.areEqual(expectedIterationCount, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :scheduler.Id]);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job-scheduler.granular-limit-five.materialize-next',
            'queueName' => queueName,
            'schedulerId' => scheduler.Id,
            'iterationCount' => updatedScheduler.IterationCount__c,
            'scheduledJobId' => updatedScheduler.ScheduledJobId__c,
            'jobId' => materializedJob.Id
        }
    )
);
