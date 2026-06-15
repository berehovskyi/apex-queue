String queueName = 'e2e-job-scheduler-probe';
String schedulerName = 'granular-limit-five';

JobScheduler__c scheduler = [
    SELECT Id, Limit__c, IterationCount__c, ScheduledJobId__c, IsActive__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];

Assert.areEqual(true, scheduler.IsActive__c);
Assert.areEqual(5, QueuesSupport.toInteger(scheduler.Limit__c, 0));
Assert.areEqual(5, QueuesSupport.toInteger(scheduler.IterationCount__c, 0));
Assert.isTrue(String.isNotBlank(scheduler.ScheduledJobId__c));
Assert.areEqual(5, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :scheduler.Id]);

new QueuesJobSchedulerService().materializeJobSchedulerAndRecordError(scheduler.Id, scheduler.ScheduledJobId__c);

JobScheduler__c updatedScheduler = [
    SELECT Id, Limit__c, IterationCount__c, ScheduledJobId__c, NextRunAt__c, IsActive__c
    FROM JobScheduler__c
    WHERE Id = :scheduler.Id
    LIMIT 1
];
String sixthExternalId = 'scheduler:' + scheduler.Id + ':6';

Assert.areEqual(false, updatedScheduler.IsActive__c);
Assert.areEqual(null, updatedScheduler.ScheduledJobId__c);
Assert.areEqual(null, updatedScheduler.NextRunAt__c);
Assert.areEqual(5, QueuesSupport.toInteger(updatedScheduler.IterationCount__c, 0));
Assert.areEqual(5, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :scheduler.Id]);
Assert.areEqual(
    0,
    [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :scheduler.Id AND ExternalId__c = :sixthExternalId]
);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job-scheduler.granular-limit-five.stopped',
            'queueName' => queueName,
            'schedulerId' => scheduler.Id,
            'iterationCount' => updatedScheduler.IterationCount__c
        }
    )
);
