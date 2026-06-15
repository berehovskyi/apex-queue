String queueName = 'e2e-job-scheduler-probe';
String schedulerName = 'cancel-after-tick';

JobScheduler__c scheduler = [
    SELECT Id, IsActive__c, ScheduledJobId__c, IterationCount__c, NextRunAt__c, Data__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];
Map<String, Object> cancellationBaseline = (Map<String, Object>) QueuesSupport.deserialize(scheduler.Data__c);
Integer expectedIterationCount = QueuesSupport.toInteger(cancellationBaseline.get('iterationCount'), null);
Integer expectedJobCount = QueuesSupport.toInteger(cancellationBaseline.get('jobCount'), null);
Decimal verifyAfterMs = QueuesSupport.toDecimal(cancellationBaseline.get('verifyAfterMs'), null);
Decimal currentTimeMs = QueuesSupport.toDecimal(System.now().getTime(), 0);
String jobNamePattern = QueuesJobSchedulerSchedule.resolveEveryJobNamePrefix(scheduler.Id) + '%';

Assert.isTrue(currentTimeMs >= verifyAfterMs, 'The post-cancel observation window has not elapsed yet.');
Assert.areEqual(false, scheduler.IsActive__c);
Assert.areEqual(null, scheduler.ScheduledJobId__c);
Assert.areEqual(null, scheduler.NextRunAt__c);
Assert.areEqual(expectedIterationCount, QueuesSupport.toInteger(scheduler.IterationCount__c, 0));
Assert.areEqual(expectedJobCount, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :scheduler.Id]);
Assert.areEqual(
    0,
    [
        SELECT COUNT()
        FROM CronTrigger
        WHERE CronJobDetail.Name LIKE :jobNamePattern AND State NOT IN ('DELETED', 'COMPLETE', 'ERROR')
    ]
);
