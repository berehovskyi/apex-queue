String queueName = 'e2e-job-scheduler-probe';
String schedulerName = 'cancel-after-tick';

JobScheduler__c scheduler = [
    SELECT Id, IsActive__c, ScheduledJobId__c, IterationCount__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];
Integer iterationCount = QueuesSupport.toInteger(scheduler.IterationCount__c, 0);
Assert.isTrue(iterationCount >= 2, 'The scheduler has not completed a real self-rescheduling tick yet.');

Boolean canceled = Queues.of(queueName).cancelJobScheduler(schedulerName);

JobScheduler__c canceledScheduler = [
    SELECT Id, IsActive__c, ScheduledJobId__c, NextRunAt__c
    FROM JobScheduler__c
    WHERE Id = :scheduler.Id
];
Integer jobCount = [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :scheduler.Id];
Map<String, Object> cancellationBaseline = new Map<String, Object>{
    'iterationCount' => iterationCount,
    'jobCount' => jobCount,
    'verifyAfterMs' => System.now().addSeconds(75).getTime()
};
update new JobScheduler__c(Id = scheduler.Id, Data__c = QueuesSupport.serialize(cancellationBaseline));

Assert.areEqual(true, canceled);
Assert.areEqual(false, canceledScheduler.IsActive__c);
Assert.areEqual(null, canceledScheduler.ScheduledJobId__c);
Assert.areEqual(null, canceledScheduler.NextRunAt__c);
Assert.areEqual(iterationCount, jobCount);
