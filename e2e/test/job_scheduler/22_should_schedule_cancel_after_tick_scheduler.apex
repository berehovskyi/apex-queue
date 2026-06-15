String queueName = 'e2e-job-scheduler-probe';
String schedulerName = 'cancel-after-tick';
String processorName = 'job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';

Queues.Queue queue = Queues.of(queueName);
queue.pause();

Queues.Job job = queue.upsertJobScheduler(
    schedulerName,
    new Queues.RepeatOptions().every(1).startAt(System.now().addMinutes(1)).lim(10),
    new Queues.JobTemplate(processorName, processorClass)
        .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'cancel-after-tick' })
);

JobScheduler__c scheduler = [
    SELECT Id, IsActive__c, ScheduledJobId__c, IterationCount__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];

Assert.areEqual(true, scheduler.IsActive__c);
Assert.isTrue(String.isNotBlank(scheduler.ScheduledJobId__c));
Assert.areEqual(1, QueuesSupport.toInteger(scheduler.IterationCount__c, 0));
Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE JobScheduler__c = :scheduler.Id]);
Assert.areNotEqual(null, job.getId());
