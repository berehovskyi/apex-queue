String queueName = 'e2e-job-scheduler-probe';
String schedulerName = 'regular-cron-every-minute';

JobScheduler__c scheduler = [
    SELECT Id, Pattern__c, EveryMinutes__c, ScheduledJobId__c, IterationCount__c, IsActive__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];

List<Job__c> jobs = [
    SELECT Id, ExternalId__c, AvailableAt__c, JobScheduler__c, ExecutionMode__c, State__c
    FROM Job__c
    WHERE JobScheduler__c = :scheduler.Id
    ORDER BY ExternalId__c ASC
];

String firstExternalId = 'scheduler:' + scheduler.Id + ':1';
String secondExternalId = 'scheduler:' + scheduler.Id + ':2';
Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
for (Job__c job : jobs) {
    jobsByExternalId.put(job.ExternalId__c, job);
}

Assert.isTrue(String.isNotBlank(scheduler.Pattern__c));
Assert.areEqual(null, scheduler.EveryMinutes__c);
Assert.areEqual(true, scheduler.IsActive__c);
Assert.isTrue(String.isNotBlank(scheduler.ScheduledJobId__c));
Assert.isTrue(
    QueuesSupport.toInteger(scheduler.IterationCount__c, 0) >= 2,
    'Regular cron scheduler has not fired yet.'
);
Assert.isTrue(jobs.size() >= 2, 'Regular cron scheduler has not materialized the next job yet.');
Assert.isTrue(jobsByExternalId.containsKey(firstExternalId));
Assert.isTrue(jobsByExternalId.containsKey(secondExternalId));
Assert.areEqual(Queues.ExecutionMode.WORKER.name(), jobsByExternalId.get(secondExternalId).ExecutionMode__c);
Assert.areEqual(scheduler.Id, jobsByExternalId.get(secondExternalId).JobScheduler__c);
Assert.isTrue(
    jobsByExternalId.get(secondExternalId).AvailableAt__c > jobsByExternalId.get(firstExternalId).AvailableAt__c
);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job-scheduler.regular-cron.materialized',
            'queueName' => queueName,
            'schedulerId' => scheduler.Id,
            'iterationCount' => scheduler.IterationCount__c,
            'jobCount' => jobs.size(),
            'secondJobId' => jobsByExternalId.get(secondExternalId).Id
        }
    )
);
