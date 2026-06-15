Set<String> queueNames = QueuesE2EStressSupport.queueNames();
String jobIdPattern = QueuesE2EStressSupport.JOB_ID_PREFIX + '%';
String schedulerNamePattern = QueuesE2EStressSupport.SCHEDULER_NAME_PREFIX + '%';

List<Job__c> directJobs = [
    SELECT Id, State__c, FailedReason__c
    FROM Job__c
    WHERE QueueName__c IN :queueNames AND ExternalId__c LIKE :jobIdPattern
];
Assert.areEqual(QueuesE2EStressSupport.JOB_COUNT_PER_MODE * 4, directJobs.size());
for (Job__c job : directJobs) {
    Assert.areEqual(Queues.State.COMPLETED.name(), job.State__c, job.Id + ': ' + job.FailedReason__c);
}

List<JobScheduler__c> schedulers = [
    SELECT Id, Name, IsActive__c, IterationCount__c, ScheduledJobId__c
    FROM JobScheduler__c
    WHERE Name LIKE :schedulerNamePattern
];
Assert.isTrue(!schedulers.isEmpty());
Set<Id> schedulerIds = new Map<Id, JobScheduler__c>(schedulers).keySet();
for (JobScheduler__c scheduler : schedulers) {
    Assert.areEqual(false, scheduler.IsActive__c, scheduler.Name);
    Assert.areEqual(2, QueuesSupport.toInteger(scheduler.IterationCount__c, 0), scheduler.Name);
    Assert.isTrue(String.isBlank(scheduler.ScheduledJobId__c), scheduler.Name);
}

List<Job__c> schedulerJobs = [
    SELECT Id, State__c, FailedReason__c
    FROM Job__c
    WHERE JobScheduler__c IN :schedulerIds
];
Assert.areEqual(schedulers.size() * 2, schedulerJobs.size());
for (Job__c job : schedulerJobs) {
    Assert.areEqual(Queues.State.COMPLETED.name(), job.State__c, job.Id + ': ' + job.FailedReason__c);
}

List<QueueableDispatch__c> unfinishedDispatches = [
    SELECT Id, State__c, FailedReason__c
    FROM QueueableDispatch__c
    WHERE QueueName__c IN :queueNames AND State__c != 'COMPLETED'
];
Assert.areEqual(0, unfinishedDispatches.size(), JSON.serialize(unfinishedDispatches));

List<QueueRuntime__c> busyRuntimes = [
    SELECT Id, QueueName__c, ActiveWorkerCount__c, WorkerToken__c, AsyncApexJobId__c
    FROM QueueRuntime__c
    WHERE
        QueueName__c IN :queueNames
        AND (ActiveWorkerCount__c != 0
        OR WorkerToken__c != NULL
        OR AsyncApexJobId__c != NULL)
];
Assert.areEqual(0, busyRuntimes.size(), JSON.serialize(busyRuntimes));

List<QueueError__c> unexpectedErrors = new List<QueueError__c>();
for (QueueError__c error : [
    SELECT Id, QueueName__c, Component__c, Message__c
    FROM QueueError__c
    WHERE QueueName__c IN :queueNames
]) {
    if (!error.Message__c?.contains('maximum number (100) of Apex scheduled jobs')) {
        unexpectedErrors.add(error);
    }
}
Assert.areEqual(0, unexpectedErrors.size(), JSON.serialize(unexpectedErrors));

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'stress.converged',
            'directJobs' => directJobs.size(),
            'schedulerJobs' => schedulerJobs.size(),
            'schedulers' => schedulers.size()
        }
    )
);
