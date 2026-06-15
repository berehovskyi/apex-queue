String queueName = 'e2e-job-scheduler-probe';
List<String> cleanupMessages = new List<String>();

List<JobScheduler__c> schedulers = [
    SELECT Id, ScheduledJobId__c, Pattern__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName
];

Set<String> scheduledJobIds = new Set<String>();
for (JobScheduler__c scheduler : schedulers) {
    if (String.isNotBlank(scheduler.ScheduledJobId__c)) {
        scheduledJobIds.add(scheduler.ScheduledJobId__c);
    }

    String jobNamePrefix = String.isNotBlank(scheduler.Pattern__c)
        ? QueuesJobSchedulerSchedule.resolvePatternJobNamePrefix(scheduler.Id)
        : QueuesJobSchedulerSchedule.resolveEveryJobNamePrefix(scheduler.Id);
    String jobNamePattern = jobNamePrefix + '%';
    for (CronTrigger scheduledJob : [
        SELECT Id
        FROM CronTrigger
        WHERE CronJobDetail.Name LIKE :jobNamePattern AND State NOT IN ('DELETED', 'COMPLETE', 'ERROR')
    ]) {
        scheduledJobIds.add(scheduledJob.Id);
    }
}

List<QueueRuntime__c> runtimes = [
    SELECT Id, QueueName__c, AsyncApexJobId__c
    FROM QueueRuntime__c
    WHERE QueueName__c = :queueName
];
for (QueueRuntime__c runtime : runtimes) {
    if (String.isNotBlank(runtime.AsyncApexJobId__c)) {
        scheduledJobIds.add(runtime.AsyncApexJobId__c);
    }
}

for (String scheduledJobId : scheduledJobIds) {
    try {
        System.abortJob(scheduledJobId);
        cleanupMessages.add('Aborted scheduled job ' + scheduledJobId + '.');
    } catch (Exception ex) {
        cleanupMessages.add(
            'Skipped abort for scheduled job ' + scheduledJobId + ': ' + QueuesSupport.resolveExceptionReason(ex)
        );
    }
}

List<QueueError__c> queueErrors = [
    SELECT Id
    FROM QueueError__c
    WHERE QueueName__c = :queueName
];
if (!queueErrors.isEmpty()) {
    delete queueErrors;
}

List<Job__c> jobs = [
    SELECT Id
    FROM Job__c
    WHERE QueueName__c = :queueName
];
if (!jobs.isEmpty()) {
    delete jobs;
}

List<QueueableDispatch__c> dispatches = [
    SELECT Id
    FROM QueueableDispatch__c
    WHERE QueueName__c = :queueName
];
if (!dispatches.isEmpty()) {
    delete dispatches;
}

if (!schedulers.isEmpty()) {
    delete schedulers;
}

if (!runtimes.isEmpty()) {
    delete runtimes;
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'queueName' => queueName,
            'aborts' => cleanupMessages,
            'deletedSchedulers' => schedulers.size(),
            'deletedErrors' => queueErrors.size(),
            'deletedJobs' => jobs.size(),
            'deletedDispatches' => dispatches.size(),
            'deletedRuntimes' => runtimes.size()
        }
    )
);
