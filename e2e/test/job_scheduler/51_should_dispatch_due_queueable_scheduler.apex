String queueName = 'e2e-job-scheduler-probe';
String schedulerName = 'queueable-granular';

JobScheduler__c scheduler = [
    SELECT Id, EveryMinutes__c, Limit__c, IterationCount__c, ScheduledJobId__c, NextRunAt__c, IsActive__c
    FROM JobScheduler__c
    WHERE QueueName__c = :queueName AND Name = :schedulerName
    LIMIT 1
];

String firstExternalId = 'scheduler:' + scheduler.Id + ':1';
Job__c firstJob = [
    SELECT Id, ExternalId__c, JobScheduler__c, AvailableAt__c, ExecutionMode__c, State__c, QueueableDispatch__c
    FROM Job__c
    WHERE JobScheduler__c = :scheduler.Id AND ExternalId__c = :firstExternalId
    LIMIT 1
];

Datetime dueRunAt = System.now().addMinutes(-1);
scheduler.NextRunAt__c = dueRunAt;
firstJob.AvailableAt__c = dueRunAt;
update scheduler;
update firstJob;

new QueuesJobSchedulerService().materializeJobSchedulerAndRecordError(scheduler.Id, scheduler.ScheduledJobId__c);

JobScheduler__c updatedScheduler = [
    SELECT Id, EveryMinutes__c, Limit__c, IterationCount__c, ScheduledJobId__c, NextRunAt__c, IsActive__c
    FROM JobScheduler__c
    WHERE Id = :scheduler.Id
    LIMIT 1
];

Job__c dispatchedJob = [
    SELECT Id, QueueableDispatch__c
    FROM Job__c
    WHERE Id = :firstJob.Id
    LIMIT 1
];

String secondExternalId = 'scheduler:' + scheduler.Id + ':2';
Job__c nextJob = [
    SELECT Id, ExternalId__c, JobScheduler__c, AvailableAt__c, ExecutionMode__c, State__c, QueueableDispatch__c
    FROM Job__c
    WHERE JobScheduler__c = :scheduler.Id AND ExternalId__c = :secondExternalId
    LIMIT 1
];

QueueableDispatch__c dispatch = [
    SELECT Id, QueueName__c, DedupeKey__c, Mode__c, State__c, RequestedCount__c, DispatcherAsyncApexJobId__c
    FROM QueueableDispatch__c
    WHERE Id = :dispatchedJob.QueueableDispatch__c
    LIMIT 1
];

Assert.areEqual(true, updatedScheduler.IsActive__c);
Assert.areEqual(2, QueuesSupport.toInteger(updatedScheduler.IterationCount__c, 0));
Assert.areEqual(1, QueuesSupport.toInteger(updatedScheduler.EveryMinutes__c, 0));
Assert.areEqual(2, QueuesSupport.toInteger(updatedScheduler.Limit__c, 0));
Assert.isTrue(String.isNotBlank(updatedScheduler.ScheduledJobId__c));
Assert.areEqual(dispatch.Id, dispatchedJob.QueueableDispatch__c);
Assert.areEqual(queueName, dispatch.QueueName__c);
Assert.areEqual(null, dispatch.DedupeKey__c);
Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), dispatch.Mode__c);
Assert.areEqual(Queues.QueueableDispatchState.WAITING.name(), dispatch.State__c);
Assert.areEqual(1, QueuesSupport.toInteger(dispatch.RequestedCount__c, 0));
Assert.isTrue(String.isNotBlank(dispatch.DispatcherAsyncApexJobId__c));
Assert.areEqual(secondExternalId, nextJob.ExternalId__c);
Assert.areEqual(scheduler.Id, nextJob.JobScheduler__c);
Assert.areEqual(updatedScheduler.NextRunAt__c, nextJob.AvailableAt__c);
Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name(), nextJob.ExecutionMode__c);
Assert.areEqual(Queues.State.DELAYED.name(), nextJob.State__c);
Assert.areEqual(null, nextJob.QueueableDispatch__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job-scheduler.queueable.dispatch-due',
            'queueName' => queueName,
            'schedulerId' => scheduler.Id,
            'dispatchId' => dispatch.Id,
            'nextJobId' => nextJob.Id
        }
    )
);
