String queueName = 'e2e-job-probe';

// Arrange
List<Job__c> jobs = [
    SELECT Id, ExternalId__c, State__c, AttemptsMade__c, FailedReason__c, ReturnValue__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN ('job-priority-high', 'job-priority-low')
    ORDER BY ExternalId__c ASC
];
Assert.areEqual(2, jobs.size());

Map<String, Job__c> jobByExternalId = new Map<String, Job__c>();
for (Job__c job : jobs) {
    jobByExternalId.put(job.ExternalId__c, job);
}

Job__c highPriorityJob = jobByExternalId.get('job-priority-high');
Job__c lowPriorityJob = jobByExternalId.get('job-priority-low');
List<JobRun__c> runs = [
    SELECT Job__c, Status__c, Message__c, AttemptNumber__c
    FROM JobRun__c
    WHERE Job__c IN :new Set<Id>{ highPriorityJob.Id, lowPriorityJob.Id }
    ORDER BY CreatedDate ASC
];

// Act
Boolean highCompleted = QueuesSupport.toState(highPriorityJob.State__c) == Queues.State.COMPLETED;
Boolean lowCompleted = QueuesSupport.toState(lowPriorityJob.State__c) == Queues.State.COMPLETED;
Map<String, Object> highReturnValue = (Map<String, Object>) JSON.deserializeUntyped(highPriorityJob.ReturnValue__c);
Map<String, Object> lowReturnValue = (Map<String, Object>) JSON.deserializeUntyped(lowPriorityJob.ReturnValue__c);

// Assert
Assert.areEqual(true, highCompleted);
Assert.areEqual(true, lowCompleted);
Assert.areEqual(1, QueuesSupport.toInteger(lowPriorityJob.AttemptsMade__c, 0));
Assert.areEqual(null, lowPriorityJob.FailedReason__c);
Assert.areEqual(queueName + ':job-priority-high', String.valueOf(highReturnValue.get('idempotencyKey')));
Assert.areEqual(queueName + ':job-priority-low', String.valueOf(lowReturnValue.get('idempotencyKey')));
Assert.areEqual(2, runs.size());
Assert.areEqual(
    Queues.RunStatus.COMPLETED,
    Queues.RunStatus.valueOf(QueuesSupport.normalizeEnumValue(runs[0].Status__c))
);
Assert.areEqual(
    Queues.RunStatus.COMPLETED,
    Queues.RunStatus.valueOf(QueuesSupport.normalizeEnumValue(runs[1].Status__c))
);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'job.priority.assert',
            'queueName' => queueName,
            'highPriorityJobId' => highPriorityJob.Id,
            'lowPriorityJobId' => lowPriorityJob.Id
        }
    )
);
