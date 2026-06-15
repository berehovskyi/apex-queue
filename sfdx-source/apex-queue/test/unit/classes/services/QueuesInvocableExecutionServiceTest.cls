@IsTest(IsParallel=true)
private class QueuesInvocableExecutionServiceTest {
    @IsTest
    static void should_construct_default_service() {
        // Act + Assert
        Assert.areNotEqual(null, new QueuesInvocableExecutionService());
    }

    @IsTest
    static void should_claim_due_invocable_job() {
        // Arrange
        Id jobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 715);
        List<Job__c> claimedJobs = new List<Job__c>{ new Job__c(Id = jobId, State__c = Queues.State.ACTIVE.name()) };
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('claimInvocableJobsAsSystem', claimedJobs);
        QueuesInvocableExecutionService service = new QueuesInvocableExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesJobRunService(),
            new QueuesStalledJobRecoveryService()
        );

        // Act
        List<Job__c> result = service.claimDueJobsAsSystem(new Set<Id>{ jobId });

        // Assert
        Assert.areEqual(claimedJobs, result);
        Assert.areEqual(1, jobServiceProvider.getCallCount('claimInvocableJobsAsSystem'));
        Assert.areEqual(
            new Set<Id>{ jobId },
            (Set<Id>) jobServiceProvider.getLastArgs('claimInvocableJobsAsSystem')[0]
        );
    }

    @IsTest
    static void should_execute_invocable_job_with_current_lease() {
        // Arrange
        Id jobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 712);
        Job__c claimedJob = new Job__c(Id = jobId, LeaseToken__c = 'invocable-lease');
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobAsSystem', claimedJob);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('executeInvocableJob', claimedJob);
        QueuesInvocableExecutionService service = new QueuesInvocableExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            new QueuesJobRunService(),
            new QueuesStalledJobRecoveryService(new QueuesJobService(), new QueuesJobRunService())
        );

        // Act
        Job__c executedJob = service.executeJobAsSystem(jobId);

        // Assert
        Assert.areEqual(claimedJob, executedJob);
        Assert.areEqual(1, jobServiceProvider.getCallCount('requireJobAsSystem'));
        Assert.areEqual(1, executionProvider.getCallCount('executeInvocableJob'));
        Assert.areEqual(jobId, executionProvider.getLastArgs('executeInvocableJob')[0]);
        Assert.areEqual('invocable-lease', executionProvider.getLastArgs('executeInvocableJob')[1]);
    }

    @IsTest
    static void should_ignore_execute_when_stale_handoff_has_no_lease() {
        // Arrange
        Id jobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 714);
        Job__c unclaimedJob = new Job__c(Id = jobId);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobAsSystem', unclaimedJob);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesInvocableExecutionService service = new QueuesInvocableExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            new QueuesJobRunService(),
            new QueuesStalledJobRecoveryService()
        );

        // Act
        Job__c result = service.executeJobAsSystem(jobId);

        // Assert
        Assert.areEqual(unclaimedJob, result);
        Assert.areEqual(1, jobServiceProvider.getCallCount('requireJobAsSystem'));
        Assert.areEqual(0, executionProvider.getCallCount('executeInvocableJob'));
        Assert.areEqual(0, executionProvider.getCallCount('failInvocableJob'));
    }

    @IsTest
    static void should_fail_invocable_job_when_execution_throws() {
        // Arrange
        Id jobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 713);
        Job__c claimedJob = new Job__c(Id = jobId, LeaseToken__c = 'failed-lease');
        Job__c failedJob = new Job__c(Id = jobId, State__c = Queues.State.FAILED.name());
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobAsSystem', claimedJob);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withException('executeInvocableJob', new Queues.QueueException('Processor exploded.'))
            .withReturn('failInvocableJob', failedJob);
        QueuesInvocableExecutionService service = new QueuesInvocableExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            new QueuesJobRunService(),
            new QueuesStalledJobRecoveryService()
        );

        // Act
        Job__c result = service.executeJobAsSystem(jobId);

        // Assert
        Assert.areEqual(failedJob, result);
        Assert.areEqual(1, executionProvider.getCallCount('executeInvocableJob'));
        Assert.areEqual(1, executionProvider.getCallCount('failInvocableJob'));
        Assert.areEqual(jobId, executionProvider.getLastArgs('failInvocableJob')[0]);
        Assert.areEqual('failed-lease', executionProvider.getLastArgs('failInvocableJob')[1]);
        Assert.areEqual('Processor exploded.', executionProvider.getLastArgs('failInvocableJob')[2]);
    }

    @IsTest
    static void should_recover_stalled_invocable_jobs_and_record_failed_runs() {
        // Arrange
        String queueName = 'invocable-recover';
        Datetime expiredAt = System.now().addMinutes(-10);
        Job__c recoverableJob = invocableJob(queueName, 'recoverable', 0, 3, 0);
        recoverableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 710);
        recoverableJob.LeaseToken__c = 'recoverable-lease';
        recoverableJob.LeaseExpiresAt__c = expiredAt;
        recoverableJob.ReturnValue__c = '{"stale":true}';
        recoverableJob.FailedReason__c = 'previous failure';

        Job__c exhaustedJob = invocableJob(queueName, 'exhausted', 3, 3, 0);
        exhaustedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 711);
        exhaustedJob.LeaseToken__c = 'exhausted-lease';
        exhaustedJob.LeaseExpiresAt__c = expiredAt;
        exhaustedJob.ReturnValue__c = '{"stale":true}';

        QueuesTestSupport.RecordingStubProvider jobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryExpiredActiveInvocableJobsForUpdate', new List<Job__c>{ recoverableJob, exhaustedJob })
            .withReturn('queryStaleDueInvocableJobsForUpdate', new List<Job__c>())
            .withReturn('updateJobs', new List<Job__c>());
        QueuesJobService jobService = new QueuesJobService(
            (QueuesJobRepository) Test.createStub(QueuesJobRepository.class, jobRepositoryProvider),
            new QueuesDefinitionService()
        );

        QueuesTestSupport.RecordingStubProvider runRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('insertJobRuns', new List<JobRun__c>());
        QueuesJobRunService jobRunService = new QueuesJobRunService(
            (QueuesJobRunRepository) Test.createStub(QueuesJobRunRepository.class, runRepositoryProvider)
        );
        QueuesInvocableExecutionService service = new QueuesInvocableExecutionService(
            jobService,
            new QueuesJobExecutionService(),
            jobRunService,
            new QueuesStalledJobRecoveryService()
        );

        // Act
        Integer recoveredCount = service.recoverAsSystem(queueName, System.now().addMinutes(-5));

        // Assert
        Assert.areEqual(2, recoveredCount);
        Assert.areEqual(1, jobRepositoryProvider.getCallCount('queryExpiredActiveInvocableJobsForUpdate'));
        Assert.areEqual(1, jobRepositoryProvider.getCallCount('updateJobs'));
        Assert.areEqual(1, runRepositoryProvider.getCallCount('insertJobRuns'));

        List<Job__c> updatedJobs = (List<Job__c>) jobRepositoryProvider.getLastArgs('updateJobs')[0];
        Job__c recoveredJob = updatedJobs[0];
        Job__c failedJob = updatedJobs[1];
        Assert.areEqual(Queues.State.WAITING.name(), recoveredJob.State__c);
        Assert.areEqual(null, recoveredJob.LeaseToken__c);
        Assert.areEqual(null, recoveredJob.LeaseExpiresAt__c);
        Assert.areEqual(null, recoveredJob.AsyncApexJobId__c);
        Assert.areEqual(null, recoveredJob.ReturnValue__c);
        Assert.areEqual(null, recoveredJob.FailedReason__c);
        Assert.areEqual(1, recoveredJob.StalledCount__c);
        Assert.areNotEqual(null, recoveredJob.AvailableAt__c);
        Assert.areEqual(Queues.State.FAILED.name(), failedJob.State__c);
        Assert.areEqual(null, failedJob.LeaseToken__c);
        Assert.areEqual(null, failedJob.LeaseExpiresAt__c);
        Assert.areEqual(
            'Lease expired before invocable action completed and no more recovery attempts remain.',
            failedJob.FailedReason__c
        );

        List<JobRun__c> runs = (List<JobRun__c>) runRepositoryProvider.getLastArgs('insertJobRuns')[0];
        Assert.areEqual(2, runs.size());
        Assert.areEqual(Queues.RunStatus.FAILED.name(), runs[0].Status__c);
        Assert.areEqual('Lease expired before invocable action completed.', runs[0].Message__c);
        Assert.areEqual(Queues.RunStatus.FAILED.name(), runs[1].Status__c);
        Assert.areEqual(
            'Lease expired before invocable action completed and no more recovery attempts remain.',
            runs[1].Message__c
        );
    }

    @IsTest
    static void should_stop_recovery_when_batch_size_resolves_to_zero() {
        // Arrange
        String queueName = 'invocable-zero-batch';
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider recoveryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('hasHeadroomForRecovery', true)
            .withReturn('resolveRecoverStalledBatchSize', 0);
        QueuesInvocableExecutionService service = new QueuesInvocableExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesJobRunService(),
            (QueuesStalledJobRecoveryService) Test.createStub(QueuesStalledJobRecoveryService.class, recoveryProvider)
        );

        // Act
        Integer recoveredCount = service.recoverAsSystem(queueName, System.now().addMinutes(-5));

        // Assert
        Assert.areEqual(0, recoveredCount);
        Assert.areEqual(2, recoveryProvider.getCallCount('hasHeadroomForRecovery'));
        Assert.areEqual(2, recoveryProvider.getCallCount('resolveRecoverStalledBatchSize'));
        Assert.areEqual(0, jobServiceProvider.getCallCount('queryExpiredActiveInvocableJobsForUpdate'));
    }

    @IsTest
    static void should_stop_recovery_when_no_stalled_jobs_are_found() {
        // Arrange
        String queueName = 'invocable-no-stalled';
        QueuesTestSupport.RecordingStubProvider jobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryExpiredActiveInvocableJobsForUpdate', new List<Job__c>())
            .withReturn('queryStaleDueInvocableJobsForUpdate', new List<Job__c>());
        QueuesJobService jobService = new QueuesJobService(
            (QueuesJobRepository) Test.createStub(QueuesJobRepository.class, jobRepositoryProvider),
            new QueuesDefinitionService()
        );
        QueuesInvocableExecutionService service = new QueuesInvocableExecutionService(
            jobService,
            new QueuesJobExecutionService(),
            new QueuesJobRunService(),
            new QueuesStalledJobRecoveryService()
        );

        // Act
        Integer recoveredCount = service.recoverAsSystem(queueName, System.now().addMinutes(-5));

        // Assert
        Assert.areEqual(0, recoveredCount);
        Assert.areEqual(1, jobRepositoryProvider.getCallCount('queryExpiredActiveInvocableJobsForUpdate'));
        Assert.areEqual(1, jobRepositoryProvider.getCallCount('queryStaleDueInvocableJobsForUpdate'));
    }

    @IsTest
    static void should_schedule_stale_due_invocable_jobs_to_repair_lost_wakes() {
        // Arrange
        String queueName = 'invocable-lost-wake';
        Datetime staleBefore = System.now().addMinutes(-5);
        Job__c waitingJob = invocableJob(queueName, 'waiting', 0, 3, 0);
        waitingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 716);
        waitingJob.State__c = Queues.State.WAITING.name();
        waitingJob.AvailableAt__c = staleBefore.addMinutes(-2);
        Job__c delayedJob = invocableJob(queueName, 'delayed', 0, 3, 0);
        delayedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 717);
        delayedJob.State__c = Queues.State.DELAYED.name();
        delayedJob.AvailableAt__c = staleBefore.addMinutes(-1);
        QueuesTestSupport.RecordingStubProvider jobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryExpiredActiveInvocableJobsForUpdate', new List<Job__c>())
            .withReturn('queryStaleDueInvocableJobsForUpdate', new List<Job__c>{ waitingJob, delayedJob })
            .withReturn('updateJobs', new List<Job__c>());
        QueuesJobService jobService = new QueuesJobService(
            (QueuesJobRepository) Test.createStub(QueuesJobRepository.class, jobRepositoryProvider),
            new QueuesDefinitionService()
        );
        QueuesInvocableExecutionService service = new QueuesInvocableExecutionService(
            jobService,
            new QueuesJobExecutionService(),
            new QueuesJobRunService(),
            new QueuesStalledJobRecoveryService()
        );

        // Act
        Integer repairedCount = service.recoverAsSystem(queueName, staleBefore);

        // Assert
        Assert.areEqual(2, repairedCount);
        Assert.areEqual(1, jobRepositoryProvider.getCallCount('queryStaleDueInvocableJobsForUpdate'));
        List<Job__c> wakeUpdates = (List<Job__c>) jobRepositoryProvider.getLastArgs('updateJobs')[0];
        Assert.areEqual(2, wakeUpdates.size());
        Assert.areEqual(new Set<String>{ 'Id', 'AvailableAt__c' }, wakeUpdates[0].getPopulatedFieldsAsMap().keySet());
        Assert.areEqual(new Set<String>{ 'Id', 'AvailableAt__c' }, wakeUpdates[1].getPopulatedFieldsAsMap().keySet());
        Assert.isTrue(wakeUpdates[0].AvailableAt__c > System.now());
        Assert.isTrue(wakeUpdates[1].AvailableAt__c > System.now());
    }

    @IsTest
    static void should_not_query_when_recovery_headroom_is_exhausted() {
        // Act + Assert
        Assert.areEqual(false, QueuesStalledJobRecoveryService.hasHeadroomForRecovery(499, 10, 5, 2000));
        Assert.areEqual(false, QueuesStalledJobRecoveryService.hasHeadroomForRecovery(500, 9, 5, 2000));
        Assert.areEqual(false, QueuesStalledJobRecoveryService.hasHeadroomForRecovery(500, 10, 4, 2000));
        Assert.areEqual(false, QueuesStalledJobRecoveryService.hasHeadroomForRecovery(500, 10, 5, 1999));
        Assert.areEqual(true, QueuesStalledJobRecoveryService.hasHeadroomForRecovery(500, 10, 5, 2000));
        Assert.areEqual(0, QueuesStalledJobRecoveryService.resolveRecoverStalledBatchSize(501));
        Assert.areEqual(1, QueuesStalledJobRecoveryService.resolveRecoverStalledBatchSize(502));
        Assert.areEqual(1000, QueuesStalledJobRecoveryService.resolveRecoverStalledBatchSize(2500));
    }

    private static Job__c invocableJob(
        final String queueName,
        final String externalId,
        final Integer attemptsMade,
        final Integer maxAttempts,
        final Integer stalledCount
    ) {
        Job__c job = QueuesTestSupport.job(
            queueName,
            QueuesTestSupport.DEFAULT_JOB_NAME,
            Queues.State.ACTIVE.name(),
            externalId
        );
        job.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
        job.AttemptsMade__c = attemptsMade;
        job.MaxAttempts__c = maxAttempts;
        job.StalledCount__c = stalledCount;
        return job;
    }
}
