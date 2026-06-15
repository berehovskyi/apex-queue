@IsTest(IsParallel=false)
private class QueuesJobExecutionServiceTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_return_job_without_processing_when_lease_token_mismatches() {
        // Arrange
        Job__c claimedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        claimedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 2);
        claimedJob.LeaseToken__c = 'stored-token';
        claimedJob.LeaseExpiresAt__c = System.now().addMinutes(5);

        Job__c scopeJob = new Job__c(Id = claimedJob.Id, LeaseToken__c = 'different-token');
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsAsSystem', new List<Job__c>{ claimedJob });
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            jobServiceProvider
        );
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
            QueuesProcessorService.class,
            processorProvider
        );
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesJobRunService jobRunStub = (QueuesJobRunService) Test.createStub(
            QueuesJobRunService.class,
            runServiceProvider
        );

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            jobServiceStub,
            jobRunStub,
            processorStub,
            new QueuesFlowProducerService()
        );

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>{ scopeJob }, null);

        // Assert
        Assert.areEqual(0, processorProvider.getCallCount('instantiate'));
        Assert.areEqual(0, runServiceProvider.getCallCount('insertJobRunsAsSystem'));
    }

    @IsTest
    static void should_execute_invocable_job_when_active_lease_is_owned() {
        // Arrange
        Job__c invocableJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        invocableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 3);
        invocableJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
        invocableJob.LeaseToken__c = 'invocable-lease-token';
        invocableJob.LeaseExpiresAt__c = System.now().addMinutes(5);
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
        Queues.ProcessResult result = new Queues.JobContext(invocableJob).complete('done');

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', invocableJob)
            .withReturn('queryJobsForUpdate', new List<Job__c>{ invocableJob });
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'getProcessorsByClassAsSystem',
                new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
            )
            .withReturn('instantiate', 'processor')
            .withReturn('execute', result);
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(QueuesProcessorService.class, processorProvider),
            new QueuesFlowProducerService()
        );

        // Act
        service.executeInvocableJob(invocableJob.Id, 'invocable-lease-token');

        // Assert
        Assert.areEqual(1, processorProvider.getCallCount('execute'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('applyResult'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRuns'));
        JobRun__c run = ((List<JobRun__c>) runServiceProvider.getLastArgs('insertJobRuns')[0])[0];
        Assert.areEqual(null, run.AsyncApexJobId__c);
        Assert.areEqual(Queues.RunStatus.COMPLETED.name(), run.Status__c);
    }

    @IsTest
    static void should_skip_invocable_execution_when_job_mode_is_not_invocable() {
        // Arrange
        Job__c workerJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        workerJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 4);
        workerJob.ExecutionMode__c = Queues.ExecutionMode.WORKER.name();
        workerJob.LeaseToken__c = 'worker-lease-token';
        workerJob.LeaseExpiresAt__c = System.now().addMinutes(5);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', workerJob);
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(QueuesProcessorService.class, processorProvider),
            new QueuesFlowProducerService()
        );

        // Act
        Job__c result = service.executeInvocableJob(workerJob.Id, 'worker-lease-token');

        // Assert
        Assert.areEqual(workerJob.Id, result.Id);
        Assert.areEqual(0, processorProvider.getCallCount('execute'));
        Assert.areEqual(0, runServiceProvider.getCallCount('insertJobRuns'));
    }

    @IsTest
    static void should_execute_batch_jobs_when_scope_entries_are_executable() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 321);
        Job__c executableJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        executableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 11);
        executableJob.LeaseToken__c = 'lease-token';
        executableJob.LeaseExpiresAt__c = System.now().addMinutes(5);
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
        Job__c skippedJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        skippedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 12);

        Queues.ProcessResult result = new Queues.JobContext(executableJob)
            .complete(new Map<String, Object>{ 'status' => 'done' });

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobs', new List<Job__c>{ executableJob, skippedJob })
            .withReturn('queryJobsForUpdate', new List<Job__c>{ executableJob });
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            jobServiceProvider
        );
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'getProcessorsByClassAsSystem',
                new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
            )
            .withReturn('instantiate', 'processor')
            .withReturn('execute', result);
        QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
            QueuesProcessorService.class,
            processorProvider
        );
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesJobRunService jobRunStub = (QueuesJobRunService) Test.createStub(
            QueuesJobRunService.class,
            runServiceProvider
        );
        QueuesTestSupport.RecordingStubProvider flowProducerProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            jobServiceStub,
            jobRunStub,
            processorStub,
            (QueuesFlowProducerService) Test.createStub(QueuesFlowProducerService.class, flowProducerProvider)
        );

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>{ executableJob, skippedJob }, asyncApexJobId);

        // Assert
        Assert.areEqual(1, processorProvider.getCallCount('getProcessorsByClassAsSystem'));
        Assert.areEqual(1, processorProvider.getCallCount('instantiate'));
        Assert.areEqual(1, processorProvider.getCallCount('execute'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('queryJobs'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('queryJobsForUpdate'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('applyResult'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
        Assert.areEqual(1, runServiceProvider.getCallCount('addTimestamps'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRuns'));
        Assert.areEqual(1, flowProducerProvider.getCallCount('deferParentDependenciesAsSystem'));
        Assert.areEqual(0, flowProducerProvider.getCallCount('resolveParentDependenciesAsSystem'));
        JobRun__c run = ((List<JobRun__c>) runServiceProvider.getLastArgs('insertJobRuns')[0])[0];
        Assert.areEqual(asyncApexJobId, run.AsyncApexJobId__c);
    }

    @IsTest
    static void should_skip_batch_finalize_when_no_claimed_jobs_are_executable() {
        // Arrange
        Job__c claimedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        claimedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 13);
        claimedJob.LeaseToken__c = 'lease-token';
        claimedJob.LeaseExpiresAt__c = System.now().addMinutes(5);

        Job__c currentJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        currentJob.Id = claimedJob.Id;

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobs', new List<Job__c>{ currentJob });
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            jobServiceProvider
        );
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('getProcessorsByClassAsSystem', new Map<String, Processor__mdt>());
        QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
            QueuesProcessorService.class,
            processorProvider
        );
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesJobRunService jobRunStub = (QueuesJobRunService) Test.createStub(
            QueuesJobRunService.class,
            runServiceProvider
        );

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            jobServiceStub,
            jobRunStub,
            processorStub,
            new QueuesFlowProducerService()
        );

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>{ claimedJob }, null);

        // Assert
        Assert.areEqual(1, jobServiceProvider.getCallCount('queryJobs'));
        Assert.areEqual(0, jobServiceProvider.getCallCount('queryJobsForUpdate'));
        Assert.areEqual(0, jobServiceProvider.getCallCount('updateJobs'));
        Assert.areEqual(0, runServiceProvider.getCallCount('insertJobRuns'));
    }

    @IsTest
    static void should_record_failed_run_when_processor_throws() {
        // Arrange
        Job__c executableJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        executableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 21);
        executableJob.LeaseToken__c = 'lease-token';
        executableJob.LeaseExpiresAt__c = System.now().addMinutes(5);
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobs', new List<Job__c>{ executableJob })
            .withReturn('queryJobsForUpdate', new List<Job__c>{ executableJob });
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            jobServiceProvider
        );
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'getProcessorsByClassAsSystem',
                new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
            )
            .withReturn('instantiate', 'processor')
            .withException('execute', new Queues.QueueException('Processor exploded.'));
        QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
            QueuesProcessorService.class,
            processorProvider
        );
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesJobRunService jobRunStub = (QueuesJobRunService) Test.createStub(
            QueuesJobRunService.class,
            runServiceProvider
        );

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            jobServiceStub,
            jobRunStub,
            processorStub,
            new QueuesFlowProducerService()
        );

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>{ executableJob }, null);

        // Assert
        Assert.areEqual(1, jobServiceProvider.getCallCount('applyFailure'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRuns'));

        List<Object> applyFailureArgs = jobServiceProvider.getLastArgs('applyFailure');
        Assert.areEqual('Processor exploded.', (String) applyFailureArgs[1]);
        Assert.areEqual(false, (Boolean) applyFailureArgs[3]);
    }

    @IsTest
    static void should_record_unrecoverable_failed_run_when_processor_throws_unrecoverable_exception() {
        // Arrange
        Job__c executableJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        executableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 22);
        executableJob.LeaseToken__c = 'lease-token';
        executableJob.LeaseExpiresAt__c = System.now().addMinutes(5);
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobs', new List<Job__c>{ executableJob })
            .withReturn('queryJobsForUpdate', new List<Job__c>{ executableJob });
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            jobServiceProvider
        );
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'getProcessorsByClassAsSystem',
                new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
            )
            .withReturn('instantiate', 'processor')
            .withException('execute', new Queues.UnrecoverableJobException('Do not retry.'));
        QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
            QueuesProcessorService.class,
            processorProvider
        );
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesJobRunService jobRunStub = (QueuesJobRunService) Test.createStub(
            QueuesJobRunService.class,
            runServiceProvider
        );

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            jobServiceStub,
            jobRunStub,
            processorStub,
            new QueuesFlowProducerService()
        );

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>{ executableJob }, null);

        // Assert
        Assert.areEqual(1, jobServiceProvider.getCallCount('applyFailure'));
        List<Object> applyFailureArgs = jobServiceProvider.getLastArgs('applyFailure');
        Assert.areEqual('Do not retry.', (String) applyFailureArgs[1]);
        Assert.areEqual(true, (Boolean) applyFailureArgs[3]);
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRuns'));
        JobRun__c run = ((List<JobRun__c>) runServiceProvider.getLastArgs('insertJobRuns')[0])[0];
        Assert.areEqual(Queues.RunStatus.FAILED.name(), run.Status__c);
        Assert.areEqual('Do not retry.', run.Message__c);
    }

    @IsTest
    static void should_skip_batch_finalize_when_job_lease_changes_before_completion() {
        // Arrange
        Job__c claimedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        claimedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 31);
        claimedJob.LeaseToken__c = 'lease-token';
        claimedJob.LeaseExpiresAt__c = System.now().addMinutes(5);
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();

        Job__c reloadedJob = (Job__c) QueuesSupport.clone(claimedJob);
        Job__c stolenJob = (Job__c) QueuesSupport.clone(claimedJob);
        stolenJob.LeaseToken__c = 'different-lease-token';

        Queues.ProcessResult result = new Queues.JobContext(claimedJob)
            .complete(new Map<String, Object>{ 'status' => 'done' });

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobs', new List<Job__c>{ reloadedJob })
            .withReturn('queryJobsForUpdate', new List<Job__c>{ stolenJob });
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            jobServiceProvider
        );
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'getProcessorsByClassAsSystem',
                new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
            )
            .withReturn('instantiate', 'processor')
            .withReturn('execute', result);
        QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
            QueuesProcessorService.class,
            processorProvider
        );
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesJobRunService jobRunStub = (QueuesJobRunService) Test.createStub(
            QueuesJobRunService.class,
            runServiceProvider
        );

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            jobServiceStub,
            jobRunStub,
            processorStub,
            new QueuesFlowProducerService()
        );

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>{ claimedJob }, null);

        // Assert
        Assert.areEqual(1, processorProvider.getCallCount('execute'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRuns'));
        Assert.areEqual(0, ((List<Job__c>) jobServiceProvider.getLastArgs('updateJobs')[0]).size());
        Assert.areEqual(0, ((List<JobRun__c>) runServiceProvider.getLastArgs('insertJobRuns')[0]).size());
    }

    @IsTest
    static void should_execute_single_claimed_job_and_record_async_run_context_when_job_is_owned() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 123);
        Job__c claimedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        claimedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 41);
        claimedJob.LeaseToken__c = 'lease-token';
        claimedJob.LeaseExpiresAt__c = System.now().addMinutes(5);
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();

        Job__c currentJob = (Job__c) QueuesSupport.clone(claimedJob);
        Queues.ProcessResult result = new Queues.JobContext(claimedJob)
            .defer(new Map<String, Object>{ 'cursor' => 5 }, 2);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsAsSystem', new List<Job__c>{ claimedJob })
            .withReturn('queryJobsForUpdateAsSystem', new List<Job__c>{ currentJob });
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'getProcessorsByClassAsSystem',
                new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
            )
            .withReturn('instantiate', 'processor')
            .withReturn('execute', result);
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(QueuesProcessorService.class, processorProvider),
            new QueuesFlowProducerService()
        );

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>{ claimedJob }, asyncApexJobId);

        // Assert
        Job__c updatedJob = ((List<Job__c>) jobServiceProvider.getLastArgs('updateJobsAsSystem')[0])[0];
        Assert.areEqual(currentJob.Id, updatedJob.Id);
        Assert.areEqual(1, processorProvider.getCallCount('instantiate'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('applyResult'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('queryJobsForUpdateAsSystem'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobsAsSystem'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRunsAsSystem'));
        JobRun__c run = ((List<JobRun__c>) runServiceProvider.getLastArgs('insertJobRunsAsSystem')[0])[0];
        Assert.areEqual(asyncApexJobId, run.AsyncApexJobId__c);
        Assert.areEqual(Queues.RunStatus.DEFERRED.name(), run.Status__c);
    }

    @IsTest
    static void should_return_single_execution_snapshot_when_job_disappears_before_finalize() {
        // Arrange
        Job__c claimedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        claimedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 48);
        claimedJob.LeaseToken__c = 'lease-token';
        claimedJob.LeaseExpiresAt__c = System.now().addMinutes(5);
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();

        Queues.ProcessResult result = new Queues.JobContext(claimedJob)
            .complete(new Map<String, Object>{ 'status' => 'done' });

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsAsSystem', new List<Job__c>{ claimedJob })
            .withReturn('queryJobsForUpdateAsSystem', new List<Job__c>());
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'getProcessorsByClassAsSystem',
                new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
            )
            .withReturn('instantiate', 'processor')
            .withReturn('execute', result);
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(QueuesProcessorService.class, processorProvider),
            new QueuesFlowProducerService()
        );

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>{ claimedJob }, null);

        // Assert
        Assert.areEqual(1, processorProvider.getCallCount('execute'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobsAsSystem'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRunsAsSystem'));
        Assert.areEqual(0, ((List<Job__c>) jobServiceProvider.getLastArgs('updateJobsAsSystem')[0]).size());
        Assert.areEqual(0, ((List<JobRun__c>) runServiceProvider.getLastArgs('insertJobRunsAsSystem')[0]).size());
    }

    @IsTest
    static void should_record_failed_and_paused_run_statuses_when_processor_returns_supported_noncompletion_outcomes() {
        // Arrange
        Job__c pausedClaimedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        pausedClaimedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 43);
        pausedClaimedJob.LeaseToken__c = 'lease-token-paused';
        pausedClaimedJob.LeaseExpiresAt__c = System.now().addMinutes(5);
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();

        Job__c failedClaimedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        failedClaimedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 44);
        failedClaimedJob.LeaseToken__c = 'lease-token-failed';
        failedClaimedJob.LeaseExpiresAt__c = System.now().addMinutes(5);

        Job__c pausedCurrentJob = (Job__c) QueuesSupport.clone(pausedClaimedJob);
        Job__c failedCurrentJob = (Job__c) QueuesSupport.clone(failedClaimedJob);

        Queues.ProcessResult pausedResult = new Queues.JobContext(pausedClaimedJob)
            .pause(new Map<String, Object>{ 'cursor' => 8 });
        Queues.ProcessResult failedResult = new Queues.JobContext(failedClaimedJob).fail('processor failed');

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsAsSystem', new List<Job__c>{ pausedClaimedJob, failedClaimedJob })
            .withReturn('queryJobsForUpdateAsSystem', new List<Job__c>{ pausedCurrentJob, failedCurrentJob });
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'getProcessorsByClassAsSystem',
                new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
            )
            .withReturn('instantiate', 'processor')
            .withReturns('execute', new List<Object>{ pausedResult, failedResult });
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(QueuesProcessorService.class, processorProvider),
            new QueuesFlowProducerService()
        );

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>{ pausedClaimedJob, failedClaimedJob }, null);

        // Assert
        List<List<Object>> insertCalls = runServiceProvider.getCalls('insertJobRunsAsSystem');
        List<JobRun__c> runs = (List<JobRun__c>) insertCalls[0][0];
        JobRun__c pausedRun = runs[0];
        JobRun__c failedRun = runs[1];
        Assert.areEqual(1, insertCalls.size());
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobsAsSystem'));
        Assert.areEqual(Queues.RunStatus.PAUSED.name(), pausedRun.Status__c);
        Assert.areEqual(Queues.RunStatus.FAILED.name(), failedRun.Status__c);
    }

    @IsTest
    static void should_mark_job_failed_when_apply_result_throws_during_finalize() {
        // Arrange
        Job__c executableJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        executableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 42);
        executableJob.LeaseToken__c = 'lease-token';
        executableJob.LeaseExpiresAt__c = System.now().addMinutes(5);
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();

        Queues.ProcessResult result = new Queues.JobContext(executableJob)
            .complete(new Map<String, Object>{ 'status' => 'done' });

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobs', new List<Job__c>{ executableJob })
            .withReturn('queryJobsForUpdate', new List<Job__c>{ executableJob })
            .withException('applyResult', new Queues.QueueException('Result exploded.'));
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'getProcessorsByClassAsSystem',
                new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
            )
            .withReturn('instantiate', 'processor')
            .withReturn('execute', result);
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(QueuesProcessorService.class, processorProvider),
            new QueuesFlowProducerService()
        );

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>{ executableJob }, null);

        // Assert
        Assert.areEqual(1, jobServiceProvider.getCallCount('applyFailure'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRuns'));
        Assert.areEqual('Result exploded.', (String) jobServiceProvider.getLastArgs('applyFailure')[1]);
    }

    @IsTest
    static void should_fail_job_when_processor_returns_malformed_result() {
        // Arrange
        Job__c claimedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        claimedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 45);
        claimedJob.LeaseToken__c = 'lease-token';
        claimedJob.LeaseExpiresAt__c = System.now().addMinutes(5);
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();

        Job__c currentJob = (Job__c) QueuesSupport.clone(claimedJob);
        Queues.ProcessResult malformedResult = (Queues.ProcessResult) JSON.deserialize(
            '{}',
            Queues.ProcessResult.class
        );

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsAsSystem', new List<Job__c>{ claimedJob })
            .withReturn('queryJobsForUpdateAsSystem', new List<Job__c>{ currentJob });
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'getProcessorsByClassAsSystem',
                new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
            )
            .withReturn('instantiate', 'processor')
            .withReturn('execute', malformedResult);
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(QueuesProcessorService.class, processorProvider),
            new QueuesFlowProducerService()
        );

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>{ claimedJob }, null);

        // Assert
        Assert.areEqual(1, jobServiceProvider.getCallCount('applyFailure'));
        Assert.areEqual(
            'Unsupported processor outcome "null".',
            (String) jobServiceProvider.getLastArgs('applyFailure')[1]
        );
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobsAsSystem'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRunsAsSystem'));
        JobRun__c run = ((List<JobRun__c>) runServiceProvider.getLastArgs('insertJobRunsAsSystem')[0])[0];
        Assert.areEqual(Queues.RunStatus.FAILED.name(), run.Status__c);
    }

    @IsTest
    static void should_fail_job_when_processor_metadata_is_missing() {
        // Arrange
        Job__c claimedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        claimedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 46);
        claimedJob.LeaseToken__c = 'lease-token';
        claimedJob.LeaseExpiresAt__c = System.now().addMinutes(5);

        Job__c currentJob = (Job__c) QueuesSupport.clone(claimedJob);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsAsSystem', new List<Job__c>{ claimedJob })
            .withReturn('queryJobsForUpdateAsSystem', new List<Job__c>{ currentJob });
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('getProcessorsByClassAsSystem', new Map<String, Processor__mdt>());
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(QueuesProcessorService.class, processorProvider),
            new QueuesFlowProducerService()
        );

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>{ claimedJob }, null);

        // Assert
        Assert.areEqual(1, jobServiceProvider.getCallCount('applyFailure'));
        Assert.areEqual(
            'Processor "' + claimedJob.ProcessorClass__c + '" does not have a processor metadata mapping.',
            (String) jobServiceProvider.getLastArgs('applyFailure')[1]
        );
        Assert.areEqual(0, processorProvider.getCallCount('instantiate'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobsAsSystem'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRunsAsSystem'));
    }

    @IsTest
    static void should_fail_job_when_processor_metadata_is_inactive() {
        // Arrange
        Job__c claimedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        claimedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 47);
        claimedJob.LeaseToken__c = 'lease-token';
        claimedJob.LeaseExpiresAt__c = System.now().addMinutes(5);

        Job__c currentJob = (Job__c) QueuesSupport.clone(claimedJob);
        Processor__mdt inactiveProcessorDefinition = QueuesTestSupport.processorDefinition(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            QueuesTestSupport.DEFAULT_JOB_NAME,
            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
            false
        );

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobsAsSystem', new List<Job__c>{ claimedJob })
            .withReturn('queryJobsForUpdateAsSystem', new List<Job__c>{ currentJob });
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'getProcessorsByClassAsSystem',
                new Map<String, Processor__mdt>{
                    QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => inactiveProcessorDefinition
                }
            );
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(QueuesProcessorService.class, processorProvider),
            new QueuesFlowProducerService()
        );

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>{ claimedJob }, null);

        // Assert
        Assert.areEqual(1, jobServiceProvider.getCallCount('applyFailure'));
        Assert.areEqual(
            'Processor "' + claimedJob.ProcessorClass__c + '" has an inactive processor metadata mapping.',
            (String) jobServiceProvider.getLastArgs('applyFailure')[1]
        );
        Assert.areEqual(0, processorProvider.getCallCount('instantiate'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobsAsSystem'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRunsAsSystem'));
    }

    @IsTest
    static void should_ignore_empty_batch_scope_when_no_claimed_jobs_are_provided() {
        // Arrange
        QueuesJobExecutionService service = new QueuesJobExecutionService();

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>(), null);

        // Assert
        Assert.areEqual(true, true);
    }

    @IsTest
    static void should_bulk_finalize_batch_jobs_and_isolate_handleable_job_failures() {
        // Arrange
        Job__c completedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        completedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 51);
        completedJob.LeaseToken__c = 'lease-token-51';
        completedJob.LeaseExpiresAt__c = System.now().addMinutes(5);
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();

        Job__c malformedJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        malformedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 52);
        malformedJob.LeaseToken__c = 'lease-token-52';
        malformedJob.LeaseExpiresAt__c = System.now().addMinutes(5);

        Job__c completedCurrentJob = (Job__c) QueuesSupport.clone(completedJob);
        Job__c malformedCurrentJob = (Job__c) QueuesSupport.clone(malformedJob);

        Queues.ProcessResult completedResult = new Queues.JobContext(completedJob)
            .complete(new Map<String, Object>{ 'status' => 'done' });
        Queues.ProcessResult malformedResult = (Queues.ProcessResult) JSON.deserialize(
            '{}',
            Queues.ProcessResult.class
        );

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryJobs', new List<Job__c>{ completedJob, malformedJob })
            .withReturn('queryJobsForUpdate', new List<Job__c>{ completedCurrentJob, malformedCurrentJob });
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'getProcessorsByClassAsSystem',
                new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
            )
            .withReturn('instantiate', 'processor')
            .withReturns('execute', new List<Object>{ completedResult, malformedResult });
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(QueuesProcessorService.class, processorProvider),
            new QueuesFlowProducerService()
        );

        // Act
        service.executeClaimedBatchJobs(new List<Job__c>{ completedJob, malformedJob }, null);

        // Assert
        Assert.areEqual(2, processorProvider.getCallCount('instantiate'));
        Assert.areEqual(2, processorProvider.getCallCount('execute'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('queryJobsForUpdate'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRuns'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('applyFailure'));

        List<Job__c> updatedJobs = (List<Job__c>) jobServiceProvider.getLastArgs('updateJobs')[0];
        Assert.areEqual(2, updatedJobs.size());

        List<JobRun__c> runs = (List<JobRun__c>) runServiceProvider.getLastArgs('insertJobRuns')[0];
        Assert.areEqual(2, runs.size());
        Assert.areEqual(Queues.RunStatus.COMPLETED.name(), runs[0].Status__c);
        Assert.areEqual(Queues.RunStatus.FAILED.name(), runs[1].Status__c);
    }

    @IsTest
    static void should_execute_queueable_job_when_async_owner_matches() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 701);
        Job__c queueableJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        queueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 701);
        queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
        queueableJob.AsyncApexJobId__c = asyncApexJobId;

        Job__c currentJob = (Job__c) QueuesSupport.clone(queueableJob);
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
        Queues.ProcessResult result = new Queues.JobContext(queueableJob)
            .complete(new Map<String, Object>{ 'status' => 'done' });

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', queueableJob)
            .withReturn('queryJobsForUpdate', new List<Job__c>{ currentJob });
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'getProcessorsByClassAsSystem',
                new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
            )
            .withReturn('instantiate', 'processor')
            .withReturn('execute', result);
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(QueuesProcessorService.class, processorProvider),
            new QueuesFlowProducerService()
        );

        // Act
        service.executeQueueableJob(queueableJob.Id, asyncApexJobId);

        // Assert
        Assert.areEqual(1, processorProvider.getCallCount('execute'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('applyResult'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRuns'));
        JobRun__c run = ((List<JobRun__c>) runServiceProvider.getLastArgs('insertJobRuns')[0])[0];
        Assert.areEqual(asyncApexJobId, run.AsyncApexJobId__c);
        Assert.areEqual(Queues.RunStatus.COMPLETED.name(), run.Status__c);
    }

    @IsTest
    static void should_pause_queueable_job_when_pause_is_requested_during_execution_finalization() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 709);
            Job__c queueableJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            queueableJob.AsyncApexJobId__c = asyncApexJobId;
            queueableJob.AttemptsMade__c = 0;
            queueableJob.MaxAttempts__c = 3;
            queueableJob.PauseRequested__c = false;
            insert queueableJob;

            ConcurrentPauseProcessorProvider processorProvider = new ConcurrentPauseProcessorProvider();
            QueuesJobExecutionService service = new QueuesJobExecutionService(
                new QueuesJobService(),
                new QueuesJobRunService(),
                (QueuesProcessorService) Test.createStub(QueuesProcessorService.class, processorProvider),
                new QueuesFlowProducerService()
            );

            // Act
            Job__c result = service.executeQueueableJob(queueableJob.Id, asyncApexJobId);

            // Assert
            Job__c updatedJob = [
                SELECT State__c, PauseRequested__c, AsyncApexJobId__c, AvailableAt__c
                FROM Job__c
                WHERE Id = :queueableJob.Id
            ];
            JobRun__c run = [
                SELECT Status__c, AsyncApexJobId__c
                FROM JobRun__c
                WHERE Job__c = :queueableJob.Id
                LIMIT 1
            ];

            Assert.areEqual(1, processorProvider.executeCount);
            Assert.areEqual(Queues.State.PAUSED.name(), result.State__c);
            Assert.areEqual(Queues.State.PAUSED.name(), updatedJob.State__c);
            Assert.areEqual(false, updatedJob.PauseRequested__c);
            Assert.areEqual(null, updatedJob.AsyncApexJobId__c);
            Assert.isTrue(updatedJob.AvailableAt__c > System.now());
            Assert.areEqual(Queues.RunStatus.DEFERRED.name(), run.Status__c);
            Assert.areEqual(asyncApexJobId, run.AsyncApexJobId__c);
        }
    }

    @IsTest
    static void should_skip_queueable_job_when_async_owner_mismatches() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 702);
        Job__c queueableJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        queueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 702);
        queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
        queueableJob.AsyncApexJobId__c = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 703);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', queueableJob);
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(QueuesProcessorService.class, processorProvider),
            new QueuesFlowProducerService()
        );

        // Act
        Job__c result = service.executeQueueableJob(queueableJob.Id, asyncApexJobId);

        // Assert
        Assert.areEqual(queueableJob.Id, result.Id);
        Assert.areEqual(0, processorProvider.getCallCount('execute'));
        Assert.areEqual(0, jobServiceProvider.getCallCount('queryJobsForUpdate'));
        Assert.areEqual(0, runServiceProvider.getCallCount('insertJobRuns'));
    }

    @IsTest
    static void should_skip_queueable_finalize_when_async_owner_changes_before_completion() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 704);
        Job__c queueableJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        queueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 704);
        queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
        queueableJob.AsyncApexJobId__c = asyncApexJobId;

        Job__c stolenJob = (Job__c) QueuesSupport.clone(queueableJob);
        stolenJob.AsyncApexJobId__c = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 705);

        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
        Queues.ProcessResult result = new Queues.JobContext(queueableJob)
            .complete(new Map<String, Object>{ 'status' => 'done' });

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', queueableJob)
            .withReturn('queryJobsForUpdate', new List<Job__c>{ stolenJob });
        QueuesTestSupport.RecordingStubProvider processorProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'getProcessorsByClassAsSystem',
                new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
            )
            .withReturn('instantiate', 'processor')
            .withReturn('execute', result);
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(QueuesProcessorService.class, processorProvider),
            new QueuesFlowProducerService()
        );

        // Act
        service.executeQueueableJob(queueableJob.Id, asyncApexJobId);

        // Assert
        Assert.areEqual(1, processorProvider.getCallCount('execute'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRuns'));
        Assert.areEqual(0, ((List<Job__c>) jobServiceProvider.getLastArgs('updateJobs')[0]).size());
        Assert.areEqual(0, ((List<JobRun__c>) runServiceProvider.getLastArgs('insertJobRuns')[0]).size());
    }

    @IsTest
    static void should_mark_owned_queueable_job_failed_from_finalizer_failure() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 706);
        Job__c queueableJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        queueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 706);
        queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
        queueableJob.AsyncApexJobId__c = asyncApexJobId;
        queueableJob.AttemptsMade__c = 1;

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobForUpdate', queueableJob);
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider flowProducerProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(
                QueuesProcessorService.class,
                new QueuesTestSupport.RecordingStubProvider()
            ),
            (QueuesFlowProducerService) Test.createStub(QueuesFlowProducerService.class, flowProducerProvider)
        );

        // Act
        service.failQueueableJob(queueableJob.Id, asyncApexJobId, 'executor exploded');

        // Assert
        Assert.areEqual(2, queueableJob.AttemptsMade__c);
        Assert.areEqual(1, jobServiceProvider.getCallCount('applyFailure'));
        Assert.areEqual('executor exploded', (String) jobServiceProvider.getLastArgs('applyFailure')[1]);
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJob'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRuns'));
        Assert.areEqual(1, flowProducerProvider.getCallCount('deferParentDependenciesAsSystem'));
        Assert.areEqual(0, flowProducerProvider.getCallCount('resolveParentDependenciesAsSystem'));
        JobRun__c run = ((List<JobRun__c>) runServiceProvider.getLastArgs('insertJobRuns')[0])[0];
        Assert.areEqual(2, run.AttemptNumber__c);
        Assert.areEqual(asyncApexJobId, run.AsyncApexJobId__c);
        Assert.areEqual(Queues.RunStatus.FAILED.name(), run.Status__c);
    }

    @IsTest
    static void should_mark_owned_invocable_job_failed_from_flow_failure() {
        // Arrange
        String leaseToken = 'lease-token';
        Job__c stolenJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        stolenJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 709);
        stolenJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
        stolenJob.LeaseToken__c = 'other-lease-token';

        Job__c invocableJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        invocableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 710);
        invocableJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
        invocableJob.LeaseToken__c = leaseToken;
        invocableJob.AttemptsMade__c = 1;

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns('requireJobForUpdate', new List<Object>{ stolenJob, invocableJob });
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(
                QueuesProcessorService.class,
                new QueuesTestSupport.RecordingStubProvider()
            ),
            new QueuesFlowProducerService()
        );

        // Act
        Job__c skippedJob = service.failInvocableJob(stolenJob.Id, leaseToken, 'ignored');
        Job__c failedJob = service.failInvocableJob(invocableJob.Id, leaseToken, 'flow exploded');

        // Assert
        Assert.areEqual(stolenJob.Id, skippedJob.Id);
        Assert.areEqual(invocableJob.Id, failedJob.Id);
        Assert.areEqual(2, invocableJob.AttemptsMade__c);
        Assert.areEqual(1, jobServiceProvider.getCallCount('applyFailure'));
        Assert.areEqual('flow exploded', (String) jobServiceProvider.getLastArgs('applyFailure')[1]);
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJob'));
        Assert.areEqual(1, runServiceProvider.getCallCount('insertJobRuns'));
        JobRun__c run = ((List<JobRun__c>) runServiceProvider.getLastArgs('insertJobRuns')[0])[0];
        Assert.areEqual(2, run.AttemptNumber__c);
        Assert.areEqual(null, run.AsyncApexJobId__c);
        Assert.areEqual(Queues.RunStatus.FAILED.name(), run.Status__c);
    }

    @IsTest
    static void should_ignore_queueable_finalizer_failure_when_async_owner_mismatches() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 707);
        Job__c queueableJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        queueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 707);
        queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
        queueableJob.AsyncApexJobId__c = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 708);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobForUpdate', queueableJob);
        QueuesTestSupport.RecordingStubProvider runServiceProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobExecutionService service = new QueuesJobExecutionService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, runServiceProvider),
            (QueuesProcessorService) Test.createStub(
                QueuesProcessorService.class,
                new QueuesTestSupport.RecordingStubProvider()
            ),
            new QueuesFlowProducerService()
        );

        // Act
        service.failQueueableJob(queueableJob.Id, asyncApexJobId, 'ignored');

        // Assert
        Assert.areEqual(0, jobServiceProvider.getCallCount('applyFailure'));
        Assert.areEqual(0, jobServiceProvider.getCallCount('updateJob'));
        Assert.areEqual(0, runServiceProvider.getCallCount('insertJobRuns'));
    }

    private class ConcurrentPauseProcessorProvider implements System.StubProvider {
        private Integer executeCount = 0;

        public Object handleMethodCall(
            final Object stubbedObject,
            final String stubbedMethodName,
            final Type returnType,
            final List<Type> listOfParamTypes,
            final List<String> listOfParamNames,
            final List<Object> listOfArgs
        ) {
            if (stubbedMethodName == 'getProcessorsByClassAsSystem') {
                return new Map<String, Processor__mdt>{
                    QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => QueuesTestSupport.processorDefinition()
                };
            }
            if (stubbedMethodName == 'instantiate') {
                return 'processor';
            }
            if (stubbedMethodName == 'execute') {
                executeCount++;
                Job__c job = (Job__c) listOfArgs[1];
                update new Job__c(Id = job.Id, PauseRequested__c = true);
                return new Queues.JobContext(job).defer(new Map<String, Object>{ 'cursor' => 1 }, 5);
            }
            return null;
        }
    }
}
