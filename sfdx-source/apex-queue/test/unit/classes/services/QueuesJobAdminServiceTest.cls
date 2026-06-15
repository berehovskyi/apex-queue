@IsTest(IsParallel=true)
private class QueuesJobAdminServiceTest {
    @IsTest
    static void should_construct_default_service_and_read_job_state() {
        // Arrange
        Job__c waitingJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        waitingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 29);
        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(
                QueuesJobService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireJob', waitingJob)
            ),
            new QueuesRuntimeService(),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            new QueuesWorkerService()
        );

        // Act
        QueuesJobAdminService defaultService = new QueuesJobAdminService();
        Queues.State state = service.getJobState(waitingJob.Id);

        // Assert
        Assert.areNotEqual(null, defaultService);
        Assert.areEqual(Queues.State.WAITING, state);
    }

    @IsTest
    static void should_resume_paused_job_when_resuming_job() {
        // Arrange
        Datetime currentNow = System.now();
        Job__c pausedJob = QueuesTestSupport.job(Queues.State.PAUSED.name());
        pausedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 1);
        pausedJob.AvailableAt__c = currentNow.addMinutes(-1);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(pausedJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', pausedJob)
            .withReturn('requireJobForUpdate', pausedJob)
            .withReturn('updateJob', pausedJob);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            jobServiceProvider
        );
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureWorkerForNextWakeAt', true);
        QueuesWorkerService workerStub = (QueuesWorkerService) Test.createStub(
            QueuesWorkerService.class,
            workerProvider
        );

        QueuesJobAdminService service = new QueuesJobAdminService(
            jobServiceStub,
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            workerStub
        );

        // Act
        Job__c resumedJob = service.resumeJob(pausedJob.Id);

        // Assert
        Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(resumedJob.State__c));
        Assert.areEqual(1, workerProvider.getCallCount('ensureWorkerForNextWakeAt'));
    }

    @IsTest
    static void should_enqueue_queueable_jobs_when_admin_actions_make_them_runnable() {
        // Arrange
        Datetime currentNow = System.now();
        Job__c pausedJob = QueuesTestSupport.job(Queues.State.PAUSED.name());
        pausedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 21);
        pausedJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
        pausedJob.AvailableAt__c = currentNow;
        Job__c failedJob = QueuesTestSupport.job(Queues.State.FAILED.name());
        failedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 22);
        failedJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
        Job__c delayedJob = QueuesTestSupport.job(Queues.State.DELAYED.name());
        delayedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 23);
        delayedJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
        delayedJob.AvailableAt__c = currentNow.addMinutes(2);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(pausedJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns('requireJob', new List<Object>{ pausedJob, failedJob, delayedJob })
            .withReturns('requireJobForUpdate', new List<Object>{ pausedJob, failedJob, delayedJob })
            .withReturns('updateJob', new List<Object>{ pausedJob, failedJob, delayedJob });
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider queueableExecutionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns('enqueueJob', new List<Object>{ pausedJob, failedJob, delayedJob });

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesQueueableExecutionService) Test.createStub(
                QueuesQueueableExecutionService.class,
                queueableExecutionProvider
            ),
            new QueuesFlowProducerService(),
            (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
        );

        // Act
        service.resumeJob(pausedJob.Id);
        service.retryJob(failedJob.Id);
        service.promoteJob(delayedJob.Id);

        // Assert
        Assert.areEqual(3, queueableExecutionProvider.getCallCount('enqueueJob'));
        Assert.areEqual(pausedJob.Id, queueableExecutionProvider.getCalls('enqueueJob')[0][0]);
        Assert.areEqual(failedJob.Id, queueableExecutionProvider.getCalls('enqueueJob')[1][0]);
        Assert.areEqual(delayedJob.Id, queueableExecutionProvider.getCalls('enqueueJob')[2][0]);
        Assert.areEqual(0, workerProvider.getCallCount('ensureWorkerForNextWakeAt'));
        Assert.areEqual(0, workerProvider.getCallCount('ensureWorker'));
    }

    @IsTest
    static void should_leave_invocable_admin_runnable_transitions_to_flow_handoff() {
        // Arrange
        Datetime currentNow = System.now();
        Job__c pausedJob = QueuesTestSupport.job(Queues.State.PAUSED.name());
        pausedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 32);
        pausedJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
        pausedJob.AvailableAt__c = currentNow;
        Job__c failedJob = QueuesTestSupport.job(Queues.State.FAILED.name());
        failedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 33);
        failedJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
        Job__c delayedJob = QueuesTestSupport.job(Queues.State.DELAYED.name());
        delayedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 34);
        delayedJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
        delayedJob.AvailableAt__c = currentNow.addMinutes(2);
        Job__c deferredJob = QueuesTestSupport.job(Queues.State.PAUSED.name());
        deferredJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 35);
        deferredJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(pausedJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns('requireJob', new List<Object>{ pausedJob, failedJob, delayedJob, deferredJob })
            .withReturns('requireJobForUpdate', new List<Object>{ pausedJob, failedJob, delayedJob, deferredJob })
            .withReturns('updateJob', new List<Object>{ pausedJob, failedJob, delayedJob, deferredJob });
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider queueableExecutionProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesQueueableExecutionService) Test.createStub(
                QueuesQueueableExecutionService.class,
                queueableExecutionProvider
            ),
            new QueuesFlowProducerService(),
            (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
        );

        // Act
        Job__c resumedJob = service.resumeJob(pausedJob.Id);
        Job__c retriedJob = service.retryJob(failedJob.Id);
        Job__c promotedJob = service.promoteJob(delayedJob.Id);
        Job__c deferredResult = service.deferJob(deferredJob.Id, 5);

        // Assert
        Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(resumedJob.State__c));
        Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(retriedJob.State__c));
        Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(promotedJob.State__c));
        Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(deferredResult.State__c));
        Assert.areEqual(0, queueableExecutionProvider.getCallCount('enqueueJob'));
        Assert.areEqual(0, workerProvider.getCallCount('ensureWorkerForNextWakeAt'));
        Assert.areEqual(0, workerProvider.getCallCount('ensureWorker'));
        Assert.areEqual(0, workerProvider.getCallCount('syncWorkerForNextWakeAt'));
    }

    @IsTest
    static void should_pause_active_job_and_resume_delayed_job_when_job_state_changes() {
        // Arrange
        Datetime currentNow = System.now();
        Job__c activeJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        activeJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 2);
        activeJob.PauseRequested__c = false;
        Job__c pausedDelayedJob = QueuesTestSupport.job(Queues.State.PAUSED.name());
        pausedDelayedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 3);
        pausedDelayedJob.AvailableAt__c = currentNow.addMinutes(5);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(activeJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns('requireJob', new List<Object>{ activeJob, pausedDelayedJob })
            .withReturns('requireJobForUpdate', new List<Object>{ activeJob, pausedDelayedJob })
            .withReturns('updateJob', new List<Object>{ activeJob, pausedDelayedJob });
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureWorkerForNextWakeAt', true);

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
        );

        // Act
        Job__c pausedJob = service.pauseJob(activeJob.Id);
        Job__c resumedJob = service.resumeJob(pausedDelayedJob.Id);

        // Assert
        Assert.areEqual(true, pausedJob.PauseRequested__c);
        Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(resumedJob.State__c));
        Assert.areEqual(1, workerProvider.getCallCount('ensureWorkerForNextWakeAt'));
    }

    @IsTest
    static void should_pause_waiting_job_and_sync_future_worker_plan() {
        // Arrange
        Job__c waitingJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        waitingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 6);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(waitingJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', waitingJob)
            .withReturn('requireJobForUpdate', waitingJob)
            .withReturn('updateJob', waitingJob);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('syncWorkerForNextWakeAt', true);

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
        );

        // Act
        Job__c pausedJob = service.pauseJob(waitingJob.Id);

        // Assert
        Assert.areEqual(Queues.State.PAUSED, QueuesSupport.toState(pausedJob.State__c));
        Assert.areEqual(false, pausedJob.PauseRequested__c);
        Assert.areEqual(1, workerProvider.getCallCount('syncWorkerForNextWakeAt'));
    }

    @IsTest
    static void should_defer_job_and_sync_future_worker_plan() {
        // Arrange
        Job__c waitingJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        waitingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 7);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(waitingJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', waitingJob)
            .withReturn('requireJobForUpdate', waitingJob)
            .withReturn('updateJob', waitingJob);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('syncWorkerForNextWakeAt', true);

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
        );

        Datetime deferBaseline = System.now();

        // Act
        Job__c deferredJob = service.deferJob(waitingJob.Id, 15);

        // Assert
        Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(deferredJob.State__c));
        Assert.isTrue(deferredJob.AvailableAt__c > deferBaseline.addMinutes(14));
        Assert.areEqual(1, workerProvider.getCallCount('syncWorkerForNextWakeAt'));
    }

    @IsTest
    static void should_defer_queueable_job_and_enqueue_executor_attempt() {
        // Arrange
        Job__c queueableJob = QueuesTestSupport.job(Queues.State.PAUSED.name());
        queueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 24);
        queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(queueableJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', queueableJob)
            .withReturn('requireJobForUpdate', queueableJob)
            .withReturn('updateJob', queueableJob);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider queueableExecutionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('enqueueJob', queueableJob);

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesQueueableExecutionService) Test.createStub(
                QueuesQueueableExecutionService.class,
                queueableExecutionProvider
            ),
            new QueuesFlowProducerService(),
            (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
        );

        Datetime deferBaseline = System.now();

        // Act
        Job__c deferredJob = service.deferJob(queueableJob.Id, 5);

        // Assert
        Assert.areEqual(Queues.State.DELAYED, QueuesSupport.toState(deferredJob.State__c));
        Assert.isTrue(deferredJob.AvailableAt__c > deferBaseline.addMinutes(4));
        Assert.areEqual(1, queueableExecutionProvider.getCallCount('enqueueJob'));
        Assert.areEqual(queueableJob.Id, queueableExecutionProvider.getLastArgs('enqueueJob')[0]);
        Assert.areEqual(0, workerProvider.getCallCount('syncWorkerForNextWakeAt'));
        Assert.areEqual(0, workerProvider.getCallCount('ensureWorkerForNextWakeAt'));
    }

    @IsTest
    static void should_not_wake_worker_when_queueable_admin_mutations_do_not_make_job_runnable() {
        // Arrange
        Job__c waitingJobToPause = QueuesTestSupport.job(Queues.State.WAITING.name());
        waitingJobToPause.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 26);
        waitingJobToPause.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
        Job__c delayedJobToPrioritize = QueuesTestSupport.job(Queues.State.DELAYED.name());
        delayedJobToPrioritize.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 27);
        delayedJobToPrioritize.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
        Job__c delayedJobToRemove = QueuesTestSupport.job(Queues.State.DELAYED.name());
        delayedJobToRemove.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 28);
        delayedJobToRemove.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(waitingJobToPause.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns(
                'requireJob',
                new List<Object>{ waitingJobToPause, delayedJobToPrioritize, delayedJobToRemove }
            )
            .withReturns(
                'requireJobForUpdate',
                new List<Object>{ waitingJobToPause, delayedJobToPrioritize, delayedJobToRemove }
            )
            .withReturns('updateJob', new List<Object>{ waitingJobToPause, delayedJobToPrioritize });
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
        );

        // Act
        Job__c pausedJob = service.pauseJob(waitingJobToPause.Id);
        Job__c reprioritizedJob = service.changePriority(delayedJobToPrioritize.Id, 9);
        service.removeJob(delayedJobToRemove.Id);

        // Assert
        Assert.areEqual(Queues.State.PAUSED, QueuesSupport.toState(pausedJob.State__c));
        Assert.areEqual(9, QueuesSupport.toInteger(reprioritizedJob.Priority__c, 0));
        Assert.areEqual(1, jobServiceProvider.getCallCount('deleteJobs'));
        Assert.areEqual(0, workerProvider.getCallCount('syncWorkerForNextWakeAt'));
        Assert.areEqual(0, workerProvider.getCallCount('ensureWorkerForNextWakeAt'));
        Assert.areEqual(0, workerProvider.getCallCount('syncExistingWorkerForNextWakeAt'));
    }

    @IsTest
    static void should_wake_worker_when_worker_priority_or_promotion_changes_execution_plan() {
        // Arrange
        Job__c waitingJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        waitingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 30);
        Job__c delayedJob = QueuesTestSupport.job(Queues.State.DELAYED.name());
        delayedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 31);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(waitingJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns('requireJob', new List<Object>{ waitingJob, delayedJob })
            .withReturns('requireJobForUpdate', new List<Object>{ waitingJob, delayedJob })
            .withReturns('updateJob', new List<Object>{ waitingJob, delayedJob });
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureWorkerForNextWakeAt', true)
            .withReturn('ensureWorker', true);

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
        );

        // Act
        Job__c reprioritizedJob = service.changePriority(waitingJob.Id, 7);
        Job__c promotedJob = service.promoteJob(delayedJob.Id);

        // Assert
        Assert.areEqual(7, QueuesSupport.toInteger(reprioritizedJob.Priority__c, 0));
        Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(promotedJob.State__c));
        Assert.areEqual(1, workerProvider.getCallCount('ensureWorkerForNextWakeAt'));
        Assert.areEqual(1, workerProvider.getCallCount('ensureWorker'));
    }

    @IsTest
    static void should_reject_queueable_defer_delay_outside_native_window() {
        // Arrange
        Job__c queueableJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        queueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 25);
        queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(queueableJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', queueableJob)
            .withReturn('requireJobForUpdate', queueableJob);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider queueableExecutionProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesQueueableExecutionService) Test.createStub(
                QueuesQueueableExecutionService.class,
                queueableExecutionProvider
            ),
            new QueuesFlowProducerService(),
            (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
        );

        String failureMessage;

        // Act
        try {
            service.deferJob(queueableJob.Id, 11);
            Assert.fail('Expected queueable defer delay to be rejected.');
        } catch (Queues.QueueException ex) {
            failureMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual(QueuesQueueableTransportService.nativeQueueableDelayMessage(11), failureMessage);
        Assert.areEqual(0, jobServiceProvider.getCallCount('updateJob'));
        Assert.areEqual(0, queueableExecutionProvider.getCallCount('enqueueJob'));
        Assert.areEqual(0, workerProvider.getCallCount('syncWorkerForNextWakeAt'));
    }

    @IsTest
    static void should_remove_non_active_job_and_rely_on_master_detail_run_cascade() {
        // Arrange
        Job__c waitingJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        waitingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 4);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(waitingJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', waitingJob)
            .withReturn('requireJobForUpdate', waitingJob);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('syncExistingWorkerForNextWakeAt', true);

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
        );

        // Act
        service.removeJob(waitingJob.Id);

        // Assert
        Assert.areEqual(1, jobServiceProvider.getCallCount('deleteJobs'));
        Assert.areEqual(1, workerProvider.getCallCount('syncExistingWorkerForNextWakeAt'));
    }

    @IsTest
    static void should_cancel_non_active_job_and_preserve_runs() {
        // Arrange
        Job__c waitingJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        waitingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 32);
        waitingJob.LeaseToken__c = 'lease-token';
        waitingJob.LeaseExpiresAt__c = System.now().addMinutes(5);
        waitingJob.AsyncApexJobId__c = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 32);
        waitingJob.PauseRequested__c = true;
        QueueRuntime__c runtime = QueuesTestSupport.runtime(waitingJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', waitingJob)
            .withReturn('requireJobForUpdate', waitingJob)
            .withReturn('updateJob', waitingJob);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('syncExistingWorkerForNextWakeAt', true);

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
        );

        // Act
        Job__c canceledJob = service.cancelJob(waitingJob.Id);

        // Assert
        Assert.areEqual(Queues.State.CANCELED.name(), canceledJob.State__c);
        Assert.areEqual('Canceled by job cancel.', canceledJob.FailedReason__c);
        Assert.areEqual(null, canceledJob.LeaseToken__c);
        Assert.areEqual(null, canceledJob.LeaseExpiresAt__c);
        Assert.areEqual(null, canceledJob.AsyncApexJobId__c);
        Assert.areEqual(false, canceledJob.PauseRequested__c);
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJob'));
        Assert.areEqual(0, jobServiceProvider.getCallCount('deleteJobs'));
        Assert.areEqual(1, workerProvider.getCallCount('syncExistingWorkerForNextWakeAt'));
    }

    @IsTest
    static void should_reject_canceling_terminal_job() {
        // Arrange
        Job__c completedJob = QueuesTestSupport.job(Queues.State.COMPLETED.name());
        completedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 33);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(completedJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', completedJob)
            .withReturn('requireJobForUpdate', completedJob);
        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(
                QueuesRuntimeService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('ensureRuntime', runtime)
                    .withReturn('lockRuntime', runtime)
            ),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            new QueuesWorkerService()
        );

        String failureMessage;

        // Act
        try {
            service.cancelJob(completedJob.Id);
            Assert.fail('Expected terminal job cancellation to be rejected.');
        } catch (Queues.InvalidOperationException ex) {
            failureMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual('Only waiting, delayed, paused, or waiting-children jobs can be canceled.', failureMessage);
        Assert.areEqual(0, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_not_reschedule_future_worker_when_removing_paused_job() {
        // Arrange
        Job__c pausedJob = QueuesTestSupport.job(Queues.State.PAUSED.name());
        pausedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 5);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(pausedJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', pausedJob)
            .withReturn('requireJobForUpdate', pausedJob);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);
        QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
        );

        // Act
        service.removeJob(pausedJob.Id);

        // Assert
        Assert.areEqual(0, workerProvider.getCallCount('syncExistingWorkerForNextWakeAt'));
    }

    @IsTest
    static void should_discard_non_active_job_after_locking_runtime() {
        // Arrange
        Job__c waitingJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        waitingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 8);
        waitingJob.AttemptsMade__c = 3;
        QueueRuntime__c runtime = QueuesTestSupport.runtime(waitingJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', waitingJob)
            .withReturn('requireJobForUpdate', waitingJob)
            .withReturn('updateJob', waitingJob);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            new QueuesWorkerService()
        );

        // Act
        Job__c discardedJob = service.discardJob(waitingJob.Id);

        // Assert
        Assert.areEqual(3, QueuesSupport.toInteger(discardedJob.MaxAttempts__c, 0));
        Assert.areEqual(1, runtimeProvider.getCallCount('ensureRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('lockRuntime'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_reject_discarding_active_job() {
        // Arrange
        Job__c activeJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        activeJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 9);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(activeJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', activeJob)
            .withReturn('requireJobForUpdate', activeJob);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            new QueuesWorkerService()
        );

        String failureMessage;

        // Act
        try {
            service.discardJob(activeJob.Id);
            Assert.fail('Expected active discard to be rejected.');
        } catch (Queues.InvalidOperationException ex) {
            failureMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual('Active jobs cannot be discarded.', failureMessage);
        Assert.areEqual(1, runtimeProvider.getCallCount('ensureRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('lockRuntime'));
        Assert.areEqual(0, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_update_job_data_after_locking_runtime() {
        // Arrange
        Job__c waitingJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        waitingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 10);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(waitingJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', waitingJob)
            .withReturn('requireJobForUpdate', waitingJob)
            .withReturn('updateJob', waitingJob);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            new QueuesWorkerService()
        );

        // Act
        Job__c updatedJob = service.updateJobData(waitingJob.Id, new Map<String, Object>{ 'invoiceId' => 'INV-901' });

        // Assert
        Assert.areEqual('{"invoiceId":"INV-901"}', updatedJob.Data__c);
        Assert.areEqual(1, runtimeProvider.getCallCount('ensureRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('lockRuntime'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_update_job_progress_after_locking_runtime() {
        // Arrange
        Job__c waitingJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        waitingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 11);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(waitingJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', waitingJob)
            .withReturn('requireJobForUpdate', waitingJob)
            .withReturn('updateJob', waitingJob);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            new QueuesWorkerService()
        );

        // Act
        Job__c updatedJob = service.updateJobProgress(waitingJob.Id, new Map<String, Object>{ 'step' => 'queued' });

        // Assert
        Assert.areEqual('{"step":"queued"}', updatedJob.Progress__c);
        Assert.areEqual(1, runtimeProvider.getCallCount('ensureRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('lockRuntime'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_reject_updating_progress_for_active_job() {
        // Arrange
        Job__c activeJob = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        activeJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 12);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(activeJob.QueueName__c, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJob', activeJob)
            .withReturn('requireJobForUpdate', activeJob);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);

        QueuesJobAdminService service = new QueuesJobAdminService(
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesQueueableExecutionService(),
            new QueuesFlowProducerService(),
            new QueuesWorkerService()
        );

        String failureMessage;

        // Act
        try {
            service.updateJobProgress(activeJob.Id, new Map<String, Object>{ 'step' => 'running' });
            Assert.fail('Expected active progress update to be rejected.');
        } catch (Queues.InvalidOperationException ex) {
            failureMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual('Active jobs cannot update progress.', failureMessage);
        Assert.areEqual(1, runtimeProvider.getCallCount('ensureRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('lockRuntime'));
        Assert.areEqual(0, jobServiceProvider.getCallCount('updateJob'));
    }
}
