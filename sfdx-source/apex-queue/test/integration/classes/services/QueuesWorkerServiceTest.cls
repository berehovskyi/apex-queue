@IsTest(IsParallel=false)
private class QueuesWorkerServiceTest {
    @IsTest
    static void should_schedule_worker_for_jobs_when_jobs_define_earliest_wake_at() {
        // Arrange
        Datetime currentNow = System.now();
        Datetime earliestWakeAt = currentNow.addMinutes(1);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('findEarliestWakeAt', earliestWakeAt);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime)
            .withReturn('shouldKeepWorker', true);

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        service.scheduleWorkerForJobs(QueuesTestSupport.DEFAULT_QUEUE_NAME, new List<Job__c>(), currentNow);
        service.scheduleWorkerForJobs(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            new List<Job__c>{ QueuesTestSupport.job(Queues.State.WAITING.name()) },
            currentNow
        );

        // Assert
        Assert.areEqual(2, jobServiceProvider.getCallCount('findEarliestWakeAt'));
        Assert.areEqual(2, runtimeProvider.getCallCount('ensureRuntime'));
        Assert.areEqual(2, runtimeProvider.getCallCount('lockRuntime'));
        Assert.areEqual(2, runtimeProvider.getCallCount('shouldKeepWorker'));
    }

    @IsTest
    static void should_return_empty_locator_when_runtime_is_missing() {
        // Arrange
        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('findQueueDefinitionAsSystem', null)
        );
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('lockRuntime', null)
        );
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('emptyJobLocator', QueuesTestSupport.emptyJobLocator());
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            jobServiceProvider
        );

        QueuesWorkerService service = new QueuesWorkerService(
            definitionStub,
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Database.QueryLocator locator = service.start('invoice-sync', 'worker-token', null);

        // Assert
        Assert.areNotEqual(null, locator);
        Assert.areEqual(1, jobServiceProvider.getCallCount('emptyJobLocator'));
    }

    @IsTest
    static void should_return_false_when_wake_at_is_null_or_worker_is_kept() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime)
            .withReturn('shouldKeepWorker', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            runtimeStub,
            new QueuesJobService(),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean ensuredWithoutWakeAt = service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, null);
        Boolean ensuredWithFutureWakeAt = service.ensureWorker(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            System.now().addMinutes(1)
        );

        // Assert
        Assert.areEqual(false, ensuredWithoutWakeAt);
        Assert.areEqual(false, ensuredWithFutureWakeAt);
        Assert.areEqual(1, runtimeProvider.getCallCount('ensureRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('lockRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('shouldKeepWorker'));
        Assert.areEqual(0, runtimeProvider.getCallCount('updateRuntime'));
    }

    @IsTest
    static void should_not_schedule_worker_when_runtime_is_paused() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, true, 0);
        runtime.WorkerToken__c = 'worker-token';

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime);

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesJobService(),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean ensured = service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now().addMinutes(1));

        // Assert
        Assert.areEqual(false, ensured);
        Assert.areEqual(1, runtimeProvider.getCallCount('ensureRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('lockRuntime'));
        Assert.areEqual(0, runtimeProvider.getCallCount('shouldKeepWorker'));
        Assert.areEqual(0, runtimeProvider.getCallCount('updateRuntime'));
    }

    @IsTest
    static void should_keep_future_worker_when_scheduled_job_is_still_active() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';
        runtime.AsyncApexJobId__c = fakeScheduledJobIdValue(9);
        runtime.NextPumpAt__c = System.now().addMinutes(5);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime)
            .withReturn('shouldKeepWorker', true);
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'queryScheduledJobById',
                new List<CronTrigger>{
                    QueuesTestSupport.cronTrigger(
                        Id.valueOf(runtime.AsyncApexJobId__c),
                        'WAITING',
                        '0 0 0 1 1 ? 2099',
                        'Queues test'
                    )
                }
            );

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesJobService(),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean ensured = service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now().addMinutes(6));

        // Assert
        Assert.areEqual(false, ensured);
        Assert.areEqual(1, runtimeProvider.getCallCount('shouldKeepWorker'));
        Assert.areEqual(1, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(0, scheduleProvider.getCallCount('abortJob'));
        Assert.areEqual(0, runtimeProvider.getCallCount('updateRuntime'));
    }

    @IsTest
    static void should_replace_future_worker_when_scheduled_job_is_missing() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';
        runtime.AsyncApexJobId__c = fakeScheduledJobIdValue(10);
        runtime.NextPumpAt__c = System.now().addMinutes(5);

        Job__c nextJob = QueuesTestSupport.job(Queues.State.DELAYED.name());
        nextJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 108);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime)
            .withReturn('shouldKeepWorker', true)
            .withReturn('updateWorkerJobId', true);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('findNextDispatchJob', nextJob);
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryScheduledJobById', new List<CronTrigger>());

        QueuesWorkerService service = new QueuesWorkerService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            ),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean ensured = service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now().addMinutes(6));

        // Assert
        Assert.areEqual(true, ensured);
        Assert.areEqual(1, runtimeProvider.getCallCount('shouldKeepWorker'));
        Assert.areEqual(2, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(0, scheduleProvider.getCallCount('abortJob'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateWorkerJobId'));
    }

    @IsTest
    static void should_return_false_when_runtime_disappears_before_worker_transition() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', null);

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            new QueuesJobService(),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean ensured = service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now().addMinutes(1));

        // Assert
        Assert.areEqual(false, ensured);
        Assert.areEqual(1, runtimeProvider.getCallCount('ensureRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('lockRuntime'));
    }

    @IsTest
    static void should_ensure_worker_for_next_wake_at_when_wake_is_available() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';
        Datetime currentNow = System.now();
        Datetime nextWakeAt = currentNow.addMinutes(1);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime)
            .withReturn('shouldKeepWorker', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('findNextWakeAt', nextWakeAt);
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            jobServiceProvider
        );

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean ensured = service.ensureWorkerForNextWakeAt(QueuesTestSupport.DEFAULT_QUEUE_NAME, currentNow);

        // Assert
        Assert.areEqual(false, ensured);
        Assert.areEqual(1, jobServiceProvider.getCallCount('findNextWakeAt'));
        Assert.areEqual(1, runtimeProvider.getCallCount('ensureRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('lockRuntime'));
    }

    @IsTest
    static void should_resolve_far_future_worker_delay_without_native_cap() {
        // Arrange
        Datetime currentNow = System.now();

        // Act
        Integer immediateDelay = QueuesWorkerService.resolveWorkerDelayMinutes(currentNow, currentNow);
        Integer roundedDelay = QueuesWorkerService.resolveWorkerDelayMinutes(currentNow.addSeconds(1), currentNow);
        Integer farFutureDelay = QueuesWorkerService.resolveWorkerDelayMinutes(currentNow.addHours(5), currentNow);

        // Assert
        Assert.areEqual(0, immediateDelay);
        Assert.areEqual(1, roundedDelay);
        Assert.areEqual(300, farFutureDelay);
    }

    @IsTest
    static void should_clear_worker_when_queue_definition_is_inactive() {
        // Arrange
        QueueDefinition__mdt inactiveQueueDefinition = QueuesTestSupport.queueDefinition(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            false
        );
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';

        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('findQueueDefinitionAsSystem', inactiveQueueDefinition)
                .withReturn(
                    'getLeaseMinutes',
                    QueuesSupport.toPositiveInteger(inactiveQueueDefinition.DefaultLeaseMinutes__c, 5)
                )
        );
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime)
            .withReturn('isCurrentWorker', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('emptyJobLocator', QueuesTestSupport.emptyJobLocator());
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            jobServiceProvider
        );

        QueuesWorkerService service = new QueuesWorkerService(
            definitionStub,
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Database.QueryLocator locator = service.start('invoice-sync', 'worker-token', null);

        // Assert
        Assert.areNotEqual(null, locator);
        Assert.areEqual(1, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
    }

    @IsTest
    static void should_stop_worker_when_runtime_is_paused() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, true, 0);
        runtime.WorkerToken__c = 'worker-token';

        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('findQueueDefinitionAsSystem', QueuesTestSupport.queueDefinition())
                .withReturn('getLeaseMinutes', 5)
        );
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime)
            .withReturn('isCurrentWorker', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('emptyJobLocator', QueuesTestSupport.emptyJobLocator());
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            jobServiceProvider
        );

        QueuesWorkerService service = new QueuesWorkerService(
            definitionStub,
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Database.QueryLocator locator = service.start('invoice-sync', 'worker-token', null);

        // Assert
        Assert.areNotEqual(null, locator);
        Assert.areEqual(1, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
    }

    @IsTest
    static void should_claim_due_jobs_when_starting_current_worker() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        queueDefinition.BatchClaimSize__c = 5;
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';
        runtime.NextPumpAt__c = System.now().addMinutes(-1);
        Job__c dueJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        dueJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 81);

        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('findQueueDefinitionAsSystem', queueDefinition)
                .withReturn(
                    'getLeaseMinutes',
                    QueuesSupport.toPositiveInteger(queueDefinition.DefaultLeaseMinutes__c, 5)
                )
        );
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime)
            .withReturn('isCurrentWorker', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryDueJobsAsSystem', new List<Job__c>{ dueJob })
            .withReturn('claimBatchScopeJobs', new List<Job__c>{ dueJob })
            .withReturn('queryClaimedJobLocatorAsSystem', QueuesTestSupport.emptyJobLocator());
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            jobServiceProvider
        );

        QueuesWorkerService service = new QueuesWorkerService(
            definitionStub,
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Database.QueryLocator locator = service.start('invoice-sync', 'worker-token', null);

        // Assert
        Assert.areNotEqual(null, locator);
        Assert.areEqual(1, runtimeProvider.getCallCount('incrementActiveWorkerCount'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('queryClaimedJobLocatorAsSystem'));
        Assert.areEqual(5, (Integer) jobServiceProvider.getLastArgs('queryDueJobsAsSystem')[3]);
    }

    @IsTest
    static void should_respect_batch_claim_size_when_queue_configuration_is_higher_than_lease_minutes() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        queueDefinition.BatchClaimSize__c = 50;
        queueDefinition.DefaultLeaseMinutes__c = 5;
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';
        runtime.NextPumpAt__c = System.now().addMinutes(-1);

        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('findQueueDefinitionAsSystem', queueDefinition)
        );
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime)
            .withReturn('isCurrentWorker', true);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryDueJobsAsSystem', new List<Job__c>())
            .withReturn('claimBatchScopeJobs', new List<Job__c>())
            .withReturn('emptyJobLocator', QueuesTestSupport.emptyJobLocator());

        QueuesWorkerService service = new QueuesWorkerService(
            definitionStub,
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        service.start(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'worker-token', null);

        // Assert
        Assert.areEqual(50, (Integer) jobServiceProvider.getLastArgs('queryDueJobsAsSystem')[3]);
    }

    @IsTest
    static void should_execute_scope_jobs_when_worker_is_current_and_unpaused() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';
        Job__c job = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        job.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 91);

        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition())
        );
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('findRuntimeAsSystem', runtime)
            .withReturn('isCurrentWorker', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesJobExecutionService executionStub = (QueuesJobExecutionService) Test.createStub(
            QueuesJobExecutionService.class,
            executionProvider
        );

        QueuesWorkerService service = new QueuesWorkerService(
            definitionStub,
            runtimeStub,
            new QueuesJobService(),
            executionStub,
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        service.execute(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'worker-token', new List<Job__c>{ job });

        // Assert
        Assert.areEqual(1, executionProvider.getCallCount('executeClaimedBatchJobs'));
        Assert.areEqual(null, executionProvider.getLastArgs('executeClaimedBatchJobs')[1]);
    }

    @IsTest
    static void should_forward_async_apex_job_id_when_executing_worker_scope() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';
        Job__c job = QueuesTestSupport.job(Queues.State.ACTIVE.name());
        job.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 92);

        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition())
        );
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('findRuntimeAsSystem', runtime)
            .withReturn('isCurrentWorker', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesJobExecutionService executionStub = (QueuesJobExecutionService) Test.createStub(
            QueuesJobExecutionService.class,
            executionProvider
        );

        QueuesWorkerService service = new QueuesWorkerService(
            definitionStub,
            runtimeStub,
            new QueuesJobService(),
            executionStub,
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        service.execute(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'worker-token',
            new List<Job__c>{ job },
            fakeAsyncApexJobId(777)
        );

        // Assert
        Assert.areEqual(1, executionProvider.getCallCount('executeClaimedBatchJobs'));
        Assert.areEqual(fakeAsyncApexJobId(777), executionProvider.getLastArgs('executeClaimedBatchJobs')[1]);
    }

    @IsTest
    static void should_clear_worker_when_finishing_without_next_wake_at() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 1);
        runtime.WorkerToken__c = 'worker-token';
        runtime.NextPumpAt__c = System.now().addMinutes(1);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime)
            .withReturn('isCurrentWorker', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('findNextWakeAt', null)
        );

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        service.finish(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'worker-token');

        // Assert
        Assert.areEqual(1, runtimeProvider.getCallCount('lockRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('decrementActiveWorkerCount'));
        Assert.areEqual(1, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
    }

    @IsTest
    static void should_schedule_future_dispatch_when_worker_needs_delay() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        Job__c nextJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        nextJob.Name__c = QueuesTestSupport.DEFAULT_JOB_NAME;

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime)
            .withReturn('shouldKeepWorker', false)
            .withReturn('isCurrentWorker', false)
            .withReturn('updateWorkerJobId', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('findNextDispatchJob', nextJob)
        );

        QueuesWorkerService service = new QueuesWorkerService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            ),
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean ensured = service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now().addMinutes(5));

        // Assert
        Assert.areEqual(true, ensured);
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateWorkerJobId'));
        Assert.areEqual(true, String.isNotBlank(runtime.WorkerToken__c));
        Assert.areNotEqual(null, runtime.NextPumpAt__c);
    }

    @IsTest
    static void should_abort_existing_scheduled_worker_when_replacing_future_worker_with_earlier_dispatch() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'old-worker-token';
        runtime.AsyncApexJobId__c = fakeScheduledJobIdValue(1);
        runtime.NextPumpAt__c = System.now().addMinutes(10);
        String previousScheduledJobId = runtime.AsyncApexJobId__c;
        String previousWorkerToken = runtime.WorkerToken__c;

        Job__c nextJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        nextJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 103);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime)
            .withReturn('shouldKeepWorker', false)
            .withReturn('updateWorkerJobId', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('findNextDispatchJob', nextJob)
        );
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'queryScheduledJobById',
                new List<CronTrigger>{
                    QueuesTestSupport.cronTrigger(
                        Id.valueOf(previousScheduledJobId),
                        'WAITING',
                        '0 0 0 1 1 ? 2099',
                        'Queues test'
                    )
                }
            );

        QueuesWorkerService service = new QueuesWorkerService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            ),
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean ensured = service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now());

        // Assert
        Assert.areEqual(true, ensured);
        Assert.areEqual(1, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(1, scheduleProvider.getCallCount('abortJob'));
        Assert.areEqual(previousScheduledJobId, (String) scheduleProvider.getLastArgs('abortJob')[0]);
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateWorkerJobId'));
        Assert.areNotEqual(previousWorkerToken, runtime.WorkerToken__c);
    }

    @IsTest
    static void should_keep_future_worker_schedule_when_next_wake_moves_later() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        Datetime laterWakeAt = System.now().addMinutes(10);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'old-worker-token';
        runtime.AsyncApexJobId__c = fakeScheduledJobIdValue(3);
        runtime.NextPumpAt__c = System.now().addMinutes(5);
        String previousScheduledJobId = runtime.AsyncApexJobId__c;

        Job__c nextJob = QueuesTestSupport.job(Queues.State.DELAYED.name());
        nextJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 105);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime)
            .withReturn('updateWorkerJobId', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('findNextWakeAt', laterWakeAt)
            .withReturn('findNextDispatchJob', nextJob);
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'queryScheduledJobById',
                new List<CronTrigger>{
                    QueuesTestSupport.cronTrigger(
                        Id.valueOf(previousScheduledJobId),
                        'WAITING',
                        '0 0 0 1 1 ? 2099',
                        'Queues test'
                    )
                }
            );

        QueuesWorkerService service = new QueuesWorkerService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            ),
            runtimeStub,
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean rescheduled = service.syncExistingWorkerForNextWakeAt(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            System.now()
        );

        // Assert
        Assert.areEqual(true, rescheduled);
        Assert.areEqual(1, jobServiceProvider.getCallCount('findNextWakeAt'));
        Assert.areEqual(1, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(0, scheduleProvider.getCallCount('abortJob'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(0, runtimeProvider.getCallCount('updateWorkerJobId'));
        Assert.areEqual(previousScheduledJobId, runtime.AsyncApexJobId__c);
        Assert.areEqual(laterWakeAt, runtime.NextPumpAt__c);
        Assert.areEqual(0, jobServiceProvider.getCallCount('findNextDispatchJob'));
    }

    @IsTest
    static void should_keep_due_scheduled_worker_when_next_wake_moves_later() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        Datetime laterWakeAt = System.now().addMinutes(10);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'old-worker-token';
        runtime.AsyncApexJobId__c = fakeScheduledJobIdValue(8);
        runtime.NextPumpAt__c = System.now().addSeconds(-5);
        String previousScheduledJobId = runtime.AsyncApexJobId__c;

        Job__c nextJob = QueuesTestSupport.job(Queues.State.DELAYED.name());
        nextJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 107);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime)
            .withReturn('updateWorkerJobId', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('findNextWakeAt', laterWakeAt)
            .withReturn('findNextDispatchJob', nextJob);
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'queryScheduledJobById',
                new List<CronTrigger>{
                    QueuesTestSupport.cronTrigger(
                        Id.valueOf(previousScheduledJobId),
                        'WAITING',
                        '0 0 0 1 1 ? 2099',
                        'Queues test'
                    )
                }
            );

        QueuesWorkerService service = new QueuesWorkerService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            ),
            runtimeStub,
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean rescheduled = service.syncExistingWorkerForNextWakeAt(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            System.now()
        );

        // Assert
        Assert.areEqual(true, rescheduled);
        Assert.areEqual(2, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(0, scheduleProvider.getCallCount('abortJob'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(0, runtimeProvider.getCallCount('updateWorkerJobId'));
        Assert.areEqual(previousScheduledJobId, runtime.AsyncApexJobId__c);
        Assert.areEqual(laterWakeAt, runtime.NextPumpAt__c);
        Assert.areEqual(0, jobServiceProvider.getCallCount('findNextDispatchJob'));
    }

    @IsTest
    static void should_keep_pending_future_worker_when_next_wake_is_unchanged_and_schedule_is_active() {
        // Arrange
        Datetime currentNow = System.now();
        Datetime sameWakeAt = currentNow.addMinutes(5);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'old-worker-token';
        runtime.AsyncApexJobId__c = fakeScheduledJobIdValue(15);
        runtime.NextPumpAt__c = sameWakeAt;

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime)
            .withReturn('shouldKeepWorker', true);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('findNextWakeAt', sameWakeAt);
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'queryScheduledJobById',
                new List<CronTrigger>{
                    QueuesTestSupport.cronTrigger(
                        Id.valueOf(runtime.AsyncApexJobId__c),
                        'WAITING',
                        '0 0 0 1 1 ? 2099',
                        'Queues test'
                    )
                }
            );

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean rescheduled = service.syncExistingWorkerForNextWakeAt(QueuesTestSupport.DEFAULT_QUEUE_NAME, currentNow);

        // Assert
        Assert.areEqual(false, rescheduled);
        Assert.areEqual(1, runtimeProvider.getCallCount('lockRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('shouldKeepWorker'));
        Assert.areEqual(1, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(0, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(0, jobServiceProvider.getCallCount('findNextDispatchJob'));
    }

    @IsTest
    static void should_replace_pending_future_worker_when_same_wake_is_requested_but_schedule_is_missing() {
        // Arrange
        Datetime currentNow = System.now();
        Datetime sameWakeAt = currentNow.addMinutes(5);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'old-worker-token';
        runtime.AsyncApexJobId__c = fakeScheduledJobIdValue(16);
        runtime.NextPumpAt__c = sameWakeAt;

        Job__c nextJob = QueuesTestSupport.job(Queues.State.DELAYED.name());
        nextJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 110);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime)
            .withReturn('shouldKeepWorker', true)
            .withReturn('updateWorkerJobId', true);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('findNextWakeAt', sameWakeAt)
            .withReturn('findNextDispatchJob', nextJob);
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns('queryScheduledJobById', new List<Object>{ new List<CronTrigger>(), new List<CronTrigger>() });

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean rescheduled = service.syncExistingWorkerForNextWakeAt(QueuesTestSupport.DEFAULT_QUEUE_NAME, currentNow);

        // Assert
        Assert.areEqual(true, rescheduled);
        Assert.areEqual(1, runtimeProvider.getCallCount('shouldKeepWorker'));
        Assert.areEqual(2, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateWorkerJobId'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('findNextDispatchJob'));
    }

    @IsTest
    static void should_sync_worker_for_next_wake_when_no_future_worker_plan_exists() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        Job__c nextJob = QueuesTestSupport.job(Queues.State.DELAYED.name());
        nextJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 106);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturns('lockRuntime', new List<Object>{ runtime, runtime })
            .withReturn('shouldKeepWorker', false)
            .withReturn('updateWorkerJobId', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('findNextWakeAt', System.now().addMinutes(10))
            .withReturn('findNextDispatchJob', nextJob);

        QueuesWorkerService service = new QueuesWorkerService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            ),
            runtimeStub,
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean synced = service.syncWorkerForNextWakeAt(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now());

        // Assert
        Assert.areEqual(true, synced);
        Assert.areEqual(1, jobServiceProvider.getCallCount('findNextWakeAt'));
        Assert.areEqual(2, runtimeProvider.getCallCount('lockRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateWorkerJobId'));
    }

    @IsTest
    static void should_not_sync_existing_worker_when_runtime_has_no_pending_plan() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('findNextWakeAt', System.now().addMinutes(5));

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean rescheduled = service.syncExistingWorkerForNextWakeAt(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            System.now()
        );

        // Assert
        Assert.areEqual(false, rescheduled);
        Assert.areEqual(1, runtimeProvider.getCallCount('lockRuntime'));
        Assert.areEqual(0, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(0, jobServiceProvider.getCallCount('findNextDispatchJob'));
    }

    @IsTest
    static void should_clear_future_worker_when_no_next_wake_remains_after_reschedule() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'old-worker-token';
        runtime.AsyncApexJobId__c = fakeScheduledJobIdValue(4);
        runtime.NextPumpAt__c = System.now().addMinutes(5);
        String previousScheduledJobId = runtime.AsyncApexJobId__c;

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime)
            .withReturn('clearWorkerAndUpdate', runtime);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('findNextWakeAt', null);
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'queryScheduledJobById',
                new List<CronTrigger>{
                    QueuesTestSupport.cronTrigger(
                        Id.valueOf(previousScheduledJobId),
                        'WAITING',
                        '0 0 0 1 1 ? 2099',
                        'Queues test'
                    )
                }
            );

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            runtimeStub,
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean rescheduled = service.syncExistingWorkerForNextWakeAt(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            System.now()
        );

        // Assert
        Assert.areEqual(true, rescheduled);
        Assert.areEqual(1, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(1, scheduleProvider.getCallCount('abortJob'));
        Assert.areEqual(previousScheduledJobId, (String) scheduleProvider.getLastArgs('abortJob')[0]);
        Assert.areEqual(1, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
        Assert.areEqual(0, runtimeProvider.getCallCount('updateWorkerJobId'));
    }

    @IsTest
    static void should_dispatch_immediate_batch_when_wake_at_is_now() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        Job__c nextJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        nextJob.Name__c = QueuesTestSupport.DEFAULT_JOB_NAME;

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime)
            .withReturn('shouldKeepWorker', false)
            .withReturn('updateWorkerJobId', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('findNextDispatchJob', nextJob)
        );

        QueuesWorkerService service = new QueuesWorkerService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            ),
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean ensured = service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now());

        // Assert
        Assert.areEqual(true, ensured);
        Assert.areEqual(1, runtimeProvider.getCallCount('updateWorkerJobId'));
    }

    @IsTest
    static void should_record_queue_error_and_rollback_runtime_when_worker_scheduling_fails() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        QueueRuntime__c latestRuntime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        Job__c nextJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        nextJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 101);
        nextJob.Name__c = QueuesTestSupport.DEFAULT_JOB_NAME;

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('shouldKeepWorker', false)
            .withReturns('lockRuntime', new List<Object>{ runtime, latestRuntime })
            .withReturn('isCurrentWorker', false)
            .withException('updateRuntime', new Queues.QueueException('Worker scheduling exploded.'));
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('findNextDispatchJob', nextJob)
        );
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('publishQueueError', successfulPublishResult());
        QueuesErrorEventRepository eventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
            QueuesErrorEventRepository.class,
            eventRepositoryProvider
        );
        QueuesErrorService errorService = new QueuesErrorService(new QueuesErrorRepository(), eventRepositoryStub);

        QueuesWorkerService service = new QueuesWorkerService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition())
            ),
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            errorService,
            new QueuesQueueEventService()
        );

        String failureMessage;

        // Act
        try {
            service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now());
            Assert.fail('Expected worker scheduling failure.');
        } catch (Queues.QueueException ex) {
            failureMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual('Worker scheduling exploded.', failureMessage);
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(0, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
        Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
        QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) eventRepositoryProvider.getLastArgs(
            'publishQueueError'
        )[0];
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, errorEvent.QueueName__c);
        Assert.areEqual('worker scheduling', errorEvent.Component__c);
        Assert.areEqual(nextJob.Id, errorEvent.JobId__c);
        Assert.areEqual(null, errorEvent.AsyncApexJobId__c);
        Assert.areEqual('Worker scheduling exploded.', errorEvent.Message__c);
    }

    @IsTest
    static void should_record_async_job_id_when_worker_job_id_update_fails_after_dispatch() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        Job__c nextJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        nextJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 102);
        nextJob.Name__c = QueuesTestSupport.DEFAULT_JOB_NAME;

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('shouldKeepWorker', false)
            .withReturns('lockRuntime', new List<Object>{ runtime, runtime })
            .withReturn('isCurrentWorker', true)
            .withException('updateWorkerJobId', new Queues.QueueException('Worker job id update failed.'));
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('findNextDispatchJob', nextJob)
        );
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('publishQueueError', successfulPublishResult());
        QueuesErrorEventRepository eventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
            QueuesErrorEventRepository.class,
            eventRepositoryProvider
        );
        QueuesErrorService errorService = new QueuesErrorService(new QueuesErrorRepository(), eventRepositoryStub);
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'queryScheduledJobById',
                new List<CronTrigger>{
                    QueuesTestSupport.cronTrigger(fakeScheduledJobId(5), 'WAITING', '0 0 0 1 1 ? 2099', 'Queues test')
                }
            );

        QueuesWorkerService service = new QueuesWorkerService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            ),
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            errorService,
            new QueuesQueueEventService()
        );

        String failureMessage;

        // Act
        try {
            service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now().addMinutes(5));
            Assert.fail('Expected worker job id update failure.');
        } catch (Queues.QueueException ex) {
            failureMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual('Worker job id update failed.', failureMessage);
        Assert.areEqual(1, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(1, scheduleProvider.getCallCount('abortJob'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
        Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
        QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) eventRepositoryProvider.getLastArgs(
            'publishQueueError'
        )[0];
        Assert.areEqual(nextJob.Id, errorEvent.JobId__c);
        Assert.areEqual(true, String.isNotBlank(errorEvent.AsyncApexJobId__c));
        Assert.areEqual('Worker job id update failed.', errorEvent.Message__c);
    }

    @IsTest
    static void should_abort_immediate_batch_worker_when_worker_job_id_update_fails_after_dispatch() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        Job__c nextJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        nextJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 109);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('shouldKeepWorker', false)
            .withReturns('lockRuntime', new List<Object>{ runtime, runtime })
            .withReturn('isCurrentWorker', true)
            .withException('updateWorkerJobId', new Queues.QueueException('Worker job id update failed.'));
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('findNextDispatchJob', nextJob)
        );
        QueuesTestSupport.RecordingStubProvider asyncRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'queryAsyncJobs',
                new List<AsyncApexJob>{
                    QueuesTestSupport.asyncApexJob(
                        QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 15),
                        'Queued',
                        'QueuesBatchableWorker'
                    )
                }
            );
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryScheduledJobById', new List<CronTrigger>());

        QueuesWorkerService service = new QueuesWorkerService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            ),
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            (QueuesAsyncApexJobRepository) Test.createStub(QueuesAsyncApexJobRepository.class, asyncRepositoryProvider),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        String failureMessage;

        // Act
        try {
            service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now());
            Assert.fail('Expected worker job id update failure.');
        } catch (Queues.QueueException ex) {
            failureMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual('Worker job id update failed.', failureMessage);
        Assert.areEqual(1, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(1, asyncRepositoryProvider.getCallCount('queryAsyncJobs'));
        Assert.areEqual(1, scheduleProvider.getCallCount('abortJob'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(1, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
    }

    @IsTest
    static void should_restore_previous_future_worker_plan_when_replacement_dispatch_fails_after_runtime_update() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        Datetime previousNextPumpAt = System.now().addMinutes(5);
        String previousWorkerToken = 'old-worker-token';
        String previousAsyncApexJobId = fakeScheduledJobIdValue(13);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = previousWorkerToken;
        runtime.AsyncApexJobId__c = previousAsyncApexJobId;
        runtime.NextPumpAt__c = previousNextPumpAt;

        Job__c nextJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        nextJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 103);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('shouldKeepWorker', false)
            .withReturns('lockRuntime', new List<Object>{ runtime, runtime })
            .withReturn('isCurrentWorker', true)
            .withException('updateWorkerJobId', new Queues.QueueException('Worker job id update failed.'));
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('findNextDispatchJob', nextJob)
        );
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('publishQueueError', successfulPublishResult());
        QueuesErrorService errorService = new QueuesErrorService(
            new QueuesErrorRepository(),
            (QueuesErrorEventRepository) Test.createStub(QueuesErrorEventRepository.class, eventRepositoryProvider)
        );
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'queryScheduledJobById',
                new List<CronTrigger>{
                    QueuesTestSupport.cronTrigger(fakeScheduledJobId(14), 'WAITING', '0 0 0 1 1 ? 2099', 'Queues test')
                }
            );

        QueuesWorkerService service = new QueuesWorkerService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            ),
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            errorService,
            new QueuesQueueEventService()
        );

        String failureMessage;

        // Act
        try {
            service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now().addMinutes(8));
            Assert.fail('Expected worker job id update failure.');
        } catch (Queues.QueueException ex) {
            failureMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual('Worker job id update failed.', failureMessage);
        Assert.areEqual(1, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(1, scheduleProvider.getCallCount('abortJob'));
        Assert.areNotEqual(previousAsyncApexJobId, (String) scheduleProvider.getLastArgs('abortJob')[0]);
        Assert.areEqual(2, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(0, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
        Assert.areEqual(previousWorkerToken, runtime.WorkerToken__c);
        Assert.areEqual(previousAsyncApexJobId, runtime.AsyncApexJobId__c);
        Assert.areEqual(previousNextPumpAt, runtime.NextPumpAt__c);
        Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
    }

    @IsTest
    static void should_abort_scheduled_worker_when_worker_job_id_update_returns_false_after_dispatch() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        Job__c nextJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        nextJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 104);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('shouldKeepWorker', false)
            .withReturns('lockRuntime', new List<Object>{ runtime, runtime })
            .withReturn('isCurrentWorker', true)
            .withReturn('updateWorkerJobId', false);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('findNextDispatchJob', nextJob)
        );
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('publishQueueError', successfulPublishResult());
        QueuesErrorEventRepository eventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
            QueuesErrorEventRepository.class,
            eventRepositoryProvider
        );
        QueuesErrorService errorService = new QueuesErrorService(new QueuesErrorRepository(), eventRepositoryStub);
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'queryScheduledJobById',
                new List<CronTrigger>{
                    QueuesTestSupport.cronTrigger(fakeScheduledJobId(6), 'WAITING', '0 0 0 1 1 ? 2099', 'Queues test')
                }
            );

        QueuesWorkerService service = new QueuesWorkerService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            ),
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            errorService,
            new QueuesQueueEventService()
        );

        String failureMessage;

        // Act
        try {
            service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now().addMinutes(5));
            Assert.fail('Expected worker job id update failure.');
        } catch (Queues.QueueException ex) {
            failureMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual('Worker job id update failed.', failureMessage);
        Assert.areEqual(1, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(1, scheduleProvider.getCallCount('abortJob'));
        Assert.areEqual(1, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
        Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
    }

    @IsTest
    static void should_abort_dispatched_scheduled_worker_even_when_it_is_not_yet_query_visible() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        Job__c nextJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        nextJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 110);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('shouldKeepWorker', false)
            .withReturns('lockRuntime', new List<Object>{ runtime, runtime })
            .withReturn('isCurrentWorker', true)
            .withException('updateWorkerJobId', new Queues.QueueException('Worker job id update failed.'));
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('findNextDispatchJob', nextJob)
        );
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('publishQueueError', successfulPublishResult());
        QueuesErrorService errorService = new QueuesErrorService(
            new QueuesErrorRepository(),
            (QueuesErrorEventRepository) Test.createStub(QueuesErrorEventRepository.class, eventRepositoryProvider)
        );
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryScheduledJobById', new List<CronTrigger>())
            .withReturn('abortJob', null);

        QueuesWorkerService service = new QueuesWorkerService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            ),
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            errorService,
            new QueuesQueueEventService()
        );

        String failureMessage;

        // Act
        try {
            service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now().addMinutes(5));
            Assert.fail('Expected worker job id update failure.');
        } catch (Queues.QueueException ex) {
            failureMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual('Worker job id update failed.', failureMessage);
        Assert.areEqual(1, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(1, scheduleProvider.getCallCount('abortJob'));
        Assert.areEqual(1, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
        Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
    }

    @IsTest
    static void should_abort_due_scheduled_worker_when_schedule_is_still_active() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.AsyncApexJobId__c = fakeScheduledJobIdValue(7);
        runtime.NextPumpAt__c = System.now().addSeconds(-5);
        String previousScheduledJobId = runtime.AsyncApexJobId__c;

        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn(
                'queryScheduledJobById',
                new List<CronTrigger>{
                    QueuesTestSupport.cronTrigger(
                        Id.valueOf(previousScheduledJobId),
                        'WAITING',
                        '0 0 0 1 1 ? 2099',
                        'Queues test'
                    )
                }
            );

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            new QueuesRuntimeService(),
            new QueuesJobService(),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        service.cancelFutureScheduledWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, runtime);

        // Assert
        Assert.areEqual(1, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(1, scheduleProvider.getCallCount('abortJob'));
        Assert.areEqual(previousScheduledJobId, (String) scheduleProvider.getLastArgs('abortJob')[0]);
    }

    @IsTest
    static void should_ignore_cancel_future_scheduled_worker_when_schedule_is_not_active() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.AsyncApexJobId__c = fakeScheduledJobIdValue(17);
        runtime.NextPumpAt__c = System.now().addMinutes(5);
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryScheduledJobById', new List<CronTrigger>());

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            new QueuesRuntimeService(),
            new QueuesJobService(),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        service.cancelFutureScheduledWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, runtime);

        // Assert
        Assert.areEqual(1, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(0, scheduleProvider.getCallCount('abortJob'));
    }

    @IsTest
    static void should_ignore_cancel_future_scheduled_worker_when_job_id_is_blank() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.AsyncApexJobId__c = null;
        runtime.NextPumpAt__c = System.now().addMinutes(5);
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            new QueuesRuntimeService(),
            new QueuesJobService(),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        service.cancelFutureScheduledWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, runtime);

        // Assert
        Assert.areEqual(0, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(0, scheduleProvider.getCallCount('abortJob'));
    }

    @IsTest
    static void should_not_record_queue_error_when_scheduled_worker_disappears_before_abort() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.AsyncApexJobId__c = fakeScheduledJobIdValue(11);
        runtime.NextPumpAt__c = System.now().addSeconds(-5);
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns(
                'queryScheduledJobById',
                new List<Object>{
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            Id.valueOf(runtime.AsyncApexJobId__c),
                            'WAITING',
                            '0 0 0 1 1 ? 2099',
                            'Queues test'
                        )
                    },
                    new List<CronTrigger>()
                }
            )
            .withException('abortJob', new Queues.QueueException('Scheduled job already disappeared.'));
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('publishQueueError', successfulPublishResult());
        QueuesErrorService errorService = new QueuesErrorService(
            new QueuesErrorRepository(),
            (QueuesErrorEventRepository) Test.createStub(QueuesErrorEventRepository.class, eventRepositoryProvider)
        );

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            new QueuesRuntimeService(),
            new QueuesJobService(),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            errorService,
            new QueuesQueueEventService()
        );

        // Act
        service.cancelFutureScheduledWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, runtime);

        // Assert
        Assert.areEqual(2, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(1, scheduleProvider.getCallCount('abortJob'));
        Assert.areEqual(0, eventRepositoryProvider.getCallCount('publishQueueError'));
    }

    @IsTest
    static void should_record_queue_error_when_abort_of_scheduled_worker_really_fails() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.AsyncApexJobId__c = fakeScheduledJobIdValue(12);
        runtime.NextPumpAt__c = System.now().addSeconds(-5);
        QueuesTestSupport.RecordingStubProvider scheduleProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns(
                'queryScheduledJobById',
                new List<Object>{
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            Id.valueOf(runtime.AsyncApexJobId__c),
                            'WAITING',
                            '0 0 0 1 1 ? 2099',
                            'Queues test'
                        )
                    },
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            Id.valueOf(runtime.AsyncApexJobId__c),
                            'WAITING',
                            '0 0 0 1 1 ? 2099',
                            'Queues test'
                        )
                    }
                }
            )
            .withException('abortJob', new Queues.QueueException('Abort failed.'));
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('publishQueueError', successfulPublishResult());
        QueuesErrorService errorService = new QueuesErrorService(
            new QueuesErrorRepository(),
            (QueuesErrorEventRepository) Test.createStub(QueuesErrorEventRepository.class, eventRepositoryProvider)
        );

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            new QueuesRuntimeService(),
            new QueuesJobService(),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleProvider),
            errorService,
            new QueuesQueueEventService()
        );

        // Act
        service.cancelFutureScheduledWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, runtime);

        // Assert
        Assert.areEqual(2, scheduleProvider.getCallCount('queryScheduledJobById'));
        Assert.areEqual(1, scheduleProvider.getCallCount('abortJob'));
        Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
        QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) eventRepositoryProvider.getLastArgs(
            'publishQueueError'
        )[0];
        Assert.areEqual(runtime.AsyncApexJobId__c, errorEvent.AsyncApexJobId__c);
        Assert.areEqual('Abort failed.', errorEvent.Message__c);
    }

    @IsTest
    static void should_not_schedule_worker_when_requested_wake_has_no_dispatch_job() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('ensureRuntime', runtime)
            .withReturn('lockRuntime', runtime)
            .withReturn('shouldKeepWorker', false);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
            QueuesJobService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('findNextDispatchJob', null)
        );

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            runtimeStub,
            jobServiceStub,
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Boolean ensured = service.ensureWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, System.now());

        // Assert
        Assert.areEqual(false, ensured);
        Assert.areEqual(0, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(0, runtimeProvider.getCallCount('updateWorkerJobId'));
    }

    @IsTest
    static void should_return_without_transitioning_when_worker_token_changes_during_continue() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';
        runtime.NextPumpAt__c = System.now().addMinutes(3);

        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('findQueueDefinitionAsSystem', QueuesTestSupport.queueDefinition())
                .withReturn('getLeaseMinutes', 5)
        );
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime)
            .withReturns('isCurrentWorker', new List<Object>{ true, false });
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('emptyJobLocator', QueuesTestSupport.emptyJobLocator());

        QueuesWorkerService service = new QueuesWorkerService(
            definitionStub,
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Database.QueryLocator locator = service.start(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'worker-token', null);

        // Assert
        Assert.areNotEqual(null, locator);
        Assert.areEqual(1, runtimeProvider.getCallCount('isCurrentWorker'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(0, runtimeProvider.getCallCount('updateWorkerJobId'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('emptyJobLocator'));
    }

    @IsTest
    static void should_return_empty_locator_without_transitioning_when_next_pump_is_still_in_future() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';
        runtime.NextPumpAt__c = System.now().addMinutes(3);

        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('findQueueDefinitionAsSystem', QueuesTestSupport.queueDefinition())
                .withReturn('getLeaseMinutes', 5)
        );
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime)
            .withReturn('isCurrentWorker', true);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('emptyJobLocator', QueuesTestSupport.emptyJobLocator());

        QueuesWorkerService service = new QueuesWorkerService(
            definitionStub,
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Database.QueryLocator locator = service.start(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'worker-token', null);

        // Assert
        Assert.areNotEqual(null, locator);
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(0, runtimeProvider.getCallCount('updateWorkerJobId'));
        Assert.areEqual(0, jobServiceProvider.getCallCount('findNextDispatchJob'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('emptyJobLocator'));
    }

    @IsTest
    static void should_clear_worker_when_next_dispatch_job_disappears_during_continue() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 1);
        runtime.WorkerToken__c = 'worker-token';
        runtime.NextPumpAt__c = System.now().addMinutes(1);

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime)
            .withReturn('isCurrentWorker', true);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('findNextWakeAt', System.now().addMinutes(1))
            .withReturn('findNextDispatchJob', null);

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        service.finish(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'worker-token');

        // Assert
        Assert.areEqual(1, runtimeProvider.getCallCount('decrementActiveWorkerCount'));
        Assert.areEqual(1, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
        Assert.areEqual(0, runtimeProvider.getCallCount('updateRuntime'));
    }

    @IsTest
    static void should_return_without_running_when_scope_is_empty_or_worker_is_not_current() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('findRuntimeAsSystem', runtime)
            .withReturn('isCurrentWorker', false);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider();

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            runtimeStub,
            new QueuesJobService(),
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        service.execute(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'worker-token', new List<Job__c>());
        service.execute(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'worker-token',
            new List<Job__c>{ QueuesTestSupport.job(Queues.State.ACTIVE.name()) }
        );

        // Assert
        Assert.areEqual(1, runtimeProvider.getCallCount('findRuntimeAsSystem'));
        Assert.areEqual(0, executionProvider.getCallCount('executeClaimedBatchJobs'));
    }

    @IsTest
    static void should_update_runtime_only_when_finishing_after_worker_token_changes() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 1);
        runtime.WorkerToken__c = 'worker-token';

        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime)
            .withReturn('isCurrentWorker', false);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );

        QueuesWorkerService service = new QueuesWorkerService(
            new QueuesDefinitionService(),
            runtimeStub,
            new QueuesJobService(),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        service.finish(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'other-worker-token');

        // Assert
        Assert.areEqual(1, runtimeProvider.getCallCount('decrementActiveWorkerCount'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(0, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
    }

    @IsTest
    static void should_default_batch_claim_size_when_queue_configuration_is_nonpositive() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        queueDefinition.BatchClaimSize__c = 0;
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';
        runtime.NextPumpAt__c = System.now().addMinutes(-1);

        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('findQueueDefinitionAsSystem', queueDefinition)
                .withReturn(
                    'getLeaseMinutes',
                    QueuesSupport.toPositiveInteger(queueDefinition.DefaultLeaseMinutes__c, 5)
                )
        );
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime)
            .withReturn('isCurrentWorker', true);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryDueJobsAsSystem', new List<Job__c>())
            .withReturn('claimBatchScopeJobs', new List<Job__c>())
            .withReturn('emptyJobLocator', QueuesTestSupport.emptyJobLocator());

        QueuesWorkerService service = new QueuesWorkerService(
            definitionStub,
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        service.start(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'worker-token', null);

        // Assert
        Assert.areEqual(1, (Integer) jobServiceProvider.getLastArgs('queryDueJobsAsSystem')[3]);
    }

    @IsTest
    static void should_return_empty_locator_when_no_due_jobs_are_claimed() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';
        runtime.NextPumpAt__c = System.now().addMinutes(-1);
        Job__c dueJob = QueuesTestSupport.job(Queues.State.WAITING.name());

        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('findQueueDefinitionAsSystem', queueDefinition)
                .withReturn(
                    'getLeaseMinutes',
                    QueuesSupport.toPositiveInteger(queueDefinition.DefaultLeaseMinutes__c, 5)
                )
        );
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime)
            .withReturn('isCurrentWorker', true);
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            runtimeProvider
        );
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryDueJobsAsSystem', new List<Job__c>{ dueJob })
            .withReturn('claimBatchScopeJobs', new List<Job__c>())
            .withReturn('emptyJobLocator', QueuesTestSupport.emptyJobLocator());

        QueuesWorkerService service = new QueuesWorkerService(
            definitionStub,
            runtimeStub,
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Database.QueryLocator locator = service.start(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'worker-token', null);

        // Assert
        Assert.areNotEqual(null, locator);
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('emptyJobLocator'));
    }

    @IsTest
    static void should_sync_current_async_job_id_when_worker_starts() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        String currentAsyncApexJobId = fakeAsyncApexJobId(18);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';
        runtime.AsyncApexJobId__c = fakeScheduledJobIdValue(18);
        runtime.NextPumpAt__c = System.now().addMinutes(-1);
        Job__c dueJob = QueuesTestSupport.job(Queues.State.WAITING.name());
        dueJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 818);

        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('findQueueDefinitionAsSystem', queueDefinition)
                .withReturn(
                    'getLeaseMinutes',
                    QueuesSupport.toPositiveInteger(queueDefinition.DefaultLeaseMinutes__c, 5)
                )
        );
        QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('lockRuntime', runtime)
            .withReturn('isCurrentWorker', true);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryDueJobsAsSystem', new List<Job__c>{ dueJob })
            .withReturn('claimBatchScopeJobs', new List<Job__c>{ dueJob })
            .withReturn('queryClaimedJobLocatorAsSystem', QueuesTestSupport.emptyJobLocator());

        QueuesWorkerService service = new QueuesWorkerService(
            definitionStub,
            (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            new QueuesJobExecutionService(),
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            new QueuesErrorService(),
            new QueuesQueueEventService()
        );

        // Act
        Database.QueryLocator locator = service.start(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'worker-token',
            currentAsyncApexJobId
        );

        // Assert
        Assert.areNotEqual(null, locator);
        Assert.areEqual(currentAsyncApexJobId, runtime.AsyncApexJobId__c);
        Assert.areEqual(1, runtimeProvider.getCallCount('incrementActiveWorkerCount'));
        Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
    }

    private static Database.SaveResult successfulPublishResult() {
        return Database.insert(
            new QueueError__c(
                QueueName__c = 'worker-test',
                Component__c = 'worker scheduling',
                OccurredAt__c = System.now()
            ),
            false
        );
    }

    private static Id fakeScheduledJobId(final Integer sequence) {
        return QueuesTestSupport.fakeId(CronTrigger.SObjectType, sequence);
    }

    private static String fakeScheduledJobIdValue(final Integer sequence) {
        return fakeScheduledJobId(sequence);
    }

    private static String fakeAsyncApexJobId(final Integer sequence) {
        return QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, sequence);
    }
}
