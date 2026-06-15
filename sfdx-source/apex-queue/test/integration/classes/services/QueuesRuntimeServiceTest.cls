@IsTest(IsParallel=false)
private class QueuesRuntimeServiceTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_find_runtime_as_system() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'runtime-system-lookup';
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
            insert runtime;

            // Act
            QueueRuntime__c foundRuntime = new QueuesRuntimeService().findRuntimeAsSystem(queueName);

            // Assert
            Assert.areEqual(runtime.Id, foundRuntime.Id);
        }
    }

    @IsTest
    static void should_identify_stale_worker_when_async_job_is_missing() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.NextPumpAt__c = System.now().addMinutes(-10);
        runtime.LastPumpAt__c = System.now().addMinutes(-12);
        runtime.WorkerToken__c = 'worker-token';
        runtime.AsyncApexJobId__c = fakeAsyncApexJobId(0);

        QueuesTestSupport.RecordingStubProvider asyncRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryAsyncJobs', new List<AsyncApexJob>());
        QueuesAsyncApexJobRepository asyncRepositoryStub = (QueuesAsyncApexJobRepository) Test.createStub(
            QueuesAsyncApexJobRepository.class,
            asyncRepositoryProvider
        );

        QueuesRuntimeService service = new QueuesRuntimeService(
            asyncRepositoryStub,
            new QueuesScheduleRepository(),
            new QueuesRuntimeRepository(),
            new QueuesDefinitionService()
        );

        // Act
        Boolean shouldClearWorker = service.shouldClearStaleWorker(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            runtime,
            System.now()
        );

        // Assert
        Assert.areEqual(true, shouldClearWorker);
        Assert.areEqual(1, asyncRepositoryProvider.getCallCount('queryAsyncJobs'));
    }

    @IsTest
    static void should_ensure_runtime_adjust_active_worker_count_and_clear_worker_when_runtime_is_missing_or_present() {
        // Arrange
        QueueRuntime__c insertedRuntime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        insertedRuntime.Id = QueuesTestSupport.fakeId(QueueRuntime__c.SObjectType, 71);
        QueueRuntime__c runtimeToClear = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 2);
        runtimeToClear.Id = insertedRuntime.Id;
        runtimeToClear.WorkerToken__c = 'worker-token';
        runtimeToClear.NextPumpAt__c = System.now().addMinutes(1);
        runtimeToClear.AsyncApexJobId__c = fakeAsyncApexJobId(0);

        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryRuntimes', new List<QueueRuntime__c>())
            .withReturn('insertRuntime', insertedRuntime);
        QueuesRuntimeRepository repositoryStub = (QueuesRuntimeRepository) Test.createStub(
            QueuesRuntimeRepository.class,
            repositoryProvider
        );

        QueuesRuntimeService service = new QueuesRuntimeService(
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            repositoryStub,
            new QueuesDefinitionService()
        );

        // Act
        QueueRuntime__c ensuredRuntime = service.ensureRuntime(QueuesTestSupport.DEFAULT_QUEUE_NAME);
        QueueRuntime__c incrementedRuntime = service.incrementActiveWorkerCount(runtimeToClear);
        QueueRuntime__c decrementedRuntime = service.decrementActiveWorkerCount(runtimeToClear);
        QueueRuntime__c clearedRuntime = service.clearWorkerAndUpdate(runtimeToClear);

        // Assert
        Assert.areEqual(insertedRuntime.Id, ensuredRuntime.Id);
        Assert.areEqual(runtimeToClear, incrementedRuntime);
        Assert.areEqual(runtimeToClear, decrementedRuntime);
        Assert.areEqual(runtimeToClear, clearedRuntime);
        Assert.areEqual(1, repositoryProvider.getCallCount('insertRuntime'));
        Assert.areEqual(1, repositoryProvider.getCallCount('queryRuntimes'));
        Assert.areEqual(1, repositoryProvider.getCallCount('updateRuntime'));
        Assert.areEqual(2, QueuesSupport.toInteger(runtimeToClear.ActiveWorkerCount__c, 0));
        Assert.areEqual(null, runtimeToClear.WorkerToken__c);
        Assert.areEqual(null, runtimeToClear.NextPumpAt__c);
        Assert.areEqual(null, runtimeToClear.AsyncApexJobId__c);
    }

    @IsTest
    static void should_return_existing_runtime_without_inserting_when_runtime_already_exists() {
        // Arrange
        QueueRuntime__c existingRuntime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        existingRuntime.Id = QueuesTestSupport.fakeId(QueueRuntime__c.SObjectType, 73);

        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryRuntimes', new List<QueueRuntime__c>{ existingRuntime });
        QueuesRuntimeRepository repositoryStub = (QueuesRuntimeRepository) Test.createStub(
            QueuesRuntimeRepository.class,
            repositoryProvider
        );

        QueuesRuntimeService service = new QueuesRuntimeService(
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            repositoryStub,
            new QueuesDefinitionService()
        );

        // Act
        QueueRuntime__c ensuredRuntime = service.ensureRuntime(QueuesTestSupport.DEFAULT_QUEUE_NAME);

        // Assert
        Assert.areEqual(existingRuntime.Id, ensuredRuntime.Id);
        Assert.areEqual(1, repositoryProvider.getCallCount('queryRuntimes'));
        Assert.areEqual(0, repositoryProvider.getCallCount('insertRuntime'));
    }

    @IsTest
    static void should_return_existing_runtime_when_unique_insert_races_with_another_transaction() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'duplicate-runtime-' + System.now().getTime();
            insert new QueueRuntime__c(QueueName__c = queueName, IsPaused__c = false, ActiveWorkerCount__c = 0);

            DmlException duplicateInsertException;
            try {
                insert new QueueRuntime__c(QueueName__c = queueName, IsPaused__c = false, ActiveWorkerCount__c = 0);
                Assert.fail('Expected duplicate QueueRuntime insert to fail.');
            } catch (DmlException ex) {
                duplicateInsertException = ex;
            }

            QueueRuntime__c existingRuntime = QueuesTestSupport.runtime(queueName, false, 0);
            existingRuntime.Id = QueuesTestSupport.fakeId(QueueRuntime__c.SObjectType, 72);

            QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns(
                    'queryRuntimes',
                    new List<Object>{ new List<QueueRuntime__c>(), new List<QueueRuntime__c>{ existingRuntime } }
                )
                .withException('insertRuntime', duplicateInsertException);
            QueuesRuntimeRepository repositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                repositoryProvider
            );

            QueuesRuntimeService service = new QueuesRuntimeService(
                new QueuesAsyncApexJobRepository(),
                new QueuesScheduleRepository(),
                repositoryStub,
                new QueuesDefinitionService()
            );

            // Act
            QueueRuntime__c ensuredRuntime = service.ensureRuntime(queueName);

            // Assert
            Assert.areEqual(existingRuntime.Id, ensuredRuntime.Id);
            Assert.areEqual(1, repositoryProvider.getCallCount('insertRuntime'));
            Assert.areEqual(2, repositoryProvider.getCallCount('queryRuntimes'));
        }
    }

    @IsTest
    static void should_return_existing_runtime_as_system_when_unique_insert_races_with_another_transaction() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'duplicate-runtime-system-' + System.now().getTime();
            insert new QueueRuntime__c(QueueName__c = queueName, IsPaused__c = false, ActiveWorkerCount__c = 0);

            DmlException duplicateInsertException;
            try {
                insert new QueueRuntime__c(QueueName__c = queueName, IsPaused__c = false, ActiveWorkerCount__c = 0);
                Assert.fail('Expected duplicate QueueRuntime insert to fail.');
            } catch (DmlException ex) {
                duplicateInsertException = ex;
            }

            QueueRuntime__c existingRuntime = QueuesTestSupport.runtime(queueName, false, 0);
            existingRuntime.Id = QueuesTestSupport.fakeId(QueueRuntime__c.SObjectType, 74);

            QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns(
                    'queryRuntimes',
                    new List<Object>{ new List<QueueRuntime__c>(), new List<QueueRuntime__c>{ existingRuntime } }
                )
                .withException('insertRuntime', duplicateInsertException);
            QueuesRuntimeRepository repositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                repositoryProvider
            );

            QueuesRuntimeService service = new QueuesRuntimeService(
                new QueuesAsyncApexJobRepository(),
                new QueuesScheduleRepository(),
                repositoryStub,
                new QueuesDefinitionService()
            );

            // Act
            QueueRuntime__c ensuredRuntime = service.ensureRuntimeAsSystem(queueName);

            // Assert
            Assert.areEqual(existingRuntime.Id, ensuredRuntime.Id);
            Assert.areEqual(1, repositoryProvider.getCallCount('insertRuntime'));
            Assert.areEqual(2, repositoryProvider.getCallCount('queryRuntimes'));
        }
    }

    @IsTest
    static void should_evaluate_worker_liveness_when_runtime_state_varies() {
        // Arrange
        Datetime currentNow = System.now();
        AsyncApexJob queuedAsyncJob = (AsyncApexJob) JSON.deserialize('{"Status":"Queued"}', AsyncApexJob.class);
        AsyncApexJob blankStatusAsyncJob = (AsyncApexJob) JSON.deserialize('{"Status":""}', AsyncApexJob.class);
        AsyncApexJob invalidStatusAsyncJob = (AsyncApexJob) JSON.deserialize(
            '{"Status":"Definitely Not Real"}',
            AsyncApexJob.class
        );
        QueuesTestSupport.RecordingStubProvider asyncRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns(
                'queryAsyncJobs',
                new List<Object>{
                    new List<AsyncApexJob>{ queuedAsyncJob },
                    new List<AsyncApexJob>{ blankStatusAsyncJob },
                    new List<AsyncApexJob>{ invalidStatusAsyncJob }
                }
            );
        QueuesAsyncApexJobRepository asyncRepositoryStub = (QueuesAsyncApexJobRepository) Test.createStub(
            QueuesAsyncApexJobRepository.class,
            asyncRepositoryProvider
        );
        QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns(
                'queryScheduledJobById',
                new List<Object>{
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            fakeScheduledJobId(11),
                            'WAITING',
                            '0 0 0 1 1 ? 2099',
                            'Queues test'
                        )
                    },
                    new List<CronTrigger>()
                }
            );
        QueuesScheduleRepository scheduleRepositoryStub = (QueuesScheduleRepository) Test.createStub(
            QueuesScheduleRepository.class,
            scheduleRepositoryProvider
        );
        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('findQueueDefinitionAsSystem', QueuesTestSupport.queueDefinition())
        );

        QueuesRuntimeService service = new QueuesRuntimeService(
            asyncRepositoryStub,
            scheduleRepositoryStub,
            new QueuesRuntimeRepository(),
            definitionStub
        );

        QueueRuntime__c runtimeWithoutNextPump = QueuesTestSupport.runtime(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            false,
            1
        );
        runtimeWithoutNextPump.WorkerToken__c = 'worker-token';

        QueueRuntime__c runtimeWithFuturePump = QueuesTestSupport.runtime(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            false,
            0
        );
        runtimeWithFuturePump.WorkerToken__c = 'worker-token';
        runtimeWithFuturePump.NextPumpAt__c = currentNow.addMinutes(2);

        QueueRuntime__c runtimeWithFutureScheduledJob = QueuesTestSupport.runtime(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            false,
            0
        );
        runtimeWithFutureScheduledJob.WorkerToken__c = 'worker-token';
        runtimeWithFutureScheduledJob.NextPumpAt__c = currentNow.addMinutes(2);
        runtimeWithFutureScheduledJob.AsyncApexJobId__c = fakeScheduledJobIdValue(11);

        QueueRuntime__c runtimeWithMissingFutureScheduledJob = QueuesTestSupport.runtime(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            false,
            0
        );
        runtimeWithMissingFutureScheduledJob.WorkerToken__c = 'worker-token';
        runtimeWithMissingFutureScheduledJob.NextPumpAt__c = currentNow.addMinutes(2);
        runtimeWithMissingFutureScheduledJob.AsyncApexJobId__c = fakeScheduledJobIdValue(12);

        QueueRuntime__c runtimeWithRecentPump = QueuesTestSupport.runtime(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            false,
            0
        );
        runtimeWithRecentPump.WorkerToken__c = 'worker-token';
        runtimeWithRecentPump.NextPumpAt__c = currentNow.addSeconds(-30);

        QueueRuntime__c runtimeWithAsyncJob = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtimeWithAsyncJob.WorkerToken__c = 'worker-token';
        runtimeWithAsyncJob.NextPumpAt__c = currentNow.addMinutes(-10);
        runtimeWithAsyncJob.AsyncApexJobId__c = fakeAsyncApexJobId(0);

        QueueRuntime__c runtimeWithBlankStatusJob = QueuesTestSupport.runtime(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            false,
            0
        );
        runtimeWithBlankStatusJob.WorkerToken__c = 'worker-token';
        runtimeWithBlankStatusJob.NextPumpAt__c = currentNow.addMinutes(-10);
        runtimeWithBlankStatusJob.AsyncApexJobId__c = fakeAsyncApexJobId(1);

        QueueRuntime__c runtimeWithInvalidStatusJob = QueuesTestSupport.runtime(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            false,
            0
        );
        runtimeWithInvalidStatusJob.WorkerToken__c = 'worker-token';
        runtimeWithInvalidStatusJob.NextPumpAt__c = currentNow.addMinutes(-10);
        runtimeWithInvalidStatusJob.AsyncApexJobId__c = fakeAsyncApexJobId(2);

        QueueRuntime__c runtimeWithFreshHeartbeat = QueuesTestSupport.runtime(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            false,
            0
        );
        runtimeWithFreshHeartbeat.WorkerToken__c = 'worker-token';
        runtimeWithFreshHeartbeat.NextPumpAt__c = currentNow.addMinutes(-4);
        runtimeWithFreshHeartbeat.LastPumpAt__c = currentNow.addMinutes(-1);

        QueueRuntime__c staleRuntime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        staleRuntime.WorkerToken__c = 'worker-token';
        staleRuntime.NextPumpAt__c = currentNow.addMinutes(-10);
        staleRuntime.LastPumpAt__c = currentNow.addMinutes(-8);

        // Act
        Boolean aliveWithoutNextPump = service.isWorkerAlive(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            runtimeWithoutNextPump,
            currentNow
        );
        Boolean aliveWithFuturePump = service.isWorkerAlive(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            runtimeWithFuturePump,
            currentNow
        );
        Boolean aliveWithFutureScheduledJob = service.isWorkerAlive(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            runtimeWithFutureScheduledJob,
            currentNow
        );
        Boolean aliveWithMissingFutureScheduledJob = service.isWorkerAlive(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            runtimeWithMissingFutureScheduledJob,
            currentNow
        );
        Boolean aliveWithRecentPump = service.isWorkerAlive(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            runtimeWithRecentPump,
            currentNow
        );
        Boolean aliveWithAsyncJob = service.isWorkerAlive(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            runtimeWithAsyncJob,
            currentNow
        );
        Boolean aliveWithBlankStatusJob = service.isWorkerAlive(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            runtimeWithBlankStatusJob,
            currentNow
        );
        Boolean aliveWithInvalidStatusJob = service.isWorkerAlive(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            runtimeWithInvalidStatusJob,
            currentNow
        );
        Boolean aliveWithFreshHeartbeat = service.isWorkerAlive(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            runtimeWithFreshHeartbeat,
            currentNow
        );
        Boolean keepActiveWorkerWithoutNextPump = service.shouldKeepWorker(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            runtimeWithoutNextPump,
            currentNow.addMinutes(1),
            currentNow
        );
        Boolean shouldClearStaleWorker = service.shouldClearStaleWorker(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            staleRuntime,
            currentNow
        );

        // Assert
        Assert.areEqual(true, aliveWithoutNextPump);
        Assert.areEqual(true, aliveWithFuturePump);
        Assert.areEqual(true, aliveWithFutureScheduledJob);
        Assert.areEqual(false, aliveWithMissingFutureScheduledJob);
        Assert.areEqual(true, aliveWithRecentPump);
        Assert.areEqual(true, aliveWithAsyncJob);
        Assert.areEqual(false, aliveWithBlankStatusJob);
        Assert.areEqual(false, aliveWithInvalidStatusJob);
        Assert.areEqual(true, aliveWithFreshHeartbeat);
        Assert.areEqual(true, keepActiveWorkerWithoutNextPump);
        Assert.areEqual(true, shouldClearStaleWorker);
        Assert.areEqual(3, asyncRepositoryProvider.getCallCount('queryAsyncJobs'));
        Assert.areEqual(2, scheduleRepositoryProvider.getCallCount('queryScheduledJobById'));
    }

    @IsTest
    static void should_update_worker_job_id_and_clear_runtime_when_worker_is_current() {
        // Arrange
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';
        runtime.NextPumpAt__c = System.now().addMinutes(1);
        runtime.AsyncApexJobId__c = fakeAsyncApexJobId(0);

        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturns(
                'queryRuntimesForUpdate',
                new List<Object>{ new List<QueueRuntime__c>{ runtime }, new List<QueueRuntime__c>{ runtime } }
            );
        QueuesRuntimeRepository repositoryStub = (QueuesRuntimeRepository) Test.createStub(
            QueuesRuntimeRepository.class,
            repositoryProvider
        );

        QueuesRuntimeService service = new QueuesRuntimeService(
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            repositoryStub,
            new QueuesDefinitionService()
        );

        // Act
        Boolean updatedCurrentWorkerJob = service.updateWorkerJobId(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'worker-token',
            '707NEW'
        );
        Boolean updatedOtherWorkerJob = service.updateWorkerJobId(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            'other-token',
            '707OTHER'
        );
        Boolean isCurrentWorker = service.isCurrentWorker(runtime, 'worker-token');
        Boolean isBlankTokenCurrentWorker = service.isCurrentWorker(runtime, '');
        service.clearWorker(runtime);

        // Assert
        Assert.areEqual(1, repositoryProvider.getCallCount('updateRuntime'));
        Assert.areEqual(true, updatedCurrentWorkerJob);
        Assert.areEqual(false, updatedOtherWorkerJob);
        Assert.areEqual(true, isCurrentWorker);
        Assert.areEqual(false, isBlankTokenCurrentWorker);
        Assert.areEqual(null, runtime.NextPumpAt__c);
        Assert.areEqual(null, runtime.WorkerToken__c);
        Assert.areEqual(null, runtime.AsyncApexJobId__c);
    }

    @IsTest
    static void should_clear_worker_and_update_runtime_as_system() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'runtime-clear-system-' + System.now().getTime();
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
            runtime.WorkerToken__c = 'worker-token';
            runtime.NextPumpAt__c = System.now().addMinutes(1);
            runtime.AsyncApexJobId__c = fakeAsyncApexJobId(9);
            insert runtime;

            QueuesRuntimeService service = new QueuesRuntimeService();

            // Act
            QueueRuntime__c clearedRuntime = service.clearWorkerAndUpdateAsSystem(runtime);

            // Assert
            QueueRuntime__c persistedRuntime = [
                SELECT WorkerToken__c, NextPumpAt__c, AsyncApexJobId__c
                FROM QueueRuntime__c
                WHERE Id = :runtime.Id
            ];
            Assert.areEqual(runtime.Id, clearedRuntime.Id);
            Assert.areEqual(null, persistedRuntime.WorkerToken__c);
            Assert.areEqual(null, persistedRuntime.NextPumpAt__c);
            Assert.areEqual(null, persistedRuntime.AsyncApexJobId__c);
        }
    }

    @IsTest
    static void should_return_null_or_false_when_runtime_inputs_are_missing() {
        // Arrange
        Datetime currentNow = System.now();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);

        QueuesRuntimeService service = new QueuesRuntimeService();

        // Act
        QueueRuntime__c incrementedRuntime = service.incrementActiveWorkerCount(null);
        QueueRuntime__c decrementedRuntime = service.decrementActiveWorkerCount(null);
        Boolean keepWorker = service.shouldKeepWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, runtime, null, currentNow);
        Boolean workerAlive = service.isWorkerAlive(QueuesTestSupport.DEFAULT_QUEUE_NAME, runtime, null);
        service.clearWorker(null);
        QueueRuntime__c clearedRuntime = service.clearWorkerAndUpdate(null);
        QueueRuntime__c clearedSystemRuntime = service.clearWorkerAndUpdateAsSystem(null);

        // Assert
        Assert.areEqual(null, incrementedRuntime);
        Assert.areEqual(null, decrementedRuntime);
        Assert.areEqual(false, keepWorker);
        Assert.areEqual(false, workerAlive);
        Assert.areEqual(null, clearedRuntime);
        Assert.areEqual(null, clearedSystemRuntime);
    }

    @IsTest
    static void should_not_keep_worker_when_desired_wake_moves_earlier_than_live_worker_plan() {
        // Arrange
        Datetime currentNow = System.now();
        QueueRuntime__c runtime = QueuesTestSupport.runtime(QueuesTestSupport.DEFAULT_QUEUE_NAME, false, 0);
        runtime.WorkerToken__c = 'worker-token';
        runtime.NextPumpAt__c = currentNow.addMinutes(5);

        QueuesRuntimeService service = new QueuesRuntimeService();

        // Act
        Boolean keepWorker = service.shouldKeepWorker(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            runtime,
            currentNow.addMinutes(1),
            currentNow
        );

        // Assert
        Assert.areEqual(false, keepWorker);
    }

    @IsTest
    static void should_rethrow_runtime_insert_exception_when_duplicate_race_cannot_be_resolved() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'missing-runtime-' + System.now().getTime();
            insert new QueueRuntime__c(QueueName__c = queueName, IsPaused__c = false, ActiveWorkerCount__c = 0);

            DmlException duplicateInsertException;
            try {
                insert new QueueRuntime__c(QueueName__c = queueName, IsPaused__c = false, ActiveWorkerCount__c = 0);
                Assert.fail('Expected duplicate QueueRuntime insert to fail.');
            } catch (DmlException ex) {
                duplicateInsertException = ex;
            }

            QueuesRuntimeRepository repositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturns(
                        'queryRuntimes',
                        new List<Object>{ new List<QueueRuntime__c>(), new List<QueueRuntime__c>() }
                    )
                    .withException('insertRuntime', duplicateInsertException)
            );

            QueuesRuntimeService service = new QueuesRuntimeService(
                new QueuesAsyncApexJobRepository(),
                new QueuesScheduleRepository(),
                repositoryStub,
                new QueuesDefinitionService()
            );

            String thrownMessage;

            // Act
            try {
                service.ensureRuntime(queueName);
                Assert.fail('Expected duplicate insert to be rethrown when runtime cannot be reloaded.');
            } catch (DmlException ex) {
                thrownMessage = ex.getMessage();
            }

            // Assert
            Assert.areEqual(true, String.isNotBlank(thrownMessage));
        }
    }

    @IsTest
    static void should_rethrow_non_duplicate_runtime_insert_exception_when_insert_fails() {
        // Arrange
        DmlException insertException;
        try {
            update new QueueRuntime__c(Id = QueuesTestSupport.fakeId(QueueRuntime__c.SObjectType, 73));
            Assert.fail('Expected invalid runtime update to fail.');
        } catch (DmlException ex) {
            insertException = ex;
        }

        QueuesRuntimeRepository repositoryStub = (QueuesRuntimeRepository) Test.createStub(
            QueuesRuntimeRepository.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryRuntimes', new List<QueueRuntime__c>())
                .withException('insertRuntime', insertException)
        );

        QueuesRuntimeService service = new QueuesRuntimeService(
            new QueuesAsyncApexJobRepository(),
            new QueuesScheduleRepository(),
            repositoryStub,
            new QueuesDefinitionService()
        );

        String thrownMessage;

        // Act
        try {
            service.ensureRuntime('non-duplicate-runtime-error');
            Assert.fail('Expected non-duplicate insert exception to be rethrown.');
        } catch (DmlException ex) {
            thrownMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual(true, String.isNotBlank(thrownMessage));
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
