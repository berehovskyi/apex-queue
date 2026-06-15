@IsTest(IsParallel=false)
private class QueuesQueueAdminServiceTest {
    private static final String WORKER_MANAGER_ALIAS = 'qwmgr';
    private static final String READ_ONLY_ALIAS = 'qwred';

    @TestSetup
    static void setupWorkerManagers() {
        Profile standardProfile = [SELECT Id FROM Profile WHERE Name = 'Standard User' LIMIT 1];
        insert new List<User>{
            newUser(standardProfile.Id, WORKER_MANAGER_ALIAS),
            newUser(standardProfile.Id, READ_ONLY_ALIAS)
        };

        PermissionSet adminPermissionSet = [SELECT Id FROM PermissionSet WHERE Name = 'QueuesAdmin' LIMIT 1];
        insert new PermissionSetAssignment(
            AssigneeId = queryUser(WORKER_MANAGER_ALIAS).Id,
            PermissionSetId = adminPermissionSet.Id
        );
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_construct_default_service() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Act + Assert
            Assert.areNotEqual(null, new QueuesQueueAdminService());
        }
    }

    @IsTest
    static void should_inspect_and_reorder_holding_worker() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-worker-priority';
            Id workerJobId = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 401);
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 1);
            runtime.AsyncApexJobId__c = workerJobId;
            AsyncApexJob holdingJob = (AsyncApexJob) JSON.deserialize(
                '{"Id":"' + workerJobId + '","Status":"Holding"}',
                AsyncApexJob.class
            );
            QueuesTestSupport.RecordingStubProvider asyncJobProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryAsyncJobs', new List<AsyncApexJob>{ holdingJob })
                .withReturn('moveJobToFront', true)
                .withReturn('moveJobToEnd', true);
            QueuesQueueAdminService service = new QueuesQueueAdminService(
                (QueuesDefinitionService) Test.createStub(
                    QueuesDefinitionService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, true))
                ),
                (QueuesRuntimeService) Test.createStub(
                    QueuesRuntimeService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('findRuntime', runtime)
                        .withReturn('lockRuntime', runtime)
                ),
                new QueuesJobService(),
                new QueuesJobRunService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                (QueuesAsyncApexJobRepository) Test.createStub(QueuesAsyncApexJobRepository.class, asyncJobProvider),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                new QueuesWorkerService()
            );

            // Act
            Queues.WorkerStatus workerStatus;
            Boolean movedToFront;
            Boolean movedToEnd;
            System.runAs(queryUser(WORKER_MANAGER_ALIAS)) {
                workerStatus = service.getWorkerStatus(queueName);
                movedToFront = service.moveWorkerToFront(queueName);
                movedToEnd = service.moveWorkerToEnd(queueName);
            }

            // Assert
            Assert.areEqual(queueName, workerStatus.getQueueName());
            Assert.areEqual(workerJobId, workerStatus.getJobId());
            Assert.areEqual('Holding', workerStatus.getStatus());
            Assert.areEqual(true, workerStatus.hasWorker());
            Assert.areEqual(true, workerStatus.isHolding());
            Assert.areEqual(true, movedToFront);
            Assert.areEqual(true, movedToEnd);
            Assert.areEqual(3, asyncJobProvider.getCallCount('queryAsyncJobs'));
            Assert.areEqual(1, asyncJobProvider.getCallCount('moveJobToFront'));
            Assert.areEqual(1, asyncJobProvider.getCallCount('moveJobToEnd'));
        }
    }

    @IsTest
    static void should_not_reorder_worker_that_is_not_holding() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-worker-not-holding';
            Id workerJobId = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 402);
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 1);
            runtime.AsyncApexJobId__c = workerJobId;
            AsyncApexJob queuedJob = (AsyncApexJob) JSON.deserialize(
                '{"Id":"' + workerJobId + '","Status":"Queued"}',
                AsyncApexJob.class
            );
            QueuesTestSupport.RecordingStubProvider asyncJobProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryAsyncJobs', new List<AsyncApexJob>{ queuedJob });
            QueuesQueueAdminService service = new QueuesQueueAdminService(
                (QueuesDefinitionService) Test.createStub(
                    QueuesDefinitionService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, true))
                ),
                (QueuesRuntimeService) Test.createStub(
                    QueuesRuntimeService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('findRuntime', runtime)
                        .withReturn('lockRuntime', runtime)
                ),
                new QueuesJobService(),
                new QueuesJobRunService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                (QueuesAsyncApexJobRepository) Test.createStub(QueuesAsyncApexJobRepository.class, asyncJobProvider),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                new QueuesWorkerService()
            );

            // Act
            Queues.WorkerStatus workerStatus;
            Boolean movedToFront;
            System.runAs(queryUser(WORKER_MANAGER_ALIAS)) {
                workerStatus = service.getWorkerStatus(queueName);
                movedToFront = service.moveWorkerToFront(queueName);
            }

            // Assert
            Assert.areEqual(true, workerStatus.hasWorker());
            Assert.areEqual(false, workerStatus.isHolding());
            Assert.areEqual(false, movedToFront);
            Assert.areEqual(0, asyncJobProvider.getCallCount('moveJobToFront'));
        }
    }

    @IsTest
    static void should_require_manage_workers_permission_to_reorder_worker() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-worker-permission';
            QueuesQueueAdminService service = new QueuesQueueAdminService();
            String failureMessage;

            // Act
            System.runAs(queryUser(READ_ONLY_ALIAS)) {
                try {
                    service.moveWorkerToFront(queueName);
                    Assert.fail('Expected worker reordering to require QueuesManageWorkers.');
                } catch (Queues.QueueException ex) {
                    failureMessage = ex.getMessage();
                }
            }

            // Assert
            Assert.areEqual('QueuesManageWorkers permission is required.', failureMessage);
        }
    }

    @IsTest
    static void should_report_missing_worker() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-worker-missing';
            QueuesTestSupport.RecordingStubProvider asyncJobProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryAsyncJobs', new List<AsyncApexJob>());
            QueuesQueueAdminService service = new QueuesQueueAdminService(
                (QueuesDefinitionService) Test.createStub(
                    QueuesDefinitionService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, true))
                ),
                (QueuesRuntimeService) Test.createStub(
                    QueuesRuntimeService.class,
                    new QueuesTestSupport.RecordingStubProvider().withReturn('findRuntime', null)
                ),
                new QueuesJobService(),
                new QueuesJobRunService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                (QueuesAsyncApexJobRepository) Test.createStub(QueuesAsyncApexJobRepository.class, asyncJobProvider),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                new QueuesWorkerService()
            );

            // Act
            Queues.WorkerStatus workerStatus = service.getWorkerStatus(queueName);

            // Assert
            Assert.areEqual(false, workerStatus.hasWorker());
            Assert.areEqual(false, workerStatus.isHolding());
            Assert.areEqual(null, workerStatus.getStatus());
        }
    }

    @IsTest
    static void should_pause_queue_when_queue_is_paused() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-unit';
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 1);
            runtime.WorkerToken__c = 'worker-token';

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, true))
            );
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', runtime)
                .withReturn('lockRuntime', runtime)
                .withReturn('clearWorkerAndUpdate', runtime);
            QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
                QueuesRuntimeService.class,
                runtimeProvider
            );
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesWorkerService workerStub = (QueuesWorkerService) Test.createStub(
                QueuesWorkerService.class,
                workerProvider
            );

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                runtimeStub,
                new QueuesJobService(),
                new QueuesJobRunService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                workerStub
            );

            // Act
            QueueRuntime__c pausedRuntime = service.pause(queueName);

            // Assert
            Assert.areEqual(runtime, pausedRuntime);
            Assert.areEqual(true, pausedRuntime.IsPaused__c);
            Assert.areEqual(1, runtimeProvider.getCallCount('ensureRuntime'));
            Assert.areEqual(1, runtimeProvider.getCallCount('lockRuntime'));
            Assert.areEqual(1, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
            Assert.areEqual(1, workerProvider.getCallCount('cancelFutureScheduledWorker'));
        }
    }

    @IsTest
    static void should_delegate_wake_when_queue_is_woken() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-unit';
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, true))
            );
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureWorker', true);
            QueuesWorkerService workerStub = (QueuesWorkerService) Test.createStub(
                QueuesWorkerService.class,
                workerProvider
            );

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                new QueuesRuntimeService(),
                new QueuesJobService(),
                new QueuesJobRunService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                workerStub
            );

            // Act
            Integer wakeResult = service.wake(queueName);

            // Assert
            Assert.areEqual(1, wakeResult);
            Assert.areEqual(1, workerProvider.getCallCount('ensureWorker'));
        }
    }

    @IsTest
    static void should_resume_queue_when_runtime_is_restarted() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-resume';
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, true, 0);

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, true))
            );
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', runtime)
                .withReturn('lockRuntime', runtime)
                .withReturn('updateRuntime', runtime);
            QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
                QueuesRuntimeService.class,
                runtimeProvider
            );
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureWorkerForNextWakeAt', true);
            QueuesWorkerService workerStub = (QueuesWorkerService) Test.createStub(
                QueuesWorkerService.class,
                workerProvider
            );

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                runtimeStub,
                new QueuesJobService(),
                new QueuesJobRunService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                workerStub
            );

            // Act
            QueueRuntime__c resumedRuntime = service.resume(queueName);

            // Assert
            Assert.areEqual(runtime, resumedRuntime);
            Assert.areEqual(false, resumedRuntime.IsPaused__c);
            Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
            Assert.areEqual(1, workerProvider.getCallCount('ensureWorkerForNextWakeAt'));
        }
    }

    @IsTest
    static void should_recover_stalled_jobs_and_restart_worker_when_queue_is_active() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-recover';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            Job__c recoverableJob = QueuesTestSupport.job(queueName, 'sync', Queues.State.ACTIVE.name(), 'recoverable');
            recoverableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 501);
            recoverableJob.AttemptsMade__c = 0;
            recoverableJob.MaxAttempts__c = 3;
            recoverableJob.LeaseToken__c = 'recoverable-lease';
            recoverableJob.LeaseExpiresAt__c = System.now().addMinutes(-10);
            recoverableJob.AsyncApexJobId__c = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 501);
            recoverableJob.ReturnValue__c = '{"stale":true}';
            Job__c exhaustedJob = QueuesTestSupport.job(queueName, 'sync', Queues.State.ACTIVE.name(), 'exhausted');
            exhaustedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 502);
            exhaustedJob.AttemptsMade__c = 1;
            exhaustedJob.MaxAttempts__c = 1;
            exhaustedJob.LeaseToken__c = 'exhausted-lease';
            exhaustedJob.LeaseExpiresAt__c = System.now().addMinutes(-10);
            exhaustedJob.AsyncApexJobId__c = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 502);
            exhaustedJob.ReturnValue__c = '{"stale":true}';
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 2);
            runtime.WorkerToken__c = '';

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', runtime)
                .withReturn('lockRuntime', runtime)
                .withReturn('shouldClearStaleWorker', true)
                .withReturn('updateRuntime', runtime);
            QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
                QueuesRuntimeService.class,
                runtimeProvider
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryExpiredActiveJobsForUpdate', new List<Job__c>{ recoverableJob, exhaustedJob })
                .withReturn('queryExpiredActiveQueueableJobsForUpdate', new List<Job__c>())
                .withReturn('hasActiveJobs', false);
            QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
                QueuesJobService.class,
                jobServiceProvider
            );
            QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesJobRunService jobRunStub = (QueuesJobRunService) Test.createStub(
                QueuesJobRunService.class,
                jobRunProvider
            );
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureWorkerForNextWakeAt', true);
            QueuesWorkerService workerStub = (QueuesWorkerService) Test.createStub(
                QueuesWorkerService.class,
                workerProvider
            );

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                runtimeStub,
                jobServiceStub,
                jobRunStub,
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                workerStub
            );

            // Act
            Integer recoveredCount = service.recoverStalled(queueName);

            // Assert
            Assert.areEqual(2, recoveredCount);
            Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(recoverableJob.State__c));
            Assert.areEqual(Queues.State.FAILED, QueuesSupport.toState(exhaustedJob.State__c));
            Assert.areEqual(null, recoverableJob.LeaseToken__c);
            Assert.areEqual(null, recoverableJob.LeaseExpiresAt__c);
            Assert.areEqual(null, recoverableJob.AsyncApexJobId__c);
            Assert.areEqual(null, recoverableJob.ReturnValue__c);
            Assert.areEqual(null, exhaustedJob.LeaseToken__c);
            Assert.areEqual(null, exhaustedJob.LeaseExpiresAt__c);
            Assert.areEqual(null, exhaustedJob.AsyncApexJobId__c);
            Assert.areEqual(null, exhaustedJob.ReturnValue__c);
            Assert.areEqual(0, QueuesSupport.toInteger(runtime.ActiveWorkerCount__c, -1));
            Assert.areNotEqual(null, runtime.LastSweepAt__c);
            Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
            Assert.areEqual(1, jobRunProvider.getCallCount('insertJobRuns'));
            List<JobRun__c> runs = (List<JobRun__c>) jobRunProvider.getLastArgs('insertJobRuns')[0];
            Assert.areEqual(2, runs.size());
            Assert.areNotEqual(null, runs[0].FinishedAt__c);
            Assert.areNotEqual(null, runs[0].DurationMs__c);
            Assert.areEqual(1, runtimeProvider.getCallCount('clearWorker'));
            Assert.areEqual(1, workerProvider.getCallCount('syncWorkerForNextWakeAt'));
        }
    }

    @IsTest
    static void should_recover_stalled_queueable_jobs_and_enqueue_executor_attempts() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-recover-queueable';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, false);
            Job__c queueableJob = QueuesTestSupport.job(
                queueName,
                'sync',
                Queues.State.ACTIVE.name(),
                'queueable-stall'
            );
            queueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 611);
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            queueableJob.AttemptsMade__c = 0;
            queueableJob.MaxAttempts__c = 3;
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
            runtime.WorkerToken__c = '';

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('requireQueueDefinition', queueDefinition)
                    .withReturn('getLeaseMinutes', 5)
            );
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', runtime)
                .withReturn('lockRuntime', runtime)
                .withReturn('shouldClearStaleWorker', false)
                .withReturn('updateRuntime', runtime);
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryExpiredActiveJobsForUpdate', new List<Job__c>())
                .withReturn('queryExpiredActiveQueueableJobsForUpdate', new List<Job__c>{ queueableJob })
                .withReturn('hasActiveJobs', false);
            QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider queueableExecutionProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('enqueueJob', queueableJob);

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
                new QueuesQueueableDispatchService(),
                (QueuesQueueableExecutionService) Test.createStub(
                    QueuesQueueableExecutionService.class,
                    queueableExecutionProvider
                ),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                new QueuesWorkerService()
            );

            // Act
            Integer recoveredCount = service.recoverStalled(queueName);

            // Assert
            Assert.areEqual(1, recoveredCount);
            Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(queueableJob.State__c));
            Assert.areEqual(1, jobServiceProvider.getCallCount('queryExpiredActiveQueueableJobsForUpdate'));
            Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
            Assert.areEqual(1, jobRunProvider.getCallCount('insertJobRuns'));
            Assert.areEqual(1, queueableExecutionProvider.getCallCount('enqueueJob'));
            Assert.areEqual(queueableJob.Id, queueableExecutionProvider.getLastArgs('enqueueJob')[0]);
        }
    }

    @IsTest
    static void should_recover_stale_initiated_queueable_jobs_and_enqueue_executor_attempts() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-recover-queueable-initiated';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, false);
            Job__c queueableJob = QueuesTestSupport.job(
                queueName,
                'sync',
                Queues.State.WAITING.name(),
                'queueable-stale'
            );
            queueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 614);
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            queueableJob.QueueableDispatchInitiatedAt__c = System.now().addMinutes(-10);
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
            runtime.WorkerToken__c = '';

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('requireQueueDefinition', queueDefinition)
                    .withReturn('getLeaseMinutes', 5)
            );
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', runtime)
                .withReturn('lockRuntime', runtime)
                .withReturn('shouldClearStaleWorker', false)
                .withReturn('updateRuntime', runtime);
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryExpiredActiveJobsForUpdate', new List<Job__c>())
                .withReturn('queryStaleInitiatedQueueableJobsForUpdate', new List<Job__c>{ queueableJob })
                .withReturn('queryExpiredActiveQueueableJobsForUpdate', new List<Job__c>())
                .withReturn('hasActiveJobs', false);
            QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider queueableExecutionProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('enqueueJob', queueableJob);

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
                new QueuesQueueableDispatchService(),
                (QueuesQueueableExecutionService) Test.createStub(
                    QueuesQueueableExecutionService.class,
                    queueableExecutionProvider
                ),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                new QueuesWorkerService()
            );

            // Act
            Integer recoveredCount = service.recoverStalled(queueName);

            // Assert
            Assert.areEqual(1, recoveredCount);
            Assert.areEqual(1, jobServiceProvider.getCallCount('queryStaleInitiatedQueueableJobsForUpdate'));
            Assert.areEqual(0, jobServiceProvider.getCallCount('updateJobs'));
            Assert.areEqual(0, jobRunProvider.getCallCount('insertJobRuns'));
            Assert.areEqual(1, queueableExecutionProvider.getCallCount('enqueueJob'));
            Assert.areEqual(queueableJob.Id, queueableExecutionProvider.getLastArgs('enqueueJob')[0]);
        }
    }

    @IsTest
    @SuppressWarnings('PMD.OperationWithLimitsInLoop')
    static void should_recover_stale_queueable_dispatches_before_initiated_jobs() {
        // Arrange
        String queueName = 'queue-admin-recover-queueable-dispatch';
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, false);
        Job__c queueableJob = QueuesTestSupport.job(queueName, 'sync', Queues.State.WAITING.name(), 'queueable-stale');
        queueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 616);
        queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
        queueableJob.QueueableDispatchInitiatedAt__c = System.now().addMinutes(-10);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
        runtime.WorkerToken__c = '';

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryExpiredActiveJobsForUpdate', new List<Job__c>())
            .withReturn('queryStaleInitiatedQueueableJobsForUpdate', new List<Job__c>{ queueableJob })
            .withReturn('queryExpiredActiveQueueableJobsForUpdate', new List<Job__c>())
            .withReturn('hasActiveJobs', false);
        QueuesTestSupport.RecordingStubProvider queueableDispatchProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('recoverStaleDispatches', 1);
        QueuesTestSupport.RecordingStubProvider queueableExecutionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('enqueueJob', queueableJob);
        QueuesQueueAdminService service = new QueuesQueueAdminService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('requireQueueDefinition', queueDefinition)
                    .withReturn('getLeaseMinutes', 5)
            ),
            (QueuesRuntimeService) Test.createStub(
                QueuesRuntimeService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('ensureRuntime', runtime)
                    .withReturn('lockRuntime', runtime)
                    .withReturn('shouldClearStaleWorker', false)
                    .withReturn('updateRuntime', runtime)
            ),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(
                QueuesJobRunService.class,
                new QueuesTestSupport.RecordingStubProvider()
            ),
            (QueuesQueueableDispatchService) Test.createStub(
                QueuesQueueableDispatchService.class,
                queueableDispatchProvider
            ),
            (QueuesQueueableExecutionService) Test.createStub(
                QueuesQueueableExecutionService.class,
                queueableExecutionProvider
            ),
            new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
            new QueuesStalledJobRecoveryService(),
            new QueuesFlowProducerService(),
            new QueuesWorkerService()
        );

        // Act
        Integer recoveredCount = service.recoverStalled(queueName);
        for (Integer index = 1; index < Limits.getLimitQueueableJobs(); index++) {
            System.enqueueJob(new NoopQueueable());
        }
        Integer sliceRecoveredCount = service.recoverStalled(queueName);

        // Assert
        Assert.areEqual(2, recoveredCount);
        Assert.areEqual(1, sliceRecoveredCount);
        Assert.areEqual(2, queueableDispatchProvider.getCallCount('recoverStaleDispatches'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('queryStaleInitiatedQueueableJobsForUpdate'));
        Assert.areEqual(1, queueableExecutionProvider.getCallCount('enqueueJob'));
        Assert.areEqual(queueName, queueableDispatchProvider.getLastArgs('recoverStaleDispatches')[0]);
    }

    @IsTest
    @SuppressWarnings('PMD.OperationWithLimitsInLoop')
    static void should_stop_queueable_recovery_when_stale_initiated_jobs_fill_current_slice() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-recover-queueable-initiated-slice';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, false);
            Job__c queueableJob = QueuesTestSupport.job(
                queueName,
                'sync',
                Queues.State.WAITING.name(),
                'queueable-stale'
            );
            queueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 615);
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            queueableJob.QueueableDispatchInitiatedAt__c = System.now().addMinutes(-10);
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
            runtime.WorkerToken__c = '';

            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryExpiredActiveJobsForUpdate', new List<Job__c>())
                .withReturn('queryStaleInitiatedQueueableJobsForUpdate', new List<Job__c>{ queueableJob })
                .withReturn('queryExpiredActiveQueueableJobsForUpdate', new List<Job__c>())
                .withReturn('hasActiveJobs', false);
            QueuesTestSupport.RecordingStubProvider queueableExecutionProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('enqueueJob', queueableJob);

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                (QueuesDefinitionService) Test.createStub(
                    QueuesDefinitionService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('requireQueueDefinition', queueDefinition)
                        .withReturn('getLeaseMinutes', 5)
                ),
                (QueuesRuntimeService) Test.createStub(
                    QueuesRuntimeService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('ensureRuntime', runtime)
                        .withReturn('lockRuntime', runtime)
                        .withReturn('shouldClearStaleWorker', false)
                        .withReturn('updateRuntime', runtime)
                ),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                (QueuesJobRunService) Test.createStub(
                    QueuesJobRunService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                ),
                new QueuesQueueableDispatchService(),
                (QueuesQueueableExecutionService) Test.createStub(
                    QueuesQueueableExecutionService.class,
                    queueableExecutionProvider
                ),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                new QueuesWorkerService()
            );

            for (Integer index = 1; index < Limits.getLimitQueueableJobs(); index++) {
                System.enqueueJob(new NoopQueueable());
            }

            // Act
            Integer recoveredCount = service.recoverStalled(queueName);

            // Assert
            Assert.areEqual(1, recoveredCount);
            Assert.areEqual(1, jobServiceProvider.getCallCount('queryStaleInitiatedQueueableJobsForUpdate'));
            Assert.areEqual(0, jobServiceProvider.getCallCount('queryExpiredActiveQueueableJobsForUpdate'));
            Assert.areEqual(1, queueableExecutionProvider.getCallCount('enqueueJob'));
            Assert.areEqual(queueableJob.Id, queueableExecutionProvider.getLastArgs('enqueueJob')[0]);
        }
    }

    @IsTest
    static void should_recover_stalled_queueable_jobs_as_system() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-recover-queueable-system';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, false);
            Job__c queueableJob = QueuesTestSupport.job(
                queueName,
                'sync',
                Queues.State.ACTIVE.name(),
                'queueable-system-stall'
            );
            queueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 612);
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            queueableJob.AttemptsMade__c = 0;
            queueableJob.MaxAttempts__c = 3;
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
            runtime.WorkerToken__c = '';

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('requireQueueDefinition', queueDefinition)
                    .withReturn('getLeaseMinutes', 5)
            );
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', runtime)
                .withReturn('lockRuntime', runtime)
                .withReturn('shouldClearStaleWorker', false)
                .withReturn('updateRuntime', runtime);
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryExpiredActiveJobsForUpdate', new List<Job__c>())
                .withReturn('queryExpiredActiveQueueableJobsForUpdate', new List<Job__c>{ queueableJob })
                .withReturn('hasActiveJobs', false);
            QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider queueableExecutionProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('enqueueJob', queueableJob);

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
                new QueuesQueueableDispatchService(),
                (QueuesQueueableExecutionService) Test.createStub(
                    QueuesQueueableExecutionService.class,
                    queueableExecutionProvider
                ),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                new QueuesWorkerService()
            );

            // Act
            Integer recoveredCount = service.recoverStalledAsSystem(queueName);

            // Assert
            Assert.areEqual(1, recoveredCount);
            Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(queueableJob.State__c));
            Assert.areEqual(1, jobServiceProvider.getCallCount('queryExpiredActiveQueueableJobsForUpdate'));
            Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
            Assert.areEqual(1, jobRunProvider.getCallCount('insertJobRuns'));
            Assert.areEqual(1, queueableExecutionProvider.getCallCount('enqueueJob'));
        }
    }

    @IsTest
    static void should_recover_stalled_worker_jobs_as_system() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-recover-worker-system';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, false);
            Job__c workerJob = QueuesTestSupport.job(
                queueName,
                'sync',
                Queues.State.ACTIVE.name(),
                'worker-system-stall'
            );
            workerJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 616);
            workerJob.ExecutionMode__c = Queues.ExecutionMode.WORKER.name();
            workerJob.AttemptsMade__c = 0;
            workerJob.MaxAttempts__c = 3;
            workerJob.LeaseToken__c = 'worker-lease';
            workerJob.LeaseExpiresAt__c = System.now().addMinutes(-10);
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
            runtime.WorkerToken__c = '';

            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryExpiredActiveJobsForUpdate', new List<Job__c>{ workerJob })
                .withReturn('queryExpiredActiveQueueableJobsForUpdate', new List<Job__c>())
                .withReturn('hasActiveJobs', false);
            QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                (QueuesDefinitionService) Test.createStub(
                    QueuesDefinitionService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('requireQueueDefinition', queueDefinition)
                        .withReturn('getLeaseMinutes', 5)
                ),
                (QueuesRuntimeService) Test.createStub(
                    QueuesRuntimeService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('ensureRuntime', runtime)
                        .withReturn('lockRuntime', runtime)
                        .withReturn('shouldClearStaleWorker', false)
                        .withReturn('updateRuntime', runtime)
                ),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                new QueuesWorkerService()
            );

            // Act
            Integer recoveredCount = service.recoverStalledAsSystem(queueName);

            // Assert
            Assert.areEqual(1, recoveredCount);
            Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(workerJob.State__c));
            Assert.areEqual(null, workerJob.LeaseToken__c);
            Assert.areEqual(null, workerJob.LeaseExpiresAt__c);
            Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
            Assert.areEqual(1, jobRunProvider.getCallCount('insertJobRuns'));
        }
    }

    @IsTest
    static void should_skip_stalled_queueable_recovery_when_async_job_is_still_active() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-recover-live-queueable';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, false);
            Id liveAsyncApexJobId = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 612);
            Id terminalAsyncApexJobId = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 613);

            Job__c liveQueueableJob = QueuesTestSupport.job(
                queueName,
                'sync',
                Queues.State.ACTIVE.name(),
                'live-queueable-stall'
            );
            liveQueueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 612);
            liveQueueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            liveQueueableJob.AsyncApexJobId__c = liveAsyncApexJobId;
            liveQueueableJob.AttemptsMade__c = 0;
            liveQueueableJob.MaxAttempts__c = 3;

            Job__c terminalQueueableJob = QueuesTestSupport.job(
                queueName,
                'sync',
                Queues.State.ACTIVE.name(),
                'terminal-queueable-stall'
            );
            terminalQueueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 613);
            terminalQueueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            terminalQueueableJob.AsyncApexJobId__c = terminalAsyncApexJobId;
            terminalQueueableJob.AttemptsMade__c = 0;
            terminalQueueableJob.MaxAttempts__c = 3;

            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
            runtime.WorkerToken__c = '';

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('requireQueueDefinition', queueDefinition)
                    .withReturn('getLeaseMinutes', 5)
            );
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', runtime)
                .withReturn('lockRuntime', runtime)
                .withReturn('shouldClearStaleWorker', false)
                .withReturn('updateRuntime', runtime);
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryExpiredActiveJobsForUpdate', new List<Job__c>())
                .withReturn(
                    'queryExpiredActiveQueueableJobsForUpdate',
                    new List<Job__c>{ liveQueueableJob, terminalQueueableJob }
                )
                .withReturn('hasActiveJobs', false);
            QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider queueableExecutionProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('enqueueJob', terminalQueueableJob);
            QueuesTestSupport.RecordingStubProvider asyncApexJobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryAsyncJobsByIds',
                    new List<AsyncApexJob>{
                        QueuesTestSupport.asyncApexJob(liveAsyncApexJobId, 'Queued', 'QueuesQueueableExecutor'),
                        QueuesTestSupport.asyncApexJob(terminalAsyncApexJobId, 'Completed', 'QueuesQueueableExecutor')
                    }
                );

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
                new QueuesQueueableDispatchService(),
                (QueuesQueueableExecutionService) Test.createStub(
                    QueuesQueueableExecutionService.class,
                    queueableExecutionProvider
                ),
                (QueuesAsyncApexJobRepository) Test.createStub(
                    QueuesAsyncApexJobRepository.class,
                    asyncApexJobRepositoryProvider
                ),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                new QueuesWorkerService()
            );

            // Act
            Integer recoveredCount = service.recoverStalled(queueName);

            // Assert
            Assert.areEqual(1, recoveredCount);
            Assert.areEqual(Queues.State.ACTIVE, QueuesSupport.toState(liveQueueableJob.State__c));
            Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(terminalQueueableJob.State__c));
            Assert.areEqual(1, asyncApexJobRepositoryProvider.getCallCount('queryAsyncJobsByIds'));

            List<Object> asyncQueryArgs = asyncApexJobRepositoryProvider.getLastArgs('queryAsyncJobsByIds');
            Set<Id> queriedAsyncApexJobIds = (Set<Id>) asyncQueryArgs[0];
            Assert.areEqual(true, queriedAsyncApexJobIds.contains(liveAsyncApexJobId));
            Assert.areEqual(true, queriedAsyncApexJobIds.contains(terminalAsyncApexJobId));

            List<Job__c> updatedJobs = (List<Job__c>) jobServiceProvider.getLastArgs('updateJobs')[0];
            Assert.areEqual(1, updatedJobs.size());
            Assert.areEqual(terminalQueueableJob.Id, updatedJobs[0].Id);
            Assert.areEqual(1, jobRunProvider.getCallCount('insertJobRuns'));
            Assert.areEqual(1, queueableExecutionProvider.getCallCount('enqueueJob'));
            Assert.areEqual(terminalQueueableJob.Id, queueableExecutionProvider.getLastArgs('enqueueJob')[0]);
        }
    }

    @IsTest
    static void should_skip_all_stalled_queueable_recovery_when_async_jobs_are_still_active() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-skip-live-queueable';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, false);
            Id liveAsyncApexJobId = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 614);

            Job__c liveQueueableJob = QueuesTestSupport.job(
                queueName,
                'sync',
                Queues.State.ACTIVE.name(),
                'live-queueable-only'
            );
            liveQueueableJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 614);
            liveQueueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            liveQueueableJob.AsyncApexJobId__c = liveAsyncApexJobId;
            liveQueueableJob.AttemptsMade__c = 0;
            liveQueueableJob.MaxAttempts__c = 3;

            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
            runtime.WorkerToken__c = '';

            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryExpiredActiveJobsForUpdate', new List<Job__c>())
                .withReturn('queryExpiredActiveQueueableJobsForUpdate', new List<Job__c>{ liveQueueableJob })
                .withReturn('hasActiveJobs', false);
            QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider queueableExecutionProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider asyncApexJobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryAsyncJobsByIds',
                    new List<AsyncApexJob>{
                        QueuesTestSupport.asyncApexJob(liveAsyncApexJobId, 'Processing', 'QueuesQueueableExecutor')
                    }
                );

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                (QueuesDefinitionService) Test.createStub(
                    QueuesDefinitionService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('requireQueueDefinition', queueDefinition)
                        .withReturn('getLeaseMinutes', 5)
                ),
                (QueuesRuntimeService) Test.createStub(
                    QueuesRuntimeService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('ensureRuntime', runtime)
                        .withReturn('lockRuntime', runtime)
                        .withReturn('shouldClearStaleWorker', false)
                        .withReturn('updateRuntime', runtime)
                ),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
                new QueuesQueueableDispatchService(),
                (QueuesQueueableExecutionService) Test.createStub(
                    QueuesQueueableExecutionService.class,
                    queueableExecutionProvider
                ),
                (QueuesAsyncApexJobRepository) Test.createStub(
                    QueuesAsyncApexJobRepository.class,
                    asyncApexJobRepositoryProvider
                ),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                new QueuesWorkerService()
            );

            // Act
            Integer recoveredCount = service.recoverStalled(queueName);

            // Assert
            Assert.areEqual(0, recoveredCount);
            Assert.areEqual(Queues.State.ACTIVE, QueuesSupport.toState(liveQueueableJob.State__c));
            Assert.areEqual(1, asyncApexJobRepositoryProvider.getCallCount('queryAsyncJobsByIds'));
            Assert.areEqual(0, jobServiceProvider.getCallCount('updateJobs'));
            Assert.areEqual(0, jobRunProvider.getCallCount('insertJobRuns'));
            Assert.areEqual(0, queueableExecutionProvider.getCallCount('enqueueJob'));
        }
    }

    @IsTest
    static void should_recover_stalled_jobs_in_single_bounded_slice_when_jobs_fit_batch() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-recover-bounded';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            Job__c firstJob = QueuesTestSupport.job(queueName, 'sync', Queues.State.ACTIVE.name(), 'recover-1');
            firstJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 601);
            Job__c secondJob = QueuesTestSupport.job(queueName, 'sync', Queues.State.ACTIVE.name(), 'recover-2');
            secondJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 602);
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
            runtime.WorkerToken__c = '';

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', runtime)
                .withReturn('lockRuntime', runtime)
                .withReturn('shouldClearStaleWorker', false)
                .withReturn('updateRuntime', runtime);
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns(
                    'queryExpiredActiveJobsForUpdate',
                    new List<Object>{ new List<Job__c>{ firstJob, secondJob } }
                )
                .withReturn('queryExpiredActiveQueueableJobsForUpdate', new List<Job__c>())
                .withReturn('hasActiveJobs', false);
            QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('syncWorkerForNextWakeAt', true);

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            Integer recoveredCount = service.recoverStalled(queueName);

            // Assert
            Assert.areEqual(2, recoveredCount);
            Assert.areEqual(1, jobServiceProvider.getCallCount('queryExpiredActiveJobsForUpdate'));
            Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
            Assert.areEqual(1, jobRunProvider.getCallCount('insertJobRuns'));
        }
    }

    @IsTest
    static void should_keep_runtime_discoverable_when_active_leases_remain_after_recovery() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-active-leases';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, false);
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
            runtime.WorkerToken__c = '';

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', runtime)
                .withReturn('lockRuntime', runtime)
                .withReturn('shouldClearStaleWorker', false)
                .withReturn('updateRuntime', runtime);
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryExpiredActiveJobsForUpdate', new List<Job__c>())
                .withReturn('queryExpiredActiveQueueableJobsForUpdate', new List<Job__c>())
                .withReturn('hasActiveJobs', true);

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                (QueuesJobRunService) Test.createStub(
                    QueuesJobRunService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                ),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                (QueuesWorkerService) Test.createStub(
                    QueuesWorkerService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                )
            );

            // Act
            Integer recoveredCount = service.recoverStalled(queueName);

            // Assert
            Assert.areEqual(0, recoveredCount);
            Assert.areEqual(1, QueuesSupport.toInteger(runtime.ActiveWorkerCount__c, 0));
            Assert.areEqual(1, jobServiceProvider.getCallCount('hasActiveJobs'));
        }
    }

    @IsTest
    @SuppressWarnings('PMD.OperationWithLimitsInLoop')
    static void should_skip_queueable_recovery_when_no_queueable_submit_slots_remain() {
        // Arrange
        String queueName = 'queue-admin-no-queueable-slots';
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, false);
        QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
        runtime.WorkerToken__c = 'worker-token';

        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryExpiredActiveJobsForUpdate', new List<Job__c>());

        QueuesQueueAdminService service = new QueuesQueueAdminService(
            (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            ),
            (QueuesRuntimeService) Test.createStub(
                QueuesRuntimeService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('ensureRuntime', runtime)
                    .withReturn('lockRuntime', runtime)
                    .withReturn('shouldClearStaleWorker', false)
                    .withReturn('updateRuntime', runtime)
            ),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
            (QueuesJobRunService) Test.createStub(
                QueuesJobRunService.class,
                new QueuesTestSupport.RecordingStubProvider()
            ),
            new QueuesQueueableDispatchService(),
            new QueuesQueueableExecutionService(),
            new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
            new QueuesStalledJobRecoveryService(),
            new QueuesFlowProducerService(),
            new QueuesWorkerService()
        );

        for (Integer index = 0; index < Limits.getLimitQueueableJobs(); index++) {
            System.enqueueJob(new NoopQueueable());
        }

        // Act
        Integer recoveredCount = service.recoverStalled(queueName);

        // Assert
        Assert.areEqual(0, recoveredCount);
        Assert.areEqual(0, jobServiceProvider.getCallCount('queryExpiredActiveQueueableJobsForUpdate'));
    }

    @IsTest
    static void should_leave_runtime_maintenance_hint_unchanged_when_runtime_is_missing_or_worker_is_present() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueRuntime__c runtime = QueuesTestSupport.runtime('queue-admin-hint-noop', false, 2);
            runtime.WorkerToken__c = 'worker-token';

            // Act
            QueuesQueueAdminService.syncActiveLeaseMaintenanceHint(null, true);
            QueuesQueueAdminService.syncActiveLeaseMaintenanceHint(runtime, true);

            // Assert
            Assert.areEqual(2, QueuesSupport.toInteger(runtime.ActiveWorkerCount__c, 0));
        }
    }

    @IsTest
    static void should_drain_queue_when_drainable_jobs_exist() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-drain';
            Job__c waitingJob = new Job__c(Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 503));
            Job__c pausedJob = new Job__c(Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 504));
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
            runtime.WorkerToken__c = 'worker-token';
            runtime.AsyncApexJobId__c = QueuesTestSupport.fakeIdValue(CronTrigger.SObjectType, 1);
            runtime.NextPumpAt__c = System.now().addMinutes(10);

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, false))
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryDrainableJobsForUpdate', new List<Job__c>{ waitingJob, pausedJob });
            QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
                QueuesJobService.class,
                jobServiceProvider
            );
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('lockRuntime', runtime)
                .withReturn('clearWorkerAndUpdate', runtime);
            QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
                QueuesRuntimeService.class,
                runtimeProvider
            );
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesWorkerService workerStub = (QueuesWorkerService) Test.createStub(
                QueuesWorkerService.class,
                workerProvider
            );

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                runtimeStub,
                jobServiceStub,
                new QueuesJobRunService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                workerStub
            );

            // Act
            Integer drainedCount = service.drain(queueName);

            // Assert
            Assert.areEqual(2, drainedCount);
            Assert.areEqual(1, jobServiceProvider.getCallCount('deleteJobs'));
            Assert.areEqual(1, workerProvider.getCallCount('syncExistingWorkerForNextWakeAt'));
            Assert.areEqual(0, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
        }
    }

    @IsTest
    static void should_cancel_queue_when_drainable_jobs_exist() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-cancel';
            Job__c waitingJob = QueuesTestSupport.job(queueName, 'waiting', Queues.State.WAITING.name(), 'waiting');
            waitingJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 608);
            waitingJob.LeaseToken__c = 'lease-token';
            waitingJob.LeaseExpiresAt__c = System.now().addMinutes(5);
            waitingJob.AsyncApexJobId__c = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 608);
            waitingJob.PauseRequested__c = true;
            Job__c pausedJob = QueuesTestSupport.job(queueName, 'paused', Queues.State.PAUSED.name(), 'paused');
            pausedJob.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 609);

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, false))
            );
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', QueuesTestSupport.runtime(queueName, false, 0))
                .withReturn('lockRuntime', QueuesTestSupport.runtime(queueName, false, 0));
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryDrainableJobsForUpdate', new List<Job__c>{ waitingJob, pausedJob });
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobRunService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            // Act
            Integer canceledCount = service.cancel(queueName);

            // Assert
            Assert.areEqual(2, canceledCount);
            Assert.areEqual(Queues.State.CANCELED.name(), waitingJob.State__c);
            Assert.areEqual('Canceled by queue cancel.', waitingJob.FailedReason__c);
            Assert.areEqual(null, waitingJob.LeaseToken__c);
            Assert.areEqual(null, waitingJob.LeaseExpiresAt__c);
            Assert.areEqual(null, waitingJob.AsyncApexJobId__c);
            Assert.areEqual(false, waitingJob.PauseRequested__c);
            Assert.areEqual(Queues.State.CANCELED.name(), pausedJob.State__c);
            Assert.areEqual(1, jobServiceProvider.getCallCount('updateJobs'));
            Assert.areEqual(1, workerProvider.getCallCount('syncExistingWorkerForNextWakeAt'));
        }
    }

    @IsTest
    static void should_cancel_drainable_jobs_across_execution_modes() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-cancel-modes';
            Job__c workerJob = QueuesTestSupport.job(queueName, 'worker', Queues.State.WAITING.name(), 'worker');
            Job__c queueableJob = QueuesTestSupport.job(
                queueName,
                'queueable',
                Queues.State.DELAYED.name(),
                'queueable'
            );
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            Job__c invocableJob = QueuesTestSupport.job(
                queueName,
                'invocable',
                Queues.State.PAUSED.name(),
                'invocable'
            );
            invocableJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
            Job__c activeQueueableJob = QueuesTestSupport.job(
                queueName,
                'active-queueable',
                Queues.State.ACTIVE.name(),
                'active-queueable'
            );
            activeQueueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            insert new List<Job__c>{ workerJob, queueableJob, invocableJob, activeQueueableJob };

            QueuesQueueAdminService service = newQueueAdminService(queueName);

            // Act
            Integer canceledCount = service.cancel(queueName);

            // Assert
            Map<Id, Job__c> jobsById = new Map<Id, Job__c>(
                [
                    SELECT State__c, FailedReason__c
                    FROM Job__c
                    WHERE Id IN :new Set<Id>{ workerJob.Id, queueableJob.Id, invocableJob.Id, activeQueueableJob.Id }
                ]
            );
            Assert.areEqual(3, canceledCount);
            Assert.areEqual(Queues.State.CANCELED.name(), jobsById.get(workerJob.Id).State__c);
            Assert.areEqual(Queues.State.CANCELED.name(), jobsById.get(queueableJob.Id).State__c);
            Assert.areEqual(Queues.State.CANCELED.name(), jobsById.get(invocableJob.Id).State__c);
            Assert.areEqual(Queues.State.ACTIVE.name(), jobsById.get(activeQueueableJob.Id).State__c);
            Assert.areEqual('Canceled by queue cancel.', jobsById.get(queueableJob.Id).FailedReason__c);
        }
    }

    @IsTest
    static void should_drain_drainable_jobs_across_execution_modes() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-drain-modes';
            Job__c workerJob = QueuesTestSupport.job(queueName, 'worker', Queues.State.WAITING.name(), 'worker');
            Job__c queueableJob = QueuesTestSupport.job(
                queueName,
                'queueable',
                Queues.State.DELAYED.name(),
                'queueable'
            );
            queueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            Job__c invocableJob = QueuesTestSupport.job(
                queueName,
                'invocable',
                Queues.State.PAUSED.name(),
                'invocable'
            );
            invocableJob.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
            Job__c activeQueueableJob = QueuesTestSupport.job(
                queueName,
                'active-queueable',
                Queues.State.ACTIVE.name(),
                'active-queueable'
            );
            activeQueueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            insert new List<Job__c>{ workerJob, queueableJob, invocableJob, activeQueueableJob };
            insert new List<JobRun__c>{
                QueuesTestSupport.jobRun(workerJob.Id, 1, Queues.RunStatus.COMPLETED.name()),
                QueuesTestSupport.jobRun(queueableJob.Id, 1, Queues.RunStatus.COMPLETED.name()),
                QueuesTestSupport.jobRun(invocableJob.Id, 1, Queues.RunStatus.COMPLETED.name())
            };

            QueuesQueueAdminService service = newQueueAdminService(queueName);

            // Act
            Integer drainedCount = service.drain(queueName);

            // Assert
            Assert.areEqual(3, drainedCount);
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE QueueName__c = :queueName]);
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE Id = :activeQueueableJob.Id]);
            Assert.areEqual(
                0,
                [
                    SELECT COUNT()
                    FROM JobRun__c
                    WHERE Job__c IN :new Set<Id>{ workerJob.Id, queueableJob.Id, invocableJob.Id }
                ]
            );
        }
    }

    @IsTest
    static void should_release_parent_in_another_queue_when_draining_child_queue() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String childQueueName = 'queue-admin-drain-child';
            Job__c parent = QueuesTestSupport.job(
                'queue-admin-drain-parent',
                'parent',
                Queues.State.WAITING_CHILDREN.name(),
                'parent'
            );
            parent.ExecutionMode__c = Queues.ExecutionMode.INVOCABLE.name();
            insert parent;
            Job__c child = QueuesTestSupport.job(childQueueName, 'child', Queues.State.WAITING.name(), 'child');
            child.Parent__c = parent.Id;
            insert child;

            // Act
            Integer drainedCount = newQueueAdminService(childQueueName).drain(childQueueName);

            // Assert
            parent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            Assert.areEqual(1, drainedCount);
            Assert.areEqual(Queues.State.WAITING.name(), parent.State__c);
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE Id = :child.Id]);
        }
    }

    @IsTest
    static void should_not_wake_worker_while_draining_released_parent_in_same_queue() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-drain-same-queue';
            Job__c parent = QueuesTestSupport.job(queueName, 'parent', Queues.State.WAITING_CHILDREN.name(), 'parent');
            Job__c child = QueuesTestSupport.job(queueName, 'child', Queues.State.WAITING.name(), 'child');
            insert parent;
            child.Parent__c = parent.Id;
            insert child;

            // Act
            Integer drainedCount = newQueueAdminService(queueName).drain(queueName);

            // Assert
            Assert.areEqual(2, drainedCount);
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE QueueName__c = :queueName]);
            Assert.areEqual(0, [SELECT COUNT() FROM QueueRuntime__c WHERE QueueName__c = :queueName]);
        }
    }

    @IsTest
    static void should_apply_cancel_policy_to_parent_in_another_queue_when_canceling_child_queue() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String childQueueName = 'queue-admin-cancel-child';
            Job__c parent = QueuesTestSupport.job(
                'queue-admin-cancel-parent',
                'parent',
                Queues.State.WAITING_CHILDREN.name(),
                'parent'
            );
            insert parent;
            Job__c child = QueuesTestSupport.job(childQueueName, 'child', Queues.State.WAITING.name(), 'child');
            child.Parent__c = parent.Id;
            child.DependencyCanceledPolicy__c = Queues.DependencyCanceledPolicy.FAIL_PARENT.name();
            insert child;

            // Act
            Test.startTest();
            Integer canceledCount = newQueueAdminService(childQueueName).cancel(childQueueName);
            Job__c waitingParent = [SELECT State__c FROM Job__c WHERE Id = :parent.Id];
            Test.stopTest();

            // Assert
            parent = [SELECT FailedReason__c, State__c FROM Job__c WHERE Id = :parent.Id];
            child = [SELECT State__c FROM Job__c WHERE Id = :child.Id];
            Assert.areEqual(1, canceledCount);
            Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), waitingParent.State__c);
            Assert.areEqual(Queues.State.FAILED.name(), parent.State__c);
            Assert.areEqual('Child job was canceled.', parent.FailedReason__c);
            Assert.areEqual(Queues.State.CANCELED.name(), child.State__c);
        }
    }

    @IsTest
    static void should_not_clear_worker_plan_when_draining_queue_with_active_workers() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-drain-active-worker';
            Job__c waitingJob = new Job__c(Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 505));
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 1);
            runtime.WorkerToken__c = 'worker-token';
            runtime.AsyncApexJobId__c = QueuesTestSupport.fakeIdValue(CronTrigger.SObjectType, 2);
            runtime.NextPumpAt__c = System.now().addMinutes(10);

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, false))
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryDrainableJobsForUpdate', new List<Job__c>{ waitingJob });
            QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
                QueuesJobService.class,
                jobServiceProvider
            );
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('lockRuntime', runtime);
            QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
                QueuesRuntimeService.class,
                runtimeProvider
            );
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesWorkerService workerStub = (QueuesWorkerService) Test.createStub(
                QueuesWorkerService.class,
                workerProvider
            );

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                runtimeStub,
                jobServiceStub,
                new QueuesJobRunService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                workerStub
            );

            // Act
            Integer drainedCount = service.drain(queueName);

            // Assert
            Assert.areEqual(1, drainedCount);
            Assert.areEqual(1, workerProvider.getCallCount('syncExistingWorkerForNextWakeAt'));
            Assert.areEqual(0, runtimeProvider.getCallCount('clearWorkerAndUpdate'));
        }
    }

    @IsTest
    static void should_use_bounded_drain_slice_sizes_when_deleting_jobs() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-drain-bounded';
            Job__c waitingJob = new Job__c(Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 606));
            Job__c pausedJob = new Job__c(Id = QueuesTestSupport.fakeId(Job__c.SObjectType, 607));

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, false))
            );
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', QueuesTestSupport.runtime(queueName, false, 0))
                .withReturn('lockRuntime', QueuesTestSupport.runtime(queueName, false, 0));
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryDrainableJobsForUpdate', new List<Job__c>{ waitingJob, pausedJob });
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobRunService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            List<QueueError__c> fillerErrors = new List<QueueError__c>();
            Integer fillerCount = Math.max(0, (Limits.getLimitDmlRows() - Limits.getDmlRows()) - 510);
            for (Integer i = 0; i < fillerCount; i++) {
                fillerErrors.add(
                    new QueueError__c(
                        QueueName__c = 'queue-admin-drain-bounded',
                        Component__c = 'runtime maintenance',
                        OccurredAt__c = System.now()
                    )
                );
            }
            Database.insert(fillerErrors, false);

            // Act
            Integer drainedCount = service.drain(queueName);

            // Assert
            Assert.areEqual(2, drainedCount);
            Assert.areEqual(9, (Integer) jobServiceProvider.getLastArgs('queryDrainableJobsForUpdate')[1]);
        }
    }

    @IsTest
    static void should_stop_recovery_before_querying_jobs_when_remaining_dml_rows_cannot_cover_one_slice() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-recover-headroom';
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 0);
            runtime.WorkerToken__c = '';
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', runtime)
                .withReturn('lockRuntime', runtime)
                .withReturn('shouldClearStaleWorker', false)
                .withReturn('updateRuntime', runtime);
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('hasActiveJobs', false);

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                (QueuesDefinitionService) Test.createStub(
                    QueuesDefinitionService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, false))
                ),
                (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                (QueuesJobRunService) Test.createStub(
                    QueuesJobRunService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                ),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                (QueuesWorkerService) Test.createStub(
                    QueuesWorkerService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                )
            );

            List<QueueError__c> fillerErrors = new List<QueueError__c>();
            Integer fillerCount = Math.max(0, (Limits.getLimitDmlRows() - Limits.getDmlRows()) - 500);
            for (Integer i = 0; i < fillerCount; i++) {
                fillerErrors.add(
                    new QueueError__c(
                        QueueName__c = queueName,
                        Component__c = 'runtime maintenance',
                        OccurredAt__c = System.now()
                    )
                );
            }
            Database.insert(fillerErrors, false);

            // Act
            Integer recoveredCount = service.recoverStalled(queueName);

            // Assert
            Assert.areEqual(0, recoveredCount);
            Assert.areEqual(0, jobServiceProvider.getCallCount('queryExpiredActiveJobsForUpdate'));
            Assert.areEqual(1, runtimeProvider.getCallCount('updateRuntime'));
        }
    }

    @IsTest
    static void should_stop_drain_before_querying_jobs_when_remaining_dml_rows_cannot_cover_one_slice() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-drain-headroom';
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', QueuesTestSupport.runtime(queueName, false, 0))
                .withReturn('lockRuntime', QueuesTestSupport.runtime(queueName, false, 0));
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesQueueAdminService service = new QueuesQueueAdminService(
                (QueuesDefinitionService) Test.createStub(
                    QueuesDefinitionService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, false))
                ),
                (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                (QueuesJobRunService) Test.createStub(
                    QueuesJobRunService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                ),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            List<QueueError__c> fillerErrors = new List<QueueError__c>();
            Integer fillerCount = Math.max(0, (Limits.getLimitDmlRows() - Limits.getDmlRows()) - 500);
            for (Integer i = 0; i < fillerCount; i++) {
                fillerErrors.add(
                    new QueueError__c(
                        QueueName__c = queueName,
                        Component__c = 'runtime maintenance',
                        OccurredAt__c = System.now()
                    )
                );
            }
            Database.insert(fillerErrors, false);

            // Act
            Integer drainedCount = service.drain(queueName);

            // Assert
            Assert.areEqual(0, drainedCount);
            Assert.areEqual(0, jobServiceProvider.getCallCount('queryDrainableJobsForUpdate'));
            Assert.areEqual(1, workerProvider.getCallCount('syncExistingWorkerForNextWakeAt'));
        }
    }

    @IsTest
    static void should_stop_cancel_before_querying_jobs_when_remaining_dml_rows_cannot_cover_one_slice() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-cancel-headroom';
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', QueuesTestSupport.runtime(queueName, false, 0))
                .withReturn('lockRuntime', QueuesTestSupport.runtime(queueName, false, 0));
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesQueueAdminService service = new QueuesQueueAdminService(
                (QueuesDefinitionService) Test.createStub(
                    QueuesDefinitionService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, false))
                ),
                (QueuesRuntimeService) Test.createStub(QueuesRuntimeService.class, runtimeProvider),
                (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider),
                new QueuesJobRunService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                (QueuesWorkerService) Test.createStub(QueuesWorkerService.class, workerProvider)
            );

            List<QueueError__c> fillerErrors = new List<QueueError__c>();
            Integer fillerCount = Math.max(0, (Limits.getLimitDmlRows() - Limits.getDmlRows()) - 500);
            for (Integer i = 0; i < fillerCount; i++) {
                fillerErrors.add(
                    new QueueError__c(
                        QueueName__c = queueName,
                        Component__c = 'runtime maintenance',
                        OccurredAt__c = System.now()
                    )
                );
            }
            Database.insert(fillerErrors, false);

            // Act
            Integer canceledCount = service.cancel(queueName);

            // Assert
            Assert.areEqual(0, canceledCount);
            Assert.areEqual(0, jobServiceProvider.getCallCount('queryDrainableJobsForUpdate'));
            Assert.areEqual(1, workerProvider.getCallCount('syncExistingWorkerForNextWakeAt'));
        }
    }

    @IsTest
    static void should_return_zero_when_no_stalled_or_drainable_jobs_are_found() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-admin-empty';
            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 1);
            runtime.WorkerToken__c = 'worker-token';
            QueueDefinition__mdt activeDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            QueueDefinition__mdt inactiveDefinition = QueuesTestSupport.queueDefinition(queueName, false);

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturns('requireQueueDefinition', new List<Object>{ activeDefinition, inactiveDefinition })
            );
            QueuesTestSupport.RecordingStubProvider runtimeProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', runtime)
                .withReturn('lockRuntime', runtime)
                .withReturn('shouldClearStaleWorker', false)
                .withReturn('updateRuntime', runtime);
            QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
                QueuesRuntimeService.class,
                runtimeProvider
            );
            QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryExpiredActiveJobsForUpdate', new List<Job__c>())
                .withReturn('queryExpiredActiveQueueableJobsForUpdate', new List<Job__c>())
                .withReturn('queryDrainableJobsForUpdate', new List<Job__c>())
                .withReturn('hasActiveJobs', false);
            QueuesJobService jobServiceStub = (QueuesJobService) Test.createStub(
                QueuesJobService.class,
                jobServiceProvider
            );
            QueuesTestSupport.RecordingStubProvider jobRunProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesWorkerService workerStub = (QueuesWorkerService) Test.createStub(
                QueuesWorkerService.class,
                workerProvider
            );

            QueuesQueueAdminService service = new QueuesQueueAdminService(
                definitionStub,
                runtimeStub,
                jobServiceStub,
                (QueuesJobRunService) Test.createStub(QueuesJobRunService.class, jobRunProvider),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                workerStub
            );

            // Act
            Integer recoveredCount = service.recoverStalled(queueName);
            Integer drainedCount = service.drain(queueName);
            Integer canceledCount = service.cancel(queueName);

            // Assert
            Assert.areEqual(0, recoveredCount);
            Assert.areEqual(0, drainedCount);
            Assert.areEqual(0, canceledCount);
            Assert.areEqual(0, jobServiceProvider.getCallCount('updateJobs'));
            Assert.areEqual(0, jobRunProvider.getCallCount('insertJobRuns'));
            Assert.areEqual(2, workerProvider.getCallCount('syncExistingWorkerForNextWakeAt'));
        }
    }

    @IsTest
    static void should_require_cpu_headroom_for_drain() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Act + Assert
            Assert.areEqual(true, QueuesQueueAdminService.hasHeadroomForDrain(500, 10, 5, 2000));
            Assert.areEqual(false, QueuesQueueAdminService.hasHeadroomForDrain(500, 9, 5, 2000));
            Assert.areEqual(false, QueuesQueueAdminService.hasHeadroomForDrain(500, 10, 5, 1999));
            Assert.areEqual(false, QueuesQueueAdminService.hasHeadroomForDrain(499, 10, 5, 2000));
            Assert.areEqual(false, QueuesQueueAdminService.hasHeadroomForDrain(500, 10, 4, 2000));
        }
    }

    @IsTest
    static void should_resolve_bounded_recovery_batch_size_from_remaining_dml_rows() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Act + Assert
            Assert.areEqual(0, QueuesStalledJobRecoveryService.resolveRecoverStalledBatchSize(500));
            Assert.areEqual(1, QueuesStalledJobRecoveryService.resolveRecoverStalledBatchSize(502));
            Assert.areEqual(25, QueuesStalledJobRecoveryService.resolveRecoverStalledBatchSize(550));
            Assert.areEqual(1000, QueuesStalledJobRecoveryService.resolveRecoverStalledBatchSize(2500));
        }
    }

    @IsTest
    static void should_resolve_bounded_drain_slice_sizes_from_remaining_dml_rows() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Act + Assert
            Assert.areEqual(0, QueuesQueueAdminService.resolveDrainJobBatchSize(500));
            Assert.areEqual(1, QueuesQueueAdminService.resolveDrainJobBatchSize(502));
            Assert.areEqual(9, QueuesQueueAdminService.resolveDrainJobBatchSize(510));
            Assert.areEqual(200, QueuesQueueAdminService.resolveDrainJobBatchSize(1000));
        }
    }

    private static QueuesQueueAdminService newQueueAdminService(final String queueName) {
        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('requireQueueDefinition', QueuesTestSupport.queueDefinition(queueName, false))
        );
        QueuesRuntimeService runtimeStub = (QueuesRuntimeService) Test.createStub(
            QueuesRuntimeService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureRuntime', QueuesTestSupport.runtime(queueName, false, 0))
                .withReturn('lockRuntime', QueuesTestSupport.runtime(queueName, false, 0))
        );
        QueuesWorkerService workerStub = (QueuesWorkerService) Test.createStub(
            QueuesWorkerService.class,
            new QueuesTestSupport.RecordingStubProvider()
        );
        return new QueuesQueueAdminService(
            definitionStub,
            runtimeStub,
            new QueuesJobService(),
            new QueuesJobRunService(),
            new QueuesQueueableDispatchService(),
            new QueuesQueueableExecutionService(),
            new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
            new QueuesStalledJobRecoveryService(),
            new QueuesFlowProducerService(),
            workerStub
        );
    }

    private static User newUser(final Id profileId, final String aliasValue) {
        String uniqueSuffix =
            String.valueOf(System.now().getTime()) + String.valueOf(Crypto.getRandomInteger()).replace('-', '0');
        String uniqueName = 'apex.queue.' + aliasValue + '.' + uniqueSuffix;
        return new User(
            Alias = aliasValue,
            CommunityNickname = ('aq' + aliasValue + uniqueSuffix).left(40),
            Email = uniqueName + '@example.com',
            EmailEncodingKey = 'UTF-8',
            LanguageLocaleKey = 'en_US',
            LastName = aliasValue,
            LocaleSidKey = 'en_US',
            ProfileId = profileId,
            TimeZoneSidKey = 'America/Los_Angeles',
            Username = uniqueName + '@example.com'
        );
    }

    private static User queryUser(final String aliasValue) {
        return [SELECT Id FROM User WHERE Alias = :aliasValue LIMIT 1];
    }

    @SuppressWarnings('PMD.QueueableWithoutFinalizer')
    private class NoopQueueable implements Queueable {
        public void execute(final QueueableContext ctx) {
        }
    }
}
