@IsTest(IsParallel=false)
private class QueuesIntegrationTest {
    @TestSetup
    static void setup() {
        Profile minimumAccessProfile = [SELECT Id FROM Profile WHERE Name = 'Minimum Access - Salesforce' LIMIT 1];
        String uniqueSuffix =
            String.valueOf(System.now().getTime()) + String.valueOf(Crypto.getRandomInteger()).replace('-', '0');
        String uniqueName = 'apex.queue.integration.admin.' + uniqueSuffix;
        User adminUser = new User(
            Alias = 'aqitadm',
            CommunityNickname = ('aqit' + uniqueSuffix).left(40),
            Email = uniqueName + '@example.com',
            EmailEncodingKey = 'UTF-8',
            LanguageLocaleKey = 'en_US',
            LastName = 'Queues Integration Admin',
            LocaleSidKey = 'en_US',
            ProfileId = minimumAccessProfile.Id,
            TimeZoneSidKey = 'America/Los_Angeles',
            Username = uniqueName + '@example.com'
        );
        insert adminUser;
        insert new PermissionSetAssignment(
            AssigneeId = adminUser.Id,
            PermissionSetId = [SELECT Id FROM PermissionSet WHERE Name = 'QueuesAdmin' LIMIT 1]
            .Id
        );
        QueuesTestSupport.setupQueueUser();
    }

    private static User queuesAdminUser() {
        return [
            SELECT Id
            FROM User
            WHERE LastName = 'Queues Integration Admin'
            ORDER BY CreatedDate DESC
            LIMIT 1
        ];
    }

    @IsTest
    static void should_store_waiting_job_with_queue_defaults_when_adding_job() {
        System.runAs(queuesAdminUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            try {
                Queues.queueService = QueuesTestSupport.newFacadeServiceWithQueueMetadata(workerProvider);

                // Act
                Queues.Job jobRef = Queues.of('invoice-sync')
                    .add(
                        'sync-invoice',
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'invoiceId' => 'INV-001' },
                        new Queues.JobOptions()
                            .jobId('invoice:INV-001')
                            .attempts(5)
                            .priority(2)
                            .backoff(Queues.Backoff.EXPONENTIAL, 1)
                    );

                // Assert
                Job__c storedJob = [
                    SELECT
                        QueueName__c,
                        Name__c,
                        State__c,
                        ExternalId__c,
                        QueueScopedExternalId__c,
                        Data__c,
                        Priority__c,
                        AttemptsMade__c,
                        MaxAttempts__c,
                        BackoffType__c,
                        BackoffValue__c,
                        ProcessorClass__c,
                        PauseRequested__c
                    FROM Job__c
                    WHERE Id = :jobRef.getId()
                ];

                Assert.areEqual('invoice-sync', storedJob.QueueName__c);
                Assert.areEqual('sync-invoice', storedJob.Name__c);
                Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(storedJob.State__c));
                Assert.areEqual('invoice:INV-001', storedJob.ExternalId__c);
                Assert.areEqual(
                    QueuesSupport.buildQueueScopedExternalId('invoice-sync', 'invoice:INV-001'),
                    storedJob.QueueScopedExternalId__c
                );
                Assert.areEqual('{"invoiceId":"INV-001"}', storedJob.Data__c);
                Assert.areEqual(2, QueuesSupport.toInteger(storedJob.Priority__c, 0));
                Assert.areEqual(0, QueuesSupport.toInteger(storedJob.AttemptsMade__c, 0));
                Assert.areEqual(5, QueuesSupport.toInteger(storedJob.MaxAttempts__c, 0));
                Assert.areEqual(Queues.Backoff.EXPONENTIAL, QueuesSupport.toBackoff(storedJob.BackoffType__c));
                Assert.areEqual(1, QueuesSupport.toInteger(storedJob.BackoffValue__c, 0));
                Assert.areEqual(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, storedJob.ProcessorClass__c);
                Assert.areEqual(false, storedJob.PauseRequested__c);
                Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
            } finally {
                QueuesTestSupport.resetFacadeService();
            }
        }
    }

    @IsTest
    static void should_retry_queueable_job_from_public_api_and_cap_native_retry_delay() {
        System.runAs(queuesAdminUser()) {
            // Arrange
            TestableQueueableDispatchService dispatchService = new TestableQueueableDispatchService();
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            try {
                Queues.queueService = newQueueableFacadeService(dispatchService, workerProvider);

                // Act
                Queues.Job jobRef = Queues.of('invoice-sync')
                    .add(
                        'sync-invoice',
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'invoiceId' => 'INV-QUEUEABLE-RETRY' },
                        new Queues.JobOptions().attempts(6).backoff(Queues.Backoff.EXPONENTIAL, 2),
                        new Queues.BulkJobOptions().queueable()
                    );

                Job__c storedJob = [
                    SELECT Id, ExecutionMode__c, QueueableDispatch__c
                    FROM Job__c
                    WHERE Id = :jobRef.getId()
                ];
                QueueableDispatch__c dispatch = [
                    SELECT Id, State__c, DispatcherAsyncApexJobId__c
                    FROM QueueableDispatch__c
                    WHERE Id = :storedJob.QueueableDispatch__c
                ];

                dispatchService.startDispatchAsSystem(dispatch.Id, dispatch.DispatcherAsyncApexJobId__c);
                Job__c dispatchedJob = [
                    SELECT AsyncApexJobId__c, State__c
                    FROM Job__c
                    WHERE Id = :storedJob.Id
                ];

                TestableQueueableExecutionService executionService = newQueueableExecutionService();
                String activeAsyncApexJobId = dispatchedJob.AsyncApexJobId__c;
                List<Integer> retryDelays = new List<Integer>();
                for (Integer index = 0; index < 4; index++) {
                    Job__c retryJob = executionService.execute(storedJob.Id, activeAsyncApexJobId);
                    retryDelays.add(executionService.lastDelayMinutes);
                    activeAsyncApexJobId = retryJob.AsyncApexJobId__c;
                }

                // Assert
                Job__c retriedJob = [
                    SELECT AttemptsMade__c, AsyncApexJobId__c, AvailableAt__c, ExecutionMode__c, State__c
                    FROM Job__c
                    WHERE Id = :storedJob.Id
                ];
                Integer runCount = [SELECT COUNT() FROM JobRun__c WHERE Job__c = :storedJob.Id];

                Assert.areEqual(Queues.ExecutionMode.QUEUEABLE_SERIAL.name(), storedJob.ExecutionMode__c);
                Assert.areEqual(Queues.State.ACTIVE.name(), dispatchedJob.State__c);
                Assert.areEqual(new List<Integer>{ 2, 4, 8, 10 }, retryDelays);
                Assert.areEqual(4, QueuesSupport.toInteger(retriedJob.AttemptsMade__c, 0));
                Assert.areEqual(Queues.State.ACTIVE.name(), retriedJob.State__c);
                Assert.areEqual(activeAsyncApexJobId, retriedJob.AsyncApexJobId__c);
                Assert.areEqual(4, runCount);
                Assert.areEqual(0, workerProvider.getCallCount('scheduleWorkerForJobs'));
            } finally {
                QueuesTestSupport.resetFacadeService();
            }
        }
    }

    @IsTest
    static void should_create_distinct_jobs_when_bulk_enqueuing_without_job_ids() {
        System.runAs(queuesAdminUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            try {
                Queues.queueService = QueuesTestSupport.newFacadeServiceWithQueueMetadata(workerProvider);

                // Act
                List<Queues.Job> jobRefs = Queues.of('invoice-sync')
                    .addBulk(
                        new List<Queues.JobData>{
                            new Queues.JobData(
                                'sync-invoice',
                                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                                new Map<String, Object>{ 'invoiceId' => 'INV-NO-ID' }
                            ),
                            new Queues.JobData(
                                'sync-invoice',
                                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                                new Map<String, Object>{ 'invoiceId' => 'INV-NO-ID' }
                            )
                        }
                    );

                // Assert
                List<Job__c> storedJobs = [
                    SELECT Id, ExternalId__c, QueueScopedExternalId__c, Data__c
                    FROM Job__c
                    WHERE Id IN :new Set<Id>{ jobRefs[0].getId(), jobRefs[1].getId() }
                    ORDER BY CreatedDate ASC
                ];

                Assert.areEqual(2, jobRefs.size());
                Assert.areNotEqual(jobRefs[0].getId(), jobRefs[1].getId());
                Assert.areEqual(2, storedJobs.size());
                Assert.areEqual(null, storedJobs[0].ExternalId__c);
                Assert.areEqual(null, storedJobs[0].QueueScopedExternalId__c);
                Assert.areEqual('{"invoiceId":"INV-NO-ID"}', storedJobs[0].Data__c);
                Assert.areEqual(null, storedJobs[1].ExternalId__c);
                Assert.areEqual(null, storedJobs[1].QueueScopedExternalId__c);
                Assert.areEqual('{"invoiceId":"INV-NO-ID"}', storedJobs[1].Data__c);
                Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
            } finally {
                QueuesTestSupport.resetFacadeService();
            }
        }
    }

    @IsTest
    static void should_add_bulk_jobs_with_and_without_job_ids_when_job_ids_are_mixed() {
        System.runAs(queuesAdminUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            try {
                Queues.queueService = QueuesTestSupport.newFacadeServiceWithQueueMetadata(workerProvider);

                Job__c existingJob = insertQueueJob(
                    'invoice-sync',
                    'sync-invoice',
                    Queues.State.WAITING.name(),
                    'invoice:INV-MIXED'
                );

                // Act
                List<Queues.Job> jobRefs = Queues.of('invoice-sync')
                    .addBulk(
                        new List<Queues.JobData>{
                            new Queues.JobData(
                                'sync-invoice',
                                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                                new Map<String, Object>{ 'invoiceId' => 'INV-MIXED-A' }
                            ),
                            new Queues.JobData(
                                'sync-invoice',
                                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                                new Map<String, Object>{ 'invoiceId' => 'INV-MIXED-B' },
                                new Queues.JobOptions().jobId('invoice:INV-MIXED')
                            ),
                            new Queues.JobData(
                                'sync-invoice',
                                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                                new Map<String, Object>{ 'invoiceId' => 'INV-MIXED-C' }
                            )
                        }
                    );

                // Assert
                List<Job__c> storedJobs = [
                    SELECT Id, ExternalId__c, QueueScopedExternalId__c, Data__c
                    FROM Job__c
                    WHERE Id IN :new Set<Id>{ jobRefs[0].getId(), jobRefs[1].getId(), jobRefs[2].getId() }
                ];
                Map<Id, Job__c> storedJobsById = new Map<Id, Job__c>(storedJobs);

                Assert.areEqual(3, jobRefs.size());
                Assert.areNotEqual(jobRefs[0].getId(), jobRefs[2].getId());
                Assert.areEqual(existingJob.Id, jobRefs[1].getId());
                Assert.areEqual(3, storedJobs.size());
                Assert.areEqual(null, storedJobsById.get(jobRefs[0].getId()).ExternalId__c);
                Assert.areEqual(null, storedJobsById.get(jobRefs[0].getId()).QueueScopedExternalId__c);
                Assert.areEqual('{"invoiceId":"INV-MIXED-A"}', storedJobsById.get(jobRefs[0].getId()).Data__c);
                Assert.areEqual('invoice:INV-MIXED', storedJobsById.get(jobRefs[1].getId()).ExternalId__c);
                Assert.areEqual(
                    QueuesSupport.buildQueueScopedExternalId('invoice-sync', 'invoice:INV-MIXED'),
                    storedJobsById.get(jobRefs[1].getId()).QueueScopedExternalId__c
                );
                Assert.areEqual(null, storedJobsById.get(jobRefs[2].getId()).ExternalId__c);
                Assert.areEqual(null, storedJobsById.get(jobRefs[2].getId()).QueueScopedExternalId__c);
                Assert.areEqual('{"invoiceId":"INV-MIXED-C"}', storedJobsById.get(jobRefs[2].getId()).Data__c);
                Assert.areEqual(1, workerProvider.getCallCount('scheduleWorkerForJobs'));
            } finally {
                QueuesTestSupport.resetFacadeService();
            }
        }
    }

    @IsTest
    static void should_dedupe_existing_and_repeated_job_ids_when_adding_bulk_jobs() {
        System.runAs(queuesAdminUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider();
            try {
                Queues.queueService = QueuesTestSupport.newFacadeServiceWithQueueMetadata(workerProvider);

                Job__c existingJob = insertQueueJob(
                    'invoice-sync',
                    'sync-invoice',
                    Queues.State.WAITING.name(),
                    'invoice:INV-201'
                );

                // Act
                List<Queues.Job> jobRefs = Queues.of('invoice-sync')
                    .addBulk(
                        new List<Queues.JobData>{
                            new Queues.JobData(
                                'sync-invoice',
                                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                                new Map<String, Object>{ 'invoiceId' => 'INV-201-NEW' },
                                new Queues.JobOptions().jobId('invoice:INV-201')
                            ),
                            new Queues.JobData(
                                'sync-invoice',
                                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                                new Map<String, Object>{ 'invoiceId' => 'INV-202-A' },
                                new Queues.JobOptions().jobId('invoice:INV-202')
                            ),
                            new Queues.JobData(
                                'sync-invoice',
                                QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                                new Map<String, Object>{ 'invoiceId' => 'INV-202-B' },
                                new Queues.JobOptions().jobId('invoice:INV-202').delay(5)
                            )
                        }
                    );

                // Assert
                Job__c dedupedJob = [
                    SELECT ExternalId__c, Data__c, State__c
                    FROM Job__c
                    WHERE Id = :jobRefs[1].getId()
                ];

                Assert.areEqual(3, jobRefs.size());
                Assert.areEqual(existingJob.Id, jobRefs[0].getId());
                Assert.areEqual(jobRefs[1].getId(), jobRefs[2].getId());
                Assert.areEqual('invoice:INV-202', dedupedJob.ExternalId__c);
                Assert.areEqual('{"invoiceId":"INV-202-A"}', dedupedJob.Data__c);
                Assert.areEqual(2, [SELECT COUNT() FROM Job__c WHERE QueueName__c = 'invoice-sync']);
            } finally {
                QueuesTestSupport.resetFacadeService();
            }
        }
    }

    @IsTest
    static void should_rollback_inserted_jobs_when_worker_scheduling_fails_during_enqueue() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesProcessorService processorStub = (QueuesProcessorService) Test.createStub(
                QueuesProcessorService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'requireProcessors',
                        new Map<String, Processor__mdt>{ processorDefinition.ProcessorClass__c => processorDefinition }
                    )
            );
            QueuesWorkerService workerStub = (QueuesWorkerService) Test.createStub(
                QueuesWorkerService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withException('scheduleWorkerForJobs', new Queues.QueueException('Worker scheduling failed.'))
            );
            QueuesErrorEventRepository errorEventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
                QueuesErrorEventRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
            );
            QueuesErrorService errorStub = new QueuesErrorService(
                new QueuesErrorRepository(),
                errorEventRepositoryStub
            );

            QueuesService service = new QueuesService(
                definitionStub,
                errorStub,
                new QueuesFlowProducerService(),
                new QueuesJobService(),
                new QueuesJobAdminService(),
                new QueuesJobSchedulerService(),
                new QueuesMaintenanceService(),
                new QueuesMaintenanceSchedule(),
                processorStub,
                new QueuesQueueableDispatchService(),
                new QueuesQueueAdminService(),
                new QueuesStatsService(),
                workerStub
            );

            String failureMessage;

            // Act
            try {
                service.enqueue(
                    'invoice-sync',
                    new Queues.JobData(
                        'sync-invoice',
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'invoiceId' => 'INV-ROLLBACK' },
                        new Queues.JobOptions().jobId('invoice:ROLLBACK')
                    )
                );
                Assert.fail('Expected enqueue to fail when worker scheduling fails.');
            } catch (Queues.QueueException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            Assert.areEqual('Worker scheduling failed.', failureMessage);
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE ExternalId__c = 'invoice:ROLLBACK']);
        }
    }

    @IsTest
    static void should_pause_resume_and_report_stats_when_runtime_row_exists() {
        System.runAs(queuesAdminUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('syncWorkerForNextWakeAt', true);
            try {
                Queues.queueService = QueuesTestSupport.newFacadeServiceWithQueueMetadata(workerProvider);

                // Act
                Queues.of('invoice-sync')
                    .add(
                        'sync-invoice',
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'invoiceId' => 'INV-003' }
                    );
                Queues.of('invoice-sync')
                    .add(
                        'sync-invoice',
                        QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
                        new Map<String, Object>{ 'invoiceId' => 'INV-004' },
                        new Queues.JobOptions().delay(5)
                    );

                Queues.of('invoice-sync').pause();
                QueueRuntime__c pausedRuntime = [
                    SELECT QueueName__c, IsPaused__c, ActiveWorkerCount__c, NextPumpAt__c, WorkerToken__c
                    FROM QueueRuntime__c
                    WHERE QueueName__c = 'invoice-sync'
                    LIMIT 1
                ];
                Queues.QueueStats pausedStats = Queues.of('invoice-sync').getStats();

                Queues.of('invoice-sync').resume();
                QueueRuntime__c resumedRuntime = [
                    SELECT IsPaused__c, NextPumpAt__c, WorkerToken__c
                    FROM QueueRuntime__c
                    WHERE Id = :pausedRuntime.Id
                ];

                // Assert
                Assert.areEqual(true, pausedRuntime.IsPaused__c);
                Assert.areEqual(true, pausedStats.isPaused);
                Assert.areEqual(2, pausedStats.total);
                Assert.areEqual(1, pausedStats.waiting);
                Assert.areEqual(1, pausedStats.delayed);
                Assert.areEqual(0, pausedStats.activeWorkerCount);
                Assert.areEqual(null, pausedRuntime.NextPumpAt__c);
                Assert.areEqual(null, pausedRuntime.WorkerToken__c);
                Assert.areEqual(false, resumedRuntime.IsPaused__c);
                Assert.areEqual(1, workerProvider.getCallCount('ensureWorkerForNextWakeAt'));
                Assert.areEqual(null, resumedRuntime.NextPumpAt__c);
                Assert.areEqual(null, resumedRuntime.WorkerToken__c);
                Assert.areEqual(
                    QueuesTestSupport.DEFAULT_QUEUE_NAME,
                    (String) workerProvider.getLastArgs('ensureWorkerForNextWakeAt')[0]
                );
                Assert.areNotEqual(null, workerProvider.getLastArgs('ensureWorkerForNextWakeAt')[1]);
            } finally {
                QueuesTestSupport.resetFacadeService();
            }
        }
    }

    @IsTest
    static void should_pause_resume_promote_discard_retry_defer_and_remove_job_when_admin_actions_are_requested() {
        System.runAs(queuesAdminUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('ensureWorkerForNextWakeAt', true)
                .withReturn('syncWorkerForNextWakeAt', true)
                .withReturn('syncExistingWorkerForNextWakeAt', true);
            try {
                Queues.queueService = QueuesTestSupport.newFacadeServiceWithQueueMetadata(workerProvider);

                Job__c job = insertQueueJob(
                    'invoice-sync',
                    'sync-invoice',
                    Queues.State.WAITING.name(),
                    'invoice:INV-301'
                );
                insert QueuesTestSupport.jobRun(job.Id, 1, Queues.RunStatus.COMPLETED.name());

                // Act
                Queues.Job jobRef = Queues.job(job.Id);
                Boolean isWaitingBeforePause = jobRef.isWaiting();

                jobRef.pause();
                Boolean isPausedAfterPause = Queues.job(job.Id).isPaused();
                Integer pausedCountAfterPause = Queues.of('invoice-sync').getPausedCount();

                jobRef.resume();
                Boolean isWaitingAfterResume = Queues.job(job.Id).isWaiting();

                jobRef.defer(5);
                Job__c deferredJob = [SELECT State__c, AvailableAt__c FROM Job__c WHERE Id = :job.Id];
                Boolean isDelayedAfterDefer = QueuesSupport.toState(deferredJob.State__c) == Queues.State.DELAYED;
                Boolean deferredLongEnough = deferredJob.AvailableAt__c > System.now().addMinutes(4);

                jobRef.promote();
                Boolean isWaitingAfterPromote = Queues.job(job.Id).isWaiting();

                jobRef.changePriority(7);
                jobRef.updateData(new Map<String, Object>{ 'invoiceId' => 'INV-301B' });
                jobRef.updateProgress(new Map<String, Object>{ 'step' => 'queued' });
                jobRef.discard();
                jobRef.pause();
                jobRef.retry();

                Job__c updatedJob = [
                    SELECT State__c, Priority__c, Data__c, Progress__c, MaxAttempts__c, AttemptsMade__c
                    FROM Job__c
                    WHERE Id = :job.Id
                ];

                Queues.job(job.Id).remove();
                Integer remainingJobs = [SELECT COUNT() FROM Job__c WHERE Id = :job.Id];
                Integer remainingRuns = [SELECT COUNT() FROM JobRun__c WHERE Job__c = :job.Id];
                Integer ensureWorkerCallCount = workerProvider.getCallCount('ensureWorkerForNextWakeAt');
                Integer syncWorkerCallCount = workerProvider.getCallCount('syncWorkerForNextWakeAt');

                // Assert
                Assert.areEqual(true, isWaitingBeforePause);
                Assert.areEqual(true, isPausedAfterPause);
                Assert.areEqual(1, pausedCountAfterPause);
                Assert.areEqual(true, isWaitingAfterResume);
                Assert.areEqual(true, isDelayedAfterDefer);
                Assert.areEqual(true, deferredLongEnough);
                Assert.areEqual(true, isWaitingAfterPromote);
                Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(updatedJob.State__c));
                Assert.areEqual(7, QueuesSupport.toInteger(updatedJob.Priority__c, 0));
                Assert.areEqual('{"invoiceId":"INV-301B"}', updatedJob.Data__c);
                Assert.areEqual(null, updatedJob.Progress__c);
                Assert.areEqual(1, QueuesSupport.toInteger(updatedJob.MaxAttempts__c, 0));
                Assert.areEqual(0, QueuesSupport.toInteger(updatedJob.AttemptsMade__c, 0));
                Assert.areEqual(0, remainingJobs);
                Assert.areEqual(0, remainingRuns);
                Assert.areEqual(true, ensureWorkerCallCount >= 3);
                Assert.areEqual(true, syncWorkerCallCount >= 2);
            } finally {
                QueuesTestSupport.resetFacadeService();
            }
        }
    }

    @IsTest
    static void should_reject_discarding_active_job_from_public_facade() {
        System.runAs(queuesAdminUser()) {
            // Arrange
            Job__c activeJob = insertQueueJob(
                'invoice-sync',
                'sync-invoice',
                Queues.State.ACTIVE.name(),
                'invoice:INV-302'
            );
            String failureMessage;

            // Act
            try {
                Queues.job(activeJob.Id).discard();
                Assert.fail('Expected active discard to be rejected.');
            } catch (Queues.InvalidOperationException ex) {
                failureMessage = ex.getMessage();
            }

            // Assert
            Job__c persistedJob = [SELECT MaxAttempts__c, State__c FROM Job__c WHERE Id = :activeJob.Id];
            Assert.areEqual('Active jobs cannot be discarded.', failureMessage);
            Assert.areEqual(Queues.State.ACTIVE, QueuesSupport.toState(persistedJob.State__c));
            Assert.areEqual(
                QueuesSupport.toInteger(activeJob.MaxAttempts__c, 0),
                QueuesSupport.toInteger(persistedJob.MaxAttempts__c, 0)
            );
        }
    }

    @IsTest
    static void should_execute_inline_processor_and_record_completed_run() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            Job__c job = QueuesTestSupport.job(
                'invoice-sync',
                'sync-invoice',
                Queues.State.ACTIVE.name(),
                'invoice:INV-401'
            );
            job.ProcessorClass__c = 'QueuesIntegrationTest.CompletingProcessor';
            job.LeaseToken__c = 'lease-token';
            job.LeaseExpiresAt__c = System.now().addMinutes(5);
            insert job;
            QueuesJobExecutionService service = new QueuesJobExecutionService(
                new QueuesJobService(),
                new QueuesJobRunService(),
                (QueuesProcessorService) Test.createStub(
                    QueuesProcessorService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn(
                            'getProcessorsByClassAsSystem',
                            new Map<String, Processor__mdt>{
                                'QueuesIntegrationTest.CompletingProcessor' => new Processor__mdt(
                                    ProcessorClass__c = 'QueuesIntegrationTest.CompletingProcessor',
                                    IsActive__c = true
                                )
                            }
                        )
                        .withReturn('instantiate', new CompletingProcessor())
                        .withReturn(
                            'execute',
                            new Queues.JobContext(job)
                                .complete(new Map<String, Object>{ 'status' => 'done' })
                                .withProgress(new Map<String, Object>{ 'step' => 'completed' })
                        )
                ),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            service.executeClaimedBatchJobs(new List<Job__c>{ job }, null);

            // Assert
            Job__c updatedJob = [
                SELECT State__c, ReturnValue__c, Progress__c
                FROM Job__c
                WHERE Id = :job.Id
                LIMIT 1
            ];
            JobRun__c run = [
                SELECT Status__c, Message__c, DurationMs__c
                FROM JobRun__c
                WHERE Job__c = :job.Id
                LIMIT 1
            ];

            Assert.areEqual(Queues.State.COMPLETED, QueuesSupport.toState(updatedJob.State__c));
            Assert.areEqual('{"status":"done"}', updatedJob.ReturnValue__c);
            Assert.areEqual('{"step":"completed"}', updatedJob.Progress__c);
            Assert.areEqual(
                Queues.RunStatus.COMPLETED,
                Queues.RunStatus.valueOf(QueuesSupport.normalizeEnumValue(run.Status__c))
            );
            Assert.areEqual(true, QueuesSupport.toInteger(run.DurationMs__c, 0) >= 0);
        }
    }

    @IsTest
    static void should_fail_job_without_retry_when_processor_throws_unrecoverable_job_exception() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            Job__c job = QueuesTestSupport.job(
                'invoice-sync',
                'sync-invoice',
                Queues.State.ACTIVE.name(),
                'invoice:INV-402'
            );
            job.ProcessorClass__c = 'QueuesIntegrationTest.UnrecoverableThrowingProcessor';
            job.LeaseToken__c = 'lease-token';
            job.LeaseExpiresAt__c = System.now().addMinutes(5);
            insert job;
            QueuesJobExecutionService service = new QueuesJobExecutionService(
                new QueuesJobService(),
                new QueuesJobRunService(),
                (QueuesProcessorService) Test.createStub(
                    QueuesProcessorService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                        .withReturn(
                            'getProcessorsByClassAsSystem',
                            new Map<String, Processor__mdt>{
                                'QueuesIntegrationTest.UnrecoverableThrowingProcessor' => new Processor__mdt(
                                    ProcessorClass__c = 'QueuesIntegrationTest.UnrecoverableThrowingProcessor',
                                    IsActive__c = true
                                )
                            }
                        )
                        .withReturn('instantiate', new UnrecoverableThrowingProcessor())
                        .withException(
                            'execute',
                            new Queues.UnrecoverableJobException('Unrecoverable processor failure.')
                        )
                ),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            service.executeClaimedBatchJobs(new List<Job__c>{ job }, null);

            // Assert
            Job__c updatedJob = [
                SELECT State__c, FailedReason__c
                FROM Job__c
                WHERE Id = :job.Id
                LIMIT 1
            ];
            JobRun__c run = [
                SELECT Status__c, Message__c
                FROM JobRun__c
                WHERE Job__c = :job.Id
                LIMIT 1
            ];

            Assert.areEqual(Queues.State.FAILED, QueuesSupport.toState(updatedJob.State__c));
            Assert.areEqual('Unrecoverable processor failure.', updatedJob.FailedReason__c);
            Assert.areEqual(
                Queues.RunStatus.FAILED,
                Queues.RunStatus.valueOf(QueuesSupport.normalizeEnumValue(run.Status__c))
            );
            Assert.areEqual('Unrecoverable processor failure.', run.Message__c);
        }
    }

    @IsTest
    static void should_recover_stalled_job_and_wake_queue_when_recovering_stalled_jobs() {
        System.runAs(queuesAdminUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('syncWorkerForNextWakeAt', true);
            try {
                Queues.queueService = QueuesTestSupport.newFacadeServiceWithQueueMetadata(workerProvider);

                insert new QueueRuntime__c(
                    QueueName__c = 'invoice-sync',
                    IsPaused__c = false,
                    ActiveWorkerCount__c = 0
                );

                Job__c stalledJob = insertQueueJob(
                    'invoice-sync',
                    'sync-invoice',
                    Queues.State.ACTIVE.name(),
                    'invoice:INV-501'
                );
                stalledJob.AttemptsMade__c = 1;
                stalledJob.MaxAttempts__c = 3;
                stalledJob.StalledCount__c = 0;
                stalledJob.LeaseToken__c = 'lease-token';
                stalledJob.LeaseExpiresAt__c = System.now().addMinutes(-2);
                stalledJob.AvailableAt__c = System.now().addMinutes(-5);
                update stalledJob;

                // Act
                Integer recoveredCount = Queues.of('invoice-sync').recoverStalled();

                // Assert
                Job__c updatedJob = [
                    SELECT State__c, FailedReason__c, StalledCount__c, LeaseToken__c, LeaseExpiresAt__c
                    FROM Job__c
                    WHERE Id = :stalledJob.Id
                ];
                QueueRuntime__c runtime = [
                    SELECT LastSweepAt__c
                    FROM QueueRuntime__c
                    WHERE QueueName__c = 'invoice-sync'
                ];

                Assert.areEqual(1, recoveredCount);
                Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(updatedJob.State__c));
                Assert.areEqual(null, updatedJob.FailedReason__c);
                Assert.areEqual(1, QueuesSupport.toInteger(updatedJob.StalledCount__c, 0));
                Assert.areEqual(null, updatedJob.LeaseToken__c);
                Assert.areEqual(null, updatedJob.LeaseExpiresAt__c);
                Assert.areNotEqual(null, runtime.LastSweepAt__c);
                Assert.areEqual(1, [SELECT COUNT() FROM JobRun__c WHERE Job__c = :stalledJob.Id]);
            } finally {
                QueuesTestSupport.resetFacadeService();
            }
        }
    }

    @IsTest
    static void should_keep_runtime_discoverable_when_non_expired_active_leases_remain() {
        System.runAs(queuesAdminUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('syncWorkerForNextWakeAt', true);
            try {
                Queues.queueService = QueuesTestSupport.newFacadeServiceWithQueueMetadata(workerProvider);

                insert new QueueRuntime__c(
                    QueueName__c = 'invoice-sync',
                    IsPaused__c = false,
                    ActiveWorkerCount__c = 0,
                    WorkerToken__c = 'stale-worker-token',
                    AsyncApexJobId__c = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 41),
                    NextPumpAt__c = System.now().addMinutes(-10)
                );

                Job__c expiredJob = insertQueueJob(
                    'invoice-sync',
                    'sync-invoice',
                    Queues.State.ACTIVE.name(),
                    'invoice:INV-KEEP-DISCOVERY-EXPIRED'
                );
                expiredJob.AttemptsMade__c = 1;
                expiredJob.MaxAttempts__c = 3;
                expiredJob.LeaseToken__c = 'expired-lease';
                expiredJob.LeaseExpiresAt__c = System.now().addMinutes(-2);
                expiredJob.AvailableAt__c = System.now().addMinutes(-5);
                update expiredJob;

                Job__c leasedJob = insertQueueJob(
                    'invoice-sync',
                    'sync-invoice',
                    Queues.State.ACTIVE.name(),
                    'invoice:INV-KEEP-DISCOVERY-LIVE'
                );
                leasedJob.AttemptsMade__c = 1;
                leasedJob.MaxAttempts__c = 3;
                leasedJob.LeaseToken__c = 'live-lease';
                leasedJob.LeaseExpiresAt__c = System.now().addMinutes(3);
                leasedJob.AvailableAt__c = System.now().addMinutes(-5);
                update leasedJob;

                // Act
                Integer recoveredCount = Queues.of('invoice-sync').recoverStalled();

                // Assert
                QueueRuntime__c runtime = [
                    SELECT ActiveWorkerCount__c, WorkerToken__c, AsyncApexJobId__c, LastSweepAt__c
                    FROM QueueRuntime__c
                    WHERE QueueName__c = 'invoice-sync'
                ];
                Job__c updatedExpiredJob = [
                    SELECT State__c, LeaseToken__c, LeaseExpiresAt__c
                    FROM Job__c
                    WHERE Id = :expiredJob.Id
                ];
                Job__c updatedLeasedJob = [
                    SELECT State__c, LeaseToken__c, LeaseExpiresAt__c
                    FROM Job__c
                    WHERE Id = :leasedJob.Id
                ];

                Assert.areEqual(1, recoveredCount);
                Assert.areEqual(1, QueuesSupport.toInteger(runtime.ActiveWorkerCount__c, 0));
                Assert.areEqual(null, runtime.WorkerToken__c);
                Assert.areEqual(null, runtime.AsyncApexJobId__c);
                Assert.areNotEqual(null, runtime.LastSweepAt__c);
                Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(updatedExpiredJob.State__c));
                Assert.areEqual(null, updatedExpiredJob.LeaseToken__c);
                Assert.areEqual(null, updatedExpiredJob.LeaseExpiresAt__c);
                Assert.areEqual(Queues.State.ACTIVE, QueuesSupport.toState(updatedLeasedJob.State__c));
                Assert.areEqual('live-lease', updatedLeasedJob.LeaseToken__c);
                Assert.areNotEqual(null, updatedLeasedJob.LeaseExpiresAt__c);
            } finally {
                QueuesTestSupport.resetFacadeService();
            }
        }
    }

    @IsTest
    static void should_record_queue_error_when_operational_failure_is_logged() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 3);
        Job__c job;
        System.runAs(QueuesTestSupport.queueUser()) {
            job = insertQueueJob(
                'queue-error-integration',
                'sync-invoice',
                Queues.State.WAITING.name(),
                'invoice:ERR-001'
            );
        }
        QueuesErrorService service = new QueuesErrorService();

        // Act
        Test.startTest();
        System.runAs(QueuesTestSupport.queueUser()) {
            service.recordQueueError(
                'queue-error-integration',
                'runtime maintenance',
                job.Id,
                asyncApexJobId,
                new Queues.QueueException('Maintenance exploded.')
            );
        }
        Test.getEventBus().deliver();
        Test.stopTest();

        // Assert
        QueueError__c error = QueuesTestSupport.queryQueueErrorsAsSystem('queue-error-integration')[0];

        Assert.areEqual('queue-error-integration', error.QueueName__c);
        Assert.areEqual('runtime maintenance', error.Component__c);
        Assert.areEqual(job.Id, error.Job__c);
        Assert.areEqual(asyncApexJobId, error.AsyncApexJobId__c);
        Assert.areEqual('Maintenance exploded.', error.Message__c);
        Assert.areEqual(true, String.isNotBlank(error.StackTrace__c));
        Assert.areNotEqual(null, error.OccurredAt__c);
    }

    @IsTest
    static void should_preserve_queue_error_telemetry_when_savepoint_is_rolled_back_after_publish() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 4);
        Job__c job;
        System.runAs(QueuesTestSupport.queueUser()) {
            job = insertQueueJob(
                'queue-error-rollback',
                'sync-invoice',
                Queues.State.WAITING.name(),
                'invoice:ERR-ROLLBACK'
            );
        }
        QueuesErrorService service = new QueuesErrorService();

        // Act
        Test.startTest();
        System.runAs(QueuesTestSupport.queueUser()) {
            Savepoint savepoint = Database.setSavepoint();
            service.recordQueueError(
                'queue-error-rollback',
                'worker scheduling',
                job.Id,
                asyncApexJobId,
                new Queues.QueueException('Publish-before-rollback.')
            );
            Database.rollback(savepoint);
        }
        Test.getEventBus().deliver();
        Test.stopTest();

        // Assert
        QueueError__c error = QueuesTestSupport.queryQueueErrorsAsSystem('queue-error-rollback')[0];

        Assert.areEqual('queue-error-rollback', error.QueueName__c);
        Assert.areEqual('worker scheduling', error.Component__c);
        Assert.areEqual(job.Id, error.Job__c);
        Assert.areEqual(asyncApexJobId, error.AsyncApexJobId__c);
        Assert.areEqual('Publish-before-rollback.', error.Message__c);
    }

    @IsTest
    static void should_run_runtime_maintenance_when_stalled_job_is_discovered_globally() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('findQueueDefinitionAsSystem', queueDefinition)
                    .withReturn('requireQueueDefinition', queueDefinition)
            );
            QueuesTestSupport.RecordingStubProvider workerProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('syncWorkerForNextWakeAt', true);
            QueuesWorkerService workerStub = (QueuesWorkerService) Test.createStub(
                QueuesWorkerService.class,
                workerProvider
            );
            QueuesQueueAdminService queueAdminService = new QueuesQueueAdminService(
                definitionStub,
                new QueuesRuntimeService(),
                new QueuesJobService(),
                new QueuesJobRunService(),
                new QueuesQueueableDispatchService(),
                new QueuesQueueableExecutionService(),
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesStalledJobRecoveryService(),
                new QueuesFlowProducerService(),
                workerStub,
                new QueuesQueueEventService()
            );

            QueuesMaintenanceService maintenanceService = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                definitionStub,
                new QueuesErrorService(),
                queueAdminService,
                new QueuesRuntimeRepository(),
                new QueuesJobRepository(),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            insert new QueueRuntime__c(
                QueueName__c = 'invoice-sync',
                IsPaused__c = false,
                ActiveWorkerCount__c = 0,
                WorkerToken__c = 'stale-worker-token'
            );

            Job__c stalledJob = insertQueueJob(
                'invoice-sync',
                'sync-invoice',
                Queues.State.ACTIVE.name(),
                'invoice:INV-701'
            );
            stalledJob.AttemptsMade__c = 1;
            stalledJob.MaxAttempts__c = 3;
            stalledJob.StalledCount__c = 0;
            stalledJob.LeaseToken__c = 'lease-token';
            stalledJob.LeaseExpiresAt__c = System.now().addMinutes(-2);
            stalledJob.AvailableAt__c = System.now().addMinutes(-5);
            update stalledJob;

            // Act
            Queues.MaintenanceResult recovered = maintenanceService.runRuntimeMaintenance();

            // Assert
            Job__c updatedJob = [
                SELECT State__c, FailedReason__c, StalledCount__c, LeaseToken__c, LeaseExpiresAt__c
                FROM Job__c
                WHERE Id = :stalledJob.Id
            ];
            QueueRuntime__c runtime = [
                SELECT LastSweepAt__c
                FROM QueueRuntime__c
                WHERE QueueName__c = 'invoice-sync'
            ];

            Assert.areEqual(1, recovered.total);
            Assert.areEqual(1, recovered.getCount('invoice-sync'));
            Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(updatedJob.State__c));
            Assert.areEqual(null, updatedJob.FailedReason__c);
            Assert.areEqual(1, QueuesSupport.toInteger(updatedJob.StalledCount__c, 0));
            Assert.areEqual(null, updatedJob.LeaseToken__c);
            Assert.areEqual(null, updatedJob.LeaseExpiresAt__c);
            Assert.areNotEqual(null, runtime.LastSweepAt__c);
            Assert.areEqual(1, [SELECT COUNT() FROM JobRun__c WHERE Job__c = :stalledJob.Id]);
        }
    }

    @IsTest
    static void should_materialize_one_queue_error_when_maintenance_reports_same_failure_twice() {
        String queueName = 'maintenance-dedupe-' + System.now().getTime();
        Queues.MaintenanceResult recovered;

        Test.startTest();
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('findQueueDefinitionAsSystem', queueDefinition)
            );
            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn(
                        'queryMaintenanceRuntimes',
                        new List<QueueRuntime__c>{ new QueueRuntime__c(QueueName__c = queueName) }
                    )
            );
            QueuesErrorService errorService = new QueuesErrorService();
            QueuesQueueAdminService queueAdminStub = (QueuesQueueAdminService) Test.createStub(
                QueuesQueueAdminService.class,
                new MaintenanceSchedulingFailureStubProvider(errorService)
            );

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                definitionStub,
                errorService,
                queueAdminStub,
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            recovered = service.runRuntimeMaintenance();
        }
        Test.getEventBus().deliver();
        Test.stopTest();

        // Assert
        List<QueueError__c> errors = QueuesTestSupport.queryQueueErrorsAsSystem(queueName);

        Assert.areEqual(0, recovered.total);
        Assert.areEqual(1, errors.size());
        Assert.areEqual(queueName, errors[0].QueueName__c);
        Assert.areEqual('worker scheduling', errors[0].Component__c);
        Assert.areEqual('Maintenance scheduling failed.', errors[0].Message__c);
    }

    @IsTest
    static void should_record_failed_run_when_handling_catastrophic_batch_scope_failure() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 1);
            Job__c job = insertQueueJob('invoice-sync', 'sync-invoice', Queues.State.ACTIVE.name(), 'invoice:INV-702');
            job.AttemptsMade__c = 1;
            job.MaxAttempts__c = 3;
            job.AsyncApexJobId__c = asyncApexJobId;
            update job;

            Datetime occurredAt = System.now();
            QueuesBatchApexErrorEventHandler handler = new QueuesBatchApexErrorEventHandler();

            // Act
            handler.handleFailureScopes(
                new List<QueuesBatchApexErrorEventHandler.FailureScope>{
                    new QueuesBatchApexErrorEventHandler.FailureScope(
                        new Set<Id>{ job.Id },
                        asyncApexJobId,
                        occurredAt,
                        'System.LimitException: Too many SOQL queries: 101',
                        'stack trace'
                    )
                }
            );

            // Assert
            Job__c updatedJob = [
                SELECT State__c, AttemptsMade__c, FailedReason__c
                FROM Job__c
                WHERE Id = :job.Id
            ];
            JobRun__c run = [
                SELECT AttemptNumber__c, Status__c, Message__c, AsyncApexJobId__c
                FROM JobRun__c
                WHERE Job__c = :job.Id
                LIMIT 1
            ];

            Assert.areEqual(Queues.State.WAITING, QueuesSupport.toState(updatedJob.State__c));
            Assert.areEqual(2, QueuesSupport.toInteger(updatedJob.AttemptsMade__c, 0));
            Assert.areEqual('System.LimitException: Too many SOQL queries: 101', updatedJob.FailedReason__c);
            Assert.areEqual(2, QueuesSupport.toInteger(run.AttemptNumber__c, 0));
            Assert.areEqual(
                Queues.RunStatus.FAILED,
                Queues.RunStatus.valueOf(QueuesSupport.normalizeEnumValue(run.Status__c))
            );
            Assert.areEqual(true, run.Message__c.contains('stack trace'));
            Assert.areEqual(asyncApexJobId, run.AsyncApexJobId__c);
        }
    }

    @IsTest
    static void should_ignore_stale_batch_error_scope_when_job_claim_owner_points_to_newer_worker() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String staleAsyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 51);
            String currentAsyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 52);
            Job__c job = insertQueueJob('invoice-sync', 'sync-invoice', Queues.State.ACTIVE.name(), 'invoice:INV-703');
            job.AttemptsMade__c = 1;
            job.MaxAttempts__c = 3;
            job.LeaseToken__c = 'current-lease-token';
            job.LeaseExpiresAt__c = System.now().addMinutes(5);
            job.AsyncApexJobId__c = currentAsyncApexJobId;
            update job;

            QueuesBatchApexErrorEventHandler handler = new QueuesBatchApexErrorEventHandler();

            // Act
            handler.handleFailureScopes(
                new List<QueuesBatchApexErrorEventHandler.FailureScope>{
                    new QueuesBatchApexErrorEventHandler.FailureScope(
                        new Set<Id>{ job.Id },
                        staleAsyncApexJobId,
                        System.now(),
                        'System.LimitException: Too many SOQL queries: 101',
                        'stack trace'
                    )
                }
            );

            // Assert
            Job__c updatedJob = [
                SELECT State__c, AttemptsMade__c, FailedReason__c, LeaseToken__c, LeaseExpiresAt__c
                FROM Job__c
                WHERE Id = :job.Id
            ];

            Assert.areEqual(Queues.State.ACTIVE, QueuesSupport.toState(updatedJob.State__c));
            Assert.areEqual(1, QueuesSupport.toInteger(updatedJob.AttemptsMade__c, 0));
            Assert.areEqual(null, updatedJob.FailedReason__c);
            Assert.areEqual('current-lease-token', updatedJob.LeaseToken__c);
            Assert.areNotEqual(null, updatedJob.LeaseExpiresAt__c);
            Assert.areEqual(0, [SELECT COUNT() FROM JobRun__c WHERE Job__c = :job.Id]);
        }
    }

    @IsTest
    static void should_schedule_and_unschedule_runtime_maintenance_job() {
        // Arrange
        QueuesMaintenanceSchedule maintenanceSchedule = new QueuesMaintenanceSchedule();

        // Act
        String scheduledJobId = maintenanceSchedule.schedule('0 0 0 1 1 ? 2099');
        Boolean isScheduled = maintenanceSchedule.isScheduled();
        List<CronTrigger> scheduledJobs = [
            SELECT Id, CronJobDetail.Name
            FROM CronTrigger
            WHERE Id = :scheduledJobId
        ];
        Integer unscheduledCount = maintenanceSchedule.unschedule();

        // Assert
        Assert.areEqual(true, isScheduled);
        Assert.areEqual(1, scheduledJobs.size());
        Assert.areEqual(QueuesMaintenanceSchedule.resolveJobName(), scheduledJobs[0].CronJobDetail.Name);
        Assert.areEqual(1, unscheduledCount);
    }

    @IsTest
    static void should_build_value_objects_when_using_queue_primitives() {
        // Arrange
        Datetime availableAt = System.now().addMinutes(5);

        // Act
        Queues.JobOptions options = new Queues.JobOptions()
            .jobId('job-primitive')
            .attempts(4)
            .priority(2)
            .delay(3)
            .availableAt(availableAt)
            .backoff(Queues.Backoff.FIXED, 7);
        Queues.JobData jobData = new Queues.JobData(
            'sync-invoice',
            QueuesTestSupport.DEFAULT_PROCESSOR_CLASS,
            new Map<String, Object>{ 'invoiceId' => 'INV-601' },
            options
        );
        Queues.QueueStats stats = new Queues.QueueStats('invoice-sync')
            .withRuntime(true, 2)
            .withStateCount(Queues.State.WAITING, 3)
            .withStateCount(Queues.State.FAILED, 1);

        // Assert
        Assert.areEqual('job-primitive', options.getJobId());
        Assert.areEqual(4, options.getAttempts());
        Assert.areEqual(2, options.getPriority());
        Assert.areEqual(3, options.getDelay());
        Assert.areEqual(availableAt, options.getAvailableAt());
        Assert.areEqual(Queues.Backoff.FIXED, options.getBackoffType());
        Assert.areEqual(7, options.getBackoffValue());
        Assert.areEqual('sync-invoice', jobData.getName());
        Assert.areEqual(QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, jobData.getProcessor());
        Assert.areEqual(4, stats.total);
        Assert.areEqual(3, stats.getCount(Queues.State.WAITING));
        Assert.areEqual(1, stats.getCount(Queues.State.FAILED));
        Assert.areEqual(true, stats.isPaused);
        Assert.areEqual(2, stats.activeWorkerCount);
    }

    @IsTest
    static void should_throw_queue_exception_when_inputs_are_invalid() {
        // Arrange
        String invalidAttemptsMessage;
        String invalidPriorityMessage;
        String blankNameMessage;
        String blankProcessorMessage;
        String longJobIdMessage;
        String longJobId = ''.leftPad(175, 'x');

        // Act
        try {
            new Queues.JobOptions().attempts(0);
            Assert.fail('Expected QueueException for invalid attempts.');
        } catch (Queues.QueueException ex) {
            invalidAttemptsMessage = ex.getMessage();
        }

        try {
            new Queues.JobOptions().priority(-1);
            Assert.fail('Expected QueueException for invalid priority.');
        } catch (Queues.QueueException ex) {
            invalidPriorityMessage = ex.getMessage();
        }

        try {
            new Queues.JobData('', QueuesTestSupport.DEFAULT_PROCESSOR_CLASS, null);
            Assert.fail('Expected QueueException for blank name.');
        } catch (Queues.QueueException ex) {
            blankNameMessage = ex.getMessage();
        }

        try {
            new Queues.JobData('sync-invoice', '', null);
            Assert.fail('Expected QueueException for blank processor.');
        } catch (Queues.QueueException ex) {
            blankProcessorMessage = ex.getMessage();
        }

        try {
            new Queues.JobOptions().jobId(longJobId);
            Assert.fail('Expected QueueException for oversized jobId.');
        } catch (Queues.QueueException ex) {
            longJobIdMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual('attempts must be greater than 0', invalidAttemptsMessage);
        Assert.areEqual('priority must be zero or greater', invalidPriorityMessage);
        Assert.areEqual('name cannot be blank', blankNameMessage);
        Assert.areEqual('processor cannot be blank', blankProcessorMessage);
        Assert.areEqual('jobId must be 174 characters or fewer', longJobIdMessage);
    }

    private static Job__c insertQueueJob(
        final String queueName,
        final String name,
        final String stateValue,
        final String externalId
    ) {
        Job__c job = QueuesTestSupport.job(queueName, name, stateValue, externalId);
        insert job;
        return job;
    }

    private static QueuesService newQueueableFacadeService(
        final QueuesQueueableDispatchService queueableDispatchService,
        final QueuesTestSupport.RecordingStubProvider workerProvider
    ) {
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
        QueuesDefinitionService definitionService = new QueuesDefinitionService(
            (QueuesDefinitionRepository) Test.createStub(
                QueuesDefinitionRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ queueDefinition })
            )
        );
        QueuesProcessorService processorService = new QueuesProcessorService(
            (QueuesProcessorRepository) Test.createStub(
                QueuesProcessorRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryProcessorsByClassNames', new List<Processor__mdt>{ processorDefinition })
            )
        );
        QueuesWorkerService workerService = (QueuesWorkerService) Test.createStub(
            QueuesWorkerService.class,
            workerProvider
        );

        return new QueuesService(
            definitionService,
            new QueuesErrorService(),
            new QueuesFlowProducerService(),
            new QueuesJobService(),
            new QueuesJobAdminService(),
            new QueuesJobSchedulerService(),
            new QueuesMaintenanceService(),
            new QueuesMaintenanceSchedule(),
            processorService,
            queueableDispatchService,
            new QueuesQueueAdminService(),
            new QueuesStatsService(),
            workerService
        );
    }

    private static TestableQueueableExecutionService newQueueableExecutionService() {
        Processor__mdt processorDefinition = QueuesTestSupport.processorDefinition();
        Queues.ProcessResult failedResult = new Queues.JobContext(new Job__c()).fail('handled retry');
        QueuesProcessorService processorService = (QueuesProcessorService) Test.createStub(
            QueuesProcessorService.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'getProcessorsByClassAsSystem',
                    new Map<String, Processor__mdt>{ QueuesTestSupport.DEFAULT_PROCESSOR_CLASS => processorDefinition }
                )
                .withReturn('instantiate', 'processor')
                .withReturn('execute', failedResult)
        );
        return new TestableQueueableExecutionService(
            new QueuesJobExecutionService(
                new QueuesJobService(),
                new QueuesJobRunService(),
                processorService,
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            ),
            new QueuesJobService()
        );
    }

    private class TestableQueueableDispatchService extends QueuesQueueableDispatchService {
        private Integer dispatcherCount = 0;
        private Integer executorCount = 0;

        protected override Id enqueueDispatcherJob(final Id dispatchId, final Boolean useInitialStackDepth) {
            dispatcherCount++;
            return QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 1200 + dispatcherCount);
        }

        protected override Id enqueueExecutor(final Id jobId, final Integer delayMinutes) {
            executorCount++;
            return QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 1210 + executorCount);
        }
    }

    private class TestableQueueableExecutionService extends QueuesQueueableExecutionService {
        private Integer executorCount = 0;
        private Integer lastDelayMinutes;

        private TestableQueueableExecutionService(
            final QueuesJobExecutionService jobExecutionService,
            final QueuesJobService jobService
        ) {
            super(jobExecutionService, jobService, new QueuesQueueEventService());
        }

        protected override Id enqueueExecutor(final Id jobId, final Integer delayMinutes) {
            executorCount++;
            lastDelayMinutes = delayMinutes;
            return QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 1220 + executorCount);
        }

        protected override Boolean shouldBridgeQueueableStack() {
            return false;
        }
    }

    private class MaintenanceSchedulingFailureStubProvider implements System.StubProvider {
        private final QueuesErrorService errorService;

        private MaintenanceSchedulingFailureStubProvider(final QueuesErrorService errorService) {
            this.errorService = errorService;
        }

        public Object handleMethodCall(
            final Object stubbedObject,
            final String stubbedMethodName,
            final Type returnType,
            final List<Type> listOfParamTypes,
            final List<String> listOfParamNames,
            final List<Object> listOfArgs
        ) {
            if (stubbedMethodName != 'recoverStalled' && stubbedMethodName != 'recoverStalledAsSystem') {
                return null;
            }

            String queueName = (String) listOfArgs[0];
            Exception ex = new Queues.QueueException('Maintenance scheduling failed.');
            errorService.recordQueueError(queueName, 'worker scheduling', ex);
            throw ex;
        }
    }

    public class CompletingProcessor implements Queues.JobProcessor {
        public Queues.ProcessResult process(final Queues.JobContext ctx) {
            return ctx.complete(new Map<String, Object>{ 'status' => 'done' })
                .withProgress(new Map<String, Object>{ 'step' => 'completed' });
        }
    }

    public class UnrecoverableThrowingProcessor implements Queues.JobProcessor {
        public Queues.ProcessResult process(final Queues.JobContext ctx) {
            throw new Queues.UnrecoverableJobException('Unrecoverable processor failure.');
        }
    }
}
