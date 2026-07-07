@IsTest(IsParallel=false)
private class QueuesMaintenanceServiceTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    // <editor-fold desc="Runtime Maintenance">
    @IsTest
    static void should_construct_default_runtime_maintenance_service() {
        System.runAs(QueuesTestSupport.queueUser()) {
            QueuesMaintenanceService service = new QueuesMaintenanceService();

            Assert.areEqual(0, service.queryRuntimeMaintenanceQueueNames().size());
        }
    }

    @IsTest
    static void should_recover_configured_queues_when_runtime_requires_maintenance() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();

            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn(
                        'queryMaintenanceRuntimes',
                        new List<QueueRuntime__c>{
                            new QueueRuntime__c(QueueName__c = 'invoice-sync'),
                            new QueueRuntime__c(QueueName__c = 'payments-sync')
                        }
                    )
            );
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('findQueueDefinitionAsSystem', queueDefinition)
                    .withReturn('getLeaseMinutes', 5)
            );
            QueuesTestSupport.RecordingStubProvider queueAdminProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturns('recoverStalled', new List<Object>{ 1, 2 });
            QueuesQueueAdminService queueAdminStub = (QueuesQueueAdminService) Test.createStub(
                QueuesQueueAdminService.class,
                queueAdminProvider
            );

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                definitionStub,
                new QueuesErrorService(),
                queueAdminStub,
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                emptyScheduleRepositoryStub(),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Set<String> queueNames = service.queryRuntimeMaintenanceQueueNames();
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(2, queueNames.size());
            Assert.areEqual(true, queueNames.contains('invoice-sync'));
            Assert.areEqual(true, queueNames.contains('payments-sync'));
            Assert.areEqual(3, recovered.total);
            Assert.areEqual(1, recovered.getCount('invoice-sync'));
            Assert.areEqual(2, recovered.getCount('payments-sync'));
            Assert.areEqual(2, queueAdminProvider.getCallCount('recoverStalled'));
        }
    }

    @IsTest
    static void should_abandon_recovery_events_when_runtime_maintenance_queue_rolls_back() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'maintenance-event-rollback';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            Job__c aggregateSource = QueuesTestSupport.job(
                queueName,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING_CHILDREN.name(),
                'maintenance-event-rollback-source'
            );
            insert aggregateSource;
            List<AggregateResult> queueNameRows = [
                SELECT QueueName__c queueName
                FROM Job__c
                WHERE Id = :aggregateSource.Id
                GROUP BY QueueName__c
            ];

            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('findQueueDefinitionAsSystem', queueDefinition)
                    .withReturn('getLeaseMinutes', 5)
            );
            QueuesJobRepository jobRepositoryStub = (QueuesJobRepository) Test.createStub(
                QueuesJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryWaitingChildrenMaintenanceJobQueueNames', queueNameRows)
                    .withReturn('queryRemovedDependencyMaintenanceJobQueueNames', queueNameRows)
                    .withReturn('hasStaleWaitingChildrenJob', true)
                    .withReturn('hasStaleRemovedDependency', true)
            );
            QueuesTestSupport.RecordingStubProvider flowProducerProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('repairStaleParentDependenciesAsSystem', 1)
                .withException(
                    'repairStaleRemovedFailureDependenciesAsSystem',
                    new Queues.QueueException('dependency cleanup exploded')
                );
            QueuesTestSupport.RecordingStubProvider eventProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesTestSupport.RecordingStubProvider errorEventProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesMaintenanceService service = new QueuesMaintenanceService(
                emptyAsyncApexJobRepositoryStub(),
                definitionStub,
                newRecordingErrorService(errorEventProvider),
                new QueuesQueueAdminService(),
                emptyRuntimeRepositoryStub(),
                jobRepositoryStub,
                emptyScheduleRepositoryStub(),
                new QueuesInvocableExecutionService(),
                emptyQueueableDispatchRepositoryStub(),
                (QueuesFlowProducerService) Test.createStub(QueuesFlowProducerService.class, flowProducerProvider),
                QueuesTestSupport.newRecordingQueueEventService(queueName, 'RECOVERED', eventProvider)
            );

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.isFalse(recovered.done);
            Assert.areEqual(0, recovered.total);
            Assert.areEqual(1, flowProducerProvider.getCallCount('repairStaleParentDependenciesAsSystem'));
            Assert.areEqual(1, flowProducerProvider.getCallCount('repairStaleRemovedFailureDependenciesAsSystem'));
            Assert.areEqual(0, eventProvider.getCallCount('publishQueueEvents'));
            Assert.areEqual(1, errorEventProvider.getCallCount('publishQueueError'));
        }
    }

    @IsTest
    static void should_recover_queueable_only_active_jobs_when_runtime_has_no_worker_hint() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queueable-active-maint';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            Job__c activeQueueableJob = QueuesTestSupport.job(
                queueName,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.ACTIVE.name(),
                'queueable-active-maint-job'
            );
            activeQueueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            insert activeQueueableJob;
            List<AggregateResult> activeQueueableQueueNames = [
                SELECT QueueName__c queueName
                FROM Job__c
                WHERE Id = :activeQueueableJob.Id
                GROUP BY QueueName__c
            ];

            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesAsyncApexJobRepository asyncApexJobRepositoryStub = (QueuesAsyncApexJobRepository) Test.createStub(
                QueuesAsyncApexJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryActiveAsyncJobsByApexClass', new List<AsyncApexJob>())
            );
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('findQueueDefinitionAsSystem', queueDefinition)
                    .withReturn('getLeaseMinutes', 5)
            );
            QueuesTestSupport.RecordingStubProvider queueAdminProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('recoverStalled', 1);
            QueuesJobRepository jobRepositoryStub = (QueuesJobRepository) Test.createStub(
                QueuesJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryQueueableMaintenanceJobQueueNames', activeQueueableQueueNames)
                    .withReturn('hasExpiredActiveQueueableJob', true)
            );

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                asyncApexJobRepositoryStub,
                definitionStub,
                new QueuesErrorService(),
                (QueuesQueueAdminService) Test.createStub(QueuesQueueAdminService.class, queueAdminProvider),
                runtimeRepositoryStub,
                jobRepositoryStub,
                emptyScheduleRepositoryStub(),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Set<String> queueNames = service.queryRuntimeMaintenanceQueueNames();
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(new Set<String>{ queueName }, queueNames);
            Assert.areEqual(1, recovered.total);
            Assert.areEqual(1, queueAdminProvider.getCallCount('recoverStalled'));
            Assert.areEqual(queueName, (String) queueAdminProvider.getLastArgs('recoverStalled')[0]);
        }
    }

    @IsTest
    static void should_recover_queueable_only_stale_initiated_jobs_when_runtime_has_no_worker_hint() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queueable-initiated-maint';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            Job__c staleQueueableJob = QueuesTestSupport.job(
                queueName,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.WAITING.name(),
                'queueable-initiated-maint-job'
            );
            staleQueueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
            staleQueueableJob.QueueableDispatchInitiatedAt__c = System.now().addMinutes(-10);
            insert staleQueueableJob;
            List<AggregateResult> staleQueueableQueueNames = [
                SELECT QueueName__c queueName
                FROM Job__c
                WHERE Id = :staleQueueableJob.Id
                GROUP BY QueueName__c
            ];

            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesAsyncApexJobRepository asyncApexJobRepositoryStub = (QueuesAsyncApexJobRepository) Test.createStub(
                QueuesAsyncApexJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryActiveAsyncJobsByApexClass', new List<AsyncApexJob>())
            );
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('findQueueDefinitionAsSystem', queueDefinition)
                    .withReturn('getLeaseMinutes', 5)
            );
            QueuesTestSupport.RecordingStubProvider queueAdminProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('recoverStalled', 1);
            QueuesJobRepository jobRepositoryStub = (QueuesJobRepository) Test.createStub(
                QueuesJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryQueueableMaintenanceJobQueueNames', new List<AggregateResult>())
                    .withReturn('queryStaleInitiatedQueueableMaintenanceJobQueueNames', staleQueueableQueueNames)
                    .withReturn('hasStaleInitiatedQueueableJob', true)
            );

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                asyncApexJobRepositoryStub,
                definitionStub,
                new QueuesErrorService(),
                (QueuesQueueAdminService) Test.createStub(QueuesQueueAdminService.class, queueAdminProvider),
                runtimeRepositoryStub,
                jobRepositoryStub,
                emptyScheduleRepositoryStub(),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Set<String> queueNames = service.queryRuntimeMaintenanceQueueNames();
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(new Set<String>{ queueName }, queueNames);
            Assert.areEqual(1, recovered.total);
            Assert.areEqual(1, queueAdminProvider.getCallCount('recoverStalled'));
            Assert.areEqual(queueName, (String) queueAdminProvider.getLastArgs('recoverStalled')[0]);
        }
    }

    @IsTest
    static void should_recover_queueable_dispatches_when_runtime_has_no_worker_hint() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queueable-dispatch-maint';
            Datetime currentNow = System.now();
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            QueueableDispatch__c staleDispatch = new QueueableDispatch__c(
                QueueName__c = queueName,
                State__c = Queues.QueueableDispatchState.WAITING.name(),
                Mode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name(),
                RequestedCount__c = 1,
                DispatchedCount__c = 0,
                DispatchDepth__c = 0
            );
            insert staleDispatch;
            Test.setCreatedDate(staleDispatch.Id, currentNow.addMinutes(-10));

            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesAsyncApexJobRepository asyncApexJobRepositoryStub = (QueuesAsyncApexJobRepository) Test.createStub(
                QueuesAsyncApexJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryActiveAsyncJobsByApexClass', new List<AsyncApexJob>())
            );
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('findQueueDefinitionAsSystem', queueDefinition)
                    .withReturn('getLeaseMinutes', 5)
            );
            QueuesTestSupport.RecordingStubProvider queueAdminProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('recoverStalled', 1);

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                asyncApexJobRepositoryStub,
                definitionStub,
                new QueuesErrorService(),
                (QueuesQueueAdminService) Test.createStub(QueuesQueueAdminService.class, queueAdminProvider),
                runtimeRepositoryStub,
                (QueuesJobRepository) Test.createStub(
                    QueuesJobRepository.class,
                    new QueuesTestSupport.RecordingStubProvider()
                ),
                emptyScheduleRepositoryStub(),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Set<String> queueNames = service.queryRuntimeMaintenanceQueueNames();
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(new Set<String>{ queueName }, queueNames);
            Assert.areEqual(1, recovered.total);
            Assert.areEqual(1, queueAdminProvider.getCallCount('recoverStalled'));
            Assert.areEqual(queueName, (String) queueAdminProvider.getLastArgs('recoverStalled')[0]);
        }
    }

    @IsTest
    static void should_discover_stale_waiting_children_queues_for_dependency_repair() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'dependency-maint';
            Job__c waitingParent = QueuesTestSupport.job(
                queueName,
                'waiting-parent',
                Queues.State.WAITING_CHILDREN.name(),
                'waiting-parent'
            );
            insert waitingParent;
            List<AggregateResult> waitingChildrenQueueNames = [
                SELECT QueueName__c queueName
                FROM Job__c
                WHERE Id = :waitingParent.Id
                GROUP BY QueueName__c
            ];
            QueuesJobRepository jobRepositoryStub = (QueuesJobRepository) Test.createStub(
                QueuesJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryWaitingChildrenMaintenanceJobQueueNames', waitingChildrenQueueNames)
                    .withReturn('hasStaleWaitingChildrenJob', true)
            );
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('findQueueDefinitionAsSystem', QueuesTestSupport.queueDefinition(queueName, true))
                    .withReturn('getLeaseMinutes', 5)
            );
            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                definitionStub,
                new QueuesErrorService(),
                new QueuesQueueAdminService(),
                emptyRuntimeRepositoryStub(),
                jobRepositoryStub,
                emptyScheduleRepositoryStub(),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Set<String> queueNames = service.queryDependencyMaintenanceQueueNames();
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(new Set<String>{ queueName }, queueNames);
            Assert.areEqual(0, recovered.total);
        }
    }

    @IsTest
    static void should_repair_stale_removed_dependencies() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'dependency-cleanup-maint';
            Job__c parent = QueuesTestSupport.job(queueName, 'parent', Queues.State.WAITING.name(), 'parent');
            insert parent;
            Job__c failedChild = QueuesTestSupport.job(queueName, 'child', Queues.State.FAILED.name(), 'child');
            failedChild.Parent__c = parent.Id;
            failedChild.DependencyFailurePolicy__c = Queues.DependencyFailurePolicy.REMOVE_DEPENDENCY.name();
            insert failedChild;
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('findQueueDefinitionAsSystem', QueuesTestSupport.queueDefinition(queueName, true))
                    .withReturn('getLeaseMinutes', -1)
            );
            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                definitionStub,
                new QueuesErrorService(),
                new QueuesQueueAdminService(),
                emptyRuntimeRepositoryStub(),
                new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE),
                emptyScheduleRepositoryStub(),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Set<String> queueNames = service.queryDependencyCleanupMaintenanceQueueNames();
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            failedChild = [SELECT Parent__c FROM Job__c WHERE Id = :failedChild.Id];
            Assert.areEqual(new Set<String>{ queueName }, queueNames);
            Assert.areEqual(1, recovered.total);
            Assert.areEqual(null, failedChild.Parent__c);
        }
    }

    @IsTest
    static void should_recover_invocable_only_active_jobs_when_runtime_has_no_worker_hint() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'invocable-active-maint';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            Job__c aggregateSeedJob = QueuesTestSupport.job(
                queueName,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.ACTIVE.name(),
                'invocable-active-maint-job'
            );
            insert aggregateSeedJob;
            List<AggregateResult> activeInvocableQueueNames = [
                SELECT QueueName__c queueName
                FROM Job__c
                WHERE Id = :aggregateSeedJob.Id
                GROUP BY QueueName__c
            ];

            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesAsyncApexJobRepository asyncApexJobRepositoryStub = (QueuesAsyncApexJobRepository) Test.createStub(
                QueuesAsyncApexJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryActiveAsyncJobsByApexClass', new List<AsyncApexJob>())
            );
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('findQueueDefinitionAsSystem', queueDefinition)
                    .withReturn('getLeaseMinutes', 5)
            );
            QueuesTestSupport.RecordingStubProvider jobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryQueueableMaintenanceJobQueueNames', new List<AggregateResult>())
                .withReturn('queryInvocableMaintenanceJobQueueNames', activeInvocableQueueNames)
                .withReturn('hasRecoverableInvocableJob', true);
            QueuesTestSupport.RecordingStubProvider queueAdminProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('recoverStalled', 10);
            QueuesTestSupport.RecordingStubProvider invocableProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('recoverAsSystem', 1);

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                asyncApexJobRepositoryStub,
                definitionStub,
                new QueuesErrorService(),
                (QueuesQueueAdminService) Test.createStub(QueuesQueueAdminService.class, queueAdminProvider),
                runtimeRepositoryStub,
                (QueuesJobRepository) Test.createStub(QueuesJobRepository.class, jobRepositoryProvider),
                emptyScheduleRepositoryStub(),
                (QueuesInvocableExecutionService) Test.createStub(
                    QueuesInvocableExecutionService.class,
                    invocableProvider
                ),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Set<String> queueNames = service.queryInvocableMaintenanceQueueNames();
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(new Set<String>{ queueName }, queueNames);
            Assert.areEqual(1, recovered.total);
            Assert.areEqual(0, queueAdminProvider.getCallCount('recoverStalled'));
            Assert.areEqual(1, invocableProvider.getCallCount('recoverAsSystem'));
            Assert.areEqual(queueName, (String) invocableProvider.getLastArgs('recoverAsSystem')[0]);
        }
    }

    @IsTest
    static void should_skip_queueable_maintenance_queue_names_without_expired_recovery_candidates() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String healthyQueueName = 'queueable-healthy-maint';
            String stalledQueueName = 'queueable-stalled-maint';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(healthyQueueName, true);
            Job__c healthyJob = QueuesTestSupport.job(
                healthyQueueName,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.ACTIVE.name(),
                'queueable-healthy-maint-job'
            );
            healthyJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            Job__c stalledJob = QueuesTestSupport.job(
                stalledQueueName,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.ACTIVE.name(),
                'queueable-stalled-maint-job'
            );
            stalledJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            insert new List<Job__c>{ healthyJob, stalledJob };
            Set<Id> activeJobIds = new Set<Id>{ healthyJob.Id, stalledJob.Id };
            List<AggregateResult> activeQueueableQueueNames = [
                SELECT QueueName__c queueName
                FROM Job__c
                WHERE Id IN :activeJobIds
                GROUP BY QueueName__c
                ORDER BY QueueName__c ASC
            ];

            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('findQueueDefinitionAsSystem', queueDefinition)
                    .withReturn('getLeaseMinutes', 5)
            );
            QueuesTestSupport.RecordingStubProvider jobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryQueueableMaintenanceJobQueueNames', activeQueueableQueueNames)
                .withReturns('hasExpiredActiveQueueableJob', new List<Object>{ false, true, false, true });
            QueuesTestSupport.RecordingStubProvider queueAdminProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('recoverStalled', 3);

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                definitionStub,
                new QueuesErrorService(),
                (QueuesQueueAdminService) Test.createStub(QueuesQueueAdminService.class, queueAdminProvider),
                runtimeRepositoryStub,
                (QueuesJobRepository) Test.createStub(QueuesJobRepository.class, jobRepositoryProvider),
                emptyScheduleRepositoryStub(),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Set<String> queueNames = service.queryRuntimeMaintenanceQueueNames();
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(new Set<String>{ stalledQueueName }, queueNames);
            Assert.areEqual(3, recovered.total);
            Assert.areEqual(1, queueAdminProvider.getCallCount('recoverStalled'));
            Assert.areEqual(stalledQueueName, (String) queueAdminProvider.getLastArgs('recoverStalled')[0]);
            Assert.areEqual(4, jobRepositoryProvider.getCallCount('hasExpiredActiveQueueableJob'));
        }
    }

    @IsTest
    static void should_skip_unconfigured_queueable_maintenance_queue_names() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queueable-unconfigured-maint';
            Job__c activeQueueableJob = QueuesTestSupport.job(
                queueName,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.ACTIVE.name(),
                'queueable-unconfigured-maint-job'
            );
            activeQueueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            insert activeQueueableJob;
            List<AggregateResult> activeQueueableQueueNames = [
                SELECT QueueName__c queueName
                FROM Job__c
                WHERE Id = :activeQueueableJob.Id
                GROUP BY QueueName__c
            ];

            QueuesTestSupport.RecordingStubProvider jobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryQueueableMaintenanceJobQueueNames', activeQueueableQueueNames);
            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                (QueuesDefinitionService) Test.createStub(
                    QueuesDefinitionService.class,
                    new QueuesTestSupport.RecordingStubProvider().withReturn('findQueueDefinitionAsSystem', null)
                ),
                new QueuesErrorService(),
                new QueuesQueueAdminService(),
                emptyRuntimeRepositoryStub(),
                (QueuesJobRepository) Test.createStub(QueuesJobRepository.class, jobRepositoryProvider),
                emptyScheduleRepositoryStub(),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Set<String> queueNames = service.queryRuntimeMaintenanceQueueNames();

            // Assert
            Assert.areEqual(0, queueNames.size());
            Assert.areEqual(0, jobRepositoryProvider.getCallCount('hasExpiredActiveQueueableJob'));
        }
    }

    @IsTest
    static void should_stop_queueable_maintenance_discovery_when_query_headroom_is_exhausted() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queueable-headroom-maint';
            Job__c activeQueueableJob = QueuesTestSupport.job(
                queueName,
                QueuesTestSupport.DEFAULT_JOB_NAME,
                Queues.State.ACTIVE.name(),
                'queueable-headroom-maint-job'
            );
            activeQueueableJob.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
            insert activeQueueableJob;
            List<AggregateResult> activeQueueableQueueNames = [
                SELECT QueueName__c queueName
                FROM Job__c
                WHERE Id = :activeQueueableJob.Id
                GROUP BY QueueName__c
            ];

            QueuesTestSupport.RecordingStubProvider jobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryQueueableMaintenanceJobQueueNames', activeQueueableQueueNames)
                .withReturn('hasExpiredActiveQueueableJob', true);
            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesDefinitionService(),
                new QueuesErrorService(),
                new QueuesQueueAdminService(),
                emptyRuntimeRepositoryStub(),
                (QueuesJobRepository) Test.createStub(QueuesJobRepository.class, jobRepositoryProvider),
                emptyScheduleRepositoryStub(),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            exhaustMaintenanceQueryHeadroom();

            // Act
            Set<String> queueNames = service.queryRuntimeMaintenanceQueueNames();

            // Assert
            Assert.areEqual(0, queueNames.size());
            Assert.areEqual(0, jobRepositoryProvider.getCallCount('hasExpiredActiveQueueableJob'));
        }
    }

    @IsTest
    static void should_record_queue_error_when_recovery_throws_exception() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();

            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn(
                        'queryMaintenanceRuntimes',
                        new List<QueueRuntime__c>{ new QueueRuntime__c(QueueName__c = 'invoice-sync') }
                    )
            );
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider().withReturn('findQueueDefinitionAsSystem', queueDefinition)
            );
            QueuesQueueAdminService queueAdminStub = (QueuesQueueAdminService) Test.createStub(
                QueuesQueueAdminService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withException('recoverStalled', new Queues.QueueException('Maintenance failed.'))
            );
            QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('publishQueueError', successfulPublishResult());
            QueuesErrorEventRepository eventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
                QueuesErrorEventRepository.class,
                eventRepositoryProvider
            );
            QueuesErrorService errorService = new QueuesErrorService(new QueuesErrorRepository(), eventRepositoryStub);

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                definitionStub,
                errorService,
                queueAdminStub,
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                emptyScheduleRepositoryStub(),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(0, recovered.total);
            Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
            QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) eventRepositoryProvider.getLastArgs(
                'publishQueueError'
            )[0];
            Assert.areEqual('invoice-sync', errorEvent.QueueName__c);
            Assert.areEqual('runtime maintenance', errorEvent.Component__c);
            Assert.areEqual('Maintenance failed.', errorEvent.Message__c);
        }
    }

    @IsTest
    static void should_skip_blank_or_unconfigured_queue_names_when_discovering_runtime_maintenance_candidates() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'payments-sync-' + System.now().getTime();

            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn(
                        'queryMaintenanceRuntimes',
                        new List<QueueRuntime__c>{
                            new QueueRuntime__c(QueueName__c = ''),
                            new QueueRuntime__c(QueueName__c = 'invoice-sync'),
                            new QueueRuntime__c(QueueName__c = queueName)
                        }
                    )
            );
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturns(
                        'findQueueDefinitionAsSystem',
                        new List<Object>{ null, QueuesTestSupport.queueDefinition(queueName, true) }
                    )
            );
            QueuesTestSupport.RecordingStubProvider queueAdminProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('recoverStalled', 4);

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                definitionStub,
                new QueuesErrorService(),
                (QueuesQueueAdminService) Test.createStub(QueuesQueueAdminService.class, queueAdminProvider),
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                emptyScheduleRepositoryStub(),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Set<String> queueNames = service.queryRuntimeMaintenanceQueueNames();
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(new Set<String>{ 'invoice-sync', queueName }, queueNames);
            Assert.areEqual(4, recovered.total);
            Assert.areEqual(1, queueAdminProvider.getCallCount('recoverStalled'));
        }
    }

    @IsTest
    static void should_stop_runtime_maintenance_when_query_headroom_is_exhausted() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn(
                        'queryMaintenanceRuntimes',
                        new List<QueueRuntime__c>{ new QueueRuntime__c(QueueName__c = 'invoice-sync') }
                    )
            );
            QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
                QueuesDefinitionService.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('findQueueDefinitionAsSystem', QueuesTestSupport.queueDefinition())
            );
            QueuesTestSupport.RecordingStubProvider queueAdminProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('recoverStalled', 1);

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                definitionStub,
                new QueuesErrorService(),
                (QueuesQueueAdminService) Test.createStub(QueuesQueueAdminService.class, queueAdminProvider),
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                emptyScheduleRepositoryStub(),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            exhaustMaintenanceQueryHeadroom();

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(0, recovered.total);
            Assert.areEqual(0, queueAdminProvider.getCallCount('recoverStalled'));
        }
    }

    @IsTest
    static void should_mark_runtime_maintenance_incomplete_when_headroom_exhausts_before_queue_recovery() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'maintenance-queue-headroom';
            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn(
                        'queryMaintenanceRuntimes',
                        new List<QueueRuntime__c>{ new QueueRuntime__c(QueueName__c = queueName) }
                    )
            );
            QueuesTestSupport.RecordingStubProvider definitionProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesMaintenanceService service = new QueuesMaintenanceService(
                emptyAsyncApexJobRepositoryStub(),
                (QueuesDefinitionService) Test.createStub(QueuesDefinitionService.class, definitionProvider),
                new QueuesErrorService(),
                new QueuesQueueAdminService(),
                runtimeRepositoryStub,
                emptyJobRepositoryStub(),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesInvocableExecutionService(),
                emptyQueueableDispatchRepositoryStub(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            leaveMaintenanceQueryHeadroom();

            // Act
            Queues.MaintenanceResult result = service.runRuntimeMaintenance();

            // Assert
            Assert.isFalse(result.done);
            Assert.areEqual(0, definitionProvider.getCallCount('findQueueDefinitionAsSystem'));
        }
    }

    @IsTest
    static void should_mark_runtime_maintenance_incomplete_when_async_worker_cleanup_exhausts_headroom() {
        Id asyncWorkerJobId = Database.executeBatch(
            new QueuesBatchableWorker('maintenance-async-headroom', 'worker-token'),
            1
        );
        AsyncApexJob asyncWorker = [SELECT ApexClass.Name, Status FROM AsyncApexJob WHERE Id = :asyncWorkerJobId];
        Assert.areEqual(QueuesBatchableWorker.class.getName(), asyncWorker.ApexClass.Name);

        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>());
            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesDefinitionService(),
                new QueuesErrorService(),
                new QueuesQueueAdminService(),
                emptyRuntimeRepositoryStub(),
                emptyJobRepositoryStub(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesInvocableExecutionService(),
                emptyQueueableDispatchRepositoryStub(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            leaveMaintenanceQueryHeadroom();

            // Act
            Queues.MaintenanceResult result = service.runRuntimeMaintenance();

            // Assert
            Assert.isFalse(result.done);
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('queryScheduledJobsByNamePrefix'));
        }
    }

    @IsTest
    static void should_mark_runtime_maintenance_incomplete_when_scheduled_worker_cleanup_exhausts_headroom() {
        System.schedule(
            'Queues:maintenance-scheduled-headroom:1713300000000:deadbeef',
            '0 0 0 1 1 ? 2099',
            new QueuesMaintenanceScheduler()
        );

        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesMaintenanceService service = new QueuesMaintenanceService(
                emptyAsyncApexJobRepositoryStub(),
                new QueuesDefinitionService(),
                new QueuesErrorService(),
                new QueuesQueueAdminService(),
                emptyRuntimeRepositoryStub(),
                emptyJobRepositoryStub(),
                new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesInvocableExecutionService(),
                emptyQueueableDispatchRepositoryStub(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            leaveMaintenanceQueryHeadroom();

            // Act
            Queues.MaintenanceResult result = service.runRuntimeMaintenance();

            // Assert
            Assert.isFalse(result.done);
        }
    }

    @IsTest
    static void should_require_cpu_headroom_for_runtime_maintenance() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Act + Assert
            Assert.areEqual(true, QueuesMaintenanceService.hasHeadroomForRecovery(1200, 10, 25, 2000));
            Assert.areEqual(false, QueuesMaintenanceService.hasHeadroomForRecovery(1200, 10, 25, 1999));
            Assert.areEqual(false, QueuesMaintenanceService.hasHeadroomForRecovery(1199, 10, 25, 2000));
            Assert.areEqual(false, QueuesMaintenanceService.hasHeadroomForRecovery(1200, 9, 25, 2000));
            Assert.areEqual(false, QueuesMaintenanceService.hasHeadroomForRecovery(1200, 10, 24, 2000));
        }
    }

    @IsTest
    static void should_abort_orphaned_scheduled_workers_before_runtime_recovery() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'invoice-sync';
            String referencedJobId = QueuesTestSupport.fakeIdValue(CronTrigger.SObjectType, 91);
            String orphanedJobId = QueuesTestSupport.fakeIdValue(CronTrigger.SObjectType, 92);
            QueueRuntime__c referencedRuntime = QueuesTestSupport.runtime(queueName, false, 0);
            referencedRuntime.AsyncApexJobId__c = referencedJobId;

            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>{ referencedRuntime })
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobsByNamePrefix',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            Id.valueOf(referencedJobId),
                            'WAITING',
                            '0 0 0 1 1 ? 2099',
                            'Queues:' + queueName + ':1713300000000:deadbeef'
                        ),
                        QueuesTestSupport.cronTrigger(
                            Id.valueOf(orphanedJobId),
                            'WAITING',
                            '0 0 0 1 1 ? 2099',
                            'Queues:' + queueName + ':1713300000001:feedface'
                        )
                    }
                );
            QueuesScheduleRepository scheduleRepositoryStub = (QueuesScheduleRepository) Test.createStub(
                QueuesScheduleRepository.class,
                scheduleRepositoryProvider
            );
            QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('publishQueueError', successfulPublishResult());
            QueuesErrorService errorService = new QueuesErrorService(
                new QueuesErrorRepository(),
                (QueuesErrorEventRepository) Test.createStub(QueuesErrorEventRepository.class, eventRepositoryProvider)
            );

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesDefinitionService(),
                errorService,
                (QueuesQueueAdminService) Test.createStub(
                    QueuesQueueAdminService.class,
                    new QueuesTestSupport.RecordingStubProvider()
                ),
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                scheduleRepositoryStub,
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(0, recovered.total);
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
            Assert.areEqual(orphanedJobId, (String) scheduleRepositoryProvider.getLastArgs('abortJob')[0]);
            Assert.areEqual(0, eventRepositoryProvider.getCallCount('publishQueueError'));
        }
    }

    @IsTest
    static void should_abort_orphaned_batch_workers_before_runtime_recovery() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String referencedAsyncJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 95);
            String orphanedAsyncJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 96);
            QueueRuntime__c referencedRuntime = QueuesTestSupport.runtime('invoice-sync', false, 0);
            referencedRuntime.AsyncApexJobId__c = referencedAsyncJobId;

            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>{ referencedRuntime })
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesAsyncApexJobRepository asyncApexJobRepositoryStub = (QueuesAsyncApexJobRepository) Test.createStub(
                QueuesAsyncApexJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'queryActiveAsyncJobsByApexClass',
                        new List<AsyncApexJob>{
                            QueuesTestSupport.asyncApexJob(
                                Id.valueOf(referencedAsyncJobId),
                                'PROCESSING',
                                'QueuesBatchableWorker',
                                System.now().addMinutes(-2)
                            ),
                            QueuesTestSupport.asyncApexJob(
                                Id.valueOf(orphanedAsyncJobId),
                                'PROCESSING',
                                'QueuesBatchableWorker',
                                System.now().addMinutes(-2)
                            )
                        }
                    )
            );
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>());

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                asyncApexJobRepositoryStub,
                new QueuesDefinitionService(),
                new QueuesErrorService(),
                new QueuesQueueAdminService(),
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(0, recovered.total);
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
            Assert.areEqual(orphanedAsyncJobId, (String) scheduleRepositoryProvider.getLastArgs('abortJob')[0]);
        }
    }

    @IsTest
    static void should_not_abort_recent_batch_workers_before_runtime_recovery() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String recentAsyncJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 97);

            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesAsyncApexJobRepository asyncApexJobRepositoryStub = (QueuesAsyncApexJobRepository) Test.createStub(
                QueuesAsyncApexJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'queryActiveAsyncJobsByApexClass',
                        new List<AsyncApexJob>{
                            QueuesTestSupport.asyncApexJob(
                                Id.valueOf(recentAsyncJobId),
                                'PROCESSING',
                                'QueuesBatchableWorker',
                                System.now()
                            )
                        }
                    )
            );
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>());

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                asyncApexJobRepositoryStub,
                new QueuesDefinitionService(),
                new QueuesErrorService(),
                new QueuesQueueAdminService(),
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(0, recovered.total);
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('abortJob'));
        }
    }

    @IsTest
    static void should_not_abort_queued_batch_workers_before_runtime_recovery() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queuedAsyncJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 98);
            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesTestSupport.RecordingStubProvider asyncApexJobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryActiveAsyncJobsByApexClass',
                    new List<AsyncApexJob>{
                        QueuesTestSupport.asyncApexJob(
                            Id.valueOf(queuedAsyncJobId),
                            'QUEUED',
                            'QueuesBatchableWorker',
                            System.now().addMinutes(-2)
                        )
                    }
                );
            QueuesAsyncApexJobRepository asyncApexJobRepositoryStub = (QueuesAsyncApexJobRepository) Test.createStub(
                QueuesAsyncApexJobRepository.class,
                asyncApexJobRepositoryProvider
            );
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>());

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                asyncApexJobRepositoryStub,
                new QueuesDefinitionService(),
                new QueuesErrorService(),
                new QueuesQueueAdminService(),
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(0, recovered.total);
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('abortJob'));
            List<Object> queryArgs = asyncApexJobRepositoryProvider.getLastArgs('queryActiveAsyncJobsByApexClass');
            Assert.areEqual(
                new Set<String>{ 'HOLDING', 'QUEUED', 'PREPARING', 'PROCESSING', 'ACQUIRED' },
                (Set<String>) queryArgs[1]
            );
        }
    }

    @IsTest
    static void should_abort_old_queued_batch_workers_before_runtime_recovery() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String orphanedAsyncJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 981);
            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesAsyncApexJobRepository asyncApexJobRepositoryStub = (QueuesAsyncApexJobRepository) Test.createStub(
                QueuesAsyncApexJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'queryActiveAsyncJobsByApexClass',
                        new List<AsyncApexJob>{
                            QueuesTestSupport.asyncApexJob(
                                Id.valueOf(orphanedAsyncJobId),
                                'QUEUED',
                                'QueuesBatchableWorker',
                                System.now().addMinutes(-16)
                            )
                        }
                    )
            );
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>());

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                asyncApexJobRepositoryStub,
                new QueuesDefinitionService(),
                new QueuesErrorService(),
                new QueuesQueueAdminService(),
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(0, recovered.total);
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
            Assert.areEqual(orphanedAsyncJobId, (String) scheduleRepositoryProvider.getLastArgs('abortJob')[0]);
        }
    }

    @IsTest
    static void should_tolerate_empty_scheduled_worker_query_results() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesScheduleRepository scheduleRepositoryStub = (QueuesScheduleRepository) Test.createStub(
                QueuesScheduleRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>())
            );

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesDefinitionService(),
                new QueuesErrorService(),
                new QueuesQueueAdminService(),
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                scheduleRepositoryStub,
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(0, recovered.total);
        }
    }

    @IsTest
    static void should_stop_orphaned_worker_cleanup_when_headroom_is_exhausted() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String orphanedJobId = QueuesTestSupport.fakeIdValue(CronTrigger.SObjectType, 93);
            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobsByNamePrefix',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            Id.valueOf(orphanedJobId),
                            'WAITING',
                            '0 0 0 1 1 ? 2099',
                            'Queues:invoice-sync:1713300000002:decafbad'
                        )
                    }
                );

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesDefinitionService(),
                new QueuesErrorService(),
                new QueuesQueueAdminService(),
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            exhaustMaintenanceQueryHeadroom();

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(0, recovered.total);
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('abortJob'));
        }
    }

    @IsTest
    static void should_stop_orphaned_async_worker_cleanup_when_headroom_is_exhausted() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String orphanedAsyncJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 99);
            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesAsyncApexJobRepository asyncApexJobRepositoryStub = (QueuesAsyncApexJobRepository) Test.createStub(
                QueuesAsyncApexJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'queryActiveAsyncJobsByApexClass',
                        new List<AsyncApexJob>{
                            QueuesTestSupport.asyncApexJob(
                                Id.valueOf(orphanedAsyncJobId),
                                'PROCESSING',
                                'QueuesBatchableWorker',
                                System.now().addMinutes(-2)
                            )
                        }
                    )
            );
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>());

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                asyncApexJobRepositoryStub,
                new QueuesDefinitionService(),
                new QueuesErrorService(),
                new QueuesQueueAdminService(),
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            exhaustMaintenanceQueryHeadroom();

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(0, recovered.total);
            Assert.areEqual(0, scheduleRepositoryProvider.getCallCount('abortJob'));
        }
    }

    @IsTest
    static void should_record_queue_error_when_orphaned_worker_abort_fails() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'invoice-sync';
            String orphanedJobId = QueuesTestSupport.fakeIdValue(CronTrigger.SObjectType, 94);
            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesScheduleRepository scheduleRepositoryStub = (QueuesScheduleRepository) Test.createStub(
                QueuesScheduleRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'queryScheduledJobsByNamePrefix',
                        new List<CronTrigger>{
                            QueuesTestSupport.cronTrigger(
                                Id.valueOf(orphanedJobId),
                                'WAITING',
                                '0 0 0 1 1 ? 2099',
                                'Queues:' + queueName + ':1713300000003:c001d00d'
                            )
                        }
                    )
                    .withException('abortJob', new Queues.QueueException('Abort failed.'))
            );
            QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('publishQueueError', successfulPublishResult());
            QueuesErrorService errorService = new QueuesErrorService(
                new QueuesErrorRepository(),
                (QueuesErrorEventRepository) Test.createStub(QueuesErrorEventRepository.class, eventRepositoryProvider)
            );

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesDefinitionService(),
                errorService,
                new QueuesQueueAdminService(),
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                scheduleRepositoryStub,
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(0, recovered.total);
            Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
            QueueErrorEvent__e abortFailureEvent = (QueueErrorEvent__e) eventRepositoryProvider.getCalls(
                'publishQueueError'
            )[0][0];
            Assert.areEqual(queueName, abortFailureEvent.QueueName__c);
            Assert.areEqual('Abort failed.', abortFailureEvent.Message__c);
        }
    }

    @IsTest
    static void should_record_durable_error_when_truncated_worker_job_name_hides_queue_name() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queue-name-with-many-characters-to-force-worker-job-name-truncation-in-maintenance';
            String orphanedJobId = QueuesTestSupport.fakeIdValue(CronTrigger.SObjectType, 941);
            String truncatedJobName = ('Queues:' + queueName + ':1713300000003:c001d00d').left(80);
            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn(
                    'queryScheduledJobsByNamePrefix',
                    new List<CronTrigger>{
                        QueuesTestSupport.cronTrigger(
                            Id.valueOf(orphanedJobId),
                            'WAITING',
                            '0 0 0 1 1 ? 2099',
                            truncatedJobName
                        )
                    }
                )
                .withException('abortJob', new Queues.QueueException('Abort failed.'));
            QueuesTestSupport.RecordingStubProvider errorRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('insertQueueError', new QueueError__c());
            QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
                new QueuesDefinitionService(),
                new QueuesErrorService(
                    (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, errorRepositoryProvider),
                    (QueuesErrorEventRepository) Test.createStub(
                        QueuesErrorEventRepository.class,
                        eventRepositoryProvider
                    )
                ),
                new QueuesQueueAdminService(),
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(0, recovered.total);
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
            Assert.areEqual(1, errorRepositoryProvider.getCallCount('insertQueueError'));
            Assert.areEqual(0, eventRepositoryProvider.getCallCount('publishQueueError'));
            QueueError__c error = (QueueError__c) errorRepositoryProvider.getLastArgs('insertQueueError')[0];
            Assert.areEqual('[orphaned scheduled worker]', error.QueueName__c);
            Assert.areEqual('runtime maintenance', error.Component__c);
            Assert.areEqual(orphanedJobId, error.AsyncApexJobId__c);
            Assert.areEqual('Abort failed.', error.Message__c);
        }
    }

    @IsTest
    static void should_record_durable_queue_error_when_orphaned_async_worker_abort_fails() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String orphanedAsyncJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 100);
            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesAsyncApexJobRepository asyncApexJobRepositoryStub = (QueuesAsyncApexJobRepository) Test.createStub(
                QueuesAsyncApexJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'queryActiveAsyncJobsByApexClass',
                        new List<AsyncApexJob>{
                            QueuesTestSupport.asyncApexJob(
                                Id.valueOf(orphanedAsyncJobId),
                                'PROCESSING',
                                'QueuesBatchableWorker',
                                System.now().addMinutes(-2)
                            )
                        }
                    )
                    .withReturn(
                        'queryAsyncJobs',
                        new List<AsyncApexJob>{
                            QueuesTestSupport.asyncApexJob(
                                Id.valueOf(orphanedAsyncJobId),
                                'PROCESSING',
                                'QueuesBatchableWorker'
                            )
                        }
                    )
            );
            QueuesTestSupport.RecordingStubProvider errorRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('insertQueueError', new QueueError__c());
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>())
                .withException('abortJob', new Queues.QueueException('Abort failed.'));

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                asyncApexJobRepositoryStub,
                new QueuesDefinitionService(),
                new QueuesErrorService(
                    (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, errorRepositoryProvider),
                    new QueuesErrorEventRepository()
                ),
                new QueuesQueueAdminService(),
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(0, recovered.total);
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
            Assert.areEqual(1, errorRepositoryProvider.getCallCount('insertQueueError'));
            QueueError__c error = (QueueError__c) errorRepositoryProvider.getLastArgs('insertQueueError')[0];
            Assert.areEqual('[orphaned async worker]', error.QueueName__c);
            Assert.areEqual('runtime maintenance', error.Component__c);
            Assert.areEqual(orphanedAsyncJobId, error.AsyncApexJobId__c);
            Assert.areEqual('Abort failed.', error.Message__c);
        }
    }

    @IsTest
    static void should_tolerate_orphaned_async_worker_abort_log_failure() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String orphanedAsyncJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 101);
            QueuesRuntimeRepository runtimeRepositoryStub = (QueuesRuntimeRepository) Test.createStub(
                QueuesRuntimeRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                    .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
            );
            QueuesAsyncApexJobRepository asyncApexJobRepositoryStub = (QueuesAsyncApexJobRepository) Test.createStub(
                QueuesAsyncApexJobRepository.class,
                new QueuesTestSupport.RecordingStubProvider()
                    .withReturn(
                        'queryActiveAsyncJobsByApexClass',
                        new List<AsyncApexJob>{
                            QueuesTestSupport.asyncApexJob(
                                Id.valueOf(orphanedAsyncJobId),
                                'PROCESSING',
                                'QueuesBatchableWorker',
                                System.now().addMinutes(-2)
                            )
                        }
                    )
                    .withReturn(
                        'queryAsyncJobs',
                        new List<AsyncApexJob>{
                            QueuesTestSupport.asyncApexJob(
                                Id.valueOf(orphanedAsyncJobId),
                                'PROCESSING',
                                'QueuesBatchableWorker'
                            )
                        }
                    )
            );
            QueuesTestSupport.RecordingStubProvider errorRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withException('insertQueueError', new Queues.QueueException('Logging failed.'));
            QueuesTestSupport.RecordingStubProvider scheduleRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>())
                .withException('abortJob', new Queues.QueueException('Abort failed.'));

            QueuesMaintenanceService service = new QueuesMaintenanceService(
                asyncApexJobRepositoryStub,
                new QueuesDefinitionService(),
                new QueuesErrorService(
                    (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, errorRepositoryProvider),
                    new QueuesErrorEventRepository()
                ),
                new QueuesQueueAdminService(),
                runtimeRepositoryStub,
                new QueuesJobRepository(),
                (QueuesScheduleRepository) Test.createStub(QueuesScheduleRepository.class, scheduleRepositoryProvider),
                new QueuesInvocableExecutionService(),
                new QueuesQueueableDispatchRepository(),
                new QueuesFlowProducerService(),
                new QueuesQueueEventService()
            );

            // Act
            Queues.MaintenanceResult recovered = service.runRuntimeMaintenance();

            // Assert
            Assert.areEqual(0, recovered.total);
            Assert.areEqual(1, scheduleRepositoryProvider.getCallCount('abortJob'));
            Assert.areEqual(1, errorRepositoryProvider.getCallCount('insertQueueError'));
        }
    }

    @IsTest
    static void should_delete_expired_terminal_jobs_and_preserve_referential_integrity() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'terminal-cleanup-delete';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            queueDefinition.TerminalCleanupPolicy__c = 'DELETE';
            queueDefinition.CompletedRetentionDays__c = 0;
            QueuesJobRepository jobRepository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
            Job__c completedJob = QueuesTestSupport.job(
                queueName,
                'completed',
                Queues.State.COMPLETED.name(),
                'completed'
            );
            Job__c failedJob = QueuesTestSupport.job(queueName, 'failed', Queues.State.FAILED.name(), 'failed');
            Job__c childJob = QueuesTestSupport.job(queueName, 'child', Queues.State.WAITING.name(), 'child');
            jobRepository.insertJobs(new List<Job__c>{ completedJob, failedJob });
            childJob.Parent__c = completedJob.Id;
            jobRepository.insertJobs(new List<Job__c>{ childJob });
            JobRun__c run = new JobRun__c(
                Job__c = completedJob.Id,
                AttemptNumber__c = 1,
                Status__c = Queues.RunStatus.COMPLETED.name()
            );
            insert run;
            QueueError__c error = new QueueError__c(
                QueueName__c = queueName,
                Component__c = 'test',
                Message__c = 'linked error',
                OccurredAt__c = System.now(),
                Job__c = completedJob.Id
            );
            insert error;
            QueuesMaintenanceService service = terminalCleanupService(queueDefinition, jobRepository);

            // Act
            Queues.MaintenanceResult result = service.runTerminalCleanupMaintenance();

            // Assert
            Assert.areEqual(1, result.total);
            Assert.areEqual(1, result.getCount(queueName));
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE Id = :completedJob.Id]);
            Assert.areEqual(0, [SELECT COUNT() FROM JobRun__c WHERE Id = :run.Id]);
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE Id = :failedJob.Id]);
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE Id = :childJob.Id]);
            Assert.areEqual(null, [SELECT Parent__c FROM Job__c WHERE Id = :childJob.Id].Parent__c);
            Assert.areEqual(null, [SELECT Job__c FROM QueueError__c WHERE Id = :error.Id].Job__c);
        }
    }

    @IsTest
    static void should_skip_terminal_cleanup_when_policy_keeps_jobs() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'terminal-cleanup-keep';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            queueDefinition.TerminalCleanupPolicy__c = 'KEEP';
            queueDefinition.CompletedRetentionDays__c = 0;
            QueuesJobRepository jobRepository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
            Job__c completedJob = QueuesTestSupport.job(
                queueName,
                'completed',
                Queues.State.COMPLETED.name(),
                'completed'
            );
            jobRepository.insertJobs(new List<Job__c>{ completedJob });
            QueuesMaintenanceService service = terminalCleanupService(queueDefinition, jobRepository);

            // Act
            Queues.MaintenanceResult result = service.runTerminalCleanupMaintenance();

            // Assert
            Assert.areEqual(0, result.total);
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE Id = :completedJob.Id]);
        }
    }

    @IsTest
    static void should_skip_terminal_cleanup_states_with_blank_retention() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'terminal-cleanup-blank';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            queueDefinition.TerminalCleanupPolicy__c = 'DELETE';
            QueuesJobRepository jobRepository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
            Job__c completedJob = QueuesTestSupport.job(
                queueName,
                'completed',
                Queues.State.COMPLETED.name(),
                'completed'
            );
            jobRepository.insertJobs(new List<Job__c>{ completedJob });
            QueuesMaintenanceService service = terminalCleanupService(queueDefinition, jobRepository);

            // Act
            Queues.MaintenanceResult result = service.runTerminalCleanupMaintenance();

            // Assert
            Assert.areEqual(0, result.total);
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE Id = :completedJob.Id]);
        }
    }

    @IsTest
    static void should_keep_recent_terminal_jobs_when_retention_is_not_elapsed() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'terminal-cleanup-retention';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            queueDefinition.TerminalCleanupPolicy__c = 'DELETE';
            queueDefinition.CompletedRetentionDays__c = 1;
            QueuesJobRepository jobRepository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
            Job__c completedJob = QueuesTestSupport.job(
                queueName,
                'completed',
                Queues.State.COMPLETED.name(),
                'completed'
            );
            jobRepository.insertJobs(new List<Job__c>{ completedJob });
            QueuesMaintenanceService service = terminalCleanupService(queueDefinition, jobRepository);

            // Act
            Queues.MaintenanceResult result = service.runTerminalCleanupMaintenance();

            // Assert
            Assert.areEqual(true, result.done);
            Assert.areEqual(0, result.total);
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE Id = :completedJob.Id]);
        }
    }

    @IsTest
    static void should_stop_terminal_cleanup_when_query_headroom_is_exhausted() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesMaintenanceService service = new QueuesMaintenanceService();
            exhaustMaintenanceQueryHeadroom();

            // Act
            Queues.MaintenanceResult result = service.runTerminalCleanupMaintenance();

            // Assert
            Assert.areEqual(false, result.done);
            Assert.areEqual(0, result.total);
        }
    }

    @IsTest
    static void should_stop_terminal_cleanup_before_queue_when_headroom_disappears() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'terminal-cleanup-headroom-before-queue';
            QueuesJobRepository jobRepository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
            Job__c completedJob = QueuesTestSupport.job(
                queueName,
                'completed',
                Queues.State.COMPLETED.name(),
                'completed'
            );
            jobRepository.insertJobs(new List<Job__c>{ completedJob });
            QueuesMaintenanceService service = new QueuesMaintenanceService();

            // Act
            QueuesMaintenanceService.recoveryHeadroomOverrides = new List<Boolean>{ true, false };
            Queues.MaintenanceResult result = null;
            try {
                result = service.runTerminalCleanupMaintenance();
            } finally {
                QueuesMaintenanceService.recoveryHeadroomOverrides = null;
            }

            // Assert
            Assert.areEqual(false, result.done);
            Assert.areEqual(0, result.total);
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE Id = :completedJob.Id]);
        }
    }

    @IsTest
    static void should_delete_failed_and_canceled_terminal_jobs() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'terminal-cleanup-terminal-states';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            queueDefinition.TerminalCleanupPolicy__c = 'DELETE';
            queueDefinition.FailedRetentionDays__c = 0;
            queueDefinition.CanceledRetentionDays__c = 0;
            QueuesJobRepository jobRepository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
            Job__c failedJob = QueuesTestSupport.job(queueName, 'failed', Queues.State.FAILED.name(), 'failed');
            Job__c canceledJob = QueuesTestSupport.job(queueName, 'canceled', Queues.State.CANCELED.name(), 'canceled');
            Job__c completedJob = QueuesTestSupport.job(
                queueName,
                'completed',
                Queues.State.COMPLETED.name(),
                'completed'
            );
            jobRepository.insertJobs(new List<Job__c>{ failedJob, canceledJob, completedJob });
            QueuesMaintenanceService service = terminalCleanupService(queueDefinition, jobRepository);

            // Act
            Queues.MaintenanceResult result = service.runTerminalCleanupMaintenance();

            // Assert
            Set<Id> deletedJobIds = new Set<Id>{ failedJob.Id, canceledJob.Id };
            Assert.areEqual(true, result.done);
            Assert.areEqual(2, result.total);
            Assert.areEqual(2, result.getCount(queueName));
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE Id IN :deletedJobIds]);
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE Id = :completedJob.Id]);
        }
    }

    @IsTest
    static void should_resume_terminal_cleanup_after_dml_row_budget_is_exhausted() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'terminal-cleanup-resume';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            queueDefinition.TerminalCleanupPolicy__c = 'DELETE';
            queueDefinition.CompletedRetentionDays__c = 0;
            QueuesJobRepository jobRepository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
            Job__c completedJob = QueuesTestSupport.job(
                queueName,
                'completed',
                Queues.State.COMPLETED.name(),
                'completed'
            );
            jobRepository.insertJobs(new List<Job__c>{ completedJob });
            QueuesMaintenanceService service = terminalCleanupService(queueDefinition, jobRepository);

            // Act
            QueuesMaintenanceService.terminalCleanupDmlRowBudgetOverride = 0;
            Queues.MaintenanceResult blockedResult = service.runTerminalCleanupMaintenance();
            QueuesMaintenanceService.terminalCleanupDmlRowBudgetOverride = null;
            Queues.MaintenanceResult resumedResult = service.runTerminalCleanupMaintenance();

            // Assert
            Assert.areEqual(false, blockedResult.done);
            Assert.areEqual(0, blockedResult.total);
            Assert.areEqual(true, resumedResult.done);
            Assert.areEqual(1, resumedResult.total);
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE Id = :completedJob.Id]);
        }
    }

    @IsTest
    static void should_continue_terminal_cleanup_across_multiple_batches_when_row_budget_is_one() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'terminal-cleanup-partial';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            queueDefinition.TerminalCleanupPolicy__c = 'DELETE';
            queueDefinition.CompletedRetentionDays__c = 0;
            QueuesJobRepository jobRepository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
            Job__c firstJob = QueuesTestSupport.job(queueName, 'completed-1', Queues.State.COMPLETED.name(), 'done-1');
            Job__c secondJob = QueuesTestSupport.job(queueName, 'completed-2', Queues.State.COMPLETED.name(), 'done-2');
            jobRepository.insertJobs(new List<Job__c>{ firstJob, secondJob });
            QueuesMaintenanceService service = terminalCleanupService(queueDefinition, jobRepository);

            // Act
            QueuesMaintenanceService.terminalCleanupDmlRowBudgetOverride = 1;
            Queues.MaintenanceResult result = service.runTerminalCleanupMaintenance();
            QueuesMaintenanceService.terminalCleanupDmlRowBudgetOverride = null;

            // Assert
            Set<Id> cleanupJobIds = new Set<Id>{ firstJob.Id, secondJob.Id };
            Assert.areEqual(true, result.done);
            Assert.areEqual(2, result.total);
            Assert.areEqual(2, result.getCount(queueName));
            Assert.areEqual(0, [SELECT COUNT() FROM Job__c WHERE Id IN :cleanupJobIds]);
        }
    }

    @IsTest
    static void should_mark_terminal_cleanup_incomplete_when_locked_jobs_are_no_longer_eligible() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String firstQueueName = 'terminal-cleanup-defensive-a';
            String secondQueueName = 'terminal-cleanup-defensive-b';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(firstQueueName, true);
            queueDefinition.TerminalCleanupPolicy__c = 'DELETE';
            queueDefinition.CompletedRetentionDays__c = 0;

            QueuesJobRepository setupRepository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
            Job__c firstAggregateJob = QueuesTestSupport.job(
                firstQueueName,
                'completed-a',
                Queues.State.COMPLETED.name(),
                'completed-a'
            );
            Job__c secondAggregateJob = QueuesTestSupport.job(
                secondQueueName,
                'completed-b',
                Queues.State.COMPLETED.name(),
                'completed-b'
            );
            Job__c waitingJob = QueuesTestSupport.job(
                secondQueueName,
                'waiting',
                Queues.State.WAITING.name(),
                'waiting'
            );
            setupRepository.insertJobs(new List<Job__c>{ firstAggregateJob, secondAggregateJob, waitingJob });

            Set<String> queueNames = new Set<String>{ firstQueueName, secondQueueName };
            List<AggregateResult> queueNameRows = [
                SELECT QueueName__c queueName
                FROM Job__c
                WHERE QueueName__c IN :queueNames
                GROUP BY QueueName__c
            ];
            Job__c noLastModifiedDateCandidate = QueuesTestSupport.job(
                firstQueueName,
                'completed-without-lmd',
                Queues.State.COMPLETED.name(),
                'completed-without-lmd'
            );
            Job__c nonTerminalCandidate = [
                SELECT Id, QueueName__c, State__c, LastModifiedDate
                FROM Job__c
                WHERE Id = :waitingJob.Id
            ];
            QueuesTestSupport.RecordingStubProvider jobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryTerminalCleanupMaintenanceQueueNames', queueNameRows)
                .withReturns(
                    'queryExpiredTerminalCleanupJobsForUpdate',
                    new List<Object>{
                        new List<Job__c>{ noLastModifiedDateCandidate },
                        new List<Job__c>{ nonTerminalCandidate }
                    }
                );
            QueuesMaintenanceService service = terminalCleanupService(
                queueDefinition,
                (QueuesJobRepository) Test.createStub(QueuesJobRepository.class, jobRepositoryProvider)
            );

            // Act
            Queues.MaintenanceResult result = service.runTerminalCleanupMaintenance();

            // Assert
            Assert.areEqual(false, result.done);
            Assert.areEqual(0, result.total);
            Assert.areEqual(0, jobRepositoryProvider.getCallCount('deleteJobs'));
        }
    }

    @IsTest
    static void should_mark_terminal_cleanup_incomplete_when_headroom_exhausts_after_delete_batch() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'terminal-cleanup-headroom-after-delete';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            queueDefinition.TerminalCleanupPolicy__c = 'DELETE';
            queueDefinition.CompletedRetentionDays__c = 0;

            QueuesJobRepository setupRepository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
            Job__c completedJob = QueuesTestSupport.job(
                queueName,
                'completed',
                Queues.State.COMPLETED.name(),
                'completed'
            );
            setupRepository.insertJobs(new List<Job__c>{ completedJob });
            List<AggregateResult> queueNameRows = [
                SELECT QueueName__c queueName
                FROM Job__c
                WHERE QueueName__c = :queueName
                GROUP BY QueueName__c
            ];
            Job__c lockedCompletedJob = [
                SELECT Id, QueueName__c, State__c, LastModifiedDate
                FROM Job__c
                WHERE Id = :completedJob.Id
            ];
            QueuesTestSupport.RecordingStubProvider jobRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryTerminalCleanupMaintenanceQueueNames', queueNameRows)
                .withReturn('queryExpiredTerminalCleanupJobsForUpdate', new List<Job__c>{ lockedCompletedJob })
                .withReturn('deleteJobs', new List<Job__c>{ lockedCompletedJob });
            QueuesMaintenanceService service = terminalCleanupService(
                queueDefinition,
                (QueuesJobRepository) Test.createStub(QueuesJobRepository.class, jobRepositoryProvider)
            );

            // Act
            QueuesMaintenanceService.recoveryHeadroomOverrides = new List<Boolean>{ true, true, true, false };
            Queues.MaintenanceResult result = null;
            try {
                result = service.runTerminalCleanupMaintenance();
            } finally {
                QueuesMaintenanceService.recoveryHeadroomOverrides = null;
            }

            // Assert
            Assert.areEqual(false, result.done);
            Assert.areEqual(1, result.total);
            Assert.areEqual(1, result.getCount(queueName));
            Assert.areEqual(1, jobRepositoryProvider.getCallCount('deleteJobs'));
        }
    }

    @IsTest
    static void should_record_queue_error_when_terminal_cleanup_configuration_is_invalid() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'terminal-cleanup-invalid';
            QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition(queueName, true);
            queueDefinition.TerminalCleanupPolicy__c = 'DELETE';
            queueDefinition.CompletedRetentionDays__c = -1;
            QueuesJobRepository jobRepository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
            Job__c completedJob = QueuesTestSupport.job(
                queueName,
                'completed',
                Queues.State.COMPLETED.name(),
                'completed'
            );
            jobRepository.insertJobs(new List<Job__c>{ completedJob });
            QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
            QueuesErrorEventRepository eventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
                QueuesErrorEventRepository.class,
                eventRepositoryProvider
            );
            QueuesMaintenanceService service = terminalCleanupService(
                queueDefinition,
                jobRepository,
                new QueuesErrorService(new QueuesErrorRepository(), eventRepositoryStub)
            );

            // Act
            Queues.MaintenanceResult result = service.runTerminalCleanupMaintenance();

            // Assert
            Assert.areEqual(false, result.done);
            Assert.areEqual(0, result.total);
            Assert.areEqual(1, [SELECT COUNT() FROM Job__c WHERE Id = :completedJob.Id]);
            Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
            QueueErrorEvent__e queueError = (QueueErrorEvent__e) eventRepositoryProvider.getLastArgs(
                'publishQueueError'
            )[0];
            Assert.areEqual(queueName, queueError.QueueName__c);
            Assert.areEqual('terminal cleanup', queueError.Component__c);
            Assert.isTrue(queueError.Message__c.contains('CompletedRetentionDays__c must be zero or greater.'));
        }
    }

    @IsTest
    static void should_parse_worker_queue_name_from_scheduled_job_name() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Act + Assert
            Assert.areEqual(
                'invoice-sync',
                QueuesMaintenanceService.resolveWorkerQueueName('Queues:invoice-sync:1713300000000:deadbeef')
            );
            Assert.areEqual(
                'invoice:sync',
                QueuesMaintenanceService.resolveWorkerQueueName('Queues:invoice:sync:1713300000000:deadbeef:abcdef')
            );
            Assert.areEqual(null, QueuesMaintenanceService.resolveWorkerQueueName(''));
        }
    }

    private static QueuesMaintenanceService terminalCleanupService(
        final QueueDefinition__mdt queueDefinition,
        final QueuesJobRepository jobRepository
    ) {
        return terminalCleanupService(queueDefinition, jobRepository, new QueuesErrorService());
    }

    private static QueuesMaintenanceService terminalCleanupService(
        final QueueDefinition__mdt queueDefinition,
        final QueuesJobRepository jobRepository,
        final QueuesErrorService errorService
    ) {
        QueuesDefinitionService definitionStub = (QueuesDefinitionService) Test.createStub(
            QueuesDefinitionService.class,
            new QueuesTestSupport.RecordingStubProvider().withReturn('findQueueDefinitionAsSystem', queueDefinition)
        );
        return new QueuesMaintenanceService(
            new QueuesAsyncApexJobRepository(System.AccessLevel.SYSTEM_MODE),
            definitionStub,
            errorService,
            new QueuesQueueAdminService(),
            emptyRuntimeRepositoryStub(),
            jobRepository,
            emptyScheduleRepositoryStub(),
            new QueuesInvocableExecutionService(),
            new QueuesQueueableDispatchRepository(),
            new QueuesFlowProducerService(),
            new QueuesQueueEventService()
        );
    }

    private static Database.SaveResult successfulPublishResult() {
        return Database.insert(
            new QueueError__c(
                QueueName__c = 'maintenance-test',
                Component__c = 'runtime maintenance',
                OccurredAt__c = System.now()
            ),
            false
        );
    }

    private static QueuesErrorService newRecordingErrorService(
        final QueuesTestSupport.RecordingStubProvider eventRepositoryProvider
    ) {
        QueuesErrorService.resetEmittedErrorFingerprints();
        return new QueuesErrorService(
            new QueuesErrorRepository(),
            (QueuesErrorEventRepository) Test.createStub(QueuesErrorEventRepository.class, eventRepositoryProvider)
        );
    }

    private static Id consumeOneQuery() {
        return [SELECT Id FROM User WHERE Id = :UserInfo.getUserId()].Id;
    }

    private static void exhaustMaintenanceQueryHeadroom() {
        while (Limits.getLimitQueries() - Limits.getQueries() >= 25) {
            Assert.areNotEqual(null, consumeOneQuery());
        }
    }

    private static void leaveMaintenanceQueryHeadroom() {
        while (Limits.getLimitQueries() - Limits.getQueries() > 25) {
            Assert.areNotEqual(null, consumeOneQuery());
        }
    }

    private static QueuesAsyncApexJobRepository emptyAsyncApexJobRepositoryStub() {
        return (QueuesAsyncApexJobRepository) Test.createStub(
            QueuesAsyncApexJobRepository.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryActiveAsyncJobsByApexClass', new List<AsyncApexJob>())
        );
    }

    private static QueuesJobRepository emptyJobRepositoryStub() {
        return (QueuesJobRepository) Test.createStub(
            QueuesJobRepository.class,
            new QueuesTestSupport.RecordingStubProvider()
        );
    }

    private static QueuesQueueableDispatchRepository emptyQueueableDispatchRepositoryStub() {
        return (QueuesQueueableDispatchRepository) Test.createStub(
            QueuesQueueableDispatchRepository.class,
            new QueuesTestSupport.RecordingStubProvider()
        );
    }

    private static QueuesScheduleRepository emptyScheduleRepositoryStub() {
        return (QueuesScheduleRepository) Test.createStub(
            QueuesScheduleRepository.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryScheduledJobsByNamePrefix', new List<CronTrigger>())
        );
    }

    private static QueuesRuntimeRepository emptyRuntimeRepositoryStub() {
        return (QueuesRuntimeRepository) Test.createStub(
            QueuesRuntimeRepository.class,
            new QueuesTestSupport.RecordingStubProvider()
                .withReturn('queryRuntimesWithWorkerJobs', new List<QueueRuntime__c>())
                .withReturn('queryMaintenanceRuntimes', new List<QueueRuntime__c>())
        );
    }
    // </editor-fold>
}
