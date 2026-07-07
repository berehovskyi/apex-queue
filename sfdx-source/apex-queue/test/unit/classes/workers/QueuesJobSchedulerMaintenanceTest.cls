@IsTest(IsParallel=true)
private class QueuesJobSchedulerMaintenanceTest {
    @IsTest
    static void should_delegate_job_scheduler_maintenance_when_queueable_executes() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider serviceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('runJobSchedulerMaintenance', new Queues.MaintenanceResult().add('scheduler-queue', 2));
        QueuesService serviceStub = (QueuesService) Test.createStub(QueuesService.class, serviceProvider);
        QueuesMaintenanceWorker maintenance = new QueuesMaintenanceWorker(
            serviceStub,
            new QueuesErrorService(),
            new QueuesJobSchedulerMaintenance(),
            false
        );

        // Act
        maintenance.execute((QueueableContext) null);

        // Assert
        Assert.areEqual(1, serviceProvider.getCallCount('runJobSchedulerMaintenance'));
        Assert.areEqual(1, Limits.getQueueableJobs());
    }

    @IsTest
    static void should_skip_terminal_cleanup_enqueue_when_queueable_headroom_is_exhausted() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider serviceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('runJobSchedulerMaintenance', new Queues.MaintenanceResult().add('scheduler-queue', 2));
        QueuesService serviceStub = (QueuesService) Test.createStub(QueuesService.class, serviceProvider);
        QueuesMaintenanceWorker maintenance = new QueuesMaintenanceWorker(
            serviceStub,
            new QueuesErrorService(),
            new QueuesJobSchedulerMaintenance(),
            false
        );
        QueuesJobSchedulerMaintenance.queueableSubmissionHeadroomOverride = false;

        // Act
        maintenance.execute((QueueableContext) null);

        // Assert
        Assert.areEqual(1, serviceProvider.getCallCount('runJobSchedulerMaintenance'));
        Assert.areEqual(0, Limits.getQueueableJobs());
    }

    @IsTest
    static void should_skip_terminal_cleanup_enqueue_when_queueable_stack_depth_is_exhausted() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider serviceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('runJobSchedulerMaintenance', new Queues.MaintenanceResult().add('scheduler-queue', 2));
        QueuesService serviceStub = (QueuesService) Test.createStub(QueuesService.class, serviceProvider);
        QueuesMaintenanceWorker maintenance = new QueuesMaintenanceWorker(
            serviceStub,
            new QueuesErrorService(),
            new QueuesJobSchedulerMaintenance(),
            false
        );
        QueuesQueueableStackSupport.forceQueueableStackContext = true;
        QueuesQueueableStackSupport.forceCurrentQueueableStackDepth = 4;
        QueuesQueueableStackSupport.forceMaximumQueueableStackDepth = 5;

        // Act
        maintenance.execute((QueueableContext) null);

        // Assert
        Assert.areEqual(1, serviceProvider.getCallCount('runJobSchedulerMaintenance'));
        Assert.areEqual(0, Limits.getQueueableJobs());
    }

    @IsTest
    static void should_attach_finalizer_when_public_worker_runs_as_queueable() {
        // Act
        Test.startTest();
        Id asyncApexJobId = System.enqueueJob(new QueuesMaintenanceWorker(new QueuesTerminalCleanupMaintenance()));
        Test.stopTest();

        // Assert
        Assert.areNotEqual(null, asyncApexJobId);
    }

    @IsTest
    static void should_record_queue_error_when_finalizer_observes_failure() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 1);
        Exception failure = new Queues.QueueException('Maintenance exploded.');

        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesErrorEventRepository eventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
            QueuesErrorEventRepository.class,
            eventRepositoryProvider
        );
        QueuesErrorService errorService = new QueuesErrorService(new QueuesErrorRepository(), eventRepositoryStub);
        QueuesService serviceStub = (QueuesService) Test.createStub(
            QueuesService.class,
            new QueuesTestSupport.RecordingStubProvider()
        );
        QueuesMaintenanceWorker maintenance = new QueuesMaintenanceWorker(
            serviceStub,
            errorService,
            new QueuesJobSchedulerMaintenance(),
            false
        );

        // Act
        maintenance.recordFinalizerFailure(asyncApexJobId, failure);

        // Assert
        Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
        QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) eventRepositoryProvider.getLastArgs(
            'publishQueueError'
        )[0];
        Assert.areEqual('__maintenance__', errorEvent.QueueName__c);
        Assert.areEqual('job scheduler maintenance', errorEvent.Component__c);
        Assert.areEqual(null, errorEvent.JobId__c);
        Assert.areEqual(asyncApexJobId, errorEvent.AsyncApexJobId__c);
        Assert.areEqual('Maintenance exploded.', errorEvent.Message__c);
    }

    @IsTest
    static void should_ignore_successful_finalizer_result() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesErrorEventRepository eventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
            QueuesErrorEventRepository.class,
            eventRepositoryProvider
        );
        QueuesMaintenanceWorker maintenance = new QueuesMaintenanceWorker(
            (QueuesService) Test.createStub(QueuesService.class, new QueuesTestSupport.RecordingStubProvider()),
            new QueuesErrorService(new QueuesErrorRepository(), eventRepositoryStub),
            new QueuesJobSchedulerMaintenance(),
            false
        );

        // Act
        maintenance.execute(new FakeFinalizerContext(ParentJobResult.SUCCESS, null));

        // Assert
        Assert.areEqual(0, eventRepositoryProvider.getCallCount('publishQueueError'));
    }

    @IsTest
    static void should_record_default_component_when_finalizer_task_component_is_blank() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesErrorEventRepository eventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
            QueuesErrorEventRepository.class,
            eventRepositoryProvider
        );
        QueuesMaintenanceWorker maintenance = new QueuesMaintenanceWorker(
            (QueuesService) Test.createStub(QueuesService.class, new QueuesTestSupport.RecordingStubProvider()),
            new QueuesErrorService(new QueuesErrorRepository(), eventRepositoryStub),
            new BlankMaintenanceTask(),
            false
        );

        // Act
        maintenance.execute(
            new FakeFinalizerContext(
                ParentJobResult.UNHANDLED_EXCEPTION,
                new Queues.QueueException('Blank task failed.')
            )
        );

        // Assert
        Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
        QueueErrorEvent__e errorEvent = (QueueErrorEvent__e) eventRepositoryProvider.getLastArgs(
            'publishQueueError'
        )[0];
        Assert.areEqual('maintenance', errorEvent.Component__c);
        Assert.areEqual('Blank task failed.', errorEvent.Message__c);
    }

    @IsTest
    static void should_swallow_finalizer_telemetry_failure() {
        // Arrange
        QueuesErrorService.resetEmittedErrorFingerprints();
        QueuesTestSupport.RecordingStubProvider eventRepositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withException('publishQueueError', new Queues.QueueException('Event publish failed.'));
        QueuesErrorEventRepository eventRepositoryStub = (QueuesErrorEventRepository) Test.createStub(
            QueuesErrorEventRepository.class,
            eventRepositoryProvider
        );
        QueuesMaintenanceWorker maintenance = new QueuesMaintenanceWorker(
            (QueuesService) Test.createStub(QueuesService.class, new QueuesTestSupport.RecordingStubProvider()),
            new QueuesErrorService(new QueuesErrorRepository(), eventRepositoryStub),
            new QueuesJobSchedulerMaintenance(),
            false
        );

        // Act
        maintenance.recordFinalizerFailure(
            QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 6),
            new Queues.QueueException('Maintenance exploded.')
        );

        // Assert
        Assert.areEqual(1, eventRepositoryProvider.getCallCount('publishQueueError'));
    }

    private class FakeFinalizerContext implements FinalizerContext {
        private final ParentJobResult result;
        private final Exception ex;

        private FakeFinalizerContext(final ParentJobResult result, final Exception ex) {
            this.result = result;
            this.ex = ex;
        }

        public Id getAsyncApexJobId() {
            return QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 5);
        }

        public Exception getException() {
            return ex;
        }

        public String getRequestId() {
            return 'request-id';
        }

        public ParentJobResult getResult() {
            return result;
        }
    }

    private class BlankMaintenanceTask implements QueuesMaintenanceWorker.Task {
        public void run(final QueuesService queueService) {
        }

        public String component() {
            return '';
        }
    }
}
