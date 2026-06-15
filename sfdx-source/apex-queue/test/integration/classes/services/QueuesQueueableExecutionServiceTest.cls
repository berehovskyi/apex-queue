@IsTest(IsParallel=false)
private class QueuesQueueableExecutionServiceTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_execute_job_without_reenqueue_when_job_is_terminal() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 901);
        Job__c completedJob = queueableJob(Queues.State.COMPLETED, 901);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('executeQueueableJob', completedJob);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider();

        TestableQueueableExecutionService service = new TestableQueueableExecutionService(
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider)
        );

        // Act
        Job__c result = service.execute(completedJob.Id, asyncApexJobId);

        // Assert
        Assert.areEqual(completedJob.Id, result.Id);
        Assert.areEqual(1, executionProvider.getCallCount('executeQueueableJob'));
        Assert.areEqual(completedJob.Id, executionProvider.getLastArgs('executeQueueableJob')[0]);
        Assert.areEqual(asyncApexJobId, executionProvider.getLastArgs('executeQueueableJob')[1]);
        Assert.areEqual(0, service.enqueueCount);
        Assert.areEqual(0, jobServiceProvider.getCallCount('requireJobForUpdate'));
    }

    @IsTest
    static void should_reenqueue_waiting_queueable_job_after_handled_retry() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 902);
        Job__c waitingJob = queueableJob(Queues.State.WAITING, 902);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('executeQueueableJob', waitingJob);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobForUpdate', waitingJob)
            .withReturn('updateJob', waitingJob);

        TestableQueueableExecutionService service = new TestableQueueableExecutionService(
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider)
        );

        // Act
        Job__c result = service.execute(waitingJob.Id, asyncApexJobId);

        // Assert
        Assert.areEqual(Queues.State.ACTIVE.name(), result.State__c);
        Assert.areEqual(service.enqueuedAsyncApexJobId, result.AsyncApexJobId__c);
        Assert.areNotEqual(null, result.QueueableDispatchInitiatedAt__c);
        Assert.areEqual(1, service.enqueueCount);
        Assert.areEqual(waitingJob.Id, service.lastEnqueuedJobId);
        Assert.areEqual(0, service.lastDelayMinutes);
        Assert.areEqual(1, jobServiceProvider.getCallCount('requireJobForUpdate'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_skip_reenqueue_when_locked_job_is_no_longer_runnable() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 906);
        Job__c waitingJob = queueableJob(Queues.State.WAITING, 906);
        Job__c completedJob = queueableJob(Queues.State.COMPLETED, 906);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('executeQueueableJob', waitingJob);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobForUpdate', completedJob);

        TestableQueueableExecutionService service = new TestableQueueableExecutionService(
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider)
        );

        // Act
        Job__c result = service.execute(waitingJob.Id, asyncApexJobId);

        // Assert
        Assert.areEqual(Queues.State.COMPLETED.name(), result.State__c);
        Assert.areEqual(0, service.enqueueCount);
        Assert.areEqual(0, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_reenqueue_delayed_queueable_job_with_native_delay() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 903);
        Job__c delayedJob = queueableJob(Queues.State.DELAYED, 903);
        delayedJob.AvailableAt__c = System.now().addMinutes(3);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('executeQueueableJob', delayedJob);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobForUpdate', delayedJob)
            .withReturn('updateJob', delayedJob);

        TestableQueueableExecutionService service = new TestableQueueableExecutionService(
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider)
        );

        // Act
        service.execute(delayedJob.Id, asyncApexJobId);

        // Assert
        Assert.areEqual(1, service.enqueueCount);
        Assert.areEqual(3, service.lastDelayMinutes);
        Assert.areEqual(Queues.State.ACTIVE.name(), delayedJob.State__c);
    }

    @IsTest
    static void should_bridge_queueable_retry_when_stack_is_exhausted() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 907);
        Job__c waitingJob = queueableJob(Queues.State.WAITING, 907);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('executeQueueableJob', waitingJob);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobForUpdate', waitingJob)
            .withReturn('updateJob', waitingJob);
        TestableQueueableExecutionService service = new TestableQueueableExecutionService(
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider)
        );

        service.bridgeQueueableStack = true;

        // Act
        Job__c result = service.execute(waitingJob.Id, asyncApexJobId);

        // Assert
        Assert.areEqual(Queues.State.WAITING.name(), result.State__c);
        Assert.areNotEqual(null, result.QueueableDispatchInitiatedAt__c);
        Assert.areEqual(0, service.enqueueCount);
        Assert.areEqual(1, service.futureBridgeCount);
        Assert.areEqual(waitingJob.Id, service.lastFutureBridgeJobId);
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_continue_queueable_retry_from_future_bridge() {
        // Arrange
        Job__c waitingJob = queueableJob(Queues.State.WAITING, 908);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobForUpdate', waitingJob)
            .withReturn('updateJob', waitingJob);
        TestableQueueableExecutionService service = new TestableQueueableExecutionService(
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider)
        );

        // Act
        Job__c result = service.continueRetryForTest(waitingJob.Id);

        // Assert
        Assert.areEqual(Queues.State.ACTIVE.name(), result.State__c);
        Assert.areEqual(service.enqueuedAsyncApexJobId, result.AsyncApexJobId__c);
        Assert.areEqual(1, service.enqueueCount);
        Assert.areEqual(waitingJob.Id, service.lastEnqueuedJobId);
        Assert.areEqual(0, service.lastDelayMinutes);
        Assert.areEqual(1, jobServiceProvider.getCallCount('requireJobForUpdate'));
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_refresh_queueable_dispatch_initiated_at_when_enqueuing_stale_job() {
        // Arrange
        Datetime staleInitiatedAt = System.now().addMinutes(-10);
        Job__c waitingJob = queueableJob(Queues.State.WAITING, 913);
        waitingJob.QueueableDispatchInitiatedAt__c = staleInitiatedAt;
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobForUpdate', waitingJob)
            .withReturn('updateJob', waitingJob);
        TestableQueueableExecutionService service = new TestableQueueableExecutionService(
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider)
        );

        // Act
        Job__c result = service.continueRetryForTest(waitingJob.Id);

        // Assert
        Assert.areEqual(Queues.State.ACTIVE.name(), result.State__c);
        Assert.areEqual(service.enqueuedAsyncApexJobId, result.AsyncApexJobId__c);
        Assert.isTrue(result.QueueableDispatchInitiatedAt__c > staleInitiatedAt);
        Assert.areEqual(1, service.enqueueCount);
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_ignore_future_retry_bridge_when_locked_job_is_not_runnable() {
        // Arrange
        Job__c completedJob = queueableJob(Queues.State.COMPLETED, 909);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobForUpdate', completedJob);
        TestableQueueableExecutionService service = new TestableQueueableExecutionService(
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider)
        );

        // Act
        Job__c result = service.continueRetryForTest(completedJob.Id);

        // Assert
        Assert.areEqual(Queues.State.COMPLETED.name(), result.State__c);
        Assert.areEqual(0, service.enqueueCount);
        Assert.areEqual(0, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_fail_queueable_job_when_retry_enqueue_fails() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 910);
        Job__c waitingJob = queueableJob(Queues.State.WAITING, 910);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('executeQueueableJob', waitingJob);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobForUpdate', waitingJob)
            .withReturn('updateJob', waitingJob);
        TestableQueueableExecutionService service = new TestableQueueableExecutionService(
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider)
        );
        service.enqueueException = new Queues.QueueException('async limit');

        // Act
        Job__c result = service.execute(waitingJob.Id, asyncApexJobId);

        // Assert
        Assert.areEqual(Queues.State.FAILED.name(), result.State__c);
        Assert.areEqual(null, result.AsyncApexJobId__c);
        Assert.areEqual('Queueable retry enqueue failed: async limit', result.FailedReason__c);
        Assert.areEqual(1, service.enqueueCount);
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_bridge_queueable_retry_when_native_enqueue_reports_async_exception() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 912);
        Job__c waitingJob = queueableJob(Queues.State.WAITING, 912);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('executeQueueableJob', waitingJob);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobForUpdate', waitingJob)
            .withReturn('updateJob', waitingJob);
        TestableQueueableExecutionService service = new TestableQueueableExecutionService(
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider)
        );
        service.enqueueException = new System.AsyncException('native queueable enqueue rejected');

        // Act
        Job__c result = service.execute(waitingJob.Id, asyncApexJobId);

        // Assert
        Assert.areEqual(Queues.State.WAITING.name(), result.State__c);
        Assert.areEqual(null, result.AsyncApexJobId__c);
        Assert.areNotEqual(null, result.QueueableDispatchInitiatedAt__c);
        Assert.areEqual(1, service.enqueueCount);
        Assert.areEqual(1, service.futureBridgeCount);
        Assert.areEqual(waitingJob.Id, service.lastFutureBridgeJobId);
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_fail_queueable_job_when_retry_future_bridge_fails() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 911);
        Job__c waitingJob = queueableJob(Queues.State.WAITING, 911);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('executeQueueableJob', waitingJob);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobForUpdate', waitingJob)
            .withReturn('updateJob', waitingJob);
        TestableQueueableExecutionService service = new TestableQueueableExecutionService(
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider)
        );
        service.futureBridgeException = new Queues.QueueException('future limit');

        service.bridgeQueueableStack = true;

        // Act
        Job__c result = service.execute(waitingJob.Id, asyncApexJobId);

        // Assert
        Assert.areEqual(Queues.State.FAILED.name(), result.State__c);
        Assert.areEqual(null, result.AsyncApexJobId__c);
        Assert.areEqual('Queueable retry enqueue failed: future limit', result.FailedReason__c);
        Assert.areEqual(0, service.enqueueCount);
        Assert.areEqual(1, service.futureBridgeCount);
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_ignore_terminal_job_through_default_future_bridge() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            Job__c completedJob = insertQueueableJob(Queues.State.COMPLETED, System.now());

            // Act
            Test.startTest();
            new PlatformQueueableExecutionService().enqueueRetryFromFutureBridgeForTest(completedJob.Id);
            Test.stopTest();

            // Assert
            Job__c updatedJob = [
                SELECT AsyncApexJobId__c, FailedReason__c, State__c
                FROM Job__c
                WHERE Id = :completedJob.Id
            ];
            Assert.areEqual(Queues.State.COMPLETED.name(), updatedJob.State__c);
            Assert.areEqual(null, updatedJob.AsyncApexJobId__c);
            Assert.areEqual(null, updatedJob.FailedReason__c);
        }
    }

    @IsTest
    static void should_ignore_empty_retry_future_bridge_input() {
        // Act
        Test.startTest();
        QueuesQueueableExecutionService.continueRetryFromFuture(null);
        Test.stopTest();

        // Assert
        Assert.isTrue(true);
    }

    @IsTest
    static void should_resolve_queueable_stack_depth_from_queueable_context() {
        // Arrange
        QueuesQueueableStackSupport.forceQueueableStackContext = true;
        QueuesQueueableStackSupport.forceCurrentQueueableStackDepth = 4;
        QueuesQueueableStackSupport.forceMaximumQueueableStackDepth = 5;
        StackDepthProbeService service = new StackDepthProbeService();

        // Act
        Boolean shouldBridge = false;
        try {
            shouldBridge = service.shouldBridgeForTest();
        } finally {
            QueuesQueueableStackSupport.forceQueueableStackContext = false;
            QueuesQueueableStackSupport.forceCurrentQueueableStackDepth = null;
            QueuesQueueableStackSupport.forceMaximumQueueableStackDepth = null;
        }

        // Assert
        Assert.isTrue(shouldBridge);
    }

    @IsTest
    static void should_resolve_native_stack_depth_inside_queueable_context() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'queueable-native-stack-probe';
            AsyncOptions options = new AsyncOptions();
            options.maximumQueueableStackDepth = 5;

            // Act
            Test.startTest();
            System.enqueueJob(new NativeStackDepthProbe(queueName), options);
            Test.stopTest();

            // Assert
            QueueError__c probe = [
                SELECT Component__c, Message__c
                FROM QueueError__c
                WHERE QueueName__c = :queueName
                LIMIT 1
            ];
            Assert.areEqual('native-stack-depth', probe.Component__c);
            Assert.isTrue(probe.Message__c.startsWith('5:'));
        }
    }

    @IsTest
    static void should_fail_queueable_job_when_retry_delay_exceeds_native_window() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 904);
        Job__c delayedJob = queueableJob(Queues.State.DELAYED, 904);
        delayedJob.AvailableAt__c = System.now()
            .addMinutes(QueuesQueueableTransportService.NATIVE_MAX_QUEUEABLE_DELAY_MINUTES + 1);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('executeQueueableJob', delayedJob);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobForUpdate', delayedJob)
            .withReturn('updateJob', delayedJob);

        TestableQueueableExecutionService service = new TestableQueueableExecutionService(
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider)
        );

        // Act
        Job__c result = service.execute(delayedJob.Id, asyncApexJobId);

        // Assert
        Assert.areEqual(Queues.State.FAILED.name(), result.State__c);
        Assert.areEqual(QueuesQueueableTransportService.nativeQueueableDelayMessage(11), result.FailedReason__c);
        Assert.areEqual(0, service.enqueueCount);
        Assert.areEqual(1, jobServiceProvider.getCallCount('updateJob'));
    }

    @IsTest
    static void should_apply_finalizer_failure_and_reenqueue_retryable_job() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 905);
        Job__c waitingJob = queueableJob(Queues.State.WAITING, 905);
        QueuesTestSupport.RecordingStubProvider executionProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('failQueueableJob', waitingJob);
        QueuesTestSupport.RecordingStubProvider jobServiceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('requireJobForUpdate', waitingJob)
            .withReturn('updateJob', waitingJob);

        TestableQueueableExecutionService service = new TestableQueueableExecutionService(
            (QueuesJobExecutionService) Test.createStub(QueuesJobExecutionService.class, executionProvider),
            (QueuesJobService) Test.createStub(QueuesJobService.class, jobServiceProvider)
        );

        // Act
        service.handleFailure(waitingJob.Id, asyncApexJobId, 'catastrophic');

        // Assert
        Assert.areEqual(1, executionProvider.getCallCount('failQueueableJob'));
        Assert.areEqual(waitingJob.Id, executionProvider.getLastArgs('failQueueableJob')[0]);
        Assert.areEqual(asyncApexJobId, executionProvider.getLastArgs('failQueueableJob')[1]);
        Assert.areEqual('catastrophic', executionProvider.getLastArgs('failQueueableJob')[2]);
        Assert.areEqual(1, service.enqueueCount);
        Assert.areEqual(Queues.State.ACTIVE.name(), waitingJob.State__c);
    }

    private static Job__c queueableJob(final Queues.State state, final Integer seed) {
        Job__c job = QueuesTestSupport.job(state.name());
        job.Id = QueuesTestSupport.fakeId(Job__c.SObjectType, seed);
        job.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
        job.AvailableAt__c = System.now();
        return job;
    }

    private static Job__c insertQueueableJob(final Queues.State state, final Datetime availableAt) {
        Job__c job = QueuesTestSupport.job(state.name());
        job.ExecutionMode__c = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
        job.AvailableAt__c = availableAt;
        insert job;
        return job;
    }

    private class PlatformQueueableExecutionService extends QueuesQueueableExecutionService {
        private void enqueueRetryFromFutureBridgeForTest(final Id jobId) {
            enqueueRetryFromFutureBridge(jobId);
        }
    }

    private class StackDepthProbeService extends QueuesQueueableExecutionService {
        private Boolean shouldBridgeForTest() {
            return shouldBridgeQueueableStack();
        }
    }

    @SuppressWarnings('PMD.QueueableWithoutFinalizer')
    private class NativeStackDepthProbe implements Queueable {
        private final String queueName;

        private NativeStackDepthProbe(final String queueName) {
            this.queueName = queueName;
        }

        public void execute(final QueueableContext ctx) {
            QueuesQueueableStackSupport.forceQueueableStackContext = true;
            try {
                Boolean shouldBridge = new StackDepthProbeService().shouldBridgeForTest();
                insert new QueueError__c(
                    QueueName__c = queueName,
                    Component__c = 'native-stack-depth',
                    Message__c = System.AsyncInfo.getMaximumQueueableStackDepth() + ':' + shouldBridge,
                    OccurredAt__c = System.now()
                );
            } finally {
                QueuesQueueableStackSupport.forceQueueableStackContext = false;
            }
        }
    }

    private class TestableQueueableExecutionService extends QueuesQueueableExecutionService {
        private Integer enqueueCount = 0;
        private Integer futureBridgeCount = 0;
        private Id lastEnqueuedJobId;
        private Id lastFutureBridgeJobId;
        private Integer lastDelayMinutes;
        private Boolean bridgeQueueableStack = false;
        private Exception enqueueException;
        private Exception futureBridgeException;
        private final Id enqueuedAsyncApexJobId = QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 999);

        private TestableQueueableExecutionService(
            final QueuesJobExecutionService jobExecutionService,
            final QueuesJobService jobService
        ) {
            super(jobExecutionService, jobService);
        }

        protected override Id enqueueExecutor(final Id jobId, final Integer delayMinutes) {
            enqueueCount++;
            lastEnqueuedJobId = jobId;
            lastDelayMinutes = delayMinutes;
            if (enqueueException != null) {
                throw enqueueException;
            }
            return enqueuedAsyncApexJobId;
        }

        protected override Boolean shouldBridgeQueueableStack() {
            return bridgeQueueableStack;
        }

        protected override void enqueueRetryFromFutureBridge(final Id jobId) {
            futureBridgeCount++;
            lastFutureBridgeJobId = jobId;
            if (futureBridgeException != null) {
                throw futureBridgeException;
            }
        }

        private Job__c continueRetryForTest(final Id jobId) {
            return continueRetry(jobId);
        }
    }
}
