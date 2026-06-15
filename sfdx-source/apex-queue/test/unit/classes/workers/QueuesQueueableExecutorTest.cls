@IsTest(IsParallel=true)
private class QueuesQueueableExecutorTest {
    @IsTest
    static void should_delegate_queueable_execution_to_execution_service() {
        // Arrange
        Id jobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 811);
        QueuesTestSupport.RecordingStubProvider serviceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueableExecutor executor = new QueuesQueueableExecutor(
            jobId,
            (QueuesQueueableExecutionService) Test.createStub(QueuesQueueableExecutionService.class, serviceProvider),
            false
        );
        QueueableContext ctx = new FakeQueueableContext(812);

        // Act
        executor.execute(ctx);

        // Assert
        Assert.areEqual(1, serviceProvider.getCallCount('execute'));
        Assert.areEqual(jobId, serviceProvider.getLastArgs('execute')[0]);
        Assert.areEqual(ctx.getJobId(), serviceProvider.getLastArgs('execute')[1]);
    }

    @IsTest
    static void should_ignore_successful_finalizer_result() {
        // Arrange
        Id jobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 813);
        QueuesTestSupport.RecordingStubProvider serviceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueableExecutor executor = new QueuesQueueableExecutor(
            jobId,
            (QueuesQueueableExecutionService) Test.createStub(QueuesQueueableExecutionService.class, serviceProvider),
            false
        );

        // Act
        executor.execute(new FakeFinalizerContext(ParentJobResult.SUCCESS, null, 814));

        // Assert
        Assert.areEqual(0, serviceProvider.getCallCount('handleFailure'));
    }

    @IsTest
    static void should_delegate_finalizer_failure_to_execution_service() {
        // Arrange
        Id jobId = QueuesTestSupport.fakeId(Job__c.SObjectType, 815);
        QueuesTestSupport.RecordingStubProvider serviceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueableExecutor executor = new QueuesQueueableExecutor(
            jobId,
            (QueuesQueueableExecutionService) Test.createStub(QueuesQueueableExecutionService.class, serviceProvider),
            false
        );
        FinalizerContext ctx = new FakeFinalizerContext(
            ParentJobResult.UNHANDLED_EXCEPTION,
            new Queues.QueueException('boom'),
            816
        );

        // Act
        executor.execute(ctx);

        // Assert
        Assert.areEqual(1, serviceProvider.getCallCount('handleFailure'));
        Assert.areEqual(jobId, serviceProvider.getLastArgs('handleFailure')[0]);
        Assert.areEqual(ctx.getAsyncApexJobId(), serviceProvider.getLastArgs('handleFailure')[1]);
        Assert.areEqual('boom', serviceProvider.getLastArgs('handleFailure')[2]);
    }

    @IsTest
    static void should_throw_when_job_id_is_missing() {
        // Arrange
        String message;

        // Act
        try {
            new QueuesQueueableExecutor(null);
            Assert.fail('Expected missing job id to throw.');
        } catch (Queues.QueueException ex) {
            message = ex.getMessage();
        }

        // Assert
        Assert.areEqual('jobId cannot be null', message);
    }

    private class FakeQueueableContext implements QueueableContext {
        private final Integer seed;

        private FakeQueueableContext(final Integer seed) {
            this.seed = seed;
        }

        public Id getJobId() {
            return QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, seed);
        }
    }

    private class FakeFinalizerContext implements FinalizerContext {
        private final ParentJobResult result;
        private final Exception ex;
        private final Integer seed;

        private FakeFinalizerContext(final ParentJobResult result, final Exception ex, final Integer seed) {
            this.result = result;
            this.ex = ex;
            this.seed = seed;
        }

        public Id getAsyncApexJobId() {
            return QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, seed);
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
}
