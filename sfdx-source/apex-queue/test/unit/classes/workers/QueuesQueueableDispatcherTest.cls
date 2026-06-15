@IsTest(IsParallel=true)
private class QueuesQueueableDispatcherTest {
    @IsTest
    static void should_delegate_queueable_execution_to_dispatch_service() {
        // Arrange
        Id dispatchId = QueuesTestSupport.fakeId(QueueableDispatch__c.SObjectType, 801);
        QueuesTestSupport.RecordingStubProvider serviceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueableDispatcher dispatcher = new QueuesQueueableDispatcher(
            dispatchId,
            (QueuesQueueableDispatchService) Test.createStub(QueuesQueueableDispatchService.class, serviceProvider),
            false
        );
        QueueableContext ctx = new FakeQueueableContext();

        // Act
        dispatcher.execute(ctx);

        // Assert
        Assert.areEqual(1, serviceProvider.getCallCount('startDispatchAsSystem'));
        Assert.areEqual(dispatchId, serviceProvider.getLastArgs('startDispatchAsSystem')[0]);
        Assert.areEqual(ctx.getJobId(), serviceProvider.getLastArgs('startDispatchAsSystem')[1]);
    }

    @IsTest
    static void should_continue_dispatch_when_finalizer_succeeds() {
        // Arrange
        Id dispatchId = QueuesTestSupport.fakeId(QueueableDispatch__c.SObjectType, 803);
        QueuesTestSupport.RecordingStubProvider serviceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueableDispatcher dispatcher = new QueuesQueueableDispatcher(
            dispatchId,
            (QueuesQueueableDispatchService) Test.createStub(QueuesQueueableDispatchService.class, serviceProvider),
            false
        );

        // Act
        dispatcher.execute(new FakeFinalizerContext(ParentJobResult.SUCCESS, null));

        // Assert
        Assert.areEqual(1, serviceProvider.getCallCount('continueDispatch'));
        Assert.areEqual(dispatchId, serviceProvider.getLastArgs('continueDispatch')[0]);
        Assert.areEqual(0, serviceProvider.getCallCount('failDispatchAsSystem'));
    }

    @IsTest
    static void should_fail_dispatch_when_finalizer_reports_unhandled_exception() {
        // Arrange
        Id dispatchId = QueuesTestSupport.fakeId(QueueableDispatch__c.SObjectType, 804);
        QueuesTestSupport.RecordingStubProvider serviceProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueableDispatcher dispatcher = new QueuesQueueableDispatcher(
            dispatchId,
            (QueuesQueueableDispatchService) Test.createStub(QueuesQueueableDispatchService.class, serviceProvider),
            false
        );

        // Act
        dispatcher.execute(
            new FakeFinalizerContext(ParentJobResult.UNHANDLED_EXCEPTION, new Queues.QueueException('boom'))
        );

        // Assert
        Assert.areEqual(0, serviceProvider.getCallCount('continueDispatch'));
        Assert.areEqual(1, serviceProvider.getCallCount('failDispatchAsSystem'));
        Assert.areEqual(dispatchId, serviceProvider.getLastArgs('failDispatchAsSystem')[0]);
        Assert.areEqual('boom', serviceProvider.getLastArgs('failDispatchAsSystem')[1]);
    }

    @IsTest
    static void should_throw_when_dispatch_id_is_missing() {
        // Arrange
        String message;

        // Act
        try {
            new QueuesQueueableDispatcher(null);
            Assert.fail('Expected missing dispatch id to throw.');
        } catch (Queues.QueueException ex) {
            message = ex.getMessage();
        }

        // Assert
        Assert.areEqual('dispatchId cannot be null', message);
    }

    private class FakeQueueableContext implements QueueableContext {
        public Id getJobId() {
            return QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 802);
        }
    }

    private class FakeFinalizerContext implements FinalizerContext {
        private final ParentJobResult result;
        private final Exception ex;

        private FakeFinalizerContext(final ParentJobResult result, final Exception ex) {
            this.result = result;
            this.ex = ex;
        }

        public Id getAsyncApexJobId() {
            return QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 805);
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
