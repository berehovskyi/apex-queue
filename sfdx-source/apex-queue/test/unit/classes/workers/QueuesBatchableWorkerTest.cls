@IsTest(IsParallel=true)
private class QueuesBatchableWorkerTest {
    @IsTest
    static void should_delegate_batchable_lifecycle_when_worker_runs() {
        // Arrange
        QueuesBatchableWorker worker = new QueuesBatchableWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, 'worker-token');
        Database.BatchableContext ctx = new FakeBatchableContext();

        // Act
        Database.QueryLocator locator = worker.start(ctx);
        worker.execute(ctx, new List<SObject>());
        worker.finish(ctx);

        // Assert
        Assert.areNotEqual(null, locator);
    }

    @IsTest
    static void should_throw_queue_exception_when_constructor_arguments_are_invalid() {
        // Arrange
        String blankQueueMessage;
        String blankWorkerTokenMessage;

        // Act
        try {
            new QueuesBatchableWorker('', 'worker-token');
            Assert.fail('Expected blank queue name to throw.');
        } catch (Queues.QueueException ex) {
            blankQueueMessage = ex.getMessage();
        }

        try {
            new QueuesBatchableWorker(QueuesTestSupport.DEFAULT_QUEUE_NAME, '');
            Assert.fail('Expected blank worker token to throw.');
        } catch (Queues.QueueException ex) {
            blankWorkerTokenMessage = ex.getMessage();
        }

        // Assert
        Assert.areEqual('queueName cannot be blank', blankQueueMessage);
        Assert.areEqual('workerToken cannot be blank', blankWorkerTokenMessage);
    }

    private class FakeBatchableContext implements Database.BatchableContext {
        public Id getJobId() {
            return QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 901);
        }

        public Id getChildJobId() {
            return QueuesTestSupport.fakeId(AsyncApexJob.SObjectType, 902);
        }
    }
}
