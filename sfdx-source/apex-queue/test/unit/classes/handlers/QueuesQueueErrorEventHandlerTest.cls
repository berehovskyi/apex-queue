@IsTest(IsParallel=true)
private class QueuesQueueErrorEventHandlerTest {
    @IsTest
    static void should_return_without_inserting_when_events_are_null_or_empty() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesErrorService errorService = new QueuesErrorService(
            (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, repositoryProvider),
            new QueuesErrorEventRepository()
        );
        QueuesQueueErrorEventHandler handler = new QueuesQueueErrorEventHandler(errorService);

        // Act
        handler.handle(null);
        handler.handle(new List<QueueErrorEvent__e>());

        // Assert
        Assert.areEqual(0, repositoryProvider.getCallCount('insertQueueErrors'));
    }

    @IsTest
    static void should_insert_only_valid_queue_errors_from_events() {
        // Arrange
        String asyncApexJobId = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 221);
        QueueErrorEvent__e validEvent = new QueueErrorEvent__e(
            QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
            Component__c = 'runtime maintenance',
            JobId__c = 'not-an-id',
            AsyncApexJobId__c = asyncApexJobId,
            Message__c = 'Maintenance exploded.',
            StackTrace__c = 'stack'
        );
        QueueErrorEvent__e invalidEvent = new QueueErrorEvent__e(
            QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
            Message__c = 'Missing component.'
        );

        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesErrorService errorService = new QueuesErrorService(
            (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, repositoryProvider),
            new QueuesErrorEventRepository()
        );
        QueuesQueueErrorEventHandler handler = new QueuesQueueErrorEventHandler(errorService);

        // Act
        handler.handle(new List<QueueErrorEvent__e>{ validEvent, invalidEvent });

        // Assert
        Assert.areEqual(1, repositoryProvider.getCallCount('insertQueueErrors'));
        List<QueueError__c> errors = (List<QueueError__c>) repositoryProvider.getLastArgs('insertQueueErrors')[0];
        Assert.areEqual(1, errors.size());
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, errors[0].QueueName__c);
        Assert.areEqual('runtime maintenance', errors[0].Component__c);
        Assert.areEqual(null, errors[0].Job__c);
        Assert.areEqual(asyncApexJobId, errors[0].AsyncApexJobId__c);
        Assert.areEqual('Maintenance exploded.', errors[0].Message__c);
        Assert.areEqual('stack', errors[0].StackTrace__c);
        Assert.areNotEqual(null, errors[0].OccurredAt__c);
    }
}
