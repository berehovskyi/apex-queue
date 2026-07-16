@IsTest(IsParallel=true)
private class QueuesQueueEventRepositoryTest {
    private class FakeFailureResult implements EventBus.FailureResult {
        private final List<String> eventUuids;

        private FakeFailureResult(final List<String> eventUuidValues) {
            eventUuids = eventUuidValues;
        }

        public List<String> getEventUuids() {
            return eventUuids;
        }
    }

    private class RecordingQueueEventRepository extends QueuesQueueEventRepository {
        private final List<SObject> publishedEvents = new List<SObject>();
        private Object publishCallback;

        protected override List<Database.SaveResult> publishEvents(final List<SObject> events, final Object callback) {
            publishedEvents.addAll(events);
            publishCallback = callback;
            return new List<Database.SaveResult>();
        }
    }

    @IsTest
    static void should_publish_queue_events_through_publish_seam() {
        // Arrange
        RecordingQueueEventRepository repository = new RecordingQueueEventRepository();
        QueueEvent__e eventRecord = new QueueEvent__e(
            Type__c = 'COMPLETED',
            QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME,
            OccurredAt__c = System.now()
        );

        // Act
        repository.publishQueueEvents(new List<QueueEvent__e>{ eventRecord });
        repository.publishQueueEvents(new List<QueueEvent__e>());
        repository.publishQueueEvents(null);

        // Assert
        Assert.areEqual(1, repository.publishedEvents.size());
        Assert.areEqual('COMPLETED', repository.publishedEvents[0].get('Type__c'));
        Assert.isTrue(repository.publishCallback instanceof QueuesQueueEventRepository.PublishFailureCallback);
    }

    @IsTest
    static void should_throw_when_event_enqueue_fails() {
        // Arrange
        QueuesQueueEventRepository repository = new QueuesQueueEventRepository();
        QueueEvent__e invalidEvent = (QueueEvent__e) QueueEvent__e.SObjectType.newSObject(null, true);

        // Act
        try {
            repository.publishQueueEvents(new List<QueueEvent__e>{ invalidEvent });
            Assert.fail('Expected the unsuccessful event enqueue result to throw.');
        } catch (Queues.QueueException ex) {
            // Assert
            Assert.isTrue(String.isNotBlank(ex.getMessage()));
        }
    }

    @IsTest
    static void should_persist_queue_errors_for_async_publish_failures() {
        // Arrange
        QueueEvent__e successfulEvent = (QueueEvent__e) QueueEvent__e.SObjectType.newSObject(null, true);
        successfulEvent.Type__c = 'COMPLETED';
        successfulEvent.QueueName__c = QueuesTestSupport.DEFAULT_QUEUE_NAME;
        successfulEvent.OccurredAt__c = System.now();
        QueueEvent__e failedEvent = (QueueEvent__e) QueueEvent__e.SObjectType.newSObject(null, true);
        failedEvent.Type__c = 'FAILED';
        failedEvent.QueueName__c = 'failed-queue';
        failedEvent.OccurredAt__c = System.now();
        QueuesTestSupport.RecordingStubProvider errorRepositoryProvider = new QueuesTestSupport.RecordingStubProvider();
        QueuesQueueEventRepository.PublishFailureCallback callback = new QueuesQueueEventRepository.PublishFailureCallback(
            new List<QueueEvent__e>{ successfulEvent, failedEvent },
            (QueuesErrorRepository) Test.createStub(QueuesErrorRepository.class, errorRepositoryProvider)
        );

        // Act
        callback.onFailure(new FakeFailureResult(new List<String>{ failedEvent.EventUuid }));

        // Assert
        Assert.areEqual(1, errorRepositoryProvider.getCallCount('insertQueueErrors'));
        List<QueueError__c> errors = (List<QueueError__c>) errorRepositoryProvider.getLastArgs('insertQueueErrors')[0];
        Assert.areEqual(1, errors.size());
        Assert.areEqual('failed-queue', errors[0].QueueName__c);
        Assert.areEqual('queue events', errors[0].Component__c);
        Assert.areEqual('Queue lifecycle event publication failed asynchronously.', errors[0].Message__c);
        Assert.isNotNull(errors[0].OccurredAt__c);
    }
}
