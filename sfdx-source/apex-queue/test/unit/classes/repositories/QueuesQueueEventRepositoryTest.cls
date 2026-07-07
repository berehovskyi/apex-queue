@IsTest(IsParallel=true)
private class QueuesQueueEventRepositoryTest {
    private class RecordingQueueEventRepository extends QueuesQueueEventRepository {
        private final List<SObject> publishedEvents = new List<SObject>();

        protected override void publishEvents(final List<SObject> events) {
            publishedEvents.addAll(events);
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
    }
}
