@IsTest(IsParallel=true)
private class QueuesQueueErrorLogTriggerTest {
    @IsTest
    static void should_fire_queue_error_log_trigger_when_queue_error_event_is_published() {
        // Arrange
        QueueErrorEvent__e eventRecord = new QueueErrorEvent__e(
            QueueName__c = 'queue-error-trigger',
            Component__c = 'runtime maintenance',
            Message__c = 'Trigger published.',
            OccurredAt__c = System.now()
        );

        // Act
        Test.startTest();
        Database.SaveResult publishResult = EventBus.publish(eventRecord);
        Test.getEventBus().deliver();
        Test.stopTest();

        // Assert
        Assert.areEqual(true, publishResult.isSuccess());
        Assert.areEqual(1, [SELECT COUNT() FROM QueueError__c WHERE QueueName__c = 'queue-error-trigger']);
    }
}
