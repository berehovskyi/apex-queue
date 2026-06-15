@IsTest(IsParallel=true)
private class QueuesBatchApexErrorEventTriggerTest {
    // <editor-fold desc="Trigger Coverage">
    @IsTest
    static void should_fire_batch_apex_error_event_trigger_when_batch_scope_fails() {
        // Act
        Test.startTest();
        Database.executeBatch(new QueuesBatchApexErrorEventTestBatch(), 1);
        Exception batchFailure;
        try {
            Test.stopTest();
        } catch (Exception ex) {
            batchFailure = ex;
        }
        Test.getEventBus().deliver();

        // Assert
        Assert.isInstanceOfType(
            batchFailure,
            QueuesBatchApexErrorEventTestBatch.IntentionalBatchFailureException.class
        );
    }
    // </editor-fold>
}
