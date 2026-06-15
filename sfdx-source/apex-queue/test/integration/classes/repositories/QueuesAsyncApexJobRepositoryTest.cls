@IsTest(IsParallel=false)
private class QueuesAsyncApexJobRepositoryTest {
    // <editor-fold desc="Queries">
    @IsTest
    static void should_query_async_jobs_when_job_ids_are_provided() {
        // Arrange
        QueuesAsyncApexJobRepository repository = new QueuesAsyncApexJobRepository();

        // Act
        Test.startTest();
        Id asyncJobId = System.enqueueJob(new NoOpQueueable());
        Id batchJobId = Database.executeBatch(new AsyncJobRepoTestBatchable(), 1);
        AsyncApexJob batchJob = [
            SELECT ApexClass.Name, Status
            FROM AsyncApexJob
            WHERE Id = :batchJobId
        ];
        List<AsyncApexJob> activeBatchJobs = repository.queryActiveAsyncJobsByApexClass(
            batchJob.ApexClass.Name,
            new Set<String>{ batchJob.Status }
        );
        List<AsyncApexJob> blankClassJobs = repository.queryActiveAsyncJobsByApexClass(
            '',
            new Set<String>{ batchJob.Status }
        );
        List<AsyncApexJob> emptyStatusJobs = repository.queryActiveAsyncJobsByApexClass(
            batchJob.ApexClass.Name,
            new Set<String>()
        );
        Test.stopTest();
        List<AsyncApexJob> singleAsyncJobs = repository.queryAsyncJobs(asyncJobId);
        List<AsyncApexJob> asyncJobs = repository.queryAsyncJobsByIds(new Set<Id>{ asyncJobId });
        List<AsyncApexJob> emptyJobs = repository.queryAsyncJobsByIds(new Set<Id>());
        List<AsyncApexJob> blankAsyncJobs = repository.queryAsyncJobs(null);

        // Assert
        Assert.areEqual(1, singleAsyncJobs.size());
        Assert.areEqual(asyncJobId, singleAsyncJobs[0].Id);
        Assert.areEqual(1, asyncJobs.size());
        Assert.areEqual(asyncJobId, asyncJobs[0].Id);
        Assert.areEqual(1, activeBatchJobs.size());
        Assert.areEqual(batchJobId, activeBatchJobs[0].Id);
        Assert.areEqual(true, String.isNotBlank(asyncJobs[0].Status));
        Assert.areEqual(0, emptyJobs.size());
        Assert.areEqual(0, blankAsyncJobs.size());
        Assert.areEqual(0, blankClassJobs.size());
        Assert.areEqual(0, emptyStatusJobs.size());
    }
    // </editor-fold>

    // <editor-fold desc="Commands">
    @IsTest
    static void should_reorder_batch_jobs_in_flex_queue() {
        // Arrange
        QueuesAsyncApexJobRepository repository = new QueuesAsyncApexJobRepository();
        Test.enqueueBatchJobs(8);
        List<Id> batchJobIds = Test.getFlexQueueOrder();

        // Act
        Boolean movedToFront = repository.moveJobToFront(batchJobIds[2]);
        List<Id> frontOrder = Test.getFlexQueueOrder();
        Boolean movedToEnd = repository.moveJobToEnd(batchJobIds[2]);
        List<Id> endOrder = Test.getFlexQueueOrder();

        // Assert
        Assert.areEqual(true, movedToFront);
        Assert.areEqual(batchJobIds[2], frontOrder[0]);
        Assert.areEqual(true, movedToEnd);
        Assert.areEqual(batchJobIds[2], endOrder[endOrder.size() - 1]);
    }
    // </editor-fold>

    @SuppressWarnings('PMD.QueueableWithoutFinalizer')
    private class NoOpQueueable implements Queueable {
        public void execute(final QueueableContext ctx) {
        }
    }
}
