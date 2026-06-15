@IsTest(IsParallel=false)
private class QueuesJobSchedulerRepositoryTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_insert_query_update_list_count_and_delete_job_schedulers() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'scheduler-repo-' + System.now().getTime();
            JobScheduler__c scheduler = QueuesTestSupport.jobScheduler(queueName, 'nightly');
            QueuesJobSchedulerRepository repository = new QueuesJobSchedulerRepository();

            // Act
            repository.insertJobScheduler(scheduler);
            List<JobScheduler__c> byExternalId = repository.queryJobSchedulersByExternalId(scheduler.ExternalId__c);
            List<JobScheduler__c> byId = repository.queryJobSchedulersById(scheduler.Id);
            List<JobScheduler__c> byIdForUpdate = repository.queryJobSchedulersByIdForUpdate(scheduler.Id);
            List<JobScheduler__c> listed = repository.queryJobSchedulers(queueName, 10, 0, true);
            Integer schedulerCount = repository.queryJobSchedulersCount(queueName);
            scheduler.NextRunAt__c = scheduler.NextRunAt__c.addMinutes(5);
            repository.updateJobScheduler(scheduler);
            repository.deleteJobScheduler(scheduler);
            List<JobScheduler__c> afterDelete = repository.queryJobSchedulersById(scheduler.Id);

            // Assert
            Assert.areEqual(1, byExternalId.size());
            Assert.areEqual(scheduler.Id, byExternalId[0].Id);
            Assert.areEqual(1, byId.size());
            Assert.areEqual(1, byIdForUpdate.size());
            Assert.areEqual(1, listed.size());
            Assert.areEqual(1, schedulerCount);
            Assert.areEqual(0, afterDelete.size());
        }
    }

    @IsTest
    static void should_return_empty_results_for_empty_query_inputs() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            QueuesJobSchedulerRepository repository = new QueuesJobSchedulerRepository();

            // Act
            List<JobScheduler__c> blankExternalIdRows = repository.queryJobSchedulersByExternalId(null);
            List<JobScheduler__c> blankIdRows = repository.queryJobSchedulersById(null);
            List<JobScheduler__c> blankIdForUpdateRows = repository.queryJobSchedulersByIdForUpdate(null);
            List<JobScheduler__c> blankListRows = repository.queryJobSchedulers(null, 10, 0, true);
            List<JobScheduler__c> zeroLimitRows = repository.queryJobSchedulers('queue', 0, 0, true);
            Integer blankCount = repository.queryJobSchedulersCount(null);

            // Assert
            Assert.areEqual(0, blankExternalIdRows.size());
            Assert.areEqual(0, blankIdRows.size());
            Assert.areEqual(0, blankIdForUpdateRows.size());
            Assert.areEqual(0, blankListRows.size());
            Assert.areEqual(0, zeroLimitRows.size());
            Assert.areEqual(0, blankCount);
        }
    }
}
