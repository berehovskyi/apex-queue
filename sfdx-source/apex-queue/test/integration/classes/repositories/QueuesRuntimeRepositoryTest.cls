@IsTest(IsParallel=false)
private class QueuesRuntimeRepositoryTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_query_and_update_runtime_rows_when_runtime_exists() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            String queueName = 'runtime-repo';
            QueuesRuntimeRepository repository = new QueuesRuntimeRepository();

            QueueRuntime__c runtime = QueuesTestSupport.runtime(queueName, false, 1);
            repository.insertRuntime(runtime);
            QueueRuntime__c systemRuntime = QueuesTestSupport.runtime(queueName + '-system', false, 0);
            repository.insertRuntime(systemRuntime);

            // Act
            List<QueueRuntime__c> runtimes = repository.queryRuntimes(queueName);
            List<QueueRuntime__c> systemRuntimes = repository.queryRuntimes(queueName + '-system');
            List<QueueRuntime__c> lockedRuntimes = repository.queryRuntimesForUpdate(queueName);
            runtime.AsyncApexJobId__c = QueuesTestSupport.fakeIdValue(AsyncApexJob.SObjectType, 17);
            repository.updateRuntime(runtime);
            List<QueueRuntime__c> runtimesWithWorkerJobs = new QueuesRuntimeRepository(System.AccessLevel.SYSTEM_MODE)
                .queryRuntimesWithWorkerJobs();
            List<QueueRuntime__c> maintenanceRuntimes = repository.queryMaintenanceRuntimes(0);
            runtime.IsPaused__c = true;
            runtime.ActiveWorkerCount__c = 3;
            repository.updateRuntime(runtime);

            QueueRuntime__c updatedRuntime = [
                SELECT IsPaused__c, ActiveWorkerCount__c
                FROM QueueRuntime__c
                WHERE Id = :runtime.Id
            ];

            // Assert
            Assert.areEqual(1, runtimes.size());
            Assert.areEqual(1, systemRuntimes.size());
            Assert.areEqual(1, lockedRuntimes.size());
            Assert.areEqual(1, runtimesWithWorkerJobs.size());
            Assert.areEqual(1, maintenanceRuntimes.size());
            Assert.areEqual(true, updatedRuntime.IsPaused__c);
            Assert.areEqual(3, QueuesSupport.toInteger(updatedRuntime.ActiveWorkerCount__c, 0));
        }
    }
}
