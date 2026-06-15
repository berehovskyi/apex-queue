@IsTest(IsParallel=false)
private class QueuesJobRunRepositoryTest {
    @TestSetup
    static void setup() {
        QueuesTestSupport.setupQueueUser();
    }

    @IsTest
    static void should_insert_runs_and_rely_on_master_detail_cascade() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Arrange
            Job__c job = QueuesTestSupport.job(Queues.State.WAITING.name());
            insert job;

            QueuesJobRunRepository repository = new QueuesJobRunRepository();
            List<JobRun__c> runs = new List<JobRun__c>{
                QueuesTestSupport.jobRun(job.Id, 1, Queues.RunStatus.COMPLETED.name()),
                QueuesTestSupport.jobRun(job.Id, 2, Queues.RunStatus.FAILED.name())
            };

            // Act
            List<JobRun__c> insertedRuns = repository.insertJobRuns(runs);
            delete job;

            // Assert
            Assert.areEqual(2, insertedRuns.size());
            Assert.areEqual(0, [SELECT COUNT() FROM JobRun__c WHERE Job__c = :job.Id]);
        }
    }

    @IsTest
    static void should_return_empty_result_when_runs_are_empty() {
        System.runAs(QueuesTestSupport.queueUser()) {
            // Act
            List<JobRun__c> insertedRuns = new QueuesJobRunRepository().insertJobRuns(new List<JobRun__c>());

            // Assert
            Assert.areEqual(0, insertedRuns.size());
        }
    }
}
