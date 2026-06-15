@IsTest(IsParallel=true)
private class QueuesJobRunServiceTest {
    @IsTest
    static void should_insert_runs_in_requested_mode_and_add_timestamps() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('insertJobRuns', new List<JobRun__c>());
        QueuesJobRunService service = new QueuesJobRunService(
            (QueuesJobRunRepository) Test.createStub(QueuesJobRunRepository.class, repositoryProvider)
        );
        Datetime finishedAt = System.now();
        JobRun__c run = new JobRun__c(
            Job__c = QueuesTestSupport.fakeId(Job__c.SObjectType, 1),
            AttemptNumber__c = 1,
            StartedAt__c = finishedAt.addSeconds(-5)
        );

        // Act
        service.insertJobRuns(new List<JobRun__c>{ run });
        service.insertJobRunsAsSystem(new List<JobRun__c>{ run });
        service.insertJobRuns(new List<JobRun__c>{ run }, false);
        service.insertJobRuns(new List<JobRun__c>{ run }, true);
        service.addTimestamps(run, finishedAt.addSeconds(-5).getTime(), finishedAt);

        // Assert
        Assert.areEqual(4, repositoryProvider.getCallCount('insertJobRuns'));
        Assert.areEqual(finishedAt, run.FinishedAt__c);
        Assert.areEqual(true, run.DurationMs__c >= 5000);
    }

    @IsTest
    static void should_floor_negative_duration_to_zero() {
        // Arrange
        Datetime finishedAt = System.now();
        JobRun__c run = new JobRun__c(
            Job__c = QueuesTestSupport.fakeId(Job__c.SObjectType, 2),
            AttemptNumber__c = 1,
            StartedAt__c = finishedAt
        );

        // Act
        new QueuesJobRunService().addTimestamps(run, finishedAt.addSeconds(5).getTime(), finishedAt);

        // Assert
        Assert.areEqual(0, QueuesSupport.toInteger(run.DurationMs__c, -1));
    }
}
