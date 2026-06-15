@IsTest(IsParallel=true)
private class QueuesJobSchedulerMaintenanceTest {
    @IsTest
    static void should_delegate_job_scheduler_maintenance_when_queueable_executes() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider serviceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('runJobSchedulerMaintenance', new Queues.MaintenanceResult().add('scheduler-queue', 2));
        QueuesService serviceStub = (QueuesService) Test.createStub(QueuesService.class, serviceProvider);
        QueuesJobSchedulerMaintenance maintenance = new QueuesJobSchedulerMaintenance(serviceStub);

        // Act
        maintenance.execute(null);

        // Assert
        Assert.areEqual(1, serviceProvider.getCallCount('runJobSchedulerMaintenance'));
    }
}
