@IsTest(IsParallel=true)
private class QueuesTerminalCleanupMaintenanceTest {
    @IsTest
    static void should_delegate_terminal_cleanup_maintenance_when_queueable_executes() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider serviceProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('runTerminalCleanupMaintenance', new Queues.MaintenanceResult().add('cleanup-queue', 3));
        QueuesService serviceStub = (QueuesService) Test.createStub(QueuesService.class, serviceProvider);
        QueuesMaintenanceWorker maintenance = new QueuesMaintenanceWorker(
            serviceStub,
            new QueuesTerminalCleanupMaintenance()
        );

        // Act
        maintenance.execute((QueueableContext) null);

        // Assert
        Assert.areEqual(1, serviceProvider.getCallCount('runTerminalCleanupMaintenance'));
        Assert.areEqual('terminal cleanup maintenance', new QueuesTerminalCleanupMaintenance().component());
    }
}
