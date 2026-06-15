@IsTest(IsParallel=true)
private class QueuesMaintenanceSchedulerTest {
    // <editor-fold desc="Scheduler Execution">
    @IsTest
    static void should_run_runtime_maintenance_and_submit_job_scheduler_maintenance() {
        // Arrange
        MaintenanceOrderStubProvider serviceProvider = new MaintenanceOrderStubProvider();
        QueuesService serviceStub = (QueuesService) Test.createStub(QueuesService.class, serviceProvider);
        QueuesMaintenanceScheduler scheduler = new QueuesMaintenanceScheduler(serviceStub);

        // Act
        Test.startTest();
        scheduler.execute(new TestSchedulableContext(QueuesTestSupport.fakeId(CronTrigger.SObjectType, 1)));

        // Assert
        Assert.areEqual(1, serviceProvider.runtimeMaintenanceCallCount);
        Assert.areEqual(1, serviceProvider.queueableJobsWhenRuntimeMaintenanceRan);
        Assert.areEqual(1, Limits.getQueueableJobs());
        Test.stopTest();
    }
    // </editor-fold>

    private class TestSchedulableContext implements SchedulableContext {
        private final String triggerId;

        private TestSchedulableContext(final Id triggerId) {
            this.triggerId = triggerId.toString();
        }

        public String getTriggerId() {
            return triggerId;
        }
    }

    private class MaintenanceOrderStubProvider implements System.StubProvider {
        private Integer runtimeMaintenanceCallCount = 0;
        private Integer queueableJobsWhenRuntimeMaintenanceRan;

        public Object handleMethodCall(
            final Object stubbedObject,
            final String stubbedMethodName,
            final Type returnType,
            final List<Type> listOfParamTypes,
            final List<String> listOfParamNames,
            final List<Object> listOfArgs
        ) {
            if (stubbedMethodName != 'runRuntimeMaintenance') {
                return null;
            }

            runtimeMaintenanceCallCount++;
            queueableJobsWhenRuntimeMaintenanceRan = Limits.getQueueableJobs();
            return new Queues.MaintenanceResult().add('runtime-queue', 1);
        }
    }
}
