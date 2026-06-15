Queues.MaintenanceResult result = Queues.runJobSchedulerMaintenance();

Assert.areEqual(true, result.done);
Assert.areEqual(0, result.total);
