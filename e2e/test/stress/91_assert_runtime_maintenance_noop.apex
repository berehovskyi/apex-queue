Queues.MaintenanceResult result = Queues.runRuntimeMaintenance();

Assert.areEqual(true, result.done);
Assert.areEqual(0, result.total);
