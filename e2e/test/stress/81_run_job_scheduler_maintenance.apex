Queues.MaintenanceResult result = Queues.runJobSchedulerMaintenance();

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'stress.job-scheduler-maintenance',
            'done' => result.done,
            'total' => result.total
        }
    )
);
