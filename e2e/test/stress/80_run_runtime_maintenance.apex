Queues.MaintenanceResult result = Queues.runRuntimeMaintenance();

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'stress.runtime-maintenance',
            'done' => result.done,
            'total' => result.total
        }
    )
);
