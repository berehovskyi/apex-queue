QueuesE2ERawPerformanceProbe.cleanup(QueuesE2ERawPerformanceProbe.DEFAULT_RUN_KEY);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'performance.raw.cleanup',
            'runKey' => QueuesE2ERawPerformanceProbe.DEFAULT_RUN_KEY
        }
    )
);
