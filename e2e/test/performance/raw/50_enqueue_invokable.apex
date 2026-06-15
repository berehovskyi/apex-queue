String runKey = QueuesE2ERawPerformanceProbe.DEFAULT_RUN_KEY;
Integer jobCount = QueuesE2ERawPerformanceProbe.DEFAULT_JOB_COUNT;
Integer delayMs = QueuesE2ERawPerformanceProbe.DEFAULT_DELAY_MS;

QueuesE2ERawPerformanceProbe.enqueueInvocable(runKey, jobCount);

Integer storedCount = [
    SELECT COUNT()
    FROM RawPerformanceRun__c
    WHERE RunKey__c = :runKey AND Variant__c = :QueuesE2ERawPerformanceProbe.VARIANT_INVOCABLE
];

Assert.areEqual(jobCount, storedCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'performance.raw.invocable.enqueue',
            'runKey' => runKey,
            'variant' => QueuesE2ERawPerformanceProbe.VARIANT_INVOCABLE,
            'jobCount' => jobCount,
            'delayMs' => delayMs
        }
    )
);
