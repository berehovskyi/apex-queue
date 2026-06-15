String runKey = QueuesE2ERawPerformanceProbe.DEFAULT_RUN_KEY;
Integer jobCount = QueuesE2ERawPerformanceProbe.DEFAULT_JOB_COUNT;
Integer delayMs = QueuesE2ERawPerformanceProbe.DEFAULT_DELAY_MS;

Id asyncApexJobId = QueuesE2ERawPerformanceProbe.enqueueBatchCursor(runKey, jobCount, delayMs);
Integer storedCount = [
    SELECT COUNT()
    FROM RawPerformanceRun__c
    WHERE RunKey__c = :runKey AND Variant__c = :QueuesE2ERawPerformanceProbe.VARIANT_BATCH_CURSOR
];

Assert.areEqual(jobCount, storedCount);
Assert.isTrue(String.isNotBlank(asyncApexJobId));

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'performance.raw.batch_cursor.enqueue',
            'runKey' => runKey,
            'variant' => QueuesE2ERawPerformanceProbe.VARIANT_BATCH_CURSOR,
            'jobCount' => jobCount,
            'delayMs' => delayMs,
            'asyncApexJobId' => asyncApexJobId
        }
    )
);
