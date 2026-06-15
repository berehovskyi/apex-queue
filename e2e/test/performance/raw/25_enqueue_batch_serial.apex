String runKey = QueuesE2ERawPerformanceProbe.DEFAULT_RUN_KEY;
Integer jobCount = QueuesE2ERawPerformanceProbe.DEFAULT_JOB_COUNT;
Integer delayMs = QueuesE2ERawPerformanceProbe.DEFAULT_DELAY_MS;

Id asyncApexJobId = QueuesE2ERawPerformanceProbe.enqueueBatchSerial(runKey, jobCount, delayMs);
Integer storedCount = [
    SELECT COUNT()
    FROM RawPerformanceRun__c
    WHERE RunKey__c = :runKey AND Variant__c = :QueuesE2ERawPerformanceProbe.VARIANT_BATCH_SERIAL
];

Assert.areEqual(jobCount, storedCount);
Assert.isTrue(String.isNotBlank(asyncApexJobId));

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'performance.raw.batch_serial.enqueue',
            'runKey' => runKey,
            'variant' => QueuesE2ERawPerformanceProbe.VARIANT_BATCH_SERIAL,
            'jobCount' => jobCount,
            'delayMs' => delayMs,
            'asyncApexJobId' => asyncApexJobId
        }
    )
);
