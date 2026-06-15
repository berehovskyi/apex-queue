String runKey = QueuesE2ERawPerformanceProbe.DEFAULT_RUN_KEY;
Integer jobCount = QueuesE2ERawPerformanceProbe.DEFAULT_JOB_COUNT;
Integer delayMs = QueuesE2ERawPerformanceProbe.DEFAULT_DELAY_MS;

List<Id> launcherIds = QueuesE2ERawPerformanceProbe.enqueueQueueableConcurrentViaFuture(runKey, jobCount, delayMs);
Integer storedCount = [
    SELECT COUNT()
    FROM RawPerformanceRun__c
    WHERE RunKey__c = :runKey AND Variant__c = :QueuesE2ERawPerformanceProbe.VARIANT_QUEUEABLE_FUTURE
];

Assert.areEqual(jobCount, storedCount);
Assert.areEqual(10, launcherIds.size());

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'performance.raw.queueable_future.enqueue',
            'runKey' => runKey,
            'variant' => QueuesE2ERawPerformanceProbe.VARIANT_QUEUEABLE_FUTURE,
            'jobCount' => jobCount,
            'delayMs' => delayMs,
            'launcherIds' => launcherIds
        }
    )
);
