@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesE2ERawQueueableFutureLauncher implements Queueable {
    private final String runKey;
    private final Integer startIndex;
    private final Integer rowLimit;
    private final String requestedAtMs;
    private final Integer delayMs;

    public QueuesE2ERawQueueableFutureLauncher(
        final String runKey,
        final Integer startIndex,
        final Integer rowLimit,
        final String requestedAtMs,
        final Integer delayMs
    ) {
        this.runKey = runKey;
        this.startIndex = startIndex;
        this.rowLimit = rowLimit;
        this.requestedAtMs = requestedAtMs;
        this.delayMs = delayMs;
    }

    public void execute(final QueueableContext ctx) {
        for (
            RawPerformanceRun__c run : QueuesE2ERawPerformanceProbe.queryRuns(
                runKey,
                QueuesE2ERawPerformanceProbe.VARIANT_QUEUEABLE_FUTURE,
                startIndex,
                rowLimit
            )
        ) {
            QueuesE2ERawPerformanceProbe.enqueueConcurrentQueueableFromFuture(run.Id, requestedAtMs, delayMs);
        }
    }
}
