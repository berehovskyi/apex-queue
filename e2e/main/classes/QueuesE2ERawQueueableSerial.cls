@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesE2ERawQueueableSerial implements Queueable {
    private final String runKey;
    private final Integer itemIndex;
    private final String requestedAtMs;
    private final Integer delayMs;

    public QueuesE2ERawQueueableSerial(
        final String runKey,
        final Integer itemIndex,
        final String requestedAtMs,
        final Integer delayMs
    ) {
        this.runKey = runKey;
        this.itemIndex = itemIndex;
        this.requestedAtMs = requestedAtMs;
        this.delayMs = delayMs;
    }

    public void execute(final QueueableContext ctx) {
        List<RawPerformanceRun__c> runs = QueuesE2ERawPerformanceProbe.queryRuns(
            runKey,
            QueuesE2ERawPerformanceProbe.VARIANT_QUEUEABLE_SERIAL,
            itemIndex,
            1
        );
        if (runs.isEmpty()) {
            return;
        }

        try {
            QueuesE2ERawPerformanceProbe.capture(runs[0].Id, requestedAtMs, delayMs, ctx.getJobId());
        } catch (Exception ex) {
            QueuesE2ERawPerformanceProbe.captureFailure(runs[0].Id, ctx.getJobId(), ex);
        }

        Integer nextIndex = itemIndex + 1;
        if (
            QueuesE2ERawPerformanceProbe.hasRun(
                runKey,
                QueuesE2ERawPerformanceProbe.VARIANT_QUEUEABLE_SERIAL,
                nextIndex
            )
        ) {
            QueuesE2ERawPerformanceProbe.enqueueSerialQueueableFromFuture(runKey, nextIndex, requestedAtMs, delayMs);
        }
    }
}
