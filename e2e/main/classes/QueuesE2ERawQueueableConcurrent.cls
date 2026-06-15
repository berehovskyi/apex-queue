@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesE2ERawQueueableConcurrent implements Queueable {
    private final Id runId;
    private final String requestedAtMs;
    private final Integer delayMs;

    public QueuesE2ERawQueueableConcurrent(final Id runId, final String requestedAtMs, final Integer delayMs) {
        this.runId = runId;
        this.requestedAtMs = requestedAtMs;
        this.delayMs = delayMs;
    }

    public void execute(final QueueableContext ctx) {
        try {
            QueuesE2ERawPerformanceProbe.capture(runId, requestedAtMs, delayMs, ctx.getJobId());
        } catch (Exception ex) {
            QueuesE2ERawPerformanceProbe.captureFailure(runId, ctx.getJobId(), ex);
        }
    }
}
