@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesE2ERawBatchIterator implements Database.Batchable<RawPerformanceRun__c> {
    private final String runKey;
    private final String requestedAtMs;
    private final Integer delayMs;

    public QueuesE2ERawBatchIterator(final String runKey, final String requestedAtMs, final Integer delayMs) {
        this.runKey = runKey;
        this.requestedAtMs = requestedAtMs;
        this.delayMs = delayMs;
    }

    public Iterable<RawPerformanceRun__c> start(final Database.BatchableContext ctx) {
        String variant = QueuesE2ERawPerformanceProbe.VARIANT_BATCH_ITERATOR;
        return [
            SELECT Id
            FROM RawPerformanceRun__c
            WHERE RunKey__c = :runKey AND Variant__c = :variant
            ORDER BY ItemIndex__c
        ];
    }

    public void execute(final Database.BatchableContext ctx, final List<RawPerformanceRun__c> scope) {
        for (RawPerformanceRun__c run : scope) {
            try {
                QueuesE2ERawPerformanceProbe.capture(run.Id, requestedAtMs, delayMs, ctx.getJobId());
            } catch (Exception ex) {
                QueuesE2ERawPerformanceProbe.captureFailure(run.Id, ctx.getJobId(), ex);
            }
        }
    }

    public void finish(final Database.BatchableContext ctx) {
    }
}
