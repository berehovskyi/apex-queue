@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesE2ERawBatchCursor implements Database.Batchable<SObject> {
    private final String runKey;
    private final String requestedAtMs;
    private final Integer delayMs;

    public QueuesE2ERawBatchCursor(final String runKey, final String requestedAtMs, final Integer delayMs) {
        this.runKey = runKey;
        this.requestedAtMs = requestedAtMs;
        this.delayMs = delayMs;
    }

    public Database.QueryLocator start(final Database.BatchableContext ctx) {
        String variant = QueuesE2ERawPerformanceProbe.VARIANT_BATCH_CURSOR;
        return Database.getQueryLocator(
            [
                SELECT Id
                FROM RawPerformanceRun__c
                WHERE RunKey__c = :runKey AND Variant__c = :variant
                ORDER BY ItemIndex__c
            ]
        );
    }

    public void execute(final Database.BatchableContext ctx, final List<SObject> scope) {
        for (SObject row : scope) {
            Id runId = (Id) row.get('Id');
            try {
                QueuesE2ERawPerformanceProbe.capture(runId, requestedAtMs, delayMs, ctx.getJobId());
            } catch (Exception ex) {
                QueuesE2ERawPerformanceProbe.captureFailure(runId, ctx.getJobId(), ex);
            }
        }
    }

    public void finish(final Database.BatchableContext ctx) {
    }
}
