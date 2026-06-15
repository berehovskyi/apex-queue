@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesE2EQueueableStackDepthProbe implements Queueable {
    public static final String VARIANT = 'queueable-stack-depth';

    private final String runKey;
    private final Integer depth;
    private final Integer targetDepth;

    public QueuesE2EQueueableStackDepthProbe(final String runKey, final Integer depth, final Integer targetDepth) {
        this.runKey = runKey;
        this.depth = depth;
        this.targetDepth = targetDepth;
    }

    public void execute(final QueueableContext ctx) {
        insert new RawPerformanceRun__c(
            RunKey__c = runKey,
            Variant__c = VARIANT,
            ItemIndex__c = depth,
            Status__c = 'COMPLETED',
            AsyncApexJobId__c = ctx.getJobId(),
            Message__c = JSON.serialize(
                new Map<String, Object>{
                    'declaredDepth' => depth,
                    'currentDepth' => AsyncInfo.getCurrentQueueableStackDepth(),
                    'maximumDepth' => AsyncInfo.getMaximumQueueableStackDepth()
                }
            )
        );

        if (depth >= targetDepth) {
            return;
        }

        System.enqueueJob(new QueuesE2EQueueableStackDepthProbe(runKey, depth + 1, targetDepth));
    }
}
