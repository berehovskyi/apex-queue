/**
 * @description Queueable and Finalizer that executes a single job with retry handling.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesQueueableExecutor implements Queueable, Finalizer, Database.AllowsCallouts {
    private final Id jobId;
    private final QueuesQueueableExecutionService service;
    private final Boolean shouldAttachFinalizer;

    public QueuesQueueableExecutor(final Id jobId) {
        this(jobId, new QueuesQueueableExecutionService(), true);
    }

    @TestVisible
    private QueuesQueueableExecutor(
        final Id jobId,
        final QueuesQueueableExecutionService service,
        final Boolean shouldAttachFinalizer
    ) {
        QueuesSupport.notNull(jobId, 'jobId');
        this.jobId = jobId;
        this.service = service;
        this.shouldAttachFinalizer = shouldAttachFinalizer ?? false;
    }

    public void execute(final QueueableContext ctx) {
        if (shouldAttachFinalizer) {
            System.attachFinalizer(this);
        }
        service.execute(jobId, ctx.getJobId());
    }

    public void execute(final FinalizerContext ctx) {
        if (ctx.getResult() == ParentJobResult.SUCCESS) {
            return;
        }

        service.handleFailure(jobId, ctx.getAsyncApexJobId(), QueuesSupport.resolveExceptionReason(ctx.getException()));
    }
}
