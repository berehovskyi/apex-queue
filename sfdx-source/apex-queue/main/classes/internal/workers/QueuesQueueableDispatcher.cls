/**
 * @description Queueable and Finalizer that drives serial/concurrent dispatch progression.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesQueueableDispatcher implements Queueable, Finalizer {
    private final Id dispatchId;
    private final QueuesQueueableDispatchService service;
    private final Boolean shouldAttachFinalizer;

    public QueuesQueueableDispatcher(final Id dispatchId) {
        this(dispatchId, new QueuesQueueableDispatchService(), true);
    }

    @TestVisible
    private QueuesQueueableDispatcher(
        final Id dispatchId,
        final QueuesQueueableDispatchService service,
        final Boolean shouldAttachFinalizer
    ) {
        QueuesSupport.notNull(dispatchId, 'dispatchId');
        this.dispatchId = dispatchId;
        this.service = service;
        this.shouldAttachFinalizer = shouldAttachFinalizer ?? false;
    }

    public void execute(final QueueableContext ctx) {
        if (shouldAttachFinalizer) {
            System.attachFinalizer(this);
        }
        service.startDispatchAsSystem(dispatchId, ctx.getJobId());
    }

    public void execute(final FinalizerContext ctx) {
        if (ctx.getResult() == ParentJobResult.SUCCESS) {
            service.continueDispatch(dispatchId);
            return;
        }

        service.failDispatchAsSystem(dispatchId, QueuesSupport.resolveExceptionReason(ctx.getException()));
    }
}
