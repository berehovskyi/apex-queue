/**
 * @description Queueable entry point that executes one maintenance task with a fresh governor budget.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
public without sharing class QueuesMaintenanceWorker implements Queueable, Finalizer {
    private static final String MAINTENANCE_QUEUE_NAME = '__maintenance__';
    private static final String UNKNOWN_MAINTENANCE_COMPONENT = 'maintenance';

    private final QueuesService queueService;
    private final QueuesErrorService errorService;
    private final QueuesMaintenanceWorker.Task task;
    private final Boolean shouldAttachFinalizer;

    public QueuesMaintenanceWorker(final QueuesMaintenanceWorker.Task task) {
        this(new QueuesService(), new QueuesErrorService(), task, true);
    }

    @TestVisible
    private QueuesMaintenanceWorker(
        final QueuesService queueService,
        final QueuesErrorService errorService,
        final QueuesMaintenanceWorker.Task task,
        final Boolean shouldAttachFinalizer
    ) {
        QueuesSupport.notNull(task, 'task');
        this.queueService = queueService;
        this.errorService = errorService;
        this.task = task;
        this.shouldAttachFinalizer = shouldAttachFinalizer ?? false;
    }

    // <editor-fold desc="Execution">
    public void execute(final QueueableContext ctx) {
        if (shouldAttachFinalizer) {
            System.attachFinalizer(this);
        }
        task.run(queueService);
    }

    public void execute(final FinalizerContext ctx) {
        if (ctx.getResult() == ParentJobResult.SUCCESS) {
            return;
        }

        recordFinalizerFailure(String.valueOf(ctx.getAsyncApexJobId()), ctx.getException());
    }
    // </editor-fold>

    @TestVisible
    private void recordFinalizerFailure(final String asyncApexJobId, final Exception failure) {
        try {
            errorService.recordQueueError(MAINTENANCE_QUEUE_NAME, resolveComponent(), null, asyncApexJobId, failure);
        } catch (Exception ex) {
            QueuesSupport.debugError(
                String.format(
                    'Maintenance finalizer failed to record queue error.\n{0}\n{1}',
                    new List<String>{ ex.getMessage(), ex.getStackTraceString() }
                )
            );
        }
    }

    private String resolveComponent() {
        String component = task.component();
        return String.isBlank(component) ? UNKNOWN_MAINTENANCE_COMPONENT : component;
    }

    public interface Task {
        void run(QueuesService queueService);
        String component();
    }
}
