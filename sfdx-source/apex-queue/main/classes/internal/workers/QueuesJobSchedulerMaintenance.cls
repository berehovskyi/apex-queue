/**
 * @description Queueable entry point that runs job scheduler maintenance with a fresh governor budget.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesJobSchedulerMaintenance implements Queueable {
    private final QueuesService queueService;

    public QueuesJobSchedulerMaintenance() {
        this(new QueuesService());
    }

    @TestVisible
    private QueuesJobSchedulerMaintenance(final QueuesService queueService) {
        this.queueService = queueService;
    }

    // <editor-fold desc="Execution">
    @SuppressWarnings('PMD.QueueableWithoutFinalizer')
    public void execute(final QueueableContext ctx) {
        queueService.runJobSchedulerMaintenance();
    }
    // </editor-fold>
}
