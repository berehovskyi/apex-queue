/**
 * @description Maintenance task that runs job scheduler maintenance with a fresh governor budget.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
public without sharing class QueuesJobSchedulerMaintenance implements QueuesMaintenanceWorker.Task {
    private static final String COMPONENT = 'job scheduler maintenance';
    @TestVisible
    private static Boolean queueableSubmissionHeadroomOverride;

    public QueuesJobSchedulerMaintenance() {
    }

    // <editor-fold desc="Execution">
    public void run(final QueuesService queueService) {
        queueService.runJobSchedulerMaintenance();
        if (canSubmitTerminalCleanupMaintenance()) {
            submitTerminalCleanupMaintenance();
        }
    }

    public String component() {
        return COMPONENT;
    }
    // </editor-fold>

    @TestVisible
    private static Boolean canSubmitTerminalCleanupMaintenance() {
        return hasQueueableSubmissionHeadroom() && !QueuesQueueableStackSupport.shouldBridge();
    }

    private static Boolean hasQueueableSubmissionHeadroom() {
        return queueableSubmissionHeadroomOverride ?? QueuesSupport.hasQueueableSubmissionSlot();
    }

    private static void submitTerminalCleanupMaintenance() {
        try {
            System.enqueueJob(new QueuesMaintenanceWorker(new QueuesTerminalCleanupMaintenance()));
        } catch (System.AsyncException ex) {
            QueuesSupport.debugError(
                'Skipped terminal cleanup maintenance enqueue: ' + QueuesSupport.resolveExceptionReason(ex)
            );
        }
    }
}
