/**
 * @description Maintenance task that runs terminal job cleanup after recovery maintenance.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesTerminalCleanupMaintenance implements QueuesMaintenanceWorker.Task {
    private static final String COMPONENT = 'terminal cleanup maintenance';

    public QueuesTerminalCleanupMaintenance() {
    }

    // <editor-fold desc="Execution">
    public void run(final QueuesService queueService) {
        queueService.runTerminalCleanupMaintenance();
    }

    public String component() {
        return COMPONENT;
    }
    // </editor-fold>
}
