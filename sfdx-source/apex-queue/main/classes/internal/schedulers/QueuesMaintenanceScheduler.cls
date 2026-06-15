/**
 * @description Schedulable entry point that runs runtime maintenance and submits scheduler maintenance.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesMaintenanceScheduler implements Schedulable {
    private QueuesService queueService = new QueuesService();

    public QueuesMaintenanceScheduler() {
    }

    @TestVisible
    private QueuesMaintenanceScheduler(final QueuesService queueService) {
        this.queueService = queueService;
    }

    public void execute(final SchedulableContext ctx) {
        System.enqueueJob(new QueuesJobSchedulerMaintenance());
        queueService.runRuntimeMaintenance();
    }
}
