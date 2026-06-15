/**
 * @description Schedulable that materializes due scheduled jobs into Job records.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesJobSchedulerMaterializer implements Schedulable {
    private final Id jobSchedulerRecordId;
    private final QueuesJobSchedulerService schedulerService;

    public QueuesJobSchedulerMaterializer(final Id jobSchedulerRecordId) {
        this(jobSchedulerRecordId, new QueuesJobSchedulerService());
    }

    @TestVisible
    private QueuesJobSchedulerMaterializer(
        final Id jobSchedulerRecordId,
        final QueuesJobSchedulerService schedulerService
    ) {
        this.jobSchedulerRecordId = jobSchedulerRecordId;
        this.schedulerService = schedulerService;
    }

    public void execute(final SchedulableContext ctx) {
        schedulerService.materializeJobSchedulerAndRecordError(jobSchedulerRecordId, ctx.getTriggerId());
    }
}
