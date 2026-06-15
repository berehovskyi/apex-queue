/**
 * @description Manages the maintenance cron job lifecycle (schedule, reschedule, unschedule).
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public virtual inherited sharing class QueuesMaintenanceSchedule {
    public static final String JOB_NAME = 'Apex Queue Maintenance';
    public static final String DEFAULT_CRON_EXPRESSION = '0 0 * * * ?';
    @TestVisible
    private static final String TEST_JOB_NAME = JOB_NAME + ' Test';
    private static final Set<String> TERMINAL_JOB_STATES = new Set<String>{ 'DELETED', 'COMPLETE' };

    private QueuesScheduleRepository repository = new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE);

    public QueuesMaintenanceSchedule() {
    }

    @TestVisible
    protected QueuesMaintenanceSchedule(final QueuesScheduleRepository repository) {
        this.repository = repository;
    }

    // <editor-fold desc="Scheduling">
    public Id schedule() {
        return schedule(DEFAULT_CRON_EXPRESSION);
    }

    public Id schedule(final String cronExpression) {
        QueuesSupport.notBlank(cronExpression, 'cronExpression');

        Id scheduledJobId = findScheduledJobId();
        if (scheduledJobId != null) {
            return scheduledJobId;
        }

        try {
            return scheduleNew(cronExpression);
        } catch (Exception ex) {
            // Another transaction may have created the maintenance job after the
            // initial lookup but before this transaction got to schedule it.
            scheduledJobId = findScheduledJobId();
            if (scheduledJobId != null) {
                return scheduledJobId;
            }

            throw ex;
        }
    }

    public Id reschedule(final String cronExpression) {
        QueuesSupport.notBlank(cronExpression, 'cronExpression');
        unschedule();
        return scheduleNew(cronExpression);
    }

    public Integer unschedule() {
        Integer abortedCount = 0;

        for (CronTrigger scheduledJob : queryScheduledJobs()) {
            try {
                abortJob(scheduledJob.Id);
                abortedCount++;
            } catch (Exception ex) {
                QueuesSupport.debugError(
                    'Failed to abort maintenance schedule job ' +
                        scheduledJob.Id +
                        ': ' +
                        ex.getMessage() +
                        '\n' +
                        ex.getStackTraceString()
                );
            }
        }

        return abortedCount;
    }

    public Boolean isScheduled() {
        return findScheduledJobId() != null;
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    @TestVisible
    protected virtual Id scheduleNew(final String cronExpression) {
        return System.schedule(resolveJobName(), cronExpression, new QueuesMaintenanceScheduler());
    }

    @TestVisible
    protected virtual void abortJob(final Id jobId) {
        repository.abortJob(jobId);
    }

    @TestVisible
    private static String resolveJobName() {
        return Test.isRunningTest() ? TEST_JOB_NAME : JOB_NAME;
    }

    private List<CronTrigger> queryScheduledJobs() {
        return repository.queryScheduledJobs(resolveJobName(), TERMINAL_JOB_STATES);
    }

    private Id findScheduledJobId() {
        List<CronTrigger> scheduledJobs = queryScheduledJobs();
        return scheduledJobs.isEmpty() ? null : scheduledJobs[0].Id;
    }
    // </editor-fold>
}
