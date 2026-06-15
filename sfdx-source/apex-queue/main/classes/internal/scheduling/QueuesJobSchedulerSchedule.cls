/**
 * @description Manages native cron scheduling for job schedulers.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesJobSchedulerSchedule {
    @TestVisible
    private static final String EVERY_JOB_NAME_PREFIX = 'Apex Queue Every Scheduler ';
    @TestVisible
    private static final String PATTERN_JOB_NAME_PREFIX = 'Apex Queue Pattern Scheduler ';
    @TestVisible
    private static final String TEST_JOB_NAME_TOKEN = 'Test ';

    // <editor-fold desc="Name Resolution">
    public static String resolveEveryJobNamePrefix(final Id jobSchedulerRecordId) {
        return resolveJobNamePrefix(EVERY_JOB_NAME_PREFIX, jobSchedulerRecordId);
    }

    public static String resolvePatternJobNamePrefix(final Id jobSchedulerRecordId) {
        return resolveJobNamePrefix(PATTERN_JOB_NAME_PREFIX, jobSchedulerRecordId);
    }

    public static String resolveEveryJobNamePrefix() {
        return resolveBaseJobNamePrefix(EVERY_JOB_NAME_PREFIX);
    }

    public static String resolvePatternJobNamePrefix() {
        return resolveBaseJobNamePrefix(PATTERN_JOB_NAME_PREFIX);
    }
    // </editor-fold>

    // <editor-fold desc="Scheduling">
    public Id schedulePattern(final Id jobSchedulerRecordId, final String cronExpression) {
        QueuesSupport.notNull(jobSchedulerRecordId, 'jobSchedulerRecordId');
        QueuesSupport.notBlank(cronExpression, 'cronExpression');

        return System.schedule(
            resolvePatternJobName(jobSchedulerRecordId),
            cronExpression,
            new QueuesJobSchedulerMaterializer(jobSchedulerRecordId)
        );
    }

    public Id scheduleEvery(final Id jobSchedulerRecordId, final Datetime runAt) {
        QueuesSupport.notNull(jobSchedulerRecordId, 'jobSchedulerRecordId');
        QueuesSupport.notNull(runAt, 'runAt');

        return System.schedule(
            resolveEveryJobName(jobSchedulerRecordId),
            buildOneShotCronExpression(runAt),
            new QueuesJobSchedulerMaterializer(jobSchedulerRecordId)
        );
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    @TestVisible
    private static String buildOneShotCronExpression(final Datetime runAt) {
        QueuesSupport.notNull(runAt, 'runAt');
        return runAt.second() +
            ' ' +
            runAt.minute() +
            ' ' +
            runAt.hour() +
            ' ' +
            runAt.day() +
            ' ' +
            runAt.month() +
            ' ? ' +
            runAt.year();
    }

    private static String resolveEveryJobName(final Id jobSchedulerRecordId) {
        return resolveEveryJobNamePrefix(jobSchedulerRecordId) + System.now().getTime();
    }

    private static String resolvePatternJobName(final Id jobSchedulerRecordId) {
        return resolvePatternJobNamePrefix(jobSchedulerRecordId) + System.now().getTime();
    }

    private static String resolveJobNamePrefix(final String prefix, final Id jobSchedulerRecordId) {
        QueuesSupport.notNull(jobSchedulerRecordId, 'jobSchedulerRecordId');
        return resolveBaseJobNamePrefix(prefix) + jobSchedulerRecordId + ' ';
    }

    private static String resolveBaseJobNamePrefix(final String prefix) {
        return Test.isRunningTest() ? prefix + TEST_JOB_NAME_TOKEN : prefix;
    }
    // </editor-fold>
}
