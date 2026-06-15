/**
 * @description Recovers stalled active jobs that have exceeded their lease.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesStalledJobRecoveryService {
    private static final Integer RECOVER_STALLED_BATCH_SIZE = 1000;
    private static final Integer RECOVERY_DML_ROW_HEADROOM = 500;
    private static final Integer RECOVERY_DML_STATEMENT_HEADROOM = 10;
    private static final Integer RECOVERY_QUERY_HEADROOM = 5;
    private static final Integer RECOVERY_CPU_MS_HEADROOM = 2000;

    private QueuesJobService jobService = new QueuesJobService();
    private QueuesJobRunService jobRunService = new QueuesJobRunService();

    public QueuesStalledJobRecoveryService() {
    }

    @TestVisible
    private QueuesStalledJobRecoveryService(
        final QueuesJobService jobService,
        final QueuesJobRunService jobRunService
    ) {
        this.jobService = jobService;
        this.jobRunService = jobRunService;
    }

    // <editor-fold desc="Recovery">
    public Boolean hasHeadroomForRecovery() {
        QueuesSupport.GovernorHeadroom headroom = QueuesSupport.currentGovernorHeadroom();
        return hasHeadroomForRecovery(headroom.dmlRows, headroom.dmlStatements, headroom.queries, headroom.cpuMs);
    }

    public Integer resolveRecoverStalledBatchSize() {
        return resolveRecoverStalledBatchSize(Limits.getLimitDmlRows() - Limits.getDmlRows());
    }

    public JobRun__c recoverStalledJob(
        final Job__c job,
        final Datetime recoveredAt,
        final String stalledMessage,
        final String exhaustedMessage
    ) {
        QueuesSupport.notNull(job, 'job');
        QueuesSupport.notNull(recoveredAt, 'recoveredAt');

        jobService.clearLease(job);
        job.ReturnValue__c = null;

        Integer attemptsMade = QueuesSupport.toInteger(job.AttemptsMade__c, 0);
        Integer maxAttempts = QueuesSupport.toPositiveInteger(job.MaxAttempts__c, 1);
        Integer stalledCount = QueuesSupport.toInteger(job.StalledCount__c, 0) + 1;
        job.StalledCount__c = stalledCount;

        Boolean hasRecoveryAttemptsRemaining = attemptsMade < maxAttempts && stalledCount < maxAttempts;
        String resolvedMessage = hasRecoveryAttemptsRemaining ? stalledMessage : exhaustedMessage;
        if (hasRecoveryAttemptsRemaining) {
            job.State__c = Queues.State.WAITING.name();
            job.AvailableAt__c = recoveredAt;
            job.FailedReason__c = null;
        } else {
            job.State__c = Queues.State.FAILED.name();
            job.AvailableAt__c = recoveredAt;
            job.FailedReason__c = exhaustedMessage;
        }

        Datetime startedAt = job.LastModifiedDate ?? recoveredAt;
        JobRun__c run = new JobRun__c(
            Job__c = job.Id,
            AttemptNumber__c = Math.max(attemptsMade, 1),
            StartedAt__c = startedAt,
            Status__c = Queues.RunStatus.FAILED.name(),
            Message__c = resolvedMessage
        );
        jobRunService.addTimestamps(run, startedAt.getTime(), recoveredAt);
        return run;
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    @TestVisible
    private static Boolean hasHeadroomForRecovery(
        final Integer remainingDmlRows,
        final Integer remainingDmlStatements,
        final Integer remainingQueries,
        final Integer remainingCpuMs
    ) {
        return remainingDmlRows >= RECOVERY_DML_ROW_HEADROOM &&
            remainingDmlStatements >= RECOVERY_DML_STATEMENT_HEADROOM &&
            remainingQueries >= RECOVERY_QUERY_HEADROOM &&
            remainingCpuMs >= RECOVERY_CPU_MS_HEADROOM;
    }

    @TestVisible
    private static Integer resolveRecoverStalledBatchSize(final Integer remainingDmlRows) {
        Integer availableDmlRows = remainingDmlRows - RECOVERY_DML_ROW_HEADROOM;
        if (availableDmlRows < 2) {
            return 0;
        }
        return Math.min(RECOVER_STALLED_BATCH_SIZE, availableDmlRows / 2);
    }
    // </editor-fold>
}
