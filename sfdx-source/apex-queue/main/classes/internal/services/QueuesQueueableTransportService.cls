/**
 * @description Shared queueable transport: delay validation, submission, and stack-depth support.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public abstract inherited sharing class QueuesQueueableTransportService {
    public static final Integer NATIVE_MAX_QUEUEABLE_DELAY_MINUTES = 10;
    public static final String NATIVE_INVALID_QUEUEABLE_DELAY_MESSAGE = 'Queueable delay must be between 0 and 10 minutes. Actual: {0}';

    private static final Long MILLIS_PER_MINUTE = 60 * 1000;

    // <editor-fold desc="Queueable Utilities">
    public static Integer resolveNativeQueueableDelayMinutes(final Datetime availableAt, final Datetime now) {
        if (availableAt == null || availableAt <= now) {
            return 0;
        }

        Long delayMillis = availableAt.getTime() - now.getTime();
        Long delayMinutes = (delayMillis + MILLIS_PER_MINUTE - 1) / MILLIS_PER_MINUTE;
        return QueuesSupport.toInteger(delayMinutes, 0);
    }

    public static void requireValidNativeQueueableDelayMinutes(final Integer delayMinutes) {
        QueuesSupport.isTrue(
            isValidNativeQueueableDelayMinutes(delayMinutes),
            new Queues.QueueException(nativeQueueableDelayMessage(delayMinutes))
        );
    }

    public static Boolean isValidNativeQueueableDelayMinutes(final Integer delayMinutes) {
        Integer safeDelayMinutes = delayMinutes ?? 0;
        return safeDelayMinutes >= 0 && safeDelayMinutes <= NATIVE_MAX_QUEUEABLE_DELAY_MINUTES;
    }
    // </editor-fold>

    // <editor-fold desc="Hooks">
    protected virtual Id enqueueExecutor(final Id jobId, final Integer delayMinutes) {
        return System.enqueueJob(new QueuesQueueableExecutor(jobId), delayMinutes);
    }

    protected QueueableSubmission submitExecutor(final Job__c job, final Datetime now) {
        return submitExecutor(job, prepareExecutorSubmission(job, now));
    }

    protected QueueableSubmission prepareExecutorSubmission(final Job__c job, final Datetime now) {
        QueueableSubmission submission = new QueueableSubmission(
            resolveNativeQueueableDelayMinutes(job.AvailableAt__c, now)
        );
        if (!isValidNativeQueueableDelayMinutes(submission.delayMinutes)) {
            submission.failedReason = nativeQueueableDelayMessage(submission.delayMinutes);
        }
        return submission;
    }

    protected QueueableSubmission submitExecutor(final Job__c job, final QueueableSubmission submission) {
        if (submission.isInvalidDelay()) {
            return submission;
        }

        try {
            submission.asyncApexJobId = enqueueExecutor(job.Id, submission.delayMinutes);
        } catch (System.AsyncException ex) {
            submission.asyncEnqueueException = true;
            submission.enqueueException = ex;
            submission.failedReason = QueuesSupport.resolveExceptionReason(ex);
        } catch (Exception ex) {
            submission.enqueueException = ex;
            submission.failedReason = QueuesSupport.resolveExceptionReason(ex);
        }
        return submission;
    }

    protected void markInvalidQueueableDelay(
        final Job__c job,
        final QueueableSubmission submission,
        final Datetime now
    ) {
        job.State__c = Queues.State.FAILED.name();
        job.AvailableAt__c = now;
        job.FailedReason__c = submission.failedReason;
    }

    protected void markQueueableExecutorSubmitted(final Job__c job, final QueueableSubmission submission) {
        job.State__c = Queues.State.ACTIVE.name();
        job.AsyncApexJobId__c = submission.asyncApexJobId;
    }

    protected void markQueueableSubmissionFailed(final Job__c job, final String failedReason, final Datetime now) {
        job.State__c = Queues.State.FAILED.name();
        job.AvailableAt__c = now;
        job.AsyncApexJobId__c = null;
        job.FailedReason__c = failedReason;
    }

    protected virtual Boolean shouldBridgeQueueableStack() {
        return QueuesQueueableStackSupport.shouldBridge();
    }

    protected virtual Boolean isQueueableStackContext() {
        return QueuesQueueableStackSupport.isQueueableStackContext();
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    @TestVisible
    private static String nativeQueueableDelayMessage(final Integer delayMinutes) {
        return String.format(NATIVE_INVALID_QUEUEABLE_DELAY_MESSAGE, new List<Integer>{ delayMinutes });
    }

    // </editor-fold>

    public class QueueableSubmission {
        public Id asyncApexJobId;
        private Boolean asyncEnqueueException = false;
        public Integer delayMinutes;
        public Exception enqueueException;
        public String failedReason;

        private QueueableSubmission(final Integer delayMinutes) {
            this.delayMinutes = delayMinutes;
        }

        public Boolean hasEnqueueException() {
            return enqueueException != null;
        }

        public Boolean hasAsyncEnqueueException() {
            return asyncEnqueueException == true;
        }

        public Boolean isInvalidDelay() {
            return enqueueException == null && String.isNotBlank(failedReason);
        }
    }
}
