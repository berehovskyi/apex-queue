/**
 * @description Executes queueable jobs and bridges native stack depth.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public virtual inherited sharing class QueuesQueueableExecutionService extends QueuesQueueableTransportService {
    private static final Set<Queues.State> RUNNABLE_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED
    };
    private QueuesJobExecutionService jobExecutionService = new QueuesJobExecutionService();
    private QueuesJobService jobService = new QueuesJobService();
    private QueuesQueueEventService queueEventService = new QueuesQueueEventService();

    public QueuesQueueableExecutionService() {
    }

    @TestVisible
    protected QueuesQueueableExecutionService(
        final QueuesJobExecutionService jobExecutionService,
        final QueuesJobService jobService,
        final QueuesQueueEventService queueEventService
    ) {
        this.jobExecutionService = jobExecutionService;
        this.jobService = jobService;
        this.queueEventService = queueEventService;
    }

    // <editor-fold desc="Future Bridges">
    @SuppressWarnings('PMD.AvoidFutureAnnotation')
    @Future
    public static void continueRetryFromFuture(final Id jobId) {
        if (String.isBlank(jobId)) {
            return;
        }

        new QueuesQueueableExecutionService().continueRetry(jobId);
    }
    // </editor-fold>

    // <editor-fold desc="Queueables">
    public virtual Job__c execute(final Id jobId, final String asyncApexJobId) {
        Job__c job = jobExecutionService.executeQueueableJob(jobId, asyncApexJobId);
        return enqueueNextAttemptIfRunnable(job);
    }

    public virtual Job__c handleFailure(final Id jobId, final String asyncApexJobId, final String failedReason) {
        Job__c job = jobExecutionService.failQueueableJob(jobId, asyncApexJobId, failedReason);
        return enqueueNextAttemptIfRunnable(job);
    }

    public virtual Job__c enqueueJob(final Id jobId) {
        QueuesSupport.notNull(jobId, 'jobId');

        Job__c currentJob = jobService.requireJobForUpdateAsSystem(jobId);
        if (!isRunnableQueueableJob(currentJob)) {
            return currentJob;
        }

        return enqueueNextAttempt(currentJob, false);
    }
    // </editor-fold>

    // <editor-fold desc="Hooks">
    protected virtual void enqueueRetryFromFutureBridge(final Id jobId) {
        continueRetryFromFuture(jobId);
    }

    @TestVisible
    protected virtual Job__c continueRetry(final Id jobId) {
        QueuesSupport.notNull(jobId, 'jobId');

        return enqueueJob(jobId);
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private static Boolean isRunnableQueueableJob(final Job__c job) {
        return job != null &&
            QueuesSupport.isQueueableExecutionMode(QueuesSupport.toExecutionMode(job.ExecutionMode__c)) &&
            RUNNABLE_JOB_STATES.contains(QueuesSupport.toState(job.State__c));
    }

    private Job__c enqueueNextAttemptIfRunnable(final Job__c job) {
        if (!isRunnableQueueableJob(job)) {
            return job;
        }

        Job__c currentJob = jobService.requireJobForUpdateAsSystem(job.Id);
        if (!isRunnableQueueableJob(currentJob)) {
            return currentJob;
        }

        return enqueueNextAttempt(currentJob, true);
    }

    private Job__c enqueueNextAttempt(final Job__c currentJob, final Boolean allowFutureBridge) {
        Datetime now = QueuesSupport.now();
        currentJob.QueueableDispatchInitiatedAt__c = null;
        QueuesQueueableTransportService.QueueableSubmission submission = prepareExecutorSubmission(currentJob, now);
        if (submission.isInvalidDelay()) {
            markInvalidQueueableDelay(currentJob, submission, now);
            return updateFailedJob(currentJob);
        }
        if (allowFutureBridge == true && shouldBridgeQueueableStack()) {
            return bridgeNextAttemptFromFuture(currentJob, now);
        }

        submission = submitExecutor(currentJob, submission);
        if (submission.hasEnqueueException()) {
            if (allowFutureBridge == true && submission.hasAsyncEnqueueException()) {
                return bridgeNextAttemptFromFuture(currentJob, now);
            }
            return failRetryEnqueue(currentJob, now, submission.enqueueException);
        }
        markQueueableExecutorSubmitted(currentJob, submission);
        currentJob.QueueableDispatchInitiatedAt__c = now;
        Job__c updatedJob = jobService.updateJobAsSystem(currentJob);
        queueEventService.publishActiveJobEvents(new List<Job__c>{ updatedJob });
        return updatedJob;
    }

    private Job__c bridgeNextAttemptFromFuture(final Job__c currentJob, final Datetime now) {
        try {
            enqueueRetryFromFutureBridge(currentJob.Id);
        } catch (Exception ex) {
            return failRetryEnqueue(currentJob, now, ex);
        }

        currentJob.QueueableDispatchInitiatedAt__c = now;
        return jobService.updateJobAsSystem(currentJob);
    }

    private Job__c failRetryEnqueue(final Job__c currentJob, final Datetime now, final Exception ex) {
        markQueueableSubmissionFailed(
            currentJob,
            'Queueable retry enqueue failed: ' + QueuesSupport.resolveExceptionReason(ex),
            now
        );
        return updateFailedJob(currentJob);
    }

    private Job__c updateFailedJob(final Job__c job) {
        Job__c updatedJob = jobService.updateJobAsSystem(job);
        queueEventService.publishTerminalJobEvents(new List<Job__c>{ updatedJob }, null);
        return updatedJob;
    }
    // </editor-fold>
}
