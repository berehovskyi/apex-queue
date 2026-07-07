/**
 * @description Administrative job operations: pause, resume, retry, cancel, defer, remove.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesJobAdminService {
    private static final String INVALID_PAUSE_MESSAGE = 'Only waiting, delayed, active, or paused jobs can be paused.';
    private static final String INVALID_RESUME_MESSAGE = 'Only paused jobs can be resumed.';
    private static final String INVALID_PROMOTE_MESSAGE = 'Only delayed jobs can be promoted.';
    private static final String INVALID_RETRY_MESSAGE = 'Only failed, completed, or paused jobs can be retried.';
    private static final String INVALID_REMOVE_MESSAGE = 'Active jobs cannot be removed.';
    private static final String INVALID_DEFER_MESSAGE = 'Only waiting, delayed, or paused jobs can be deferred.';
    private static final String INVALID_CHANGE_PRIORITY_MESSAGE = 'Only waiting, delayed, or paused jobs can change priority.';
    private static final String INVALID_UPDATE_DATA_MESSAGE = 'Active jobs cannot update data.';
    private static final String INVALID_UPDATE_PROGRESS_MESSAGE = 'Active jobs cannot update progress.';
    private static final String INVALID_DISCARD_MESSAGE = 'Active jobs cannot be discarded.';
    private static final String INVALID_CANCEL_MESSAGE = 'Only waiting, delayed, paused, or waiting-children jobs can be canceled.';
    private static final String INVALID_DELAY_MINUTES_MESSAGE = 'delayMinutes must be zero or greater';
    private static final String INVALID_PRIORITY_MESSAGE = 'priority must be zero or greater';
    private static final String CANCELED_BY_JOB_CANCEL_REASON = 'Canceled by job cancel.';

    private static final Set<Queues.State> PAUSEABLE_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED,
        Queues.State.ACTIVE,
        Queues.State.PAUSED
    };
    private static final Set<Queues.State> DEFERABLE_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED,
        Queues.State.PAUSED
    };
    private static final Set<Queues.State> RETRYABLE_JOB_STATES = new Set<Queues.State>{
        Queues.State.FAILED,
        Queues.State.COMPLETED,
        Queues.State.PAUSED
    };
    private static final Set<Queues.State> CHANGE_PRIORITY_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED,
        Queues.State.PAUSED
    };
    private static final Set<Queues.State> CANCELABLE_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED,
        Queues.State.PAUSED,
        Queues.State.WAITING_CHILDREN
    };

    private QueuesJobService jobService = new QueuesJobService();
    private QueuesRuntimeService runtimeService = new QueuesRuntimeService();
    private QueuesQueueableExecutionService queueableExecutionService = new QueuesQueueableExecutionService();
    private QueuesFlowProducerService flowProducerService = new QueuesFlowProducerService();
    private QueuesWorkerService workerService = new QueuesWorkerService();
    private QueuesQueueEventService queueEventService = new QueuesQueueEventService();

    public QueuesJobAdminService() {
    }

    @TestVisible
    private QueuesJobAdminService(
        final QueuesJobService jobService,
        final QueuesRuntimeService runtimeService,
        final QueuesQueueableExecutionService queueableExecutionService,
        final QueuesFlowProducerService flowProducerService,
        final QueuesWorkerService workerService,
        final QueuesQueueEventService queueEventService
    ) {
        this.jobService = jobService;
        this.runtimeService = runtimeService;
        this.queueableExecutionService = queueableExecutionService;
        this.flowProducerService = flowProducerService;
        this.workerService = workerService;
        this.queueEventService = queueEventService;
    }

    // <editor-fold desc="Commands">
    public Queues.State getJobState(final Id jobId) {
        return QueuesSupport.toState(jobService.requireJob(jobId).State__c);
    }

    public Job__c pauseJob(final Id jobId) {
        lockQueueRuntimeForJob(jobId);
        Job__c job = jobService.requireJobForUpdate(jobId);
        Queues.State currentState = QueuesSupport.toState(job.State__c);
        QueuesSupport.isTrue(
            PAUSEABLE_JOB_STATES.contains(currentState),
            new Queues.InvalidOperationException(INVALID_PAUSE_MESSAGE)
        );

        if (currentState == Queues.State.ACTIVE) {
            job.PauseRequested__c = true;
            return jobService.updateJob(job);
        }

        Datetime currentNow = QueuesSupport.now();
        job.State__c = Queues.State.PAUSED.name();
        job.PauseRequested__c = false;
        job = jobService.updateJob(job);
        if (isWorkerJob(job)) {
            workerService.syncWorkerForNextWakeAt(job.QueueName__c, currentNow);
        }
        return job;
    }

    public Job__c resumeJob(final Id jobId) {
        lockQueueRuntimeForJob(jobId);
        Job__c job = jobService.requireJobForUpdate(jobId);
        QueuesSupport.isTrue(
            QueuesSupport.toState(job.State__c) == Queues.State.PAUSED,
            new Queues.InvalidOperationException(INVALID_RESUME_MESSAGE)
        );

        Datetime currentNow = QueuesSupport.now();
        job.State__c = job.AvailableAt__c > currentNow ? Queues.State.DELAYED.name() : Queues.State.WAITING.name();
        job.PauseRequested__c = false;
        job = jobService.updateJob(job);
        dispatchRunnableJob(job, currentNow, false);
        return job;
    }

    public Job__c retryJob(final Id jobId) {
        lockQueueRuntimeForJob(jobId);
        Job__c job = jobService.requireJobForUpdate(jobId);
        QueuesSupport.isTrue(
            RETRYABLE_JOB_STATES.contains(QueuesSupport.toState(job.State__c)),
            new Queues.InvalidOperationException(INVALID_RETRY_MESSAGE)
        );

        Datetime currentNow = QueuesSupport.now();
        job.State__c = Queues.State.WAITING.name();
        job.AvailableAt__c = currentNow;
        job.AttemptsMade__c = 0;
        job.FailedReason__c = null;
        job.ReturnValue__c = null;
        job.Checkpoint__c = null;
        job.Progress__c = null;
        jobService.clearLease(job);
        job.PauseRequested__c = false;
        job = jobService.updateJob(job);
        dispatchRunnableJob(job, currentNow, false);
        return job;
    }

    public Job__c promoteJob(final Id jobId) {
        lockQueueRuntimeForJob(jobId);
        Job__c job = jobService.requireJobForUpdate(jobId);
        QueuesSupport.isTrue(
            QueuesSupport.toState(job.State__c) == Queues.State.DELAYED,
            new Queues.InvalidOperationException(INVALID_PROMOTE_MESSAGE)
        );

        Datetime currentNow = QueuesSupport.now();
        job.State__c = Queues.State.WAITING.name();
        job.AvailableAt__c = currentNow;
        job = jobService.updateJob(job);
        dispatchRunnableJob(job, currentNow, true);
        return job;
    }

    public Job__c deferJob(final Id jobId, final Integer delayMinutes) {
        QueuesSupport.isTrue(delayMinutes >= 0, INVALID_DELAY_MINUTES_MESSAGE);

        lockQueueRuntimeForJob(jobId);
        Job__c job = jobService.requireJobForUpdate(jobId);
        QueuesSupport.isTrue(
            DEFERABLE_JOB_STATES.contains(QueuesSupport.toState(job.State__c)),
            new Queues.InvalidOperationException(INVALID_DEFER_MESSAGE)
        );
        if (isQueueableJob(job)) {
            QueuesQueueableTransportService.requireValidNativeQueueableDelayMinutes(delayMinutes);
        }

        Datetime currentNow = QueuesSupport.now();
        job.AvailableAt__c = currentNow.addMinutes(delayMinutes);
        job.State__c = delayMinutes > 0 ? Queues.State.DELAYED.name() : Queues.State.WAITING.name();
        job.PauseRequested__c = false;
        job = jobService.updateJob(job);
        dispatchDeferredJob(job, currentNow);
        return job;
    }

    public Job__c discardJob(final Id jobId) {
        lockQueueRuntimeForJob(jobId);
        Job__c job = jobService.requireJobForUpdate(jobId);
        Queues.State currentState = QueuesSupport.toState(job.State__c);
        QueuesSupport.isTrue(
            currentState != Queues.State.ACTIVE,
            new Queues.InvalidOperationException(INVALID_DISCARD_MESSAGE)
        );
        Integer attemptsMade = QueuesSupport.toInteger(job.AttemptsMade__c, 0);
        job.MaxAttempts__c = Math.max(attemptsMade, 1);
        return jobService.updateJob(job);
    }

    public Job__c cancelJob(final Id jobId) {
        String lockedRuntimeQueueName = lockQueueRuntimeForJob(jobId);
        Job__c job = jobService.requireJobForUpdate(jobId);
        Queues.State currentState = QueuesSupport.toState(job.State__c);
        QueuesSupport.isTrue(
            CANCELABLE_JOB_STATES.contains(currentState),
            new Queues.InvalidOperationException(INVALID_CANCEL_MESSAGE)
        );

        Datetime currentNow = QueuesSupport.now();
        QueuesJobService.applyCanceled(job, CANCELED_BY_JOB_CANCEL_REASON);
        job = jobService.updateJob(job);
        queueEventService.publishCanceledJobEvents(new List<Job__c>{ job });
        flowProducerService.resolveParentDependenciesAsSystem(
            new List<Job__c>{ job },
            new Set<String>(),
            new Set<String>{ lockedRuntimeQueueName }
        );
        if (isWorkerJob(job) && (currentState == Queues.State.WAITING || currentState == Queues.State.DELAYED)) {
            workerService.syncExistingWorkerForNextWakeAt(job.QueueName__c, currentNow);
        }
        return job;
    }

    public Job__c changePriority(final Id jobId, final Integer priority) {
        QueuesSupport.isTrue(priority >= 0, INVALID_PRIORITY_MESSAGE);

        lockQueueRuntimeForJob(jobId);
        Job__c job = jobService.requireJobForUpdate(jobId);
        QueuesSupport.isTrue(
            CHANGE_PRIORITY_JOB_STATES.contains(QueuesSupport.toState(job.State__c)),
            new Queues.InvalidOperationException(INVALID_CHANGE_PRIORITY_MESSAGE)
        );

        Datetime currentNow = QueuesSupport.now();
        job.Priority__c = priority;
        job = jobService.updateJob(job);
        if (isWorkerJob(job)) {
            workerService.ensureWorkerForNextWakeAt(job.QueueName__c, currentNow);
        }
        return job;
    }

    public Job__c updateJobData(final Id jobId, final Object data) {
        lockQueueRuntimeForJob(jobId);
        Job__c job = jobService.requireJobForUpdate(jobId);
        QueuesSupport.isTrue(
            QueuesSupport.toState(job.State__c) != Queues.State.ACTIVE,
            new Queues.InvalidOperationException(INVALID_UPDATE_DATA_MESSAGE)
        );

        job.Data__c = QueuesSupport.serialize(data);
        return jobService.updateJob(job);
    }

    public Job__c updateJobProgress(final Id jobId, final Object progress) {
        lockQueueRuntimeForJob(jobId);
        Job__c job = jobService.requireJobForUpdate(jobId);
        QueuesSupport.isTrue(
            QueuesSupport.toState(job.State__c) != Queues.State.ACTIVE,
            new Queues.InvalidOperationException(INVALID_UPDATE_PROGRESS_MESSAGE)
        );

        job.Progress__c = QueuesSupport.serialize(progress);
        return jobService.updateJob(job);
    }

    public void removeJob(final Id jobId) {
        String lockedRuntimeQueueName = lockQueueRuntimeForJob(jobId);
        Job__c job = jobService.requireJobForUpdate(jobId);
        Queues.State currentState = QueuesSupport.toState(job.State__c);
        QueuesSupport.isTrue(
            currentState != Queues.State.ACTIVE,
            new Queues.InvalidOperationException(INVALID_REMOVE_MESSAGE)
        );

        jobService.deleteJobs(new List<Job__c>{ job });
        flowProducerService.resolveRemovedParentDependenciesAsSystem(
            new List<Job__c>{ job },
            new Set<String>(),
            new Set<String>{ lockedRuntimeQueueName }
        );
        if (isWorkerJob(job) && (currentState == Queues.State.WAITING || currentState == Queues.State.DELAYED)) {
            workerService.syncExistingWorkerForNextWakeAt(job.QueueName__c, QueuesSupport.now());
        }
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private static Boolean isQueueableJob(final Job__c job) {
        return QueuesSupport.isQueueableExecutionMode(QueuesSupport.toExecutionMode(job.ExecutionMode__c));
    }

    private static Boolean isInvocableJob(final Job__c job) {
        return QueuesSupport.isInvocableExecutionMode(QueuesSupport.toExecutionMode(job.ExecutionMode__c));
    }

    private static Boolean isWorkerJob(final Job__c job) {
        return QueuesSupport.toExecutionMode(job.ExecutionMode__c) == Queues.ExecutionMode.WORKER;
    }

    private String lockQueueRuntimeForJob(final Id jobId) {
        String queueName = jobService.requireJob(jobId).QueueName__c;
        runtimeService.ensureRuntimeAsSystem(queueName);
        runtimeService.lockRuntimeAsSystem(queueName);
        return queueName;
    }

    private void dispatchRunnableJob(final Job__c job, final Datetime currentNow, final Boolean forceImmediateWorker) {
        if (isQueueableJob(job)) {
            queueableExecutionService.enqueueJob(job.Id);
            return;
        }
        if (isInvocableJob(job)) {
            return;
        }

        if (forceImmediateWorker == true) {
            workerService.ensureWorker(job.QueueName__c, currentNow);
            return;
        }

        workerService.ensureWorkerForNextWakeAt(job.QueueName__c, currentNow);
    }

    private void dispatchDeferredJob(final Job__c job, final Datetime currentNow) {
        if (isQueueableJob(job)) {
            queueableExecutionService.enqueueJob(job.Id);
            return;
        }
        if (isInvocableJob(job)) {
            return;
        }

        workerService.syncWorkerForNextWakeAt(job.QueueName__c, currentNow);
    }
    // </editor-fold>
}
