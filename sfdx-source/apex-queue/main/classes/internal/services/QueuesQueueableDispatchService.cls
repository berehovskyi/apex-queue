/**
 * @description Dispatches queueable jobs through durable dispatch records.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public virtual inherited sharing class QueuesQueueableDispatchService extends QueuesQueueableTransportService {
    private static final Integer SINGLE_JOB_LIMIT = 1;
    private static final Integer INITIAL_QUEUEABLE_STACK_DEPTH = 5;
    private static final Integer DUPLICATE_SIGNATURE_MAX_BYTES = 32;
    private static final String COMPRESSED_DUPLICATE_SIGNATURE_PREFIX = 'q:';
    private static final Set<Queues.State> DISPATCHABLE_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED
    };
    private static final Set<Queues.QueueableDispatchState> RECOVERABLE_DISPATCH_STATES = new Set<Queues.QueueableDispatchState>{
        Queues.QueueableDispatchState.WAITING,
        Queues.QueueableDispatchState.ACTIVE
    };
    private static final Set<String> ACTIVE_QUEUEABLE_DISPATCHER_ASYNC_JOB_STATUSES = new Set<String>{
        'HOLDING',
        'QUEUED',
        'PREPARING',
        'PROCESSING',
        'ACQUIRED'
    };
    @TestVisible
    private static Boolean forceFutureBridge;

    private QueuesQueueableDispatchRepository userModeDispatchRepository = new QueuesQueueableDispatchRepository();
    private QueuesQueueableDispatchRepository systemModeDispatchRepository = new QueuesQueueableDispatchRepository(
        System.AccessLevel.SYSTEM_MODE
    );
    private QueuesJobRepository jobRepository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
    private QueuesAsyncApexJobRepository asyncApexJobRepository = new QueuesAsyncApexJobRepository(
        System.AccessLevel.SYSTEM_MODE
    );

    public QueuesQueueableDispatchService() {
    }

    @TestVisible
    private QueuesQueueableDispatchService(
        final QueuesQueueableDispatchRepository dispatchRepository,
        final QueuesJobRepository jobRepository,
        final QueuesAsyncApexJobRepository asyncApexJobRepository
    ) {
        this.userModeDispatchRepository = dispatchRepository;
        this.systemModeDispatchRepository = dispatchRepository;
        this.jobRepository = jobRepository;
        this.asyncApexJobRepository = asyncApexJobRepository;
    }

    // <editor-fold desc="Future Bridges">
    @SuppressWarnings('PMD.AvoidFutureAnnotation')
    @Future
    public static void enqueueExecutorFromFuture(final Id jobId) {
        if (String.isBlank(jobId)) {
            return;
        }

        new QueuesQueueableDispatchService().submitExecutorFromFuture(jobId);
    }

    @SuppressWarnings('PMD.AvoidFutureAnnotation')
    @Future
    public static void enqueueDispatcherFromFuture(final Id dispatchId) {
        if (String.isBlank(dispatchId)) {
            return;
        }

        new QueuesQueueableDispatchService().submitDispatcherFromFuture(dispatchId);
    }

    @SuppressWarnings('PMD.AvoidFutureAnnotation')
    @Future
    public static void continueDispatchFromFuture(final Id dispatchId) {
        if (String.isBlank(dispatchId)) {
            return;
        }

        new QueuesQueueableDispatchService().continueDispatch(dispatchId, false, true);
    }
    // </editor-fold>

    // <editor-fold desc="Dispatches">
    public virtual QueueableDispatch__c createWaitingDispatch(
        final String queueName,
        final String dedupeKey,
        final Queues.ExecutionMode executionMode,
        final List<Job__c> jobs
    ) {
        return createWaitingDispatch(queueName, dedupeKey, executionMode, jobs, false);
    }

    public virtual QueueableDispatch__c createWaitingDispatchAsSystem(
        final String queueName,
        final String dedupeKey,
        final Queues.ExecutionMode executionMode,
        final List<Job__c> jobs
    ) {
        return createWaitingDispatch(queueName, dedupeKey, executionMode, jobs, true);
    }

    public virtual QueueableDispatch__c createWaitingDispatch(
        final String queueName,
        final String dedupeKey,
        final Queues.ExecutionMode executionMode,
        final List<Job__c> jobs,
        final Boolean asSystem
    ) {
        QueuesSupport.notBlank(queueName, 'queueName');
        if (dedupeKey != null) {
            QueuesSupport.notBlank(dedupeKey, 'dedupeKey');
        }
        QueuesSupport.notNull(jobs, 'jobs');
        if (jobs.isEmpty()) {
            return null;
        }
        QueuesSupport.isTrue(
            QueuesSupport.isQueueableExecutionMode(executionMode),
            new Queues.QueueException('queueable execution mode is invalid.')
        );

        QueueableDispatch__c dispatch = new QueueableDispatch__c(
            QueueName__c = queueName,
            DedupeKey__c = dedupeKey,
            Mode__c = executionMode.name(),
            State__c = Queues.QueueableDispatchState.WAITING.name(),
            RequestedCount__c = jobs.size(),
            DispatchedCount__c = 0,
            DispatchDepth__c = 0
        );
        dispatch = getDispatchRepository(asSystem).insertDispatch(dispatch);

        for (Job__c job : jobs) {
            job.QueueableDispatch__c = dispatch.Id;
        }
        return dispatch;
    }

    public Id enqueueDispatcher(final QueueableDispatch__c dispatch) {
        return enqueueDispatcher(dispatch, false);
    }

    public Id enqueueDispatcherAsSystem(final QueueableDispatch__c dispatch) {
        return enqueueDispatcher(dispatch, true);
    }

    public Id enqueueDispatcher(final QueueableDispatch__c dispatch, final Boolean asSystem) {
        if (dispatch?.Id == null) {
            return null;
        }
        String dedupeKey = dispatch.DedupeKey__c;
        if (dedupeKey != null) {
            QueuesSupport.notBlank(dedupeKey, 'dedupeKey');
        }
        if (shouldBridgeInitialDispatcher()) {
            enqueueDispatcherViaFutureBridge(dispatch.Id);
            return null;
        }
        if (asSystem == true && !QueuesSupport.hasQueueableSubmissionSlot()) {
            return null;
        }

        Id asyncApexJobId = String.isBlank(dedupeKey)
            ? enqueueDispatcherJob(dispatch.Id, true)
            : enqueueDispatcherJob(dispatch.Id, true, dispatch.QueueName__c, dedupeKey);
        dispatch.DispatcherAsyncApexJobId__c = asyncApexJobId;
        getDispatchRepository(asSystem).updateDispatch(dispatch);
        return asyncApexJobId;
    }

    public QueueableDispatch__c startDispatchAsSystem(final Id dispatchId, final String asyncApexJobId) {
        QueuesSupport.notNull(dispatchId, 'dispatchId');

        QueueableDispatch__c dispatch = systemModeDispatchRepository.queryDispatchByIdForUpdate(dispatchId);
        if (dispatch == null) {
            return dispatch;
        }

        Queues.QueueableDispatchState dispatchState = QueuesSupport.toQueueableDispatchState(dispatch.State__c);
        if (dispatchState == Queues.QueueableDispatchState.WAITING) {
            dispatch.State__c = Queues.QueueableDispatchState.ACTIVE.name();
            dispatch.StartedAt__c = QueuesSupport.now();
            dispatch.DispatcherAsyncApexJobId__c = asyncApexJobId;
            dispatch = systemModeDispatchRepository.updateDispatch(dispatch);
            dispatchState = Queues.QueueableDispatchState.ACTIVE;
        }
        if (dispatchState != Queues.QueueableDispatchState.ACTIVE) {
            return dispatch;
        }
        if (shouldBridgeQueueableStack()) {
            return dispatch;
        }

        return dispatchNextJob(dispatch);
    }

    public QueueableDispatch__c continueDispatch(final Id dispatchId) {
        return continueDispatch(dispatchId, true, false);
    }

    public virtual Integer recoverStaleDispatches(
        final String queueName,
        final Datetime staleBefore,
        final Integer dispatchLimit
    ) {
        return recoverStaleDispatches(queueName, staleBefore, dispatchLimit, false);
    }

    public virtual Integer recoverStaleDispatchesAsSystem(
        final String queueName,
        final Datetime staleBefore,
        final Integer dispatchLimit
    ) {
        return recoverStaleDispatches(queueName, staleBefore, dispatchLimit, true);
    }

    public virtual Integer recoverStaleDispatches(
        final String queueName,
        final Datetime staleBefore,
        final Integer dispatchLimit,
        final Boolean asSystem
    ) {
        Set<String> recoverableStateValues = QueuesSupport.toQueueableDispatchStateValues(RECOVERABLE_DISPATCH_STATES);
        List<QueueableDispatch__c> staleDispatches = getDispatchRepository(asSystem)
            .queryStaleDispatchesForUpdate(queueName, recoverableStateValues, staleBefore, dispatchLimit);
        if (staleDispatches.isEmpty()) {
            return 0;
        }

        Set<Id> activeDispatcherAsyncJobIds = queryActiveDispatcherAsyncJobIds(staleDispatches);
        Integer recoveredCount = 0;
        for (QueueableDispatch__c staleDispatch : staleDispatches) {
            if (recoverStaleDispatch(staleDispatch, activeDispatcherAsyncJobIds, asSystem) == true) {
                recoveredCount++;
            }
        }
        return recoveredCount;
    }

    public QueueableDispatch__c failDispatchAsSystem(final Id dispatchId, final String failedReason) {
        QueuesSupport.notNull(dispatchId, 'dispatchId');

        QueueableDispatch__c dispatch = systemModeDispatchRepository.queryDispatchByIdForUpdate(dispatchId);
        if (dispatch == null) {
            return null;
        }

        Queues.QueueableDispatchState dispatchState = QueuesSupport.toQueueableDispatchState(dispatch.State__c);
        if (
            dispatchState == Queues.QueueableDispatchState.COMPLETED ||
            dispatchState == Queues.QueueableDispatchState.FAILED
        ) {
            return dispatch;
        }

        Datetime now = QueuesSupport.now();
        String normalizedFailedReason = QueuesSupport.normalizeExceptionReason(failedReason);
        List<Job__c> uninitiatedJobs = jobRepository.queryQueueableDispatchJobsForUpdate(
            dispatch.Id,
            QueuesSupport.toStateValues(DISPATCHABLE_JOB_STATES),
            null
        );
        for (Job__c job : uninitiatedJobs) {
            markQueueableSubmissionFailed(job, normalizedFailedReason, now);
        }
        jobRepository.updateJobs(uninitiatedJobs);

        dispatch.State__c = Queues.QueueableDispatchState.FAILED.name();
        dispatch.FailedReason__c = normalizedFailedReason;
        dispatch.FinishedAt__c = now;
        return systemModeDispatchRepository.updateDispatch(dispatch);
    }
    // </editor-fold>

    // <editor-fold desc="Hooks">
    protected virtual Id enqueueExecutorViaFutureBridge(final Id jobId, final Integer delayMinutes) {
        enqueueExecutorFromFuture(jobId);
        return null;
    }

    protected virtual void enqueueDispatcherViaFutureBridge(final Id dispatchId) {
        enqueueDispatcherFromFuture(dispatchId);
    }

    protected virtual Id enqueueDispatcherJob(final Id dispatchId, final Boolean useInitialStackDepth) {
        return enqueueDispatcherJob(dispatchId, useInitialStackDepth, null, null);
    }

    protected virtual Id enqueueDispatcherJob(
        final Id dispatchId,
        final Boolean useInitialStackDepth,
        final String queueName,
        final String dedupeKey
    ) {
        QueuesQueueableDispatcher dispatcher = new QueuesQueueableDispatcher(dispatchId);
        return useInitialStackDepth
            ? System.enqueueJob(dispatcher, initialDispatcherAsyncOptions(queueName, dedupeKey))
            : System.enqueueJob(dispatcher);
    }

    protected override Boolean shouldBridgeQueueableStack() {
        if (forceFutureBridge == true) {
            return true;
        }
        return super.shouldBridgeQueueableStack();
    }

    protected virtual Boolean shouldBridgeInitialDispatcher() {
        return shouldBridgeQueueableStack() || isQueueableStackContext();
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private QueueableDispatch__c dispatchNextJob(final QueueableDispatch__c dispatch) {
        if (QueuesSupport.toExecutionMode(dispatch.Mode__c) == Queues.ExecutionMode.QUEUEABLE_CONCURRENT) {
            return dispatchNextJobsConcurrently(dispatch);
        }

        return dispatchNextJobSerially(dispatch);
    }

    private QueuesQueueableDispatchRepository getDispatchRepository(final Boolean asSystem) {
        return asSystem == true ? systemModeDispatchRepository : userModeDispatchRepository;
    }

    private Boolean recoverStaleDispatch(
        final QueueableDispatch__c staleDispatch,
        final Set<Id> activeDispatcherAsyncJobIds,
        final Boolean asSystem
    ) {
        if (activeDispatcherAsyncJobIds.contains(QueuesSupport.toId(staleDispatch.DispatcherAsyncApexJobId__c))) {
            return false;
        }

        Queues.QueueableDispatchState dispatchState = QueuesSupport.toQueueableDispatchState(staleDispatch.State__c);
        if (dispatchState == Queues.QueueableDispatchState.WAITING) {
            enqueueDispatcher(staleDispatch, asSystem);
            return true;
        }
        continueDispatch(staleDispatch, false, true);
        return true;
    }

    private QueueableDispatch__c dispatchNextJobSerially(final QueueableDispatch__c dispatch) {
        List<Job__c> jobs = jobRepository.queryQueueableDispatchJobsForUpdate(
            dispatch.Id,
            QueuesSupport.toStateValues(DISPATCHABLE_JOB_STATES),
            SINGLE_JOB_LIMIT
        );
        if (jobs.isEmpty()) {
            return completeDispatch(dispatch);
        }

        Datetime now = QueuesSupport.now();
        Job__c job = jobs[0];
        QueuesQueueableTransportService.QueueableSubmission submission = submitExecutor(job, now);
        if (submission.isInvalidDelay()) {
            markInvalidQueueableDelay(job, submission, now);
        } else if (submission.hasEnqueueException()) {
            markQueueableSubmissionFailed(job, submission.failedReason, now);
        } else {
            job.QueueableDispatchInitiatedAt__c = now;
            markQueueableExecutorSubmitted(job, submission);
        }

        jobRepository.updateJobs(new List<Job__c>{ job });
        return markDispatchJobsResolved(dispatch, now, jobs.size());
    }

    private QueueableDispatch__c dispatchNextJobsConcurrently(final QueueableDispatch__c dispatch) {
        List<Job__c> jobs = jobRepository.queryQueueableDispatchJobsForUpdate(
            dispatch.Id,
            QueuesSupport.toStateValues(DISPATCHABLE_JOB_STATES),
            Limits.getLimitFutureCalls()
        );
        if (jobs.isEmpty()) {
            return completeDispatch(dispatch);
        }

        Datetime now = QueuesSupport.now();
        List<Job__c> jobsToUpdate = new List<Job__c>();
        for (Job__c job : jobs) {
            QueuesQueueableTransportService.QueueableSubmission submission = prepareExecutorSubmission(job, now);
            if (submission.isInvalidDelay()) {
                markInvalidQueueableDelay(job, submission, now);
                jobsToUpdate.add(job);
                continue;
            }

            Id asyncApexJobId;
            try {
                asyncApexJobId = enqueueExecutorViaFutureBridge(job.Id, submission.delayMinutes);
            } catch (Exception ex) {
                markQueueableSubmissionFailed(job, QueuesSupport.resolveExceptionReason(ex), now);
                jobsToUpdate.add(job);
                continue;
            }
            job.QueueableDispatchInitiatedAt__c = now;
            if (asyncApexJobId != null) {
                markQueueableExecutorSubmitted(job, submission);
                job.AsyncApexJobId__c = asyncApexJobId;
            }
            jobsToUpdate.add(job);
        }

        jobRepository.updateJobs(jobsToUpdate);
        return markDispatchJobsResolved(dispatch, now, jobs.size());
    }

    @TestVisible
    private Id submitDispatcherFromFuture(final Id dispatchId) {
        QueueableDispatch__c dispatch = systemModeDispatchRepository.queryDispatchByIdForUpdate(dispatchId);
        if (!isUnsubmittedWaitingDispatch(dispatch)) {
            return null;
        }

        return enqueueDispatcher(dispatch, true);
    }

    @TestVisible
    private Job__c submitExecutorFromFuture(final Id jobId) {
        List<Job__c> jobs = jobRepository.queryJobsByIdsForUpdate(new Set<Id>{ jobId }, null);
        if (jobs.isEmpty()) {
            return null;
        }

        Job__c job = jobs[0];
        if (!isDispatchableQueueableJob(job)) {
            return job;
        }

        Datetime now = QueuesSupport.now();
        QueuesQueueableTransportService.QueueableSubmission submission = submitExecutor(job, now);
        if (submission.isInvalidDelay()) {
            markInvalidQueueableDelay(job, submission, now);
        } else if (submission.hasEnqueueException()) {
            markQueueableSubmissionFailed(job, submission.failedReason, now);
        } else {
            markQueueableExecutorSubmitted(job, submission);
        }
        if (job.QueueableDispatchInitiatedAt__c == null) {
            job.QueueableDispatchInitiatedAt__c = now;
        }
        jobRepository.updateJobs(new List<Job__c>{ job });
        return job;
    }

    private static Boolean isDispatchableQueueableJob(final Job__c job) {
        return job != null &&
            QueuesSupport.isQueueableExecutionMode(QueuesSupport.toExecutionMode(job.ExecutionMode__c)) &&
            DISPATCHABLE_JOB_STATES.contains(QueuesSupport.toState(job.State__c));
    }

    private static Boolean isUnsubmittedWaitingDispatch(final QueueableDispatch__c dispatch) {
        return QueuesSupport.toQueueableDispatchState(dispatch?.State__c) == Queues.QueueableDispatchState.WAITING &&
            String.isBlank(dispatch.DispatcherAsyncApexJobId__c);
    }

    private QueueableDispatch__c markDispatchJobsResolved(
        final QueueableDispatch__c dispatch,
        final Datetime now,
        final Integer resolvedCount
    ) {
        dispatch.DispatchedCount__c =
            QueuesSupport.toInteger(dispatch.DispatchedCount__c, 0) + QueuesSupport.toInteger(resolvedCount, 0);
        if (
            QueuesSupport.toInteger(dispatch.DispatchedCount__c, 0) >=
            QueuesSupport.toInteger(dispatch.RequestedCount__c, 0)
        ) {
            dispatch.State__c = Queues.QueueableDispatchState.COMPLETED.name();
            dispatch.FinishedAt__c = now;
        }
        return systemModeDispatchRepository.updateDispatch(dispatch);
    }

    private QueueableDispatch__c completeDispatch(final QueueableDispatch__c dispatch) {
        dispatch.State__c = Queues.QueueableDispatchState.COMPLETED.name();
        dispatch.FinishedAt__c = QueuesSupport.now();
        return systemModeDispatchRepository.updateDispatch(dispatch);
    }

    @TestVisible
    private QueueableDispatch__c continueDispatch(
        final Id dispatchId,
        final Boolean allowFutureBridge,
        final Boolean useInitialStackDepth
    ) {
        QueuesSupport.notNull(dispatchId, 'dispatchId');

        QueueableDispatch__c dispatch = systemModeDispatchRepository.queryDispatchByIdForUpdate(dispatchId);
        return continueDispatch(dispatch, allowFutureBridge, useInitialStackDepth);
    }

    private QueueableDispatch__c continueDispatch(
        final QueueableDispatch__c dispatch,
        final Boolean allowFutureBridge,
        final Boolean useInitialStackDepth
    ) {
        if (dispatch == null) {
            return null;
        }
        Queues.QueueableDispatchState dispatchState = QueuesSupport.toQueueableDispatchState(dispatch.State__c);
        if (dispatchState != Queues.QueueableDispatchState.ACTIVE) {
            return dispatch;
        }
        if (isFullyDispatched(dispatch)) {
            return completeDispatch(dispatch);
        }
        if (!hasRemainingDispatchJob(dispatch)) {
            return completeDispatch(dispatch);
        }
        if (allowFutureBridge == true && shouldBridgeQueueableStack()) {
            continueDispatchFromFuture(dispatch.Id);
            return dispatch;
        }

        Id asyncApexJobId = enqueueDispatcherJob(dispatch.Id, useInitialStackDepth == true);
        dispatch.DispatcherAsyncApexJobId__c = asyncApexJobId;
        dispatch.DispatchDepth__c = QueuesSupport.toInteger(dispatch.DispatchDepth__c, 0) + 1;
        return systemModeDispatchRepository.updateDispatch(dispatch);
    }

    private Set<Id> queryActiveDispatcherAsyncJobIds(final List<QueueableDispatch__c> dispatches) {
        Set<Id> dispatcherAsyncJobIds = QueuesSupport.toIdSet(dispatches, 'DispatcherAsyncApexJobId__c');
        Set<Id> activeDispatcherAsyncJobIds = new Set<Id>();
        for (AsyncApexJob asyncJob : asyncApexJobRepository.queryAsyncJobsByIds(dispatcherAsyncJobIds)) {
            if (
                ACTIVE_QUEUEABLE_DISPATCHER_ASYNC_JOB_STATUSES.contains(
                    QueuesSupport.normalizeEnumValue(asyncJob.Status)
                )
            ) {
                activeDispatcherAsyncJobIds.add(asyncJob.Id);
            }
        }
        return activeDispatcherAsyncJobIds;
    }

    private Boolean hasRemainingDispatchJob(final QueueableDispatch__c dispatch) {
        return !jobRepository
            .queryQueueableDispatchJobsForUpdate(
                dispatch.Id,
                QueuesSupport.toStateValues(DISPATCHABLE_JOB_STATES),
                SINGLE_JOB_LIMIT
            )
            .isEmpty();
    }

    private static Boolean isFullyDispatched(final QueueableDispatch__c dispatch) {
        return QueuesSupport.toInteger(dispatch.DispatchedCount__c, 0) >=
            QueuesSupport.toInteger(dispatch.RequestedCount__c, 0);
    }

    private static AsyncOptions initialDispatcherAsyncOptions(final String queueName, final String dedupeKey) {
        AsyncOptions options = new AsyncOptions();
        options.maximumQueueableStackDepth = INITIAL_QUEUEABLE_STACK_DEPTH;
        if (String.isNotBlank(dedupeKey)) {
            QueuesSupport.notBlank(queueName, 'queueName');
            String compositeKey = queueName + ':' + dedupeKey;
            options.duplicateSignature = QueueableDuplicateSignature.builder()
                .addString(resolveDispatcherDuplicateSignature(compositeKey))
                .build();
        }
        return options;
    }

    private static String resolveDispatcherDuplicateSignature(final String compositeKey) {
        if (Blob.valueOf(compositeKey).size() <= DUPLICATE_SIGNATURE_MAX_BYTES) {
            return compositeKey;
        }

        String digestHex = EncodingUtil.convertToHex(Crypto.generateDigest('SHA-256', Blob.valueOf(compositeKey)));
        return COMPRESSED_DUPLICATE_SIGNATURE_PREFIX +
            digestHex.substring(0, DUPLICATE_SIGNATURE_MAX_BYTES - COMPRESSED_DUPLICATE_SIGNATURE_PREFIX.length());
    }
    // </editor-fold>
}
