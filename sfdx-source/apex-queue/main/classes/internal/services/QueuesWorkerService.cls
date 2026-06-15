/**
 * @description Schedules and drives the batch worker that claims worker-mode jobs.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesWorkerService {
    private static final Integer DEFAULT_BATCH_CLAIM_SIZE = 1;
    private static final Integer DEFAULT_BATCH_SCOPE_SIZE = 1;
    private static final Integer MAX_BATCH_CLAIM_SIZE = 2000;
    private static final String WORKER_SCHEDULING_COMPONENT = 'worker scheduling';
    private static final String WORKER_JOB_ID_UPDATE_FAILED_MESSAGE = 'Worker job id update failed.';
    private static final Set<String> ACTIVE_ASYNC_WORKER_JOB_STATUSES = new Set<String>{
        'HOLDING',
        'QUEUED',
        'PREPARING',
        'PROCESSING',
        'ACQUIRED'
    };
    private static final Set<String> TERMINAL_SCHEDULED_JOB_STATES = new Set<String>{ 'DELETED', 'COMPLETE', 'ERROR' };
    private static final Set<Queues.State> DUE_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED
    };

    private QueuesDefinitionService definitionService = new QueuesDefinitionService();
    private QueuesRuntimeService runtimeService = new QueuesRuntimeService();
    private QueuesJobService jobService = new QueuesJobService();
    private QueuesJobExecutionService jobExecutionService = new QueuesJobExecutionService();
    private QueuesAsyncApexJobRepository asyncApexJobRepository = new QueuesAsyncApexJobRepository(
        System.AccessLevel.SYSTEM_MODE
    );
    private QueuesScheduleRepository scheduleRepository = new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE);
    private QueuesErrorService errorService = new QueuesErrorService();

    public QueuesWorkerService() {
    }

    @TestVisible
    private QueuesWorkerService(
        final QueuesDefinitionService definitionService,
        final QueuesRuntimeService runtimeService,
        final QueuesJobService jobService,
        final QueuesJobExecutionService jobExecutionService,
        final QueuesAsyncApexJobRepository asyncApexJobRepository,
        final QueuesScheduleRepository scheduleRepository,
        final QueuesErrorService errorService
    ) {
        this.definitionService = definitionService;
        this.runtimeService = runtimeService;
        this.jobService = jobService;
        this.jobExecutionService = jobExecutionService;
        this.asyncApexJobRepository = asyncApexJobRepository;
        this.scheduleRepository = scheduleRepository;
        this.errorService = errorService;
    }

    // <editor-fold desc="Workers">
    public void scheduleWorkerForJobs(final String queueName, final List<Job__c> jobs, final Datetime now) {
        ensureWorker(queueName, jobService.findEarliestWakeAt(jobs, now));
    }

    public Boolean ensureWorkerForNextWakeAt(final String queueName, final Datetime currentNow) {
        return ensureWorker(queueName, jobService.findNextWakeAtAsSystem(queueName, currentNow));
    }

    public Boolean syncWorkerForNextWakeAt(final String queueName, final Datetime currentNow) {
        Datetime desiredWakeAt = jobService.findNextWakeAtAsSystem(queueName, currentNow);
        return syncExistingWorkerForNextWakeAt(queueName, currentNow, desiredWakeAt) ||
            ensureWorker(queueName, desiredWakeAt);
    }

    public Boolean syncExistingWorkerForNextWakeAt(final String queueName, final Datetime currentNow) {
        return syncExistingWorkerForNextWakeAt(
            queueName,
            currentNow,
            jobService.findNextWakeAtAsSystem(queueName, currentNow)
        );
    }

    public Boolean ensureWorker(final String queueName, final Datetime desiredWakeAt) {
        if (desiredWakeAt == null) {
            return false;
        }

        runtimeService.ensureRuntimeAsSystem(queueName);
        QueueRuntime__c runtime = runtimeService.lockRuntimeAsSystem(queueName);
        if (runtime == null || QueuesSupport.isPaused(runtime)) {
            return false;
        }

        Datetime currentNow = QueuesSupport.now();
        Datetime normalizedWakeAt = QueuesSupport.normalizeWakeAt(desiredWakeAt);
        if (shouldKeepExistingWorker(queueName, runtime, normalizedWakeAt, currentNow)) {
            return false;
        }

        return replaceWorkerPlan(queueName, runtime, normalizedWakeAt, null);
    }

    public void cancelFutureScheduledWorker(final String queueName, final QueueRuntime__c runtime) {
        abortExistingScheduledWorker(queueName, runtime);
    }

    public Database.QueryLocator start(final String queueName, final String workerToken, final String asyncApexJobId) {
        QueuesSupport.notBlank(queueName, 'queueName');
        QueuesSupport.notBlank(workerToken, 'workerToken');

        QueueDefinition__mdt queueDefinition = definitionService.findQueueDefinitionAsSystem(queueName);
        QueueRuntime__c runtime = runtimeService.lockRuntimeAsSystem(queueName);
        if (runtime == null || !runtimeService.isCurrentWorker(runtime, workerToken)) {
            return jobService.emptyJobLocator();
        }

        syncCurrentAsyncApexJobId(runtime, asyncApexJobId);

        if (!QueuesSupport.isActive(queueDefinition) || QueuesSupport.isPaused(runtime)) {
            runtimeService.clearWorkerAndUpdateAsSystem(runtime);
            return jobService.emptyJobLocator();
        }

        Datetime now = QueuesSupport.now();
        runtime.LastPumpAt__c = now;

        if (runtime.NextPumpAt__c > now) {
            runtimeService.updateRuntimeAsSystem(runtime);
            return jobService.emptyJobLocator();
        }

        List<Job__c> dueJobs = jobService.queryDueJobsAsSystem(
            queueName,
            DUE_JOB_STATES,
            now,
            resolveBatchClaimSize(queueDefinition)
        );
        List<Job__c> claimedJobs = jobService.claimBatchScopeJobsAsSystem(
            queueDefinition,
            dueJobs,
            now,
            runtime.AsyncApexJobId__c
        );
        if (claimedJobs.isEmpty()) {
            runtimeService.updateRuntimeAsSystem(runtime);
            return jobService.emptyJobLocator();
        }

        runtimeService.incrementActiveWorkerCount(runtime);
        runtimeService.updateRuntimeAsSystem(runtime);
        return jobService.queryClaimedJobLocatorAsSystem(claimedJobs);
    }

    public void execute(final String queueName, final String workerToken, final List<Job__c> scopeJobs) {
        execute(queueName, workerToken, scopeJobs, null);
    }

    public void execute(
        final String queueName,
        final String workerToken,
        final List<Job__c> scopeJobs,
        final String asyncApexJobId
    ) {
        QueuesSupport.notBlank(queueName, 'queueName');
        QueuesSupport.notBlank(workerToken, 'workerToken');
        QueuesSupport.notNull(scopeJobs, 'scopeJobs');
        if (scopeJobs.isEmpty()) {
            return;
        }

        QueueRuntime__c runtime = runtimeService.findRuntimeAsSystem(queueName);
        if (
            runtime == null ||
            !runtimeService.isCurrentWorker(runtime, workerToken) ||
            QueuesSupport.isPaused(runtime)
        ) {
            return;
        }

        definitionService.requireQueueDefinition(queueName, true);
        jobExecutionService.executeClaimedBatchJobs(scopeJobs, asyncApexJobId);
    }

    public void finish(final String queueName, final String workerToken) {
        QueuesSupport.notBlank(queueName, 'queueName');
        QueuesSupport.notBlank(workerToken, 'workerToken');

        QueueRuntime__c runtime = runtimeService.lockRuntimeAsSystem(queueName);
        if (runtime == null) {
            return;
        }

        runtimeService.decrementActiveWorkerCount(runtime);
        if (!runtimeService.isCurrentWorker(runtime, workerToken)) {
            runtimeService.updateRuntimeAsSystem(runtime);
            return;
        }

        continueWorker(
            queueName,
            workerToken,
            jobService.findNextWakeAtAsSystem(queueName, QueuesSupport.now()),
            runtime
        );
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private Boolean syncExistingWorkerForNextWakeAt(
        final String queueName,
        final Datetime currentNow,
        final Datetime desiredWakeAt
    ) {
        QueuesSupport.notBlank(queueName, 'queueName');
        QueuesSupport.notNull(currentNow, 'currentNow');

        QueueRuntime__c runtime = lockPendingFutureWorkerRuntime(queueName, currentNow);
        return runtime != null && applyNextWakeToPendingFutureWorker(queueName, runtime, desiredWakeAt, currentNow);
    }

    private QueueRuntime__c lockPendingFutureWorkerRuntime(final String queueName, final Datetime currentNow) {
        QueueRuntime__c runtime = runtimeService.lockRuntimeAsSystem(queueName);
        if (
            runtime == null ||
            QueuesSupport.isPaused(runtime) ||
            QueuesSupport.toInteger(runtime.ActiveWorkerCount__c, 0) > 0 ||
            runtime.NextPumpAt__c == null ||
            !hasPendingWorkerPlan(runtime, currentNow)
        ) {
            return null;
        }

        return runtime;
    }

    private Boolean applyNextWakeToPendingFutureWorker(
        final String queueName,
        final QueueRuntime__c runtime,
        final Datetime desiredWakeAt,
        final Datetime currentNow
    ) {
        if (desiredWakeAt == null) {
            cancelFutureScheduledWorker(queueName, runtime);
            runtimeService.clearWorkerAndUpdateAsSystem(runtime);
            return true;
        }

        Datetime normalizedWakeAt = QueuesSupport.normalizeWakeAt(desiredWakeAt);
        if (runtime.NextPumpAt__c < normalizedWakeAt && hasActiveScheduledWorker(runtime.AsyncApexJobId__c)) {
            runtime.NextPumpAt__c = normalizedWakeAt;
            runtimeService.updateRuntimeAsSystem(runtime);
            return true;
        }

        if (
            runtime.NextPumpAt__c == normalizedWakeAt &&
            shouldKeepExistingWorker(queueName, runtime, normalizedWakeAt, currentNow)
        ) {
            return false;
        }

        return replaceWorkerPlan(queueName, runtime, normalizedWakeAt, null);
    }

    private void continueWorker(
        final String queueName,
        final String currentWorkerToken,
        final Datetime desiredWakeAt,
        final QueueRuntime__c runtime
    ) {
        if (QueuesSupport.isPaused(runtime) || desiredWakeAt == null) {
            runtimeService.clearWorkerAndUpdateAsSystem(runtime);
            return;
        }

        replaceWorkerPlan(queueName, runtime, QueuesSupport.normalizeWakeAt(desiredWakeAt), currentWorkerToken);
    }

    private Boolean replaceWorkerPlan(
        final String queueName,
        final QueueRuntime__c runtime,
        final Datetime normalizedWakeAt,
        final String currentWorkerToken
    ) {
        WorkerPlan previousPlan = captureExistingWorkerPlan(currentWorkerToken, runtime);
        String nextWorkerToken;
        String asyncApexJobId;
        Job__c nextDispatchJob;
        WorkerDispatch dispatch;
        try {
            nextWorkerToken = QueuesSupport.generateRandomToken();
            nextDispatchJob = jobService.findNextDispatchJobAsSystem(queueName, normalizedWakeAt);
            if (nextDispatchJob == null) {
                if (String.isNotBlank(currentWorkerToken)) {
                    runtimeService.clearWorkerAndUpdateAsSystem(runtime);
                }
                return false;
            }

            dispatch = resolveWorkerDispatch(nextDispatchJob);
            runtime.NextPumpAt__c = normalizedWakeAt;
            runtime.WorkerToken__c = nextWorkerToken;
            runtime.AsyncApexJobId__c = null;
            runtimeService.updateRuntimeAsSystem(runtime);
            asyncApexJobId = scheduleBatchWorker(queueName, nextWorkerToken, normalizedWakeAt, dispatch.scopeSize);
            Boolean workerJobIdUpdated = runtimeService.updateWorkerJobId(queueName, nextWorkerToken, asyncApexJobId);
            QueuesSupport.isTrue(workerJobIdUpdated, new Queues.QueueException(WORKER_JOB_ID_UPDATE_FAILED_MESSAGE));
            abortReplacedWorkerIfStillActive(queueName, previousPlan?.asyncApexJobId, asyncApexJobId);
        } catch (Exception ex) {
            abortDispatchedWorker(queueName, dispatch?.jobId ?? nextDispatchJob?.Id, asyncApexJobId);
            errorService.recordQueueError(
                queueName,
                WORKER_SCHEDULING_COMPONENT,
                dispatch?.jobId ?? nextDispatchJob?.Id,
                asyncApexJobId,
                ex
            );
            if (String.isNotBlank(nextWorkerToken)) {
                rollbackWorkerTransition(queueName, nextWorkerToken, previousPlan);
            }
            throw ex;
        }
        return true;
    }

    private static WorkerPlan captureExistingWorkerPlan(
        final String currentWorkerToken,
        final QueueRuntime__c runtime
    ) {
        if (String.isNotBlank(currentWorkerToken) || String.isBlank(runtime?.WorkerToken__c)) {
            return null;
        }

        return new WorkerPlan(runtime.NextPumpAt__c, runtime.WorkerToken__c, runtime.AsyncApexJobId__c);
    }

    private void abortReplacedWorkerIfStillActive(
        final String queueName,
        final String previousAsyncApexJobId,
        final String nextAsyncApexJobId
    ) {
        if (String.isBlank(previousAsyncApexJobId) || previousAsyncApexJobId == nextAsyncApexJobId) {
            return;
        }

        abortWorkerJobIfStillActive(queueName, null, previousAsyncApexJobId);
    }

    private void abortDispatchedWorker(final String queueName, final Id jobId, final String asyncApexJobId) {
        if (String.isBlank(asyncApexJobId)) {
            return;
        }

        // Query current liveness for diagnostics, but do not gate the abort on it.
        // Freshly dispatched async workers can exist before AsyncApexJob/CronTrigger
        // visibility catches up, and savepoint rollback will not undo the dispatch.
        hasActiveWorkerJob(asyncApexJobId);
        abortJob(queueName, jobId, asyncApexJobId);
    }

    private void abortWorkerJobIfStillActive(final String queueName, final Id jobId, final String asyncApexJobId) {
        if (!hasActiveWorkerJob(asyncApexJobId)) {
            return;
        }

        abortJob(queueName, jobId, asyncApexJobId);
    }

    private void abortExistingScheduledWorker(final String queueName, final QueueRuntime__c runtime) {
        if (runtime == null || !hasActiveScheduledWorker(runtime.AsyncApexJobId__c)) {
            return;
        }

        abortJob(queueName, null, runtime.AsyncApexJobId__c);
    }

    private void abortJob(final String queueName, final Id jobId, final String asyncApexJobId) {
        try {
            scheduleRepository.abortJob(asyncApexJobId);
        } catch (Exception ex) {
            if (hasActiveWorkerJob(asyncApexJobId)) {
                errorService.recordQueueError(queueName, WORKER_SCHEDULING_COMPONENT, jobId, asyncApexJobId, ex);
            }
        }
    }

    private Boolean hasActiveWorkerJob(final String asyncApexJobId) {
        return hasActiveScheduledWorker(asyncApexJobId) || hasActiveAsyncWorker(asyncApexJobId);
    }

    private Boolean hasActiveScheduledWorker(final String asyncApexJobId) {
        if (String.isBlank(asyncApexJobId)) {
            return false;
        }

        List<CronTrigger> scheduledJobs = scheduleRepository.queryScheduledJobById(
            asyncApexJobId,
            TERMINAL_SCHEDULED_JOB_STATES
        );
        return !scheduledJobs.isEmpty();
    }

    private Boolean hasActiveAsyncWorker(final String asyncApexJobId) {
        List<AsyncApexJob> asyncJobs = asyncApexJobRepository.queryAsyncJobs(asyncApexJobId);
        return !asyncJobs.isEmpty() &&
            ACTIVE_ASYNC_WORKER_JOB_STATUSES.contains(QueuesSupport.normalizeEnumValue(asyncJobs[0].Status));
    }

    private Boolean shouldKeepExistingWorker(
        final String queueName,
        final QueueRuntime__c runtime,
        final Datetime desiredWakeAt,
        final Datetime currentNow
    ) {
        if (!runtimeService.shouldKeepWorker(queueName, runtime, desiredWakeAt, currentNow)) {
            return false;
        }

        if (runtime.NextPumpAt__c > currentNow) {
            return hasActiveScheduledWorker(runtime.AsyncApexJobId__c);
        }

        return true;
    }

    private Boolean hasPendingWorkerPlan(final QueueRuntime__c runtime, final Datetime currentNow) {
        return runtime.NextPumpAt__c > currentNow || hasActiveScheduledWorker(runtime.AsyncApexJobId__c);
    }

    private void rollbackWorkerTransition(
        final String queueName,
        final String nextWorkerToken,
        final WorkerPlan previousPlan
    ) {
        QueueRuntime__c latestRuntime = runtimeService.lockRuntimeAsSystem(queueName);
        if (latestRuntime == null || !runtimeService.isCurrentWorker(latestRuntime, nextWorkerToken)) {
            return;
        }

        if (String.isBlank(previousPlan?.workerToken)) {
            runtimeService.clearWorkerAndUpdateAsSystem(latestRuntime);
            return;
        }

        latestRuntime.NextPumpAt__c = previousPlan.nextPumpAt;
        latestRuntime.WorkerToken__c = previousPlan.workerToken;
        latestRuntime.AsyncApexJobId__c = previousPlan.asyncApexJobId;
        runtimeService.updateRuntimeAsSystem(latestRuntime);
    }

    private static WorkerDispatch resolveWorkerDispatch(final Job__c nextJob) {
        return new WorkerDispatch(DEFAULT_BATCH_SCOPE_SIZE, nextJob.Id);
    }

    private static void syncCurrentAsyncApexJobId(final QueueRuntime__c runtime, final String asyncApexJobId) {
        if (runtime == null || String.isBlank(asyncApexJobId) || runtime.AsyncApexJobId__c == asyncApexJobId) {
            return;
        }

        runtime.AsyncApexJobId__c = asyncApexJobId;
    }

    private static Integer resolveBatchClaimSize(final QueueDefinition__mdt queueDefinition) {
        Integer claimSize = QueuesSupport.toInteger(queueDefinition?.BatchClaimSize__c, DEFAULT_BATCH_CLAIM_SIZE);
        if (claimSize <= 0) {
            return DEFAULT_BATCH_CLAIM_SIZE;
        }

        return Math.min(claimSize, MAX_BATCH_CLAIM_SIZE);
    }

    private static String scheduleBatchWorker(
        final String queueName,
        final String workerToken,
        final Datetime desiredWakeAt,
        final Integer scopeSize
    ) {
        Integer delayMinutes = resolveWorkerDelayMinutes(desiredWakeAt, QueuesSupport.now());
        Integer normalizedScopeSize = scopeSize > 0 ? scopeSize : DEFAULT_BATCH_SCOPE_SIZE;

        QueuesBatchableWorker worker = new QueuesBatchableWorker(queueName, workerToken);
        if (delayMinutes > 0) {
            return System.scheduleBatch(
                worker,
                toBatchJobName(queueName, workerToken, desiredWakeAt),
                delayMinutes,
                normalizedScopeSize
            );
        }

        return Database.executeBatch(worker, normalizedScopeSize);
    }

    @TestVisible
    private static Integer resolveWorkerDelayMinutes(final Datetime desiredWakeAt, final Datetime now) {
        if (desiredWakeAt > now) {
            Long diffMs = desiredWakeAt.getTime() - now.getTime();
            Long roundedMs = diffMs + 59999;
            return (Integer) (roundedMs / 60000);
        }
        return 0;
    }

    private static String toBatchJobName(
        final String queueName,
        final String workerToken,
        final Datetime desiredWakeAt
    ) {
        String safeQueueName = String.isBlank(queueName) ? 'queue' : queueName;
        String safeToken = String.isBlank(workerToken) ? 'worker' : workerToken.left(8);
        String safeWakeAt = desiredWakeAt == null ? 'now' : '' + desiredWakeAt.getTime();
        String testSuffix = Test.isRunningTest() ? ':' + QueuesSupport.generateRandomToken().left(6) : '';
        String jobName = 'Queues:' + safeQueueName + ':' + safeWakeAt + ':' + safeToken + testSuffix;
        return jobName.length() <= 80 ? jobName : jobName.left(80);
    }
    // </editor-fold>

    private class WorkerDispatch {
        private final Integer scopeSize;
        private final Id jobId;

        private WorkerDispatch(final Integer scopeSize, final Id jobId) {
            this.scopeSize = scopeSize ?? DEFAULT_BATCH_SCOPE_SIZE;
            this.jobId = jobId;
        }
    }

    private class WorkerPlan {
        private final Datetime nextPumpAt;
        private final String workerToken;
        private final String asyncApexJobId;

        private WorkerPlan(final Datetime nextPumpAt, final String workerToken, final String asyncApexJobId) {
            this.nextPumpAt = nextPumpAt;
            this.workerToken = workerToken;
            this.asyncApexJobId = asyncApexJobId;
        }
    }
}
