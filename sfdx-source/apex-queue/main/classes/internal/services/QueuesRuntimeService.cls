/**
 * @description Manages per-queue worker runtime locking and state.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesRuntimeService {
    private static final Integer WORKER_DISPATCH_GRACE_MINUTES = 1;
    private static final Integer DEFAULT_LEASE_MINUTES = 5;
    private static final Set<String> TERMINAL_SCHEDULED_JOB_STATES = new Set<String>{ 'DELETED', 'COMPLETE', 'ERROR' };
    private static final Set<AsyncJobStatus> ACTIVE_WORKER_JOB_STATUSES = new Set<AsyncJobStatus>{
        AsyncJobStatus.HOLDING,
        AsyncJobStatus.QUEUED,
        AsyncJobStatus.PREPARING,
        AsyncJobStatus.PROCESSING,
        AsyncJobStatus.ACQUIRED
    };

    private QueuesAsyncApexJobRepository asyncApexJobRepository = new QueuesAsyncApexJobRepository(
        System.AccessLevel.SYSTEM_MODE
    );
    private QueuesScheduleRepository scheduleRepository = new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE);
    private QueuesRuntimeRepository userModeRepository = new QueuesRuntimeRepository();
    private QueuesRuntimeRepository systemModeRepository = new QueuesRuntimeRepository(System.AccessLevel.SYSTEM_MODE);
    private QueuesDefinitionService definitionService = new QueuesDefinitionService();

    public QueuesRuntimeService() {
    }

    @TestVisible
    private QueuesRuntimeService(
        final QueuesAsyncApexJobRepository asyncApexJobRepository,
        final QueuesScheduleRepository scheduleRepository,
        final QueuesRuntimeRepository repository,
        final QueuesDefinitionService definitionService
    ) {
        this.asyncApexJobRepository = asyncApexJobRepository;
        this.scheduleRepository = scheduleRepository;
        this.userModeRepository = repository;
        this.systemModeRepository = repository;
        this.definitionService = definitionService;
    }

    // <editor-fold desc="Runtime">
    public QueueRuntime__c ensureRuntime(final String queueName) {
        return ensureRuntime(queueName, false);
    }

    public QueueRuntime__c ensureRuntimeAsSystem(final String queueName) {
        return ensureRuntime(queueName, true);
    }

    public QueueRuntime__c ensureRuntime(final String queueName, final Boolean asSystem) {
        QueueRuntime__c runtime = findRuntime(queueName, asSystem);
        if (runtime != null) {
            return runtime;
        }

        runtime = new QueueRuntime__c(QueueName__c = queueName, IsPaused__c = false, ActiveWorkerCount__c = 0);
        try {
            return getRepository(asSystem).insertRuntime(runtime);
        } catch (DmlException ex) {
            if (!isDuplicateRuntimeInsert(ex)) {
                throw ex;
            }

            QueueRuntime__c existingRuntime = findRuntime(queueName, asSystem);
            if (existingRuntime != null) {
                return existingRuntime;
            }
            throw ex;
        }
    }
    // </editor-fold>

    // <editor-fold desc="Runtime State">
    public QueueRuntime__c findRuntime(final String queueName) {
        List<QueueRuntime__c> runtimes = userModeRepository.queryRuntimes(queueName);
        return runtimes.isEmpty() ? null : runtimes[0];
    }

    public QueueRuntime__c findRuntimeAsSystem(final String queueName) {
        List<QueueRuntime__c> runtimes = systemModeRepository.queryRuntimes(queueName);
        return runtimes.isEmpty() ? null : runtimes[0];
    }

    public QueueRuntime__c findRuntime(final String queueName, final Boolean asSystem) {
        List<QueueRuntime__c> runtimes = getRepository(asSystem).queryRuntimes(queueName);
        return runtimes.isEmpty() ? null : runtimes[0];
    }

    public QueueRuntime__c findVisibleRuntime(final String queueName) {
        return findRuntime(queueName);
    }

    public QueueRuntime__c lockRuntime(final String queueName) {
        List<QueueRuntime__c> runtimes = userModeRepository.queryRuntimesForUpdate(queueName);
        return runtimes.isEmpty() ? null : runtimes[0];
    }

    public QueueRuntime__c lockRuntimeAsSystem(final String queueName) {
        List<QueueRuntime__c> runtimes = systemModeRepository.queryRuntimesForUpdate(queueName);
        return runtimes.isEmpty() ? null : runtimes[0];
    }

    public QueueRuntime__c lockRuntime(final String queueName, final Boolean asSystem) {
        List<QueueRuntime__c> runtimes = getRepository(asSystem).queryRuntimesForUpdate(queueName);
        return runtimes.isEmpty() ? null : runtimes[0];
    }

    public QueueRuntime__c incrementActiveWorkerCount(final QueueRuntime__c runtime) {
        if (runtime == null) {
            return null;
        }

        runtime.ActiveWorkerCount__c = QueuesSupport.toInteger(runtime.ActiveWorkerCount__c, 0) + 1;
        return runtime;
    }

    public QueueRuntime__c decrementActiveWorkerCount(final QueueRuntime__c runtime) {
        if (runtime == null) {
            return null;
        }

        Integer updatedActiveWorkerCount = QueuesSupport.toInteger(runtime.ActiveWorkerCount__c, 0) - 1;
        runtime.ActiveWorkerCount__c = Math.max(updatedActiveWorkerCount, 0);
        return runtime;
    }

    public Boolean shouldKeepWorker(
        final String queueName,
        final QueueRuntime__c runtime,
        final Datetime desiredWakeAt,
        final Datetime now
    ) {
        if (!isWorkerAlive(queueName, runtime, now)) {
            return false;
        }

        if (runtime.NextPumpAt__c == null) {
            return true;
        }

        return runtime.NextPumpAt__c <= desiredWakeAt;
    }

    public Boolean shouldClearStaleWorker(final String queueName, final QueueRuntime__c runtime, final Datetime now) {
        return String.isNotBlank(runtime?.WorkerToken__c) && !isWorkerAlive(queueName, runtime, now);
    }

    public Boolean updateWorkerJobId(final String queueName, final String workerToken, final String workerJobId) {
        QueueRuntime__c runtime = lockRuntimeAsSystem(queueName);
        if (runtime == null || !isCurrentWorker(runtime, workerToken)) {
            return false;
        }

        runtime.AsyncApexJobId__c = workerJobId;
        updateRuntimeAsSystem(runtime);
        return true;
    }

    public QueueRuntime__c updateRuntime(final QueueRuntime__c runtime) {
        userModeRepository.updateRuntime(runtime);
        return runtime;
    }

    public QueueRuntime__c updateRuntimeAsSystem(final QueueRuntime__c runtime) {
        systemModeRepository.updateRuntime(runtime);
        return runtime;
    }

    public QueueRuntime__c updateRuntime(final QueueRuntime__c runtime, final Boolean asSystem) {
        getRepository(asSystem).updateRuntime(runtime);
        return runtime;
    }

    public Boolean isCurrentWorker(final QueueRuntime__c runtime, final String workerToken) {
        return String.isNotBlank(workerToken) && runtime?.WorkerToken__c == workerToken;
    }

    public void clearWorker(final QueueRuntime__c runtime) {
        if (runtime == null) {
            return;
        }

        runtime.NextPumpAt__c = null;
        runtime.WorkerToken__c = null;
        runtime.AsyncApexJobId__c = null;
    }

    public QueueRuntime__c clearWorkerAndUpdate(final QueueRuntime__c runtime) {
        if (runtime == null) {
            return null;
        }

        clearWorker(runtime);
        return updateRuntime(runtime);
    }

    public QueueRuntime__c clearWorkerAndUpdateAsSystem(final QueueRuntime__c runtime) {
        if (runtime == null) {
            return null;
        }

        clearWorker(runtime);
        return updateRuntimeAsSystem(runtime);
    }

    // </editor-fold>

    // <editor-fold desc="Helpers">
    private QueuesRuntimeRepository getRepository(final Boolean asSystem) {
        return asSystem == true ? systemModeRepository : userModeRepository;
    }

    @TestVisible
    private Boolean isWorkerAlive(final String queueName, final QueueRuntime__c runtime, final Datetime now) {
        if (runtime == null || String.isBlank(runtime.WorkerToken__c) || now == null) {
            return false;
        }

        if (runtime.NextPumpAt__c == null) {
            return QueuesSupport.toInteger(runtime.ActiveWorkerCount__c, 0) > 0;
        }

        if (runtime.NextPumpAt__c > now) {
            return String.isBlank(runtime.AsyncApexJobId__c) || isScheduledWorkerJobAlive(runtime.AsyncApexJobId__c);
        }

        if (runtime.NextPumpAt__c >= now.addMinutes(-WORKER_DISPATCH_GRACE_MINUTES)) {
            return true;
        }

        if (String.isNotBlank(runtime.AsyncApexJobId__c)) {
            return isWorkerJobAlive(runtime.AsyncApexJobId__c);
        }

        Integer workerLivenessMinutes = resolveLeaseMinutes(definitionService.findQueueDefinitionAsSystem(queueName));
        Datetime workerLivenessCutoff = now.addMinutes(-workerLivenessMinutes);
        return runtime.LastPumpAt__c >= runtime.NextPumpAt__c && runtime.LastPumpAt__c >= workerLivenessCutoff;
    }

    private Boolean isWorkerJobAlive(final String workerJobId) {
        List<AsyncApexJob> jobs = asyncApexJobRepository.queryAsyncJobs(workerJobId);
        if (jobs.isEmpty()) {
            return false;
        }

        return ACTIVE_WORKER_JOB_STATUSES.contains(toAsyncJobStatus(jobs[0].Status));
    }

    private Boolean isScheduledWorkerJobAlive(final String workerJobId) {
        List<CronTrigger> scheduledJobs = scheduleRepository.queryScheduledJobById(
            workerJobId,
            TERMINAL_SCHEDULED_JOB_STATES
        );
        return !scheduledJobs.isEmpty();
    }

    private static Integer resolveLeaseMinutes(final QueueDefinition__mdt queueDefinition) {
        return QueuesSupport.toPositiveInteger(queueDefinition?.DefaultLeaseMinutes__c, DEFAULT_LEASE_MINUTES);
    }

    private static AsyncJobStatus toAsyncJobStatus(final String asyncJobStatusValue) {
        String normalizedAsyncJobStatusValue = QueuesSupport.normalizeEnumValue(asyncJobStatusValue);
        if (String.isBlank(normalizedAsyncJobStatusValue)) {
            return null;
        }
        try {
            return AsyncJobStatus.valueOf(normalizedAsyncJobStatusValue);
        } catch (Exception ex) {
            return null;
        }
    }

    private static Boolean isDuplicateRuntimeInsert(final DmlException ex) {
        for (Integer i = 0; i < ex.getNumDml(); i++) {
            System.StatusCode statusCode = ex.getDmlType(i);
            if (
                statusCode == System.StatusCode.DUPLICATE_VALUE ||
                statusCode == System.StatusCode.DUPLICATES_DETECTED
            ) {
                return true;
            }
        }

        return false;
    }
    // </editor-fold>

    private enum AsyncJobStatus {
        HOLDING,
        QUEUED,
        PREPARING,
        PROCESSING,
        ACQUIRED
    }
}
