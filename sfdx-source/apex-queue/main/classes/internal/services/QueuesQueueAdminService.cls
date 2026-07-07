/**
 * @description Queue-level administration: drain, cancel, and stalled recovery.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesQueueAdminService {
    private static final Integer DRAIN_JOB_BATCH_SIZE = 200;
    private static final Integer DRAIN_DML_ROW_HEADROOM = 500;
    private static final Integer DRAIN_DML_STATEMENT_HEADROOM = 10;
    private static final Integer DRAIN_QUERY_HEADROOM = 5;
    private static final Integer DRAIN_CPU_MS_HEADROOM = 2000;
    private static final String STALLED_JOB_MESSAGE = 'Lease expired before worker completed.';
    private static final String STALLED_JOB_EXHAUSTED_MESSAGE = 'Lease expired before worker completed and no more recovery attempts remain.';
    private static final String CANCELED_BY_QUEUE_CANCEL_REASON = 'Canceled by queue cancel.';
    private static final String STALLED_JOB_RECOVERY_COMPONENT = 'stalled job recovery';
    private static final String QUEUEABLE_DISPATCH_RECOVERY_COMPONENT = 'queueable dispatch recovery';
    private static final String QUEUEABLE_HANDOFF_RECOVERY_COMPONENT = 'queueable handoff recovery';
    private static final String QUEUEABLE_STALLED_JOB_RECOVERY_COMPONENT = 'queueable stalled job recovery';
    private static final String MANAGE_WORKERS_PERMISSION = 'QueuesManageWorkers';
    private static final String MANAGE_WORKERS_PERMISSION_MESSAGE = 'QueuesManageWorkers permission is required.';
    private static final Set<String> ACTIVE_QUEUEABLE_ASYNC_JOB_STATUSES = new Set<String>{
        'HOLDING',
        'QUEUED',
        'PREPARING',
        'PROCESSING',
        'ACQUIRED'
    };

    private QueuesDefinitionService definitionService = new QueuesDefinitionService();
    private QueuesRuntimeService runtimeService = new QueuesRuntimeService();
    private QueuesJobService jobService = new QueuesJobService();
    private QueuesJobRunService jobRunService = new QueuesJobRunService();
    private QueuesQueueableDispatchService queueableDispatchService = new QueuesQueueableDispatchService();
    private QueuesQueueableExecutionService queueableExecutionService = new QueuesQueueableExecutionService();
    private QueuesFlowProducerService flowProducerService = new QueuesFlowProducerService();
    private QueuesAsyncApexJobRepository asyncApexJobRepository = new QueuesAsyncApexJobRepository(
        System.AccessLevel.SYSTEM_MODE
    );
    private QueuesStalledJobRecoveryService stalledJobRecoveryService = new QueuesStalledJobRecoveryService();
    private QueuesWorkerService workerService = new QueuesWorkerService();
    private QueuesQueueEventService queueEventService = new QueuesQueueEventService();

    public QueuesQueueAdminService() {
    }

    @TestVisible
    private QueuesQueueAdminService(
        final QueuesDefinitionService definitionService,
        final QueuesRuntimeService runtimeService,
        final QueuesJobService jobService,
        final QueuesJobRunService jobRunService,
        final QueuesQueueableDispatchService queueableDispatchService,
        final QueuesQueueableExecutionService queueableExecutionService,
        final QueuesAsyncApexJobRepository asyncApexJobRepository,
        final QueuesStalledJobRecoveryService stalledJobRecoveryService,
        final QueuesFlowProducerService flowProducerService,
        final QueuesWorkerService workerService,
        final QueuesQueueEventService queueEventService
    ) {
        this.definitionService = definitionService;
        this.runtimeService = runtimeService;
        this.jobService = jobService;
        this.jobRunService = jobRunService;
        this.queueableDispatchService = queueableDispatchService;
        this.queueableExecutionService = queueableExecutionService;
        this.asyncApexJobRepository = asyncApexJobRepository;
        this.stalledJobRecoveryService = stalledJobRecoveryService;
        this.flowProducerService = flowProducerService;
        this.workerService = workerService;
        this.queueEventService = queueEventService;
    }

    // <editor-fold desc="Queue Administration">
    public QueueRuntime__c pause(final String queueName) {
        definitionService.requireQueueDefinition(queueName, false);
        runtimeService.ensureRuntime(queueName);
        QueueRuntime__c runtime = runtimeService.lockRuntime(queueName);
        workerService.cancelFutureScheduledWorker(queueName, runtime);
        runtime.IsPaused__c = true;
        return runtimeService.clearWorkerAndUpdate(runtime);
    }

    public QueueRuntime__c resume(final String queueName) {
        definitionService.requireQueueDefinition(queueName, true);
        runtimeService.ensureRuntime(queueName);
        QueueRuntime__c runtime = runtimeService.lockRuntime(queueName);
        runtime.IsPaused__c = false;
        runtime = runtimeService.updateRuntime(runtime);

        Datetime currentNow = QueuesSupport.now();
        workerService.ensureWorkerForNextWakeAt(queueName, currentNow);
        return runtime;
    }

    public Integer wake(final String queueName) {
        definitionService.requireQueueDefinition(queueName, true);
        return workerService.ensureWorker(queueName, QueuesSupport.now()) ? 1 : 0;
    }

    public Integer recoverStalled(final String queueName) {
        return recoverStalled(queueName, false);
    }

    public Integer recoverStalledAsSystem(final String queueName) {
        return recoverStalled(queueName, true);
    }

    public Integer drain(final String queueName) {
        return processDrainableJobs(
            queueName,
            new DeleteDrainableJobAction(queueName, jobService, flowProducerService)
        );
    }

    public Integer cancel(final String queueName) {
        return processDrainableJobs(
            queueName,
            new CancelDrainableJobAction(queueName, jobService, flowProducerService, queueEventService)
        );
    }

    public Queues.WorkerStatus getWorkerStatus(final String queueName) {
        definitionService.requireQueueDefinition(queueName, false);
        return resolveWorkerStatus(queueName, runtimeService.findRuntime(queueName));
    }

    public Boolean moveWorkerToFront(final String queueName) {
        return moveWorker(queueName, true);
    }

    public Boolean moveWorkerToEnd(final String queueName) {
        return moveWorker(queueName, false);
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private Boolean moveWorker(final String queueName, final Boolean toFront) {
        requireManageWorkersPermission();
        definitionService.requireQueueDefinition(queueName, false);
        Queues.WorkerStatus workerStatus = resolveWorkerStatus(queueName, runtimeService.lockRuntime(queueName));
        if (!workerStatus.isHolding()) {
            return false;
        }

        return toFront
            ? asyncApexJobRepository.moveJobToFront(workerStatus.getJobId())
            : asyncApexJobRepository.moveJobToEnd(workerStatus.getJobId());
    }

    private Queues.WorkerStatus resolveWorkerStatus(final String queueName, final QueueRuntime__c runtime) {
        Id workerJobId = QueuesSupport.toId(runtime?.AsyncApexJobId__c);
        List<AsyncApexJob> asyncJobs = asyncApexJobRepository.queryAsyncJobs(workerJobId);
        return new Queues.WorkerStatus(queueName, workerJobId, asyncJobs.isEmpty() ? null : asyncJobs[0].Status);
    }

    private static void requireManageWorkersPermission() {
        QueuesSupport.isTrue(
            FeatureManagement.checkPermission(MANAGE_WORKERS_PERMISSION),
            new Queues.QueueException(MANAGE_WORKERS_PERMISSION_MESSAGE)
        );
    }

    private Integer recoverStalled(final String queueName, final Boolean asSystem) {
        QueueDefinition__mdt queueDefinition = definitionService.requireQueueDefinition(queueName, false);
        Datetime currentNow = QueuesSupport.now();
        runtimeService.ensureRuntime(queueName, asSystem);
        QueueRuntime__c runtime = runtimeService.lockRuntime(queueName, asSystem);
        Integer recoveredCount = 0;
        while (stalledJobRecoveryService.hasHeadroomForRecovery()) {
            Integer recoverBatchSize = stalledJobRecoveryService.resolveRecoverStalledBatchSize();
            if (recoverBatchSize <= 0) {
                break;
            }

            Integer recoveredBatchCount = recoverStalledJobs(queueDefinition, currentNow, recoverBatchSize, asSystem);
            Integer remainingRecoverBatchSize = recoverBatchSize - recoveredBatchCount;
            if (remainingRecoverBatchSize > 0) {
                recoveredBatchCount += recoverStalledQueueableJobs(
                    queueDefinition,
                    currentNow,
                    remainingRecoverBatchSize,
                    asSystem
                );
            }
            if (recoveredBatchCount == 0) {
                break;
            }
            recoveredCount += recoveredBatchCount;
            if (recoveredBatchCount < recoverBatchSize) {
                break;
            }
        }
        runtime.LastSweepAt__c = currentNow;
        if (runtimeService.shouldClearStaleWorker(queueName, runtime, currentNow)) {
            runtimeService.clearWorker(runtime);
        }
        if (String.isBlank(runtime.WorkerToken__c)) {
            syncActiveLeaseMaintenanceHint(runtime, jobService.hasActiveJobs(queueName, asSystem));
        }
        runtimeService.updateRuntime(runtime, asSystem);

        if (QueuesSupport.isActive(queueDefinition) && !QueuesSupport.isPaused(runtime)) {
            workerService.syncWorkerForNextWakeAt(queueName, currentNow);
        }
        return recoveredCount;
    }

    private Integer processDrainableJobs(final String queueName, final DrainableJobAction action) {
        definitionService.requireQueueDefinition(queueName, false);
        Datetime currentNow = QueuesSupport.now();
        runtimeService.ensureRuntime(queueName);
        runtimeService.lockRuntime(queueName);
        Integer processedCount = 0;

        while (hasHeadroomForDrain()) {
            Integer remainingDmlRows = Limits.getLimitDmlRows() - Limits.getDmlRows();
            Integer drainJobBatchSize = resolveDrainJobBatchSize(remainingDmlRows);
            if (drainJobBatchSize <= 0) {
                break;
            }

            List<Job__c> jobs = jobService.queryDrainableJobsForUpdate(queueName, drainJobBatchSize);
            if (jobs.isEmpty()) {
                break;
            }

            DrainableJobActionResult result = action.apply(jobs);
            processedCount += result.processedCount;
            if (!result.continueProcessing || jobs.size() < drainJobBatchSize) {
                break;
            }
        }

        workerService.syncExistingWorkerForNextWakeAt(queueName, currentNow);
        return processedCount;
    }

    private Integer recoverStalledJobs(
        final QueueDefinition__mdt queueDefinition,
        final Datetime currentNow,
        final Integer jobLimit,
        final Boolean asSystem
    ) {
        List<Job__c> stalledJobs = jobService.queryExpiredActiveJobsForUpdate(
            queueDefinition.QueueName__c,
            currentNow,
            jobLimit,
            asSystem
        );
        if (stalledJobs.isEmpty()) {
            return 0;
        }

        List<JobRun__c> runs = new List<JobRun__c>();
        for (Job__c stalledJob : stalledJobs) {
            runs.add(
                stalledJobRecoveryService.recoverStalledJob(
                    stalledJob,
                    currentNow,
                    STALLED_JOB_MESSAGE,
                    STALLED_JOB_EXHAUSTED_MESSAGE
                )
            );
        }

        jobService.updateJobs(stalledJobs, asSystem);
        jobRunService.insertJobRuns(runs, asSystem);
        queueEventService.publishStallRecoveryJobEvents(stalledJobs, runs, STALLED_JOB_RECOVERY_COMPONENT);
        return stalledJobs.size();
    }

    private Integer recoverStalledQueueableJobs(
        final QueueDefinition__mdt queueDefinition,
        final Datetime currentNow,
        final Integer jobLimit,
        final Boolean asSystem
    ) {
        Integer queueableJobLimit = Math.min(jobLimit, QueuesSupport.remainingQueueableSubmissionSlots());
        if (queueableJobLimit <= 0) {
            return 0;
        }

        Integer leaseMinutes = QueuesSupport.toPositiveInteger(definitionService.getLeaseMinutes(queueDefinition), 1);
        Integer recoveredCount = recoverStaleQueueableDispatches(
            queueDefinition,
            currentNow.addMinutes(-leaseMinutes),
            queueableJobLimit,
            asSystem
        );
        Integer remainingJobLimit = queueableJobLimit - recoveredCount;
        if (remainingJobLimit <= 0) {
            return recoveredCount;
        }

        recoveredCount += recoverStaleInitiatedQueueableJobs(
            queueDefinition,
            currentNow,
            currentNow.addMinutes(-leaseMinutes),
            remainingJobLimit,
            asSystem
        );
        remainingJobLimit = queueableJobLimit - recoveredCount;
        if (remainingJobLimit <= 0) {
            return recoveredCount;
        }

        return recoveredCount +
            recoverExpiredActiveQueueableJobs(
                queueDefinition,
                currentNow,
                currentNow.addMinutes(-leaseMinutes),
                remainingJobLimit,
                asSystem
            );
    }

    private Integer recoverStaleQueueableDispatches(
        final QueueDefinition__mdt queueDefinition,
        final Datetime staleBefore,
        final Integer dispatchLimit,
        final Boolean asSystem
    ) {
        Integer recoveredCount = queueableDispatchService.recoverStaleDispatches(
            queueDefinition.QueueName__c,
            staleBefore,
            dispatchLimit,
            asSystem
        );
        queueEventService.publishRecovered(
            queueDefinition.QueueName__c,
            QUEUEABLE_DISPATCH_RECOVERY_COMPONENT,
            recoveredCount
        );
        return recoveredCount;
    }

    private Integer recoverStaleInitiatedQueueableJobs(
        final QueueDefinition__mdt queueDefinition,
        final Datetime currentNow,
        final Datetime initiatedBefore,
        final Integer jobLimit,
        final Boolean asSystem
    ) {
        List<Job__c> staleJobs = jobService.queryStaleInitiatedQueueableJobsForUpdate(
            queueDefinition.QueueName__c,
            initiatedBefore,
            currentNow,
            jobLimit,
            asSystem
        );
        if (staleJobs == null || staleJobs.isEmpty()) {
            return 0;
        }

        for (Job__c staleJob : staleJobs) {
            queueableExecutionService.enqueueJob(staleJob.Id);
        }
        queueEventService.publishRecoveredJobEvents(staleJobs, null, QUEUEABLE_HANDOFF_RECOVERY_COMPONENT);
        return staleJobs.size();
    }

    private Integer recoverExpiredActiveQueueableJobs(
        final QueueDefinition__mdt queueDefinition,
        final Datetime currentNow,
        final Datetime expiredBefore,
        final Integer jobLimit,
        final Boolean asSystem
    ) {
        List<Job__c> stalledJobs = jobService.queryExpiredActiveQueueableJobsForUpdate(
            queueDefinition.QueueName__c,
            expiredBefore,
            jobLimit,
            asSystem
        );
        if (stalledJobs == null || stalledJobs.isEmpty()) {
            return 0;
        }

        stalledJobs = filterRecoverableQueueableJobs(stalledJobs);
        if (stalledJobs.isEmpty()) {
            return 0;
        }

        List<JobRun__c> runs = new List<JobRun__c>();
        List<Id> recoveredJobIds = new List<Id>();
        for (Job__c stalledJob : stalledJobs) {
            runs.add(
                stalledJobRecoveryService.recoverStalledJob(
                    stalledJob,
                    currentNow,
                    STALLED_JOB_MESSAGE,
                    STALLED_JOB_EXHAUSTED_MESSAGE
                )
            );
            if (QueuesSupport.toState(stalledJob.State__c) == Queues.State.WAITING) {
                recoveredJobIds.add(stalledJob.Id);
            }
        }

        jobService.updateJobs(stalledJobs, asSystem);
        jobRunService.insertJobRuns(runs, asSystem);
        queueEventService.publishStallRecoveryJobEvents(stalledJobs, runs, QUEUEABLE_STALLED_JOB_RECOVERY_COMPONENT);
        for (Id recoveredJobId : recoveredJobIds) {
            queueableExecutionService.enqueueJob(recoveredJobId);
        }
        return stalledJobs.size();
    }

    private List<Job__c> filterRecoverableQueueableJobs(final List<Job__c> jobs) {
        Set<Id> asyncApexJobIds = QueuesSupport.toIdSet(jobs, 'AsyncApexJobId__c');
        if (asyncApexJobIds.isEmpty()) {
            return jobs;
        }

        Set<String> activeAsyncApexJobIds = new Set<String>();
        for (AsyncApexJob asyncJob : asyncApexJobRepository.queryAsyncJobsByIds(asyncApexJobIds)) {
            if (ACTIVE_QUEUEABLE_ASYNC_JOB_STATUSES.contains(QueuesSupport.normalizeEnumValue(asyncJob.Status))) {
                activeAsyncApexJobIds.add(asyncJob.Id);
            }
        }

        List<Job__c> recoverableJobs = new List<Job__c>();
        for (Job__c job : jobs) {
            if (String.isBlank(job.AsyncApexJobId__c) || !activeAsyncApexJobIds.contains(job.AsyncApexJobId__c)) {
                recoverableJobs.add(job);
            }
        }
        return recoverableJobs;
    }

    private static Boolean hasHeadroomForDrain() {
        QueuesSupport.GovernorHeadroom headroom = QueuesSupport.currentGovernorHeadroom();
        return hasHeadroomForDrain(headroom.dmlRows, headroom.dmlStatements, headroom.queries, headroom.cpuMs);
    }

    @TestVisible
    private static Boolean hasHeadroomForDrain(
        final Integer remainingDmlRows,
        final Integer remainingDmlStatements,
        final Integer remainingQueries,
        final Integer remainingCpuMs
    ) {
        return remainingDmlRows >= DRAIN_DML_ROW_HEADROOM &&
            remainingDmlStatements >= DRAIN_DML_STATEMENT_HEADROOM &&
            remainingQueries >= DRAIN_QUERY_HEADROOM &&
            remainingCpuMs >= DRAIN_CPU_MS_HEADROOM;
    }

    @TestVisible
    private static Integer resolveDrainJobBatchSize(final Integer remainingDmlRows) {
        Integer availableDmlRows = remainingDmlRows - DRAIN_DML_ROW_HEADROOM;
        if (availableDmlRows < 2) {
            return 0;
        }
        return Math.min(DRAIN_JOB_BATCH_SIZE, availableDmlRows - 1);
    }

    @TestVisible
    private static void syncActiveLeaseMaintenanceHint(final QueueRuntime__c runtime, final Boolean hasActiveJobs) {
        if (runtime == null || String.isNotBlank(runtime.WorkerToken__c)) {
            return;
        }
        if (hasActiveJobs == true) {
            // Keep the runtime discoverable until the remaining ACTIVE leases can expire and be recovered.
            runtime.ActiveWorkerCount__c = Math.max(QueuesSupport.toInteger(runtime.ActiveWorkerCount__c, 0), 1);
            return;
        }
        runtime.ActiveWorkerCount__c = 0;
    }

    private interface DrainableJobAction {
        DrainableJobActionResult apply(List<Job__c> jobs);
    }

    private inherited sharing class DeleteDrainableJobAction implements DrainableJobAction {
        private final String queueName;
        private final QueuesJobService jobService;
        private final QueuesFlowProducerService flowProducerService;

        private DeleteDrainableJobAction(
            final String queueName,
            final QueuesJobService jobService,
            final QueuesFlowProducerService flowProducerService
        ) {
            this.queueName = queueName;
            this.jobService = jobService;
            this.flowProducerService = flowProducerService;
        }

        public DrainableJobActionResult apply(final List<Job__c> jobs) {
            jobService.deleteJobs(jobs);
            flowProducerService.resolveRemovedParentDependenciesAsSystem(
                jobs,
                new Set<String>{ queueName },
                new Set<String>{ queueName }
            );
            return new DrainableJobActionResult(jobs.size(), true);
        }
    }

    private inherited sharing class CancelDrainableJobAction implements DrainableJobAction {
        private final String queueName;
        private final QueuesJobService jobService;
        private final QueuesFlowProducerService flowProducerService;
        private final QueuesQueueEventService queueEventService;

        private CancelDrainableJobAction(
            final String queueName,
            final QueuesJobService jobService,
            final QueuesFlowProducerService flowProducerService,
            final QueuesQueueEventService queueEventService
        ) {
            this.queueName = queueName;
            this.jobService = jobService;
            this.flowProducerService = flowProducerService;
            this.queueEventService = queueEventService;
        }

        public DrainableJobActionResult apply(final List<Job__c> jobs) {
            for (Job__c job : jobs) {
                QueuesJobService.applyCanceled(job, CANCELED_BY_QUEUE_CANCEL_REASON);
            }
            jobService.updateJobs(jobs);
            queueEventService.publishCanceledJobEvents(jobs);
            flowProducerService.resolveParentDependenciesAsSystem(
                jobs,
                new Set<String>{ queueName },
                new Set<String>{ queueName }
            );
            return new DrainableJobActionResult(jobs.size(), true);
        }
    }

    private class DrainableJobActionResult {
        private final Integer processedCount;
        private final Boolean continueProcessing;

        private DrainableJobActionResult(final Integer processedCount, final Boolean continueProcessing) {
            this.processedCount = processedCount;
            this.continueProcessing = continueProcessing;
        }
    }
    // </editor-fold>
}
