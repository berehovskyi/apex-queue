/**
 * @description Runs recovery and repair sweeps across queues.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesMaintenanceService {
    private static final String BATCH_WORKER_CLASS_NAME = QueuesBatchableWorker.class.getName();
    private static final Integer MINIMUM_ACTIVE_WORKER_COUNT = 0;
    private static final Integer ORPHANED_PROCESSING_ASYNC_WORKER_GRACE_MINUTES = 1;
    private static final Integer ORPHANED_PENDING_ASYNC_WORKER_GRACE_MINUTES = 15;
    private static final Integer MAINTENANCE_DML_ROW_HEADROOM = 1200;
    private static final Integer MAINTENANCE_DML_STATEMENT_HEADROOM = 10;
    private static final Integer MAINTENANCE_QUERY_HEADROOM = 25;
    private static final Integer MAINTENANCE_CPU_MS_HEADROOM = 2000;
    private static final Integer TERMINAL_CLEANUP_JOB_BATCH_SIZE = 200;
    private static final Integer TERMINAL_CLEANUP_DML_ROW_SLACK = 50;
    private static final Integer SECONDS_PER_DAY = 24 * 60 * 60;
    private static final String ORPHANED_ASYNC_WORKER_QUEUE_NAME = '[orphaned async worker]';
    private static final String ORPHANED_SCHEDULED_WORKER_QUEUE_NAME = '[orphaned scheduled worker]';
    private static final String RUNTIME_MAINTENANCE_COMPONENT = 'runtime maintenance';
    private static final String TERMINAL_CLEANUP_COMPONENT = 'terminal cleanup';
    private static final String TERMINAL_CLEANUP_DELETE_POLICY = 'DELETE';
    private static final String WORKER_JOB_NAME_PREFIX = 'Queues:';
    private static final Set<Queues.State> QUEUEABLE_RECOVERY_JOB_STATES = new Set<Queues.State>{ Queues.State.ACTIVE };
    private static final Set<Queues.State> STALE_INITIATED_QUEUEABLE_RECOVERY_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED
    };
    private static final Set<Queues.QueueableDispatchState> QUEUEABLE_DISPATCH_RECOVERY_STATES = new Set<Queues.QueueableDispatchState>{
        Queues.QueueableDispatchState.WAITING,
        Queues.QueueableDispatchState.ACTIVE
    };
    private static final Set<Queues.State> INVOCABLE_RECOVERY_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED,
        Queues.State.ACTIVE
    };
    private static final Set<Queues.State> INVOCABLE_RUNNABLE_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED
    };
    private static final Set<String> ACTIVE_ASYNC_WORKER_JOB_STATUSES = new Set<String>{
        'HOLDING',
        'QUEUED',
        'PREPARING',
        'PROCESSING',
        'ACQUIRED'
    };
    private static final Set<String> TERMINAL_SCHEDULED_JOB_STATES = new Set<String>{ 'DELETED', 'COMPLETE', 'ERROR' };
    private QueuesAsyncApexJobRepository asyncApexJobRepository = new QueuesAsyncApexJobRepository(
        System.AccessLevel.SYSTEM_MODE
    );
    private QueuesDefinitionService definitionService = new QueuesDefinitionService();
    private QueuesErrorService errorService = new QueuesErrorService();
    private QueuesJobRepository jobRepository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
    private QueuesQueueableDispatchRepository queueableDispatchRepository = new QueuesQueueableDispatchRepository(
        System.AccessLevel.SYSTEM_MODE
    );
    private QueuesInvocableExecutionService invocableExecutionService = new QueuesInvocableExecutionService();
    private QueuesFlowProducerService flowProducerService = new QueuesFlowProducerService();
    private QueuesQueueAdminService queueAdminService = new QueuesQueueAdminService();
    private QueuesRuntimeRepository runtimeRepository = new QueuesRuntimeRepository(System.AccessLevel.SYSTEM_MODE);
    private QueuesScheduleRepository scheduleRepository = new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE);
    @TestVisible
    private static Integer terminalCleanupDmlRowBudgetOverride;
    @TestVisible
    private static List<Boolean> recoveryHeadroomOverrides;

    public QueuesMaintenanceService() {
    }

    @TestVisible
    private QueuesMaintenanceService(
        final QueuesAsyncApexJobRepository asyncApexJobRepository,
        final QueuesDefinitionService definitionService,
        final QueuesErrorService errorService,
        final QueuesQueueAdminService queueAdminService,
        final QueuesRuntimeRepository runtimeRepository,
        final QueuesJobRepository jobRepository,
        final QueuesScheduleRepository scheduleRepository,
        final QueuesInvocableExecutionService invocableExecutionService,
        final QueuesQueueableDispatchRepository queueableDispatchRepository,
        final QueuesFlowProducerService flowProducerService
    ) {
        this.asyncApexJobRepository = asyncApexJobRepository;
        this.definitionService = definitionService;
        this.errorService = errorService;
        this.queueAdminService = queueAdminService;
        this.runtimeRepository = runtimeRepository;
        this.jobRepository = jobRepository;
        this.scheduleRepository = scheduleRepository;
        this.invocableExecutionService = invocableExecutionService;
        this.queueableDispatchRepository = queueableDispatchRepository;
        this.flowProducerService = flowProducerService;
    }

    // <editor-fold desc="Maintenance">
    @SuppressWarnings('PMD.OperationWithLimitsInLoop')
    public Queues.MaintenanceResult runRuntimeMaintenance() {
        Queues.MaintenanceResult result = new Queues.MaintenanceResult();
        if (!hasHeadroomForRecovery()) {
            result.markIncomplete();
            return result;
        }

        if (!abortOrphanedScheduledWorkers()) {
            result.markIncomplete();
            return result;
        }
        Set<String> runtimeQueueNames = queryRuntimeMaintenanceQueueNames();
        Set<String> invocableQueueNames = queryInvocableMaintenanceQueueNames();
        Set<String> dependencyQueueNames = queryDependencyMaintenanceQueueNames();
        Set<String> dependencyCleanupQueueNames = queryDependencyCleanupMaintenanceQueueNames();
        Set<String> queueNameSet = new Set<String>();
        queueNameSet.addAll(runtimeQueueNames);
        queueNameSet.addAll(invocableQueueNames);
        queueNameSet.addAll(dependencyQueueNames);
        queueNameSet.addAll(dependencyCleanupQueueNames);
        List<String> queueNames = new List<String>(queueNameSet);
        queueNames.sort();
        for (String queueName : queueNames) {
            if (!hasHeadroomForRecovery()) {
                result.markIncomplete();
                break;
            }

            QueueDefinition__mdt queueDefinition = definitionService.findQueueDefinitionAsSystem(queueName);
            if (queueDefinition == null) {
                continue;
            }

            Savepoint savepoint = Database.setSavepoint();
            try {
                Integer recoveredCount = 0;
                if (runtimeQueueNames.contains(queueName)) {
                    recoveredCount += queueAdminService.recoverStalledAsSystem(queueName);
                }
                if (invocableQueueNames.contains(queueName)) {
                    recoveredCount += invocableExecutionService.recoverAsSystem(
                        queueName,
                        QueuesSupport.now().addMinutes(-definitionService.getLeaseMinutes(queueDefinition))
                    );
                }
                if (dependencyQueueNames.contains(queueName)) {
                    recoveredCount += flowProducerService.repairStaleParentDependenciesAsSystem(
                        queueName,
                        QueuesSupport.now().addMinutes(-definitionService.getLeaseMinutes(queueDefinition))
                    );
                }
                if (dependencyCleanupQueueNames.contains(queueName)) {
                    recoveredCount += flowProducerService.repairStaleRemovedFailureDependenciesAsSystem(
                        queueName,
                        QueuesSupport.now().addMinutes(-definitionService.getLeaseMinutes(queueDefinition))
                    );
                }
                result.add(queueName, recoveredCount);
            } catch (Exception ex) {
                Database.rollback(savepoint);
                errorService.recordQueueError(queueName, RUNTIME_MAINTENANCE_COMPONENT, ex);
                result.markIncomplete();
            }
        }

        return result;
    }

    @SuppressWarnings('PMD.OperationWithLimitsInLoop')
    public Queues.MaintenanceResult runTerminalCleanupMaintenance() {
        Queues.MaintenanceResult result = new Queues.MaintenanceResult();
        if (!hasHeadroomForRecovery()) {
            result.markIncomplete();
            return result;
        }

        List<String> queueNames = new List<String>(queryTerminalCleanupMaintenanceQueueNames());
        queueNames.sort();
        for (String queueName : queueNames) {
            if (!hasHeadroomForRecovery()) {
                result.markIncomplete();
                break;
            }

            QueueDefinition__mdt queueDefinition = definitionService.findQueueDefinitionAsSystem(queueName);
            if (!shouldDeleteTerminalJobs(queueDefinition)) {
                continue;
            }

            Savepoint savepoint = Database.setSavepoint();
            try {
                TerminalCleanupCutoffs cutoffs = resolveTerminalCleanupCutoffs(queueDefinition, QueuesSupport.now());
                TerminalCleanupResult cleanupResult = deleteExpiredTerminalJobs(queueName, cutoffs);
                result.add(queueName, cleanupResult.deletedCount);
                if (!cleanupResult.completed) {
                    result.markIncomplete();
                }
            } catch (Exception ex) {
                Database.rollback(savepoint);
                errorService.recordQueueError(queueName, TERMINAL_CLEANUP_COMPONENT, ex);
                result.markIncomplete();
            }
        }

        return result;
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    @TestVisible
    private Set<String> queryTerminalCleanupMaintenanceQueueNames() {
        return QueuesSupport.toStringSet(jobRepository.queryTerminalCleanupMaintenanceQueueNames(), 'queueName');
    }

    @TestVisible
    private Set<String> queryRuntimeMaintenanceQueueNames() {
        Datetime currentNow = QueuesSupport.now();
        Set<String> queueNames = QueuesSupport.toStringSet(
            runtimeRepository.queryMaintenanceRuntimes(MINIMUM_ACTIVE_WORKER_COUNT),
            'QueueName__c'
        );
        addRecoveryCandidateQueueNames(
            queueNames,
            jobRepository.queryQueueableMaintenanceJobQueueNames(
                QueuesSupport.toStateValues(QUEUEABLE_RECOVERY_JOB_STATES)
            ),
            currentNow,
            RecoveryTransport.QUEUEABLE
        );
        addRecoveryCandidateQueueNames(
            queueNames,
            jobRepository.queryStaleInitiatedQueueableMaintenanceJobQueueNames(
                QueuesSupport.toStateValues(STALE_INITIATED_QUEUEABLE_RECOVERY_JOB_STATES)
            ),
            currentNow,
            RecoveryTransport.STALE_INITIATED_QUEUEABLE
        );
        addRecoveryCandidateQueueNames(
            queueNames,
            queueableDispatchRepository.queryStaleDispatchMaintenanceQueueNames(
                QueuesSupport.toQueueableDispatchStateValues(QUEUEABLE_DISPATCH_RECOVERY_STATES)
            ),
            currentNow,
            RecoveryTransport.QUEUEABLE_DISPATCH
        );
        addRecoveryCandidateQueueNames(
            queueNames,
            queueableDispatchRepository.queryUnsubmittedWaitingDispatchMaintenanceQueueNames(),
            currentNow,
            RecoveryTransport.QUEUEABLE_DISPATCH
        );
        return queueNames;
    }

    @TestVisible
    private Set<String> queryInvocableMaintenanceQueueNames() {
        return addRecoveryCandidateQueueNames(
            new Set<String>(),
            jobRepository.queryInvocableMaintenanceJobQueueNames(
                QueuesSupport.toStateValues(INVOCABLE_RECOVERY_JOB_STATES)
            ),
            QueuesSupport.now(),
            RecoveryTransport.INVOCABLE
        );
    }

    @TestVisible
    private Set<String> queryDependencyMaintenanceQueueNames() {
        return addRecoveryCandidateQueueNames(
            new Set<String>(),
            jobRepository.queryWaitingChildrenMaintenanceJobQueueNames(),
            QueuesSupport.now(),
            RecoveryTransport.DEPENDENCY
        );
    }

    @TestVisible
    private Set<String> queryDependencyCleanupMaintenanceQueueNames() {
        return addRecoveryCandidateQueueNames(
            new Set<String>(),
            jobRepository.queryRemovedDependencyMaintenanceJobQueueNames(),
            QueuesSupport.now(),
            RecoveryTransport.DEPENDENCY_CLEANUP
        );
    }

    private Set<String> addRecoveryCandidateQueueNames(
        final Set<String> queueNames,
        final List<AggregateResult> queueNameRows,
        final Datetime currentNow,
        final RecoveryTransport transport
    ) {
        Set<String> resolvedQueueNames = queueNames ?? new Set<String>();
        for (AggregateResult queueNameRow : queueNameRows ?? new List<AggregateResult>()) {
            if (!hasHeadroomForRecovery()) {
                break;
            }

            String queueName = (String) queueNameRow?.get('queueName');
            if (String.isNotBlank(queueName) && hasExpiredRecoveryCandidate(queueName, currentNow, transport)) {
                resolvedQueueNames.add(queueName);
            }
        }
        return resolvedQueueNames;
    }

    private Boolean hasExpiredRecoveryCandidate(
        final String queueName,
        final Datetime currentNow,
        final RecoveryTransport transport
    ) {
        QueueDefinition__mdt queueDefinition = definitionService.findQueueDefinitionAsSystem(queueName);
        if (queueDefinition == null) {
            return false;
        }

        Datetime expiredBefore = currentNow.addMinutes(-definitionService.getLeaseMinutes(queueDefinition));
        if (transport == RecoveryTransport.INVOCABLE) {
            return jobRepository.hasRecoverableInvocableJob(
                queueName,
                Queues.State.ACTIVE.name(),
                QueuesSupport.toStateValues(INVOCABLE_RUNNABLE_JOB_STATES),
                currentNow,
                expiredBefore
            );
        }
        if (transport == RecoveryTransport.DEPENDENCY) {
            return jobRepository.hasStaleWaitingChildrenJob(queueName, expiredBefore);
        }
        if (transport == RecoveryTransport.DEPENDENCY_CLEANUP) {
            return jobRepository.hasStaleRemovedDependency(queueName, expiredBefore);
        }
        if (transport == RecoveryTransport.QUEUEABLE) {
            return jobRepository.hasExpiredActiveQueueableJob(
                queueName,
                QueuesSupport.toStateValues(QUEUEABLE_RECOVERY_JOB_STATES),
                expiredBefore
            );
        }
        if (transport == RecoveryTransport.QUEUEABLE_DISPATCH) {
            return queueableDispatchRepository.hasStaleDispatch(
                queueName,
                QueuesSupport.toQueueableDispatchStateValues(QUEUEABLE_DISPATCH_RECOVERY_STATES),
                expiredBefore
            );
        }
        return jobRepository.hasStaleInitiatedQueueableJob(
            queueName,
            QueuesSupport.toStateValues(STALE_INITIATED_QUEUEABLE_RECOVERY_JOB_STATES),
            expiredBefore,
            currentNow
        );
    }

    private Boolean abortOrphanedScheduledWorkers() {
        return abortOrphanedWorkerArtifacts(queryReferencedWorkerJobIds(), QueuesSupport.now());
    }

    private Boolean abortOrphanedWorkerArtifacts(final Set<String> referencedWorkerJobIds, final Datetime currentNow) {
        return abortOrphanedAsyncWorkers(referencedWorkerJobIds, currentNow) &&
            abortOrphanedScheduledWorkers(referencedWorkerJobIds);
    }

    private Set<String> queryReferencedWorkerJobIds() {
        return QueuesSupport.toStringSet(runtimeRepository.queryRuntimesWithWorkerJobs(), 'AsyncApexJobId__c');
    }

    private Boolean abortOrphanedAsyncWorkers(final Set<String> referencedWorkerJobIds, final Datetime currentNow) {
        for (
            AsyncApexJob asyncWorker : asyncApexJobRepository.queryActiveAsyncJobsByApexClass(
                BATCH_WORKER_CLASS_NAME,
                ACTIVE_ASYNC_WORKER_JOB_STATUSES
            )
        ) {
            if (!hasHeadroomForRecovery()) {
                return false;
            }

            String asyncWorkerJobId = asyncWorker.Id;
            if (
                String.isBlank(asyncWorkerJobId) ||
                referencedWorkerJobIds.contains(asyncWorkerJobId) ||
                asyncWorker.CreatedDate == null ||
                asyncWorker.CreatedDate >= currentNow.addMinutes(-resolveOrphanedAsyncWorkerGraceMinutes(asyncWorker))
            ) {
                continue;
            }

            try {
                scheduleRepository.abortJob(asyncWorkerJobId);
            } catch (Exception ex) {
                if (hasActiveAsyncWorker(asyncWorkerJobId)) {
                    recordOrphanedAsyncWorkerAbortFailure(asyncWorkerJobId, ex);
                }
            }
        }
        return true;
    }

    private static Integer resolveOrphanedAsyncWorkerGraceMinutes(final AsyncApexJob asyncWorker) {
        return QueuesSupport.normalizeEnumValue(asyncWorker.Status) == 'PROCESSING'
            ? ORPHANED_PROCESSING_ASYNC_WORKER_GRACE_MINUTES
            : ORPHANED_PENDING_ASYNC_WORKER_GRACE_MINUTES;
    }

    private Boolean abortOrphanedScheduledWorkers(final Set<String> referencedWorkerJobIds) {
        for (
            CronTrigger scheduledWorker : scheduleRepository.queryScheduledJobsByNamePrefix(
                WORKER_JOB_NAME_PREFIX,
                TERMINAL_SCHEDULED_JOB_STATES
            )
        ) {
            if (!hasHeadroomForRecovery()) {
                return false;
            }

            Id scheduledJobId = scheduledWorker.Id;
            if (String.isBlank(scheduledJobId) || referencedWorkerJobIds.contains(scheduledJobId)) {
                continue;
            }

            String queueName = resolveWorkerQueueName(scheduledWorker.CronJobDetail.Name);
            try {
                scheduleRepository.abortJob(scheduledJobId);
            } catch (Exception ex) {
                if (String.isNotBlank(queueName)) {
                    errorService.recordQueueError(queueName, RUNTIME_MAINTENANCE_COMPONENT, null, scheduledJobId, ex);
                    continue;
                }

                recordOrphanedScheduledWorkerAbortFailure(scheduledJobId, ex);
            }
        }
        return true;
    }

    private Boolean hasActiveAsyncWorker(final String asyncJobId) {
        List<AsyncApexJob> asyncJobs = asyncApexJobRepository.queryAsyncJobs(asyncJobId);
        return !asyncJobs.isEmpty() &&
            ACTIVE_ASYNC_WORKER_JOB_STATUSES.contains(QueuesSupport.normalizeEnumValue(asyncJobs[0].Status));
    }

    private void recordOrphanedAsyncWorkerAbortFailure(final String asyncApexJobId, final Exception ex) {
        recordOrphanedWorkerAbortFailure(ORPHANED_ASYNC_WORKER_QUEUE_NAME, asyncApexJobId, 'batch worker', ex);
    }

    private void recordOrphanedScheduledWorkerAbortFailure(final String scheduledJobId, final Exception ex) {
        recordOrphanedWorkerAbortFailure(ORPHANED_SCHEDULED_WORKER_QUEUE_NAME, scheduledJobId, 'scheduled worker', ex);
    }

    private void recordOrphanedWorkerAbortFailure(
        final String queueName,
        final String workerJobId,
        final String workerType,
        final Exception ex
    ) {
        try {
            errorService.insertQueueErrorAsSystem(
                new QueueError__c(
                    QueueName__c = queueName,
                    Component__c = RUNTIME_MAINTENANCE_COMPONENT,
                    AsyncApexJobId__c = workerJobId,
                    Message__c = QueuesSupport.resolveExceptionReason(ex),
                    StackTrace__c = ex.getStackTraceString(),
                    OccurredAt__c = QueuesSupport.now()
                )
            );
        } catch (Exception loggingEx) {
            QueuesSupport.debugError(
                'Failed to log orphaned ' +
                    workerType +
                    ' abort failure ' +
                    workerJobId +
                    ': ' +
                    loggingEx.getMessage() +
                    '\n' +
                    loggingEx.getStackTraceString()
            );
        }
    }

    private TerminalCleanupResult deleteExpiredTerminalJobs(
        final String queueName,
        final TerminalCleanupCutoffs cutoffs
    ) {
        TerminalCleanupResult result = new TerminalCleanupResult();
        if (cutoffs == null || !cutoffs.hasAnyCutoff()) {
            return result;
        }

        while (hasHeadroomForRecovery()) {
            Integer jobLimit = resolveTerminalCleanupJobLimit();
            if (jobLimit <= 0) {
                result.completed = false;
                return result;
            }

            List<Job__c> candidates = jobRepository.queryExpiredTerminalCleanupJobsForUpdate(
                queueName,
                cutoffs.completedCutoff,
                cutoffs.failedCutoff,
                cutoffs.canceledCutoff,
                jobLimit
            );
            if (candidates.isEmpty()) {
                return result;
            }

            List<Job__c> jobsToDelete = filterEligibleTerminalCleanupJobs(candidates, cutoffs);
            if (jobsToDelete.isEmpty()) {
                result.completed = false;
                return result;
            }

            jobRepository.deleteJobs(jobsToDelete);
            result.deletedCount += jobsToDelete.size();
        }

        result.completed = false;
        return result;
    }

    private static List<Job__c> filterEligibleTerminalCleanupJobs(
        final List<Job__c> candidates,
        final TerminalCleanupCutoffs cutoffs
    ) {
        List<Job__c> eligibleJobs = new List<Job__c>();
        for (Job__c candidate : candidates ?? new List<Job__c>()) {
            if (isTerminalCleanupEligible(candidate, cutoffs)) {
                eligibleJobs.add(candidate);
            }
        }
        return eligibleJobs;
    }

    private static Boolean shouldDeleteTerminalJobs(final QueueDefinition__mdt queueDefinition) {
        return QueuesSupport.normalizeEnumValue(queueDefinition?.TerminalCleanupPolicy__c) ==
            TERMINAL_CLEANUP_DELETE_POLICY;
    }

    private static TerminalCleanupCutoffs resolveTerminalCleanupCutoffs(
        final QueueDefinition__mdt queueDefinition,
        final Datetime currentNow
    ) {
        return new TerminalCleanupCutoffs(
            resolveRetentionCutoff(queueDefinition?.CompletedRetentionDays__c, currentNow, 'CompletedRetentionDays__c'),
            resolveRetentionCutoff(queueDefinition?.FailedRetentionDays__c, currentNow, 'FailedRetentionDays__c'),
            resolveRetentionCutoff(queueDefinition?.CanceledRetentionDays__c, currentNow, 'CanceledRetentionDays__c')
        );
    }

    private static Datetime resolveRetentionCutoff(
        final Object retentionDaysValue,
        final Datetime currentNow,
        final String fieldName
    ) {
        Integer retentionDays = QueuesSupport.toInteger(retentionDaysValue, null);
        if (retentionDays == null) {
            return null;
        }
        QueuesSupport.isTrue(
            retentionDays >= 0,
            new Queues.ConfigurationException(fieldName + ' must be zero or greater.')
        );
        return currentNow.addSeconds(-retentionDays * SECONDS_PER_DAY);
    }

    private static Integer resolveTerminalCleanupJobLimit() {
        return Math.min(TERMINAL_CLEANUP_JOB_BATCH_SIZE, Math.max(resolveTerminalCleanupDmlRowBudget(), 0));
    }

    private static Integer resolveTerminalCleanupDmlRowBudget() {
        if (terminalCleanupDmlRowBudgetOverride != null) {
            return terminalCleanupDmlRowBudgetOverride;
        }
        return QueuesSupport.currentGovernorHeadroom().dmlRows -
            MAINTENANCE_DML_ROW_HEADROOM -
            TERMINAL_CLEANUP_DML_ROW_SLACK;
    }

    private static Boolean isTerminalCleanupEligible(final Job__c job, final TerminalCleanupCutoffs cutoffs) {
        if (cutoffs == null || job?.LastModifiedDate == null) {
            return false;
        }

        Queues.State state = QueuesSupport.toState(job.State__c);
        if (state == Queues.State.COMPLETED) {
            return cutoffs.completedCutoff != null && job.LastModifiedDate <= cutoffs.completedCutoff;
        }
        if (state == Queues.State.FAILED) {
            return cutoffs.failedCutoff != null && job.LastModifiedDate <= cutoffs.failedCutoff;
        }
        if (state == Queues.State.CANCELED) {
            return cutoffs.canceledCutoff != null && job.LastModifiedDate <= cutoffs.canceledCutoff;
        }
        return false;
    }

    private static Boolean hasHeadroomForRecovery() {
        if (recoveryHeadroomOverrides != null && !recoveryHeadroomOverrides.isEmpty()) {
            return recoveryHeadroomOverrides.remove(0);
        }

        QueuesSupport.GovernorHeadroom headroom = QueuesSupport.currentGovernorHeadroom();
        return hasHeadroomForRecovery(headroom.dmlRows, headroom.dmlStatements, headroom.queries, headroom.cpuMs);
    }

    @TestVisible
    private static Boolean hasHeadroomForRecovery(
        final Integer remainingDmlRows,
        final Integer remainingDmlStatements,
        final Integer remainingQueries,
        final Integer remainingCpuMs
    ) {
        return remainingDmlRows >= MAINTENANCE_DML_ROW_HEADROOM &&
            remainingDmlStatements >= MAINTENANCE_DML_STATEMENT_HEADROOM &&
            remainingQueries >= MAINTENANCE_QUERY_HEADROOM &&
            remainingCpuMs >= MAINTENANCE_CPU_MS_HEADROOM;
    }

    @TestVisible
    private static String resolveWorkerQueueName(final String workerJobName) {
        if (String.isBlank(workerJobName)) {
            return null;
        }

        Matcher workerJobNameMatcher = Pattern.compile('^Queues:(.+):\\d+:[^:]+(?::[^:]+)?$').matcher(workerJobName);
        return workerJobNameMatcher.matches() ? workerJobNameMatcher.group(1) : null;
    }
    // </editor-fold>

    private enum RecoveryTransport {
        QUEUEABLE,
        QUEUEABLE_DISPATCH,
        STALE_INITIATED_QUEUEABLE,
        DEPENDENCY,
        DEPENDENCY_CLEANUP,
        INVOCABLE
    }

    private class TerminalCleanupCutoffs {
        private final Datetime completedCutoff;
        private final Datetime failedCutoff;
        private final Datetime canceledCutoff;

        private TerminalCleanupCutoffs(
            final Datetime completedCutoff,
            final Datetime failedCutoff,
            final Datetime canceledCutoff
        ) {
            this.completedCutoff = completedCutoff;
            this.failedCutoff = failedCutoff;
            this.canceledCutoff = canceledCutoff;
        }

        private Boolean hasAnyCutoff() {
            return completedCutoff != null || failedCutoff != null || canceledCutoff != null;
        }
    }

    private class TerminalCleanupResult {
        private Integer deletedCount = 0;
        private Boolean completed = true;
    }
}
