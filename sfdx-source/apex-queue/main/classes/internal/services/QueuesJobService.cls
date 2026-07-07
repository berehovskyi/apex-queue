/**
 * @description Core job persistence, state transitions, and queries.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesJobService {
    private static final String JOB_NOT_FOUND_MESSAGE = 'Queue job "{0}" was not found.';
    private static final String NULL_PROCESS_RESULT_MESSAGE = 'Processor returned a null ProcessResult.';
    private static final String INVALID_JITTER_MESSAGE = 'jitter must be between 0 and 1';
    private static final Set<Queues.State> DUE_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED
    };
    private static final Set<Queues.State> DRAINABLE_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED,
        Queues.State.PAUSED,
        Queues.State.WAITING_CHILDREN
    };
    private static final Set<Queues.State> STALE_INITIATED_QUEUEABLE_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED
    };

    private QueuesJobRepository userModeRepository = new QueuesJobRepository();
    private QueuesJobRepository systemModeRepository = new QueuesJobRepository(System.AccessLevel.SYSTEM_MODE);
    private QueuesDefinitionService definitionService = new QueuesDefinitionService();

    public QueuesJobService() {
    }

    @TestVisible
    private QueuesJobService(final QueuesJobRepository repository, final QueuesDefinitionService definitionService) {
        this.userModeRepository = repository;
        this.systemModeRepository = repository;
        this.definitionService = definitionService;
    }

    // <editor-fold desc="Jobs">
    public List<Job__c> queryDueJobsAsSystem(
        final String queueName,
        final Set<Queues.State> states,
        final Datetime availableBy,
        final Integer jobLimit
    ) {
        return systemModeRepository.queryDueJobs(queueName, QueuesSupport.toStateValues(states), availableBy, jobLimit);
    }

    public Map<String, Job__c> getJobsByExternalId(final String queueName, final List<Queues.JobData> jobs) {
        return getJobsByExternalId(queueName, toExternalIds(jobs));
    }

    public Map<String, Job__c> getJobsByExternalId(final String queueName, final Set<String> externalIds) {
        return getJobsByExternalId(queueName, externalIds, false);
    }

    public Map<String, Job__c> getJobsByExternalId(
        final String queueName,
        final Set<String> externalIds,
        final Boolean asSystem
    ) {
        Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
        for (Job__c existingJob : getRepository(asSystem).queryJobsByExternalIds(queueName, externalIds)) {
            String externalIdKey = QueuesSupport.normalizeCaseInsensitiveKey(existingJob.ExternalId__c);
            if (!jobsByExternalId.containsKey(externalIdKey)) {
                jobsByExternalId.put(externalIdKey, existingJob);
            }
        }

        return jobsByExternalId;
    }

    public Job__c findNextDispatchJobAsSystem(final String queueName, final Datetime desiredWakeAt) {
        List<Job__c> jobs = queryDueJobsAsSystem(queueName, DUE_JOB_STATES, desiredWakeAt, 1);
        return jobs.isEmpty() ? null : jobs[0];
    }

    public List<Job__c> queryExpiredActiveJobsForUpdate(
        final String queueName,
        final Datetime expiredBefore,
        final Integer jobLimit
    ) {
        return userModeRepository.queryExpiredActiveJobsForUpdate(
            queueName,
            Queues.State.ACTIVE.name(),
            expiredBefore,
            jobLimit
        );
    }

    public List<Job__c> queryExpiredActiveJobsForUpdate(
        final String queueName,
        final Datetime expiredBefore,
        final Integer jobLimit,
        final Boolean asSystem
    ) {
        return getRepository(asSystem)
            .queryExpiredActiveJobsForUpdate(queueName, Queues.State.ACTIVE.name(), expiredBefore, jobLimit);
    }

    public List<Job__c> queryExpiredActiveQueueableJobsForUpdate(
        final String queueName,
        final Datetime expiredBefore,
        final Integer jobLimit
    ) {
        return userModeRepository.queryExpiredActiveQueueableJobsForUpdate(
            queueName,
            Queues.State.ACTIVE.name(),
            expiredBefore,
            jobLimit
        );
    }

    public List<Job__c> queryExpiredActiveQueueableJobsForUpdate(
        final String queueName,
        final Datetime expiredBefore,
        final Integer jobLimit,
        final Boolean asSystem
    ) {
        return getRepository(asSystem)
            .queryExpiredActiveQueueableJobsForUpdate(queueName, Queues.State.ACTIVE.name(), expiredBefore, jobLimit);
    }

    public List<Job__c> queryStaleInitiatedQueueableJobsForUpdate(
        final String queueName,
        final Datetime initiatedBefore,
        final Datetime availableBy,
        final Integer jobLimit
    ) {
        return userModeRepository.queryStaleInitiatedQueueableJobsForUpdate(
            queueName,
            QueuesSupport.toStateValues(STALE_INITIATED_QUEUEABLE_JOB_STATES),
            initiatedBefore,
            availableBy,
            jobLimit
        );
    }

    public List<Job__c> queryStaleInitiatedQueueableJobsForUpdateAsSystem(
        final String queueName,
        final Datetime initiatedBefore,
        final Datetime availableBy,
        final Integer jobLimit
    ) {
        return systemModeRepository.queryStaleInitiatedQueueableJobsForUpdate(
            queueName,
            QueuesSupport.toStateValues(STALE_INITIATED_QUEUEABLE_JOB_STATES),
            initiatedBefore,
            availableBy,
            jobLimit
        );
    }

    public List<Job__c> queryStaleInitiatedQueueableJobsForUpdate(
        final String queueName,
        final Datetime initiatedBefore,
        final Datetime availableBy,
        final Integer jobLimit,
        final Boolean asSystem
    ) {
        return getRepository(asSystem)
            .queryStaleInitiatedQueueableJobsForUpdate(
                queueName,
                QueuesSupport.toStateValues(STALE_INITIATED_QUEUEABLE_JOB_STATES),
                initiatedBefore,
                availableBy,
                jobLimit
            );
    }

    public List<Job__c> queryExpiredActiveInvocableJobsForUpdateAsSystem(
        final String queueName,
        final Datetime expiredBefore,
        final Integer jobLimit
    ) {
        return systemModeRepository.queryExpiredActiveInvocableJobsForUpdate(
            queueName,
            Queues.State.ACTIVE.name(),
            expiredBefore,
            jobLimit
        );
    }

    public List<Job__c> queryStaleDueInvocableJobsForUpdateAsSystem(
        final String queueName,
        final Datetime staleBefore,
        final Integer jobLimit
    ) {
        return systemModeRepository.queryStaleDueInvocableJobsForUpdate(
            queueName,
            QueuesSupport.toStateValues(DUE_JOB_STATES),
            staleBefore,
            jobLimit
        );
    }

    public List<Job__c> queryDrainableJobsForUpdate(final String queueName, final Integer jobLimit) {
        return userModeRepository.queryDrainableJobsForUpdate(
            queueName,
            QueuesSupport.toStateValues(DRAINABLE_JOB_STATES),
            jobLimit
        );
    }

    public Boolean hasJobsBySchedulerId(final Id schedulerId, final Set<Queues.State> states) {
        return userModeRepository.hasJobsBySchedulerId(schedulerId, QueuesSupport.toStateValues(states));
    }

    public List<Job__c> queryJobsBySchedulerIdForUpdate(final Id schedulerId, final Integer jobLimit) {
        return userModeRepository.queryJobsBySchedulerIdForUpdate(schedulerId, null, jobLimit);
    }

    public List<Job__c> queryJobsBySchedulerIdForUpdate(
        final Id schedulerId,
        final Set<Queues.State> states,
        final Integer jobLimit
    ) {
        return userModeRepository.queryJobsBySchedulerIdForUpdate(
            schedulerId,
            QueuesSupport.toStateValues(states),
            jobLimit
        );
    }

    public List<Job__c> claimBatchScopeJobsAsSystem(
        final QueueDefinition__mdt queueDefinition,
        final List<Job__c> scopeJobs,
        final Datetime now,
        final String asyncApexJobId
    ) {
        Set<Id> jobIds = QueuesSupport.toIdSet(scopeJobs);
        Map<Id, Job__c> currentJobsById = new Map<Id, Job__c>(queryJobsForUpdateAsSystem(jobIds));
        List<Job__c> claimedJobs = new List<Job__c>();
        for (Job__c scopeJob : scopeJobs) {
            Job__c currentJob = currentJobsById.get(scopeJob?.Id);
            if (
                currentJob == null ||
                currentJob.QueueName__c != queueDefinition.QueueName__c ||
                !isRunnableState(currentJob.State__c) ||
                currentJob.AvailableAt__c == null ||
                currentJob.AvailableAt__c > now
            ) {
                continue;
            }

            claimJob(currentJob, queueDefinition, now, asyncApexJobId);
            claimedJobs.add(currentJob);
        }

        updateJobsAsSystem(claimedJobs);
        return claimedJobs;
    }

    public List<Job__c> claimInvocableJobsAsSystem(final Set<Id> jobIds, final Datetime now) {
        QueuesSupport.notNull(jobIds, 'jobIds');
        QueuesSupport.notNull(now, 'now');
        if (jobIds.isEmpty()) {
            return new List<Job__c>();
        }

        List<Job__c> claimedJobs = new List<Job__c>();
        Set<String> queueNames = new Set<String>();
        for (Job__c currentJob : queryJobsForUpdateAsSystem(jobIds)) {
            if (isClaimableInvocableJob(currentJob, now)) {
                claimedJobs.add(currentJob);
                queueNames.add(currentJob.QueueName__c);
            }
        }
        if (claimedJobs.isEmpty()) {
            return claimedJobs;
        }

        Map<String, QueueDefinition__mdt> definitionsByQueueName = definitionService
            .findQueueDefinitionsByQueueNamesAsSystem(queueNames);
        for (Job__c claimedJob : claimedJobs) {
            QueueDefinition__mdt queueDefinition = definitionsByQueueName.get(claimedJob.QueueName__c);
            if (queueDefinition == null) {
                queueDefinition = definitionService.requireQueueDefinition(claimedJob.QueueName__c, false);
                definitionsByQueueName.put(claimedJob.QueueName__c, queueDefinition);
            }

            claimJob(claimedJob, queueDefinition, now, null);
        }
        return updateJobsAsSystem(claimedJobs);
    }

    public Database.QueryLocator queryClaimedJobLocatorAsSystem(final List<Job__c> claimedJobs) {
        Set<Id> jobIds = QueuesSupport.toIdSet(claimedJobs);
        return systemModeRepository.queryClaimedJobLocator(jobIds);
    }

    public Database.QueryLocator emptyJobLocator() {
        return systemModeRepository.queryEmptyJobLocator();
    }

    public Datetime findNextWakeAtAsSystem(final String queueName, final Datetime now) {
        if (hasDueJobsAsSystem(queueName, now)) {
            return now;
        }
        return queryNextPumpAtAsSystem(queueName, now);
    }

    public Datetime queryNextPumpAtAsSystem(final String queueName, final Datetime availableAfter) {
        List<AggregateResult> rows = systemModeRepository
            .queryNextPumpAt(queueName, QueuesSupport.toStateValues(DUE_JOB_STATES), availableAfter);

        return rows.isEmpty() ? null : (Datetime) rows[0].get('nextPumpAt');
    }

    public Map<String, Integer> queryQueueStateCounts(final String queueName) {
        return userModeRepository.queryQueueStateCounts(queueName);
    }

    public Boolean hasActiveJobs(final String queueName) {
        return userModeRepository.hasJobsInState(queueName, Queues.State.ACTIVE.name());
    }

    public Boolean hasActiveJobs(final String queueName, final Boolean asSystem) {
        return getRepository(asSystem).hasJobsInState(queueName, Queues.State.ACTIVE.name());
    }

    public Job__c requireJob(final Id jobId) {
        return requireJob(jobId, false);
    }

    public Job__c requireJobAsSystem(final Id jobId) {
        return requireJob(jobId, true);
    }

    public Job__c requireJobForUpdate(final Id jobId) {
        return requireJobForUpdate(jobId, false);
    }

    public Job__c requireJobForUpdateAsSystem(final Id jobId) {
        return requireJobForUpdate(jobId, true);
    }

    public List<Job__c> queryJobsAsSystem(final Set<Id> jobIds) {
        return systemModeRepository.queryJobsByIds(jobIds);
    }

    public List<Job__c> queryJobsByParentIds(final Set<Id> parentIds) {
        return userModeRepository.queryJobsByParentIds(parentIds);
    }

    public List<Job__c> queryJobsForUpdate(final Set<Id> jobIds) {
        return userModeRepository.queryJobsByIdsForUpdate(jobIds, null);
    }

    public List<Job__c> queryJobsForUpdateAsSystem(final Set<Id> jobIds) {
        return systemModeRepository.queryJobsByIdsForUpdate(jobIds, null);
    }

    public List<Job__c> queryJobsForUpdateAsSystem(final Set<Id> jobIds, final Queues.State state) {
        return systemModeRepository.queryJobsByIdsForUpdate(jobIds, state?.name());
    }

    public List<AggregateResult> queryChildStateCountsAsSystem(final Set<Id> parentIds) {
        return systemModeRepository.queryChildStateCounts(parentIds);
    }

    public List<Job__c> queryFailedRemovedDependenciesForUpdateAsSystem(
        final Set<Id> parentIds,
        final Integer jobLimit
    ) {
        return systemModeRepository.queryFailedRemovedDependenciesForUpdate(parentIds, jobLimit);
    }

    public List<Job__c> queryStaleRemovedDependenciesForUpdateAsSystem(
        final String queueName,
        final Datetime staleBefore,
        final Integer jobLimit
    ) {
        return systemModeRepository.queryStaleRemovedDependenciesForUpdate(queueName, staleBefore, jobLimit);
    }

    public List<Job__c> queryStaleWaitingChildrenJobsAsSystem(
        final String queueName,
        final Datetime staleBefore,
        final Integer jobLimit
    ) {
        return systemModeRepository.queryStaleWaitingChildrenJobs(queueName, staleBefore, jobLimit);
    }

    public List<Job__c> insertJobs(final List<Job__c> jobs) {
        return userModeRepository.insertJobs(jobs);
    }

    public List<Database.SaveResult> insertJobsAllowingPartialSuccess(final List<Job__c> jobs) {
        return userModeRepository.insertJobsAllowingPartialSuccess(jobs);
    }

    public List<Database.SaveResult> insertJobsAllowingPartialSuccess(final List<Job__c> jobs, final Boolean asSystem) {
        return getRepository(asSystem).insertJobsAllowingPartialSuccess(jobs);
    }

    public Job__c updateJob(final Job__c job) {
        userModeRepository.updateJobs(new List<Job__c>{ job });
        return job;
    }

    public Job__c updateJobAsSystem(final Job__c job) {
        systemModeRepository.updateJobs(new List<Job__c>{ job });
        return job;
    }

    public List<Job__c> updateJobs(final List<Job__c> jobs) {
        return userModeRepository.updateJobs(jobs);
    }

    public List<Job__c> updateJobsAsSystem(final List<Job__c> jobs) {
        return systemModeRepository.updateJobs(jobs);
    }

    public List<Job__c> updateJobs(final List<Job__c> jobs, final Boolean asSystem) {
        return getRepository(asSystem).updateJobs(jobs);
    }

    public void deleteJobs(final List<Job__c> jobs) {
        userModeRepository.deleteJobs(jobs);
    }

    public Job__c getResolvedJob(
        final Map<String, Job__c> existingJobsByExternalId,
        final Map<String, Job__c> createdJobsByExternalId,
        final String externalId
    ) {
        if (String.isBlank(externalId)) {
            return null;
        }
        String externalIdKey = QueuesSupport.normalizeCaseInsensitiveKey(externalId);
        if (createdJobsByExternalId.containsKey(externalIdKey)) {
            return createdJobsByExternalId.get(externalIdKey);
        }
        return existingJobsByExternalId.get(externalIdKey);
    }

    public Datetime findEarliestWakeAt(final List<Job__c> jobs, final Datetime fallbackNow) {
        Datetime earliestWakeAt;
        for (Job__c job : jobs) {
            if (job == null || !isRunnableState(job.State__c)) {
                continue;
            }

            Datetime availableAt = job.AvailableAt__c ?? fallbackNow;
            if (earliestWakeAt == null || availableAt < earliestWakeAt) {
                earliestWakeAt = availableAt;
            }
        }
        return earliestWakeAt;
    }

    public void applyResult(final Job__c job, final Queues.ProcessResult result, final Datetime now) {
        QueuesSupport.isTrue(result != null, new Queues.QueueException(NULL_PROCESS_RESULT_MESSAGE));

        clearLease(job);
        job.Progress__c = result.getProgressJson();
        job.Checkpoint__c = result.getCheckpointJson();
        job.PauseRequested__c = false;

        switch on result.getOutcome() {
            when COMPLETED {
                job.State__c = Queues.State.COMPLETED.name();
                job.AvailableAt__c = now;
                job.ReturnValue__c = result.getReturnValueJson();
                job.FailedReason__c = null;
                return;
            }
            when PAUSED {
                job.State__c = Queues.State.PAUSED.name();
                job.AvailableAt__c = now;
                job.ReturnValue__c = null;
                job.FailedReason__c = null;
                return;
            }
            when DEFERRED {
                Integer delay = result.getDelay() ?? 0;
                job.State__c = delay > 0 ? Queues.State.DELAYED.name() : Queues.State.WAITING.name();
                job.AvailableAt__c = now.addMinutes(delay);
                job.ReturnValue__c = null;
                job.FailedReason__c = null;
                return;
            }
            when FAILED {
                applyFailure(job, result.getFailedReason(), now);
                return;
            }
        }
    }

    public void applyFailure(final Job__c job, final String failedReason, final Datetime now) {
        applyFailure(job, failedReason, now, false);
    }

    public void applyFailure(
        final Job__c job,
        final String failedReason,
        final Datetime now,
        final Boolean isUnrecoverable
    ) {
        clearLease(job);
        job.ReturnValue__c = null;
        job.Progress__c = null;
        job.Checkpoint__c = null;
        job.PauseRequested__c = false;
        job.FailedReason__c = QueuesSupport.normalizeExceptionReason(failedReason);

        Integer attemptsMade = QueuesSupport.toInteger(job.AttemptsMade__c, 0);
        Integer maxAttempts = QueuesSupport.toPositiveInteger(job.MaxAttempts__c, 1);
        if (isUnrecoverable || attemptsMade >= maxAttempts) {
            job.State__c = Queues.State.FAILED.name();
            job.AvailableAt__c = now;
            return;
        }

        Integer retryDelaySeconds = resolveRetryDelaySeconds(job, attemptsMade);
        job.State__c = retryDelaySeconds > 0 ? Queues.State.DELAYED.name() : Queues.State.WAITING.name();
        job.AvailableAt__c = now.addSeconds(retryDelaySeconds);
    }

    public void clearLease(final Job__c job) {
        job.LeaseToken__c = null;
        job.LeaseExpiresAt__c = null;
        job.AsyncApexJobId__c = null;
    }

    public static void applyCanceled(final Job__c job, final String reason) {
        job.LeaseToken__c = null;
        job.LeaseExpiresAt__c = null;
        job.AsyncApexJobId__c = null;
        job.State__c = Queues.State.CANCELED.name();
        job.FailedReason__c = reason;
        job.PauseRequested__c = false;
    }

    public static void applyCanceled(final Job__c job, final String reason, final Datetime now) {
        applyCanceled(job, reason);
        job.AvailableAt__c = now;
    }

    public Job__c mapToJob(
        final String queueName,
        final Queues.JobData jobData,
        final QueueDefinition__mdt queueDefinition,
        final Processor__mdt processorDefinition,
        final Datetime now,
        final Queues.ExecutionMode executionMode
    ) {
        Queues.JobOptions options = jobData.getOpts();
        QueuesSupport.notNull(executionMode, 'executionMode');
        Datetime availableAt = resolveAvailableAt(now, options);
        Queues.Backoff backoff = options.getBackoffType() ??
            QueuesSupport.toBackoff(queueDefinition.DefaultBackoffType__c) ?? Queues.Backoff.NONE;
        Integer backoffValue = options.getBackoffValue() ??
            QueuesSupport.toInteger(queueDefinition.DefaultBackoffValue__c, 0);
        Decimal backoffJitter = options.getBackoffJitter() ??
            QueuesSupport.toDecimal(queueDefinition.DefaultBackoffJitter__c, 0.0);
        if (backoff == Queues.Backoff.NONE) {
            backoffValue = 0;
            backoffJitter = 0.0;
        } else {
            validateBackoffJitter(backoffJitter);
        }

        return new Job__c(
            QueueName__c = queueName,
            Name__c = jobData.getName(),
            State__c = availableAt > now ? Queues.State.DELAYED.name() : Queues.State.WAITING.name(),
            ExternalId__c = options.getJobId(),
            QueueScopedExternalId__c = QueuesSupport.buildQueueScopedExternalId(queueName, options.getJobId()),
            Category__c = options.getCategory(),
            GroupKey__c = options.getGroupKey(),
            CorrelationId__c = options.getCorrelationId(),
            ExecutionMode__c = executionMode.name(),
            Data__c = QueuesSupport.serialize(jobData.getData()),
            AvailableAt__c = availableAt,
            Priority__c = options.getPriority() ?? 0,
            AttemptsMade__c = 0,
            MaxAttempts__c = options.getAttempts() ?? QueuesSupport.toInteger(queueDefinition.DefaultAttempts__c, 1),
            BackoffType__c = backoff.name(),
            BackoffValue__c = backoffValue,
            BackoffJitter__c = backoffJitter > 0 ? backoffJitter : null,
            ProcessorClass__c = processorDefinition.ProcessorClass__c,
            Parent__c = options.getParentJobId(),
            DependencyFailurePolicy__c = options.getDependencyFailurePolicy()?.name() ?? 'DEFAULT',
            DependencyCanceledPolicy__c = options.getDependencyCanceledPolicy()?.name() ?? 'DEFAULT',
            PauseRequested__c = false
        );
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private static Queues.JobNotFoundException mapToJobNotFoundException(final Id jobId) {
        return new Queues.JobNotFoundException(String.format(JOB_NOT_FOUND_MESSAGE, new List<Object>{ jobId }));
    }

    private static Datetime resolveAvailableAt(final Datetime now, final Queues.JobOptions options) {
        if (options.getAvailableAt() != null) {
            return options.getAvailableAt();
        }
        if (options.getDelay() > 0) {
            return now.addMinutes(options.getDelay());
        }
        return now;
    }

    private static Boolean isRunnableState(final String state) {
        return DUE_JOB_STATES.contains(QueuesSupport.toState(state));
    }

    private static Boolean isClaimableInvocableJob(final Job__c job, final Datetime now) {
        return job != null && isRunnableState(job.State__c) && isInvocableJob(job) && job.AvailableAt__c <= now;
    }

    private static Boolean isBackoff(final String backoffValue, final Queues.Backoff backoff) {
        return QueuesSupport.toBackoff(backoffValue) == backoff;
    }

    private static void validateBackoffJitter(final Decimal jitter) {
        QueuesSupport.isTrue(jitter >= 0 && jitter <= 1, new Queues.QueueException(INVALID_JITTER_MESSAGE));
    }

    @TestVisible
    private static Integer resolveRetryDelaySeconds(final Job__c job, final Integer attemptsMade) {
        Integer retryDelayMinutes = resolveRetryDelayMinutes(job, attemptsMade);
        if (retryDelayMinutes <= 0) {
            return 0;
        }

        Decimal jitter = QueuesSupport.toDecimal(job.BackoffJitter__c, 0.0);
        if (jitter <= 0) {
            return retryDelayMinutes * 60;
        }

        Decimal retryDelaySeconds = retryDelayMinutes * 60;
        Decimal minimumDelaySeconds = retryDelaySeconds * (1 - jitter);
        Decimal jitterWindowSeconds = retryDelaySeconds * jitter;
        return Math.floor(minimumDelaySeconds + (jitterWindowSeconds * QueuesSupport.random())).intValue();
    }

    @TestVisible
    private static Integer resolveRetryDelayMinutes(final Job__c job, final Integer attemptsMade) {
        Integer backoffValue = QueuesSupport.toInteger(job.BackoffValue__c, 0);
        if (backoffValue <= 0) {
            return 0;
        }
        if (isBackoff(job.BackoffType__c, Queues.Backoff.EXPONENTIAL)) {
            Integer retryDelay = backoffValue;
            Integer exponent = (attemptsMade ?? 1) - 1;
            if (exponent < 0) {
                exponent = 0;
            }
            for (Integer i = 0; i < exponent; i++) {
                if (shouldCapQueueableRetryDelay(job, retryDelay)) {
                    return QueuesQueueableTransportService.NATIVE_MAX_QUEUEABLE_DELAY_MINUTES;
                }
                retryDelay *= 2;
            }
            return capQueueableRetryDelayMinutes(job, retryDelay);
        }
        if (isBackoff(job.BackoffType__c, Queues.Backoff.FIXED)) {
            return capQueueableRetryDelayMinutes(job, backoffValue);
        }
        return 0;
    }

    private static Integer capQueueableRetryDelayMinutes(final Job__c job, final Integer retryDelayMinutes) {
        Integer safeRetryDelayMinutes = Math.max(retryDelayMinutes ?? 0, 0);
        if (!isQueueableJob(job)) {
            return safeRetryDelayMinutes;
        }
        return Math.min(safeRetryDelayMinutes, QueuesQueueableTransportService.NATIVE_MAX_QUEUEABLE_DELAY_MINUTES);
    }

    private static Boolean shouldCapQueueableRetryDelay(final Job__c job, final Integer retryDelayMinutes) {
        return isQueueableJob(job) &&
            (retryDelayMinutes >= QueuesQueueableTransportService.NATIVE_MAX_QUEUEABLE_DELAY_MINUTES ||
            retryDelayMinutes > (QueuesQueueableTransportService.NATIVE_MAX_QUEUEABLE_DELAY_MINUTES / 2));
    }

    private static Boolean isQueueableJob(final Job__c job) {
        return QueuesSupport.isQueueableExecutionMode(QueuesSupport.toExecutionMode(job?.ExecutionMode__c));
    }

    private static Boolean isInvocableJob(final Job__c job) {
        return QueuesSupport.isInvocableExecutionMode(QueuesSupport.toExecutionMode(job?.ExecutionMode__c));
    }

    private static String leaseTokenForJob(final Id jobId, final String leaseToken) {
        return String.format('{0}:{1}', new List<Object>{ leaseToken, jobId });
    }

    private QueuesJobRepository getRepository(final Boolean asSystem) {
        return asSystem == true ? systemModeRepository : userModeRepository;
    }

    private static Set<String> toExternalIds(final List<Queues.JobData> jobs) {
        Set<String> externalIds = new Set<String>();
        for (Queues.JobData job : jobs) {
            String externalId = job.getOpts().getJobId();
            if (String.isNotBlank(externalId)) {
                externalIds.add(externalId);
            }
        }
        return externalIds;
    }

    private Boolean hasDueJobsAsSystem(final String queueName, final Datetime now) {
        return !queryDueJobsAsSystem(queueName, DUE_JOB_STATES, now, 1).isEmpty();
    }

    private Job__c requireJob(final Id jobId, final Boolean asSystem) {
        List<Job__c> jobs = getRepository(asSystem).queryJobsByIds(new Set<Id>{ jobId });
        Map<Id, Job__c> jobsById = new Map<Id, Job__c>(jobs);
        QueuesSupport.isTrue(jobsById.containsKey(jobId), mapToJobNotFoundException(jobId));
        return jobsById.get(jobId);
    }

    private Job__c requireJobForUpdate(final Id jobId, final Boolean asSystem) {
        List<Job__c> jobs = getRepository(asSystem).queryJobsByIdsForUpdate(new Set<Id>{ jobId }, null);
        Map<Id, Job__c> jobsById = new Map<Id, Job__c>(jobs);
        QueuesSupport.isTrue(jobsById.containsKey(jobId), mapToJobNotFoundException(jobId));
        return jobsById.get(jobId);
    }

    private Job__c claimJob(
        final Job__c currentJob,
        final QueueDefinition__mdt queueDefinition,
        final Datetime now,
        final String asyncApexJobId
    ) {
        currentJob.State__c = Queues.State.ACTIVE.name();
        currentJob.LeaseToken__c = leaseTokenForJob(currentJob.Id, QueuesSupport.generateRandomToken());
        currentJob.LeaseExpiresAt__c = now.addMinutes(definitionService.getLeaseMinutes(queueDefinition));
        currentJob.AsyncApexJobId__c = asyncApexJobId;
        return currentJob;
    }
    // </editor-fold>
}
