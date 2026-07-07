/**
 * @description Manages job scheduler lifecycle and occurrence materialization.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesJobSchedulerService {
    private static final String INVALID_REPEAT_MESSAGE = 'Queue "{0}" job scheduler "{1}" must define repeatOpts.every(...) or repeatOpts.pattern(...).';
    private static final String INVALID_REPEAT_MODE_MESSAGE = 'Queue "{0}" job scheduler "{1}" cannot define both repeatOpts.every(...) and repeatOpts.pattern(...).';
    private static final String INVALID_RANGE_MESSAGE = 'Queue "{0}" job scheduler "{1}" has startAt after endAt.';
    private static final String INVALID_PATTERN_RANGE_MESSAGE = 'Queue "{0}" job scheduler "{1}" does not support startAt or endAt with pattern(...).';
    private static final String INVALID_TEMPLATE_JOB_ID_MESSAGE = 'Queue "{0}" job scheduler "{1}" does not support template opts.jobId.';
    private static final String INVALID_TEMPLATE_DELAY_MESSAGE = 'Queue "{0}" job scheduler "{1}" does not support template opts.delay.';
    private static final String INVALID_TEMPLATE_AVAILABLE_AT_MESSAGE = 'Queue "{0}" job scheduler "{1}" does not support template opts.availableAt.';
    private static final String NO_FUTURE_RUN_MESSAGE = 'Queue "{0}" job scheduler "{1}" does not have a future run.';
    private static final String CANCELED_BY_SCHEDULER_REPLACEMENT_REASON = 'Canceled by job scheduler replacement.';
    private static final String CANCELED_BY_SCHEDULER_CANCEL_REASON = 'Canceled by job scheduler cancel.';
    private static final String INVALID_REMOVE_ACTIVE_JOBS_MESSAGE = 'Active scheduler jobs cannot be removed.';
    private static final String INCOMPLETE_REPLACEMENT_CANCEL_MESSAGE = 'Queue "{0}" job scheduler "{1}" has too many pending jobs to replace safely. Cancel or remove it before replacing.';
    private static final String EXISTING_SCHEDULER_JOB_EXECUTION_MODE_MESSAGE = 'Existing scheduler job execution mode does not match scheduler execution mode.';
    private static final String JOB_SCHEDULER_MATERIALIZER_COMPONENT = 'job scheduler materializer';
    private static final String JOB_SCHEDULER_REPAIR_COMPONENT = 'job scheduler repair';
    private static final Integer MINIMUM_FUTURE_SCHEDULE_OFFSET_MINUTES = 1;
    private static final Integer LIFECYCLE_JOB_BATCH_SIZE = 200;
    private static final Integer LIFECYCLE_DML_ROW_HEADROOM = 500;
    private static final Integer LIFECYCLE_DML_STATEMENT_HEADROOM = 10;
    private static final Integer LIFECYCLE_QUERY_HEADROOM = 5;
    private static final Integer LIFECYCLE_CPU_MS_HEADROOM = 2000;
    private static final Set<String> TERMINAL_SCHEDULED_JOB_STATES = new Set<String>{ 'DELETED', 'COMPLETE', 'ERROR' };
    private static final Set<Queues.State> ACTIVE_JOB_STATES = new Set<Queues.State>{ Queues.State.ACTIVE };
    private static final Set<Queues.State> CANCELABLE_JOB_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED,
        Queues.State.PAUSED,
        Queues.State.WAITING_CHILDREN
    };

    private QueuesDefinitionService definitionService = new QueuesDefinitionService();
    private QueuesErrorService errorService = new QueuesErrorService();
    private QueuesJobSchedulerRepository userModeSchedulerRepository = new QueuesJobSchedulerRepository();
    private QueuesJobSchedulerRepository systemModeSchedulerRepository = new QueuesJobSchedulerRepository(
        System.AccessLevel.SYSTEM_MODE
    );
    private QueuesJobService jobService = new QueuesJobService();
    private QueuesFlowProducerService flowProducerService = new QueuesFlowProducerService();
    private QueuesProcessorService processorService = new QueuesProcessorService();
    private QueuesQueueableDispatchService queueableDispatchService = new QueuesQueueableDispatchService();
    private QueuesScheduleRepository scheduleRepository = new QueuesScheduleRepository(System.AccessLevel.SYSTEM_MODE);
    private QueuesJobSchedulerSchedule scheduleService = new QueuesJobSchedulerSchedule();
    private QueuesWorkerService workerService = new QueuesWorkerService();
    private QueuesQueueEventService queueEventService = new QueuesQueueEventService();

    public QueuesJobSchedulerService() {
    }

    @TestVisible
    private QueuesJobSchedulerService(
        final QueuesDefinitionService definitionService,
        final QueuesErrorService errorService,
        final QueuesJobSchedulerRepository schedulerRepository,
        final QueuesJobService jobService,
        final QueuesProcessorService processorService,
        final QueuesQueueableDispatchService queueableDispatchService,
        final QueuesScheduleRepository scheduleRepository,
        final QueuesJobSchedulerSchedule scheduleService,
        final QueuesFlowProducerService flowProducerService,
        final QueuesWorkerService workerService,
        final QueuesQueueEventService queueEventService
    ) {
        this.definitionService = definitionService;
        this.errorService = errorService;
        this.userModeSchedulerRepository = schedulerRepository;
        this.systemModeSchedulerRepository = schedulerRepository;
        this.jobService = jobService;
        this.processorService = processorService;
        this.queueableDispatchService = queueableDispatchService;
        this.scheduleRepository = scheduleRepository;
        this.scheduleService = scheduleService;
        this.flowProducerService = flowProducerService;
        this.workerService = workerService;
        this.queueEventService = queueEventService;
    }

    // <editor-fold desc="Job Schedulers">
    public Job__c upsertJobScheduler(
        final String queueName,
        final String jobSchedulerId,
        final Queues.RepeatOptions repeatOpts,
        final Queues.JobTemplate jobTemplate
    ) {
        return upsertJobScheduler(queueName, jobSchedulerId, repeatOpts, jobTemplate, Queues.ExecutionMode.WORKER);
    }

    public Job__c upsertJobScheduler(
        final String queueName,
        final String jobSchedulerId,
        final Queues.RepeatOptions repeatOpts,
        final Queues.JobTemplate jobTemplate,
        final Queues.ExecutionMode executionMode
    ) {
        QueuesSupport.notBlank(queueName, 'queueName');
        QueuesSupport.notBlank(jobSchedulerId, 'jobSchedulerId');
        QueuesSupport.notNull(repeatOpts, 'repeatOpts');

        QueueDefinition__mdt queueDefinition = definitionService.requireQueueDefinition(queueName, true);
        validateRepeatOptions(queueName, jobSchedulerId, repeatOpts);
        Queues.JobTemplate template = resolveTemplate(jobSchedulerId, jobTemplate);
        validateTemplate(queueName, jobSchedulerId, template);
        Queues.ExecutionMode resolvedExecutionMode = executionMode ?? Queues.ExecutionMode.WORKER;
        List<Queues.JobData> templateJobs = new List<Queues.JobData>{
            new Queues.JobData(template.getName(), template.getProcessor(), template.getData(), template.getOpts())
        };
        processorService.requireProcessors(templateJobs);

        Savepoint savepoint = Database.setSavepoint();
        String newScheduledJobId;
        queueEventService.beginScope();
        try {
            Datetime currentNow = QueuesSupport.now();
            JobScheduler__c scheduler = findJobSchedulerForUpdate(queueName, jobSchedulerId);
            Boolean replacingScheduler = scheduler != null;
            if (scheduler == null) {
                try {
                    scheduler = userModeSchedulerRepository.insertJobScheduler(
                        new JobScheduler__c(
                            Name = jobSchedulerId,
                            QueueName__c = queueName,
                            ExternalId__c = buildSchedulerExternalId(queueName, jobSchedulerId),
                            IsActive__c = true,
                            IterationCount__c = 0
                        )
                    );
                } catch (DmlException ex) {
                    if (!isDuplicateInsertException(ex)) {
                        throw ex;
                    }
                    scheduler = findJobSchedulerForUpdate(queueName, jobSchedulerId);
                    if (scheduler == null) {
                        throw ex;
                    }
                    replacingScheduler = true;
                }
            }

            String previousScheduledJobId = scheduler.ScheduledJobId__c;
            List<CronTrigger> previousScheduledJobs = replacingScheduler
                ? queryMatchingScheduledJobs(scheduler)
                : new List<CronTrigger>();
            Integer nextIterationCount = QueuesSupport.toInteger(scheduler.IterationCount__c, 0) + 1;
            requireMaterializableIteration(queueName, jobSchedulerId, repeatOpts.getLimit(), nextIterationCount);
            QueuesSupport.isTrue(
                cancelPendingJobs(scheduler, CANCELED_BY_SCHEDULER_REPLACEMENT_REASON).completed,
                new Queues.InvalidOperationException(
                    String.format(INCOMPLETE_REPLACEMENT_CANCEL_MESSAGE, new List<String>{ queueName, jobSchedulerId })
                )
            );
            InitialSchedulePlan plan = buildInitialSchedulePlan(scheduler, repeatOpts, currentNow);
            newScheduledJobId = plan.scheduledJobId;
            Job__c materializedJob = enqueueOccurrence(
                scheduler,
                template,
                plan.runAt,
                nextIterationCount,
                queueDefinition,
                resolvedExecutionMode,
                false
            );

            applySchedulerState(
                scheduler,
                repeatOpts,
                template,
                currentNow,
                plan.scheduledJobId,
                plan.runAt,
                nextIterationCount,
                resolvedExecutionMode
            );
            Set<String> abortedScheduledJobIds = abortScheduledJobsExcept(previousScheduledJobs, plan.scheduledJobId);
            abortScheduledJobIfPresent(
                previousScheduledJobId,
                plan.scheduledJobId,
                abortedScheduledJobIds,
                new List<CronTrigger>()
            );
            queueEventService.commitScope();
            return materializedJob;
        } catch (Exception ex) {
            Database.rollback(savepoint);
            queueEventService.abandonScope();
            abortScheduledJobIfPresent(newScheduledJobId, null, new Set<String>(), new List<CronTrigger>());
            throw ex;
        }
    }

    public Boolean removeJobScheduler(final String queueName, final String jobSchedulerId) {
        QueuesSupport.notBlank(queueName, 'queueName');
        QueuesSupport.notBlank(jobSchedulerId, 'jobSchedulerId');

        JobScheduler__c scheduler = findJobSchedulerForUpdate(queueName, jobSchedulerId);
        if (scheduler == null) {
            return false;
        }

        requireNoActiveJobs(scheduler);
        List<CronTrigger> matchingScheduledJobs = queryMatchingScheduledJobs(scheduler);
        String scheduledJobId = scheduler.ScheduledJobId__c;
        SchedulerJobProcessResult deleteResult = deleteSchedulerJobs(scheduler);
        if (!deleteResult.completed) {
            deactivateAndAbortScheduler(scheduler, scheduledJobId, matchingScheduledJobs);
            return false;
        }

        userModeSchedulerRepository.deleteJobScheduler(scheduler);
        Set<String> abortedScheduledJobIds = abortScheduledJobsExcept(matchingScheduledJobs, null);
        abortScheduledJobIfPresent(scheduledJobId, null, abortedScheduledJobIds, matchingScheduledJobs);
        return true;
    }

    public Boolean cancelJobScheduler(final String queueName, final String jobSchedulerId) {
        QueuesSupport.notBlank(queueName, 'queueName');
        QueuesSupport.notBlank(jobSchedulerId, 'jobSchedulerId');

        JobScheduler__c scheduler = findJobSchedulerForUpdate(queueName, jobSchedulerId);
        if (scheduler == null) {
            return false;
        }

        Boolean hasSchedulerWork = hasActiveSchedulerState(scheduler);
        List<CronTrigger> matchingScheduledJobs = queryMatchingScheduledJobs(scheduler);
        String scheduledJobId = scheduler.ScheduledJobId__c;
        SchedulerJobProcessResult cancelResult = cancelPendingJobs(scheduler, CANCELED_BY_SCHEDULER_CANCEL_REASON);
        if (!hasSchedulerWork && cancelResult.processedCount == 0 && cancelResult.completed) {
            return false;
        }

        deactivateAndAbortScheduler(scheduler, scheduledJobId, matchingScheduledJobs);
        return cancelResult.completed;
    }

    public Queues.JobScheduler getJobScheduler(final String queueName, final String jobSchedulerId) {
        QueuesSupport.notBlank(queueName, 'queueName');
        QueuesSupport.notBlank(jobSchedulerId, 'jobSchedulerId');
        return toJobScheduler(findJobScheduler(queueName, jobSchedulerId));
    }

    public List<Queues.JobScheduler> getJobSchedulers(
        final String queueName,
        final Integer startIndex,
        final Integer endIndex,
        final Boolean ascending
    ) {
        QueuesSupport.notBlank(queueName, 'queueName');

        List<Queues.JobScheduler> schedulerValues = new List<Queues.JobScheduler>();
        for (JobScheduler__c scheduler : queryJobSchedulers(queueName, startIndex, endIndex, ascending)) {
            schedulerValues.add(toJobScheduler(scheduler));
        }
        return schedulerValues;
    }

    public Integer getJobSchedulersCount(final String queueName) {
        QueuesSupport.notBlank(queueName, 'queueName');
        return userModeSchedulerRepository.queryJobSchedulersCount(queueName);
    }

    public void materializeJobSchedulerAndRecordError(final Id jobSchedulerRecordId, final String scheduledJobId) {
        try {
            materializeJobScheduler(jobSchedulerRecordId, scheduledJobId);
        } catch (Exception ex) {
            recordSchedulerMaterializationError(jobSchedulerRecordId, ex);
            throw ex;
        }
    }

    public Queues.MaintenanceResult runJobSchedulerMaintenanceAsSystem() {
        Queues.MaintenanceResult result = new Queues.MaintenanceResult();
        List<JobScheduler__c> schedulers = systemModeSchedulerRepository.queryActiveJobSchedulers();
        if (schedulers.isEmpty()) {
            return result;
        }

        Map<String, QueueDefinition__mdt> definitionsByQueueName;
        List<CronTrigger> activeScheduledJobs;
        try {
            definitionsByQueueName = definitionService.findQueueDefinitionsByQueueNamesAsSystem(
                resolveQueueNames(schedulers)
            );
            activeScheduledJobs = queryAllSchedulerScheduledJobs();
        } catch (Exception ex) {
            for (JobScheduler__c scheduler : schedulers) {
                recordSchedulerRepairError(scheduler, ex);
            }
            return result.markIncomplete();
        }

        for (JobScheduler__c scheduler : schedulers) {
            if (!hasHeadroomForLifecycle()) {
                result.markIncomplete();
                break;
            }
            try {
                JobScheduler__c lockedScheduler = findJobSchedulerByIdForUpdate(scheduler.Id);
                if (!isSameActiveSchedulerIdentity(scheduler, lockedScheduler)) {
                    continue;
                }
                QueueDefinition__mdt queueDefinition = definitionsByQueueName.get(lockedScheduler.QueueName__c);
                if (!QueuesSupport.isActive(queueDefinition)) {
                    continue;
                }
                if (repairOne(lockedScheduler, activeScheduledJobs)) {
                    result.add(lockedScheduler.QueueName__c, 1);
                }
            } catch (Exception ex) {
                recordSchedulerRepairError(scheduler, ex);
                result.markIncomplete();
            }
        }
        return result;
    }
    // </editor-fold>

    // <editor-fold desc="Validation">
    private static void validateRepeatOptions(
        final String queueName,
        final String jobSchedulerId,
        final Queues.RepeatOptions repeatOpts
    ) {
        Boolean hasEvery = repeatOpts.getEveryMinutes() != null;
        Boolean hasPattern = String.isNotBlank(repeatOpts.getPattern());
        QueuesSupport.isTrue(
            hasEvery || hasPattern,
            new Queues.ConfigurationException(
                String.format(INVALID_REPEAT_MESSAGE, new List<String>{ queueName, jobSchedulerId })
            )
        );
        QueuesSupport.isTrue(
            !(hasEvery && hasPattern),
            new Queues.ConfigurationException(
                String.format(INVALID_REPEAT_MODE_MESSAGE, new List<String>{ queueName, jobSchedulerId })
            )
        );
        if (hasPattern) {
            QueuesSupport.isTrue(
                repeatOpts.getStartAt() == null && repeatOpts.getEndAt() == null,
                new Queues.ConfigurationException(
                    String.format(INVALID_PATTERN_RANGE_MESSAGE, new List<String>{ queueName, jobSchedulerId })
                )
            );
            return;
        }

        QueuesSupport.isTrue(
            repeatOpts.getEveryMinutes() > 0,
            new Queues.ConfigurationException(
                String.format(INVALID_REPEAT_MESSAGE, new List<String>{ queueName, jobSchedulerId })
            )
        );
        QueuesSupport.isTrue(
            repeatOpts.getStartAt() == null ||
                repeatOpts.getEndAt() == null ||
                repeatOpts.getStartAt() <= repeatOpts.getEndAt(),
            new Queues.ConfigurationException(
                String.format(INVALID_RANGE_MESSAGE, new List<String>{ queueName, jobSchedulerId })
            )
        );
    }

    private static void validateTemplate(
        final String queueName,
        final String jobSchedulerId,
        final Queues.JobTemplate jobTemplate
    ) {
        Queues.JobOptions opts = jobTemplate.getOpts();
        QueuesSupport.isTrue(
            String.isBlank(opts.getJobId()),
            new Queues.ConfigurationException(
                String.format(INVALID_TEMPLATE_JOB_ID_MESSAGE, new List<String>{ queueName, jobSchedulerId })
            )
        );
        QueuesSupport.isTrue(
            opts.getDelay() == null,
            new Queues.ConfigurationException(
                String.format(INVALID_TEMPLATE_DELAY_MESSAGE, new List<String>{ queueName, jobSchedulerId })
            )
        );
        QueuesSupport.isTrue(
            opts.getAvailableAt() == null,
            new Queues.ConfigurationException(
                String.format(INVALID_TEMPLATE_AVAILABLE_AT_MESSAGE, new List<String>{ queueName, jobSchedulerId })
            )
        );
    }

    // </editor-fold>

    // <editor-fold desc="Scheduling">
    private void materializeJobScheduler(final Id jobSchedulerRecordId, final String scheduledJobId) {
        if (jobSchedulerRecordId == null) {
            return;
        }

        JobScheduler__c scheduler = findJobSchedulerByIdForUpdate(jobSchedulerRecordId);
        if (!isRunnableScheduler(scheduler, scheduledJobId)) {
            return;
        }

        Integer nextIterationCount = QueuesSupport.toInteger(scheduler.IterationCount__c, 0) + 1;
        if (String.isNotBlank(scheduler.Pattern__c)) {
            materializePatternScheduler(scheduler, nextIterationCount);
            return;
        }
        materializeEveryScheduler(scheduler, nextIterationCount, QueuesSupport.now());
    }

    private InitialSchedulePlan buildInitialSchedulePlan(
        final JobScheduler__c scheduler,
        final Queues.RepeatOptions repeatOpts,
        final Datetime currentNow
    ) {
        if (String.isNotBlank(repeatOpts.getPattern())) {
            String scheduledJobId;
            try {
                Id scheduledJobRecordId = scheduleService.schedulePattern(scheduler.Id, repeatOpts.getPattern());
                scheduledJobId = scheduledJobRecordId;
                Datetime firstRunAt = findActiveScheduledJobById(scheduledJobId)?.NextFireTime;
                requireFutureRun(scheduler.QueueName__c, scheduler.Name, firstRunAt);
                return new InitialSchedulePlan(scheduledJobId, firstRunAt);
            } catch (Exception ex) {
                abortScheduledJobIfPresent(scheduledJobId, null, new Set<String>(), new List<CronTrigger>());
                throw ex;
            }
        }

        Datetime firstRunAt = resolveInitialEveryRunAt(repeatOpts, currentNow);
        requireFutureRun(scheduler.QueueName__c, scheduler.Name, firstRunAt);
        String scheduledJobId = scheduleService.scheduleEvery(scheduler.Id, firstRunAt);
        return new InitialSchedulePlan(scheduledJobId, firstRunAt);
    }

    private void materializePatternScheduler(final JobScheduler__c scheduler, final Integer nextIterationCount) {
        CronTrigger scheduledJob = findActiveScheduledJobById(scheduler.ScheduledJobId__c);
        if (scheduledJob == null) {
            return;
        }

        Datetime nextRunAt = scheduledJob.NextFireTime;
        dispatchDueQueueableOccurrence(scheduler);
        if (!canMaterializeScheduledOccurrence(scheduler, nextIterationCount, nextRunAt)) {
            String scheduledJobId = scheduler.ScheduledJobId__c;
            deactivateSchedulerAsSystem(scheduler);
            abortScheduledJobIfPresent(scheduledJobId, null, new Set<String>(), new List<CronTrigger>());
            return;
        }
        materializeOccurrence(scheduler, nextRunAt, nextIterationCount, false);
    }

    private void materializeEveryScheduler(
        final JobScheduler__c scheduler,
        final Integer nextIterationCount,
        final Datetime currentNow
    ) {
        Datetime nextRunAt = resolveSubsequentEveryRunAt(scheduler, nextIterationCount, currentNow);
        dispatchDueQueueableOccurrence(scheduler);
        if (!canMaterializeScheduledOccurrence(scheduler, nextIterationCount, nextRunAt)) {
            deactivateSchedulerAsSystem(scheduler);
            return;
        }
        materializeOccurrence(scheduler, nextRunAt, nextIterationCount, true);
    }

    private static Boolean isRunnableScheduler(final JobScheduler__c scheduler, final String scheduledJobId) {
        if (
            scheduler == null ||
            scheduler.IsActive__c != true ||
            String.isBlank(scheduler.ScheduledJobId__c) ||
            QueuesSupport.toInteger(scheduler.IterationCount__c, 0) <= 0
        ) {
            return false;
        }
        return String.isBlank(scheduledJobId) || scheduler.ScheduledJobId__c == scheduledJobId;
    }

    private Boolean repairOne(final JobScheduler__c scheduler, final List<CronTrigger> activeScheduledJobs) {
        if (String.isNotBlank(scheduler.Pattern__c)) {
            return repairPatternScheduler(
                scheduler,
                findMatchingScheduledJobs(scheduler, activeScheduledJobs),
                activeScheduledJobs
            );
        }
        if (scheduler.EveryMinutes__c != null) {
            return repairEveryScheduler(
                scheduler,
                findMatchingScheduledJobs(scheduler, activeScheduledJobs),
                activeScheduledJobs
            );
        }
        return false;
    }

    private Boolean repairPatternScheduler(
        final JobScheduler__c scheduler,
        final List<CronTrigger> matchingJobs,
        final List<CronTrigger> activeScheduledJobs
    ) {
        CronTrigger scheduledJob = findScheduledJobById(matchingJobs, scheduler.ScheduledJobId__c) ??
            findScheduledJobById(activeScheduledJobs, scheduler.ScheduledJobId__c);
        if (isHealthyPatternScheduledJob(scheduler, scheduledJob)) {
            return !abortScheduledJobsExcept(matchingJobs, scheduler.ScheduledJobId__c).isEmpty();
        }

        String previousScheduledJobId = scheduler.ScheduledJobId__c;
        String repairedScheduledJobId;
        try {
            repairedScheduledJobId = scheduleService.schedulePattern(scheduler.Id, scheduler.Pattern__c);
            Datetime nextRunAt = findActiveScheduledJobById(repairedScheduledJobId)?.NextFireTime;
            requireFutureRun(scheduler.QueueName__c, scheduler.Name, nextRunAt);
            systemModeSchedulerRepository.updateJobScheduler(
                new JobScheduler__c(
                    Id = scheduler.Id,
                    ScheduledJobId__c = repairedScheduledJobId,
                    NextRunAt__c = nextRunAt
                )
            );
            abortScheduledJobsExcept(matchingJobs, repairedScheduledJobId);
            abortScheduledJobIfPresent(
                previousScheduledJobId,
                repairedScheduledJobId,
                new Set<String>(),
                activeScheduledJobs
            );
            return true;
        } catch (Exception ex) {
            abortScheduledJobIfPresent(repairedScheduledJobId, null, new Set<String>(), new List<CronTrigger>());
            throw ex;
        }
    }

    private Boolean repairEveryScheduler(
        final JobScheduler__c scheduler,
        final List<CronTrigger> matchingJobs,
        final List<CronTrigger> activeScheduledJobs
    ) {
        CronTrigger scheduledJob = findScheduledJobById(matchingJobs, scheduler.ScheduledJobId__c) ??
            findScheduledJobById(activeScheduledJobs, scheduler.ScheduledJobId__c);
        if (isHealthyEveryScheduledJob(scheduledJob)) {
            return !abortScheduledJobsExcept(matchingJobs, scheduler.ScheduledJobId__c).isEmpty();
        }

        String previousScheduledJobId = scheduler.ScheduledJobId__c;
        String repairedScheduledJobId;
        try {
            repairedScheduledJobId = scheduleService.scheduleEvery(
                scheduler.Id,
                resolveEveryRepairRunAt(scheduler.NextRunAt__c, QueuesSupport.now())
            );
            systemModeSchedulerRepository.updateJobScheduler(
                new JobScheduler__c(Id = scheduler.Id, ScheduledJobId__c = repairedScheduledJobId)
            );
            abortScheduledJobsExcept(matchingJobs, repairedScheduledJobId);
            abortScheduledJobIfPresent(
                previousScheduledJobId,
                repairedScheduledJobId,
                new Set<String>(),
                activeScheduledJobs
            );
            return true;
        } catch (Exception ex) {
            abortScheduledJobIfPresent(repairedScheduledJobId, null, new Set<String>(), new List<CronTrigger>());
            throw ex;
        }
    }

    private static Boolean isHealthyPatternScheduledJob(
        final JobScheduler__c scheduler,
        final CronTrigger scheduledJob
    ) {
        return scheduledJob != null &&
            !TERMINAL_SCHEDULED_JOB_STATES.contains(scheduledJob.State) &&
            scheduledJob.CronExpression == scheduler.Pattern__c;
    }

    private static Boolean isHealthyEveryScheduledJob(final CronTrigger scheduledJob) {
        return scheduledJob != null && !TERMINAL_SCHEDULED_JOB_STATES.contains(scheduledJob.State);
    }

    private void recordSchedulerRepairError(final JobScheduler__c scheduler, final Exception ex) {
        errorService.recordQueueError(
            scheduler.QueueName__c,
            String.format('{0}: {1}', new List<String>{ JOB_SCHEDULER_REPAIR_COMPONENT, scheduler.Name }),
            ex
        );
    }

    private void recordSchedulerMaterializationError(final Id jobSchedulerRecordId, final Exception ex) {
        JobScheduler__c scheduler = findJobSchedulerById(jobSchedulerRecordId);
        errorService.recordQueueError(
            scheduler.QueueName__c,
            String.format('{0}: {1}', new List<String>{ JOB_SCHEDULER_MATERIALIZER_COMPONENT, scheduler.Name }),
            ex
        );
    }
    // </editor-fold>

    // <editor-fold desc="Job Materialization">
    private void materializeOccurrence(
        final JobScheduler__c scheduler,
        final Datetime runAt,
        final Integer iterationCount,
        final Boolean scheduleNextEveryWake
    ) {
        Savepoint savepoint = Database.setSavepoint();
        String nextScheduledJobId;
        queueEventService.beginScope();
        try {
            if (scheduleNextEveryWake) {
                nextScheduledJobId = scheduleService.scheduleEvery(scheduler.Id, runAt);
            }

            Queues.JobTemplate template = toJobSchedulerTemplate(scheduler);
            Queues.ExecutionMode executionMode = resolveExecutionMode(scheduler);
            QueueDefinition__mdt queueDefinition = definitionService.requireQueueDefinition(
                scheduler.QueueName__c,
                true
            );
            enqueueOccurrence(scheduler, template, runAt, iterationCount, queueDefinition, executionMode, true);
            Datetime lastEnqueuedAt = QueuesSupport.now();
            JobScheduler__c schedulerUpdate = new JobScheduler__c(
                Id = scheduler.Id,
                IterationCount__c = iterationCount,
                NextRunAt__c = runAt,
                LastEnqueuedAt__c = lastEnqueuedAt
            );
            if (scheduleNextEveryWake) {
                schedulerUpdate.ScheduledJobId__c = nextScheduledJobId;
            }
            systemModeSchedulerRepository.updateJobScheduler(schedulerUpdate);
            queueEventService.commitScope();
        } catch (Exception ex) {
            Database.rollback(savepoint);
            queueEventService.abandonScope();
            abortScheduledJobIfPresent(nextScheduledJobId, null, new Set<String>(), new List<CronTrigger>());
            throw ex;
        }
    }

    private Job__c enqueueOccurrence(
        final JobScheduler__c scheduler,
        final Queues.JobTemplate template,
        final Datetime runAt,
        final Integer iterationCount,
        final QueueDefinition__mdt queueDefinition,
        final Queues.ExecutionMode executionMode,
        final Boolean asSystem
    ) {
        String externalId = buildJobSchedulerJobExternalId(scheduler, iterationCount);
        Job__c existingJob = findExistingScheduledJob(scheduler.QueueName__c, externalId, asSystem);
        if (existingJob != null) {
            requireMatchingExecutionMode(existingJob, executionMode);
            return existingJob;
        }

        Queues.JobData jobData = new Queues.JobData(
            template.getName(),
            template.getProcessor(),
            template.getData(),
            buildJobOptions(scheduler, template.getOpts(), runAt, iterationCount)
        );
        Map<String, Processor__mdt> processorByClass = processorService.requireProcessors(
            new List<Queues.JobData>{ jobData }
        );
        Job__c job = jobService.mapToJob(
            scheduler.QueueName__c,
            jobData,
            queueDefinition,
            processorByClass.get(jobData.getProcessor()),
            QueuesSupport.now(),
            executionMode
        );
        job.JobScheduler__c = scheduler.Id;

        Database.SaveResult saveResult = jobService.insertJobsAllowingPartialSuccess(
            new List<Job__c>{ job },
            asSystem
        )[0];
        if (saveResult.isSuccess()) {
            job.Id = saveResult.getId();
            scheduleWorkerForMaterializedJob(scheduler.QueueName__c, job, executionMode);
            queueEventService.publishSchedulerMaterialized(scheduler, job);
            return job;
        }

        if (isDuplicateJobInsert(saveResult)) {
            Job__c duplicateJob = findExistingScheduledJob(scheduler.QueueName__c, externalId, asSystem);
            if (duplicateJob != null) {
                requireMatchingExecutionMode(duplicateJob, executionMode);
                return duplicateJob;
            }
        }

        throw buildInsertFailure(saveResult);
    }

    private void scheduleWorkerForMaterializedJob(
        final String queueName,
        final Job__c job,
        final Queues.ExecutionMode executionMode
    ) {
        if (QueuesSupport.isWorkerExecutionMode(executionMode)) {
            workerService.scheduleWorkerForJobs(queueName, new List<Job__c>{ job }, QueuesSupport.now());
        }
    }

    private void dispatchDueQueueableOccurrence(final JobScheduler__c scheduler) {
        Queues.ExecutionMode executionMode = resolveExecutionMode(scheduler);
        if (!QueuesSupport.isQueueableExecutionMode(executionMode)) {
            return;
        }

        Integer iterationCount = QueuesSupport.toInteger(scheduler.IterationCount__c, 0);
        Job__c job = findExistingScheduledJob(
            scheduler.QueueName__c,
            buildJobSchedulerJobExternalId(scheduler, iterationCount),
            true
        );
        if (!isDispatchableQueueableOccurrence(job, QueuesSupport.now())) {
            return;
        }

        QueueableDispatch__c dispatch = queueableDispatchService.createWaitingDispatchAsSystem(
            scheduler.QueueName__c,
            null,
            executionMode,
            new List<Job__c>{ job }
        );
        job.QueueableDispatch__c = dispatch.Id;
        jobService.updateJobsAsSystem(new List<Job__c>{ job });
        queueableDispatchService.enqueueDispatcherAsSystem(dispatch);
    }

    private static Boolean isDispatchableQueueableOccurrence(final Job__c job, final Datetime currentNow) {
        Queues.State state = QueuesSupport.toState(job?.State__c);
        return job != null &&
            QueuesSupport.isQueueableExecutionMode(QueuesSupport.toExecutionMode(job.ExecutionMode__c)) &&
            (state == Queues.State.WAITING ||
            state == Queues.State.DELAYED) &&
            job.AvailableAt__c <= currentNow &&
            job.QueueableDispatch__c == null &&
            job.QueueableDispatchInitiatedAt__c == null;
    }

    private static void requireMatchingExecutionMode(final Job__c job, final Queues.ExecutionMode executionMode) {
        QueuesSupport.isTrue(
            QueuesSupport.toExecutionMode(job.ExecutionMode__c) == executionMode,
            new Queues.QueueException(EXISTING_SCHEDULER_JOB_EXECUTION_MODE_MESSAGE)
        );
    }

    private static Queues.JobOptions buildJobOptions(
        final JobScheduler__c scheduler,
        final Queues.JobOptions templateOpts,
        final Datetime runAt,
        final Integer iterationCount
    ) {
        Queues.JobOptions opts = new Queues.JobOptions()
            .jobId(buildJobSchedulerJobExternalId(scheduler, iterationCount))
            .availableAt(runAt);

        if (templateOpts.getAttempts() != null) {
            opts.attempts(templateOpts.getAttempts());
        }
        if (templateOpts.getPriority() != null) {
            opts.priority(templateOpts.getPriority());
        }
        if (templateOpts.getCategory() != null) {
            opts.category(templateOpts.getCategory());
        }
        if (templateOpts.getGroupKey() != null) {
            opts.groupKey(templateOpts.getGroupKey());
        }
        if (templateOpts.getCorrelationId() != null) {
            opts.correlationId(templateOpts.getCorrelationId());
        }
        if (templateOpts.getBackoffType() != null) {
            opts.backoff(
                templateOpts.getBackoffType(),
                templateOpts.getBackoffValue() ?? 0,
                templateOpts.getBackoffJitter() ?? 0.0
            );
        }
        if (templateOpts.getParentJobId() != null) {
            opts.parent(templateOpts.getParentJobId());
        }
        return opts;
    }

    private Job__c findExistingScheduledJob(final String queueName, final String externalId, final Boolean asSystem) {
        Map<String, Job__c> jobsByExternalId = jobService.getJobsByExternalId(
            queueName,
            new Set<String>{ externalId },
            asSystem
        );
        return jobsByExternalId.get(QueuesSupport.normalizeCaseInsensitiveKey(externalId));
    }
    // </editor-fold>

    // <editor-fold desc="State Mapping">
    private static Queues.JobTemplate resolveTemplate(
        final String jobSchedulerId,
        final Queues.JobTemplate jobTemplate
    ) {
        if (jobTemplate == null) {
            return new Queues.JobTemplate(jobSchedulerId);
        }

        String jobName = String.isBlank(jobTemplate.getName()) ? jobSchedulerId : jobTemplate.getName();
        String processor = String.isBlank(jobTemplate.getProcessor()) ? jobName : jobTemplate.getProcessor();
        return new Queues.JobTemplate(jobName, processor).data(jobTemplate.getData()).opts(jobTemplate.getOpts());
    }

    private static Queues.JobScheduler toJobScheduler(final JobScheduler__c scheduler) {
        if (scheduler == null) {
            return null;
        }

        return new Queues.JobScheduler(
            scheduler.Name,
            scheduler.JobName__c,
            scheduler.ProcessorClass__c,
            QueuesSupport.toInteger(scheduler.IterationCount__c, 0),
            QueuesSupport.toInteger(scheduler.Limit__c, null),
            scheduler.StartAt__c,
            scheduler.EndAt__c,
            QueuesSupport.toInteger(scheduler.EveryMinutes__c, null),
            scheduler.Pattern__c,
            scheduler.NextRunAt__c,
            scheduler.IsActive__c,
            toJobSchedulerTemplate(scheduler)
        );
    }

    private static Queues.JobTemplate toJobSchedulerTemplate(final JobScheduler__c scheduler) {
        return new Queues.JobTemplate(scheduler.JobName__c, scheduler.ProcessorClass__c)
            .data(QueuesSupport.deserialize(scheduler.Data__c))
            .opts(deserializeOptions(scheduler.Options__c));
    }

    private void applySchedulerState(
        final JobScheduler__c scheduler,
        final Queues.RepeatOptions repeatOpts,
        final Queues.JobTemplate template,
        final Datetime currentNow,
        final String scheduledJobId,
        final Datetime firstRunAt,
        final Integer iterationCount,
        final Queues.ExecutionMode executionMode
    ) {
        scheduler.JobName__c = template.getName();
        scheduler.ProcessorClass__c = template.getProcessor();
        scheduler.Data__c = QueuesSupport.serialize(template.getData());
        scheduler.Options__c = serializeOptions(template.getOpts());
        scheduler.ExecutionMode__c = executionMode.name();
        scheduler.Pattern__c = repeatOpts.getPattern();
        scheduler.EveryMinutes__c = repeatOpts.getEveryMinutes();
        scheduler.Limit__c = repeatOpts.getLimit();
        scheduler.StartAt__c = repeatOpts.getStartAt();
        scheduler.EndAt__c = repeatOpts.getEndAt();
        scheduler.ScheduledJobId__c = scheduledJobId;
        scheduler.IterationCount__c = iterationCount;
        scheduler.NextRunAt__c = firstRunAt;
        scheduler.LastEnqueuedAt__c = currentNow;
        scheduler.IsActive__c = true;
        userModeSchedulerRepository.updateJobScheduler(scheduler);
    }

    private static String serializeOptions(final Queues.JobOptions opts) {
        Map<String, Object> valuesByName = new Map<String, Object>();
        if (opts.getAttempts() != null) {
            valuesByName.put('attempts', opts.getAttempts());
        }
        if (opts.getPriority() != null) {
            valuesByName.put('priority', opts.getPriority());
        }
        if (opts.getCategory() != null) {
            valuesByName.put('category', opts.getCategory());
        }
        if (opts.getGroupKey() != null) {
            valuesByName.put('groupKey', opts.getGroupKey());
        }
        if (opts.getCorrelationId() != null) {
            valuesByName.put('correlationId', opts.getCorrelationId());
        }
        if (opts.getBackoffType() != null) {
            valuesByName.put('backoffType', opts.getBackoffType().name());
            valuesByName.put('backoffValue', opts.getBackoffValue() ?? 0);
            valuesByName.put('backoffJitter', opts.getBackoffJitter() ?? 0.0);
        }
        if (opts.getParentJobId() != null) {
            valuesByName.put('parentJobId', opts.getParentJobId());
        }
        return valuesByName.isEmpty() ? null : QueuesSupport.serialize(valuesByName);
    }

    private static Queues.JobOptions deserializeOptions(final String optionsJson) {
        Queues.JobOptions opts = new Queues.JobOptions();
        if (String.isBlank(optionsJson)) {
            return opts;
        }

        Map<String, Object> valuesByName = (Map<String, Object>) QueuesSupport.deserialize(optionsJson);
        if (valuesByName.containsKey('attempts')) {
            opts.attempts(QueuesSupport.toInteger(valuesByName.get('attempts'), null));
        }
        if (valuesByName.containsKey('priority')) {
            opts.priority(QueuesSupport.toInteger(valuesByName.get('priority'), null));
        }
        if (valuesByName.containsKey('category')) {
            opts.category(valuesByName.get('category')?.toString());
        }
        if (valuesByName.containsKey('groupKey')) {
            opts.groupKey(valuesByName.get('groupKey')?.toString());
        }
        if (valuesByName.containsKey('correlationId')) {
            opts.correlationId(valuesByName.get('correlationId')?.toString());
        }
        if (valuesByName.containsKey('backoffType')) {
            opts.backoff(
                Queues.Backoff.valueOf(valuesByName.get('backoffType')?.toString()),
                QueuesSupport.toInteger(valuesByName.get('backoffValue'), 0),
                QueuesSupport.toDecimal(valuesByName.get('backoffJitter'), 0.0)
            );
        }
        if (valuesByName.containsKey('parentJobId')) {
            opts.parent(Id.valueOf(valuesByName.get('parentJobId')?.toString()));
        }
        return opts;
    }

    private static Queues.ExecutionMode resolveExecutionMode(final JobScheduler__c scheduler) {
        return QueuesSupport.toExecutionMode(scheduler.ExecutionMode__c) ?? Queues.ExecutionMode.WORKER;
    }
    // </editor-fold>

    // <editor-fold desc="Queries And Commands">
    private JobScheduler__c findJobScheduler(final String queueName, final String jobSchedulerId) {
        List<JobScheduler__c> schedulers = userModeSchedulerRepository.queryJobSchedulersByExternalId(
            buildSchedulerExternalId(queueName, jobSchedulerId)
        );
        return schedulers.isEmpty() ? null : schedulers[0];
    }

    private JobScheduler__c findJobSchedulerForUpdate(final String queueName, final String jobSchedulerId) {
        JobScheduler__c scheduler = findJobScheduler(queueName, jobSchedulerId);
        JobScheduler__c lockedScheduler = lockJobScheduler(scheduler, false);
        return lockedScheduler?.ExternalId__c == buildSchedulerExternalId(queueName, jobSchedulerId)
            ? lockedScheduler
            : null;
    }

    private JobScheduler__c findJobSchedulerById(final Id jobSchedulerRecordId) {
        List<JobScheduler__c> schedulers = systemModeSchedulerRepository.queryJobSchedulersById(jobSchedulerRecordId);
        return schedulers.isEmpty() ? null : schedulers[0];
    }

    private JobScheduler__c findJobSchedulerByIdForUpdate(final Id jobSchedulerRecordId) {
        List<JobScheduler__c> schedulers = systemModeSchedulerRepository.queryJobSchedulersByIdForUpdate(
            jobSchedulerRecordId
        );
        return schedulers.isEmpty() ? null : schedulers[0];
    }

    private JobScheduler__c lockJobScheduler(final JobScheduler__c scheduler, final Boolean asSystem) {
        if (scheduler == null) {
            return null;
        }

        List<JobScheduler__c> schedulers = getSchedulerRepository(asSystem)
            .queryJobSchedulersByIdForUpdate(scheduler.Id);
        return schedulers.isEmpty() ? null : schedulers[0];
    }

    private static Boolean isSameActiveSchedulerIdentity(
        final JobScheduler__c scheduler,
        final JobScheduler__c lockedScheduler
    ) {
        return lockedScheduler?.IsActive__c == true && scheduler.ScheduledJobId__c == lockedScheduler.ScheduledJobId__c;
    }

    private List<JobScheduler__c> queryJobSchedulers(
        final String queueName,
        final Integer startIndex,
        final Integer endIndex,
        final Boolean ascending
    ) {
        Integer safeStart = Math.max(startIndex ?? 0, 0);
        Integer safeEnd = endIndex ?? 24;
        Integer limitSize = safeEnd < safeStart ? 0 : safeEnd - safeStart + 1;
        return userModeSchedulerRepository.queryJobSchedulers(queueName, limitSize, safeStart, ascending);
    }

    private static Set<String> resolveQueueNames(final List<JobScheduler__c> schedulers) {
        Set<String> queueNames = new Set<String>();
        for (JobScheduler__c scheduler : schedulers) {
            if (String.isNotBlank(scheduler.QueueName__c)) {
                queueNames.add(scheduler.QueueName__c);
            }
        }
        return queueNames;
    }

    private CronTrigger findActiveScheduledJobById(final String scheduledJobId) {
        if (String.isBlank(scheduledJobId)) {
            return null;
        }

        List<CronTrigger> scheduledJobs = scheduleRepository.queryScheduledJobById(
            scheduledJobId,
            TERMINAL_SCHEDULED_JOB_STATES
        );
        return scheduledJobs.isEmpty() ? null : scheduledJobs[0];
    }

    private List<CronTrigger> queryMatchingScheduledJobs(final JobScheduler__c scheduler) {
        String jobNamePrefix = resolveScheduledJobNamePrefix(scheduler);
        return scheduleRepository.queryScheduledJobsByNamePrefix(jobNamePrefix, TERMINAL_SCHEDULED_JOB_STATES);
    }

    private List<CronTrigger> queryAllSchedulerScheduledJobs() {
        Map<Id, CronTrigger> scheduledJobsById = new Map<Id, CronTrigger>();
        addScheduledJobsById(
            scheduledJobsById,
            scheduleRepository.queryScheduledJobsByNamePrefix(
                QueuesJobSchedulerSchedule.resolveEveryJobNamePrefix(),
                TERMINAL_SCHEDULED_JOB_STATES
            )
        );
        addScheduledJobsById(
            scheduledJobsById,
            scheduleRepository.queryScheduledJobsByNamePrefix(
                QueuesJobSchedulerSchedule.resolvePatternJobNamePrefix(),
                TERMINAL_SCHEDULED_JOB_STATES
            )
        );
        return scheduledJobsById.values();
    }

    private static void addScheduledJobsById(
        final Map<Id, CronTrigger> scheduledJobsById,
        final List<CronTrigger> scheduledJobs
    ) {
        for (CronTrigger scheduledJob : scheduledJobs) {
            scheduledJobsById.put(scheduledJob.Id, scheduledJob);
        }
    }

    private static List<CronTrigger> findMatchingScheduledJobs(
        final JobScheduler__c scheduler,
        final List<CronTrigger> scheduledJobs
    ) {
        String jobNamePrefix = resolveScheduledJobNamePrefix(scheduler);
        List<CronTrigger> matchingJobs = new List<CronTrigger>();
        for (CronTrigger scheduledJob : scheduledJobs) {
            if (scheduledJob.CronJobDetail.Name.startsWith(jobNamePrefix)) {
                matchingJobs.add(scheduledJob);
            }
        }
        return matchingJobs;
    }

    private static CronTrigger findScheduledJobById(
        final List<CronTrigger> scheduledJobs,
        final String scheduledJobId
    ) {
        if (String.isBlank(scheduledJobId)) {
            return null;
        }

        for (CronTrigger scheduledJob : scheduledJobs) {
            if (scheduledJob.Id == scheduledJobId) {
                return scheduledJob;
            }
        }
        return null;
    }

    private Set<String> abortScheduledJobsExcept(
        final List<CronTrigger> scheduledJobs,
        final String retainedScheduledJobId
    ) {
        Set<String> abortedScheduledJobIds = new Set<String>();
        for (CronTrigger scheduledJob : scheduledJobs) {
            if (scheduledJob.Id == retainedScheduledJobId) {
                continue;
            }
            String scheduledJobId = scheduledJob.Id;
            scheduleRepository.abortJob(scheduledJobId);
            abortedScheduledJobIds.add(scheduledJobId);
        }
        return abortedScheduledJobIds;
    }

    private void abortScheduledJobIfPresent(
        final String scheduledJobId,
        final String retainedScheduledJobId,
        final Set<String> alreadyAbortedScheduledJobIds,
        final List<CronTrigger> knownScheduledJobs
    ) {
        if (String.isBlank(scheduledJobId) || scheduledJobId == retainedScheduledJobId) {
            return;
        }
        if (alreadyAbortedScheduledJobIds.contains(scheduledJobId)) {
            return;
        }

        CronTrigger scheduledJob = findScheduledJobById(knownScheduledJobs, scheduledJobId) ??
            findActiveScheduledJobById(scheduledJobId);
        if (scheduledJob == null) {
            return;
        }

        scheduleRepository.abortJob(scheduledJobId);
        alreadyAbortedScheduledJobIds.add(scheduledJobId);
    }

    private SchedulerJobProcessResult cancelPendingJobs(final JobScheduler__c scheduler, final String reason) {
        Integer processedCount = 0;
        while (hasHeadroomForLifecycle()) {
            Integer jobBatchSize = resolveLifecycleJobBatchSize(Limits.getLimitDmlRows() - Limits.getDmlRows());
            List<Job__c> jobs = jobService.queryJobsBySchedulerIdForUpdate(
                scheduler.Id,
                CANCELABLE_JOB_STATES,
                jobBatchSize
            );
            if (jobs.isEmpty()) {
                return new SchedulerJobProcessResult(processedCount, true);
            }

            for (Job__c job : jobs) {
                QueuesJobService.applyCanceled(job, reason);
            }
            jobService.updateJobs(jobs);
            flowProducerService.resolveParentDependenciesAsSystem(jobs);
            processedCount += jobs.size();
            if (jobs.size() < jobBatchSize) {
                return new SchedulerJobProcessResult(processedCount, true);
            }
        }
        return new SchedulerJobProcessResult(processedCount, false);
    }

    private SchedulerJobProcessResult deleteSchedulerJobs(final JobScheduler__c scheduler) {
        Integer processedCount = 0;
        while (hasHeadroomForLifecycle()) {
            Integer remainingDmlRows = Limits.getLimitDmlRows() - Limits.getDmlRows();
            Integer jobBatchSize = resolveLifecycleJobBatchSize(remainingDmlRows);
            List<Job__c> jobs = jobService.queryJobsBySchedulerIdForUpdate(scheduler.Id, jobBatchSize);
            if (jobs.isEmpty()) {
                return new SchedulerJobProcessResult(processedCount, true);
            }
            requireNoActiveJobs(jobs);

            jobService.deleteJobs(jobs);
            flowProducerService.resolveRemovedParentDependenciesAsSystem(jobs);

            processedCount += jobs.size();
            if (jobs.size() < jobBatchSize) {
                return new SchedulerJobProcessResult(processedCount, true);
            }
        }
        return new SchedulerJobProcessResult(processedCount, false);
    }

    private void deactivateAndAbortScheduler(
        final JobScheduler__c scheduler,
        final String scheduledJobId,
        final List<CronTrigger> matchingScheduledJobs
    ) {
        if (hasActiveSchedulerState(scheduler)) {
            deactivateScheduler(scheduler);
        }
        Set<String> abortedScheduledJobIds = abortScheduledJobsExcept(matchingScheduledJobs, null);
        abortScheduledJobIfPresent(scheduledJobId, null, abortedScheduledJobIds, matchingScheduledJobs);
    }

    private static Boolean hasActiveSchedulerState(final JobScheduler__c scheduler) {
        return scheduler.IsActive__c == true ||
            String.isNotBlank(scheduler.ScheduledJobId__c) ||
            scheduler.NextRunAt__c != null;
    }

    private static Boolean hasHeadroomForLifecycle() {
        QueuesSupport.GovernorHeadroom headroom = QueuesSupport.currentGovernorHeadroom();
        return hasHeadroomForLifecycle(headroom.dmlRows, headroom.dmlStatements, headroom.queries, headroom.cpuMs);
    }

    @TestVisible
    private static Boolean hasHeadroomForLifecycle(
        final Integer remainingDmlRows,
        final Integer remainingDmlStatements,
        final Integer remainingQueries,
        final Integer remainingCpuMs
    ) {
        return remainingDmlRows >= LIFECYCLE_DML_ROW_HEADROOM + 2 &&
            remainingDmlStatements >= LIFECYCLE_DML_STATEMENT_HEADROOM &&
            remainingQueries >= LIFECYCLE_QUERY_HEADROOM &&
            remainingCpuMs >= LIFECYCLE_CPU_MS_HEADROOM;
    }

    @TestVisible
    private static Integer resolveLifecycleJobBatchSize(final Integer remainingDmlRows) {
        Integer availableDmlRows = remainingDmlRows - LIFECYCLE_DML_ROW_HEADROOM;
        if (availableDmlRows < 2) {
            return 0;
        }
        return Math.min(LIFECYCLE_JOB_BATCH_SIZE, availableDmlRows - 1);
    }

    private void requireNoActiveJobs(final JobScheduler__c scheduler) {
        QueuesSupport.isTrue(
            !jobService.hasJobsBySchedulerId(scheduler.Id, ACTIVE_JOB_STATES),
            new Queues.InvalidOperationException(INVALID_REMOVE_ACTIVE_JOBS_MESSAGE)
        );
    }

    private static void requireNoActiveJobs(final List<Job__c> jobs) {
        for (Job__c job : jobs) {
            QueuesSupport.isTrue(
                QueuesSupport.toState(job.State__c) != Queues.State.ACTIVE,
                new Queues.InvalidOperationException(INVALID_REMOVE_ACTIVE_JOBS_MESSAGE)
            );
        }
    }

    private QueuesJobSchedulerRepository getSchedulerRepository(final Boolean asSystem) {
        return asSystem == true ? systemModeSchedulerRepository : userModeSchedulerRepository;
    }

    private void deactivateScheduler(final JobScheduler__c scheduler) {
        deactivateScheduler(scheduler, false);
    }

    private void deactivateSchedulerAsSystem(final JobScheduler__c scheduler) {
        deactivateScheduler(scheduler, true);
    }

    private void deactivateScheduler(final JobScheduler__c scheduler, final Boolean asSystem) {
        getSchedulerRepository(asSystem)
            .updateJobScheduler(
                new JobScheduler__c(
                    Id = scheduler.Id,
                    IsActive__c = false,
                    NextRunAt__c = null,
                    ScheduledJobId__c = null
                )
            );
    }
    // </editor-fold>

    // <editor-fold desc="Time And Identity">
    private static Datetime resolveInitialEveryRunAt(final Queues.RepeatOptions repeatOpts, final Datetime currentNow) {
        Datetime startAt = repeatOpts.getStartAt();
        return resolveCurrentOrFutureEveryRunAt(
            startAt ?? currentNow,
            repeatOpts.getEveryMinutes(),
            currentNow,
            repeatOpts.getEndAt(),
            startAt == null
        );
    }

    @TestVisible
    private static Datetime resolveSubsequentEveryRunAt(
        final JobScheduler__c scheduler,
        final Integer nextIterationCount,
        final Datetime currentNow
    ) {
        if (!canMaterializeIteration(scheduler, nextIterationCount)) {
            return null;
        }
        Integer everyMinutes = QueuesSupport.toInteger(scheduler.EveryMinutes__c, null);
        return resolveCurrentOrFutureEveryRunAt(
            scheduler.NextRunAt__c?.addMinutes(everyMinutes),
            everyMinutes,
            currentNow,
            scheduler.EndAt__c,
            false
        );
    }

    private static Datetime resolveCurrentOrFutureEveryRunAt(
        final Datetime candidateRunAt,
        final Integer everyMinutes,
        final Datetime currentNow,
        final Datetime endAt,
        final Boolean useMinimumFutureWhenDueNow
    ) {
        if (candidateRunAt == null || everyMinutes == null || everyMinutes <= 0) {
            return null;
        }

        Boolean useMinimumFuture = useMinimumFutureWhenDueNow ?? false;
        Datetime nextRunAt = candidateRunAt;
        if (nextRunAt <= currentNow) {
            Long everyMs = ((Long) everyMinutes) * 60000;
            Long elapsedMs = currentNow.getTime() - nextRunAt.getTime();
            Long intervals = elapsedMs / everyMs;
            nextRunAt = addEveryIntervals(candidateRunAt, everyMinutes, intervals);
            if (nextRunAt < currentNow || (nextRunAt == currentNow && !useMinimumFuture)) {
                nextRunAt = addEveryIntervals(candidateRunAt, everyMinutes, intervals + 1);
            } else if (nextRunAt == currentNow) {
                nextRunAt = resolveMinimumFutureScheduleRunAt(currentNow);
            }
        }
        return isWithinWindow(nextRunAt, endAt) ? nextRunAt : null;
    }

    private static Datetime addEveryIntervals(
        final Datetime startAt,
        final Integer everyMinutes,
        final Long intervals
    ) {
        return startAt.addMinutes(everyMinutes * (Integer) intervals);
    }

    private static Datetime resolveEveryRepairRunAt(final Datetime nextRunAt, final Datetime currentNow) {
        if (nextRunAt == null || nextRunAt <= currentNow) {
            return resolveMinimumFutureScheduleRunAt(currentNow);
        }
        return nextRunAt;
    }

    private static Datetime resolveMinimumFutureScheduleRunAt(final Datetime currentNow) {
        return currentNow.addMinutes(MINIMUM_FUTURE_SCHEDULE_OFFSET_MINUTES);
    }

    private static Boolean canMaterializeScheduledOccurrence(
        final JobScheduler__c scheduler,
        final Integer nextIterationCount,
        final Datetime nextRunAt
    ) {
        return canMaterializeIteration(scheduler, nextIterationCount) && nextRunAt != null;
    }

    private static Boolean canMaterializeIteration(final JobScheduler__c scheduler, final Integer nextIterationCount) {
        Integer limitCount = QueuesSupport.toInteger(scheduler.Limit__c, null);
        return limitCount == null || nextIterationCount <= limitCount;
    }

    private static void requireMaterializableIteration(
        final String queueName,
        final String jobSchedulerId,
        final Integer limitCount,
        final Integer nextIterationCount
    ) {
        QueuesSupport.isTrue(
            limitCount == null || nextIterationCount <= limitCount,
            new Queues.ConfigurationException(
                String.format(NO_FUTURE_RUN_MESSAGE, new List<String>{ queueName, jobSchedulerId })
            )
        );
    }

    private static Boolean isWithinWindow(final Datetime runAt, final Datetime endAt) {
        return endAt == null || runAt <= endAt;
    }

    private static void requireFutureRun(final String queueName, final String jobSchedulerId, final Datetime runAt) {
        QueuesSupport.isTrue(
            runAt != null,
            new Queues.ConfigurationException(
                String.format(NO_FUTURE_RUN_MESSAGE, new List<String>{ queueName, jobSchedulerId })
            )
        );
    }

    private static String buildSchedulerExternalId(final String queueName, final String jobSchedulerId) {
        return QueuesSupport.buildQueueScopedExternalId(queueName, jobSchedulerId);
    }

    private static String buildJobSchedulerJobExternalId(
        final JobScheduler__c scheduler,
        final Integer iterationCount
    ) {
        return String.format('scheduler:{0}:{1}', new List<Object>{ scheduler.Id, iterationCount });
    }

    private static String resolveScheduledJobNamePrefix(final JobScheduler__c scheduler) {
        if (String.isNotBlank(scheduler.Pattern__c)) {
            return QueuesJobSchedulerSchedule.resolvePatternJobNamePrefix(scheduler.Id);
        }
        return QueuesJobSchedulerSchedule.resolveEveryJobNamePrefix(scheduler.Id);
    }
    // </editor-fold>

    // <editor-fold desc="Insert Failure Helpers">
    private static Exception buildInsertFailure(final Database.SaveResult saveResult) {
        List<Database.Error> errors = saveResult.getErrors();
        return new Queues.QueueException(errors[0].getMessage());
    }

    private static Boolean isDuplicateJobInsert(final Database.SaveResult saveResult) {
        List<Database.Error> errors = saveResult.getErrors();

        for (Database.Error error : errors) {
            if (!isDuplicateInsertStatusCode(error.getStatusCode())) {
                return false;
            }
        }
        return true;
    }

    private static Boolean isDuplicateInsertException(final DmlException ex) {
        for (Integer i = 0; i < ex.getNumDml(); i++) {
            if (!isDuplicateInsertStatusCode(ex.getDmlType(i))) {
                return false;
            }
        }
        return ex.getNumDml() > 0;
    }

    private static Boolean isDuplicateInsertStatusCode(final System.StatusCode statusCode) {
        return statusCode == System.StatusCode.DUPLICATE_VALUE || statusCode == System.StatusCode.DUPLICATES_DETECTED;
    }
    // </editor-fold>

    private class InitialSchedulePlan {
        private final String scheduledJobId;
        private final Datetime runAt;

        private InitialSchedulePlan(final String scheduledJobId, final Datetime runAt) {
            this.scheduledJobId = scheduledJobId;
            this.runAt = runAt;
        }
    }

    private class SchedulerJobProcessResult {
        private final Integer processedCount;
        private final Boolean completed;

        private SchedulerJobProcessResult(final Integer processedCount, final Boolean completed) {
            this.processedCount = processedCount;
            this.completed = completed;
        }
    }
}
