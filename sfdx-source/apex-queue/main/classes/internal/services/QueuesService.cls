/**
 * @description Internal orchestration layer behind the Queues facade.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesService {
    private static final Integer MAX_JOB_ID_LENGTH = 174;

    private static final String WORKER_SCHEDULING_COMPONENT = 'worker scheduling';
    private static final String QUEUEABLE_DISPATCH_COMPONENT = 'queueable dispatch';
    private static final String INVOCABLE_ENQUEUE_COMPONENT = 'invocable enqueue';
    private static final String DUPLICATE_QUEUE_JOB_RESOLUTION_MESSAGE = 'Failed to resolve duplicate queue job "{0}" after enqueue race.';
    private static final String DUPLICATE_INSERT_IDEMPOTENT_JOB_MESSAGE = 'Duplicate insert detection expects an idempotent job with ExternalId__c.';
    private static final String DUPLICATE_INSERT_SAVE_RESULT_MESSAGE = 'Duplicate insert detection expects a failed save result.';
    private static final String EXISTING_QUEUE_JOB_EXECUTION_MODE_MESSAGE = 'Existing queue job execution mode does not match bulk execution mode.';
    private static final String INSERT_DML_ERRORS_MISSING_MESSAGE = 'Failed queue job insert did not expose any DML errors.';
    private static final String INSERT_ERROR_MESSAGE_MISSING_MESSAGE = 'Failed queue job insert did not expose an error message.';
    private static final String INSERT_FAILURE_SAVE_RESULT_MESSAGE = 'Queue insert failure expected a failed save result.';
    private static final String INSERT_RESULTS_MISMATCH_MESSAGE = 'Queue insert results did not match the requested job count.';
    private static final String INVALID_JOB_ID_LENGTH_MESSAGE = 'jobId must be {0} characters or fewer';

    private QueuesDefinitionService definitionService = new QueuesDefinitionService();
    private QueuesErrorService errorService = new QueuesErrorService();
    private QueuesFlowProducerService flowProducerService = new QueuesFlowProducerService();
    private QueuesJobService jobService = new QueuesJobService();
    private QueuesJobAdminService jobAdminService = new QueuesJobAdminService();
    private QueuesJobSchedulerService jobSchedulerService = new QueuesJobSchedulerService();
    private QueuesMaintenanceService maintenanceService = new QueuesMaintenanceService();
    private QueuesMaintenanceSchedule maintenanceSchedule = new QueuesMaintenanceSchedule();
    private QueuesProcessorService processorService = new QueuesProcessorService();
    private QueuesQueueableDispatchService queueableDispatchService = new QueuesQueueableDispatchService();
    private QueuesQueueAdminService queueAdminService = new QueuesQueueAdminService();
    private QueuesStatsService statsService = new QueuesStatsService();
    private QueuesWorkerService workerService = new QueuesWorkerService();

    public QueuesService() {
    }

    @TestVisible
    private QueuesService(
        final QueuesDefinitionService definitionService,
        final QueuesErrorService errorService,
        final QueuesFlowProducerService flowProducerService,
        final QueuesJobService jobService,
        final QueuesJobAdminService jobAdminService,
        final QueuesJobSchedulerService jobSchedulerService,
        final QueuesMaintenanceService maintenanceService,
        final QueuesMaintenanceSchedule maintenanceSchedule,
        final QueuesProcessorService processorService,
        final QueuesQueueableDispatchService queueableDispatchService,
        final QueuesQueueAdminService queueAdminService,
        final QueuesStatsService statsService,
        final QueuesWorkerService workerService
    ) {
        this.definitionService = definitionService;
        this.errorService = errorService;
        this.flowProducerService = flowProducerService;
        this.jobService = jobService;
        this.jobAdminService = jobAdminService;
        this.jobSchedulerService = jobSchedulerService;
        this.maintenanceService = maintenanceService;
        this.maintenanceSchedule = maintenanceSchedule;
        this.processorService = processorService;
        this.queueableDispatchService = queueableDispatchService;
        this.queueAdminService = queueAdminService;
        this.statsService = statsService;
        this.workerService = workerService;
    }

    // <editor-fold desc="Validation">
    public static void requireValidJobId(final String jobId) {
        QueuesSupport.notBlank(jobId, 'jobId');
        QueuesSupport.isTrue(
            jobId.length() <= MAX_JOB_ID_LENGTH,
            new Queues.QueueException(
                String.format(INVALID_JOB_ID_LENGTH_MESSAGE, new List<Integer>{ MAX_JOB_ID_LENGTH })
            )
        );
    }
    // </editor-fold>

    // <editor-fold desc="Facade">
    public Job__c enqueue(final String queueName, final Queues.JobData job) {
        return enqueue(queueName, new List<Queues.JobData>{ job })[0];
    }

    public List<Job__c> enqueue(final String queueName, final List<Queues.JobData> jobs) {
        return enqueue(queueName, jobs, resolveDefaultBulkJobOptions());
    }

    public List<Job__c> enqueue(
        final String queueName,
        final List<Queues.JobData> jobs,
        final Queues.BulkJobOptions bulkOptions
    ) {
        QueuesSupport.notBlank(queueName, 'queueName');
        QueuesSupport.notNull(bulkOptions, 'bulkOptions');
        String dedupeKey = bulkOptions.getDedupeKey();
        if (dedupeKey != null) {
            QueuesSupport.notBlank(dedupeKey, 'dedupeKey');
        }
        QueuesSupport.notNull(jobs, 'jobs');
        QueuesSupport.noNullElements(jobs, 'job');
        if (jobs.isEmpty()) {
            return new List<Job__c>();
        }

        Datetime now = QueuesSupport.now();
        validateBulkJobOptions(jobs, bulkOptions, now);
        Queues.ExecutionMode executionMode = bulkOptions.getExecutionMode();

        QueueDefinition__mdt queueDefinition = definitionService.requireQueueDefinition(queueName, true);
        Map<String, Processor__mdt> processorByClass = processorService.requireProcessors(jobs);
        Map<String, Job__c> existingJobByExternalId = jobService.getJobsByExternalId(queueName, jobs);
        EnqueuePlan enqueuePlan = mapToEnqueuePlan(
            queueName,
            jobs,
            queueDefinition,
            processorByClass,
            existingJobByExternalId,
            executionMode,
            now
        );

        if (QueuesSupport.isQueueableExecutionMode(executionMode)) {
            return enqueueQueueableJobs(queueName, dedupeKey, executionMode, enqueuePlan);
        }
        if (QueuesSupport.isInvocableExecutionMode(executionMode)) {
            return enqueueInvocableJobs(queueName, enqueuePlan);
        }

        return enqueueWorkerJobs(queueName, enqueuePlan, now);
    }

    public Queues.JobNode addFlow(final Queues.FlowJob flow) {
        return flowProducerService.add(flow);
    }

    public Queues.JobNode addFlow(final Queues.FlowJob flow, final Queues.FlowOpts opts) {
        return flowProducerService.add(flow, opts);
    }

    public List<Queues.JobNode> addFlowBulk(final List<Queues.FlowJob> flows) {
        return flowProducerService.addBulk(flows);
    }

    public List<Queues.JobNode> addFlowBulk(final List<Queues.FlowJob> flows, final Queues.FlowOpts opts) {
        return flowProducerService.addBulk(flows, opts);
    }

    public Queues.JobNode getFlow(final Id rootJobId) {
        return flowProducerService.getFlow(rootJobId);
    }

    public QueueRuntime__c pause(final String queueName) {
        return queueAdminService.pause(queueName);
    }

    public QueueRuntime__c resume(final String queueName) {
        return queueAdminService.resume(queueName);
    }

    public Integer wake(final String queueName) {
        return queueAdminService.wake(queueName);
    }

    public Integer recoverStalled(final String queueName) {
        return queueAdminService.recoverStalled(queueName);
    }

    public Integer cancel(final String queueName) {
        return queueAdminService.cancel(queueName);
    }

    public Integer drain(final String queueName) {
        return queueAdminService.drain(queueName);
    }

    public Queues.WorkerStatus getWorkerStatus(final String queueName) {
        return queueAdminService.getWorkerStatus(queueName);
    }

    public Boolean moveWorkerToFront(final String queueName) {
        return queueAdminService.moveWorkerToFront(queueName);
    }

    public Boolean moveWorkerToEnd(final String queueName) {
        return queueAdminService.moveWorkerToEnd(queueName);
    }

    public Queues.MaintenanceResult runRuntimeMaintenance() {
        return maintenanceService.runRuntimeMaintenance();
    }

    public Queues.MaintenanceResult runJobSchedulerMaintenance() {
        return jobSchedulerService.runJobSchedulerMaintenanceAsSystem();
    }

    public Queues.MaintenanceResult runTerminalCleanupMaintenance() {
        return maintenanceService.runTerminalCleanupMaintenance();
    }

    public Id scheduleMaintenance() {
        return maintenanceSchedule.schedule();
    }

    public Id scheduleMaintenance(final String cronExpression) {
        return maintenanceSchedule.schedule(cronExpression);
    }

    public Integer unscheduleMaintenance() {
        return maintenanceSchedule.unschedule();
    }

    public Boolean isMaintenanceScheduled() {
        return maintenanceSchedule.isScheduled();
    }

    public List<QueueError__c> insertQueueErrors(final List<QueueError__c> errors) {
        return errorService.insertQueueErrors(errors);
    }

    public Queues.QueueStats getStats(final String queueName) {
        return statsService.getStats(queueName);
    }

    public Queues.State getJobState(final Id jobId) {
        return jobAdminService.getJobState(jobId);
    }

    public Job__c retryJob(final Id jobId) {
        return jobAdminService.retryJob(jobId);
    }

    public Job__c pauseJob(final Id jobId) {
        return jobAdminService.pauseJob(jobId);
    }

    public Job__c resumeJob(final Id jobId) {
        return jobAdminService.resumeJob(jobId);
    }

    public Job__c promoteJob(final Id jobId) {
        return jobAdminService.promoteJob(jobId);
    }

    public Job__c discardJob(final Id jobId) {
        return jobAdminService.discardJob(jobId);
    }

    public Job__c cancelJob(final Id jobId) {
        return jobAdminService.cancelJob(jobId);
    }

    public Job__c deferJob(final Id jobId, final Integer delayMinutes) {
        return jobAdminService.deferJob(jobId, delayMinutes);
    }

    public Job__c changePriority(final Id jobId, final Integer priority) {
        return jobAdminService.changePriority(jobId, priority);
    }

    public Job__c updateJobData(final Id jobId, final Object data) {
        return jobAdminService.updateJobData(jobId, data);
    }

    public Job__c updateJobProgress(final Id jobId, final Object progress) {
        return jobAdminService.updateJobProgress(jobId, progress);
    }

    public void removeJob(final Id jobId) {
        jobAdminService.removeJob(jobId);
    }

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
        return jobSchedulerService.upsertJobScheduler(
            queueName,
            jobSchedulerId,
            repeatOpts,
            jobTemplate,
            executionMode
        );
    }

    public Boolean removeJobScheduler(final String queueName, final String jobSchedulerId) {
        return jobSchedulerService.removeJobScheduler(queueName, jobSchedulerId);
    }

    public Boolean cancelJobScheduler(final String queueName, final String jobSchedulerId) {
        return jobSchedulerService.cancelJobScheduler(queueName, jobSchedulerId);
    }

    public Queues.JobScheduler getJobScheduler(final String queueName, final String jobSchedulerId) {
        return jobSchedulerService.getJobScheduler(queueName, jobSchedulerId);
    }

    public List<Queues.JobScheduler> getJobSchedulers(
        final String queueName,
        final Integer startIndex,
        final Integer endIndex,
        final Boolean ascending
    ) {
        return jobSchedulerService.getJobSchedulers(queueName, startIndex, endIndex, ascending);
    }

    public Integer getJobSchedulersCount(final String queueName) {
        return jobSchedulerService.getJobSchedulersCount(queueName);
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private List<Job__c> enqueueWorkerJobs(final String queueName, final EnqueuePlan enqueuePlan, final Datetime now) {
        Savepoint savepoint = Database.setSavepoint();
        try {
            jobService.insertJobs(enqueuePlan.jobsToInsert);
            reconcileEnqueuedJobs(
                queueName,
                enqueuePlan.enqueuedJobs,
                enqueuePlan.idempotentJobsToInsert,
                jobService.insertJobsAllowingPartialSuccess(enqueuePlan.idempotentJobsToInsert),
                Queues.ExecutionMode.WORKER
            );
        } catch (Exception ex) {
            Database.rollback(savepoint);
            throw ex;
        }

        try {
            workerService.scheduleWorkerForJobs(queueName, enqueuePlan.enqueuedJobs, now);
            return enqueuePlan.enqueuedJobs;
        } catch (Exception ex) {
            Database.rollback(savepoint);
            errorService.recordQueueError(queueName, WORKER_SCHEDULING_COMPONENT, ex);
            throw ex;
        }
    }

    private List<Job__c> enqueueQueueableJobs(
        final String queueName,
        final String dedupeKey,
        final Queues.ExecutionMode executionMode,
        final EnqueuePlan enqueuePlan
    ) {
        QueueableDispatch__c queueableDispatch;
        Savepoint savepoint = Database.setSavepoint();
        try {
            jobService.insertJobs(enqueuePlan.jobsToInsert);
            List<Database.SaveResult> idempotentSaveResults = enqueuePlan.idempotentJobsToInsert.isEmpty()
                ? new List<Database.SaveResult>()
                : jobService.insertJobsAllowingPartialSuccess(enqueuePlan.idempotentJobsToInsert);
            ReconcileResult reconcileResult = reconcileEnqueuedJobs(
                queueName,
                enqueuePlan.enqueuedJobs,
                enqueuePlan.idempotentJobsToInsert,
                idempotentSaveResults,
                executionMode
            );
            List<Job__c> jobsToDispatch = new List<Job__c>();
            jobsToDispatch.addAll(enqueuePlan.jobsToInsert);
            jobsToDispatch.addAll(reconcileResult.insertedJobs);
            if (!jobsToDispatch.isEmpty()) {
                queueableDispatch = queueableDispatchService.createWaitingDispatch(
                    queueName,
                    dedupeKey,
                    executionMode,
                    jobsToDispatch
                );
                jobService.updateJobs(jobsToDispatch);
            }
        } catch (Exception ex) {
            Database.rollback(savepoint);
            throw ex;
        }

        try {
            if (queueableDispatch != null) {
                queueableDispatchService.enqueueDispatcher(queueableDispatch);
            }
            return enqueuePlan.enqueuedJobs;
        } catch (Exception ex) {
            Database.rollback(savepoint);
            errorService.recordQueueError(queueName, QUEUEABLE_DISPATCH_COMPONENT, ex);
            throw ex;
        }
    }

    private List<Job__c> enqueueInvocableJobs(final String queueName, final EnqueuePlan enqueuePlan) {
        Savepoint savepoint = Database.setSavepoint();
        try {
            jobService.insertJobs(enqueuePlan.jobsToInsert);
            List<Database.SaveResult> idempotentSaveResults = enqueuePlan.idempotentJobsToInsert.isEmpty()
                ? new List<Database.SaveResult>()
                : jobService.insertJobsAllowingPartialSuccess(enqueuePlan.idempotentJobsToInsert);
            reconcileEnqueuedJobs(
                queueName,
                enqueuePlan.enqueuedJobs,
                enqueuePlan.idempotentJobsToInsert,
                idempotentSaveResults,
                Queues.ExecutionMode.INVOCABLE
            );

            return enqueuePlan.enqueuedJobs;
        } catch (Exception ex) {
            Database.rollback(savepoint);
            errorService.recordQueueError(queueName, INVOCABLE_ENQUEUE_COMPONENT, ex);
            throw ex;
        }
    }

    private EnqueuePlan mapToEnqueuePlan(
        final String queueName,
        final List<Queues.JobData> jobs,
        final QueueDefinition__mdt queueDefinition,
        final Map<String, Processor__mdt> processorByClass,
        final Map<String, Job__c> existingJobByExternalId,
        final Queues.ExecutionMode executionMode,
        final Datetime now
    ) {
        EnqueuePlan enqueuePlan = new EnqueuePlan();
        Map<String, Job__c> createdJobByExternalId = new Map<String, Job__c>();
        for (Queues.JobData jobData : jobs) {
            String externalId = jobData.getOpts().getJobId();
            Job__c existingJob = jobService.getResolvedJob(existingJobByExternalId, createdJobByExternalId, externalId);
            if (existingJob != null) {
                requireMatchingExecutionMode(existingJob, executionMode);
                enqueuePlan.enqueuedJobs.add(existingJob);
                continue;
            }

            Job__c job = jobService.mapToJob(
                queueName,
                jobData,
                queueDefinition,
                processorByClass.get(jobData.getProcessor()),
                now,
                executionMode
            );
            enqueuePlan.enqueuedJobs.add(job);
            if (String.isNotBlank(externalId)) {
                enqueuePlan.idempotentJobsToInsert.add(job);
            } else {
                enqueuePlan.jobsToInsert.add(job);
            }
            if (String.isNotBlank(externalId)) {
                createdJobByExternalId.put(QueuesSupport.normalizeCaseInsensitiveKey(externalId), job);
            }
        }
        return enqueuePlan;
    }

    private static void requireMatchingExecutionMode(
        final Job__c existingJob,
        final Queues.ExecutionMode executionMode
    ) {
        QueuesSupport.isTrue(
            QueuesSupport.toExecutionMode(existingJob.ExecutionMode__c) == executionMode,
            new Queues.QueueException(EXISTING_QUEUE_JOB_EXECUTION_MODE_MESSAGE)
        );
    }

    private ReconcileResult reconcileEnqueuedJobs(
        final String queueName,
        final List<Job__c> enqueuedJobs,
        final List<Job__c> jobsToInsert,
        final List<Database.SaveResult> saveResults,
        final Queues.ExecutionMode executionMode
    ) {
        QueuesSupport.isTrue(
            saveResults != null && saveResults.size() == jobsToInsert.size(),
            INSERT_RESULTS_MISMATCH_MESSAGE
        );

        ReconcileResult reconcileResult = new ReconcileResult();
        Set<String> duplicateExternalIds = new Set<String>();
        for (Integer index = 0; index < jobsToInsert.size(); index++) {
            Job__c job = jobsToInsert[index];
            Database.SaveResult saveResult = saveResults[index];
            if (saveResult.isSuccess()) {
                if (job.Id == null) {
                    job.Id = saveResult.getId();
                }
                reconcileResult.insertedJobs.add(job);
                continue;
            }

            if (isDuplicateJobInsert(job, saveResult)) {
                duplicateExternalIds.add(job.ExternalId__c);
                continue;
            }

            throw buildInsertFailure(saveResult);
        }

        if (duplicateExternalIds.isEmpty()) {
            return reconcileResult;
        }

        Map<String, Job__c> duplicateJobsByExternalId = jobService.getJobsByExternalId(queueName, duplicateExternalIds);
        for (Integer index = 0; index < enqueuedJobs.size(); index++) {
            Job__c enqueuedJob = enqueuedJobs[index];
            if (enqueuedJob == null || enqueuedJob.Id != null || String.isBlank(enqueuedJob.ExternalId__c)) {
                continue;
            }

            Job__c duplicateJob = duplicateJobsByExternalId.get(
                QueuesSupport.normalizeCaseInsensitiveKey(enqueuedJob.ExternalId__c)
            );
            QueuesSupport.isTrue(
                duplicateJob != null,
                String.format(DUPLICATE_QUEUE_JOB_RESOLUTION_MESSAGE, new List<String>{ enqueuedJob.ExternalId__c })
            );
            requireMatchingExecutionMode(duplicateJob, executionMode);
            enqueuedJobs[index] = duplicateJob;
        }
        return reconcileResult;
    }

    private static Exception buildInsertFailure(final Database.SaveResult saveResult) {
        QueuesSupport.isTrue(saveResult != null && !saveResult.isSuccess(), INSERT_FAILURE_SAVE_RESULT_MESSAGE);

        List<Database.Error> errors = saveResult.getErrors();
        QueuesSupport.isTrue(errors != null && !errors.isEmpty(), INSERT_DML_ERRORS_MISSING_MESSAGE);

        String message = errors[0].getMessage();
        QueuesSupport.isTrue(String.isNotBlank(message), INSERT_ERROR_MESSAGE_MISSING_MESSAGE);
        return new Queues.QueueException(message);
    }

    private static Boolean isDuplicateJobInsert(final Job__c job, final Database.SaveResult saveResult) {
        QueuesSupport.isTrue(
            job != null && String.isNotBlank(job.ExternalId__c),
            DUPLICATE_INSERT_IDEMPOTENT_JOB_MESSAGE
        );
        QueuesSupport.isTrue(saveResult != null && !saveResult.isSuccess(), DUPLICATE_INSERT_SAVE_RESULT_MESSAGE);

        List<Database.Error> errors = saveResult.getErrors();
        QueuesSupport.isTrue(errors != null && !errors.isEmpty(), INSERT_DML_ERRORS_MISSING_MESSAGE);

        for (Database.Error error : errors) {
            if (!isDuplicateInsertStatusCode(error.getStatusCode())) {
                return false;
            }
        }
        return true;
    }

    private static Boolean isDuplicateInsertStatusCode(final System.StatusCode statusCode) {
        return statusCode == System.StatusCode.DUPLICATE_VALUE || statusCode == System.StatusCode.DUPLICATES_DETECTED;
    }

    private static Queues.BulkJobOptions resolveDefaultBulkJobOptions() {
        return new Queues.BulkJobOptions();
    }

    private static void validateBulkJobOptions(
        final List<Queues.JobData> jobs,
        final Queues.BulkJobOptions bulkOptions,
        final Datetime now
    ) {
        if (QueuesSupport.isQueueableExecutionMode(bulkOptions.getExecutionMode())) {
            validateQueueableJobOptions(jobs, now);
        }
    }

    private static void validateQueueableJobOptions(final List<Queues.JobData> jobs, final Datetime now) {
        for (Queues.JobData job : jobs) {
            Queues.JobOptions options = job.getOpts();
            Integer delayMinutes = resolveQueueableDelayMinutes(options, now);
            QueuesQueueableTransportService.requireValidNativeQueueableDelayMinutes(delayMinutes);
        }
    }

    private static Integer resolveQueueableDelayMinutes(final Queues.JobOptions options, final Datetime now) {
        if (options.getAvailableAt() != null) {
            return QueuesQueueableTransportService.resolveNativeQueueableDelayMinutes(options.getAvailableAt(), now);
        }
        return options.getDelay() ?? 0;
    }

    private class EnqueuePlan {
        private final List<Job__c> enqueuedJobs = new List<Job__c>();
        private final List<Job__c> jobsToInsert = new List<Job__c>();
        private final List<Job__c> idempotentJobsToInsert = new List<Job__c>();
    }

    private class ReconcileResult {
        private final List<Job__c> insertedJobs = new List<Job__c>();
    }
    // </editor-fold>
}
