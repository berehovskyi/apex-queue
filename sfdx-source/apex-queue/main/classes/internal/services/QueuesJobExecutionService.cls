/**
 * @description Executes jobs and finalizes their attempts across transports.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesJobExecutionService {
    private static final Set<Queues.State> RUNNABLE_COMPLETION_STATES = new Set<Queues.State>{
        Queues.State.WAITING,
        Queues.State.DELAYED
    };

    private QueuesJobService jobService = new QueuesJobService();
    private QueuesJobRunService jobRunService = new QueuesJobRunService();
    private QueuesProcessorService processorService = new QueuesProcessorService();

    // Lazily constructed to break the FlowProducer -> Worker -> JobExecution -> FlowProducer construction cycle.
    private final QueuesFlowProducerService flowProducerService {
        get {
            if (flowProducerService == null) {
                flowProducerService = new QueuesFlowProducerService();
            }
            return flowProducerService;
        }
        set;
    }

    public QueuesJobExecutionService() {
    }

    @TestVisible
    private QueuesJobExecutionService(
        final QueuesJobService jobService,
        final QueuesJobRunService jobRunService,
        final QueuesProcessorService processorService,
        final QueuesFlowProducerService flowProducerService
    ) {
        this.jobService = jobService;
        this.jobRunService = jobRunService;
        this.processorService = processorService;
        this.flowProducerService = flowProducerService;
    }

    // <editor-fold desc="Execution">
    public Job__c executeQueueableJob(final Id jobId, final String asyncApexJobId) {
        QueuesSupport.notNull(jobId, 'jobId');
        QueuesSupport.notBlank(asyncApexJobId, 'asyncApexJobId');

        return executeSingleJob(
            beginExecution(jobService.requireJobAsSystem(jobId), null, asyncApexJobId, ExecutionTransport.QUEUEABLE)
        );
    }

    public Job__c executeInvocableJob(final Id jobId, final String leaseToken) {
        QueuesSupport.notNull(jobId, 'jobId');
        QueuesSupport.notBlank(leaseToken, 'leaseToken');

        return executeSingleJob(
            beginExecution(jobService.requireJobAsSystem(jobId), leaseToken, null, ExecutionTransport.INVOCABLE)
        );
    }

    public Job__c failQueueableJob(final Id jobId, final String asyncApexJobId, final String failedReason) {
        QueuesSupport.notNull(jobId, 'jobId');
        QueuesSupport.notBlank(asyncApexJobId, 'asyncApexJobId');

        Job__c job = jobService.requireJobForUpdateAsSystem(jobId);
        if (!isOwnedForQueueableCompletion(job, asyncApexJobId)) {
            return job;
        }

        Datetime now = QueuesSupport.now();
        Integer attemptNumber = QueuesSupport.toInteger(job.AttemptsMade__c, 0) + 1;
        job.AttemptsMade__c = attemptNumber;
        jobService.applyFailure(job, failedReason, now);
        jobService.updateJobAsSystem(job);

        JobRun__c run = new JobRun__c(
            Job__c = job.Id,
            AttemptNumber__c = attemptNumber,
            StartedAt__c = now,
            Status__c = Queues.RunStatus.FAILED.name(),
            Message__c = job.FailedReason__c,
            AsyncApexJobId__c = asyncApexJobId
        );
        jobRunService.addTimestamps(run, now.getTime(), now);
        jobRunService.insertJobRunsAsSystem(new List<JobRun__c>{ run });
        flowProducerService.deferParentDependenciesAsSystem(new List<Job__c>{ job });
        return job;
    }

    public Job__c failInvocableJob(final Id jobId, final String leaseToken, final String failedReason) {
        QueuesSupport.notNull(jobId, 'jobId');
        QueuesSupport.notBlank(leaseToken, 'leaseToken');

        Job__c job = jobService.requireJobForUpdateAsSystem(jobId);
        if (!isOwnedForInvocableCompletion(job, leaseToken)) {
            return job;
        }

        Datetime now = QueuesSupport.now();
        Integer attemptNumber = QueuesSupport.toInteger(job.AttemptsMade__c, 0) + 1;
        job.AttemptsMade__c = attemptNumber;
        jobService.applyFailure(job, failedReason, now);
        jobService.updateJobAsSystem(job);

        JobRun__c run = new JobRun__c(
            Job__c = job.Id,
            AttemptNumber__c = attemptNumber,
            StartedAt__c = now,
            Status__c = Queues.RunStatus.FAILED.name(),
            Message__c = job.FailedReason__c
        );
        jobRunService.addTimestamps(run, now.getTime(), now);
        jobRunService.insertJobRunsAsSystem(new List<JobRun__c>{ run });
        flowProducerService.resolveParentDependenciesAsSystem(new List<Job__c>{ job });
        return job;
    }

    public void executeClaimedBatchJobs(final List<Job__c> claimedJobs, final String asyncApexJobId) {
        if (claimedJobs == null || claimedJobs.isEmpty()) {
            return;
        }

        Map<Id, Job__c> currentJobsById = new Map<Id, Job__c>(
            jobService.queryJobsAsSystem(QueuesSupport.toIdSet(claimedJobs))
        );
        Map<String, Processor__mdt> processorByClass = processorService.getProcessorsByClassAsSystem(
            QueuesSupport.toStringSet(currentJobsById.values(), 'ProcessorClass__c')
        );
        List<JobExecutionState> executions = new List<JobExecutionState>();
        for (Job__c claimedJob : claimedJobs) {
            JobExecutionState execution = beginExecution(
                currentJobsById.get(claimedJob.Id),
                claimedJob.LeaseToken__c,
                asyncApexJobId,
                ExecutionTransport.WORKER
            );
            if (!execution.executable) {
                continue;
            }

            executeJob(execution, processorByClass);
            executions.add(execution);
        }

        finalizeExecutions(executions, true);
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private static JobExecutionState beginExecution(
        final Job__c claimedJob,
        final String leaseToken,
        final String asyncApexJobId,
        final ExecutionTransport transport
    ) {
        JobExecutionState execution = new JobExecutionState(claimedJob, leaseToken, asyncApexJobId, transport);
        if (!isExecutableClaim(execution.job, leaseToken, asyncApexJobId, execution.transport)) {
            execution.executable = false;
        }
        return execution;
    }

    private static void startExecution(final JobExecutionState execution, final Datetime startedAt) {
        execution.startedAt = startedAt;
        execution.startedAtMs = startedAt.getTime();
        execution.attemptNumber = QueuesSupport.toInteger(execution.job.AttemptsMade__c, 0) + 1;
        execution.job.AttemptsMade__c = execution.attemptNumber;
    }

    private static Boolean isExecutableClaim(
        final Job__c claimedJob,
        final String leaseToken,
        final String asyncApexJobId,
        final ExecutionTransport transport
    ) {
        ExecutionTransport resolvedTransport = transport ?? ExecutionTransport.WORKER;
        switch on resolvedTransport {
            when QUEUEABLE {
                return isOwnedForQueueableCompletion(claimedJob, asyncApexJobId);
            }
            when INVOCABLE {
                return isExecutableInvocableClaim(claimedJob, leaseToken, QueuesSupport.now());
            }
            when else {
                return isExecutableWorkerClaim(claimedJob, leaseToken, QueuesSupport.now());
            }
        }
    }

    private static Boolean isExecutableWorkerClaim(
        final Job__c claimedJob,
        final String leaseToken,
        final Datetime now
    ) {
        return QueuesSupport.toState(claimedJob?.State__c) == Queues.State.ACTIVE &&
            QueuesSupport.isWorkerExecutionMode(QueuesSupport.toExecutionMode(claimedJob?.ExecutionMode__c)) &&
            (leaseToken == null ||
            claimedJob?.LeaseToken__c == leaseToken) &&
            String.isNotBlank(claimedJob?.LeaseToken__c) &&
            claimedJob?.LeaseExpiresAt__c >= now;
    }

    private static Boolean isExecutableInvocableClaim(
        final Job__c claimedJob,
        final String leaseToken,
        final Datetime now
    ) {
        return QueuesSupport.toState(claimedJob?.State__c) == Queues.State.ACTIVE &&
            QueuesSupport.isInvocableExecutionMode(QueuesSupport.toExecutionMode(claimedJob?.ExecutionMode__c)) &&
            String.isNotBlank(leaseToken) &&
            claimedJob?.LeaseToken__c == leaseToken &&
            claimedJob?.LeaseExpiresAt__c >= now;
    }

    private static Boolean isOwnedForCompletion(final Job__c claimedJob, final JobExecutionState execution) {
        ExecutionTransport resolvedTransport = execution?.transport ?? ExecutionTransport.WORKER;
        switch on resolvedTransport {
            when QUEUEABLE {
                return isOwnedForQueueableCompletion(claimedJob, execution?.asyncApexJobId);
            }
            when INVOCABLE {
                return isOwnedForInvocableCompletion(claimedJob, execution?.leaseToken);
            }
            when else {
                return isOwnedForBatchCompletion(claimedJob, execution?.leaseToken);
            }
        }
    }

    private static Boolean isOwnedForBatchCompletion(final Job__c claimedJob, final String leaseToken) {
        return QueuesSupport.toState(claimedJob?.State__c) == Queues.State.ACTIVE &&
            QueuesSupport.isWorkerExecutionMode(QueuesSupport.toExecutionMode(claimedJob?.ExecutionMode__c)) &&
            String.isNotBlank(leaseToken) &&
            claimedJob?.LeaseToken__c == leaseToken;
    }

    private static Boolean isOwnedForInvocableCompletion(final Job__c job, final String leaseToken) {
        return QueuesSupport.toState(job?.State__c) == Queues.State.ACTIVE &&
            QueuesSupport.isInvocableExecutionMode(QueuesSupport.toExecutionMode(job?.ExecutionMode__c)) &&
            String.isNotBlank(leaseToken) &&
            job?.LeaseToken__c == leaseToken;
    }

    private static Boolean isOwnedForQueueableCompletion(final Job__c job, final String asyncApexJobId) {
        return QueuesSupport.toState(job?.State__c) == Queues.State.ACTIVE &&
            QueuesSupport.isQueueableExecutionMode(QueuesSupport.toExecutionMode(job?.ExecutionMode__c)) &&
            String.isNotBlank(asyncApexJobId) &&
            job?.AsyncApexJobId__c == asyncApexJobId;
    }

    private static Set<Id> toExecutionJobIds(final List<JobExecutionState> executions) {
        Set<Id> jobIds = new Set<Id>();
        for (JobExecutionState execution : executions) {
            if (execution?.job?.Id != null) {
                jobIds.add(execution.job.Id);
            }
        }
        return jobIds;
    }

    private static Processor__mdt requireProcessorDefinition(
        final Map<String, Processor__mdt> processorByClass,
        final Job__c job
    ) {
        String processorClassName = job?.ProcessorClass__c;
        Processor__mdt processorDefinition = processorByClass?.get(processorClassName);
        QueuesSupport.isTrue(
            processorDefinition != null,
            new Queues.ConfigurationException(
                'Processor "' + processorClassName + '" does not have a processor metadata mapping.'
            )
        );
        QueuesSupport.isTrue(
            QueuesSupport.isActive(processorDefinition),
            new Queues.ConfigurationException(
                'Processor "' + processorClassName + '" has an inactive processor metadata mapping.'
            )
        );
        return processorDefinition;
    }

    private static void applySnapshot(final Job__c currentJob, final Job__c snapshotJob) {
        Boolean pauseRequested = currentJob.PauseRequested__c ?? false;
        currentJob.AttemptsMade__c = snapshotJob.AttemptsMade__c;
        currentJob.State__c = snapshotJob.State__c;
        currentJob.AvailableAt__c = snapshotJob.AvailableAt__c;
        currentJob.LeaseToken__c = snapshotJob.LeaseToken__c;
        currentJob.LeaseExpiresAt__c = snapshotJob.LeaseExpiresAt__c;
        currentJob.AsyncApexJobId__c = snapshotJob.AsyncApexJobId__c;
        currentJob.Progress__c = snapshotJob.Progress__c;
        currentJob.Checkpoint__c = snapshotJob.Checkpoint__c;
        currentJob.ReturnValue__c = snapshotJob.ReturnValue__c;
        currentJob.FailedReason__c = snapshotJob.FailedReason__c;
        currentJob.PauseRequested__c = snapshotJob.PauseRequested__c;
        applyConcurrentPauseRequest(currentJob, pauseRequested);
    }

    private static void applyConcurrentPauseRequest(final Job__c currentJob, final Boolean pauseRequested) {
        if (
            pauseRequested != true || !RUNNABLE_COMPLETION_STATES.contains(QueuesSupport.toState(currentJob.State__c))
        ) {
            return;
        }

        currentJob.State__c = Queues.State.PAUSED.name();
        currentJob.PauseRequested__c = false;
    }

    private static Queues.RunStatus normalizeRunStatus(final Queues.ProcessResult result) {
        switch on result.getOutcome() {
            when COMPLETED {
                return Queues.RunStatus.COMPLETED;
            }
            when FAILED {
                return Queues.RunStatus.FAILED;
            }
            when PAUSED {
                return Queues.RunStatus.PAUSED;
            }
            when DEFERRED {
                return Queues.RunStatus.DEFERRED;
            }
            when else {
                throw new Queues.QueueException('Unsupported processor outcome "' + result.getOutcome() + '".');
            }
        }
    }

    private Job__c executeSingleJob(final JobExecutionState execution) {
        if (!execution.executable) {
            return execution.job;
        }

        Map<String, Processor__mdt> processorByClass = processorService.getProcessorsByClassAsSystem(
            QueuesSupport.toStringSet(new List<Job__c>{ execution.job }, 'ProcessorClass__c')
        );
        executeJob(execution, processorByClass);

        Map<Id, Job__c> finalizedJobsById = finalizeExecutions(new List<JobExecutionState>{ execution }, false);
        return finalizedJobsById.get(execution.job.Id);
    }

    private void executeJob(final JobExecutionState execution, final Map<String, Processor__mdt> processorByClass) {
        startExecution(execution, QueuesSupport.now());
        executeProcessor(execution, processorByClass);
        finishExecution(execution);
    }

    private void executeProcessor(
        final JobExecutionState execution,
        final Map<String, Processor__mdt> processorByClass
    ) {
        try {
            Processor__mdt processorDefinition = requireProcessorDefinition(processorByClass, execution.job);
            Object processor = processorService.instantiate(processorDefinition.ProcessorClass__c);
            execution.result = processorService.execute(processor, execution.job);
        } catch (Queues.UnrecoverableJobException ex) {
            execution.unrecoverableExceptionMessage = QueuesSupport.resolveExceptionReason(ex);
        } catch (Exception ex) {
            execution.exceptionMessage = QueuesSupport.resolveExceptionReason(ex);
        }
    }

    private void finishExecution(final JobExecutionState execution) {
        execution.finishedAt = QueuesSupport.now();
        execution.run = new JobRun__c(
            Job__c = execution.job.Id,
            AttemptNumber__c = execution.attemptNumber,
            StartedAt__c = execution.startedAt,
            AsyncApexJobId__c = execution.asyncApexJobId
        );

        if (execution.unrecoverableExceptionMessage != null) {
            markFailed(execution.job, execution, execution.unrecoverableExceptionMessage, true);
        } else if (execution.exceptionMessage != null) {
            markFailed(execution.job, execution, execution.exceptionMessage, false);
        } else {
            try {
                jobService.applyResult(execution.job, execution.result, execution.finishedAt);
                execution.run.Status__c = normalizeRunStatus(execution.result).name();
                execution.run.Message__c = execution.result?.getFailedReason();
            } catch (Exception ex) {
                execution.finishedAt = QueuesSupport.now();
                execution.exceptionMessage = QueuesSupport.resolveExceptionReason(ex);
                markFailed(execution.job, execution, execution.exceptionMessage, false);
            }
        }

        jobRunService.addTimestamps(execution.run, execution.startedAtMs, execution.finishedAt);
    }

    private Map<Id, Job__c> finalizeExecutions(
        final List<JobExecutionState> executions,
        final Boolean deferParentResolution
    ) {
        Map<Id, Job__c> currentJobsById = new Map<Id, Job__c>();
        if (executions == null || executions.isEmpty()) {
            return currentJobsById;
        }

        currentJobsById = new Map<Id, Job__c>(jobService.queryJobsForUpdateAsSystem(toExecutionJobIds(executions)));
        List<Job__c> jobsToUpdate = new List<Job__c>();
        List<JobRun__c> runs = new List<JobRun__c>();

        for (JobExecutionState execution : executions) {
            Job__c currentJob = currentJobsById.get(execution.job?.Id);
            if (!isOwnedForCompletion(currentJob, execution)) {
                continue;
            }

            applySnapshot(currentJob, execution.job);
            jobsToUpdate.add(currentJob);
            runs.add(execution.run);
        }

        jobService.updateJobsAsSystem(jobsToUpdate);
        jobRunService.insertJobRunsAsSystem(runs);
        if (deferParentResolution == true) {
            flowProducerService.deferParentDependenciesAsSystem(jobsToUpdate);
        } else {
            flowProducerService.resolveParentDependenciesAsSystem(jobsToUpdate);
        }
        return currentJobsById;
    }

    private void markFailed(
        final Job__c job,
        final JobExecutionState execution,
        final String failureMessage,
        final Boolean isUnrecoverable
    ) {
        jobService.applyFailure(job, failureMessage, execution.finishedAt, isUnrecoverable);
        execution.run.Status__c = Queues.RunStatus.FAILED.name();
        execution.run.Message__c = failureMessage;
    }
    // </editor-fold>

    private enum ExecutionTransport {
        WORKER,
        QUEUEABLE,
        INVOCABLE
    }

    private class JobExecutionState {
        private final Job__c job;
        private final String leaseToken;
        private Boolean executable;
        private Datetime startedAt;
        private Long startedAtMs;
        private Integer attemptNumber;
        private Datetime finishedAt;
        private Queues.ProcessResult result;
        private String exceptionMessage;
        private String unrecoverableExceptionMessage;
        private final String asyncApexJobId;
        private final ExecutionTransport transport;
        private JobRun__c run;

        private JobExecutionState(
            final Job__c claimedJob,
            final String leaseToken,
            final String asyncApexJobId,
            final ExecutionTransport transport
        ) {
            ExecutionTransport resolvedTransport = transport ?? ExecutionTransport.WORKER;
            job = (Job__c) QueuesSupport.clone(claimedJob);
            this.leaseToken = resolvedTransport == ExecutionTransport.QUEUEABLE ? null : leaseToken;
            executable = claimedJob != null;
            this.asyncApexJobId = asyncApexJobId;
            this.transport = resolvedTransport;
        }
    }
}
