/**
 * @description Handles BatchApexErrorEvent platform events to recover failed batch worker runs.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesBatchApexErrorEventHandler {
    private static final String BATCH_WORKER_CLASS_NAME = QueuesBatchableWorker.class.getName();
    private static final String EXECUTE_PHASE = 'EXECUTE';
    private static final String BATCH_WORKER_FAILURE_RECOVERY_COMPONENT = 'batch worker failure recovery';

    private QueuesAsyncApexJobRepository asyncApexJobRepository = new QueuesAsyncApexJobRepository(
        System.AccessLevel.SYSTEM_MODE
    );
    private QueuesJobRunService jobRunService = new QueuesJobRunService();
    private QueuesJobService jobService = new QueuesJobService();
    private QueuesQueueEventService queueEventService = new QueuesQueueEventService();

    public QueuesBatchApexErrorEventHandler() {
    }

    @TestVisible
    private QueuesBatchApexErrorEventHandler(
        final QueuesAsyncApexJobRepository asyncApexJobRepository,
        final QueuesJobRunService jobRunService,
        final QueuesJobService jobService,
        final QueuesQueueEventService queueEventService
    ) {
        this.asyncApexJobRepository = asyncApexJobRepository;
        this.jobRunService = jobRunService;
        this.jobService = jobService;
        this.queueEventService = queueEventService;
    }

    // <editor-fold desc="Commands">
    public void handle(final List<BatchApexErrorEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        Map<Id, AsyncApexJob> jobsById = new Map<Id, AsyncApexJob>(
            asyncApexJobRepository.queryAsyncJobsByIds(QueuesSupport.toIdSet(events, 'AsyncApexJobId'))
        );

        handleFailureScopes(mapToFailureScopes(events, jobsById));
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    @TestVisible
    private static List<FailureScope> mapToFailureScopes(
        final List<BatchApexErrorEvent> events,
        final Map<Id, AsyncApexJob> jobsById
    ) {
        List<FailureScope> failureScopes = new List<FailureScope>();
        for (BatchApexErrorEvent eventRecord : events) {
            if (eventRecord.Phase != EXECUTE_PHASE) {
                continue;
            }

            Id asyncApexJobId = QueuesSupport.toId(eventRecord.AsyncApexJobId);
            AsyncApexJob asyncJob = jobsById.get(asyncApexJobId);
            if (asyncJob?.ApexClass?.Name != BATCH_WORKER_CLASS_NAME) {
                continue;
            }

            Set<Id> jobIds = extractScopedJobIds(eventRecord.JobScope);
            if (jobIds.isEmpty()) {
                continue;
            }

            failureScopes.add(
                new FailureScope(
                    jobIds,
                    asyncJob?.ParentJobId ?? eventRecord.AsyncApexJobId,
                    resolveOccurredAt(eventRecord),
                    buildFailureMessage(eventRecord.ExceptionType, eventRecord.Message),
                    eventRecord.StackTrace
                )
            );
        }
        return failureScopes;
    }

    @TestVisible
    private static Set<Id> extractScopedJobIds(final String jobScope) {
        Set<Id> jobIds = new Set<Id>();
        if (String.isBlank(jobScope)) {
            return jobIds;
        }

        for (String value : jobScope.removeStart('JobScope=').removeStart('[').removeEnd(']').split(',')) {
            Id jobId = QueuesSupport.toId(value.trim());
            if (jobId?.getSobjectType() == Job__c.SObjectType) {
                jobIds.add(jobId);
            }
        }
        return jobIds;
    }

    @TestVisible
    private static Boolean isCurrentFailureOwner(final Job__c job, final FailureScope failureScope) {
        if (QueuesSupport.toState(job?.State__c) != Queues.State.ACTIVE) {
            return false;
        }

        if (String.isBlank(failureScope?.asyncApexJobId)) {
            return true;
        }

        Id failureAsyncApexJobId = QueuesSupport.toId(failureScope.asyncApexJobId);
        Id jobAsyncApexJobId = QueuesSupport.toId(job?.AsyncApexJobId__c);
        return jobAsyncApexJobId != null && jobAsyncApexJobId == failureAsyncApexJobId;
    }

    private static String buildFailureMessage(final String exceptionType, final String message) {
        if (String.isBlank(exceptionType)) {
            return String.isBlank(message) ? 'Batch scope failed.' : message;
        }
        if (String.isBlank(message)) {
            return exceptionType;
        }
        return String.format('{0}: {1}', new List<String>{ exceptionType, message });
    }

    private static Datetime resolveOccurredAt(final BatchApexErrorEvent eventRecord) {
        return eventRecord.CreatedDate ?? QueuesSupport.now();
    }

    private static String buildRunMessage(final String failureMessage, final String stackTrace) {
        if (String.isBlank(stackTrace)) {
            return failureMessage;
        }
        if (String.isBlank(failureMessage)) {
            return stackTrace;
        }
        return String.format('{0}\n{1}', new List<String>{ failureMessage, stackTrace });
    }

    @TestVisible
    private void handleFailureScopes(final List<FailureScope> failureScopes) {
        if (failureScopes == null || failureScopes.isEmpty()) {
            return;
        }

        Set<Id> scopedJobIds = new Set<Id>();
        for (FailureScope failureScope : failureScopes) {
            if (failureScope?.jobIds != null) {
                scopedJobIds.addAll(failureScope.jobIds);
            }
        }
        if (scopedJobIds.isEmpty()) {
            return;
        }

        Map<Id, Job__c> jobsById = new Map<Id, Job__c>(jobService.queryJobsForUpdateAsSystem(scopedJobIds));
        List<Job__c> jobsToUpdate = new List<Job__c>();
        List<JobRun__c> runs = new List<JobRun__c>();
        Set<Id> processedJobIds = new Set<Id>();

        for (FailureScope failureScope : failureScopes) {
            if (failureScope?.jobIds == null || failureScope.jobIds.isEmpty()) {
                continue;
            }

            Datetime occurredAt = failureScope.occurredAt ?? QueuesSupport.now();
            for (Id jobId : failureScope.jobIds) {
                if (processedJobIds.contains(jobId)) {
                    continue;
                }

                Job__c job = jobsById.get(jobId);
                if (!isCurrentFailureOwner(job, failureScope)) {
                    continue;
                }

                Integer attemptNumber = QueuesSupport.toInteger(job.AttemptsMade__c, 0) + 1;
                job.AttemptsMade__c = attemptNumber;
                jobService.applyFailure(job, failureScope.failureMessage, occurredAt);

                jobsToUpdate.add(job);
                runs.add(mapToFailedRun(job.Id, attemptNumber, failureScope, occurredAt));
                processedJobIds.add(jobId);
            }
        }

        jobService.updateJobsAsSystem(jobsToUpdate);
        jobRunService.insertJobRunsAsSystem(runs);
        queueEventService.publishStallRecoveryJobEvents(jobsToUpdate, runs, BATCH_WORKER_FAILURE_RECOVERY_COMPONENT);
    }

    private JobRun__c mapToFailedRun(
        final Id jobId,
        final Integer attemptNumber,
        final FailureScope failureScope,
        final Datetime occurredAt
    ) {
        JobRun__c run = new JobRun__c(
            Job__c = jobId,
            AttemptNumber__c = attemptNumber,
            StartedAt__c = occurredAt,
            Status__c = Queues.RunStatus.FAILED.name(),
            Message__c = buildRunMessage(failureScope.failureMessage, failureScope.stackTrace),
            AsyncApexJobId__c = failureScope.asyncApexJobId
        );
        jobRunService.addTimestamps(run, occurredAt.getTime(), occurredAt);
        return run;
    }
    // </editor-fold>

    public class FailureScope {
        public final Set<Id> jobIds;
        public final String asyncApexJobId;
        public final Datetime occurredAt;
        public final String failureMessage;
        public final String stackTrace;

        public FailureScope(
            final Set<Id> jobIds,
            final String asyncApexJobId,
            final Datetime occurredAt,
            final String failureMessage,
            final String stackTrace
        ) {
            this.jobIds = jobIds ?? new Set<Id>();
            this.asyncApexJobId = asyncApexJobId;
            this.occurredAt = occurredAt;
            this.failureMessage = failureMessage;
            this.stackTrace = stackTrace;
        }
    }
}
