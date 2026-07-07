/**
 * @description Claims and executes invocable-mode jobs for the queue Flows.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesInvocableExecutionService {
    private static final String STALLED_JOB_MESSAGE = 'Lease expired before invocable action completed.';
    private static final String STALLED_JOB_EXHAUSTED_MESSAGE = 'Lease expired before invocable action completed and no more recovery attempts remain.';
    private static final String INVOCABLE_STALLED_JOB_RECOVERY_COMPONENT = 'invocable stalled job recovery';
    private static final String INVOCABLE_LOST_WAKE_RECOVERY_COMPONENT = 'invocable lost wake recovery';
    private static final Integer LOST_WAKE_REPAIR_DELAY_MINUTES = 1;

    private QueuesJobService jobService = new QueuesJobService();
    private QueuesJobExecutionService jobExecutionService = new QueuesJobExecutionService();
    private QueuesJobRunService jobRunService = new QueuesJobRunService();
    private QueuesStalledJobRecoveryService stalledJobRecoveryService = new QueuesStalledJobRecoveryService();
    private QueuesQueueEventService queueEventService = new QueuesQueueEventService();

    public QueuesInvocableExecutionService() {
    }

    @TestVisible
    private QueuesInvocableExecutionService(
        final QueuesJobService jobService,
        final QueuesJobExecutionService jobExecutionService,
        final QueuesJobRunService jobRunService,
        final QueuesStalledJobRecoveryService stalledJobRecoveryService,
        final QueuesQueueEventService queueEventService
    ) {
        this.jobService = jobService;
        this.jobExecutionService = jobExecutionService;
        this.jobRunService = jobRunService;
        this.stalledJobRecoveryService = stalledJobRecoveryService;
        this.queueEventService = queueEventService;
    }

    public List<Job__c> claimDueJobsAsSystem(final Set<Id> jobIds) {
        List<Job__c> claimedJobs = jobService.claimInvocableJobsAsSystem(jobIds, QueuesSupport.now());
        queueEventService.publishActiveJobEvents(claimedJobs);
        return claimedJobs;
    }

    public Job__c executeJobAsSystem(final Id jobId) {
        QueuesSupport.notNull(jobId, 'jobId');
        Job__c job = jobService.requireJobAsSystem(jobId);
        String leaseToken = job.LeaseToken__c;
        if (String.isBlank(leaseToken)) {
            return job;
        }
        try {
            return jobExecutionService.executeInvocableJob(jobId, leaseToken);
        } catch (Exception ex) {
            return jobExecutionService.failInvocableJob(jobId, leaseToken, QueuesSupport.resolveExceptionReason(ex));
        }
    }

    public Integer recoverAsSystem(final String queueName, final Datetime staleBefore) {
        QueuesSupport.notBlank(queueName, 'queueName');
        QueuesSupport.notNull(staleBefore, 'staleBefore');
        return recoverStalledJobsAsSystem(queueName) + repairLostWakesAsSystem(queueName, staleBefore);
    }

    private Integer recoverStalledJobsAsSystem(final String queueName) {
        Datetime currentNow = QueuesSupport.now();
        Integer recoveredCount = 0;

        while (stalledJobRecoveryService.hasHeadroomForRecovery()) {
            Integer recoverBatchSize = stalledJobRecoveryService.resolveRecoverStalledBatchSize();
            if (recoverBatchSize <= 0) {
                break;
            }

            List<Job__c> stalledJobs = jobService.queryExpiredActiveInvocableJobsForUpdateAsSystem(
                queueName,
                currentNow,
                recoverBatchSize
            );
            if (stalledJobs.isEmpty()) {
                break;
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
            jobService.updateJobsAsSystem(stalledJobs);
            jobRunService.insertJobRunsAsSystem(runs);
            queueEventService.publishStallRecoveryJobEvents(
                stalledJobs,
                runs,
                INVOCABLE_STALLED_JOB_RECOVERY_COMPONENT
            );
            recoveredCount += stalledJobs.size();
            if (stalledJobs.size() < recoverBatchSize) {
                break;
            }
        }

        return recoveredCount;
    }

    private Integer repairLostWakesAsSystem(final String queueName, final Datetime staleBefore) {
        Datetime repairWakeAt = QueuesSupport.now().addMinutes(LOST_WAKE_REPAIR_DELAY_MINUTES);
        Integer repairedCount = 0;

        while (stalledJobRecoveryService.hasHeadroomForRecovery()) {
            Integer repairBatchSize = stalledJobRecoveryService.resolveRecoverStalledBatchSize();
            if (repairBatchSize <= 0) {
                break;
            }

            List<Job__c> staleJobs = jobService.queryStaleDueInvocableJobsForUpdateAsSystem(
                queueName,
                staleBefore,
                repairBatchSize
            );
            if (staleJobs.isEmpty()) {
                break;
            }

            List<Job__c> wakeUpdates = new List<Job__c>();
            for (Job__c staleJob : staleJobs) {
                wakeUpdates.add(new Job__c(Id = staleJob.Id, AvailableAt__c = repairWakeAt));
            }
            jobService.updateJobsAsSystem(wakeUpdates);
            queueEventService.publishRecoveredJobEvents(staleJobs, null, INVOCABLE_LOST_WAKE_RECOVERY_COMPONENT);
            repairedCount += wakeUpdates.size();
            if (staleJobs.size() < repairBatchSize) {
                break;
            }
        }

        return repairedCount;
    }
}
