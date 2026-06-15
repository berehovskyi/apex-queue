/**
 * @description Builds and persists JobRun attempt records.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesJobRunService {
    private QueuesJobRunRepository userModeRepository = new QueuesJobRunRepository();
    private QueuesJobRunRepository systemModeRepository = new QueuesJobRunRepository(System.AccessLevel.SYSTEM_MODE);

    public QueuesJobRunService() {
    }

    @TestVisible
    private QueuesJobRunService(final QueuesJobRunRepository repository) {
        this.userModeRepository = repository;
        this.systemModeRepository = repository;
    }

    // <editor-fold desc="Runs">
    public List<JobRun__c> insertJobRuns(final List<JobRun__c> runs) {
        return userModeRepository.insertJobRuns(runs);
    }

    public List<JobRun__c> insertJobRunsAsSystem(final List<JobRun__c> runs) {
        return systemModeRepository.insertJobRuns(runs);
    }

    public List<JobRun__c> insertJobRuns(final List<JobRun__c> runs, final Boolean asSystem) {
        return getRepository(asSystem).insertJobRuns(runs);
    }

    public void addTimestamps(final JobRun__c run, final Long startedAtMs, final Datetime finishedAt) {
        run.FinishedAt__c = finishedAt;
        Long durationMs = finishedAt.getTime() - startedAtMs;
        if (durationMs < 0) {
            durationMs = 0;
        }
        run.DurationMs__c = durationMs;
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private QueuesJobRunRepository getRepository(final Boolean asSystem) {
        return asSystem == true ? systemModeRepository : userModeRepository;
    }
    // </editor-fold>
}
