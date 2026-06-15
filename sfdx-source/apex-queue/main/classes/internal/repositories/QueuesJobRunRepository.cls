/**
 * @description Data access for JobRun attempt records.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesJobRunRepository {
    private final AccessLevel accessLevel;

    public QueuesJobRunRepository() {
        this(System.AccessLevel.USER_MODE);
    }

    public QueuesJobRunRepository(final AccessLevel accessLevel) {
        this.accessLevel = accessLevel;
    }

    // <editor-fold desc="DMLs">
    @SuppressWarnings('PMD.ApexCRUDViolation')
    public List<JobRun__c> insertJobRuns(final List<JobRun__c> runs) {
        if (runs == null || runs.isEmpty()) {
            return new List<JobRun__c>();
        }

        Database.insert(runs, accessLevel);
        return runs;
    }

    // </editor-fold>
}
