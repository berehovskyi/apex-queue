/**
 * @description Data access for CronTrigger scheduled jobs.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesScheduleRepository {
    private final AccessLevel accessLevel;

    public QueuesScheduleRepository() {
        this(System.AccessLevel.USER_MODE);
    }

    public QueuesScheduleRepository(final AccessLevel accessLevel) {
        this.accessLevel = accessLevel;
    }

    // <editor-fold desc="Queries">
    public List<CronTrigger> queryScheduledJobs(final String jobName, final Set<String> terminalStates) {
        return Database.queryWithBinds(
            'SELECT ' +
                'Id, ' +
                'State, ' +
                'CronExpression, ' +
                'NextFireTime, ' +
                'CronJobDetail.Name ' +
                'FROM CronTrigger ' +
                'WHERE CronJobDetail.Name = :jobName ' +
                'AND State NOT IN :terminalStates ' +
                'ORDER BY CreatedDate DESC',
            new Map<String, Object>{ 'jobName' => jobName, 'terminalStates' => terminalStates },
            accessLevel
        );
    }

    public List<CronTrigger> queryScheduledJobsByNamePrefix(
        final String jobNamePrefix,
        final Set<String> terminalStates
    ) {
        return Database.queryWithBinds(
            'SELECT ' +
                'Id, ' +
                'State, ' +
                'CronExpression, ' +
                'NextFireTime, ' +
                'CronJobDetail.Name ' +
                'FROM CronTrigger ' +
                'WHERE CronJobDetail.Name LIKE :jobNamePrefix ' +
                'AND State NOT IN :terminalStates ' +
                'ORDER BY CreatedDate DESC',
            new Map<String, Object>{ 'jobNamePrefix' => jobNamePrefix + '%', 'terminalStates' => terminalStates },
            accessLevel
        );
    }

    public List<CronTrigger> queryScheduledJobById(final String jobId, final Set<String> terminalStates) {
        return Database.queryWithBinds(
            'SELECT ' +
                'Id, ' +
                'State, ' +
                'CronExpression, ' +
                'NextFireTime, ' +
                'CronJobDetail.Name ' +
                'FROM CronTrigger ' +
                'WHERE Id = :jobId ' +
                'AND State NOT IN :terminalStates',
            new Map<String, Object>{ 'jobId' => jobId, 'terminalStates' => terminalStates },
            accessLevel
        );
    }
    // </editor-fold>

    // <editor-fold desc="Commands">
    public void abortJob(final String jobId) {
        QueuesSupport.notBlank(jobId, 'jobId');
        System.abortJob(jobId);
    }
    // </editor-fold>
}
