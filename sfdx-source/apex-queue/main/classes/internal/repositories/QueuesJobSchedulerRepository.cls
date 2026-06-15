/**
 * @description Data access for JobScheduler records.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public virtual inherited sharing class QueuesJobSchedulerRepository {
    private static final String JOB_SCHEDULER_FIELDS =
        'Id, ' +
        'Name, ' +
        'QueueName__c, ' +
        'ExternalId__c, ' +
        'IsActive__c, ' +
        'JobName__c, ' +
        'ProcessorClass__c, ' +
        'Data__c, ' +
        'Options__c, ' +
        'ExecutionMode__c, ' +
        'Pattern__c, ' +
        'EveryMinutes__c, ' +
        'Limit__c, ' +
        'StartAt__c, ' +
        'EndAt__c, ' +
        'NextRunAt__c, ' +
        'LastEnqueuedAt__c, ' +
        'ScheduledJobId__c, ' +
        'IterationCount__c ';

    private final AccessLevel accessLevel;

    public QueuesJobSchedulerRepository() {
        this(System.AccessLevel.USER_MODE);
    }

    public QueuesJobSchedulerRepository(final AccessLevel accessLevel) {
        this.accessLevel = accessLevel;
    }

    // <editor-fold desc="Queries">
    public virtual List<JobScheduler__c> queryJobSchedulersByExternalId(final String externalId) {
        if (String.isBlank(externalId)) {
            return new List<JobScheduler__c>();
        }

        return Database.queryWithBinds(
            'SELECT ' + JOB_SCHEDULER_FIELDS + 'FROM JobScheduler__c WHERE ExternalId__c = :externalId',
            new Map<String, Object>{ 'externalId' => externalId },
            accessLevel
        );
    }

    public List<JobScheduler__c> queryJobSchedulersById(final Id jobSchedulerId) {
        if (jobSchedulerId == null) {
            return new List<JobScheduler__c>();
        }

        return Database.queryWithBinds(
            'SELECT ' + JOB_SCHEDULER_FIELDS + 'FROM JobScheduler__c WHERE Id = :jobSchedulerId',
            new Map<String, Object>{ 'jobSchedulerId' => jobSchedulerId },
            accessLevel
        );
    }

    public List<JobScheduler__c> queryJobSchedulersByIdForUpdate(final Id jobSchedulerId) {
        if (jobSchedulerId == null) {
            return new List<JobScheduler__c>();
        }

        return Database.queryWithBinds(
            'SELECT ' + JOB_SCHEDULER_FIELDS + 'FROM JobScheduler__c WHERE Id = :jobSchedulerId FOR UPDATE',
            new Map<String, Object>{ 'jobSchedulerId' => jobSchedulerId },
            accessLevel
        );
    }

    public List<JobScheduler__c> queryJobSchedulers(
        final String queueName,
        final Integer limitSize,
        final Integer offsetRows,
        final Boolean ascending
    ) {
        if (String.isBlank(queueName) || limitSize == null || limitSize <= 0) {
            return new List<JobScheduler__c>();
        }

        Integer safeOffsetRows = Math.max(offsetRows ?? 0, 0);
        String orderDirection = (ascending ?? true) ? 'ASC' : 'DESC';
        return Database.queryWithBinds(
            'SELECT ' +
                JOB_SCHEDULER_FIELDS +
                'FROM JobScheduler__c ' +
                'WHERE QueueName__c = :queueName ' +
                'ORDER BY Name ' +
                orderDirection +
                ' LIMIT :limitSize OFFSET :safeOffsetRows',
            new Map<String, Object>{
                'queueName' => queueName,
                'limitSize' => limitSize,
                'safeOffsetRows' => safeOffsetRows
            },
            accessLevel
        );
    }

    public Integer queryJobSchedulersCount(final String queueName) {
        if (String.isBlank(queueName)) {
            return 0;
        }

        List<AggregateResult> rows = Database.queryWithBinds(
            'SELECT COUNT(Id) rowCount FROM JobScheduler__c WHERE QueueName__c = :queueName',
            new Map<String, Object>{ 'queueName' => queueName },
            accessLevel
        );
        return rows.isEmpty() ? 0 : QueuesSupport.toInteger(rows[0].get('rowCount'), 0);
    }

    public List<JobScheduler__c> queryActiveJobSchedulers() {
        return Database.queryWithBinds(
            'SELECT ' +
                JOB_SCHEDULER_FIELDS +
                'FROM JobScheduler__c ' +
                'WHERE IsActive__c = TRUE ' +
                'ORDER BY QueueName__c ASC, Name ASC',
            new Map<String, Object>(),
            accessLevel
        );
    }
    // </editor-fold>

    // <editor-fold desc="DMLs">
    @SuppressWarnings('PMD.ApexCRUDViolation')
    public virtual JobScheduler__c insertJobScheduler(final JobScheduler__c jobScheduler) {
        Database.insert(jobScheduler, accessLevel);
        return jobScheduler;
    }

    @SuppressWarnings('PMD.ApexCRUDViolation')
    public JobScheduler__c updateJobScheduler(final JobScheduler__c jobScheduler) {
        Database.update(jobScheduler, accessLevel);
        return jobScheduler;
    }

    @SuppressWarnings('PMD.ApexCRUDViolation')
    public JobScheduler__c deleteJobScheduler(final JobScheduler__c jobScheduler) {
        Database.delete(jobScheduler, accessLevel);
        return jobScheduler;
    }
    // </editor-fold>
}
