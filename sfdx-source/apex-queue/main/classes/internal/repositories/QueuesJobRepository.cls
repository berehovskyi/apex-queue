/**
 * @description Data access for Job records: queries, row locks, and DML.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesJobRepository {
    private static final String JOB_FIELDS =
        'Id, ' +
        'QueueName__c, ' +
        'Name__c, ' +
        'State__c, ' +
        'ExecutionMode__c, ' +
        'ExternalId__c, ' +
        'QueueScopedExternalId__c, ' +
        'Priority__c, ' +
        'AvailableAt__c, ' +
        'AttemptsMade__c, ' +
        'MaxAttempts__c, ' +
        'StalledCount__c, ' +
        'FailedReason__c, ' +
        'ReturnValue__c, ' +
        'Checkpoint__c, ' +
        'Progress__c, ' +
        'Data__c, ' +
        'ProcessorClass__c, ' +
        'JobScheduler__c, ' +
        'QueueableDispatch__c, ' +
        'QueueableDispatchInitiatedAt__c, ' +
        'BackoffType__c, ' +
        'BackoffValue__c, ' +
        'BackoffJitter__c, ' +
        'Parent__c, ' +
        'DependencyFailurePolicy__c, ' +
        'DependencyCanceledPolicy__c, ' +
        'PauseRequested__c, ' +
        'LeaseToken__c, ' +
        'LeaseExpiresAt__c, ' +
        'AsyncApexJobId__c, ' +
        'LastModifiedDate ';
    private static final String WORKER_EXECUTION_MODE_VALUE = Queues.ExecutionMode.WORKER.name();
    private static final String QUEUEABLE_SERIAL_EXECUTION_MODE_VALUE = Queues.ExecutionMode.QUEUEABLE_SERIAL.name();
    private static final String QUEUEABLE_CONCURRENT_EXECUTION_MODE_VALUE = Queues.ExecutionMode.QUEUEABLE_CONCURRENT.name();
    private static final String INVOCABLE_EXECUTION_MODE_VALUE = Queues.ExecutionMode.INVOCABLE.name();
    private static final String WORKER_EXECUTION_MODE_CONDITION =
        'AND ExecutionMode__c = \'' +
        WORKER_EXECUTION_MODE_VALUE +
        '\' ';
    private static final String QUEUEABLE_EXECUTION_MODE_CONDITION =
        'AND ExecutionMode__c IN (\'' +
        QUEUEABLE_SERIAL_EXECUTION_MODE_VALUE +
        '\', \'' +
        QUEUEABLE_CONCURRENT_EXECUTION_MODE_VALUE +
        '\') ';
    private static final String INVOCABLE_EXECUTION_MODE_CONDITION =
        'AND ExecutionMode__c = \'' +
        INVOCABLE_EXECUTION_MODE_VALUE +
        '\' ';
    private static final String INITIATED_QUEUEABLE_JOB_CONDITION =
        QUEUEABLE_EXECUTION_MODE_CONDITION + 'AND AsyncApexJobId__c = NULL ';
    private static final String STALE_INITIATED_QUEUEABLE_JOB_CONDITION =
        INITIATED_QUEUEABLE_JOB_CONDITION +
        'AND QueueableDispatchInitiatedAt__c < :initiatedBefore ' +
        'AND AvailableAt__c <= :availableBy ';
    private static final String FAILED_REMOVED_DEPENDENCY_CONDITION =
        'AND Parent__c != NULL ' +
        'AND State__c = :failedState ' +
        'AND DependencyFailurePolicy__c = :removeDependencyPolicy ';
    private static final String STALE_WAITING_CHILDREN_CONDITION =
        'AND ((DependencyRepairCheckedAt__c = NULL AND LastModifiedDate < :staleBefore) ' +
        'OR DependencyRepairCheckedAt__c < :staleBefore) ';

    private final AccessLevel accessLevel;

    public QueuesJobRepository() {
        this(System.AccessLevel.USER_MODE);
    }

    public QueuesJobRepository(final AccessLevel accessLevel) {
        this.accessLevel = accessLevel;
    }

    // <editor-fold desc="Queries">
    public List<Job__c> queryDueJobs(
        final String queueName,
        final Set<String> stateValues,
        final Datetime availableBy,
        final Integer jobLimit
    ) {
        if (jobLimit <= 0) {
            return new List<Job__c>();
        }

        Map<String, Object> binds = new Map<String, Object>{
            'queueName' => queueName,
            'states' => stateValues,
            'availableBy' => availableBy
        };
        String query =
            'SELECT ' +
            JOB_FIELDS +
            'FROM Job__c ' +
            'WHERE QueueName__c = :queueName ' +
            'AND State__c IN :states ' +
            WORKER_EXECUTION_MODE_CONDITION +
            'AND AvailableAt__c <= :availableBy ';

        query += 'ORDER BY Priority__c ASC, AvailableAt__c ASC, CreatedDate ASC ';
        if (jobLimit != null) {
            query += 'LIMIT :jobLimit';
            binds.put('jobLimit', jobLimit);
        }

        return Database.queryWithBinds(query, binds, accessLevel);
    }

    public List<Job__c> queryJobsByExternalIds(final String queueName, final Set<String> externalIds) {
        Set<String> queueScopedExternalIds = new Set<String>();
        for (String externalId : externalIds ?? new Set<String>()) {
            String queueScopedExternalId = QueuesSupport.buildQueueScopedExternalId(queueName, externalId);
            if (String.isNotBlank(queueScopedExternalId)) {
                queueScopedExternalIds.add(queueScopedExternalId);
            }
        }
        if (queueScopedExternalIds.isEmpty()) {
            return new List<Job__c>();
        }

        return Database.queryWithBinds(
            'SELECT ' +
                JOB_FIELDS +
                'FROM Job__c ' +
                'WHERE QueueName__c = :queueName ' +
                'AND QueueScopedExternalId__c IN :queueScopedExternalIds ' +
                'ORDER BY ExternalId__c ASC, CreatedDate DESC',
            new Map<String, Object>{ 'queueName' => queueName, 'queueScopedExternalIds' => queueScopedExternalIds },
            accessLevel
        );
    }

    public List<Job__c> queryJobsByIds(final Set<Id> jobIds) {
        if (jobIds == null || jobIds.isEmpty()) {
            return new List<Job__c>();
        }

        return Database.queryWithBinds(
            'SELECT ' + JOB_FIELDS + ' FROM Job__c WHERE Id IN :jobIds',
            new Map<String, Object>{ 'jobIds' => jobIds },
            accessLevel
        );
    }

    public List<Job__c> queryJobsByParentIds(final Set<Id> parentIds) {
        if (parentIds == null || parentIds.isEmpty()) {
            return new List<Job__c>();
        }

        return Database.queryWithBinds(
            'SELECT ' +
                JOB_FIELDS +
                'FROM Job__c ' +
                'WHERE Parent__c IN :parentIds ' +
                'ORDER BY CreatedDate ASC, Id ASC',
            new Map<String, Object>{ 'parentIds' => parentIds },
            accessLevel
        );
    }

    public List<Job__c> queryJobsByIdsForUpdate(final Set<Id> jobIds, final String stateValue) {
        if (jobIds == null || jobIds.isEmpty()) {
            return new List<Job__c>();
        }

        String stateCondition = stateValue == null ? '' : ' AND State__c = :stateValue';
        return Database.queryWithBinds(
            'SELECT ' + JOB_FIELDS + ' FROM Job__c WHERE Id IN :jobIds' + stateCondition + ' FOR UPDATE',
            new Map<String, Object>{ 'jobIds' => jobIds, 'stateValue' => stateValue },
            accessLevel
        );
    }

    public List<AggregateResult> queryChildStateCounts(final Set<Id> parentIds) {
        if (parentIds == null || parentIds.isEmpty()) {
            return new List<AggregateResult>();
        }

        return Database.queryWithBinds(
            'SELECT Parent__c parentId, State__c stateValue, DependencyFailurePolicy__c failurePolicyValue, ' +
                'DependencyCanceledPolicy__c canceledPolicyValue, COUNT(Id) rowCount ' +
                'FROM Job__c ' +
                'WHERE Parent__c IN :parentIds ' +
                'GROUP BY Parent__c, State__c, DependencyFailurePolicy__c, DependencyCanceledPolicy__c',
            new Map<String, Object>{ 'parentIds' => parentIds },
            accessLevel
        );
    }

    public List<Job__c> queryFailedRemovedDependenciesForUpdate(final Set<Id> parentIds, final Integer jobLimit) {
        if (parentIds == null || parentIds.isEmpty() || jobLimit == null || jobLimit <= 0) {
            return new List<Job__c>();
        }

        return Database.queryWithBinds(
            'SELECT Id, Parent__c ' +
                'FROM Job__c ' +
                'WHERE Parent__c IN :parentIds ' +
                FAILED_REMOVED_DEPENDENCY_CONDITION +
                'LIMIT :jobLimit ' +
                'FOR UPDATE',
            new Map<String, Object>{
                'parentIds' => parentIds,
                'failedState' => Queues.State.FAILED.name(),
                'removeDependencyPolicy' => Queues.DependencyFailurePolicy.REMOVE_DEPENDENCY.name(),
                'jobLimit' => jobLimit
            },
            accessLevel
        );
    }

    public List<AggregateResult> queryRemovedDependencyMaintenanceJobQueueNames() {
        return Database.queryWithBinds(
            'SELECT QueueName__c queueName ' +
                'FROM Job__c ' +
                'WHERE QueueName__c != NULL ' +
                FAILED_REMOVED_DEPENDENCY_CONDITION +
                'GROUP BY QueueName__c',
            failedRemovedDependencyBinds(),
            accessLevel
        );
    }

    public Boolean hasStaleRemovedDependency(final String queueName, final Datetime staleBefore) {
        if (String.isBlank(queueName) || staleBefore == null) {
            return false;
        }

        return Database.getQueryLocatorWithBinds(
                'SELECT Id ' +
                    'FROM Job__c ' +
                    'WHERE QueueName__c = :queueName ' +
                    FAILED_REMOVED_DEPENDENCY_CONDITION +
                    'AND LastModifiedDate < :staleBefore ' +
                    'LIMIT 1',
                failedRemovedDependencyBinds(queueName, staleBefore, null),
                accessLevel
            )
            .iterator()
            .hasNext();
    }

    public List<Job__c> queryStaleRemovedDependenciesForUpdate(
        final String queueName,
        final Datetime staleBefore,
        final Integer jobLimit
    ) {
        if (String.isBlank(queueName) || staleBefore == null || jobLimit == null || jobLimit <= 0) {
            return new List<Job__c>();
        }

        return Database.queryWithBinds(
            'SELECT Id, Parent__c ' +
                'FROM Job__c ' +
                'WHERE QueueName__c = :queueName ' +
                FAILED_REMOVED_DEPENDENCY_CONDITION +
                'AND LastModifiedDate < :staleBefore ' +
                'LIMIT :jobLimit ' +
                'FOR UPDATE',
            failedRemovedDependencyBinds(queueName, staleBefore, jobLimit),
            accessLevel
        );
    }

    public Boolean hasJobsBySchedulerId(final Id schedulerId, final Set<String> stateValues) {
        if (schedulerId == null || stateValues == null || stateValues.isEmpty()) {
            return false;
        }

        return Database.getQueryLocatorWithBinds(
                'SELECT Id ' +
                    'FROM Job__c ' +
                    'WHERE JobScheduler__c = :schedulerId ' +
                    'AND State__c IN :stateValues ' +
                    'LIMIT 1',
                new Map<String, Object>{ 'schedulerId' => schedulerId, 'stateValues' => stateValues },
                accessLevel
            )
            .iterator()
            .hasNext();
    }

    public List<Job__c> queryJobsBySchedulerIdForUpdate(
        final Id schedulerId,
        final Set<String> stateValues,
        final Integer jobLimit
    ) {
        if (schedulerId == null || jobLimit == null || jobLimit <= 0 || stateValues?.isEmpty() == true) {
            return new List<Job__c>();
        }

        Map<String, Object> binds = new Map<String, Object>{ 'schedulerId' => schedulerId, 'jobLimit' => jobLimit };
        String query = 'SELECT ' + JOB_FIELDS + 'FROM Job__c ' + 'WHERE JobScheduler__c = :schedulerId ';
        if (stateValues != null) {
            query += 'AND State__c IN :stateValues ';
            binds.put('stateValues', stateValues);
        }
        query += 'ORDER BY CreatedDate ASC, Id ASC LIMIT :jobLimit';
        List<Job__c> orderedJobRefs = Database.queryWithBinds(query, binds, accessLevel);
        return querySchedulerJobsByIdsForUpdateInOrder(QueuesSupport.toIds(orderedJobRefs), schedulerId, stateValues);
    }

    public List<Job__c> queryQueueableDispatchJobsForUpdate(
        final Id dispatchId,
        final Set<String> stateValues,
        final Integer jobLimit
    ) {
        if (dispatchId == null || stateValues == null || stateValues.isEmpty() || (jobLimit != null && jobLimit <= 0)) {
            return new List<Job__c>();
        }

        Map<String, Object> binds = new Map<String, Object>{ 'dispatchId' => dispatchId, 'stateValues' => stateValues };
        String query =
            'SELECT Id ' +
            'FROM Job__c ' +
            'WHERE QueueableDispatch__c = :dispatchId ' +
            'AND QueueableDispatchInitiatedAt__c = NULL ' +
            'AND State__c IN :stateValues ' +
            QUEUEABLE_EXECUTION_MODE_CONDITION;
        query += 'ORDER BY Priority__c ASC, AvailableAt__c ASC, CreatedDate ASC, Id ASC ';
        if (jobLimit != null) {
            query += 'LIMIT :jobLimit';
            binds.put('jobLimit', jobLimit);
        }

        List<Job__c> orderedJobRefs = Database.queryWithBinds(query, binds, accessLevel);
        return queryQueueableDispatchJobsByIdsForUpdateInOrder(
            QueuesSupport.toIds(orderedJobRefs),
            dispatchId,
            stateValues
        );
    }

    public Database.QueryLocator queryClaimedJobLocator(final Set<Id> jobIds) {
        if (jobIds.isEmpty()) {
            return queryEmptyJobLocator();
        }

        return Database.getQueryLocatorWithBinds(
            'SELECT ' +
                JOB_FIELDS +
                ' FROM Job__c ' +
                'WHERE Id IN :jobIds ' +
                'ORDER BY Priority__c ASC, AvailableAt__c ASC, CreatedDate ASC',
            new Map<String, Object>{ 'jobIds' => jobIds },
            accessLevel
        );
    }

    public Database.QueryLocator queryEmptyJobLocator() {
        return Database.getQueryLocatorWithBinds(
            'SELECT Id FROM Job__c LIMIT 0',
            new Map<String, Object>(),
            accessLevel
        );
    }

    public List<AggregateResult> queryNextPumpAt(
        final String queueName,
        final Set<String> stateValues,
        final Datetime availableAfter
    ) {
        return Database.queryWithBinds(
            'SELECT MIN(AvailableAt__c) nextPumpAt ' +
                'FROM Job__c ' +
                'WHERE QueueName__c = :queueName ' +
                'AND State__c IN :states ' +
                WORKER_EXECUTION_MODE_CONDITION +
                'AND AvailableAt__c > :availableAfter',
            new Map<String, Object>{
                'queueName' => queueName,
                'states' => stateValues,
                'availableAfter' => availableAfter
            },
            accessLevel
        );
    }

    public Map<String, Integer> queryQueueStateCounts(final String queueName) {
        Map<String, Integer> countsByState = new Map<String, Integer>();
        List<AggregateResult> stateCountRows = Database.queryWithBinds(
            'SELECT State__c stateValue, COUNT(Id) rowCount ' +
                'FROM Job__c ' +
                'WHERE QueueName__c = :queueName ' +
                'GROUP BY State__c',
            new Map<String, Object>{ 'queueName' => queueName },
            accessLevel
        );
        for (AggregateResult stateCountRow : stateCountRows) {
            countsByState.put(
                (String) stateCountRow.get('stateValue'),
                QueuesSupport.toInteger(stateCountRow.get('rowCount'), 0)
            );
        }
        return countsByState;
    }

    public Boolean hasJobsInState(final String queueName, final String stateValue) {
        return Database.getQueryLocatorWithBinds(
                'SELECT Id ' +
                    'FROM Job__c ' +
                    'WHERE QueueName__c = :queueName ' +
                    'AND State__c = :state ' +
                    WORKER_EXECUTION_MODE_CONDITION +
                    'LIMIT 1',
                new Map<String, Object>{ 'queueName' => queueName, 'state' => stateValue },
                accessLevel
            )
            .iterator()
            .hasNext();
    }

    public List<AggregateResult> queryQueueableMaintenanceJobQueueNames(final Set<String> stateValues) {
        if (stateValues == null || stateValues.isEmpty()) {
            return new List<AggregateResult>();
        }

        return Database.queryWithBinds(
            'SELECT QueueName__c queueName ' +
                'FROM Job__c ' +
                'WHERE State__c IN :stateValues ' +
                QUEUEABLE_EXECUTION_MODE_CONDITION +
                'GROUP BY QueueName__c',
            new Map<String, Object>{ 'stateValues' => stateValues },
            accessLevel
        );
    }

    public List<AggregateResult> queryStaleInitiatedQueueableMaintenanceJobQueueNames(final Set<String> stateValues) {
        if (stateValues == null || stateValues.isEmpty()) {
            return new List<AggregateResult>();
        }

        return Database.queryWithBinds(
            'SELECT QueueName__c queueName ' +
                'FROM Job__c ' +
                'WHERE State__c IN :stateValues ' +
                INITIATED_QUEUEABLE_JOB_CONDITION +
                'AND QueueableDispatchInitiatedAt__c != NULL ' +
                'GROUP BY QueueName__c',
            new Map<String, Object>{ 'stateValues' => stateValues },
            accessLevel
        );
    }

    public List<AggregateResult> queryInvocableMaintenanceJobQueueNames(final Set<String> stateValues) {
        if (stateValues == null || stateValues.isEmpty()) {
            return new List<AggregateResult>();
        }

        return Database.queryWithBinds(
            'SELECT QueueName__c queueName ' +
                'FROM Job__c ' +
                'WHERE State__c IN :stateValues ' +
                INVOCABLE_EXECUTION_MODE_CONDITION +
                'GROUP BY QueueName__c',
            new Map<String, Object>{ 'stateValues' => stateValues },
            accessLevel
        );
    }

    public List<AggregateResult> queryWaitingChildrenMaintenanceJobQueueNames() {
        return Database.queryWithBinds(
            'SELECT QueueName__c queueName ' +
                'FROM Job__c ' +
                'WHERE State__c = :waitingChildrenState ' +
                'AND QueueName__c != NULL ' +
                'GROUP BY QueueName__c',
            new Map<String, Object>{ 'waitingChildrenState' => Queues.State.WAITING_CHILDREN.name() },
            accessLevel
        );
    }

    public Boolean hasStaleWaitingChildrenJob(final String queueName, final Datetime staleBefore) {
        if (String.isBlank(queueName) || staleBefore == null) {
            return false;
        }

        return Database.getQueryLocatorWithBinds(
                'SELECT Id ' +
                    'FROM Job__c ' +
                    'WHERE QueueName__c = :queueName ' +
                    'AND State__c = :waitingChildrenState ' +
                    STALE_WAITING_CHILDREN_CONDITION +
                    'LIMIT 1',
                new Map<String, Object>{
                    'queueName' => queueName,
                    'waitingChildrenState' => Queues.State.WAITING_CHILDREN.name(),
                    'staleBefore' => staleBefore
                },
                accessLevel
            )
            .iterator()
            .hasNext();
    }

    public List<Job__c> queryStaleWaitingChildrenJobs(
        final String queueName,
        final Datetime staleBefore,
        final Integer jobLimit
    ) {
        if (String.isBlank(queueName) || staleBefore == null || jobLimit == null || jobLimit <= 0) {
            return new List<Job__c>();
        }

        return Database.queryWithBinds(
            'SELECT ' +
                JOB_FIELDS +
                'FROM Job__c ' +
                'WHERE QueueName__c = :queueName ' +
                'AND State__c = :waitingChildrenState ' +
                STALE_WAITING_CHILDREN_CONDITION +
                'ORDER BY DependencyRepairCheckedAt__c ASC NULLS FIRST, LastModifiedDate ASC, CreatedDate ASC, Id ASC ' +
                'LIMIT :jobLimit',
            new Map<String, Object>{
                'queueName' => queueName,
                'waitingChildrenState' => Queues.State.WAITING_CHILDREN.name(),
                'staleBefore' => staleBefore,
                'jobLimit' => jobLimit
            },
            accessLevel
        );
    }

    public Boolean hasExpiredActiveQueueableJob(
        final String queueName,
        final Set<String> stateValues,
        final Datetime expiredBefore
    ) {
        if (String.isBlank(queueName) || stateValues == null || stateValues.isEmpty() || expiredBefore == null) {
            return false;
        }

        return Database.getQueryLocatorWithBinds(
                'SELECT Id ' +
                    'FROM Job__c ' +
                    'WHERE QueueName__c = :queueName ' +
                    'AND State__c IN :stateValues ' +
                    QUEUEABLE_EXECUTION_MODE_CONDITION +
                    'AND LastModifiedDate < :expiredBefore ' +
                    'AND AvailableAt__c <= :expiredBefore ' +
                    'LIMIT 1',
                new Map<String, Object>{
                    'queueName' => queueName,
                    'stateValues' => stateValues,
                    'expiredBefore' => expiredBefore
                },
                accessLevel
            )
            .iterator()
            .hasNext();
    }

    public Boolean hasStaleInitiatedQueueableJob(
        final String queueName,
        final Set<String> stateValues,
        final Datetime initiatedBefore,
        final Datetime availableBy
    ) {
        if (
            String.isBlank(queueName) ||
            stateValues == null ||
            stateValues.isEmpty() ||
            initiatedBefore == null ||
            availableBy == null
        ) {
            return false;
        }

        return Database.getQueryLocatorWithBinds(
                'SELECT Id ' +
                    'FROM Job__c ' +
                    'WHERE QueueName__c = :queueName ' +
                    'AND State__c IN :stateValues ' +
                    STALE_INITIATED_QUEUEABLE_JOB_CONDITION +
                    'LIMIT 1',
                new Map<String, Object>{
                    'queueName' => queueName,
                    'stateValues' => stateValues,
                    'initiatedBefore' => initiatedBefore,
                    'availableBy' => availableBy
                },
                accessLevel
            )
            .iterator()
            .hasNext();
    }

    public List<Job__c> queryExpiredActiveJobsForUpdate(
        final String queueName,
        final String stateValue,
        final Datetime expiredBefore,
        final Integer jobLimit
    ) {
        if (jobLimit <= 0) {
            return new List<Job__c>();
        }

        Map<String, Object> binds = new Map<String, Object>{
            'queueName' => queueName,
            'state' => stateValue,
            'expiredBefore' => expiredBefore
        };
        String query =
            'SELECT Id ' +
            'FROM Job__c ' +
            'WHERE QueueName__c = :queueName ' +
            'AND State__c = :state ' +
            WORKER_EXECUTION_MODE_CONDITION +
            'AND LeaseExpiresAt__c < :expiredBefore ' +
            'ORDER BY LeaseExpiresAt__c ASC, CreatedDate ASC, Id ASC ';
        if (jobLimit != null) {
            query += 'LIMIT :jobLimit';
            binds.put('jobLimit', jobLimit);
        }

        List<Job__c> orderedJobRefs = Database.queryWithBinds(query, binds, accessLevel);
        return queryExpiredActiveJobsByIdsForUpdateInOrder(
            QueuesSupport.toIds(orderedJobRefs),
            queueName,
            stateValue,
            expiredBefore
        );
    }

    public List<Job__c> queryExpiredActiveQueueableJobsForUpdate(
        final String queueName,
        final String stateValue,
        final Datetime expiredBefore,
        final Integer jobLimit
    ) {
        if (jobLimit <= 0) {
            return new List<Job__c>();
        }

        Map<String, Object> binds = new Map<String, Object>{
            'queueName' => queueName,
            'state' => stateValue,
            'expiredBefore' => expiredBefore
        };
        String query =
            'SELECT Id ' +
            'FROM Job__c ' +
            'WHERE QueueName__c = :queueName ' +
            'AND State__c = :state ' +
            QUEUEABLE_EXECUTION_MODE_CONDITION +
            'AND LastModifiedDate < :expiredBefore ' +
            'AND AvailableAt__c <= :expiredBefore ' +
            'ORDER BY LastModifiedDate ASC, CreatedDate ASC, Id ASC ';
        if (jobLimit != null) {
            query += 'LIMIT :jobLimit';
            binds.put('jobLimit', jobLimit);
        }

        List<Job__c> orderedJobRefs = Database.queryWithBinds(query, binds, accessLevel);
        return queryExpiredActiveQueueableJobsByIdsForUpdateInOrder(
            QueuesSupport.toIds(orderedJobRefs),
            queueName,
            stateValue,
            expiredBefore
        );
    }

    public List<Job__c> queryStaleInitiatedQueueableJobsForUpdate(
        final String queueName,
        final Set<String> stateValues,
        final Datetime initiatedBefore,
        final Datetime availableBy,
        final Integer jobLimit
    ) {
        if (
            String.isBlank(queueName) ||
            stateValues == null ||
            stateValues.isEmpty() ||
            initiatedBefore == null ||
            availableBy == null ||
            jobLimit <= 0
        ) {
            return new List<Job__c>();
        }

        Map<String, Object> binds = new Map<String, Object>{
            'queueName' => queueName,
            'stateValues' => stateValues,
            'initiatedBefore' => initiatedBefore,
            'availableBy' => availableBy
        };
        String query =
            'SELECT Id ' +
            'FROM Job__c ' +
            'WHERE QueueName__c = :queueName ' +
            'AND State__c IN :stateValues ' +
            STALE_INITIATED_QUEUEABLE_JOB_CONDITION +
            'ORDER BY QueueableDispatchInitiatedAt__c ASC, AvailableAt__c ASC, CreatedDate ASC, Id ASC ';
        if (jobLimit != null) {
            query += 'LIMIT :jobLimit';
            binds.put('jobLimit', jobLimit);
        }

        List<Job__c> orderedJobRefs = Database.queryWithBinds(query, binds, accessLevel);
        return queryStaleInitiatedQueueableJobsByIdsForUpdateInOrder(
            QueuesSupport.toIds(orderedJobRefs),
            queueName,
            stateValues,
            initiatedBefore,
            availableBy
        );
    }

    public Boolean hasRecoverableInvocableJob(
        final String queueName,
        final String activeState,
        final Set<String> runnableStates,
        final Datetime currentNow,
        final Datetime staleBefore
    ) {
        if (
            String.isBlank(queueName) ||
            String.isBlank(activeState) ||
            runnableStates == null ||
            runnableStates.isEmpty() ||
            currentNow == null ||
            staleBefore == null
        ) {
            return false;
        }

        return Database.getQueryLocatorWithBinds(
                'SELECT Id ' +
                    'FROM Job__c ' +
                    'WHERE QueueName__c = :queueName ' +
                    INVOCABLE_EXECUTION_MODE_CONDITION +
                    'AND ((State__c = :activeState AND LeaseExpiresAt__c < :currentNow) ' +
                    'OR (State__c IN :runnableStates AND AvailableAt__c < :staleBefore)) ' +
                    'LIMIT 1',
                new Map<String, Object>{
                    'queueName' => queueName,
                    'activeState' => activeState,
                    'runnableStates' => runnableStates,
                    'currentNow' => currentNow,
                    'staleBefore' => staleBefore
                },
                accessLevel
            )
            .iterator()
            .hasNext();
    }

    public List<Job__c> queryExpiredActiveInvocableJobsForUpdate(
        final String queueName,
        final String stateValue,
        final Datetime expiredBefore,
        final Integer jobLimit
    ) {
        if (jobLimit <= 0) {
            return new List<Job__c>();
        }

        Map<String, Object> binds = new Map<String, Object>{
            'queueName' => queueName,
            'state' => stateValue,
            'expiredBefore' => expiredBefore
        };
        String query =
            'SELECT Id ' +
            'FROM Job__c ' +
            'WHERE QueueName__c = :queueName ' +
            'AND State__c = :state ' +
            INVOCABLE_EXECUTION_MODE_CONDITION +
            'AND LeaseExpiresAt__c < :expiredBefore ' +
            'ORDER BY LeaseExpiresAt__c ASC, CreatedDate ASC, Id ASC ';
        if (jobLimit != null) {
            query += 'LIMIT :jobLimit';
            binds.put('jobLimit', jobLimit);
        }

        List<Job__c> orderedJobRefs = Database.queryWithBinds(query, binds, accessLevel);
        return queryExpiredActiveInvocableJobsByIdsForUpdateInOrder(
            QueuesSupport.toIds(orderedJobRefs),
            queueName,
            stateValue,
            expiredBefore
        );
    }

    public List<Job__c> queryStaleDueInvocableJobsForUpdate(
        final String queueName,
        final Set<String> stateValues,
        final Datetime staleBefore,
        final Integer jobLimit
    ) {
        if (
            String.isBlank(queueName) ||
            stateValues == null ||
            stateValues.isEmpty() ||
            staleBefore == null ||
            jobLimit == null ||
            jobLimit <= 0
        ) {
            return new List<Job__c>();
        }

        List<Job__c> orderedJobRefs = Database.queryWithBinds(
            'SELECT Id ' +
                'FROM Job__c ' +
                'WHERE QueueName__c = :queueName ' +
                'AND State__c IN :stateValues ' +
                INVOCABLE_EXECUTION_MODE_CONDITION +
                'AND AvailableAt__c < :staleBefore ' +
                'ORDER BY AvailableAt__c ASC, CreatedDate ASC, Id ASC ' +
                'LIMIT :jobLimit',
            new Map<String, Object>{
                'queueName' => queueName,
                'stateValues' => stateValues,
                'staleBefore' => staleBefore,
                'jobLimit' => jobLimit
            },
            accessLevel
        );
        return queryStaleDueInvocableJobsByIdsForUpdateInOrder(
            QueuesSupport.toIds(orderedJobRefs),
            queueName,
            stateValues,
            staleBefore
        );
    }

    public List<Job__c> queryDrainableJobsForUpdate(
        final String queueName,
        final Set<String> stateValues,
        final Integer jobLimit
    ) {
        if (jobLimit <= 0) {
            return new List<Job__c>();
        }

        Map<String, Object> binds = new Map<String, Object>{ 'queueName' => queueName, 'states' => stateValues };
        String query =
            'SELECT Id ' +
            'FROM Job__c ' +
            'WHERE QueueName__c = :queueName ' +
            'AND State__c IN :states ' +
            'ORDER BY CreatedDate ASC, Id ASC ';
        if (jobLimit != null) {
            query += 'LIMIT :jobLimit';
            binds.put('jobLimit', jobLimit);
        }

        List<Job__c> orderedJobRefs = Database.queryWithBinds(query, binds, accessLevel);
        return queryDrainableJobsByIdsForUpdateInOrder(QueuesSupport.toIds(orderedJobRefs), queueName, stateValues);
    }

    // </editor-fold>

    // <editor-fold desc="DMLs">
    @SuppressWarnings('PMD.ApexCRUDViolation')
    public List<Job__c> insertJobs(final List<Job__c> jobs) {
        if (jobs == null || jobs.isEmpty()) {
            return new List<Job__c>();
        }

        Database.insert(jobs, accessLevel);
        return jobs;
    }

    public List<Database.SaveResult> insertJobsAllowingPartialSuccess(final List<Job__c> jobs) {
        if (jobs == null || jobs.isEmpty()) {
            return new List<Database.SaveResult>();
        }

        return Database.insert(jobs, false, accessLevel);
    }

    @SuppressWarnings('PMD.ApexCRUDViolation')
    public List<Job__c> updateJobs(final List<Job__c> jobs) {
        if (jobs == null || jobs.isEmpty()) {
            return new List<Job__c>();
        }

        Database.update(jobs, accessLevel);
        return jobs;
    }

    @SuppressWarnings('PMD.ApexCRUDViolation')
    public List<Job__c> deleteJobs(final List<Job__c> jobs) {
        if (jobs == null || jobs.isEmpty()) {
            return new List<Job__c>();
        }

        Database.delete(jobs, accessLevel);
        return jobs;
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private static Map<String, Object> failedRemovedDependencyBinds() {
        return failedRemovedDependencyBinds(null, null, null);
    }

    private static Map<String, Object> failedRemovedDependencyBinds(
        final String queueName,
        final Datetime staleBefore,
        final Integer jobLimit
    ) {
        Map<String, Object> binds = new Map<String, Object>{
            'failedState' => Queues.State.FAILED.name(),
            'removeDependencyPolicy' => Queues.DependencyFailurePolicy.REMOVE_DEPENDENCY.name()
        };
        if (queueName != null) {
            binds.put('queueName', queueName);
        }
        if (staleBefore != null) {
            binds.put('staleBefore', staleBefore);
        }
        if (jobLimit != null) {
            binds.put('jobLimit', jobLimit);
        }
        return binds;
    }

    private List<Job__c> queryExpiredActiveJobsByIdsForUpdateInOrder(
        final List<Id> orderedJobIds,
        final String queueName,
        final String stateValue,
        final Datetime expiredBefore
    ) {
        List<Job__c> lockedJobs = Database.queryWithBinds(
            'SELECT ' +
                JOB_FIELDS +
                'FROM Job__c ' +
                'WHERE Id IN :orderedJobIds ' +
                'AND QueueName__c = :queueName ' +
                'AND State__c = :stateValue ' +
                WORKER_EXECUTION_MODE_CONDITION +
                'AND LeaseExpiresAt__c < :expiredBefore ' +
                'FOR UPDATE',
            new Map<String, Object>{
                'orderedJobIds' => new Set<Id>(orderedJobIds),
                'queueName' => queueName,
                'stateValue' => stateValue,
                'expiredBefore' => expiredBefore
            },
            accessLevel
        );
        return restoreJobOrder(orderedJobIds, lockedJobs);
    }

    private List<Job__c> queryExpiredActiveQueueableJobsByIdsForUpdateInOrder(
        final List<Id> orderedJobIds,
        final String queueName,
        final String stateValue,
        final Datetime expiredBefore
    ) {
        List<Job__c> lockedJobs = Database.queryWithBinds(
            'SELECT ' +
                JOB_FIELDS +
                'FROM Job__c ' +
                'WHERE Id IN :orderedJobIds ' +
                'AND QueueName__c = :queueName ' +
                'AND State__c = :stateValue ' +
                QUEUEABLE_EXECUTION_MODE_CONDITION +
                'AND LastModifiedDate < :expiredBefore ' +
                'AND AvailableAt__c <= :expiredBefore ' +
                'FOR UPDATE',
            new Map<String, Object>{
                'orderedJobIds' => new Set<Id>(orderedJobIds),
                'queueName' => queueName,
                'stateValue' => stateValue,
                'expiredBefore' => expiredBefore
            },
            accessLevel
        );
        return restoreJobOrder(orderedJobIds, lockedJobs);
    }

    private List<Job__c> queryExpiredActiveInvocableJobsByIdsForUpdateInOrder(
        final List<Id> orderedJobIds,
        final String queueName,
        final String stateValue,
        final Datetime expiredBefore
    ) {
        List<Job__c> lockedJobs = Database.queryWithBinds(
            'SELECT ' +
                JOB_FIELDS +
                'FROM Job__c ' +
                'WHERE Id IN :orderedJobIds ' +
                'AND QueueName__c = :queueName ' +
                'AND State__c = :stateValue ' +
                INVOCABLE_EXECUTION_MODE_CONDITION +
                'AND LeaseExpiresAt__c < :expiredBefore ' +
                'FOR UPDATE',
            new Map<String, Object>{
                'orderedJobIds' => new Set<Id>(orderedJobIds),
                'queueName' => queueName,
                'stateValue' => stateValue,
                'expiredBefore' => expiredBefore
            },
            accessLevel
        );
        return restoreJobOrder(orderedJobIds, lockedJobs);
    }

    private List<Job__c> queryStaleDueInvocableJobsByIdsForUpdateInOrder(
        final List<Id> orderedJobIds,
        final String queueName,
        final Set<String> stateValues,
        final Datetime staleBefore
    ) {
        List<Job__c> lockedJobs = Database.queryWithBinds(
            'SELECT ' +
                JOB_FIELDS +
                'FROM Job__c ' +
                'WHERE Id IN :orderedJobIds ' +
                'AND QueueName__c = :queueName ' +
                'AND State__c IN :stateValues ' +
                INVOCABLE_EXECUTION_MODE_CONDITION +
                'AND AvailableAt__c < :staleBefore ' +
                'FOR UPDATE',
            new Map<String, Object>{
                'orderedJobIds' => new Set<Id>(orderedJobIds),
                'queueName' => queueName,
                'stateValues' => stateValues,
                'staleBefore' => staleBefore
            },
            accessLevel
        );
        return restoreJobOrder(orderedJobIds, lockedJobs);
    }

    private List<Job__c> queryStaleInitiatedQueueableJobsByIdsForUpdateInOrder(
        final List<Id> orderedJobIds,
        final String queueName,
        final Set<String> stateValues,
        final Datetime initiatedBefore,
        final Datetime availableBy
    ) {
        List<Job__c> lockedJobs = Database.queryWithBinds(
            'SELECT ' +
                JOB_FIELDS +
                'FROM Job__c ' +
                'WHERE Id IN :orderedJobIds ' +
                'AND QueueName__c = :queueName ' +
                'AND State__c IN :stateValues ' +
                STALE_INITIATED_QUEUEABLE_JOB_CONDITION +
                'FOR UPDATE',
            new Map<String, Object>{
                'orderedJobIds' => new Set<Id>(orderedJobIds),
                'queueName' => queueName,
                'stateValues' => stateValues,
                'initiatedBefore' => initiatedBefore,
                'availableBy' => availableBy
            },
            accessLevel
        );
        return restoreJobOrder(orderedJobIds, lockedJobs);
    }

    private List<Job__c> queryDrainableJobsByIdsForUpdateInOrder(
        final List<Id> orderedJobIds,
        final String queueName,
        final Set<String> stateValues
    ) {
        List<Job__c> lockedJobs = Database.queryWithBinds(
            'SELECT ' +
                JOB_FIELDS +
                'FROM Job__c ' +
                'WHERE Id IN :orderedJobIds ' +
                'AND QueueName__c = :queueName ' +
                'AND State__c IN :stateValues ' +
                'FOR UPDATE',
            new Map<String, Object>{
                'orderedJobIds' => new Set<Id>(orderedJobIds),
                'queueName' => queueName,
                'stateValues' => stateValues
            },
            accessLevel
        );
        return restoreJobOrder(orderedJobIds, lockedJobs);
    }

    private List<Job__c> queryQueueableDispatchJobsByIdsForUpdateInOrder(
        final List<Id> orderedJobIds,
        final Id dispatchId,
        final Set<String> stateValues
    ) {
        if (orderedJobIds == null || orderedJobIds.isEmpty()) {
            return new List<Job__c>();
        }

        Map<String, Object> binds = new Map<String, Object>{
            'orderedJobIds' => new Set<Id>(orderedJobIds),
            'dispatchId' => dispatchId,
            'stateValues' => stateValues
        };
        String query =
            'SELECT ' +
            JOB_FIELDS +
            'FROM Job__c ' +
            'WHERE Id IN :orderedJobIds ' +
            'AND QueueableDispatch__c = :dispatchId ' +
            'AND QueueableDispatchInitiatedAt__c = NULL ' +
            'AND State__c IN :stateValues ' +
            QUEUEABLE_EXECUTION_MODE_CONDITION;
        query += 'FOR UPDATE';

        List<Job__c> lockedJobs = Database.queryWithBinds(query, binds, accessLevel);
        return restoreJobOrder(orderedJobIds, lockedJobs);
    }

    private List<Job__c> querySchedulerJobsByIdsForUpdateInOrder(
        final List<Id> orderedJobIds,
        final Id schedulerId,
        final Set<String> stateValues
    ) {
        if (orderedJobIds == null || orderedJobIds.isEmpty()) {
            return new List<Job__c>();
        }

        Map<String, Object> binds = new Map<String, Object>{
            'orderedJobIds' => new Set<Id>(orderedJobIds),
            'schedulerId' => schedulerId
        };
        String query =
            'SELECT ' +
            JOB_FIELDS +
            'FROM Job__c ' +
            'WHERE Id IN :orderedJobIds ' +
            'AND JobScheduler__c = :schedulerId ';
        if (stateValues != null) {
            query += 'AND State__c IN :stateValues ';
            binds.put('stateValues', stateValues);
        }
        query += 'FOR UPDATE';

        List<Job__c> lockedJobs = Database.queryWithBinds(query, binds, accessLevel);
        return restoreJobOrder(orderedJobIds, lockedJobs);
    }

    private static List<Job__c> restoreJobOrder(final List<Id> orderedJobIds, final List<Job__c> jobs) {
        Map<Id, Job__c> jobsById = new Map<Id, Job__c>(jobs);
        List<Job__c> orderedJobs = new List<Job__c>();
        for (Id jobId : orderedJobIds) {
            if (jobsById.containsKey(jobId)) {
                orderedJobs.add(jobsById.get(jobId));
            }
        }
        return orderedJobs;
    }

    // </editor-fold>
}
