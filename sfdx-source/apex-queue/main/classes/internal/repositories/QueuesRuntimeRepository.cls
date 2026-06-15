/**
 * @description Data access for QueueRuntime worker-state records.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesRuntimeRepository {
    private static final String QUEUE_RUNTIME_FIELDS =
        'Id, ' +
        'QueueName__c, ' +
        'IsPaused__c, ' +
        'ActiveWorkerCount__c, ' +
        'LastPumpAt__c, ' +
        'NextPumpAt__c, ' +
        'AsyncApexJobId__c, ' +
        'WorkerToken__c, ' +
        'LastSweepAt__c ';

    private final AccessLevel accessLevel;

    public QueuesRuntimeRepository() {
        this(System.AccessLevel.USER_MODE);
    }

    public QueuesRuntimeRepository(final AccessLevel accessLevel) {
        this.accessLevel = accessLevel;
    }

    // <editor-fold desc="Queries">
    public List<QueueRuntime__c> queryRuntimes(final String queueName) {
        return Database.queryWithBinds(
            'SELECT ' + QUEUE_RUNTIME_FIELDS + 'FROM QueueRuntime__c ' + 'WHERE QueueName__c = :queueName',
            new Map<String, Object>{ 'queueName' => queueName },
            accessLevel
        );
    }

    public List<QueueRuntime__c> queryRuntimesForUpdate(final String queueName) {
        return Database.queryWithBinds(
            'SELECT ' + QUEUE_RUNTIME_FIELDS + 'FROM QueueRuntime__c WHERE QueueName__c = :queueName FOR UPDATE',
            new Map<String, Object>{ 'queueName' => queueName },
            accessLevel
        );
    }

    public List<QueueRuntime__c> queryRuntimesWithWorkerJobs() {
        return Database.queryWithBinds(
            'SELECT ' + QUEUE_RUNTIME_FIELDS + 'FROM QueueRuntime__c WHERE AsyncApexJobId__c != NULL',
            new Map<String, Object>(),
            accessLevel
        );
    }

    public List<QueueRuntime__c> queryMaintenanceRuntimes(final Integer minimumActiveWorkerCount) {
        return Database.queryWithBinds(
            'SELECT QueueName__c ' +
                'FROM QueueRuntime__c ' +
                'WHERE WorkerToken__c != NULL ' +
                'OR ActiveWorkerCount__c > :minimumActiveWorkerCount',
            new Map<String, Object>{ 'minimumActiveWorkerCount' => minimumActiveWorkerCount },
            accessLevel
        );
    }
    // </editor-fold>

    // <editor-fold desc="DMLs">
    @SuppressWarnings('PMD.ApexCRUDViolation')
    public QueueRuntime__c insertRuntime(final QueueRuntime__c runtime) {
        Database.insert(runtime, accessLevel);
        return runtime;
    }

    @SuppressWarnings('PMD.ApexCRUDViolation')
    public QueueRuntime__c updateRuntime(final QueueRuntime__c runtime) {
        Database.update(runtime, accessLevel);
        return runtime;
    }
    // </editor-fold>
}
