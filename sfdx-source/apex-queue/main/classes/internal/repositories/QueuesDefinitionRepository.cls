/**
 * @description Data access for QueueDefinition custom metadata.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesDefinitionRepository {
    private static final String QUEUE_DEFINITION_FIELDS =
        'QueueName__c, ' +
        'IsActive__c, ' +
        'DefaultAttempts__c, ' +
        'DefaultBackoffType__c, ' +
        'DefaultBackoffValue__c, ' +
        'DefaultBackoffJitter__c, ' +
        'DefaultLeaseMinutes__c, ' +
        'BatchClaimSize__c, ' +
        'PublishedEventTypes__c, ' +
        'TerminalCleanupPolicy__c, ' +
        'CompletedRetentionDays__c, ' +
        'FailedRetentionDays__c, ' +
        'CanceledRetentionDays__c ';
    private final AccessLevel accessLevel;

    public QueuesDefinitionRepository() {
        this(System.AccessLevel.USER_MODE);
    }

    public QueuesDefinitionRepository(final AccessLevel accessLevel) {
        this.accessLevel = accessLevel;
    }

    // <editor-fold desc="Queries">
    public List<QueueDefinition__mdt> queryQueueDefinitions(final String queueName) {
        return Database.queryWithBinds(
            'SELECT ' + QUEUE_DEFINITION_FIELDS + 'FROM QueueDefinition__mdt ' + 'WHERE QueueName__c = :queueName',
            new Map<String, Object>{ 'queueName' => queueName },
            accessLevel
        );
    }

    public List<QueueDefinition__mdt> queryQueueDefinitionsByQueueNames(final Set<String> queueNames) {
        if (queueNames == null || queueNames.isEmpty()) {
            return new List<QueueDefinition__mdt>();
        }

        return Database.queryWithBinds(
            'SELECT ' + QUEUE_DEFINITION_FIELDS + 'FROM QueueDefinition__mdt ' + 'WHERE QueueName__c IN :queueNames',
            new Map<String, Object>{ 'queueNames' => queueNames },
            accessLevel
        );
    }
    // </editor-fold>
}
