/**
 * @description Resolves and validates queue and processor definitions.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesDefinitionService {
    private static final Integer DEFAULT_LEASE_MINUTES = 5;

    private static final String MISSING_QUEUE_MESSAGE = 'Queue "{0}" is not configured.';
    private static final String INACTIVE_QUEUE_MESSAGE = 'Queue "{0}" is inactive.';

    private QueuesDefinitionRepository repository = new QueuesDefinitionRepository(System.AccessLevel.SYSTEM_MODE);

    public QueuesDefinitionService() {
    }

    @TestVisible
    private QueuesDefinitionService(final QueuesDefinitionRepository repository) {
        this.repository = repository;
    }

    // <editor-fold desc="Definitions">
    public QueueDefinition__mdt requireQueueDefinition(final String queueName, final Boolean requireActive) {
        QueueDefinition__mdt definition = findQueueDefinitionAsSystem(queueName);
        QueuesSupport.isTrue(
            definition != null,
            new Queues.ConfigurationException(String.format(MISSING_QUEUE_MESSAGE, new List<String>{ queueName }))
        );
        QueuesSupport.isTrue(
            !requireActive || QueuesSupport.isActive(definition),
            new Queues.ConfigurationException(String.format(INACTIVE_QUEUE_MESSAGE, new List<String>{ queueName }))
        );
        return definition;
    }

    public QueueDefinition__mdt findQueueDefinitionAsSystem(final String queueName) {
        List<QueueDefinition__mdt> definitions = repository.queryQueueDefinitions(queueName);
        return definitions.isEmpty() ? null : definitions[0];
    }

    public Map<String, QueueDefinition__mdt> findQueueDefinitionsByQueueNamesAsSystem(final Set<String> queueNames) {
        Map<String, QueueDefinition__mdt> definitionsByQueueName = new Map<String, QueueDefinition__mdt>();
        for (QueueDefinition__mdt definition : repository.queryQueueDefinitionsByQueueNames(queueNames)) {
            definitionsByQueueName.put(definition.QueueName__c, definition);
        }
        return definitionsByQueueName;
    }

    public Integer getLeaseMinutes(final QueueDefinition__mdt queueDefinition) {
        return QueuesSupport.toPositiveInteger(queueDefinition?.DefaultLeaseMinutes__c, DEFAULT_LEASE_MINUTES);
    }
    // </editor-fold>
}
