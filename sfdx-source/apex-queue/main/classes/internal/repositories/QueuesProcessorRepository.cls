/**
 * @description Data access for Processor custom metadata.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesProcessorRepository {
    private final AccessLevel accessLevel;

    public QueuesProcessorRepository() {
        this(System.AccessLevel.USER_MODE);
    }

    public QueuesProcessorRepository(final AccessLevel accessLevel) {
        this.accessLevel = accessLevel;
    }

    // <editor-fold desc="Queries">
    public List<Processor__mdt> queryProcessorsByClassNames(final Set<String> processorClassNames) {
        if (processorClassNames == null || processorClassNames.isEmpty()) {
            return new List<Processor__mdt>();
        }

        return Database.queryWithBinds(
            'SELECT ' +
                'IsActive__c, ' +
                'Name__c, ' +
                'ProcessorClass__c ' +
                'FROM Processor__mdt ' +
                'WHERE ProcessorClass__c IN :processorClassNames',
            new Map<String, Object>{ 'processorClassNames' => processorClassNames },
            accessLevel
        );
    }
    // </editor-fold>
}
