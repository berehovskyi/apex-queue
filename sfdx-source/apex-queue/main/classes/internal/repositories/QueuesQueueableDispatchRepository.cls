/**
 * @description Data access for QueueableDispatch records.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesQueueableDispatchRepository {
    private static final String DISPATCH_FIELDS =
        'Id, QueueName__c, Mode__c, State__c, RequestedCount__c, DispatchedCount__c, DispatchDepth__c, ' +
        'DispatcherAsyncApexJobId__c, DedupeKey__c, StartedAt__c, FinishedAt__c, FailedReason__c ';
    private static final String STALE_DISPATCH_AGE_CONDITION =
        'AND ((State__c = :waitingState AND CreatedDate < :staleBefore) ' +
        'OR (State__c = :activeState AND StartedAt__c < :staleBefore)) ';

    private final AccessLevel accessLevel;

    public QueuesQueueableDispatchRepository() {
        this(System.AccessLevel.USER_MODE);
    }

    public QueuesQueueableDispatchRepository(final AccessLevel accessLevel) {
        this.accessLevel = accessLevel;
    }

    // <editor-fold desc="Queries">
    public List<AggregateResult> queryStaleDispatchMaintenanceQueueNames(final Set<String> stateValues) {
        if (stateValues == null || stateValues.isEmpty()) {
            return new List<AggregateResult>();
        }

        return Database.queryWithBinds(
            'SELECT QueueName__c queueName ' +
                'FROM QueueableDispatch__c ' +
                'WHERE State__c IN :stateValues ' +
                'AND QueueName__c != NULL ' +
                'AND DispatcherAsyncApexJobId__c != NULL ' +
                'GROUP BY QueueName__c',
            new Map<String, Object>{ 'stateValues' => stateValues },
            accessLevel
        );
    }

    public List<AggregateResult> queryUnsubmittedWaitingDispatchMaintenanceQueueNames() {
        return Database.queryWithBinds(
            'SELECT QueueName__c queueName ' +
                'FROM QueueableDispatch__c ' +
                'WHERE State__c = :waitingState ' +
                'AND QueueName__c != NULL ' +
                'AND DispatcherAsyncApexJobId__c = NULL ' +
                'GROUP BY QueueName__c',
            new Map<String, Object>{ 'waitingState' => Queues.QueueableDispatchState.WAITING.name() },
            accessLevel
        );
    }

    public Boolean hasStaleDispatch(final String queueName, final Set<String> stateValues, final Datetime staleBefore) {
        if (String.isBlank(queueName) || stateValues == null || stateValues.isEmpty() || staleBefore == null) {
            return false;
        }

        return hasSubmittedStaleDispatch(queueName, stateValues, staleBefore) ||
            hasUnsubmittedStaleWaitingDispatch(queueName, stateValues, staleBefore);
    }

    public List<QueueableDispatch__c> queryStaleDispatchesForUpdate(
        final String queueName,
        final Set<String> stateValues,
        final Datetime staleBefore,
        final Integer dispatchLimit
    ) {
        if (
            String.isBlank(queueName) ||
            stateValues == null ||
            stateValues.isEmpty() ||
            staleBefore == null ||
            dispatchLimit == null ||
            dispatchLimit <= 0
        ) {
            return new List<QueueableDispatch__c>();
        }

        List<QueueableDispatch__c> staleDispatches = new List<QueueableDispatch__c>();
        Set<Id> staleDispatchIds = new Set<Id>();
        addDispatches(
            staleDispatches,
            staleDispatchIds,
            querySubmittedStaleDispatchesForUpdate(queueName, stateValues, staleBefore, dispatchLimit)
        );
        Integer remainingDispatchLimit = dispatchLimit - staleDispatches.size();
        if (remainingDispatchLimit <= 0) {
            return staleDispatches;
        }

        addDispatches(
            staleDispatches,
            staleDispatchIds,
            queryUnsubmittedStaleWaitingDispatchesForUpdate(queueName, stateValues, staleBefore, remainingDispatchLimit)
        );
        return staleDispatches;
    }

    public QueueableDispatch__c queryDispatchByIdForUpdate(final Id dispatchId) {
        if (dispatchId == null) {
            return null;
        }

        List<QueueableDispatch__c> dispatches = Database.queryWithBinds(
            'SELECT ' + DISPATCH_FIELDS + 'FROM QueueableDispatch__c ' + 'WHERE Id = :dispatchId ' + 'FOR UPDATE',
            new Map<String, Object>{ 'dispatchId' => dispatchId },
            accessLevel
        );
        return dispatches.isEmpty() ? null : dispatches[0];
    }
    // </editor-fold>

    // <editor-fold desc="DMLs">
    @SuppressWarnings('PMD.ApexCRUDViolation')
    public QueueableDispatch__c insertDispatch(final QueueableDispatch__c dispatch) {
        Database.insert(dispatch, accessLevel);
        return dispatch;
    }

    @SuppressWarnings('PMD.ApexCRUDViolation')
    public QueueableDispatch__c updateDispatch(final QueueableDispatch__c dispatch) {
        if (dispatch == null) {
            return null;
        }

        Database.update(dispatch, accessLevel);
        return dispatch;
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    private static void addDispatches(
        final List<QueueableDispatch__c> dispatches,
        final Set<Id> dispatchIds,
        final List<QueueableDispatch__c> candidates
    ) {
        for (QueueableDispatch__c candidate : candidates) {
            if (!dispatchIds.contains(candidate.Id)) {
                dispatches.add(candidate);
                dispatchIds.add(candidate.Id);
            }
        }
    }

    private static Map<String, Object> staleDispatchBinds(
        final String queueName,
        final Set<String> stateValues,
        final Datetime staleBefore,
        final Integer dispatchLimit
    ) {
        Map<String, Object> binds = new Map<String, Object>{
            'queueName' => queueName,
            'stateValues' => stateValues,
            'staleBefore' => staleBefore,
            'waitingState' => Queues.QueueableDispatchState.WAITING.name(),
            'activeState' => Queues.QueueableDispatchState.ACTIVE.name()
        };
        if (dispatchLimit != null) {
            binds.put('dispatchLimit', dispatchLimit);
        }
        return binds;
    }

    private Boolean hasSubmittedStaleDispatch(
        final String queueName,
        final Set<String> stateValues,
        final Datetime staleBefore
    ) {
        return Database.getQueryLocatorWithBinds(
                'SELECT Id ' +
                    'FROM QueueableDispatch__c ' +
                    'WHERE QueueName__c = :queueName ' +
                    'AND State__c IN :stateValues ' +
                    'AND DispatcherAsyncApexJobId__c != NULL ' +
                    STALE_DISPATCH_AGE_CONDITION +
                    'LIMIT 1',
                staleDispatchBinds(queueName, stateValues, staleBefore, null),
                accessLevel
            )
            .iterator()
            .hasNext();
    }

    private Boolean hasUnsubmittedStaleWaitingDispatch(
        final String queueName,
        final Set<String> stateValues,
        final Datetime staleBefore
    ) {
        if (!stateValues.contains(Queues.QueueableDispatchState.WAITING.name())) {
            return false;
        }

        return Database.getQueryLocatorWithBinds(
                'SELECT Id ' +
                    'FROM QueueableDispatch__c ' +
                    'WHERE QueueName__c = :queueName ' +
                    'AND State__c = :waitingState ' +
                    'AND DispatcherAsyncApexJobId__c = NULL ' +
                    'AND CreatedDate < :staleBefore ' +
                    'LIMIT 1',
                staleDispatchBinds(queueName, stateValues, staleBefore, null),
                accessLevel
            )
            .iterator()
            .hasNext();
    }

    private List<QueueableDispatch__c> querySubmittedStaleDispatchesForUpdate(
        final String queueName,
        final Set<String> stateValues,
        final Datetime staleBefore,
        final Integer dispatchLimit
    ) {
        return Database.queryWithBinds(
            'SELECT ' +
                DISPATCH_FIELDS +
                'FROM QueueableDispatch__c ' +
                'WHERE QueueName__c = :queueName ' +
                'AND State__c IN :stateValues ' +
                'AND DispatcherAsyncApexJobId__c != NULL ' +
                STALE_DISPATCH_AGE_CONDITION +
                'LIMIT :dispatchLimit ' +
                'FOR UPDATE',
            staleDispatchBinds(queueName, stateValues, staleBefore, dispatchLimit),
            accessLevel
        );
    }

    private List<QueueableDispatch__c> queryUnsubmittedStaleWaitingDispatchesForUpdate(
        final String queueName,
        final Set<String> stateValues,
        final Datetime staleBefore,
        final Integer dispatchLimit
    ) {
        if (!stateValues.contains(Queues.QueueableDispatchState.WAITING.name())) {
            return new List<QueueableDispatch__c>();
        }

        return Database.queryWithBinds(
            'SELECT ' +
                DISPATCH_FIELDS +
                'FROM QueueableDispatch__c ' +
                'WHERE QueueName__c = :queueName ' +
                'AND State__c = :waitingState ' +
                'AND DispatcherAsyncApexJobId__c = NULL ' +
                'AND CreatedDate < :staleBefore ' +
                'LIMIT :dispatchLimit ' +
                'FOR UPDATE',
            staleDispatchBinds(queueName, stateValues, staleBefore, dispatchLimit),
            accessLevel
        );
    }
    // </editor-fold>
}
