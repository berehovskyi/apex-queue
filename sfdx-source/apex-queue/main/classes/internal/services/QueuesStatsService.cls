/**
 * @description Aggregates queue statistics and job state counts.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesStatsService {
    private QueuesDefinitionService definitionService = new QueuesDefinitionService();
    private QueuesRuntimeService runtimeService = new QueuesRuntimeService();
    private QueuesJobService jobService = new QueuesJobService();

    public QueuesStatsService() {
    }

    @TestVisible
    private QueuesStatsService(
        final QueuesDefinitionService definitionService,
        final QueuesRuntimeService runtimeService,
        final QueuesJobService jobService
    ) {
        this.definitionService = definitionService;
        this.runtimeService = runtimeService;
        this.jobService = jobService;
    }

    // <editor-fold desc="Stats">
    public Queues.QueueStats getStats(final String queueName) {
        definitionService.requireQueueDefinition(queueName, false);

        QueueRuntime__c runtime = runtimeService.findVisibleRuntime(queueName);
        Map<String, Integer> stateCountsByValue = jobService.queryQueueStateCounts(queueName);
        return mapToStats(queueName, runtime, stateCountsByValue);
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    @TestVisible
    private static Queues.QueueStats mapToStats(
        final String queueName,
        final QueueRuntime__c runtime,
        final Map<String, Integer> stateCountsByValue
    ) {
        Queues.QueueStats stats = new Queues.QueueStats(queueName)
            .withRuntime(runtime?.IsPaused__c, QueuesSupport.toInteger(runtime?.ActiveWorkerCount__c, 0));

        for (String stateValue : (stateCountsByValue ?? new Map<String, Integer>()).keySet()) {
            stats.withStateCount(QueuesSupport.toState(stateValue), stateCountsByValue.get(stateValue));
        }

        return stats;
    }
    // </editor-fold>
}
