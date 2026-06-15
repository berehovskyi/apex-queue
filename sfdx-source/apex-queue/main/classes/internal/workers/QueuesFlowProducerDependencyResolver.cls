/**
 * @description Queueable that continues bounded parent dependency resolution and cleanup.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesFlowProducerDependencyResolver implements Queueable {
    @TestVisible
    private static Boolean forceSubmissionSlotAvailable;
    @TestVisible
    private static System.AsyncException forceSubmissionException;

    private final Set<Id> parentIds;
    private final Set<Id> cleanupParentIds;
    private final Set<String> suppressedWorkerQueueNames;
    private final QueuesFlowProducerService service;

    public QueuesFlowProducerDependencyResolver(
        final Set<Id> parentIds,
        final Set<Id> cleanupParentIds,
        final Set<String> suppressedWorkerQueueNames
    ) {
        this(parentIds, cleanupParentIds, suppressedWorkerQueueNames, new QueuesFlowProducerService());
    }

    @TestVisible
    private QueuesFlowProducerDependencyResolver(
        final Set<Id> parentIds,
        final Set<Id> cleanupParentIds,
        final Set<String> suppressedWorkerQueueNames,
        final QueuesFlowProducerService service
    ) {
        this.parentIds = parentIds;
        this.cleanupParentIds = cleanupParentIds;
        this.suppressedWorkerQueueNames = suppressedWorkerQueueNames ?? new Set<String>();
        this.service = service;
    }

    // <editor-fold desc="Submissions">
    public static Boolean tryEnqueue(
        final Set<Id> parentIds,
        final Set<Id> cleanupParentIds,
        final Set<String> suppressedWorkerQueueNames
    ) {
        Set<String> resolvedSuppressedWorkerQueueNames = suppressedWorkerQueueNames ?? new Set<String>();
        if (QueuesQueueableStackSupport.shouldBridge()) {
            if (!hasSubmissionSlot(Limits.getFutureCalls(), Limits.getLimitFutureCalls())) {
                return false;
            }
            try {
                throwForcedSubmissionException();
                enqueueFromFuture(
                    new List<Id>(parentIds),
                    new List<Id>(cleanupParentIds),
                    new List<String>(resolvedSuppressedWorkerQueueNames)
                );
                return true;
            } catch (System.AsyncException ex) {
                return false;
            }
        }

        if (!hasQueueableSubmissionSlot()) {
            return false;
        }
        try {
            throwForcedSubmissionException();
            System.enqueueJob(
                new QueuesFlowProducerDependencyResolver(
                    parentIds,
                    cleanupParentIds,
                    resolvedSuppressedWorkerQueueNames
                )
            );
            return true;
        } catch (System.AsyncException ex) {
            return false;
        }
    }

    @SuppressWarnings('PMD.AvoidFutureAnnotation')
    @Future
    public static void enqueueFromFuture(
        final List<Id> parentIds,
        final List<Id> cleanupParentIds,
        final List<String> suppressedWorkerQueueNames
    ) {
        System.enqueueJob(
            new QueuesFlowProducerDependencyResolver(
                new Set<Id>(parentIds),
                new Set<Id>(cleanupParentIds),
                new Set<String>(suppressedWorkerQueueNames)
            )
        );
    }
    // </editor-fold>

    // <editor-fold desc="Execution">
    @SuppressWarnings('PMD.QueueableWithoutFinalizer')
    public void execute(final QueueableContext ctx) {
        service.continueParentDependencyResolutionAsSystem(parentIds, cleanupParentIds, suppressedWorkerQueueNames);
    }
    // </editor-fold>

    // <editor-fold desc="Helpers">
    @TestVisible
    private static Boolean hasSubmissionSlot(final Integer usedSlots, final Integer slotLimit) {
        return forceSubmissionSlotAvailable ?? usedSlots < slotLimit;
    }

    private static Boolean hasQueueableSubmissionSlot() {
        return forceSubmissionSlotAvailable ?? QueuesSupport.hasQueueableSubmissionSlot();
    }

    private static void throwForcedSubmissionException() {
        if (forceSubmissionException != null) {
            throw forceSubmissionException;
        }
    }
    // </editor-fold>
}
