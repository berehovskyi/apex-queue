/**
 * @description Shared queueable stack-depth probing and bridge decision.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public inherited sharing class QueuesQueueableStackSupport {
    @TestVisible
    private static Boolean forceQueueableStackContext;
    @TestVisible
    private static Integer forceCurrentQueueableStackDepth;
    @TestVisible
    private static Integer forceMaximumQueueableStackDepth;

    public static Boolean shouldBridge() {
        if (!isQueueableStackContext()) {
            return false;
        }

        Boolean hasMaxStackDepth = forceMaximumQueueableStackDepth != null || System.AsyncInfo.hasMaxStackDepth();
        Integer maximumQueueableStackDepth = forceMaximumQueueableStackDepth ??
            System.AsyncInfo.getMaximumQueueableStackDepth();
        Integer currentQueueableStackDepth = forceCurrentQueueableStackDepth ??
            System.AsyncInfo.getCurrentQueueableStackDepth();

        return hasMaxStackDepth &&
            maximumQueueableStackDepth > 0 &&
            currentQueueableStackDepth >= maximumQueueableStackDepth - 1;
    }

    public static Boolean isQueueableStackContext() {
        if (forceQueueableStackContext == true) {
            return true;
        }

        Quiddity currentQuiddity = Request.getCurrent().getQuiddity();
        return currentQuiddity == Quiddity.QUEUEABLE || currentQuiddity == Quiddity.TRANSACTION_FINALIZER_QUEUEABLE;
    }
}
