@IsTest
public class QueuesBatchApexErrorEventTestBatch implements Database.Batchable<Integer>, Database.RaisesPlatformEvents {
    public Iterable<Integer> start(final Database.BatchableContext context) {
        return new List<Integer>{ 1 };
    }

    public void execute(final Database.BatchableContext context, final List<Integer> scope) {
        throw new IntentionalBatchFailureException();
    }

    public void finish(final Database.BatchableContext context) {
    }

    public class IntentionalBatchFailureException extends Exception {
    }
}
