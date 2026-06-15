@IsTest
public class AsyncJobRepoTestBatchable implements Database.Batchable<SObject> {
    public Database.QueryLocator start(final Database.BatchableContext ctx) {
        return Database.getQueryLocator('SELECT Id FROM User LIMIT 1');
    }

    public void execute(final Database.BatchableContext ctx, final List<SObject> scope) {
    }

    public void finish(final Database.BatchableContext ctx) {
    }
}
