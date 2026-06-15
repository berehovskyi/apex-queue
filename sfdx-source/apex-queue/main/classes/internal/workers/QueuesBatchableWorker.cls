/**
 * @description Batchable worker that claims and executes worker-mode jobs.
 * <p><strong>Note: </strong></p>
 * <p>This is an internal class and should not be referenced outside of this package.</p>
 *
 * @group Internal
 */
@SuppressWarnings('PMD.ApexDoc')
public without sharing class QueuesBatchableWorker implements Database.Batchable<SObject>, Database.AllowsCallouts, Database.RaisesPlatformEvents {
    private final String queueName;
    private final String workerToken;
    private final QueuesWorkerService service;

    public QueuesBatchableWorker(final String queueName, final String workerToken) {
        this(queueName, workerToken, new QueuesWorkerService());
    }

    @TestVisible
    private QueuesBatchableWorker(final String queueName, final String workerToken, final QueuesWorkerService service) {
        QueuesSupport.notBlank(queueName, 'queueName');
        QueuesSupport.notBlank(workerToken, 'workerToken');
        this.queueName = queueName;
        this.workerToken = workerToken;
        this.service = service;
    }

    public Database.QueryLocator start(final Database.BatchableContext ctx) {
        return service.start(queueName, workerToken, ctx.getJobId());
    }

    public void execute(final Database.BatchableContext ctx, final List<SObject> scope) {
        service.execute(queueName, workerToken, scope, ctx.getJobId());
    }

    public void finish(final Database.BatchableContext ctx) {
        service.finish(queueName, workerToken);
    }
}
