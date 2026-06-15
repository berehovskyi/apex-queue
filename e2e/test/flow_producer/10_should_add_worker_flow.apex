String queueName = 'e2e-job-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
String shipOrderId = 'flow-producer:worker:ship-order';
String warehouseId = 'flow-producer:worker:warehouse';
String capturePaymentId = 'flow-producer:worker:capture-payment';
String reserveStockId = 'flow-producer:worker:reserve-stock';
String validateAddressId = 'flow-producer:worker:validate-address';
String notifyId = 'flow-producer:worker:notify';
String emailId = 'flow-producer:worker:email';
String smsId = 'flow-producer:worker:sms';
Set<String> externalIds = new Set<String>{
    shipOrderId,
    warehouseId,
    capturePaymentId,
    reserveStockId,
    validateAddressId,
    notifyId,
    emailId,
    smsId
};

// Ship Order
// |- Warehouse: Capture Payment + Reserve Stock + Validate Address are required.
// `- Notify: at least one notification channel must complete.
Queues.JobNode rootNode = Queues.flowProducer()
    .add(
        new Queues.FlowJob('Ship Order', queueName, processorClass)
            .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'ship-order' })
            .opts(new Queues.JobOptions().jobId(shipOrderId))
            .child(
                new Queues.FlowJob('Warehouse', queueName, processorClass)
                    .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'warehouse' })
                    .opts(new Queues.JobOptions().jobId(warehouseId))
                    .child(
                        new Queues.FlowJob('Capture Payment', queueName, processorClass)
                            .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'capture-payment' })
                            .opts(new Queues.JobOptions().jobId(capturePaymentId))
                    )
                    .child(
                        new Queues.FlowJob('Reserve Stock', queueName, processorClass)
                            .data(
                                new Map<String, Object>{
                                    'mode' => 'fail-until-attempt',
                                    'payload' => 'reserve-stock',
                                    'succeedAtAttempt' => 2,
                                    'message' => 'Stock reservation temporarily unavailable.'
                                }
                            )
                            .opts(new Queues.JobOptions().jobId(reserveStockId).attempts(2))
                    )
                    .child(
                        new Queues.FlowJob('Validate Address', queueName, processorClass)
                            .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'validate-address' })
                            .opts(new Queues.JobOptions().jobId(validateAddressId))
                    )
            )
            .child(
                new Queues.FlowJob('Notify', queueName, processorClass)
                    .data(
                        new Map<String, Object>{
                            'mode' => 'assert-any-completed-child',
                            'payload' => 'notify',
                            'message' => 'Every notification channel failed.'
                        }
                    )
                    .opts(new Queues.JobOptions().jobId(notifyId))
                    .child(
                        new Queues.FlowJob('Email', queueName, processorClass)
                            .data(new Map<String, Object>{ 'mode' => 'complete', 'payload' => 'email' })
                            .opts(new Queues.JobOptions().jobId(emailId).ignoreDependencyOnFailure())
                    )
                    .child(
                        new Queues.FlowJob('SMS', queueName, processorClass)
                            .data(
                                new Map<String, Object>{
                                    'mode' => 'fail',
                                    'payload' => 'sms',
                                    'message' => 'SMS provider unavailable.'
                                }
                            )
                            .opts(new Queues.JobOptions().jobId(smsId).attempts(3).ignoreDependencyOnFailure())
                    )
            )
    );

Assert.isTrue(rootNode.hasChildren());
Assert.areEqual(2, rootNode.getChildren().size());
Assert.areEqual(3, rootNode.getChildren()[0].getChildren().size());
Assert.areEqual(2, rootNode.getChildren()[1].getChildren().size());

Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
for (Job__c job : [
    SELECT Id, Name__c, Parent__c, ExternalId__c, State__c, ExecutionMode__c, MaxAttempts__c, DependencyFailurePolicy__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
]) {
    jobsByExternalId.put(job.ExternalId__c, job);
    Assert.areEqual(Queues.ExecutionMode.WORKER.name(), job.ExecutionMode__c);
}
Assert.areEqual(externalIds.size(), jobsByExternalId.size());

Job__c shipOrder = jobsByExternalId.get(shipOrderId);
Job__c warehouse = jobsByExternalId.get(warehouseId);
Job__c notify = jobsByExternalId.get(notifyId);
Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), shipOrder.State__c);
Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), warehouse.State__c);
Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), notify.State__c);
Assert.areEqual(null, shipOrder.Parent__c);
Assert.areEqual(shipOrder.Id, warehouse.Parent__c);
Assert.areEqual(shipOrder.Id, notify.Parent__c);

for (String externalId : new List<String>{ capturePaymentId, reserveStockId, validateAddressId }) {
    Job__c warehouseDependency = jobsByExternalId.get(externalId);
    Assert.areEqual(Queues.State.WAITING.name(), warehouseDependency.State__c);
    Assert.areEqual(warehouse.Id, warehouseDependency.Parent__c);
}
for (String externalId : new List<String>{ emailId, smsId }) {
    Job__c notifyDependency = jobsByExternalId.get(externalId);
    Assert.areEqual(Queues.State.WAITING.name(), notifyDependency.State__c);
    Assert.areEqual(notify.Id, notifyDependency.Parent__c);
}

Assert.areEqual(2, QueuesSupport.toInteger(jobsByExternalId.get(reserveStockId).MaxAttempts__c, 0));
Assert.areEqual(3, QueuesSupport.toInteger(jobsByExternalId.get(smsId).MaxAttempts__c, 0));
Assert.areEqual('IGNORE_DEPENDENCY', jobsByExternalId.get(emailId).DependencyFailurePolicy__c);
Assert.areEqual('IGNORE_DEPENDENCY', jobsByExternalId.get(smsId).DependencyFailurePolicy__c);
Assert.areEqual(0, [SELECT COUNT() FROM QueueableDispatch__c WHERE QueueName__c = :queueName]);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'flow-producer.worker.add',
            'queueName' => queueName,
            'rootJobId' => shipOrder.Id,
            'externalIds' => new List<String>(externalIds)
        }
    )
);
