String queueName = 'e2e-job-probe';
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
Set<String> completedExternalIds = new Set<String>(externalIds);
completedExternalIds.remove(smsId);

Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
for (Job__c job : [
    SELECT
        Id,
        Parent__c,
        ExternalId__c,
        State__c,
        AttemptsMade__c,
        MaxAttempts__c,
        FailedReason__c,
        ReturnValue__c,
        Progress__c,
        ExecutionMode__c,
        DependencyFailurePolicy__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
]) {
    jobsByExternalId.put(job.ExternalId__c, job);
    Assert.areEqual(Queues.ExecutionMode.WORKER.name(), job.ExecutionMode__c);
}
Assert.areEqual(externalIds.size(), jobsByExternalId.size());

List<String> unfinishedJobs = new List<String>();
for (String externalId : completedExternalIds) {
    Job__c job = jobsByExternalId.get(externalId);
    if (QueuesSupport.toState(job.State__c) != Queues.State.COMPLETED) {
        unfinishedJobs.add(externalId + '=' + job.State__c + ' (' + job.FailedReason__c + ')');
        continue;
    }

    Map<String, Object> returnValue = (Map<String, Object>) JSON.deserializeUntyped(job.ReturnValue__c);
    Map<String, Object> progress = (Map<String, Object>) JSON.deserializeUntyped(job.Progress__c);
    Assert.areEqual(queueName + ':' + externalId, String.valueOf(returnValue.get('idempotencyKey')));
    Assert.areEqual('completed', String.valueOf(progress.get('status')));
}
Assert.areEqual(
    0,
    unfinishedJobs.size(),
    'Worker flow jobs are not finished yet or failed: ' + String.join(unfinishedJobs, ', ')
);

Job__c shipOrder = jobsByExternalId.get(shipOrderId);
Job__c warehouse = jobsByExternalId.get(warehouseId);
Job__c reserveStock = jobsByExternalId.get(reserveStockId);
Job__c notify = jobsByExternalId.get(notifyId);
Job__c sms = jobsByExternalId.get(smsId);
Assert.areEqual(null, shipOrder.Parent__c);
Assert.areEqual(shipOrder.Id, warehouse.Parent__c);
Assert.areEqual(shipOrder.Id, notify.Parent__c);
Assert.areEqual(warehouse.Id, jobsByExternalId.get(capturePaymentId).Parent__c);
Assert.areEqual(warehouse.Id, reserveStock.Parent__c);
Assert.areEqual(warehouse.Id, jobsByExternalId.get(validateAddressId).Parent__c);
Assert.areEqual(notify.Id, jobsByExternalId.get(emailId).Parent__c);
Assert.areEqual(notify.Id, sms.Parent__c);

Assert.areEqual(Queues.State.COMPLETED.name(), reserveStock.State__c);
Assert.areEqual(2, QueuesSupport.toInteger(reserveStock.AttemptsMade__c, 0));
Assert.areEqual(2, QueuesSupport.toInteger(reserveStock.MaxAttempts__c, 0));
Assert.areEqual(null, reserveStock.FailedReason__c);
Assert.areEqual(Queues.State.FAILED.name(), sms.State__c);
Assert.areEqual(3, QueuesSupport.toInteger(sms.AttemptsMade__c, 0));
Assert.areEqual(3, QueuesSupport.toInteger(sms.MaxAttempts__c, 0));
Assert.areEqual('SMS provider unavailable.', sms.FailedReason__c);
Assert.areEqual('IGNORE_DEPENDENCY', sms.DependencyFailurePolicy__c);
Assert.areEqual(Queues.State.COMPLETED.name(), notify.State__c);
Assert.areEqual(Queues.State.COMPLETED.name(), shipOrder.State__c);
Map<String, Object> notifyProgress = (Map<String, Object>) JSON.deserializeUntyped(notify.Progress__c);
Assert.areEqual(1, QueuesSupport.toInteger(notifyProgress.get('completedChildCount'), 0));

Map<String, List<JobRun__c>> runsByExternalId = new Map<String, List<JobRun__c>>();
List<JobRun__c> runs = [
    SELECT Job__r.ExternalId__c, Status__c, AttemptNumber__c, Message__c
    FROM JobRun__c
    WHERE Job__r.QueueName__c = :queueName AND Job__r.ExternalId__c IN :externalIds
    ORDER BY Job__r.ExternalId__c ASC, AttemptNumber__c ASC
];
for (JobRun__c run : runs) {
    String externalId = run.Job__r.ExternalId__c;
    if (!runsByExternalId.containsKey(externalId)) {
        runsByExternalId.put(externalId, new List<JobRun__c>());
    }
    runsByExternalId.get(externalId).add(run);
}
Assert.areEqual(11, runs.size());

for (String externalId : completedExternalIds) {
    Integer expectedRunCount = externalId == reserveStockId ? 2 : 1;
    Assert.areEqual(expectedRunCount, runsByExternalId.get(externalId).size(), externalId);
}
Assert.areEqual(Queues.RunStatus.FAILED.name(), runsByExternalId.get(reserveStockId)[0].Status__c);
Assert.areEqual('Stock reservation temporarily unavailable.', runsByExternalId.get(reserveStockId)[0].Message__c);
Assert.areEqual(Queues.RunStatus.COMPLETED.name(), runsByExternalId.get(reserveStockId)[1].Status__c);
Assert.areEqual(3, runsByExternalId.get(smsId).size());
for (JobRun__c smsRun : runsByExternalId.get(smsId)) {
    Assert.areEqual(Queues.RunStatus.FAILED.name(), smsRun.Status__c);
    Assert.areEqual('SMS provider unavailable.', smsRun.Message__c);
}

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'flow-producer.worker.assert',
            'queueName' => queueName,
            'jobCount' => jobsByExternalId.size(),
            'runCount' => runs.size()
        }
    )
);
