String queueName = 'e2e-job-probe';
String externalIdPattern = 'flow-producer:policy:%';

Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
for (Job__c job : [
    SELECT ExternalId__c, Parent__c, State__c, FailedReason__c, DependencyFailurePolicy__c, DependencyCanceledPolicy__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c LIKE :externalIdPattern
]) {
    jobsByExternalId.put(job.ExternalId__c, job);
}
Assert.areEqual(16, jobsByExternalId.size());

Job__c defaultRoot = jobsByExternalId.get('flow-producer:policy:default:root');
Job__c defaultChild = jobsByExternalId.get('flow-producer:policy:default:child');
Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), defaultRoot.State__c);
Assert.areEqual(Queues.State.FAILED.name(), defaultChild.State__c);
Assert.areEqual('DEFAULT', defaultChild.DependencyFailurePolicy__c);
Assert.areEqual('DEFAULT', defaultChild.DependencyCanceledPolicy__c);

Job__c failedRoot = jobsByExternalId.get('flow-producer:policy:fail:root');
Job__c failedChild = jobsByExternalId.get('flow-producer:policy:fail:child');
Assert.areEqual(Queues.State.FAILED.name(), failedRoot.State__c);
Assert.areEqual('Child job failed.', failedRoot.FailedReason__c);
Assert.areEqual(Queues.State.FAILED.name(), failedChild.State__c);
Assert.areEqual('FAIL_PARENT', failedChild.DependencyFailurePolicy__c);
Assert.areEqual('DEFAULT', failedChild.DependencyCanceledPolicy__c);

Job__c continuedRoot = jobsByExternalId.get('flow-producer:policy:continue:root');
Job__c continuedChild = jobsByExternalId.get('flow-producer:policy:continue:child');
Assert.areEqual(Queues.State.COMPLETED.name(), continuedRoot.State__c);
Assert.areEqual(Queues.State.FAILED.name(), continuedChild.State__c);
Assert.areEqual('CONTINUE_PARENT', continuedChild.DependencyFailurePolicy__c);
Assert.areEqual('DEFAULT', continuedChild.DependencyCanceledPolicy__c);

Job__c ignoredRoot = jobsByExternalId.get('flow-producer:policy:ignore:root');
Job__c ignoredChild = jobsByExternalId.get('flow-producer:policy:ignore:child');
Assert.areEqual(Queues.State.COMPLETED.name(), ignoredRoot.State__c);
Assert.areEqual(Queues.State.FAILED.name(), ignoredChild.State__c);
Assert.areEqual('IGNORE_DEPENDENCY', ignoredChild.DependencyFailurePolicy__c);
Assert.areEqual('DEFAULT', ignoredChild.DependencyCanceledPolicy__c);

Job__c removedRoot = jobsByExternalId.get('flow-producer:policy:remove:root');
Job__c removedChild = jobsByExternalId.get('flow-producer:policy:remove:child');
Assert.areEqual(Queues.State.COMPLETED.name(), removedRoot.State__c);
Assert.areEqual(Queues.State.FAILED.name(), removedChild.State__c);
Assert.areEqual(null, removedChild.Parent__c);
Assert.areEqual('REMOVE_DEPENDENCY', removedChild.DependencyFailurePolicy__c);
Assert.areEqual('DEFAULT', removedChild.DependencyCanceledPolicy__c);

Job__c canceledRoot = jobsByExternalId.get('flow-producer:policy:cancel:root');
Job__c canceledChild = jobsByExternalId.get('flow-producer:policy:cancel:child');
Assert.areEqual(Queues.State.CANCELED.name(), canceledRoot.State__c);
Assert.areEqual(Queues.State.CANCELED.name(), canceledChild.State__c);
Assert.areEqual('DEFAULT', canceledChild.DependencyFailurePolicy__c);
Assert.areEqual('CANCEL_PARENT', canceledChild.DependencyCanceledPolicy__c);

Job__c failedCanceledRoot = jobsByExternalId.get('flow-producer:policy:fail-canceled:root');
Job__c failedCanceledChild = jobsByExternalId.get('flow-producer:policy:fail-canceled:child');
Assert.areEqual(Queues.State.FAILED.name(), failedCanceledRoot.State__c);
Assert.areEqual('Child job was canceled.', failedCanceledRoot.FailedReason__c);
Assert.areEqual(Queues.State.CANCELED.name(), failedCanceledChild.State__c);
Assert.areEqual('DEFAULT', failedCanceledChild.DependencyFailurePolicy__c);
Assert.areEqual('FAIL_PARENT', failedCanceledChild.DependencyCanceledPolicy__c);

Job__c ignoredCanceledRoot = jobsByExternalId.get('flow-producer:policy:ignore-canceled:root');
Job__c ignoredCanceledChild = jobsByExternalId.get('flow-producer:policy:ignore-canceled:child');
Assert.areEqual(Queues.State.COMPLETED.name(), ignoredCanceledRoot.State__c);
Assert.areEqual(Queues.State.CANCELED.name(), ignoredCanceledChild.State__c);
Assert.areEqual('DEFAULT', ignoredCanceledChild.DependencyFailurePolicy__c);
Assert.areEqual('IGNORE_DEPENDENCY', ignoredCanceledChild.DependencyCanceledPolicy__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'flow-producer.policy.assert',
            'queueName' => queueName,
            'jobCount' => jobsByExternalId.size()
        }
    )
);
