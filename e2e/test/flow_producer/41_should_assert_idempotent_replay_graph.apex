String queueName = 'e2e-job-probe';
Set<String> externalIds = new Set<String>{ 'flow-producer:replay:root', 'flow-producer:replay:child' };
List<Job__c> jobs = [
    SELECT Id, Parent__c, ExternalId__c, State__c, ExecutionMode__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :externalIds
];

Assert.areEqual(2, jobs.size());
Map<String, Job__c> jobsByExternalId = new Map<String, Job__c>();
for (Job__c job : jobs) {
    jobsByExternalId.put(job.ExternalId__c, job);
    Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), job.ExecutionMode__c);
}
Job__c root = jobsByExternalId.get('flow-producer:replay:root');
Job__c child = jobsByExternalId.get('flow-producer:replay:child');
Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), root.State__c);
Assert.areEqual(root.Id, child.Parent__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'flow-producer.concurrent-replay.assert',
            'rootJobId' => root.Id,
            'childJobId' => child.Id
        }
    )
);
