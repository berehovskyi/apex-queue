String queueName = 'e2e-job-probe';
String rootExternalId = 'flow-producer:anonymous-extension:root';
Job__c root = [
    SELECT Id, State__c, ExecutionMode__c
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c = :rootExternalId
    LIMIT 1
];
List<Job__c> children = [
    SELECT Id, Parent__c, ExternalId__c, State__c, ExecutionMode__c
    FROM Job__c
    WHERE Parent__c = :root.Id
];

Assert.areEqual(Queues.State.WAITING_CHILDREN.name(), root.State__c);
Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), root.ExecutionMode__c);
Assert.areEqual(1, children.size());
Assert.areEqual(root.Id, children[0].Parent__c);
Assert.areEqual(null, children[0].ExternalId__c);
Assert.areEqual(Queues.ExecutionMode.INVOCABLE.name(), children[0].ExecutionMode__c);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'flow-producer.concurrent-anonymous-child.assert',
            'rootJobId' => root.Id,
            'childJobId' => children[0].Id
        }
    )
);
