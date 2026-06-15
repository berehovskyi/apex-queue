String runKey = 'queueable:stack-depth:inheritance';
Integer expectedMaximumDepth = 5;
Integer expectedRunCount = 3;

List<RawPerformanceRun__c> runs = [
    SELECT ItemIndex__c, Message__c, AsyncApexJobId__c
    FROM RawPerformanceRun__c
    WHERE RunKey__c = :runKey AND Variant__c = :QueuesE2EQueueableStackDepthProbe.VARIANT
    ORDER BY ItemIndex__c ASC
];

Assert.areEqual(expectedRunCount, runs.size());
for (Integer index = 0; index < runs.size(); index++) {
    RawPerformanceRun__c run = runs[index];
    Integer expectedDepth = index + 1;
    Map<String, Object> message = (Map<String, Object>) JSON.deserializeUntyped(run.Message__c);
    Assert.areEqual(expectedDepth, QueuesSupport.toInteger(run.ItemIndex__c, 0));
    Assert.areEqual(expectedDepth, QueuesSupport.toInteger(message.get('declaredDepth'), 0));
    Assert.areEqual(expectedDepth, QueuesSupport.toInteger(message.get('currentDepth'), 0));
    Assert.areEqual(expectedMaximumDepth, QueuesSupport.toInteger(message.get('maximumDepth'), 0));
    Assert.isTrue(String.isNotBlank(run.AsyncApexJobId__c));
}

delete runs;

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queueable.stack-depth.assert',
            'runKey' => runKey,
            'runCount' => runs.size(),
            'expectedMaximumDepth' => expectedMaximumDepth
        }
    )
);
