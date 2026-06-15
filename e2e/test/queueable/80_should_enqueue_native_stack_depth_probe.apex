String runKey = 'queueable:stack-depth:inheritance';
Integer targetDepth = 3;

List<RawPerformanceRun__c> existingRuns = [
    SELECT Id
    FROM RawPerformanceRun__c
    WHERE RunKey__c = :runKey AND Variant__c = :QueuesE2EQueueableStackDepthProbe.VARIANT
];
if (!existingRuns.isEmpty()) {
    delete existingRuns;
}

AsyncOptions options = new AsyncOptions();
options.maximumQueueableStackDepth = 5;
Id asyncApexJobId = System.enqueueJob(new QueuesE2EQueueableStackDepthProbe(runKey, 1, targetDepth), options);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queueable.stack-depth.enqueue',
            'runKey' => runKey,
            'targetDepth' => targetDepth,
            'asyncApexJobId' => asyncApexJobId
        }
    )
);
