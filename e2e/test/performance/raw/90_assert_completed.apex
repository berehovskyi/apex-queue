String runKey = QueuesE2ERawPerformanceProbe.DEFAULT_RUN_KEY;
Integer expectedCount = QueuesE2ERawPerformanceProbe.DEFAULT_JOB_COUNT;
Set<String> expectedVariants = new Set<String>{
    QueuesE2ERawPerformanceProbe.VARIANT_BATCH_ITERATOR,
    QueuesE2ERawPerformanceProbe.VARIANT_BATCH_CURSOR,
    QueuesE2ERawPerformanceProbe.VARIANT_BATCH_SERIAL,
    QueuesE2ERawPerformanceProbe.VARIANT_QUEUEABLE_SERIAL,
    QueuesE2ERawPerformanceProbe.VARIANT_QUEUEABLE_FUTURE,
    QueuesE2ERawPerformanceProbe.VARIANT_INVOCABLE
};

List<String> unexpectedRows = new List<String>();
for (AggregateResult result : [
    SELECT Variant__c variant, Status__c status, COUNT(Id) rowCount
    FROM RawPerformanceRun__c
    WHERE RunKey__c = :runKey AND Status__c != :QueuesE2ERawPerformanceProbe.STATUS_COMPLETED
    GROUP BY Variant__c, Status__c
    ORDER BY Variant__c, Status__c
]) {
    unexpectedRows.add(
        result.get('variant')?.toString() +
            ':' +
            result.get('status')?.toString() +
            '=' +
            result.get('rowCount')?.toString()
    );
}

Assert.areEqual('', String.join(unexpectedRows, ', '), 'Raw performance rows are not fully completed.');

Map<String, Integer> completedByVariant = new Map<String, Integer>();
for (AggregateResult result : [
    SELECT Variant__c variant, COUNT(Id) rowCount
    FROM RawPerformanceRun__c
    WHERE RunKey__c = :runKey AND Status__c = :QueuesE2ERawPerformanceProbe.STATUS_COMPLETED
    GROUP BY Variant__c
]) {
    completedByVariant.put(result.get('variant')?.toString(), Integer.valueOf(result.get('rowCount')?.toString()));
}

for (String variant : expectedVariants) {
    Assert.areEqual(expectedCount, completedByVariant.get(variant), 'Unexpected completed count for ' + variant);
}

System.debug(
    LoggingLevel.ERROR,
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'performance.raw.assert',
            'runKey' => runKey,
            'completedByVariant' => completedByVariant
        }
    )
);
