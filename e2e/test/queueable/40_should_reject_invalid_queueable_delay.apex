String queueName = 'e2e-queueable-probe';
String processorName = 'queueable-probe';
String processorClass = 'QueuesE2EJobProbeProcessor';
List<String> failureMessages = new List<String>();
Set<String> invalidExternalIds = new Set<String>{ 'queueable:invalid-delay', 'queueable:invalid-available-at' };

try {
    Queues.of(queueName)
        .add(
            processorName,
            processorClass,
            new Map<String, Object>{ 'mode' => 'complete' },
            new Queues.JobOptions().jobId('queueable:invalid-delay').delay(11),
            new Queues.BulkJobOptions().queueable()
        );
    Assert.fail('Expected invalid queueable delay to throw.');
} catch (Queues.QueueException ex) {
    failureMessages.add(ex.getMessage());
}

try {
    Queues.of(queueName)
        .add(
            processorName,
            processorClass,
            new Map<String, Object>{ 'mode' => 'complete' },
            new Queues.JobOptions().jobId('queueable:invalid-available-at').availableAt(System.now().addMinutes(11)),
            new Queues.BulkJobOptions().queueable()
        );
    Assert.fail('Expected invalid queueable availableAt to throw.');
} catch (Queues.QueueException ex) {
    failureMessages.add(ex.getMessage());
}

Assert.areEqual(2, failureMessages.size());
String expectedFailureMessage = String.format(
    QueuesQueueableTransportService.NATIVE_INVALID_QUEUEABLE_DELAY_MESSAGE,
    new List<String>{ '11' }
);
Assert.areEqual(expectedFailureMessage, failureMessages[0]);
Assert.areEqual(expectedFailureMessage, failureMessages[1]);

Integer invalidJobCount = [
    SELECT COUNT()
    FROM Job__c
    WHERE QueueName__c = :queueName AND ExternalId__c IN :invalidExternalIds
];
Assert.areEqual(0, invalidJobCount);

System.debug(
    JSON.serializePretty(
        new Map<String, Object>{
            'feature' => 'queueable.invalid-delay',
            'queueName' => queueName,
            'failureMessages' => failureMessages
        }
    )
);
