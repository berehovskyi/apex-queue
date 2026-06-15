@IsTest(IsParallel=true)
private class QueuesDefinitionServiceTest {
    @IsTest
    static void should_require_active_queue_definition_when_requested() {
        // Arrange
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ QueuesTestSupport.queueDefinition() });
        QueuesDefinitionRepository repositoryStub = (QueuesDefinitionRepository) Test.createStub(
            QueuesDefinitionRepository.class,
            repositoryProvider
        );

        QueuesDefinitionService service = new QueuesDefinitionService(repositoryStub);

        // Act
        QueueDefinition__mdt definition = service.requireQueueDefinition(QueuesTestSupport.DEFAULT_QUEUE_NAME, true);

        // Assert
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, definition.QueueName__c);
        Assert.areEqual(1, repositoryProvider.getCallCount('queryQueueDefinitions'));
    }

    @IsTest
    static void should_query_definitions_and_resolve_default_lease_minutes_when_metadata_is_invalid() {
        // Arrange
        QueueDefinition__mdt queueDefinition = QueuesTestSupport.queueDefinition();
        queueDefinition.DefaultLeaseMinutes__c = 0;
        QueuesTestSupport.RecordingStubProvider repositoryProvider = new QueuesTestSupport.RecordingStubProvider()
            .withReturn('queryQueueDefinitions', new List<QueueDefinition__mdt>{ queueDefinition })
            .withReturn('queryQueueDefinitionsByQueueNames', new List<QueueDefinition__mdt>{ queueDefinition });
        QueuesDefinitionRepository repositoryStub = (QueuesDefinitionRepository) Test.createStub(
            QueuesDefinitionRepository.class,
            repositoryProvider
        );

        QueuesDefinitionService service = new QueuesDefinitionService(repositoryStub);

        // Act
        QueueDefinition__mdt definition = service.findQueueDefinitionAsSystem(QueuesTestSupport.DEFAULT_QUEUE_NAME);
        Map<String, QueueDefinition__mdt> definitionsByQueueName = service.findQueueDefinitionsByQueueNamesAsSystem(
            new Set<String>{ QueuesTestSupport.DEFAULT_QUEUE_NAME }
        );
        Integer leaseMinutes = service.getLeaseMinutes(queueDefinition);

        // Assert
        Assert.areEqual(QueuesTestSupport.DEFAULT_QUEUE_NAME, definition.QueueName__c);
        Assert.areEqual(
            QueuesTestSupport.DEFAULT_QUEUE_NAME,
            definitionsByQueueName.get(QueuesTestSupport.DEFAULT_QUEUE_NAME).QueueName__c
        );
        Assert.areEqual(5, leaseMinutes);
        Assert.areEqual(1, repositoryProvider.getCallCount('queryQueueDefinitions'));
        Assert.areEqual(1, repositoryProvider.getCallCount('queryQueueDefinitionsByQueueNames'));
    }
}
